use std::{io::Error, pin::Pin, process::exit, sync::Arc, u64};

use hashbrown::HashMap;
use log::{debug, error, trace, warn};
use prost::Message as _;
use tokio::sync::{mpsc::{UnboundedReceiver, UnboundedSender}, oneshot, Mutex};

use crate::{client::worker, config::AtomicConfig, crypto::{AtomicKeyStore, CachedBlock}, proto::{checkpoint::{ProtoAuthSenderType, ProtoBackfillQuery}, consensus::{ProtoReconfiguration, ProtoReconfigurationStorageVote, ProtoVote, StorageList}, rpc::ProtoPayload}, rpc::{MessageRef, PinnedMessage, SenderType, client::{Client, PinnedClient}}, utils::{OptSender, channel::{Receiver, Sender}, timer::ResettableTimer}};

use super::fork_receiver::ForkReceiverCommand;
use crate::utils::OptReceiver;

pub struct Staging {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    client: PinnedClient,
    block_rx: tokio::sync::mpsc::Receiver<(oneshot::Receiver<Result<CachedBlock, Error>>, SenderType /* sender */, SenderType /* origin */)>, // Sender may not be equal to origin.
    logserver_tx: Sender<(SenderType, CachedBlock)>,
    gc_tx: Option<Sender<(SenderType, u64)>>,
    gc_timer: Arc<Pin<Box<ResettableTimer>>>,
    fork_receiver_cmd_tx: UnboundedSender<ForkReceiverCommand>,

    last_confirmed_n: HashMap<SenderType, u64>,
    block_broadcaster_tx: Option<Sender<oneshot::Receiver<CachedBlock>>>,

    must_vote: bool, // Disabled in the sequencer.

    is_defunct: bool,
    reconfiguration_rx: OptReceiver<(SenderType, ProtoReconfiguration)>,
    logserver_query_tx: OptSender<ProtoBackfillQuery>,

    /// If this is Some, then after our last_confirmed_n is at least >= the value in the map, we will ack "sequencer1".
    /// If this is None, simply ignore.
    ack_after_backfill: Option<HashMap<String, u64>>,
}

const PER_PEER_BLOCK_WSS: u64 = 1_000;

impl Staging {
    pub fn new(
        config: AtomicConfig, keystore: AtomicKeyStore,
        block_rx: tokio::sync::mpsc::Receiver<(oneshot::Receiver<Result<CachedBlock, Error>>, SenderType /* sender */, SenderType /* origin */)>, // Sender may not be equal to origin.
        logserver_tx: Sender<(SenderType, CachedBlock)>,
        gc_tx: Option<Sender<(SenderType, u64)>>,
        reconfiguration_rx: OptReceiver<(SenderType, ProtoReconfiguration)>,
        fork_receiver_cmd_tx: UnboundedSender<ForkReceiverCommand>,
        block_broadcaster_tx: Option<Sender<oneshot::Receiver<CachedBlock>>>,
        must_vote: bool,
        logserver_query_tx: OptSender<ProtoBackfillQuery>,
    ) -> Self {
        let client = Client::new_atomic(config.clone(), keystore.clone(), false, 0);
        let gc_timer = ResettableTimer::new(
            std::time::Duration::from_millis(config.get().app_config.checkpoint_interval_ms)
        );
        Self {
            config,
            keystore,
            block_rx,
            logserver_tx,
            fork_receiver_cmd_tx,
            gc_tx,
            client: client.into(),

            last_confirmed_n: HashMap::new(),
            gc_timer,
            block_broadcaster_tx,
            must_vote,

            is_defunct: false,
            reconfiguration_rx,
            logserver_query_tx,

            ack_after_backfill: None,
        }
    }

    pub async fn run(staging: Arc<Mutex<Staging>>) {
        let mut staging = staging.lock().await;

        staging.gc_timer.run().await;

        while let Ok(_) = staging.worker().await {
        
        }

    }

    async fn worker(&mut self) -> Result<(), ()> {
        tokio::select! {
            _tick = self.gc_timer.wait() => {
                self.handle_gc().await?;
            },
            block_and_sender_and_origin = self.block_rx.recv() => {
                if self.is_defunct {
                    trace!("Received block after defunct signal. Dropping block.");
                    return Ok(());
                }
                self.handle_block(block_and_sender_and_origin).await?;
            }
            Some((sender, proto_reconfiguration)) = self.reconfiguration_rx.recv() => {
                self.handle_reconfiguration(sender, proto_reconfiguration).await?;
            }
        }
        Ok(())
    }

    async fn handle_backfill(&mut self, proto_reconfiguration: ProtoReconfiguration) -> Result<(), ()> {
        self.ack_after_backfill = Some(proto_reconfiguration.last_confirmed_n
            .iter().map(|(worker_name, last_n)| (worker_name.clone(), *last_n)).collect());

        let my_name = self.config.get().net_config.name.clone();
        for (worker_name, start_n) in proto_reconfiguration.start_n.iter() {
            let end_n = proto_reconfiguration.last_confirmed_n.get(worker_name).unwrap_or(&u64::MAX);
            let who_to_ask = proto_reconfiguration.request_from.get(worker_name).unwrap();
            let query = ProtoBackfillQuery {
                reply_name: my_name.clone(),
                origin: Some(ProtoAuthSenderType {
                    name: worker_name.clone(),
                    sub_id: 0,
                }),
                start_index: *start_n,
                end_index: *end_n,
            };

            let payload = ProtoPayload {
                message: Some(crate::proto::rpc::proto_payload::Message::BackfillQuery(query)),
            };

            let buf = payload.encode_to_vec();
            let sz = buf.len();
            let msg = PinnedMessage::from(buf, sz, SenderType::Anon);

            let _ = PinnedClient::send(
                &self.client,
                who_to_ask,
                msg.as_ref(),
            ).await;
        }


        Ok(())
    }

    async fn handle_reconfiguration(&mut self, sender: SenderType, proto_reconfiguration: ProtoReconfiguration) -> Result<(), ()> {
        if proto_reconfiguration.kill {
            warn!("Received kill signal. Dying forcefully.");
            exit(0);
        }

        if proto_reconfiguration.backfill {
            warn!("Received backfill signal. Starting backfill.");
            self.handle_backfill(proto_reconfiguration).await?;
            return Ok(());
        }

        // Must be proto_reconfiguration.stop_acks.

        // Forward query to logserver.
        self.is_defunct = true;
        
        let my_name = self.config.get().net_config.name.clone();
        let (sender_name, _) = sender.to_name_and_sub_id();

        // for new_server in
        //     proto_reconfiguration.forward_to.get(&my_name).unwrap_or(&StorageList { storage_servers: Vec::new() }).storage_servers.iter() 
        // {
        //     for (worker_name, commit_index) in proto_reconfiguration.commit_indices.iter() {
        //         let query = ProtoBackfillQuery {
        //             reply_name: new_server.clone(),
        //             origin: Some(ProtoAuthSenderType {
        //                 name: worker_name.clone(),
        //                 sub_id: 0,
        //             }),
        //             start_index: *commit_index,
        //             end_index: *self.last_confirmed_n.get(&SenderType::Auth(worker_name.clone(), 0)).unwrap_or(&u64::MAX),
        //         };
        //         let _ = self.logserver_query_tx.send(query).await;
        //     }
        
        // }

        let vote = ProtoReconfigurationStorageVote {
            storage_server_name: my_name.clone(),
            last_confirmed_n: self.last_confirmed_n.iter().map(|(sender, last_n)| (sender.to_name_and_sub_id().0.clone(), *last_n)).collect(),
        };

        let payload = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::ReconfigurationStorageVote(vote)),
        };

        let buf = payload.encode_to_vec();
        let sz = buf.len();

        log::info!("Sending reconfiguration storage vote from {} to {}", my_name, sender_name);
        let _ = PinnedClient::send(
            &self.client,
            &sender_name,
            MessageRef(&buf, sz, &SenderType::Anon),
        ).await;

        Ok(())
    }

    async fn handle_gc(&mut self) -> Result<(), ()> {
        if self.gc_tx.is_none() {
            return Ok(());
        }

        let gc_tx = self.gc_tx.as_ref().unwrap();

        for (sender, last_n) in self.last_confirmed_n.iter() {
            if *last_n > PER_PEER_BLOCK_WSS {
                let _ = gc_tx.send((sender.clone(), *last_n - PER_PEER_BLOCK_WSS)).await;
            }
        }
        Ok(())
    }



    async fn handle_block(&mut self, block_and_sender_and_origin: Option<(oneshot::Receiver<Result<CachedBlock, Error>>, SenderType, SenderType)>) -> Result<(), ()> {
        // error!("Received block {:?}", block_and_sender_and_origin);
        
        if block_and_sender_and_origin.is_none() {
            return Err(());
        }

        let (block, sender, origin) = block_and_sender_and_origin.unwrap();

        let block = block.await;

        if block.is_err() {
            return Err(());
        }

        let block = block.unwrap();

        match block {
            Ok(block) => {
                self.handle_checked_block(block, sender, origin).await;
            }
            Err(err) => {
                // Handle error

                self.handle_error(err, sender, origin).await;
            }
        }


        Ok(())
    }

    /// 1. Confirm to fork receiver 
    /// 2. Send to logserver
    /// 3. Send vote to sender.
    async fn handle_checked_block(&mut self, block: CachedBlock, sender: SenderType, origin: SenderType) {
        let _ = self.fork_receiver_cmd_tx.send(
            ForkReceiverCommand::Confirm(origin.clone(), block.block.n)
        );

        let _ = self.logserver_tx.send((origin.clone(), block.clone())).await;

        let _ = match &self.block_broadcaster_tx {
            Some(tx) => {
                let (block_tx, block_rx) = oneshot::channel();
                let _ = tx.send(block_rx).await;
                block_tx.send(block.clone()).unwrap();
            }
            None => {
            }
        };

        let last_n = self.last_confirmed_n.entry(origin.clone())
            .or_insert(0);

        if block.block.n > *last_n {
            *last_n = block.block.n;
        }

        if self.ack_after_backfill.is_some() {
            warn!("Potential initial backfill from reconfiguration. Origin: {:?} n: {}", origin, block.block.n);
            self.maybe_ack_sequencer_as_new_server().await;
        }

        self.vote_on_block(block, sender).await;
    }

    async fn maybe_ack_sequencer_as_new_server(&mut self) {
        if self.ack_after_backfill.is_none() {
            return;
        }

        let ack_after_backfill = self.ack_after_backfill.as_ref().unwrap();
        let all_backfilled = ack_after_backfill.iter().all(|(worker_name, last_n)| {
            *self.last_confirmed_n.get(&SenderType::Auth(worker_name.clone(), 0)).unwrap_or(&0) >= *last_n
        });

        if !all_backfilled {
            return;
        }

        let _ = self.ack_after_backfill.take();

        let my_name = self.config.get().net_config.name.clone();
        const SEQUENCER_NAME: &str = "sequencer1";
        let sender_name = SEQUENCER_NAME.to_string();

        let vote = ProtoReconfigurationStorageVote {
            storage_server_name: my_name.clone(),
            last_confirmed_n: self.last_confirmed_n.iter().map(|(sender, last_n)| (sender.to_name_and_sub_id().0.clone(), *last_n)).collect(),
        };

        let payload = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::ReconfigurationStorageVote(vote)),
        };

        let buf = payload.encode_to_vec();
        let sz = buf.len();

        log::info!("Sending reconfiguration storage vote from {} to {}", my_name, sender_name);
        let _ = PinnedClient::send(
            &self.client,
            &sender_name,
            MessageRef(&buf, sz, &SenderType::Anon),
        ).await;

    }

    /// 1. Rollback anything that is not confirmed.
    /// 2. Send backfill Nack to sender
    async fn handle_error(&mut self, err: Error, sender: SenderType, origin: SenderType) {
        error!("Block verification error: {:?}", err);

        let last_n = self.last_confirmed_n.get(&sender).unwrap_or(&0);

        let _ = self.fork_receiver_cmd_tx.send(
            ForkReceiverCommand::Rollback(sender.clone(), *last_n)
        );

        let (origin_name, origin_sub_id) = origin.to_name_and_sub_id();

        let origin = ProtoAuthSenderType {
            name: origin_name,
            sub_id: origin_sub_id,
        };

        self.nack(sender, 1 + *last_n, u64::MAX, origin).await;
    }


    async fn vote_on_block(&mut self, block: CachedBlock, sender: SenderType) {
        if !self.must_vote {
            return;
        }

        let (name, _) = sender.to_name_and_sub_id();


        let vote = ProtoVote {
            fork_digest: block.block_hash.clone(),
            n: block.block.n,
            chain_id: block.block.chain_id,
            // Unused
            sig_array: vec![],
            view: 0,
            config_num: 0,
        };

        let payload = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::Vote(vote)),
        };

        let buf = payload.encode_to_vec();
        let sz = buf.len();


        if name.contains("client"){
            debug!("Voting on test clients. Dropping vote."); // Useful for local testing
            return;
        }

        let _ = PinnedClient::send(&self.client, &name,
            MessageRef(&buf, sz, &SenderType::Anon)
        ).await;


    }

    async fn nack(&mut self, sender: SenderType, start_index: u64, end_index: u64, origin: ProtoAuthSenderType) {
        let my_name = self.config.get().net_config.name.clone();
        let nack = ProtoBackfillQuery {
            start_index,
            end_index,
            reply_name: my_name,
            origin: Some(origin),
        };

        let payload = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::BackfillQuery(nack)),
        };

        let buf = payload.encode_to_vec();
        let sz = buf.len();

        let (name, _) = sender.to_name_and_sub_id();

        let _ = PinnedClient::send(&self.client, &name,
            MessageRef(&buf, sz, &SenderType::Anon)
        ).await;
    }

}