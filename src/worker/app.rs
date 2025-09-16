use std::{future::Future, marker::PhantomData, pin::Pin, sync::Arc, time::Duration};

use anyhow::Ok;
use futures::{channel::oneshot, stream::FuturesUnordered, StreamExt};
use hashbrown::HashSet;
use log::{error, info, warn};
use num_bigint::{BigInt, Sign};
use prost::Message as _;
use tokio::{sync::Mutex, task::JoinSet};

use crate::{config::{AtomicConfig, AtomicPSLWorkerConfig}, consensus::batch_proposal::{MsgAckChanWithTag, TxWithAckChanTag}, crypto::{default_hash, hash, AtomicKeyStore}, proto::{client::{ProtoClientReply, ProtoClientRequest, ProtoTransactionReceipt}, consensus::ProtoVectorClock, execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionOpResult, ProtoTransactionOpType, ProtoTransactionPhase, ProtoTransactionResult}, rpc::ProtoPayload}, rpc::{client::{Client, PinnedClient}, server::LatencyProfile, PinnedMessage, SenderType}, utils::{channel::{make_channel, Receiver, Sender}, timer::ResettableTimer}, worker::block_sequencer::{BlockSeqNumQuery, VectorClock}};

use super::cache_manager::{CacheCommand, CacheError};

pub struct CacheConnector {
    cache_tx: Sender<CacheCommand>,
    blocking_client: PinnedClient,
    nonblocking_client: PinnedClient,
}

// const NUM_WORKER_THREADS: usize = 4;
// const NUM_REPLIER_THREADS: usize = 20;

enum FutureSeqNum {
    None,
    Immediate(u64),
    Future(oneshot::Receiver<u64>),
}

impl FutureSeqNum {
    pub fn new() -> Self {
        Self::None
    }

    pub async fn get_seq_num(&mut self) -> Option<u64> {
        match self {
            Self::None => None,
            Self::Immediate(seq_num) => Some(*seq_num),
            Self::Future(rx) => {
                let seq_num = rx.await.unwrap();
                *self = Self::Immediate(seq_num);
                Some(seq_num)
            }
        }
    }
}

impl CacheConnector {
    pub fn new(cache_tx: Sender<CacheCommand>, blocking_client: PinnedClient, nonblocking_client: PinnedClient) -> Self {
        Self { cache_tx, blocking_client, nonblocking_client }
    }

    pub async fn dispatch_read_request(
        &self,
        key: Vec<u8>,
    ) -> anyhow::Result<(Vec<u8>, u64), CacheError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let command = CacheCommand::Get(key, tx);

        self.cache_tx.send(command).await;

        let result = rx.await.unwrap();

        result
    }

    pub async fn dispatch_write_request(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> anyhow::Result<(u64 /* lamport ts */, tokio::sync::oneshot::Receiver<u64 /* block seq num */>), CacheError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let val_hash = BigInt::from_bytes_be(Sign::Plus, &hash(&value));
        let command = CacheCommand::Put(key, value, val_hash, BlockSeqNumQuery::WaitForSeqNum(tx), response_tx);

        self.cache_tx.send(command).await;

        let result = response_rx.await.unwrap()?;
        std::result::Result::Ok((result, rx))
    }

    pub async fn dispatch_commit_request(&self) {
        let command = CacheCommand::Commit;

        self.cache_tx.send(command).await;
    }

    pub async fn dispatch_lock_request(&self, key: Vec<u8>, is_read: bool) -> Result<(), CacheError> {
        let op_type = if is_read {
            ProtoTransactionOpType::Read
        } else {
            ProtoTransactionOpType::Write
        };

        let tx = ProtoTransaction {
            on_crash_commit: Some(ProtoTransactionPhase {
                ops: vec![ProtoTransactionOp {
                    op_type: op_type as i32,
                    operands: vec![key],
                }],
            }),
            on_byzantine_commit: None,
            on_receive: None,
            is_reconfiguration: false,
            is_2pc: false,
        };

       let origin = self.blocking_client.0.config.get().net_config.name.clone();

        let payload = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::ClientRequest(ProtoClientRequest {
                tx: Some(tx),
                origin,
                sig: vec![0u8; 1],
                client_tag: 0,
            }))
        };

        let buf = payload.encode_to_vec();
        let sz = buf.len();
        let request = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

        let res = PinnedClient::send_and_await_reply(&self.blocking_client, &"sequencer1".to_string(), request.as_ref()).await;
        
        if let Err(e) = res {
            return Err(CacheError::LockNotAcquirable);
        }

        let res = res.unwrap();
        
        let std::result::Result::Ok(receipt) = ProtoClientReply::decode(&res.as_ref().0.as_slice()[0..res.as_ref().1]) else {
            return Err(CacheError::InternalError);
        };

        let Some(receipt) = receipt.reply else {
            return Err(CacheError::InternalError);
        };

        let crate::proto::client::proto_client_reply::Reply::Receipt(receipt) = receipt else {
            return Err(CacheError::InternalError);
        };

        let Some(result) = receipt.results else {
            return Err(CacheError::InternalError);
        };

        let Some(result) = result.result.first() else {
            return Err(CacheError::InternalError);
        };

        if result.success {
            let Some(vc) = result.values.first() else {
                return Err(CacheError::InternalError);
            };

            let _sz = vc.len();

            let std::result::Result::Ok(proto_vc) = ProtoVectorClock::decode(&vc.as_slice()[0.._sz]) else {
                return Err(CacheError::InternalError);
            };

            let vc = VectorClock::from(Some(proto_vc));
            warn!("Waiting for VC: {:?}", vc);
            let _ = self.cache_tx.send(CacheCommand::WaitForVC(vc)).await;
            std::result::Result::Ok(())
        } else {
            Err(CacheError::LockNotAcquirable)
        }
        

    }

    pub async fn dispatch_unlock_request(&self, mut keys_and_vcs: Vec<(Vec<u8>, VectorClock)>) {
        let op_type = ProtoTransactionOpType::Unblock;

        let tx = ProtoTransaction {
            on_crash_commit: Some(ProtoTransactionPhase {
                ops: keys_and_vcs.drain(..).map(|(key, vc)| {
                    ProtoTransactionOp {
                        op_type: op_type as i32,
                        operands: vec![key, vc.serialize().encode_to_vec()],
                    }
                }).collect(),
            }),
            on_byzantine_commit: None,
            on_receive: None,
            is_reconfiguration: false,
            is_2pc: false,
        };

        let origin = self.nonblocking_client.0.config.get().net_config.name.clone();

        let payload = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::ClientRequest(ProtoClientRequest {
                tx: Some(tx),
                origin,
                sig: vec![0u8; 1],
                client_tag: 0,
            }))
        };

        let buf = payload.encode_to_vec();
        let sz = buf.len();
        let request = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

        let _ = PinnedClient::send(&self.nonblocking_client, &"sequencer1".to_string(), request.as_ref()).await;

    }
}

pub type UncommittedResultSet = (Vec<ProtoTransactionOpResult>, MsgAckChanWithTag, Option<u64> /* Some(potential seq_num; wait till committed) | None(reply immediately) */);

pub trait ClientHandlerTask {
    fn new(cache_tx: CacheConnector, id: usize) -> Self;
    fn get_cache_connector(&self) -> &CacheConnector;
    fn get_id(&self) -> usize;
    fn get_total_work(&self) -> usize; // Useful for throghput calculation.
    fn on_client_request(&mut self, request: TxWithAckChanTag, reply_handler_tx: &Sender<UncommittedResultSet>) -> impl Future<Output = Result<(), anyhow::Error>> + Send + Sync;
}

pub struct PSLAppEngine<T: ClientHandlerTask> {
    config: AtomicPSLWorkerConfig,
    key_store: AtomicKeyStore,
    cache_tx: Sender<CacheCommand>,
    client_command_rx: Receiver<TxWithAckChanTag>,
    commit_tx_spawner: tokio::sync::broadcast::Sender<u64>,
    handles: JoinSet<()>,
    client_handler_phantom: PhantomData<T>,
    log_timer: Arc<Pin<Box<ResettableTimer>>>,
}

impl<T: ClientHandlerTask + Send + Sync + 'static> PSLAppEngine<T> {
    pub fn new(config: AtomicPSLWorkerConfig, key_store: AtomicKeyStore, cache_tx: Sender<CacheCommand>, client_command_rx: Receiver<TxWithAckChanTag>, commit_tx_spawner: tokio::sync::broadcast::Sender<u64>) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        Self {
            config,
            key_store,
            cache_tx,
            client_command_rx,
            commit_tx_spawner,
            handles: JoinSet::new(),
            client_handler_phantom: PhantomData,
            log_timer,
        }
    }

    pub async fn run(app: Arc<Mutex<Self>>) -> anyhow::Result<()> {
        let mut app = app.lock().await;
        let _chan_depth = app.config.get().rpc_config.channel_depth as usize;

        // We are relying on the MPMC functionality of async-channel.
        // tokio channel won't work here.
        let (reply_tx, reply_rx) = make_channel(_chan_depth);

        let mut total_work_txs: Vec<crate::utils::channel::AsyncSenderWrapper<tokio::sync::oneshot::Sender<usize>>> = Vec::new();

        app.log_timer.run().await;

        let client_config = AtomicConfig::new(app.config.get().to_config());

        for id in 0..app.config.get().worker_config.num_worker_threads_per_worker {
            let blocking_client = Client::new_atomic(client_config.clone(), app.key_store.clone(), true, (id + 1000) as u64).into();
            let nonblocking_client = Client::new_atomic(client_config.clone(), app.key_store.clone(), true, (id + 2000) as u64).into();
            let cache_tx = app.cache_tx.clone();
            let cache_connector = CacheConnector::new(cache_tx, blocking_client, nonblocking_client);
            let _reply_tx = reply_tx.clone();
            let client_command_rx = app.client_command_rx.clone();
            let (total_work_tx, total_work_rx) = make_channel(_chan_depth);
            total_work_txs.push(total_work_tx);

            app.handles.spawn(async move {
                let mut handler_task = T::new(cache_connector, id);

                loop {
                    tokio::select! {
                        Some(command) = client_command_rx.recv() => {
                            handler_task.on_client_request(command, &_reply_tx).await;
                        }
                        Some(_tx) = total_work_rx.recv() => {
                            _tx.send(handler_task.get_total_work());
                        }
                    }
                }
            });
        }

        for _ in 0..app.config.get().worker_config.num_replier_threads_per_worker {
            let _reply_rx = reply_rx.clone();
            let mut _commit_rx = app.commit_tx_spawner.subscribe();

            app.handles.spawn(async move {
                let mut commit_seq_num = 0;
                let mut pending_results = Vec::new();

                loop {
                    tokio::select! {
                        std::result::Result::Ok(seq_num) = _commit_rx.recv() => {
                            commit_seq_num = seq_num;
                            
                        },
                        Some(result) = _reply_rx.recv() => {
                            let (result, ack_chan, seq_num) = result;
                            let seq_num = seq_num.unwrap_or(0);
                            pending_results.push((result, ack_chan, seq_num));
                        }
                    }

                    for (result, ack_chan, seq_num) in &pending_results {
                        if *seq_num > commit_seq_num {
                            continue;
                        }

                        let reply = ProtoTransactionReceipt {
                            block_n: *seq_num,
                            tx_n: 0,
                            results: Some(ProtoTransactionResult {
                                result: result.clone(),
                            }),
                            await_byz_response: false,
                            byz_responses: vec![],
                            req_digest: default_hash(),
                        };

                        let reply = ProtoClientReply {
                            client_tag: ack_chan.1,
                            reply: Some(crate::proto::client::proto_client_reply::Reply::Receipt(reply)),
                        };

                        let buf = reply.encode_to_vec();
                        let len = buf.len();
                        let msg = PinnedMessage::from(buf, len, SenderType::Anon);
                        ack_chan.0.send((msg, LatencyProfile::new())).await;

                    }

                    pending_results.retain(|(_, _, seq_num)| *seq_num > commit_seq_num);
                }  
            });
        }

        loop {
            app.log_timer.wait().await;

            let mut total_work = 0;
            for tx in &total_work_txs {
                let (_tx, _rx) = tokio::sync::oneshot::channel();
                tx.send(_tx).await.unwrap();

                total_work += _rx.await.unwrap();
            }

            info!("Total requests processed: {}", total_work);
        }
        Ok(())
    }
}


pub struct KVSTask {
    cache_connector: CacheConnector,
    id: usize,
    total_work: usize,
}

impl ClientHandlerTask for KVSTask {
    fn new(cache_connector: CacheConnector, id: usize) -> Self {
        Self {
            cache_connector,
            id,
            total_work: 0,
        }
    }

    fn get_cache_connector(&self) -> &CacheConnector {
        &self.cache_connector
    }

    fn get_total_work(&self) -> usize {
        self.total_work
    }

    fn get_id(&self) -> usize {
        self.id
    }

    async fn on_client_request(&mut self, request: TxWithAckChanTag, reply_handler_tx: &Sender<UncommittedResultSet>) -> anyhow::Result<()> {
        let req = &request.0;
        let resp = &request.1;
        self.total_work += 1;
        
        
        if req.is_none() {
            return self.reply_invalid(resp, reply_handler_tx).await;
        }


        let req = req.as_ref().unwrap();
        if req.on_receive.is_none() {

            // For PSL, all transactions must be on_receive.
            // on_crash_commit and on_byz_commit are meaningless.
            return self.reply_invalid(resp, reply_handler_tx).await;
        }

        let on_receive = req.on_receive.as_ref().unwrap();


        if let std::result::Result::Ok((results, seq_num)) = self.execute_ops(on_receive.ops.as_ref()).await {
            return self.reply_receipt(resp, results, seq_num, reply_handler_tx).await;
        }

        self.reply_invalid(resp, reply_handler_tx).await
        

    }
}

impl KVSTask {
    async fn execute_ops(&self, ops: &Vec<ProtoTransactionOp>) -> Result<(Vec<ProtoTransactionOpResult>, Option<u64>), anyhow::Error> {
        let mut atleast_one_write = false;
        let mut last_write_index = 0;
        let mut highest_committed_block_seq_num_needed = 0;
        let mut block_seq_num_rx_vec = FuturesUnordered::new();
        let mut results = Vec::new();

        for (i, op) in ops.iter().enumerate() {
            let op_type = op.op_type();

            match op_type {
                ProtoTransactionOpType::Write  => {
                    atleast_one_write = true;
                    last_write_index = i;
                },
                _ => {}
            }
        }

        let mut locked_keys = Vec::new();

        let mut locking_num = 0;

        for op in ops {
            let op_type = op.op_type();

            match op_type {
                ProtoTransactionOpType::Write => {
                    let key = op.operands[0].clone();

                    let locking_key = format!("LOCK:{}", locking_num).as_bytes().to_vec();
                    locking_num += 1;
                    locked_keys.push(locking_key.clone());

                    self.cache_connector.dispatch_lock_request(locking_key, false).await;

                    let value = op.operands[1].clone();
                    let res = self.cache_connector.dispatch_write_request(key, value).await;
                    if let std::result::Result::Err(e) = res {
                        return Err(e.into());
                    }

                    let (_, block_seq_num_rx) = res.unwrap();
                    block_seq_num_rx_vec.push(block_seq_num_rx);
                    results.push(ProtoTransactionOpResult {
                        success: true,
                        values: vec![],
                    });
                },
                ProtoTransactionOpType::Read => {
                    let key = op.operands[0].clone();
                    let locking_key = format!("LOCK:{}", locking_num).as_bytes().to_vec();
                    locking_num += 1;
                    locked_keys.push(locking_key.clone());
                    self.cache_connector.dispatch_lock_request(locking_key, true).await;
                    match self.cache_connector.dispatch_read_request(key).await {
                        std::result::Result::Ok((value, seq_num)) => {
                            results.push(ProtoTransactionOpResult {
                                success: true,
                                values: vec![value, seq_num.to_be_bytes().to_vec()],
                            });
                        }
                        std::result::Result::Err(_e) => {
                            results.push(ProtoTransactionOpResult {
                                success: false,
                                values: vec![],
                            });
                        }
                    };
                },
                _ => {}
            }
            
        }

        // Must unlock in reverse order.
        error!("Locked keys: {:?}", locked_keys);
        locked_keys.reverse();
        self.cache_connector.dispatch_unlock_request(locked_keys.iter().map(|key| (key.clone(), VectorClock::new())).collect()).await;

        self.cache_connector.dispatch_commit_request().await;

        if atleast_one_write {

            // Find the highest block seq num needed.
            while let Some(seq_num) = block_seq_num_rx_vec.next().await {
                if seq_num.is_err() {
                    continue;
                }

                let seq_num = seq_num.unwrap();
                highest_committed_block_seq_num_needed = std::cmp::max(highest_committed_block_seq_num_needed, seq_num);
            }

            return Ok((results, Some(highest_committed_block_seq_num_needed)));
        }

        Ok((results, None))
    }

    async fn reply_receipt(&self, resp: &MsgAckChanWithTag, results: Vec<ProtoTransactionOpResult>, seq_num: Option<u64>, reply_handler_tx: &Sender<UncommittedResultSet>) -> anyhow::Result<()> {
        reply_handler_tx.send((results, resp.clone(), seq_num)).await;
        Ok(())
    }

    async fn reply_invalid(&self, resp: &MsgAckChanWithTag, reply_handler_tx: &Sender<UncommittedResultSet>) -> anyhow::Result<()> {
        // For now, just send a blank result.
        
        reply_handler_tx.send((vec![], resp.clone(), None)).await;
        Ok(())
    }


}