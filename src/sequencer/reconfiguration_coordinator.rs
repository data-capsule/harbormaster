use std::{collections::{HashMap, HashSet}, sync::Arc};

use tokio::sync::mpsc::UnboundedReceiver;
use log::{info, warn};
use prost::Message as _;
use tokio::sync::Mutex;

use crate::{
    config::AtomicConfig, crypto::AtomicKeyStore, proto::{consensus::{ProtoCurrentConfigurationQuery, ProtoCurrentConfigurationReply, ProtoReconfiguration, ProtoReconfigurationSignal, ProtoReconfigurationStorageVote, StorageList}, rpc::ProtoPayload}, rpc::{MessageRef, PinnedMessage, SenderType, client::{Client, PinnedClient}, server::{LatencyProfile, MsgAckChan}}, utils::channel::Receiver
};

pub enum ReconfigurationMessage {
    Signal(ProtoReconfigurationSignal),
    StorageVote(ProtoReconfigurationStorageVote),
    Query(SenderType, MsgAckChan, ProtoCurrentConfigurationQuery),
}


/// Steps for reconfiguration:
/// 1. Coordinator sends reconfiguration signal to all current storage servers.
/// 2. Storage servers stop processing workers' new blocks and replies with a vote.
/// 3. Once f_old + 1 old storage servers have voted, coordinator sends backfill signal to all new storage servers.
/// 4. Once new storage servers finish backfilling, they reply with a vote.
/// 5. Once f_new + 1 new storage servers have voted, coordinator sends kill signal to all old storage servers and finalizes the configuration.
/// 6. Workers eventually time out on committing new blocks and query the coordinator for the current configuration.
/// 7. Coordinator buffers such queries as long as the current reconfiguration is going on.
struct ReconfigurationState {
    config_num: u64,
    storage_servers: Vec<String>,
    old_storage_server_votes: HashMap<String, HashMap<String /* worker name */, u64 /* last confirmed n */>>,
    old_config_commit_threshold: usize,
    new_storage_server_votes: HashSet<String>,
    new_config_commit_threshold: usize,
    workers_to_ack: HashMap<String, MsgAckChan>,
    backfill_already_sent: bool,

}

pub struct ReconfigurationCoordinator {
    config: AtomicConfig,
    client: PinnedClient,
    signal_rx: Receiver<ReconfigurationMessage>,
    ci_rx: UnboundedReceiver<(String /* worker name */, u64 /* seq num */)>,

    /// Starts at 0, that's the first config everybody starts with.
    config_num_counter: u64,

    /// If this is None, then we can accept a new reconfiguration signal.
    /// If this is Some, then we are in the middle of a reconfiguration and all reconfiguration commands will be dropped.
    current_reconfiguration: Option<ReconfigurationState>,

    commit_indices: HashMap<String, u64>, // Worker name -> commit index.
}

impl ReconfigurationCoordinator {
    pub fn new(
        config: AtomicConfig,
        keystore: AtomicKeyStore,
        signal_rx: Receiver<ReconfigurationMessage>,
        ci_rx: UnboundedReceiver<(String /* worker name */, u64 /* seq num */)>,
    ) -> Self {
        let client = Client::new_atomic(config.clone(), keystore.clone(), false, 0).into();
        let commit_indices = config.get().consensus_config.watchlist.iter().map(|worker| (worker.clone(), 0)).collect();
        
        Self {
            config,
            client,
            signal_rx,
            ci_rx,

            config_num_counter: 0,
            current_reconfiguration: None,
            commit_indices,
        }
    }

    pub async fn run(coordinator: Arc<Mutex<Self>>) {
        let mut coordinator = coordinator.lock().await;

        while let Ok(()) = coordinator.worker().await {}
    }

    async fn worker(&mut self) -> Result<(), ()> {
        if self.ci_rx.len() > 0 {
            let mut ci_rx_buffer = Vec::with_capacity(self.ci_rx.len());
            self.ci_rx.recv_many(&mut ci_rx_buffer, self.ci_rx.len()).await;
            self.handle_new_commits(ci_rx_buffer).await;
        }
        let signal = self.signal_rx.recv().await;
        match signal {
            Some(ReconfigurationMessage::Signal(signal)) => {
                self.handle_reconfiguration(signal).await;
                Ok(())
            }
            Some(ReconfigurationMessage::StorageVote(vote)) => {
                self.handle_storage_vote(vote).await;
                Ok(())
            }
            Some(ReconfigurationMessage::Query(sender, ack_chan, query)) => {
                self.handle_query(sender, ack_chan, query).await;
                Ok(())
            }
            None => Err(()),
        }
    }

    async fn handle_new_commits(&mut self, commits: Vec<(String /* worker name */, u64 /* seq num */)>) {
        for (worker_name, seq_num) in &commits {
            self.commit_indices.insert(worker_name.clone(), *seq_num);
            // self.maybe_reply_to_buffered_query(worker_name.clone()).await;
        }
    }

    async fn handle_reconfiguration(&mut self, signal: ProtoReconfigurationSignal) {
        if self.current_reconfiguration.is_some() {
            warn!("Reconfiguration signal received while already in a reconfiguration. Dropping.");
            return;
        }

        info!(
            "Reconfiguration signal received. New storage servers: {:?}",
            signal.new_storage_servers
        );


        let current_storage_servers = self.get_current_storage_server_list();


        // Store reconfiguration state.
        self.config_num_counter += 1;
        let reconfiguration_state = ReconfigurationState {
            config_num: self.config_num_counter,
            storage_servers: signal.new_storage_servers.clone(),
            old_storage_server_votes: HashMap::new(),
            old_config_commit_threshold: current_storage_servers.len() / 2 + 1, // Majority number of old storage servers.
            new_storage_server_votes: HashSet::new(),
            new_config_commit_threshold: signal.new_storage_servers.len() / 2 + 1, // Majority number of new storage servers.
            workers_to_ack: HashMap::new(),
            backfill_already_sent: false,
        };
        self.current_reconfiguration = Some(reconfiguration_state);

        // Tell existing storage servers to stop sending acks to workers.
        self.send_stop_acks(&current_storage_servers).await;
    }

    fn get_current_storage_server_list(&self) -> Vec<String> {
        self.config
            .get()
            .consensus_config
            .node_list
            .clone()
    }

    fn get_worker_list(&self) -> Vec<String> {
        self.config
            .get()
            .consensus_config
            .watchlist
            .clone()
    }

    /// Step 1
    async fn send_stop_acks(&self, storage_servers: &[String]) {
        // Load balance each old server to new servers.
        let stop_msg = ProtoReconfiguration { stop_acks: true, kill: false, backfill: false, request_from: HashMap::new(), last_confirmed_n: HashMap::new(), start_n: HashMap::new() };

        let payload = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::Reconfiguration(
                stop_msg,
            )),
        };

        let buf = payload.encode_to_vec();
        let sz = buf.len();
        let msg = PinnedMessage::from(buf, sz, SenderType::Anon);

        let names = storage_servers.to_vec();
        let mut profile = LatencyProfile::new();

        info!("Broadcasting stop-acks to storage servers: {:?}", names);
        let resp = PinnedClient::broadcast(
            &self.client,
            &names,
            &msg,
            &mut profile,
            0,
        )
        .await;

        if let Err(e) = resp {
            warn!("Failed to broadcast stop-acks to storage servers: {:?}", e);
        }
    }


    /// Same function to capture votes for both step 2 and step 4.
    /// Assumes that no storage server in old configuration is in new configuration and vice versa.
    async fn handle_storage_vote(&mut self, vote: ProtoReconfigurationStorageVote) {
        info!("Storage vote received: {:?}", vote);

        if self.current_reconfiguration.is_none() {
        
            warn!("Dropping vote from {}. Maybe last reconfiguration already completed.", vote.storage_server_name);
            return;
        }

        let reconfiguration_state = self.current_reconfiguration.as_mut().unwrap();

        if reconfiguration_state.storage_servers.contains(&vote.storage_server_name) {
            reconfiguration_state.new_storage_server_votes.insert(vote.storage_server_name);
        } else {
            reconfiguration_state.old_storage_server_votes.insert(vote.storage_server_name, vote.last_confirmed_n);
        }
        // reconfiguration_state.old_storage_server_votes.insert(vote.storage_server_name);

        if !reconfiguration_state.backfill_already_sent {
            self.maybe_send_backfill_signal().await; // For step 3.
        }
        self.maybe_finalize_reconfiguration().await; // For step 5.
    }

    async fn maybe_send_backfill_signal(&mut self) {
        if self.current_reconfiguration.is_none() {
            return;
        }

        let reconfiguration_state = self.current_reconfiguration.as_mut().unwrap();
        if reconfiguration_state.old_storage_server_votes.len() >= reconfiguration_state.old_config_commit_threshold // Majority number of old storage servers voted.
        {
            self.send_backfill_signal().await;
        }   
    }

    async fn send_backfill_signal(&mut self) {
        let reconfiguration_state = self.current_reconfiguration.as_mut().unwrap();
        let max_last_confirmed_ns = reconfiguration_state.old_storage_server_votes.values()
            .fold(HashMap::new(), |mut acc: HashMap<String, u64>, last_confirmed_n| {
                for (worker_name, last_confirmed_n) in last_confirmed_n.iter() {
                    if !acc.contains_key(worker_name) {
                        acc.insert(worker_name.clone(), *last_confirmed_n);
                    } else {
                        if *last_confirmed_n > *acc.get(worker_name).unwrap() {
                            acc.insert(worker_name.clone(), *last_confirmed_n);
                        }
                    }
                }
                acc
            });

        let mut worker_server_map = HashMap::new(); // (worker name, new storage server name) --> old storage server name
        for (worker_name, max_confirmed_n) in max_last_confirmed_ns.iter() {
            // How many storage servers have all these entries?
            let full_servers = reconfiguration_state.old_storage_server_votes
                .iter()
                .filter(|(_, last_confirmed_n)| last_confirmed_n.contains_key(worker_name) && *last_confirmed_n.get(worker_name).unwrap() >= *max_confirmed_n)
                .map(|(storage_server_name, _)| storage_server_name.clone())
                .collect::<Vec<String>>();
            
            // If we just had the commit index, then we were guaranteed to have at least f_old + 1 entries here.
            // But we don't have that here.
            // So at least 1 is guaranteed.

            // Need to load balance new storage servers across these full servers.

            let mut __j = 0;
            for new_storage_server in reconfiguration_state.storage_servers.iter() {
                worker_server_map.insert((worker_name.clone(), new_storage_server.clone()), full_servers[__j].clone());
                __j = (__j + 1) % full_servers.len();
            }
        }

        let new_server_request_from_map = worker_server_map.iter()
            .map(|((worker_name, new_storage_server), old_storage_server)| (new_storage_server.clone(), worker_name.clone(), old_storage_server.clone()))
            .fold(HashMap::new(), |mut acc, (new_storage_server, worker_name, old_storage_server)| {
                let entry = acc.entry(new_storage_server.clone()).or_insert(HashMap::new());
                entry.insert(worker_name.clone(), old_storage_server.clone());
                acc
            });

        for new_storage_server in reconfiguration_state.storage_servers.iter() {
            let request_from = new_server_request_from_map.get(new_storage_server).unwrap().clone();
            let reconfiguration_message = ProtoReconfiguration { stop_acks: false, kill: false, backfill: true,
                request_from,
                last_confirmed_n: max_last_confirmed_ns.clone(),
                start_n: self.commit_indices.clone(),
            };
            warn!("Sending backfill signal to {} {:?}", new_storage_server, reconfiguration_message);

            let payload = ProtoPayload {
                message: Some(crate::proto::rpc::proto_payload::Message::Reconfiguration(
                    reconfiguration_message,
                )),
            };

            let buf = payload.encode_to_vec();
            let sz = buf.len();
            let msg = PinnedMessage::from(buf, sz, SenderType::Anon);


            let _ = PinnedClient::send(
                &self.client,
                new_storage_server,
                msg.as_ref(),
            ).await;
        
        }

        reconfiguration_state.backfill_already_sent = true;
    }

    async fn maybe_finalize_reconfiguration(&mut self) {
        if self.current_reconfiguration.is_none() {
            // Should be unreachable.
            return;
        }

        // let acked_workers = self.current_reconfiguration.as_ref().unwrap()
        //     .workers_to_ack.iter()
        //     .map(|(_, ack_info)| {
        //         match ack_info {
        //             Some((_, _, already_replied)) => if *already_replied { 1 } else { 0 },
        //             None => 0,
        //         }
        //     })
        //     .sum::<usize>();

        let reconfiguration_state = self.current_reconfiguration.as_mut().unwrap();
        if reconfiguration_state.old_storage_server_votes.len() >= reconfiguration_state.old_config_commit_threshold // Majority number of old storage servers voted.
        && reconfiguration_state.new_storage_server_votes.len() >= reconfiguration_state.new_config_commit_threshold // Majority number of new storage servers voted.
        {
            self.finalize_reconfiguration().await;
        }
    }

    async fn finalize_reconfiguration(&mut self) {
        let reconfiguration_state = self.current_reconfiguration.take().unwrap();
        info!("Reconfiguration finalized. New storage servers: {:?}", reconfiguration_state.storage_servers);

        let mut config = self.config.get();
        let old_storage_servers = config.consensus_config.node_list.clone();
        let new_config = Arc::make_mut(&mut config);
        new_config.consensus_config.node_list = reconfiguration_state.storage_servers;
        self.config.set(new_config.clone());

        // Send kill signal to all old storage servers.
        self.send_kill_signal(&old_storage_servers).await;

        // Reply to all buffered queries.
        for (worker_name, _) in reconfiguration_state.workers_to_ack.iter() {
            self.reply_to_buffered_query(worker_name.clone()).await;
        }
    }

    async fn send_kill_signal(&self, storage_servers: &[String]) {
        let kill_msg = ProtoReconfiguration { kill: true, stop_acks: false, backfill: false, request_from: HashMap::new(), last_confirmed_n: HashMap::new(), start_n: HashMap::new() };

        let payload = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::Reconfiguration(
                kill_msg,
            )),
        };

        let buf = payload.encode_to_vec();
        let sz = buf.len();
        let msg = PinnedMessage::from(buf, sz, SenderType::Anon);

        let names = storage_servers.to_vec();
        let mut profile = LatencyProfile::new();

        info!("Broadcasting kill signal to storage servers: {:?}", names);
        let resp = PinnedClient::broadcast(
            &self.client,
            &names,
            &msg,
            &mut profile,
            0,
        )
        .await;

        if let Err(e) = resp {
            warn!("Failed to broadcast kill signal to storage servers: {:?}", e);
        } else {
            info!("Kill signal broadcasted to storage servers: {:?}", names);
        }
    }

    async fn handle_query(&mut self, sender: SenderType, ack_chan: MsgAckChan, query: ProtoCurrentConfigurationQuery) {
        if self.current_reconfiguration.is_none() {
            self.send_current_configuration(ack_chan).await;
        } else {
            let (name, _) = sender.to_name_and_sub_id();
            warn!("Received current configuration query from {}.", name);
            self.buffer_query(name, ack_chan, query).await;
        }
    }

    async fn send_current_configuration(&self, ack_chan: MsgAckChan) {
        let current_configuration = ProtoCurrentConfigurationReply {
            storage_servers: self.get_current_storage_server_list(),
            config_num: self.config_num_counter,
        };


        let buf = current_configuration.encode_to_vec();
        let sz = buf.len();
        let msg = PinnedMessage::from(buf, sz, SenderType::Anon);

        let _ = ack_chan.send((msg, LatencyProfile::new())).await;
    }

    async fn buffer_query(&mut self, name: String, ack_chan: MsgAckChan, query: ProtoCurrentConfigurationQuery) {
        let reconfiguration_state = self.current_reconfiguration.as_mut().unwrap();
        if !reconfiguration_state.workers_to_ack.contains_key(&name) {
            warn!("Worker {} not found in workers_to_ack. Dropping query.", name);
            return;
        }
        reconfiguration_state.workers_to_ack.insert(name.clone(), ack_chan);
    }

    // async fn maybe_reply_to_buffered_query(&mut self, name: String) {
    //     if self.current_reconfiguration.is_none() {
    //         return;
    //     }
    //     if !self.current_reconfiguration.as_ref().unwrap().workers_to_ack.contains_key(&name) {
    //         return;
    //     }
    //     if self.current_reconfiguration.as_ref().unwrap().workers_to_ack.get(&name).unwrap().is_none() {
    //         return;
    //     }
    //     if self.current_reconfiguration.as_ref().unwrap().workers_to_ack.get(&name).unwrap().as_ref().unwrap().2 {
    //         // Already replied.
    //         return;
    //     }

    //     let target_ci = self.current_reconfiguration.as_ref().unwrap()
    //         .workers_to_ack.get(&name).unwrap().as_ref()
    //         .unwrap().0;


    //     let current_ci = *self.commit_indices.get(&name).unwrap();

    //     if current_ci >= target_ci {
    //         warn!("Reply to buffered query from {}. Current commit index: {} >= target commit index: {}", name, current_ci, target_ci);
    //         self.reply_to_buffered_query(name).await;
    //     }
    // }

    async fn reply_to_buffered_query(&mut self, name: String) {
        let reconfiguration_state = self.current_reconfiguration.as_mut().unwrap();
        let storage_servers = reconfiguration_state.storage_servers.clone();
        let config_num = reconfiguration_state.config_num;

        let Some(ack_chan) = reconfiguration_state.workers_to_ack.remove(&name) else {
            return;
        };

        let current_configuration = ProtoCurrentConfigurationReply {
            storage_servers,
            config_num,
        };

        let buf = current_configuration.encode_to_vec();
        let sz = buf.len();
        let msg = PinnedMessage::from(buf, sz, SenderType::Anon);
        let _ = ack_chan.send((msg, LatencyProfile::new())).await;        
    }
}
