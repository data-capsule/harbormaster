use std::{collections::HashSet, sync::Arc};

use log::{info, warn};
use prost::Message as _;
use tokio::sync::Mutex;

use crate::{
    config::AtomicConfig,
    crypto::AtomicKeyStore,
    proto::{consensus::{ProtoReconfiguration, ProtoReconfigurationSignal, ProtoReconfigurationStorageVote}, rpc::ProtoPayload},
    rpc::{PinnedMessage, SenderType, client::{Client, PinnedClient}, server::LatencyProfile},
    utils::channel::Receiver,
};

pub enum ReconfigurationMessage {
    Signal(ProtoReconfigurationSignal),
    StorageVote(ProtoReconfigurationStorageVote),
}

struct ReconfigurationState {
    config_num: u64,
    storage_servers: Vec<String>,
    old_storage_server_votes: HashSet<String>,
    old_config_commit_threshold: usize,

}

pub struct ReconfigurationCoordinator {
    config: AtomicConfig,
    client: PinnedClient,
    signal_rx: Receiver<ReconfigurationMessage>,

    /// Starts at 0, that's the first config everybody starts with.
    config_num_counter: u64,

    /// If this is None, then we can accept a new reconfiguration signal.
    /// If this is Some, then we are in the middle of a reconfiguration and all reconfiguration commands will be dropped.
    current_reconfiguration: Option<ReconfigurationState>,
}

impl ReconfigurationCoordinator {
    pub fn new(
        config: AtomicConfig,
        keystore: AtomicKeyStore,
        signal_rx: Receiver<ReconfigurationMessage>,
    ) -> Self {
        let client = Client::new_atomic(config.clone(), keystore.clone(), false, 0).into();
        Self {
            config,
            client,
            signal_rx,

            config_num_counter: 0,
            current_reconfiguration: None,
        }
    }

    pub async fn run(coordinator: Arc<Mutex<Self>>) {
        let mut coordinator = coordinator.lock().await;

        while let Ok(()) = coordinator.worker().await {}
    }

    async fn worker(&mut self) -> Result<(), ()> {
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
            None => Err(()),
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
            old_storage_server_votes: HashSet::new(),
            old_config_commit_threshold: current_storage_servers.len() / 2 + 1, // Majority number of old storage servers.
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

    async fn send_stop_acks(&self, storage_servers: &[String]) {
        let stop_msg = ProtoReconfiguration { stop_acks: true, kill: false };

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

    async fn handle_storage_vote(&mut self, vote: ProtoReconfigurationStorageVote) {
        info!("Storage vote received: {:?}", vote);

        if self.current_reconfiguration.is_none() {
        
            warn!("Dropping vote from {}. Maybe last reconfiguration already completed.", vote.storage_server_name);
            return;
        }

        let reconfiguration_state = self.current_reconfiguration.as_mut().unwrap();
        reconfiguration_state.old_storage_server_votes.insert(vote.storage_server_name);

        self.maybe_finalize_reconfiguration().await;
    }

    async fn maybe_finalize_reconfiguration(&mut self) {
        if self.current_reconfiguration.is_none() {
            // Should be unreachable.
            return;
        }

        let reconfiguration_state = self.current_reconfiguration.as_mut().unwrap();
        if reconfiguration_state.old_storage_server_votes.len() >= reconfiguration_state.old_config_commit_threshold {
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
    }

    async fn send_kill_signal(&self, storage_servers: &[String]) {
        let kill_msg = ProtoReconfiguration { kill: true, stop_acks: false };

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
}
