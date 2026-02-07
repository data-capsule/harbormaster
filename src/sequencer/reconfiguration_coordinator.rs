use std::sync::Arc;

use log::{info, warn};
use prost::Message as _;
use tokio::sync::Mutex;

use crate::{
    config::AtomicConfig,
    crypto::AtomicKeyStore,
    proto::{consensus::{ProtoReconfiguration, ProtoReconfigurationSignal}, rpc::ProtoPayload},
    rpc::{client::{Client, PinnedClient}, server::LatencyProfile, PinnedMessage, SenderType},
    utils::channel::Receiver,
};

pub struct ReconfigurationCoordinator {
    config: AtomicConfig,
    client: PinnedClient,
    signal_rx: Receiver<ProtoReconfigurationSignal>,
}

impl ReconfigurationCoordinator {
    pub fn new(
        config: AtomicConfig,
        keystore: AtomicKeyStore,
        signal_rx: Receiver<ProtoReconfigurationSignal>,
    ) -> Self {
        let client = Client::new_atomic(config.clone(), keystore.clone(), false, 0).into();
        Self {
            config,
            client,
            signal_rx,
        }
    }

    pub async fn run(coordinator: Arc<Mutex<Self>>) {
        let mut coordinator = coordinator.lock().await;

        while let Ok(()) = coordinator.worker().await {}
    }

    async fn worker(&mut self) -> Result<(), ()> {
        let signal = self.signal_rx.recv().await;
        match signal {
            Some(signal) => {
                self.handle_reconfiguration(signal).await;
                Ok(())
            }
            None => Err(()),
        }
    }

    async fn handle_reconfiguration(&mut self, signal: ProtoReconfigurationSignal) {
        info!(
            "Reconfiguration signal received. New storage servers: {:?}",
            signal.new_storage_servers
        );

        let current_storage_servers = self.get_current_storage_server_list();

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
        let stop_msg = ProtoReconfiguration { stop_acks: true };

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
}
