use std::sync::Arc;

use tokio::sync::{oneshot, Mutex};

use crate::{config::AtomicConfig, crypto::{AtomicKeyStore, CachedBlock, CryptoServiceConnector}, proto::consensus::ProtoAppendEntries, rpc::SenderType, utils::{channel::{Receiver, Sender}, StorageServiceConnector}};

pub struct ForkReceiver {
    config: AtomicConfig,
    keystore: AtomicKeyStore,

    ae_rx: Receiver<(ProtoAppendEntries, SenderType)>,
    crypto: CryptoServiceConnector,
    storage: StorageServiceConnector,
    staging_tx: Sender<oneshot::Receiver<CachedBlock>>,
}

impl ForkReceiver {
    pub fn new(
        config: AtomicConfig, keystore: AtomicKeyStore,
        ae_rx: Receiver<(ProtoAppendEntries, SenderType)>,
        crypto: CryptoServiceConnector,
        storage: StorageServiceConnector,
        staging_tx: Sender<oneshot::Receiver<CachedBlock>>,
    ) -> Self {
        Self {
            config,
            keystore,
            ae_rx,
            crypto,
            storage,
            staging_tx,
        }
    }

    pub async fn run(fork_receiver: Arc<Mutex<ForkReceiver>>) {
        let mut fork_receiver = fork_receiver.lock().await;

        while let Ok(_) = fork_receiver.worker().await {

        }

    }

    async fn worker(&mut self) -> Result<(), ()> {
        Ok(())
    }
}