use std::sync::Arc;

use tokio::sync::{oneshot, Mutex};

use crate::{config::AtomicConfig, crypto::{AtomicKeyStore, CachedBlock}, utils::channel::{Receiver, Sender}};

pub struct Staging {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    block_rx: Receiver<oneshot::Receiver<CachedBlock>>,
    logserver_tx: Sender<CachedBlock>,
}

impl Staging {
    pub fn new(
        config: AtomicConfig, keystore: AtomicKeyStore,
        block_rx: Receiver<oneshot::Receiver<CachedBlock>>,
        logserver_tx: Sender<CachedBlock>,
    ) -> Self {
        Self {
            config,
            keystore,
            block_rx,
            logserver_tx,
        }
    }

    pub async fn run(staging: Arc<Mutex<Staging>>) {
        let mut staging = staging.lock().await;

        while let Ok(_) = staging.worker().await {

        }

    }

    async fn worker(&mut self) -> Result<(), ()> {
        Ok(())
    }
}