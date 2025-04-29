use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{config::AtomicConfig, crypto::{AtomicKeyStore, CachedBlock}, proto::checkpoint::ProtoBackfillNack, utils::channel::Receiver};

pub struct LogServer {
    config: AtomicConfig,
    keystore: AtomicKeyStore,

    block_rx: Receiver<CachedBlock>,
    query_rx: Receiver<ProtoBackfillNack>,


}

impl LogServer {
    pub fn new(
        config: AtomicConfig, keystore: AtomicKeyStore,
        block_rx: Receiver<CachedBlock>,
        query_rx: Receiver<ProtoBackfillNack>,
    ) -> Self {
        Self {
            config,
            keystore,
            block_rx,
            query_rx,
        }
    }

    pub async fn run(logserver: Arc<Mutex<LogServer>>) {
        let mut logserver = logserver.lock().await;

        while let Ok(_) = logserver.worker().await {

        }

    }

    async fn worker(&mut self) -> Result<(), ()> {
        Ok(())
    }
}