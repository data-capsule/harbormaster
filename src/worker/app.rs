use std::{future::Future, sync::Arc};

use tokio::{sync::Mutex, task::JoinSet};

use crate::{config::AtomicConfig, consensus::batch_proposal::TxWithAckChanTag, utils::channel::{Receiver, Sender}};

use super::cache_manager::{CacheCommand, CacheError};

pub trait CacheConnector {
    fn dispatch_read_request(
        &self,
        command: CacheCommand,
    ) -> anyhow::Result<()>;

    fn dispatch_write_request(
        &self,
        command: CacheCommand,
    ) -> anyhow::Result<()>;
}

pub trait ClientHandler {
    fn handle_client_request(
        &self,
        request: TxWithAckChanTag,
    ) -> anyhow::Result<()>;
}

pub trait PSLAppEngine {
    type CacheConnector: CacheConnector;
    type ClientHandler: ClientHandler;

    fn new(config: AtomicConfig, cache_connector: Self::CacheConnector, client_handler: Self::ClientHandler) -> Self;
    fn run(app: Arc<Mutex<Self>>) -> impl Future<Output = anyhow::Result<()>> + Send;


}


pub struct KVSApp {
    config: AtomicConfig,
    cache_tx: Sender<CacheCommand>,
    // block_sequencer_tx: Sender<>,
    client_command_rx: Receiver<TxWithAckChanTag>,
    handles: JoinSet<()>,
}

const NUM_WORKER_THREADS: usize = 4;

#[derive(Clone)]
struct WorkerContext {
    id: usize,
    cache_tx: Sender<CacheCommand>,
    block_sequencer_tx: Sender<CacheCommand>,
    client_command_rx: Receiver<TxWithAckChanTag>,
}


impl KVSApp {
    pub fn new(config: AtomicConfig, cache_tx: Sender<CacheCommand>, client_command_rx: Receiver<TxWithAckChanTag>) -> Self {
        Self {
            config,
            cache_tx,
            client_command_rx,
            // block_sequencer_tx,
            handles: JoinSet::new(),
        }
    }



    pub async fn run(app: Arc<Mutex<Self>>) -> anyhow::Result<()> {
        let mut app = app.lock().await;
        
        for i in 0..NUM_WORKER_THREADS {
            let cache_tx = app.cache_tx.clone();
            let block_sequencer_tx = app.block_sequencer_tx.clone();

            // This relies on the MPMC queue implementation in async_channel
            let client_command_rx = app.client_command_rx.clone();
            let config = app.config.clone();
            app.handles.spawn(async move {

                let context = WorkerContext {
                    id: i,
                    cache_tx: cache_tx.clone(),
                    block_sequencer_tx: block_sequencer_tx.clone(),
                    client_command_rx: client_command_rx.clone(),
                };
                Self::worker(context, config).await
            });
        }
        Ok(())
    }

    async fn worker(
        context: WorkerContext,
        config: AtomicConfig,
    ) {
        loop {
            tokio::select! {
                Some(command) = client_command_rx.recv() => {
                    // Handle client command
                    // For example, dispatch to cache connector
                    let _ = cache_tx.send(command);
                }
                else => {
                    // Handle other tasks or exit
                    break;
                }
            }
        }
    }

    async fn dispatch_read_request(context: &WorkerContext, key: Vec<u8>) -> anyhow::Result<(Vec<u8>, u64), CacheError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let command = CacheCommand::Get(key, tx);

        context.cache_tx.send(command).await;

        let result = rx.await.unwrap();

        result
    }

    async fn dispatch_write_request(context: &WorkerContext, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<u64, CacheError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let command = CacheCommand::Put(key, value, tx);

        context.cache_tx.send(command).await;

        let result = rx.await.unwrap();

        result
    }

    
}