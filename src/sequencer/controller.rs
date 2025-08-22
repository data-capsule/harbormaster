use std::sync::Arc;

use log::info;
use tokio::sync::Mutex;

use crate::{config::AtomicConfig, rpc::{client::PinnedClient, SenderType}, utils::channel::Receiver, worker::{block_sequencer::VectorClock, cache_manager::CacheKey}};

pub enum ControllerCommand {
    BlockAllWorkers,
    UnblockAllWorkers,

    /// This is used to grant release-consistent locks to a worker.
    BlockingLockAcquire(CacheKey, SenderType, VectorClock)
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
enum BlockingState {
    Unblocked = 0,
    Blocked = 1,
}

pub struct Controller {
    config: AtomicConfig,
    client: PinnedClient,
    command_rx: Receiver<ControllerCommand>,

    blocking_state: BlockingState,
}

impl Controller {
    pub fn new(config: AtomicConfig, client: PinnedClient, command_rx: Receiver<ControllerCommand>) -> Self {
        Self {
            config,
            client,
            command_rx,
            blocking_state: BlockingState::Unblocked,
        }
    }

    pub async fn run(controller: Arc<Mutex<Self>>) {
        let mut controller = controller.lock().await;
        
        while let Ok(()) = controller.worker().await {

        }
    }

    async fn worker(&mut self) -> Result<(), ()> {
        let cmd = self.command_rx.recv().await.unwrap();

        match cmd {
            ControllerCommand::BlockAllWorkers => {
                self.block_all_workers().await;
            }
            ControllerCommand::UnblockAllWorkers => {
                self.unblock_all_workers().await;
            }
            ControllerCommand::BlockingLockAcquire(key, sender, vc) => {
                self.blocking_lock_acquire(key, sender, vc).await;
            }
        }

        Ok(())
    }

    async fn block_all_workers(&mut self) {
        if self.blocking_state == BlockingState::Blocked {
            return;
        }
        info!("Blocking workers.");
        self.blocking_state = BlockingState::Blocked;
    }

    async fn unblock_all_workers(&mut self) {
        if self.blocking_state == BlockingState::Unblocked {
            return;
        }
        info!("Unblocking workers.");
    }

    async fn blocking_lock_acquire(&mut self, key: CacheKey, sender: SenderType, vc: VectorClock) {
        if self.blocking_state == BlockingState::Blocked {
            return;
        }
        info!("Blocking worker {:?} till VC {} to acquire lock on {:?}.", sender, vc, key);
    }
}