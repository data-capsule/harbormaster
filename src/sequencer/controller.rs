use std::sync::Arc;

use log::info;
use tokio::sync::Mutex;

use crate::{config::AtomicConfig, rpc::client::PinnedClient, utils::channel::Receiver};

pub enum ControllerCommand {
    BlockWorkers,
    UnblockWorkers,
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
            ControllerCommand::BlockWorkers => {
                self.block_workers().await;
            }
            ControllerCommand::UnblockWorkers => {
                self.unblock_workers().await;
            }
        }

        Ok(())
    }

    async fn block_workers(&mut self) {
        if self.blocking_state == BlockingState::Blocked {
            return;
        }
        info!("Blocking workers.");
        self.blocking_state = BlockingState::Blocked;
    }

    async fn unblock_workers(&mut self) {
        if self.blocking_state == BlockingState::Unblocked {
            return;
        }
        info!("Unblocking workers.");
    }
}