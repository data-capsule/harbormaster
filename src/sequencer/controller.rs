use std::{collections::{HashMap, VecDeque}, pin::Pin, sync::Arc, time::Duration};

use log::info;
use tokio::sync::Mutex;

use crate::{config::AtomicConfig, crypto::{CachedBlock, HashType}, rpc::{client::PinnedClient, SenderType}, utils::{channel::Receiver, timer::ResettableTimer}, worker::block_sequencer::VectorClock};

pub struct Controller {
    config: AtomicConfig,
    client: PinnedClient, // TODO: Put this in a different thread.

    block_rx: Receiver<(SenderType, CachedBlock)>,
    log_timer: Arc<Pin<Box<ResettableTimer>>>,


    audit_buffer: VecDeque<BlockStats>, // TODO: Put this in a different thread.
    uncommitted_buffer: HashMap<(String /* origin */, u64 /* seq_num */), (BlockStats, HashType, usize /* votes */)>,

    commit_indices: HashMap<String /* origin */, u64 /* seq_num */>,
}

#[derive(Clone)]
struct BlockStats {
    origin: String,
    read_vc: VectorClock,
    seq_num: u64,

}

impl Controller {
    pub fn new(config: AtomicConfig, client: PinnedClient, block_rx: Receiver<(SenderType, CachedBlock)>) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        Self {
            config, client, block_rx, log_timer,
            audit_buffer: VecDeque::new(),
            uncommitted_buffer: HashMap::new(),
            commit_indices: HashMap::new(),
        }
    }

    pub async fn run(controller:Arc<Mutex<Self>>) {
        let mut controller = controller.lock().await;
        controller.log_timer.run().await;
        

        while let Ok(()) = controller.worker().await {

        }
    }

    async fn worker(&mut self) -> Result<(), anyhow::Error> {
        tokio::select! {
            _ = self.log_timer.wait() => {
                self.log_stats().await;
            }
            Some((_sender, block)) = self.block_rx.recv() => {
                self.handle_block(block, _sender).await;
            }
        }

        Ok(())
    }


    async fn handle_block(&mut self, block: CachedBlock, _: SenderType) {
        // The sender is going to be unused.
        let origin = block.block.origin.clone();

        let read_vc = VectorClock::from(block.block.vector_clock.clone());

        let seq_num = block.block.n;

        let block_stats = BlockStats {
            origin: origin.clone(),
            read_vc,
            seq_num,
        };

        if let Some(n) = self.commit_indices.get(&origin) {
            if *n >= seq_num {
                // Just drop the block.
                return;
            }
        }

        let commit_threshold = self.get_commit_threshold();
        
        
        let entry = self.uncommitted_buffer
            .entry((origin.clone(), seq_num))
            .or_insert((block_stats, block.block_hash.clone(), 0));

        if entry.1 != block.block_hash {
            self.throw_error().await;
            return;
        }

        entry.2 += 1;

        if entry.2 >= commit_threshold {
            self.commit_indices.insert(origin.clone(), seq_num);
            let val = self.uncommitted_buffer.remove(&(origin.clone(), seq_num)).unwrap();
            self.audit_buffer.push_back(val.0);
        }

    }

    fn get_commit_threshold(&self) -> usize {
        let n = self.config.get().consensus_config.node_list.len();
        if n == 0 {
            return 0;
        }

        n / 2 + 1
    }

    async fn log_stats(&mut self) {
        info!("Uncommitted buffer size: {}", self.uncommitted_buffer.len());
        info!("Commit indices: {:?}", self.commit_indices);
        info!("Audit buffer size: {}", self.audit_buffer.len());
    }

    async fn throw_error(&mut self) {
        unimplemented!()
    }
}