mod partition_auditor;

use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use log::info;
use tokio::{sync::{Mutex, mpsc::{UnboundedReceiver, UnboundedSender}}, task::JoinSet};
use twox_hash::xxhash64;

use crate::{config::AtomicConfig, crypto::CachedBlock, proto::consensus::ProtoReadSet, utils::{channel::{Sender, make_channel}, timer::ResettableTimer, types::{CacheKey, CachedValue}}, worker::{block_sequencer::VectorClock, cache_manager::process_tx_op}};

use partition_auditor::PartitionAuditor;

const PARTITION_SEED: u64 = 42;
struct PartitionManager {
    num_partitions: usize,
    thread_per_partition: Vec<usize>,
    thread_round_robin_counter_per_partition: Vec<usize>,
}

impl PartitionManager {
    pub fn new(thread_per_partition: Vec<usize>) -> Self {
        let num_partitions = thread_per_partition.len();
        let thread_round_robin_counter_per_partition = vec![0; num_partitions];
        Self { num_partitions, thread_per_partition, thread_round_robin_counter_per_partition }
    }

    pub fn get_partition(&mut self, key: &CacheKey) -> usize {
        let hsh = xxhash64::Hasher::oneshot(PARTITION_SEED, key) as usize;
        let partition = hsh % self.num_partitions;
        partition
    }

    pub fn get_next_thread_id(&mut self, partition: usize) -> usize {
        let thread = self.thread_round_robin_counter_per_partition[partition];
        self.thread_round_robin_counter_per_partition[partition] = (thread + 1) % self.thread_per_partition[partition];
        thread
    }

}


struct WriteSet {
    write_ops: HashMap<usize /* index in original block */, (CacheKey, CachedValue)>,
}

impl WriteSet {
    pub fn new() -> Self {
        Self { write_ops: HashMap::new() }
    }

    pub fn insert(&mut self, index: usize, key: CacheKey, value: CachedValue) {
        self.write_ops.insert(index, (key, value));
    }
}

#[derive(Clone)]
struct CachedWriteSet (Arc<Pin<Box<WriteSet>>>);

impl CachedWriteSet {
    pub fn new(write_set: WriteSet) -> Self {
        Self(Arc::new(Box::pin(write_set)))
    }

}


struct BlockPartition {
    write_set: CachedWriteSet,
    read_set: Option<ProtoReadSet>,
    origin: String,
    seq_num: u64,
    read_vc: VectorClock,
}

pub struct Auditor {
    config: AtomicConfig,
    block_rx: UnboundedReceiver<CachedBlock>,
    reconfiguration_coordinator_tx: UnboundedSender<(String /* worker name */, u64 /* seq num */)>,

    log_timer: Arc<Pin<Box<ResettableTimer>>>,
    partition_manager: PartitionManager,

    partition_auditor_txs: HashMap<(usize /* partition */, usize /* thread */), Sender<BlockPartition>>,
    partition_auditor_handles: JoinSet<()>,

}

impl Auditor {
    pub fn new(
        config: AtomicConfig,
        block_rx: UnboundedReceiver<CachedBlock>,
        reconfiguration_coordinator_tx: UnboundedSender<(String /* worker name */, u64 /* seq num */)>,
        thread_per_partition: Vec<usize>,
    ) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        
        let _chan_depth = config.get().rpc_config.channel_depth as usize;
        let mut partition_auditor_handles = JoinSet::new();
        let partition_auditor_txs = thread_per_partition.iter().enumerate()
            .map(|(partition_id, &num_threads)| {
                (0..num_threads).map(|thread_id| {
                    let (tx, rx) = make_channel(_chan_depth);
                    let _config = config.clone();
                    partition_auditor_handles.spawn(async move {
                        let mut partition_auditor = PartitionAuditor::new(_config, partition_id, thread_id, rx);
                        partition_auditor.run().await;
                    });
                    ((partition_id, thread_id), tx)
                }).collect::<Vec<_>>()
            })
            .flatten()
            .collect();

        let partition_manager = PartitionManager::new(thread_per_partition);
        Self { config, block_rx, reconfiguration_coordinator_tx, log_timer, partition_manager, partition_auditor_txs, partition_auditor_handles }
    }

    pub async fn run(auditor: Arc<Mutex<Self>>) {
        let mut auditor = auditor.lock().await;
        auditor.log_timer.run().await;

        while let Ok(()) = auditor.worker().await {
        }
    }

    async fn worker(&mut self) -> Result<(), ()> {
        tokio::select! {
            _ = self.log_timer.wait() => {
                self.log_stats().await;
            },
            Some(block) = self.block_rx.recv() => {
                self.handle_block(block).await;
            },
        }

        Ok(())
    }

    async fn log_stats(&mut self) {
        info!("Auditor stats");
    }

    async fn handle_block(&mut self, block: CachedBlock) {
        // Step 1: Split the block into partitions.
        let num_partitions = self.partition_manager.num_partitions;
        let origin = block.block.origin.clone();
        let seq_num = block.block.n;
        let read_vc = VectorClock::from(block.block.vector_clock.clone());
        let read_set = block.block.read_set.clone();

        let mut partitions: Vec<(WriteSet, Vec<Option<ProtoReadSet>>)> = (0..num_partitions)
            .map(|i| (WriteSet::new(), (0..self.partition_manager.thread_per_partition[i]).map(|_| None).collect()))
            .collect();

        let mut write_op_index = 0usize;
        for tx in &block.block.tx_list {
            let Some(ops) = &tx.on_crash_commit else { continue };
            for op in &ops.ops {
                let Some((key, cached_value)) = process_tx_op(op) else { continue };
                let partition = self.partition_manager.get_partition(&key);
                partitions[partition].0.insert(write_op_index, key, cached_value);
                write_op_index += 1;
            }
        }

        if let Some(read_set) = read_set {
            for entry in read_set.entries {
                let partition = self.partition_manager.get_partition(&entry.key);
                let thread_id = self.partition_manager.get_next_thread_id(partition);
                let partition_read_set = partitions[partition].1[thread_id].get_or_insert_with(|| ProtoReadSet {
                    entries: Vec::new(),
                    merkle_root: Vec::new(),
                });
                partition_read_set.entries.push(entry);
            }
        }

        // Step 2: Send the partitions to the auditors.
        for (partition_id, (write_set, read_sets)) in partitions.into_iter().enumerate() {
            let write_set = CachedWriteSet::new(write_set);
            for (thread_id, read_set) in read_sets.into_iter().enumerate() {
                let partition_auditor_tx = self.partition_auditor_txs.get(&(partition_id, thread_id)).unwrap();
                partition_auditor_tx.send(BlockPartition {
                    write_set: write_set.clone(),
                    read_set,
                    origin: origin.clone(),
                    seq_num,
                    read_vc: read_vc.clone(),
                });
            }
        }
    }
}