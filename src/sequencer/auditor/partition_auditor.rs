use std::collections::VecDeque;

use hashbrown::{HashMap, HashSet};

use crate::{config::AtomicConfig, proto::consensus::ProtoReadSet, rpc::SenderType, sequencer::auditor::{BlockPartition, CachedWriteSet}, utils::{channel::Receiver, types::{CacheKey, CachedValue}}, worker::block_sequencer::VectorClock};


struct VersionedValue {
    versions: HashMap<VectorClock, CachedValue>,
}


/// Sum_sender(new.get(sender) - base.get(sender))
fn vc_update_cost(base: &VectorClock, new: &VectorClock) -> u64 {
    let keys = base.keys().chain(new.keys()).collect::<HashSet<_>>();
    keys.iter().map(|k| new.get(*k) - base.get(*k)).sum()
}

impl VersionedValue {
    pub fn new() -> Self {
        Self { versions: HashMap::new() }
    }

    pub fn insert(&mut self, vc: VectorClock, value: CachedValue) {
        self.versions.insert(vc, value);
    }

    pub fn get(&self, vc: &VectorClock) -> Option<&CachedValue> {
        self.versions.get(vc)
    }

    /// Returns the closest vc <= vc in the versions map.
    pub fn get_closest(&self, vc: &VectorClock) -> (VectorClock, Option<&CachedValue>) {
        let min_vc = self.versions.keys().filter(|base| *base <= vc)
            .min_by_key(|base| vc_update_cost(base, vc));
        if let Some(min_vc) = min_vc {
            (min_vc.clone(), self.versions.get(min_vc))
        } else {
            (VectorClock::new(), None)
        }
    }

    pub fn garbage_collect(&mut self, vc: &VectorClock) {
        self.versions.retain(|base, _| !(base <= vc));
    }
}

pub struct PartitionAuditor {
    config: AtomicConfig,
    partition_id: usize,
    thread_id: usize,
    block_rx: Receiver<BlockPartition>,
    store: HashMap<CacheKey, VersionedValue>,
    write_set_buffer: HashMap<String /* origin */, VecDeque<(u64 /* seq num */, CachedWriteSet)>>,
    read_set_buffer: HashMap<String /* origin */, VecDeque<(VectorClock, ProtoReadSet)>>,
    vc_available: VectorClock,


}

impl PartitionAuditor {
    pub fn new(config: AtomicConfig, partition_id: usize, thread_id: usize, block_rx: Receiver<BlockPartition>) -> Self {
        let worker_names = config.get().net_config.nodes.keys().filter(|name| name.starts_with("node")).map(|name| name.clone()).collect::<Vec<_>>();
        let vc_available = VectorClock::from_iter(worker_names.iter().map(|name| (SenderType::Auth(name.clone(), 0), 0)));
        let write_set_buffer = worker_names.iter().map(|name| (name.clone(), VecDeque::new())).collect();
        let read_set_buffer = worker_names.iter().map(|name| (name.clone(), VecDeque::new())).collect();
        Self { config, partition_id, thread_id, block_rx, store: HashMap::new(), write_set_buffer, read_set_buffer, vc_available }
    }

    pub async fn run(&mut self) {
        while let Some(block) = self.block_rx.recv().await {
            self.buffer_block(block);

            self.maybe_audit_blocks();
        }

    }

    fn buffer_block(&mut self, block: BlockPartition) {
        self.write_set_buffer.get_mut(&block.origin).unwrap().push_back((block.seq_num, block.write_set));

        if let Some(read_set) = block.read_set {
            self.read_set_buffer.get_mut(&block.origin).unwrap().push_back((block.read_vc, read_set));
        }
        self.vc_available.advance(SenderType::Auth(block.origin.clone(), 0), block.seq_num);
    }

    fn maybe_audit_blocks(&mut self) {
        // For each origin, check how many read sets can be audited.
        let mut to_audit = HashMap::new();
        for (origin, read_set_queue) in self.read_set_buffer.iter_mut() {
            while let Some((read_vc, read_set)) = read_set_queue.front() {
                let max_needed_vc = Self::get_max_needed_vc(read_set, read_vc);
                if max_needed_vc <= self.vc_available {
                    let (read_vc, read_set) = read_set_queue.pop_front().unwrap();
                    to_audit.insert(origin.clone(), (read_vc, read_set));
                } else {
                    break;
                }
            }
        }

        // Audit the read sets.
        for (origin, (read_vc, read_set)) in to_audit.iter() {
            self.audit_read_set(origin, read_vc, read_set);
        }
    }

    fn get_max_needed_vc(read_set: &ProtoReadSet, base_vc: &VectorClock) -> VectorClock {
        let mut max_needed_vc = base_vc.clone();
        for entry in &read_set.entries {
            if let Some(vc_delta) = &entry.vc_delta {
                for entry in &vc_delta.entries {
                    max_needed_vc.advance(SenderType::Auth(entry.sender.clone(), 0), entry.seq_num);
                }
            }
        
        }
        max_needed_vc
    }

    fn audit_read_set(&mut self, origin: &String, read_vc: &VectorClock, read_set: &ProtoReadSet) {
    }
}