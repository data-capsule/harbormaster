use std::{collections::VecDeque, time::{Duration, Instant}};

use hashbrown::{HashMap, HashSet};
use log::{info, trace, warn};
use tokio::sync::mpsc::UnboundedSender;

use crate::{config::AtomicConfig, proto::consensus::ProtoReadSet, rpc::SenderType, sequencer::auditor::{AuditorLogStats, BlockPartition, CachedWriteSet}, utils::{channel::Receiver, types::{CacheKey, CachedValue}}, worker::block_sequencer::{VectorClock, cached_value_to_val_hash}};


struct VersionedValue {
    versions: HashMap<VectorClock, CachedValue>,
}

macro_rules! merge_values {
    ($old_value:expr, $new_value:expr) => {
        match $old_value {
            CachedValue::DWW(dww_val) => {
                dww_val.merge_cached($new_value.get_dww().unwrap().clone());
            },
            CachedValue::PNCounter(pn_counter_val) => {
                pn_counter_val.merge($new_value.get_pn_counter().unwrap().clone());
            }
        }
    }
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

    pub fn get_lower_bound_merged(&self, vc: &VectorClock) -> Option<CachedValue> {
        let vals = self.versions.iter().filter(|(base, val)| *base <= vc)
            .map(|(_, val)| val.clone())
            .collect::<Vec<_>>();

        if vals.is_empty() {
            return None;
        }

        let mut merged_value = vals[0].clone();
        for val in vals.iter().skip(1) {
            merge_values!(&mut merged_value, val);
        }
        Some(merged_value)

    }

    pub fn garbage_collect(&mut self, vc: &VectorClock) {
        let remove_vcs = self.versions.iter()
            .filter(|(base, _)| *base <= vc)
            .map(|(base, _)| base.clone())
            .collect::<Vec<_>>();
        let replacement_value = self.get_lower_bound_merged(vc);
        for base in remove_vcs {
            self.versions.remove(&base);
        }
        if replacement_value.is_some() {
            self.versions.insert(vc.clone(), replacement_value.unwrap());
        }
    }
}

pub struct PartitionAuditor {
    config: AtomicConfig,
    partition_id: usize,
    thread_id: usize,
    block_rx: tokio::sync::mpsc::UnboundedReceiver<BlockPartition>,
    store: HashMap<CacheKey, VersionedValue>,
    write_set_buffer: HashMap<String /* origin */, VecDeque<(u64 /* seq num */, CachedWriteSet)>>,

    /// Verifying reads also requires the write set for adding in extra writes from that block.
    read_set_buffer: HashMap<String /* origin */, VecDeque<BlockPartition>>,
    vc_available: VectorClock,
    vc_applied: VectorClock,

    __last_logged_time: Instant,
    __correct_reads: usize,
    __incorrect_reads: usize,

    log_tx: UnboundedSender<AuditorLogStats>,
    last_read_vc: HashMap<String /* origin */, VectorClock>,
}

impl PartitionAuditor {
    pub fn new(config: AtomicConfig, partition_id: usize, thread_id: usize, block_rx: tokio::sync::mpsc::UnboundedReceiver<BlockPartition>, log_tx: UnboundedSender<AuditorLogStats>) -> Self {
        let worker_names = config.get().net_config.nodes.keys().filter(|name| name.starts_with("node")).map(|name| name.clone()).collect::<Vec<_>>();
        let vc_available = VectorClock::from_iter(worker_names.iter().map(|name| (SenderType::Auth(name.clone(), 0), 0)));
        let vc_applied = VectorClock::from_iter(worker_names.iter().map(|name| (SenderType::Auth(name.clone(), 0), 0)));
        let write_set_buffer = worker_names.iter().map(|name| (name.clone(), VecDeque::new())).collect();
        let read_set_buffer = worker_names.iter().map(|name| (name.clone(), VecDeque::new())).collect();
        let __last_logged_time = Instant::now();
        let last_read_vc = worker_names.iter().map(|name| (name.clone(), VectorClock::new())).collect();
        Self { 
            config, partition_id, thread_id, block_rx, store: HashMap::new(),
            write_set_buffer, read_set_buffer, vc_available, vc_applied, __last_logged_time,
            __correct_reads: 0, __incorrect_reads: 0,
            log_tx,
            last_read_vc,
        }
    }

    pub async fn run(&mut self) {
        info!("Partition auditor for partition: {} thread: {} running", self.partition_id, self.thread_id);

        loop {
            if self.block_rx.len() > 1 {
                let mut new_blocks = Vec::with_capacity(std::cmp::min(self.block_rx.len(), 100));
                self.block_rx.recv_many(&mut new_blocks, self.block_rx.len()).await;
                if new_blocks.is_empty() {
                    break;
                }
    
                for block in new_blocks {
                    self.buffer_block(block);
                }
            } else {
                let block = self.block_rx.recv().await;
                if block.is_none() {
                    break;
                }
                let block = block.unwrap();
                self.buffer_block(block);
            }

            self.maybe_audit_blocks();

            if self.__last_logged_time.elapsed() > Duration::from_millis(self.config.get().app_config.logger_stats_report_ms) {
                self.log_stats();
                self.__last_logged_time = Instant::now();
            }

            self.maybe_garbage_collect();
        }

        info!("Partition auditor for partition: {} thread: {} exiting", self.partition_id, self.thread_id);

    }

    fn buffer_block(&mut self, block: BlockPartition) {
        self.write_set_buffer.get_mut(&block.origin).unwrap().push_back((block.seq_num, block.write_set.clone()));

        self.vc_available.advance(SenderType::Auth(block.origin.clone(), 0), block.seq_num);
        if let Some(_) = &block.read_set {
            info!("Auditor: {},{} Buffering read set for origin: {} seq num: {} read vc: {:?}", self.partition_id, self.thread_id, block.origin, block.seq_num, block.read_vc);
            self.read_set_buffer.get_mut(&block.origin).unwrap().push_back(block);
        } else {
            trace!("Received block from origin: {} but read set is None", block.origin);
        }
    }

    fn maybe_audit_blocks(&mut self) {
        // For each origin, check how many read sets can be audited.
        let mut to_audit = Vec::new();
        for (origin, read_set_queue) in self.read_set_buffer.iter_mut() {
            while let Some(block) = read_set_queue.front() {
                let read_vc = &block.read_vc;
                let read_set = block.read_set.as_ref().unwrap();
                let max_needed_vc = Self::get_max_needed_vc(read_set, read_vc);
                trace!("max_needed_vc: {:?} vc_available: {:?}", max_needed_vc, self.vc_available);
                if max_needed_vc <= self.vc_available {
                    let block = read_set_queue.pop_front().unwrap();
                    let cost = vc_update_cost(&self.vc_applied, &max_needed_vc);

                    to_audit.push((origin.clone(), block, cost));
                } else {
                    break;
                }
            }
        }

        to_audit.sort_by_key(|(_, _, cost)| *cost);

        // Audit the read sets.
        for (origin, block, _cost) in to_audit.iter(){
            let start_time = Instant::now();
            self.audit_read_set(origin, block);
            let end_time = Instant::now();
            let duration = end_time - start_time;
            info!("Auditor: {},{} Audit read set time: {:?} origin: {}", self.partition_id, self.thread_id, duration, origin);
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

    fn apply_updates_upto_vc(&mut self, vc: &VectorClock) {
        let mut total_blocks_applied = 0;
        for (origin, write_set_queue) in self.write_set_buffer.iter_mut() {
            while let Some((seq_num, _)) = write_set_queue.front() {
                let seq_num = *seq_num;
                if seq_num > vc.get(&SenderType::Auth(origin.clone(), 0)) {
                    break;
                }
                info!("Auditor: {},{} Trying to apply block origin: {} seq num: {}, vc: {:?}", self.partition_id, self.thread_id, origin, seq_num, vc);

                let (_, write_set) = write_set_queue.pop_front().unwrap();
                let mut _vc = self.vc_applied.clone();
                _vc.advance(SenderType::Auth(origin.clone(), 0), seq_num);

                for (_, (key, _value)) in write_set.write_ops.iter() {
                    let mut value = _value.clone();
                    let entry = self.store.entry(key.clone()).or_insert(VersionedValue::new());
                    let curr_value = entry.get_lower_bound_merged(&_vc);
                    if curr_value.is_some() {
                        merge_values!(&mut value, curr_value.unwrap());
                    }
                    entry.insert(_vc.clone(), value);
                }
                total_blocks_applied += 1;
            }
        }
        info!("Auditor: {},{} Applied {} blocks", self.partition_id, self.thread_id, total_blocks_applied);
    }

    fn audit_read_set(&mut self, origin: &String, block: &BlockPartition) {
        let read_set = block.read_set.as_ref().unwrap();
        
        // Step 1: Apply the updates upto max_needed_vc to self.store.
        let max_needed_vc = Self::get_max_needed_vc(read_set, &block.read_vc);
        trace!("Auditing read set for origin: {} read vc: {:?} seq num: {} size: {}", origin, block.read_vc, block.seq_num, read_set.entries.len());
        
        let start_time = Instant::now();
        self.apply_updates_upto_vc(&max_needed_vc);
        let end_time = Instant::now();
        let duration = end_time - start_time;
        info!("Auditor: {},{} Apply updates upto vc time: {:?} origin: {}", self.partition_id, self.thread_id, duration, origin);
        let start_time = Instant::now();
        self.maybe_garbage_collect();
        let end_time = Instant::now();
        let duration = end_time - start_time;
        info!("Auditor: {},{} Garbage collect time: {:?} origin: {}", self.partition_id, self.thread_id, duration, origin);

        // Step 2: For every read in the read set, derive the correct value.
        let mut local_cache = HashMap::new();
        let mut write_op_index = None; // Nothing is applied yet.
        let mut curr_vc = block.read_vc.clone();
        for entry in &read_set.entries {
            // Advance the vc to the read entry's vc.
            let start_time = Instant::now();
            if let Some(vc_delta) = &entry.vc_delta {
                for entry in &vc_delta.entries {
                    curr_vc.advance(SenderType::Auth(entry.sender.clone(), 0), entry.seq_num);
                }
            }

            // Apply the writes from the write set to the local cache.
            let write_ops = block.write_set.write_ops.iter()
                .filter(|(&index, _)| (write_op_index.is_none() || write_op_index.unwrap() <= index) && index <= entry.after_write_op_index as usize)
                .map(|(_, value)| (&value.0, &value.1))
                .collect::<Vec<_>>();

            // Apply the writes to the local cache.
            for write_op in write_ops {
                let entry = local_cache.entry(write_op.0).or_insert(write_op.1.clone());
                merge_values!(entry, write_op.1);
            }

            // Update the write op index.
            write_op_index = Some(entry.after_write_op_index as usize);

            
            // Correct value is the merge of local cache and self.store.
            let local_value = local_cache.get(&entry.key);
            let store_value = self.store.get(&entry.key);
            
            let correct_value = Self::merge_values(&local_value, &store_value, &curr_vc);
            
            let end_time = Instant::now();
            let duration = end_time - start_time;
            trace!("Auditor: {},{} Apply writes to local cache time: {:?} origin: {} key: {}",
            self.partition_id, self.thread_id, duration, origin, String::from_utf8(entry.key.clone()).unwrap_or(hex::encode(entry.key.clone())));


            // Step 3: Is the correct value the same as the read value?
            self.check_read_value(&correct_value, &entry.value_hash, &entry.key, &curr_vc);
        }

        // Step 4: Update the last read vc.
        self.last_read_vc.insert(origin.clone(), curr_vc);
    }

    fn merge_values(local_value: &Option<&CachedValue>, store_value: &Option<&VersionedValue>, read_vc: &VectorClock) -> Option<CachedValue> {
        if store_value.is_none() {
            return local_value.cloned();
        }

        let extracted_value = Self::extract_value(store_value.unwrap(), read_vc);

        if local_value.is_none() {
            return extracted_value;
        }

        if extracted_value.is_none() {
            return local_value.cloned();
        }

        let local_value = local_value.unwrap();
        let mut extracted_value = extracted_value.unwrap();

        merge_values!(&mut extracted_value, &local_value);
        Some(extracted_value)
    }

    fn extract_value(store_value: &VersionedValue, read_vc: &VectorClock) -> Option<CachedValue> {
        store_value.get_lower_bound_merged(read_vc)
    }

    fn check_read_value(&mut self, correct_value: &Option<CachedValue>, read_value: &Vec<u8>, key: &CacheKey, read_vc: &VectorClock) {
        let correct_value_hash = cached_value_to_val_hash(correct_value);

        let key_str = String::from_utf8(key.clone()).unwrap_or(hex::encode(key));
        let correct_value_hex_str = &hex::encode(correct_value_hash.clone());
        let value_hex_str = &hex::encode(read_value);

        if correct_value_hash.eq(read_value) {
            trace!("✅ Read verification passed for key: {} correct_value_hash: {} value_hash: {} read_vc: {}",
                key_str, correct_value_hex_str, value_hex_str, read_vc);
            
            self.__correct_reads += 1;
        } else {
            warn!("❌ Read verification failed for key: {} correct_value_hash: {} value_hash: {} read_vc: {}",
                key_str, correct_value_hex_str, value_hex_str, read_vc);

            self.__incorrect_reads += 1;
        }
    }

    fn log_stats(&mut self) {
        let log_stats = AuditorLogStats {
            total_correct_reads: self.__correct_reads,
            total_incorrect_reads: self.__incorrect_reads,
            partition_id: self.partition_id,
            thread_id: self.thread_id,
        };
        self.log_tx.send(log_stats).unwrap();
    }

    fn maybe_garbage_collect(&mut self) {
        let mut min_seq_nums = self.last_read_vc.keys() // all worker names
            .map(|name| (SenderType::Auth(name.clone(), 0), u64::MAX))
            .collect::<HashMap<_, _>>();

        for (_, last_read_vc) in self.last_read_vc.iter() {
            for (sender, seq_num) in last_read_vc.iter() {
                if *seq_num < *min_seq_nums.get(sender).unwrap() {
                    min_seq_nums.insert(sender.clone(), *seq_num);
                }
            }
        }

        let min_seq_num = VectorClock::from_iter(min_seq_nums.into_iter());

        for (_key, value) in self.store.iter_mut() {
            // for (_, vc) in self.last_read_vc.iter() {
                value.garbage_collect(&min_seq_num);
            // }
            // let user1 = _key.starts_with(String::from("user1000001:field0").as_bytes());
            // if user1 {
            //     let key_str = String::from_utf8(_key.clone()).unwrap_or(hex::encode(_key));
            //     info!("Garbage collected key: {} # versions: {}", key_str, value.versions.len());
            // }
        }

    }
}