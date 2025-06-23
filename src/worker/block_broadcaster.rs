use std::sync::Arc;

use indexmap::IndexMap;
use prost::Message;
use tokio::{sync::Mutex, sync::oneshot};
use crate::{config::AtomicConfig, crypto::CachedBlock, proto::consensus::{HalfSerializedBlock, ProtoAppendEntries, ProtoFork}, rpc::{client::PinnedClient, server::LatencyProfile, PinnedMessage, SenderType}, utils::channel::{Receiver, Sender}};


pub enum BroadcastMode {
    Star(String /* Name prefix to send to */),
    Gossip(Vec<String> /* Names to send to */),
    // RandomGossip(String) // TODO: Implement this
}



pub struct BlockBroadcaster {
    config: AtomicConfig,
    client: PinnedClient,

    broadcast_mode: BroadcastMode,
    forward_to_staging: bool,
    wait_for_signal: bool,

    block_rx: Receiver<oneshot::Receiver<CachedBlock>>,
    wait_rx: Option<Receiver<u64>>,
    staging_tx: Option<Sender<CachedBlock>>,

    block_buffer: IndexMap<u64, CachedBlock>,
    deliver_index: u64,

}

impl BlockBroadcaster {
    pub fn new(config: AtomicConfig, client: PinnedClient, broadcast_mode: BroadcastMode, forward_to_staging: bool, wait_for_signal: bool, block_rx: Receiver<oneshot::Receiver<CachedBlock>>, wait_rx: Option<Receiver<u64>>, staging_tx: Option<Sender<CachedBlock>>) -> Self {
        Self {
            config,
            client,
            broadcast_mode,
            forward_to_staging,
            wait_for_signal,
            block_rx,
            wait_rx,
            staging_tx,
            block_buffer: IndexMap::new(),
            deliver_index: 0,
        }
    }

    fn get_peers(&self) -> Vec<String> {
        match &self.broadcast_mode {
            BroadcastMode::Star(name_prefix) => {
                self.config.get().consensus_config.node_list
                .iter().filter(|name| name.starts_with(name_prefix))
                .map(|name| name.to_string())
                .collect()
            }
            BroadcastMode::Gossip(names) => names.clone(),
        }
    }

    fn get_success_threshold(&self) -> usize {
        match &self.broadcast_mode {
            BroadcastMode::Star(_) => self.config.get().consensus_config.node_list.len() / 2 + 1,
            BroadcastMode::Gossip(_) => 0,
        }
    }

    fn wrap_block_for_broadcast(&self, block: &CachedBlock) -> ProtoAppendEntries {
        ProtoAppendEntries {
            view: block.block.view,
            config_num: block.block.config_num,
            fork: Some(ProtoFork {
                serialized_blocks: vec![HalfSerializedBlock {
                    n: block.block.n,
                    serialized_body: block.block_ser.clone(),

                    // Unused fields
                    view: block.block.view,
                    view_is_stable: block.block.view_is_stable,
                    config_num: block.block.config_num,
                }]
            }),

            // Unused fields
            commit_index: 0,
            view_is_stable: true,
            is_backfill_response: false,
        }
    }

    pub async fn run(block_broadcaster: Arc<Mutex<BlockBroadcaster>>) {
        let mut block_broadcaster = block_broadcaster.lock().await;
        block_broadcaster.worker().await;
    }

    async fn worker(&mut self) {
        loop {
            if self.wait_for_signal {
                tokio::select! {
                    Some(block_rx) = self.block_rx.recv() => {
                        let block = block_rx.await.unwrap();
                        self.block_buffer.insert(block.block.n, block);
                    }
                    Some(idx) = self.wait_rx.as_ref().unwrap().recv() => {
                        if idx > self.deliver_index {
                            self.deliver_index = idx;
                        }
                    }
                }
            } else {
                tokio::select! {
                    Some(block_rx) = self.block_rx.recv() => {
                        let block = block_rx.await.unwrap();
                        self.block_buffer.insert(block.block.n, block);
                    }
                }
            }

            
            let peers = self.get_peers();
            let threshold = self.get_success_threshold();
            for (n, block) in self.block_buffer.iter() {
                if self.wait_for_signal {
                    if *n > self.deliver_index {
                        continue;
                    }
                }

                let ae = self.wrap_block_for_broadcast(block);
                let data = ae.encode_to_vec();

                let sz = data.len();
                let data = PinnedMessage::from(data, sz, SenderType::Anon);

                let _ = PinnedClient::broadcast(
                    &self.client,
                    &peers, &data, 
                    &mut LatencyProfile::new(),
                    threshold
                ).await;

                if self.forward_to_staging {
                    let _ = self.staging_tx.as_ref().unwrap().send(block.clone()).await;
                }
            }

            if self.wait_for_signal {
                self.block_buffer.retain(|n, _| *n > self.deliver_index);
            } else {
                self.block_buffer.clear();
            }
        }
        
    }
}