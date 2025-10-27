use std::{collections::HashSet, time::Instant};

use futures::{stream::FuturesUnordered, StreamExt as _};
use itertools::Itertools;
use log::{error, info, trace, warn};
use tokio::sync::oneshot;

use crate::{consensus::batch_proposal::MsgAckChanWithTag, proto::execution::{ProtoTransactionOp, ProtoTransactionOpResult, ProtoTransactionOpType}, utils::channel::Sender, worker::{app::{CacheConnector, ClientHandlerTask, UncommittedResultSet}, block_sequencer::VectorClock, cache_manager::CacheCommand, TxWithAckChanTag}};
use crate::utils::types::CacheKey;


pub struct LiteTask {
    cache_connector: CacheConnector,
    id: usize,
    total_work: usize,
}

enum Response {
    Invalid(MsgAckChanWithTag),
    Receipt(MsgAckChanWithTag, Vec<ProtoTransactionOpResult>, Option<u64>),
}

impl ClientHandlerTask for LiteTask {
    fn new(cache_connector: CacheConnector, id: usize) -> Self {
        Self {
            cache_connector,
            id,
            total_work: 0,
        }
    }

    fn get_locked_keys(&self) -> Vec<CacheKey> {
        vec![]
    }

    fn get_cache_connector(&self) -> &CacheConnector {
        &self.cache_connector
    }

    fn get_total_work(&self) -> usize {
        self.total_work
    }

    fn get_id(&self) -> usize {
        self.id
    }

    async fn on_client_request(&mut self, requests: Vec<TxWithAckChanTag>, reply_handler_tx: &Sender<UncommittedResultSet>) -> anyhow::Result<()> {
        let mut response_vec = Vec::new();

        let mut tx_phases_resp = Vec::new();

        for request in requests {

            let req = &request.0;
            let resp = &request.1;
            self.total_work += 1;
            
            
            if req.is_none() {
                response_vec.push(Response::Invalid(resp.clone()));
                continue;
            }
    
    
            let req = req.as_ref().unwrap();
            if req.on_receive.is_none() {
    
                // For PSL, all transactions must be on_receive.
                // on_crash_commit and on_byz_commit are meaningless.
                response_vec.push(Response::Invalid(resp.clone()));
                continue;
            }
    
            let on_receive = req.on_receive.as_ref().unwrap();

            tx_phases_resp.push((on_receive.clone(), resp.clone()));
        }


        let start_time = Instant::now();

        let mut all_reads = true;

        let mut locked_keys = Vec::new();

        for (on_receive, resp) in tx_phases_resp {
    
            if let std::result::Result::Ok((results, seq_num, atleast_one_write, _locked_keys)) = self.execute_ops(on_receive.ops.as_ref()).await {
                locked_keys.extend(_locked_keys);
                
                if atleast_one_write {
                    all_reads = false;
                }
                response_vec.push(Response::Receipt(resp, results, seq_num));
            } else {
                response_vec.push(Response::Invalid(resp));
            }
    
        }

        trace!("Execute ops time: {:?}", start_time.elapsed());


        // Some checks.
        if all_reads {
            assert!(locked_keys.len() == 0);
        }

        // if locked_keys.len() > 1, there is a chance of deadlock. And we don't do deadlock detection.
        //
        // This is not strict and conservative 2PL anymore.

        // Group commit. Supposed to improve throughput.
        locked_keys.reverse();
        let start_time = Instant::now();
        let vc = self.cache_connector.dispatch_commit_request(locked_keys.len() > 0).await;
        trace!("Committed with VC: {} Locked keys: {:?}", vc,
            locked_keys.iter().map(|key| String::from_utf8(key.clone()).unwrap_or(hex::encode(key.clone()))).collect::<Vec<_>>());

        // let vc = if all_reads {
        //     VectorClock::new() // Doesn't matter. If all reads, there are no locked keys.
        // } else {
        //     vc
        // };
        self.cache_connector.dispatch_unlock_request(locked_keys.iter().map(|key| (key.clone(), vc.clone())).collect()).await;
        trace!("Commit and unlock time: {:?}", start_time.elapsed());

        let start_time = Instant::now();
        for response in response_vec {
            match response {
                Response::Invalid(resp) => {
                    self.reply_invalid(&resp, reply_handler_tx).await;
                }
                Response::Receipt(resp, results, seq_num) => {
                    self.reply_receipt(&resp, results, seq_num, reply_handler_tx).await;
                }
            }
        }
        trace!("Reply time: {:?}", start_time.elapsed());

        Ok(())

    }
}

impl LiteTask {
    async fn execute_ops(&mut self, ops: &Vec<ProtoTransactionOp>) -> Result<(Vec<ProtoTransactionOpResult>, Option<u64>, bool, Vec<CacheKey> /* locked keys */), anyhow::Error> {
        let mut atleast_one_write = false;
        let mut last_write_index = 0;
        let mut highest_committed_block_seq_num_needed = 0;
        let mut block_seq_num_rx_vec = FuturesUnordered::new();
        let mut results = Vec::new();
        let mut locked_keys = Vec::new();
        let mut is_aborted = false;

        for (i, op) in ops.iter().enumerate() {
            let op_type = op.op_type();

            match op_type {
                _ => {}
            }
        }

        for op in ops {
            if is_aborted {
                // Preserve invariant: #ops == #results.
                results.push(ProtoTransactionOpResult {
                    success: false,
                    values: vec![],
                });
                continue;
            }
            let op_type = op.op_type();

            match op_type {
                ProtoTransactionOpType::Write => {
                    let key = op.operands[0].clone();
                    let value = op.operands[1].clone();
                    let res = self.cache_connector.dispatch_write_request(key, value).await;
                    if let std::result::Result::Err(e) = res {
                        return Err(e.into());
                    }

                    let (_, block_seq_num_rx) = res.unwrap();
                    if let Some(block_seq_num_rx) = block_seq_num_rx {
                        block_seq_num_rx_vec.push(block_seq_num_rx);
                    }
                    results.push(ProtoTransactionOpResult {
                        success: true,
                        values: vec![],
                    });
                },
                ProtoTransactionOpType::Read => {
                    let key = op.operands[0].clone();
                    let (res, block_seq_num_rx) = self.cache_connector.dispatch_read_request(key).await;
                    match res {
                        std::result::Result::Ok(_) => {
                            results.push(ProtoTransactionOpResult {
                                success: true,
                                values: vec![],
                            });
                        }
                        std::result::Result::Err(_e) => {
                            results.push(ProtoTransactionOpResult {
                                success: false,
                                values: vec![],
                            });
                        }
                    };
                    if let Some(block_seq_num_rx) = block_seq_num_rx {
                        block_seq_num_rx_vec.push(block_seq_num_rx);
                    }
                },

                _ => {}
            }
            
        }

        if atleast_one_write {

            // Find the highest block seq num needed.
            while let Some(seq_num) = block_seq_num_rx_vec.next().await {
                if seq_num.is_err() {
                    continue;
                }

                let seq_num = seq_num.unwrap();
                highest_committed_block_seq_num_needed = std::cmp::max(highest_committed_block_seq_num_needed, seq_num);
            }

            return Ok((results, Some(highest_committed_block_seq_num_needed), atleast_one_write, locked_keys));
        }

        Ok((results, None, atleast_one_write, locked_keys))
    }

    async fn reply_receipt(&self, resp: &MsgAckChanWithTag, results: Vec<ProtoTransactionOpResult>, seq_num: Option<u64>, reply_handler_tx: &Sender<UncommittedResultSet>) -> anyhow::Result<()> {
        reply_handler_tx.send((results, resp.clone(), seq_num)).await;
        Ok(())
    }

    async fn reply_invalid(&self, resp: &MsgAckChanWithTag, reply_handler_tx: &Sender<UncommittedResultSet>) -> anyhow::Result<()> {
        // For now, just send a blank result.
        
        reply_handler_tx.send((vec![], resp.clone(), None)).await;
        Ok(())
    }

}