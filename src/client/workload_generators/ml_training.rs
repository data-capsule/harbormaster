use log::info;
use std::io::{BufRead, BufReader};

use crate::proto::execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionPhase, ProtoTransactionResult};

use super::{Executor, PerWorkerWorkloadGenerator, RateControl, WorkloadUnit, WrapperMode};

pub struct MLTrainingWorkloadGenerator { 
    pub file_path: String
}

impl MLTrainingWorkloadGenerator {
    pub fn new(file_path: String) -> Self {
        Self { file_path }
    }
}

impl PerWorkerWorkloadGenerator for MLTrainingWorkloadGenerator {
    fn next(&mut self) -> WorkloadUnit {
        // Read training data, line by line.
        // Read the file line by line and use the next line as the payload.
        // If the file cannot be read or is empty, use a default payload.
        let mut payload = Vec::new();
        if let Ok(file) = std::fs::File::open(&self.file_path) {
            let mut reader = BufReader::new(file);
            let mut line = String::new();
            // Read the next line (for simplicity, just the first line each time)
            while let Ok(n) = reader.read_line(&mut line) {
                if n > 0 {
                    payload.push(line.trim_end().as_bytes().to_vec());
                } else {
                    break;
                }

                line.clear();
            }
        }

        info!("Payload size: {}", payload.len());

        WorkloadUnit {
            tx: ProtoTransaction{
                on_receive: None,
                on_crash_commit: Some(ProtoTransactionPhase {
                    ops: vec![ProtoTransactionOp {
                        op_type: crate::proto::execution::ProtoTransactionOpType::Custom.into(),
                        operands: payload,
                        // operands: vec![vec![2u8; 0]],
                    }; 1],
                }),
                on_byzantine_commit: None,
                is_reconfiguration: false,
                is_2pc: false,
            },
            executor: Executor::Any,
            wrapper_mode: WrapperMode::ClientRequest,
            rate_control: RateControl::CloseLoop,
        }
    }
    
    fn check_result(&mut self, _result: &Option<ProtoTransactionResult>) -> bool {
        true
    }
}