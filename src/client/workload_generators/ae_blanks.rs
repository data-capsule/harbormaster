use rand::{thread_rng, Rng};

use crate::proto::execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionPhase, ProtoTransactionResult};

use super::{Executor, PerWorkerWorkloadGenerator, RateControl, WorkloadUnit, WrapperMode};

pub struct BlankAEWorkloadGenerator { pub payload_size: usize, pub signature_interval: usize, pub last_signature_index: usize }

impl BlankAEWorkloadGenerator {
    pub fn new(payload_size: usize, signature_interval: usize) -> Self {
        Self { payload_size, signature_interval, last_signature_index: 0 }
    }
}

impl PerWorkerWorkloadGenerator for BlankAEWorkloadGenerator {
    fn next(&mut self) -> WorkloadUnit {
        // Sample 512 byte random payload
        let mut payload = vec![2u8; self.payload_size];
        {
            let mut rng = thread_rng();
            rng.fill(&mut payload[..]);
        }

        self.last_signature_index += 1;
        let must_sign = self.last_signature_index % self.signature_interval == 0;

        WorkloadUnit {
            tx: ProtoTransaction{
                on_receive: None,
                on_crash_commit: Some(ProtoTransactionPhase {
                    ops: vec![ProtoTransactionOp {
                        op_type: crate::proto::execution::ProtoTransactionOpType::Noop.into(),
                        operands: vec![payload],
                        // operands: vec![vec![2u8; 0]],
                    }; 1],
                }),
                on_byzantine_commit: None,
                is_reconfiguration: false,
                is_2pc: false,
            },
            executor: Executor::Leader,
            wrapper_mode: if must_sign {
                WrapperMode::AppendEntriesWithSignature
            } else {
                WrapperMode::AppendEntries
            },
            rate_control: RateControl::OpenLoop,
        }
    }
    
    fn check_result(&mut self, _result: &Option<ProtoTransactionResult>) -> bool {
        true
    }
}