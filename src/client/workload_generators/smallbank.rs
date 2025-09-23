/*
Smallbank ported from H-store.

The DDL from the H-store is as follows:

```sql

CREATE TABLE ACCOUNTS (
    custid      BIGINT      NOT NULL,
    name        VARCHAR(64) NOT NULL,
    CONSTRAINT pk_accounts PRIMARY KEY (custid),
);
CREATE INDEX IDX_ACCOUNTS_NAME ON ACCOUNTS (name);    

CREATE TABLE SAVINGS (
    custid      BIGINT      NOT NULL,
    bal         FLOAT       NOT NULL,
    CONSTRAINT pk_savings PRIMARY KEY (custid),
    FOREIGN KEY (custid) REFERENCES ACCOUNTS (custid)
);

CREATE TABLE CHECKING (
    custid      BIGINT      NOT NULL,
    bal         FLOAT       NOT NULL,
    CONSTRAINT pk_checking PRIMARY KEY (custid),
    FOREIGN KEY (custid) REFERENCES ACCOUNTS (custid)
);

```

KV Store representation will look like this:

NAME:custid => name
SAVINGS:custid => bal
CHECKING:custid => bal
where bal is IEEE 754 encoded float.

There is no concept of an index in PSL. So we ignore that.

Transaction specs:
(Every transaction reads account names first to check for validity)
(The Smallbank KV store engine is supposed to not decrement if the balance is not enough, and quit the entire transaction on the first error)
(We don't support interactive transactions, all of the operations for a transaction have to sent upfront.
In case where an intermediate value needs to read and conditioned on, we have to use stored procedures.)

--- The following can be represented as READ/INCREMENT/DECREMENT operations:----
- Balance(custid)  // reads balances for both savings and checking
    [READ: NAME:custid, READ: SAVINGS:custid, READ: CHECKING:custid]

- DepositChecking(custid, amount)  // deposits money into checking
    [READ: NAME:custid, INCREMENT: CHECKING:custid => amount]

- TransactSavings(custid, amount)  // removes/adds money from/to savings
    [READ: NAME:custid, INCREMENT/DECREMENT: SAVINGS:custid => |amount|]

- SendPayment(sender_custid, receiver_custid, amount)  // sends money from sender to receiver from checking
    [READ: NAME:sender_custid, READ: NAME:receiver_custid,
    DECREMENT: CHECKING:sender_custid => amount,
    INCREMENT: CHECKING:receiver_custid => amount]

--- The following can't be represented as READ/INCREMENT/DECREMENT operations as they require conditionals on intermediate values ---
- WriteCheck(custid, amount)  // If sum of checking and savings is less than amount, decrease checking by amount + 1, else decrease checking by amount
    [READ: NAME:custid, STORED_PROCEDURE1: (custid, amount)]

- Amalgamate(custid1, custid2) // moves all money from custid1 and custid2 into custid2's checking
    [READ: NAME:custid1, READ: NAME:custid2, STORED_PROCEDURE2: (custid1, custid2)]


--- This is needed for the load phase ---
- CreateAccount(custid, name, checking_balance, savings_balance)
    [WRITE: NAME:custid => name, WRITE: CHECKING:custid => checking_balance, WRITE: SAVINGS:custid => savings_balance]

*/

use rand::{distr::{Uniform, weighted::WeightedIndex}};
use rand::prelude::*;
use rand_chacha::ChaCha20Rng;
use rand_distr::Normal;

use crate::{client::workload_generators::{Executor, PerWorkerWorkloadGenerator, RateControl, WorkloadUnit, WrapperMode}, config::Smallbank, proto::execution::{ProtoTransaction, ProtoTransactionResult}};

enum TxOpType {
    CreateAccount,
    Balance,
    DepositChecking,
    TransactSavings,
    WriteCheck,
    Amalgamate,
    SendPayment,
}

pub struct SmallbankGenerator {
    config: Smallbank,
    load_phase_cnt: usize,
    rng: ChaCha20Rng,

    client_idx: usize,
    total_clients: usize,
    load_phase_count: usize,

    tx_weights: [(TxOpType, u64); 6],
    tx_dist: WeightedIndex<u64>,

    custid_gen_dist: Uniform<u64>,
    balance_gen_dist: Normal<f64>,
}

impl SmallbankGenerator {
    pub fn new(config: &Smallbank, client_idx: usize, total_clients: usize) -> Self {
        let rng = ChaCha20Rng::from_os_rng();
        let tx_weights = [
            (TxOpType::Amalgamate, config.frequency_amalgamate),
            (TxOpType::WriteCheck, config.frequency_write_check),
            (TxOpType::DepositChecking, config.frequency_deposit_checking),
            (TxOpType::TransactSavings, config.frequency_transact_savings),
            (TxOpType::SendPayment, config.frequency_send_payment),
            (TxOpType::Balance, config.frequency_balance),
        ];
        let tx_dist = WeightedIndex::new(tx_weights.iter().map(|(_, weight)| weight)).unwrap();

        let custid_gen_dist = Uniform::new(0, config.num_accounts as u64).unwrap();


        let mean_balance = (config.min_balance + config.max_balance) / 2.0;
        // Set the max balance to 3 standard deviations from the mean.
        let std_dev = (config.max_balance - mean_balance) / 3.0;

        let balance_gen_dist = Normal::new(mean_balance, std_dev).unwrap();

        Self {
            config: config.clone(),
            rng,
            client_idx,
            total_clients,
            load_phase_count: 0,
            tx_weights,
            tx_dist,
            custid_gen_dist,
            balance_gen_dist,
            load_phase_cnt: 0,
        }
    }

    fn get_next_custid(&mut self) -> u64 {
        self.custid_gen_dist.sample(&mut self.rng)
    }

    fn get_next_balance(&mut self) -> f64 {
        self.balance_gen_dist.sample(&mut self.rng)
    }

    fn get_next_name(&mut self) -> String {
        format!("name{}", self.get_next_custid())
    }

    fn load_phase_next(&mut self) -> WorkloadUnit {
        let custid = self.get_next_custid();
        let name = self.get_next_name();
        let balance = self.get_next_balance();

        WorkloadUnit {
            tx: ProtoTransaction {
                on_receive: None,
                on_crash_commit: None,
                on_byzantine_commit: None,
                is_reconfiguration: false,
                is_2pc: false,
            },
            executor: Executor::Leader,
            wrapper_mode: WrapperMode::ClientRequest,
            rate_control: RateControl::CloseLoop,
        }
    }

    fn run_phase_next(&mut self) -> WorkloadUnit {
        self.load_phase_next()
    }
}

impl PerWorkerWorkloadGenerator for SmallbankGenerator {
    fn next(&mut self) -> WorkloadUnit {
        if self.config.load_phase && self.load_phase_cnt < self.config.num_accounts {
            return self.load_phase_next();
        } else {
            return self.run_phase_next();
        }
    }

    fn check_result(&mut self, _result: &Option<ProtoTransactionResult>) -> bool {
        true
    }
}



