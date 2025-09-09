use std::{collections::{HashMap, HashSet}, pin::Pin, sync::Arc, time::Duration};

use log::{debug, error, info, warn};
use tokio::sync::Mutex;

use crate::{config::AtomicConfig, proto::consensus::ProtoVectorClock, rpc::SenderType, sequencer::controller::ControllerCommand, utils::{channel::{Receiver, Sender}, timer::ResettableTimer}, worker::block_sequencer::VectorClock};

pub struct HeartbeatHandler {
    config: AtomicConfig,
    heartbeat_rx: Receiver<(ProtoVectorClock, SenderType)>,
    controller_tx: Sender<ControllerCommand>,
    heartbeat_vcs: HashMap<SenderType, VectorClock>,
    log_timer: Arc<Pin<Box<ResettableTimer>>>,
    already_blocked: bool,

    __num_workers: usize,
}

impl HeartbeatHandler {
    pub fn new(config: AtomicConfig, heartbeat_rx: Receiver<(ProtoVectorClock, SenderType)>, controller_tx: Sender<ControllerCommand>) -> Self {
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        let __num_workers = config.get().net_config.nodes.keys()
            .filter(|name| name.starts_with("node"))
            .collect::<HashSet<_>>().len();
        Self {
            config,
            heartbeat_rx,
            controller_tx,
            heartbeat_vcs: HashMap::new(),
            log_timer,
            already_blocked: false,

            __num_workers,
        }
    }

    pub async fn run(heartbeat_handler: Arc<Mutex<Self>>) {
        let mut heartbeat_handler = heartbeat_handler.lock().await;

        heartbeat_handler.log_timer.run().await;

        while let Ok(_) = heartbeat_handler.handle_inputs().await {
        }
    }

    async fn handle_inputs(&mut self) -> Result<(), ()> {
        tokio::select! {
            biased;
            Some((proto_heartbeat_vc, sender)) = self.heartbeat_rx.recv() => {
                debug!("Received heartbeat from worker: {:?} with vc: {:?}", sender, proto_heartbeat_vc);
                self.handle_heartbeat(proto_heartbeat_vc, sender).await;
                Ok(())
            }
            _ = self.log_timer.wait() => {
                self.log_stats().await;
                Ok(())
            }
        }
    }

    async fn handle_heartbeat(&mut self, proto_heartbeat_vc: ProtoVectorClock, sender: SenderType) {
        self.heartbeat_vcs.insert(sender, VectorClock::from(Some(proto_heartbeat_vc)));
        let unique_heartbeat_vcs = self.heartbeat_vcs.values().cloned().collect::<HashSet<_>>();
        error!("Heartbeat VCs: {:?}. Unique heartbeat VCs: {}", self.heartbeat_vcs, unique_heartbeat_vcs.len());
        if self.heartbeat_vcs.len() < self.__num_workers {
            return;
        }
        

        let diameter = self.get_snapshot_lattice_diameter();

        let blocking_criteria = diameter > self.config.get().consensus_config.max_audit_snapshots;
        let unblocking_criteria = unique_heartbeat_vcs.len() <= 1;
        

        if unblocking_criteria {
            // if self.already_blocked {
                error!("Unblocking all workers. Heartbeat VCs: {:?}, Diameter: {}, Unique heartbeat VCs: {}", self.heartbeat_vcs, diameter, unique_heartbeat_vcs.len());
                self.already_blocked = false;
                self.controller_tx.send(ControllerCommand::UnblockAllWorkers).await.unwrap();
            // }
        } else if blocking_criteria {
            // if !self.already_blocked {
                error!("Blocking all workers. Heartbeat VCs: {:?}, Diameter: {}, Unique heartbeat VCs: {}", self.heartbeat_vcs, diameter, unique_heartbeat_vcs.len());
                self.already_blocked = true;
                self.controller_tx.send(ControllerCommand::BlockAllWorkers).await.unwrap();
            // }
        }
        
    }

    fn get_snapshot_lattice_diameter(&self) -> usize {
        Self::_get_snapshot_lattice_diameter(&self.heartbeat_vcs.values().cloned().collect::<HashSet<_>>())
    }
    

    /// Base cases:
    /// - If the list is empty, return 0.
    /// - If the list has only one element, return 1.
    /// Recursion:
    /// 1. Find the list of glbs.
    /// 2. For each glb:
    ///     - Find all vcs > glb.
    ///     - Call _get_snapshot_lattice_diameter on the list of vcs.
    /// 3. Add up the results.
    fn _get_snapshot_lattice_diameter(list: &HashSet<VectorClock>) -> usize {
        if list.is_empty() {
            return 0;
        }

        if list.len() == 1 {
            return 1;
        }

        let glbs = Self::_get_snapshot_vc_glb(list);

        let mut diameter = 0;
        for glb in &glbs {
            let _diameter = Self::_get_snapshot_lattice_diameter(
                &list.iter()
                .filter_map(|vc| {
                    if vc > glb {
                        Some(vc.clone())
                    } else {
                        None
                    }
                }).collect());
            let _diameter = usize::max(1, _diameter);
            diameter += _diameter;
        }
        // error!("Glbs: {:?} Returning diameter: {}", glbs, diameter);

        diameter
    }

    fn _get_snapshot_vc_glb(list: &HashSet<VectorClock>) -> Vec<VectorClock> {
        if list.len() <= 1 {
            return list.iter().map(|vc| vc.clone()).collect();
        }

        list.iter().filter(|test_vc| {
            list.iter().all(|other_vc| {
                !(other_vc < *test_vc)
            })
        })
        .map(|vc| vc.clone())
        .collect()
    }

    async fn log_stats(&mut self) {
        info!("Heartbeat VCs: {:?}", self.heartbeat_vcs);
    }
}
