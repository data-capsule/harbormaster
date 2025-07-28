// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use gluesql::core::ast_builder::num;
use log::{debug, error, info};
use psl::config::{self, Config, PSLWorkerConfig};
use psl::consensus::batch_proposal::TxWithAckChanTag;
use psl::utils::channel::{make_channel, Receiver, Sender};
use psl::{consensus, storage_server, worker};
use serde::Deserialize;
use tokio::{runtime, signal};
use std::process::exit;
use std::{env, fs, io, path, sync::{atomic::AtomicUsize, Arc, Mutex}};
use psl::consensus::engines::kvs::KVSAppEngine;
use std::io::Write;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// Fetch json config file from command line path.
/// Panic if not found or parsed properly.
fn process_args() -> (PSLWorkerConfig, usize) {
    macro_rules! usage_str {
        () => {
            "\x1b[31;1mUsage: {} path/to/config.json port_number\x1b[0m"
        };
    }

    let args: Vec<_> = env::args().collect();

    if args.len() != 3 {
        panic!(usage_str!(), args[0]);
    }

    
    let cfg_path = path::Path::new(args[1].as_str());
    if !cfg_path.exists() {
        panic!(usage_str!(), args[0]);
    }
    
    let cfg_contents = fs::read_to_string(cfg_path).expect("Invalid file path");
    
    let cfg = PSLWorkerConfig::deserialize(&cfg_contents);
    let port = args[2].parse::<usize>().unwrap();

    (cfg, port)
}

#[allow(unused_assignments)]
fn get_feature_set() -> (&'static str, &'static str) {
    let mut app = "";
    let mut protocol = "";

    #[cfg(feature = "app_logger")]{ app = "app_logger"; }
    #[cfg(feature = "app_kvs")]{ app = "app_kvs"; }
    #[cfg(feature = "app_sql")]{ app = "app_sql"; }

    #[cfg(feature = "lucky_raft")]{ protocol = "lucky_raft"; }
    #[cfg(feature = "signed_raft")]{ protocol = "signed_raft"; }
    #[cfg(feature = "chained_pbft")]{ protocol = "chained_pbft"; }
    #[cfg(feature = "pirateship")]{ protocol = "pirateship"; }
    #[cfg(feature = "engraft")]{ protocol = "engraft"; }

    (protocol, app)
}

macro_rules! handle_signal_till_end {
    ($node:expr) => {
        let mut handles = $node.run().await;
        match signal::ctrl_c().await {
            Ok(_) => {
                info!("Received SIGINT. Shutting down.");
                handles.abort_all();
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                info!("Force shutdown.");
                exit(0);
            },
            Err(e) => {
                error!("Signal: {:?}", e);
            }
        }
    
        while let Some(res) = handles.join_next().await {
            info!("Task completed with {:?}", res);
        }

    };
}

#[derive(Deserialize)]
struct Key {
    key: String,
}

#[derive(Deserialize)]
struct KeyValue {
    key: String,
    value: String,
}

#[get("/")]
async fn http_get(data: web::Data<Sender<TxWithAckChanTag>>, key: web::Json<Key>) -> impl Responder {

    HttpResponse::Ok().json(serde_json::json!({
        "message": format!("hello from {}", key.key),
    }))
}

#[post("/")]
async fn http_post(data: web::Data<Sender<TxWithAckChanTag>>, key_value: web::Json<KeyValue>) -> impl Responder {

    HttpResponse::Ok().json(serde_json::json!({
        "message": format!("hello from {} {}", key_value.key, key_value.value),
    }))
}


async fn run_main(cfg: PSLWorkerConfig, client_request_tx: Sender<TxWithAckChanTag>, client_request_rx: Receiver<TxWithAckChanTag>) -> Result<(), io::Error> {
    let mut node = worker::PSLWorker::<worker::app::KVSTask>::mew(cfg, client_request_tx.clone(), client_request_rx);
    handle_signal_till_end!(node);

    Ok(())
}

async fn run_actix_server(client_request_tx: Sender<TxWithAckChanTag>, port: usize) -> Result<(), io::Error> {
    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(client_request_tx.clone()))
            .service(http_get)
            .service(http_post)
    });

    server.bind(format!("0.0.0.0:{}", port)).unwrap().run().await?;

    Ok(())
}

const NUM_THREADS: usize = 32;

fn main() {
    log4rs::init_config(config::default_log4rs_config()).unwrap();

    let (cfg, port) = process_args();

    let (protocol, app) = get_feature_set();
    info!("Protocol: {}, App: {}", protocol, app);

    #[cfg(feature = "evil")]
    if cfg.evil_config.simulate_byzantine_behavior {
        warn!("Will simulate Byzantine behavior!");
    }

    let core_ids = 
        Arc::new(Mutex::new(Box::pin(core_affinity::get_core_ids().unwrap())));

    let mut num_threads = NUM_THREADS;
    {
        let _num_cores = core_ids.lock().unwrap().len();
        if _num_cores - 1 < num_threads {
            // Leave one core for the storage compaction thread.
            num_threads = _num_cores - 1;
        }
    }

    let (client_request_tx, client_request_rx) = make_channel(cfg.rpc_config.channel_depth as usize);

    let i = Box::pin(AtomicUsize::new(0));
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(num_threads / 2)
        .on_thread_start(move || {
            let _cids = core_ids.clone();
            let lcores = _cids.lock().unwrap();
            let id = (i.fetch_add(1, std::sync::atomic::Ordering::SeqCst)) % lcores.len();
            let res = core_affinity::set_for_current(lcores[id]);
    
            if res {
                debug!("Thread pinned to core {:?}", id);
            }else{
                debug!("Thread pinning to core {:?} failed", id);
            }
            std::io::stdout().flush()
                .unwrap();
        })
        .build()
        .unwrap();
    let _ = runtime.spawn(run_main(cfg.clone(), client_request_tx.clone(), client_request_rx));
    
    let frontend_runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(num_threads / 2) 
        .build()
        .unwrap();
    match frontend_runtime.block_on(run_actix_server(client_request_tx, port)) {
        Ok(_) => println!("Frontend server ran successfully."),
        Err(e) => eprintln!("Frontend server error: {:?}", e),
    };
}
