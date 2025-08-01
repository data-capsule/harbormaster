use std::io::Write as _;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use psl::worker::cache::{Cache, CacheKey};
// use psl::utils::channel::{make_channel, Receiver, Sender};
use flume::{bounded, unbounded, Receiver, Sender};
use tokio::task::JoinSet;
use tokio::runtime::{self, Runtime};
use core_affinity::{self, CoreId};

fn app_thread(cache: Arc<Cache>, request_rx: Receiver<(CacheKey, Vec<u8>, Sender<()>)>) {
    while let Ok((key, value, reply_tx)) = request_rx.recv() {
        cache.put_raw(key, value);
        let _ = reply_tx.send(());
    }
}

async fn client_thread(id: usize,num_requests: usize, request_tx: Sender<(CacheKey, Vec<u8>, Sender<()>)>) {
    let (reply_tx, reply_rx) = bounded(1);
    for j in 1..=num_requests {
        let key = b"key";
        let value = vec![0u8; 4096];
        let _ = request_tx.send((key.to_vec(), value, reply_tx.clone()));
        reply_rx.recv().unwrap();

        if j % 10_000 == 0 {
            println!("Client {} completed {} requests", id, j);
        }
    }
}

const NUM_REQUESTS: usize = 1_000_000;
const NUM_APP_THREADS: usize = 5;
const NUM_CLIENT_THREADS: usize = 10;

#[allow(dead_code)]
async fn all_in_one_runtime() {
    let (request_tx, request_rx) = unbounded();

    let cache = Arc::new(Cache::new());

    (0..NUM_APP_THREADS).for_each(|_| {
        let _cache = cache.clone();
        let _request_rx = request_rx.clone();
        tokio::spawn(async move {
            app_thread(_cache, _request_rx);
        });
    });

    let start = Instant::now();
    let mut join_set = JoinSet::new();
    (0..NUM_CLIENT_THREADS).for_each(|j| {
        let _request_tx = request_tx.clone();
        join_set.spawn(client_thread(j, NUM_REQUESTS, _request_tx));
    });

    while let Some(result) = join_set.join_next().await {
        result.unwrap();
    }

    let duration = start.elapsed();
    println!("Done in {:?}", duration);
}


fn create_app_runtime(core_ids: Vec<CoreId>) -> (Sender<(CacheKey, Vec<u8>, Sender<()>)>, Vec<Runtime>) {
    let (request_tx, request_rx) = bounded(1000);
    let cache = Arc::new(Cache::new_with_shards(core_ids.len()));
    let mut runtimes = Vec::new();

    for core_id in core_ids {
        let _cache = cache.clone();
        let _request_rx = request_rx.clone();
        
        let runtime = runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .on_thread_start(move || {
                let res = core_affinity::set_for_current(core_id);
                if res {
                    println!("Thread pinned to core {:?}", core_id);
                }else{
                    println!("Thread pinning to core {:?} failed", core_id);
                }
                std::io::stdout().flush()
                    .unwrap();
            })
            .build()
            .unwrap();

        runtime.spawn(async move {
            app_thread(_cache, _request_rx);
        });
        runtimes.push(runtime);

    }
    

    (request_tx, runtimes)

}

#[allow(dead_code)]
fn _main() {
    let i = Box::pin(AtomicUsize::new(0));

    println!("Core ids: {:?}", core_affinity::get_core_ids().unwrap());
    let core_ids = 
        Arc::new(Mutex::new(Box::pin(core_affinity::get_core_ids().unwrap())));

    let start_idx = 0;

    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(10)
        .on_thread_start(move || {
            let _cids = core_ids.clone();
            let lcores = _cids.lock().unwrap();
            let id = (start_idx + i.fetch_add(1, std::sync::atomic::Ordering::SeqCst)) % lcores.len();
            let res = core_affinity::set_for_current(lcores[id]);
    
            if res {
                println!("Thread pinned to core {:?}", lcores[id]);
            }else{
                println!("Thread pinning to core {:?} failed", lcores[id]);
            }
            std::io::stdout().flush()
                .unwrap();
        })
        .build()
        .unwrap();

    runtime.block_on(all_in_one_runtime());
}

fn main() {
    let core_ids = core_affinity::get_core_ids().unwrap();

    let (app_cores, client_cores) = core_ids.split_at(NUM_APP_THREADS);

    let client_core_len = client_cores.len();
    let client_cores = Arc::new(Mutex::new(client_cores.to_vec()));

    let (request_tx, runtimes) = create_app_runtime(app_cores.to_vec());


    let i = Box::pin(AtomicUsize::new(0));

    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(client_core_len)
        .on_thread_start(move || {
            let _cids = client_cores.clone();
            let lcores = _cids.lock().unwrap();
            let id = (i.fetch_add(1, std::sync::atomic::Ordering::SeqCst)) % lcores.len();
            let res = core_affinity::set_for_current(lcores[id]);
    
            if res {
                println!("Thread pinned to core {:?}", lcores[id]);
            }else{
                println!("Thread pinning to core {:?} failed", lcores[id]);
            }
            std::io::stdout().flush()
                .unwrap();
        })
        .build()
        .unwrap();

    runtime.block_on(async move {
        let start = Instant::now();
        let mut join_set = JoinSet::new();
        (0..NUM_CLIENT_THREADS).for_each(|j| {
            let _request_tx = request_tx.clone();
            join_set.spawn(client_thread(j, NUM_REQUESTS, _request_tx));
        });

        while let Some(result) = join_set.join_next().await {
            result.unwrap();
        }

        let duration = start.elapsed();
        println!("Done in {:?}", duration);
    });

    for runtime in runtimes {
        runtime.shutdown_background();
    }
    
}