use psl::{
    config::{ClientConfig, default_log4rs_config},
    crypto::KeyStore,
    proto::{
        consensus::ProtoReconfigurationSignal,
        rpc::ProtoPayload,
    },
    rpc::{PinnedMessage, SenderType, client::{Client, PinnedClient}},
};
use prost::Message as _;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

fn process_args() -> (ClientConfig, Vec<String>) {
    macro_rules! usage_str {
        () => {
            "\x1b[31;1mUsage: {} path/to/client_config.json storage_server1 storage_server2 ...\x1b[0m"
        };
    }

    let args: Vec<_> = std::env::args().collect();

    if args.len() < 3 {
        panic!(usage_str!(), args[0]);
    }

    let cfg_path = std::path::Path::new(args[1].as_str());
    if !cfg_path.exists() {
        panic!(usage_str!(), args[0]);
    }

    let cfg_contents = std::fs::read_to_string(cfg_path).expect("Invalid file path");
    let config = ClientConfig::deserialize(&cfg_contents);

    let new_storage_servers: Vec<String> = args[2..].iter().cloned().collect();

    (config, new_storage_servers)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    log4rs::init_config(default_log4rs_config()).unwrap();
    let (config, new_storage_servers) = process_args();

    let mut keys = KeyStore::empty();
    keys.priv_key = KeyStore::get_privkeys(&config.rpc_config.signing_priv_key_path);

    let client = Client::new(&config.fill_missing(), &keys, config.full_duplex, 0).into();

    let signal = ProtoReconfigurationSignal {
        new_storage_servers: new_storage_servers.clone(),
    };

    let payload = ProtoPayload {
        message: Some(
            psl::proto::rpc::proto_payload::Message::ReconfigurationSignal(signal),
        ),
    };

    let buf = payload.encode_to_vec();
    let sz = buf.len();
    let msg = PinnedMessage::from(buf, sz, SenderType::Anon);

    let target = String::from("sequencer1");

    log::info!(
        "Sending reconfiguration signal to {} with new storage servers: {:?}",
        target,
        new_storage_servers
    );

    let resp = PinnedClient::send(
        &client,
        &target,
        msg.as_ref(),
    )
    .await;

    match resp {
        Ok(()) => log::info!("Reconfiguration signal sent successfully."),
        Err(e) => log::error!("Failed to send reconfiguration signal: {:?}", e),
    }

    Ok(())
}
