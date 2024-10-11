use common::config::Config;
use tracing::info;
use universe::server;
use universe::storage::cassandra::Cassandra;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    info!("Hello, Universe Manager!");
    // TODO(yanniszark): take the config path as an argument.
    let config: Config =
        serde_json::from_str(&std::fs::read_to_string("config.json").unwrap()).unwrap();
    let addr = config.universe.proto_server_addr.to_string();
    let storage = Cassandra::new(config.cassandra.cql_addr.to_string()).await;

    server::run_universe_server(addr, storage).await?;
    Ok(())
}
