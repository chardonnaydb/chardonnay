use common::config::Config;
use tracing::info;

mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    info!("Hello, Universe Manager!");
    // TODO(yanniszark): take the config path as an argument.
    let config: Config =
        serde_json::from_str(&std::fs::read_to_string("config.json").unwrap()).unwrap();
    let addr = config.universe.proto_server_addr.to_string();
    server::run_universe_server(addr).await?;
    Ok(())
}
