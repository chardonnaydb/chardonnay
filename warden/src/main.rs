use common::config::Config;
use common::region::Region;
use server::run_warden_server;
use tokio_util::sync::CancellationToken;
use tracing::info;

mod assignment_computation;
mod persistence;
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = tracing_subscriber::fmt::Subscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;
    info!("Hello, Warden!");
    // TODO(tamer): take the config path as an argument.
    let config: Config =
        serde_json::from_str(&std::fs::read_to_string("config.json").unwrap()).unwrap();
    //TODO(tamer): the region should be passed in as an argument or environment.
    let region = Region {
        cloud: None,
        name: "test-region".into(),
    };
    let region_config = config.regions.get(&region).unwrap();
    let addr = region_config.warden_address.to_string();
    let universe_addr = format!("http://{}", config.universe.proto_server_addr.to_string());
    let token = CancellationToken::new();
    // TODO(purujit): set up map computation and plug it in.
    run_warden_server(
        addr,
        universe_addr,
        region,
        tokio::runtime::Handle::current(),
        token,
    )
    .await?;
    Ok(())
}
