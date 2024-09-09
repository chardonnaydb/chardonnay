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
    // TODO(purujit): set up map computation and plug it in.
    let addr = "[::1]:50051";
    let token = CancellationToken::new();
    run_warden_server(addr, tokio::runtime::Handle::current(), token).await?;
    Ok(())
}
