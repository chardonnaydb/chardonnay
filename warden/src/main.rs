use server::run_warden_server;
use tracing::info;

mod persistence;
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = tracing_subscriber::fmt::Subscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;
    info!("Hello, Warden!");
    // TODO(purujit): set up map computation and plug it in.
    let addr = "[::1]:50051";
    run_warden_server(addr, |_, _| None).await?;
    Ok(())
}
