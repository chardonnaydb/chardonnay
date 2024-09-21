use std::sync::Arc;

use common::config::Config;
use epoch::server;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    // TODO(tamer): take the config path as an argument.
    let config: Config = serde_json::from_str(
        &std::fs::read_to_string(
            "/Users/tamereldeeb/vscode/chardonnay/rangeserver/src/config.json",
        )
        .unwrap(),
    )
    .unwrap();
    let storage = epoch::storage::cassandra::Cassandra::new(
        "127.0.0.1:9042".to_string(),
        "GLOBAL".to_string(),
    )
    .await;

    let server = Arc::new(server::Server::new(storage, config));
    let cancellation_token = CancellationToken::new();
    server::Server::start(server, cancellation_token).await;
}
