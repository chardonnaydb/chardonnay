use std::sync::Arc;

use common::config::Config;
use std::net::UdpSocket;
use tracing_subscriber;

use common::{
    network::{fast_network::FastNetwork, for_testing::udp_fast_network::UdpFastNetwork},
    region::{Region, Zone},
};
use epoch_publisher::server;

use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;

fn main() {
    tracing_subscriber::fmt::init();
    // TODO(tamer): take the config path as an argument.
    let config: Config = serde_json::from_str(
        &std::fs::read_to_string(
            "/Users/tamereldeeb/vscode/chardonnay/rangeserver/src/config.json",
        )
        .unwrap(),
    )
    .unwrap();
    //TODO(tamer): the name, zone etc should be passed in as an argument or environment.
    let region = Region {
        cloud: None,
        name: "test-region".into(),
    };
    let zone = Zone {
        region: region.clone(),
        name: "a".into(),
    };
    let publisher_set_name = "ps1";
    let publisher_name = "ep1";
    let region_config = config.regions.get(&region).unwrap();
    let publisher_set = region_config
        .epoch_publishers
        .iter()
        .find(|&x| x.zone == zone && x.name == publisher_set_name)
        .unwrap();
    let publisher_config = publisher_set
        .publishers
        .iter()
        .find(|&x| x.name == publisher_name)
        .unwrap();
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let fast_network = Arc::new(UdpFastNetwork::new(
        UdpSocket::bind(publisher_config.fast_network_addr).unwrap(),
    ));
    let fast_network_clone = fast_network.clone();
    runtime.spawn(async move {
        loop {
            fast_network_clone.poll();
            tokio::task::yield_now().await
        }
    });
    let bg_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let server = server::Server::new(
        config.clone(),
        publisher_config.clone(),
        bg_runtime.handle().clone(),
    );
    let cancellation_token = CancellationToken::new();
    let ct_clone = cancellation_token.clone();
    let rt_handle = runtime.handle().clone();
    bg_runtime.spawn(async move {
        server::Server::start(server, fast_network.clone(), rt_handle, ct_clone).await;
    });

    runtime.block_on(async move { cancellation_token.cancelled().await });
}
