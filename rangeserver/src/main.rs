use std::{net::UdpSocket, sync::Arc, time};

use common::{
    config::{Config, RangeServerConfig, RegionConfig},
    host_info::HostInfo,
    network::{fast_network::FastNetwork, for_testing::udp_fast_network::UdpFastNetwork},
    region::{Region, Zone},
};
use rangeserver::{
    for_testing::mock_warden::MockWarden, server::Server, storage::cassandra::Cassandra,
};
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;

fn main() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let fast_network = UdpFastNetwork::new(UdpSocket::bind("127.0.0.1:10001").unwrap());
    runtime.spawn(async move {
        loop {
            fast_network.poll();
            tokio::task::yield_now().await
        }
    });
    let server_handle = runtime.spawn(async move {
        let mock_warden = MockWarden::new();
        mock_warden.start().await.unwrap();
        let storage = Arc::new(Cassandra::new("127.0.0.1:9042".to_string()).await);
        let epoch_provider =
            Arc::new(rangeserver::for_testing::epoch_provider::EpochProvider::new());
        let config = get_config();
        let host_info = get_host_info();
        let server = Server::new(config, host_info, storage, epoch_provider);
        let res = Server::start(server, CancellationToken::new())
            .await
            .unwrap();
        res.await.unwrap()
    });
    runtime.block_on(server_handle).unwrap().unwrap();
}

fn get_config() -> Config {
    // TODO: should be read from file!
    let region = Region {
        cloud: None,
        name: "test-region".into(),
    };
    let region_config = RegionConfig {
        warden_address: rangeserver::for_testing::mock_warden::SERVER_ADDR.into(),
    };
    let mut config = Config {
        range_server: RangeServerConfig {
            range_maintenance_duration: time::Duration::from_secs(1),
        },
        regions: std::collections::HashMap::new(),
    };
    config.regions.insert(region, region_config);
    config
}

fn get_host_info() -> HostInfo {
    // TODO: should be read from enviroment!
    let identity: String = "test_server".into();
    let region = Region {
        cloud: None,
        name: "test-region".into(),
    };
    let zone = Zone {
        region: region.clone(),
        name: "a".into(),
    };
    HostInfo {
        identity: identity.clone(),
        address: "127.0.0.1:10001".parse().unwrap(),
        zone,
    }
}
