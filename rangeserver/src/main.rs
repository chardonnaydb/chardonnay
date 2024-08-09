use std::{
    net::{SocketAddr, UdpSocket},
    sync::Arc,
    time,
};

use common::{
    config::{Config, RangeServerConfig, RegionConfig},
    host_info::HostInfo,
    network::{fast_network::FastNetwork, for_testing::udp_fast_network::UdpFastNetwork},
    region::{Region, Zone},
};
use rangeserver::{
    cache::memtabledb::MemTableDB, for_testing::mock_warden::MockWarden, server::Server,
    storage::cassandra::Cassandra,
};
use tokio::runtime::Builder;
use tokio::{
    net::TcpListener,
    sync::{mpsc, RwLock},
};
use tokio_util::sync::CancellationToken;

fn main() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let fast_network = Arc::new(UdpFastNetwork::new(
        UdpSocket::bind("127.0.0.1:10001").unwrap(),
    ));
    let fast_network_clone = fast_network.clone();
    runtime.spawn(async move {
        loop {
            fast_network_clone.poll();
            tokio::task::yield_now().await
        }
    });
    let server_handle = runtime.spawn(async move {
        let mock_warden = MockWarden::new();
        let warden_address = mock_warden.start().await.unwrap();
        let storage = Arc::new(Cassandra::new("127.0.0.1:9042".to_string()).await);
        let epoch_provider =
            Arc::new(rangeserver::for_testing::epoch_provider::EpochProvider::new());
        let proto_server_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = get_config(warden_address, &proto_server_listener).await;

        let host_info = get_host_info();
        // TODO: set number of threads and pin to cores.
        let bg_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let server = Server::<_, _, MemTableDB>::new(
            config,
            host_info,
            storage,
            epoch_provider,
            bg_runtime.handle().clone(),
        );
        let res = Server::start(
            server,
            fast_network,
            CancellationToken::new(),
            proto_server_listener,
        )
        .await
        .unwrap();
        res.await.unwrap()
    });
    runtime.block_on(server_handle).unwrap().unwrap();
}

async fn get_config(warden_address: SocketAddr, proto_server_listener: &TcpListener) -> Config {
    // TODO: should be read from file!
    let region = Region {
        cloud: None,
        name: "test-region".into(),
    };
    let region_config = RegionConfig {
        warden_address: warden_address.to_string(),
    };

    let mut config = Config {
        range_server: RangeServerConfig {
            range_maintenance_duration: time::Duration::from_secs(1),
            proto_server_addr: proto_server_listener.local_addr().unwrap(),
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
