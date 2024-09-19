use std::{net::UdpSocket, sync::Arc};
use tracing_subscriber;

use common::{
    config::Config,
    host_info::HostInfo,
    network::{fast_network::FastNetwork, for_testing::udp_fast_network::UdpFastNetwork},
    region::{Region, Zone},
};
use rangeserver::{
    cache::memtabledb::MemTableDB, for_testing::mock_warden::MockWarden, server::Server,
    storage::cassandra::Cassandra,
};
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;

fn main() {
    tracing_subscriber::fmt::init();
    // TODO(tamer): take the config path as an argument.
    let config: Config =
        serde_json::from_str(&std::fs::read_to_string("config.json").unwrap()).unwrap();
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let fast_network = Arc::new(UdpFastNetwork::new(
        UdpSocket::bind(format!(
            "127.0.0.1:{}",
            config.range_server.fast_network_port
        ))
        .unwrap(),
    ));
    let fast_network_clone = fast_network.clone();
    runtime.spawn(async move {
        loop {
            fast_network_clone.poll();
            tokio::task::yield_now().await
        }
    });
    let server_handle = runtime.spawn(async move {
        let host_info = get_host_info();
        let mock_warden = MockWarden::new();
        let warden_address = config
            .regions
            .get(&host_info.zone.region)
            .unwrap()
            .warden_address;
        let _ = mock_warden.start(Some(warden_address)).await.unwrap();
        let storage = Arc::new(Cassandra::new("127.0.0.1:9042".to_string()).await);
        let epoch_supplier =
            Arc::new(rangeserver::for_testing::epoch_supplier::EpochSupplier::new());

        // TODO: set number of threads and pin to cores.
        let bg_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let server = Server::<_, MemTableDB>::new(
            config,
            host_info,
            storage,
            epoch_supplier,
            bg_runtime.handle().clone(),
        );
        let res = Server::start(server, fast_network, CancellationToken::new())
            .await
            .unwrap();
        res.await.unwrap()
    });
    runtime.block_on(server_handle).unwrap().unwrap();
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
        address: "127.0.0.1:50054".parse().unwrap(),
        zone,
        warden_connection_epoch: 0,
    }
}
