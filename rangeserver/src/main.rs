use std::{net::UdpSocket, sync::Arc};
use tracing_subscriber;

use common::{
    config::Config,
    host_info::HostInfo,
    network::{fast_network::FastNetwork, for_testing::udp_fast_network::UdpFastNetwork},
    region::{Region, Zone},
};
use rangeserver::{cache::memtabledb::MemTableDB, server::Server, storage::cassandra::Cassandra};
use tokio::runtime::Builder;
use tokio::{
    net::TcpListener,
    sync::{mpsc, RwLock},
};
use tokio_util::sync::CancellationToken;

fn main() {
    tracing_subscriber::fmt::init();
    // TODO(tamer): take the config path as an argument.
    let config: Config =
        serde_json::from_str(&std::fs::read_to_string("config.json").unwrap()).unwrap();
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let runtime_handle = runtime.handle().clone();
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
        let cancellation_token = CancellationToken::new();
        let host_info = get_host_info();
        let region_config = config.regions.get(&host_info.zone.region).unwrap();
        let publisher_set = region_config
            .epoch_publishers
            .iter()
            .find(|&s| s.zone == host_info.zone)
            .unwrap();
        let storage = Arc::new(Cassandra::new("127.0.0.1:9042".to_string()).await);
        let epoch_supplier =
            Arc::new(rangeserver::for_testing::epoch_supplier::EpochSupplier::new());
        let proto_server_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        // let config = get_config(warden_address, &proto_server_listener).await;

        let host_info = get_host_info();
        // TODO: set number of threads and pin to cores.
        let bg_runtime = Builder::new_multi_thread().enable_all().build().unwrap();

        let epoch_supplier = Arc::new(rangeserver::epoch_supplier::reader::Reader::new(
            fast_network.clone(),
            runtime_handle,
            bg_runtime.handle().clone(),
            publisher_set.clone(),
            cancellation_token.clone(),
        ));
        let server = Server::<_, MemTableDB>::new(
            config,
            host_info,
            storage,
            epoch_supplier,
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
