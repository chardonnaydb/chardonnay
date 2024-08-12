use epoch_publisher::client::EpochPublisherClient;
use epoch_publisher::error::Error;
use epoch_publisher::for_testing::mock_epoch::MockEpoch;
use epoch_publisher::server::Server;
use std::{
    collections::HashSet,
    net::{SocketAddr, UdpSocket},
    sync::Arc,
    time,
};
use tokio_util::sync::CancellationToken;

use common::{
    config::{Config, EpochConfig, RangeServerConfig, RegionConfig},
    host_info::HostInfo,
    network::{fast_network::FastNetwork, for_testing::udp_fast_network::UdpFastNetwork},
    region::{Region, Zone},
};
use tokio::runtime::Builder;

struct TestContext {
    client: Arc<EpochPublisherClient>,
    cancellation_token: CancellationToken,
    server_runtime: tokio::runtime::Runtime,
    client_runtime: tokio::runtime::Runtime,
}

fn get_config(epoch_address: SocketAddr) -> Config {
    let region = Region {
        cloud: None,
        name: "test-region".into(),
    };
    let region_config = RegionConfig {
        // Not used in these tests.
        warden_address: "127.0.0.1:1".parse().unwrap(),
        epoch_publishers: HashSet::new(),
    };
    let epoch_config = EpochConfig {
        // Not used in these tests.
        proto_server_addr: epoch_address,
    };
    let mut config = Config {
        range_server: RangeServerConfig {
            range_maintenance_duration: time::Duration::from_secs(1),
            proto_server_addr: "127.0.0.1:50051".parse().unwrap(),
        },
        regions: std::collections::HashMap::new(),
        epoch: epoch_config,
    };
    config.regions.insert(region, region_config);
    config
}

async fn setup_server(
    server_socket: UdpSocket,
    cancellation_token: CancellationToken,
    epoch_address: SocketAddr,
) -> tokio::runtime::Runtime {
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let fast_network = Arc::new(UdpFastNetwork::new(server_socket));
    let fast_network_clone = fast_network.clone();
    runtime.spawn(async move {
        loop {
            fast_network_clone.poll();
            tokio::task::yield_now().await
        }
    });
    runtime.spawn(async move {
        let config = get_config(epoch_address);
        let bg_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let server = Server::new(config, bg_runtime.handle().clone());
        Server::start(server, fast_network, cancellation_token).await;
    });
    runtime
}

fn get_server_host_info(address: SocketAddr) -> HostInfo {
    let identity: String = "test_publisher".into();
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
        address,
        zone,
    }
}

async fn setup_client(
    cancellation_token: CancellationToken,
    server_address: SocketAddr,
) -> (Arc<EpochPublisherClient>, tokio::runtime::Runtime) {
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let fast_network = Arc::new(UdpFastNetwork::new(UdpSocket::bind("127.0.0.1:0").unwrap()));
    let fast_network_clone = fast_network.clone();
    runtime.spawn(async move {
        loop {
            fast_network_clone.poll();
            tokio::task::yield_now().await
        }
    });
    let client = EpochPublisherClient::new(
        fast_network,
        runtime.handle().clone(),
        get_server_host_info(server_address),
        cancellation_token.clone(),
    );
    return (client, runtime);
}

async fn setup(initial_epoch: u64) -> TestContext {
    let server_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let server_address = server_socket.local_addr().unwrap();
    let mock_epoch = MockEpoch::new();
    let epoch_address = mock_epoch.start().await.unwrap();
    mock_epoch.set_epoch(initial_epoch).await;
    // Give some delay so the server can connect to the epoch service to sync its epoch.
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let cancellation_token = CancellationToken::new();
    let server_runtime =
        setup_server(server_socket, cancellation_token.clone(), epoch_address).await;
    // Give some delay so the server can setup its networking.
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    let (client, client_runtime) = setup_client(cancellation_token.clone(), server_address).await;
    TestContext {
        client,
        cancellation_token,
        server_runtime,
        client_runtime,
    }
}

async fn tear_down(context: TestContext) {
    context.cancellation_token.cancel();
    // TODO: investigate why shutdown isn't clean.
    context.server_runtime.shutdown_background();
    context.client_runtime.shutdown_background();
}

#[tokio::test]
async fn read_uninitialized_epoch() {
    let context = setup(0).await;
    let err = context.client.read_epoch().await;
    let err = err.err().unwrap();
    assert!(err == Error::EpochUnknown);
    tear_down(context).await
}

#[tokio::test]
async fn read_epoch() {
    let context = setup(42).await;
    let epoch = context.client.read_epoch().await.unwrap();
    assert!(epoch == 42);
    tear_down(context).await
}
