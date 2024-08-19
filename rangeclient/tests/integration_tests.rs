use bytes::Bytes;
use std::{
    collections::HashSet,
    net::{SocketAddr, UdpSocket},
    sync::Arc,
    time,
};
use tokio_util::sync::CancellationToken;

use common::{
    config::{Config, EpochConfig, RangeServerConfig, RegionConfig},
    full_range_id::FullRangeId,
    host_info::HostInfo,
    keyspace_id::KeyspaceId,
    network::{fast_network::FastNetwork, for_testing::udp_fast_network::UdpFastNetwork},
    record::Record,
    region::{Region, Zone},
};
use rangeclient::client::RangeClient;
use rangeserver::{
    cache::memtabledb::MemTableDB,
    for_testing::{epoch_supplier::EpochSupplier, mock_warden::MockWarden},
    server::Server,
    transaction_info::TransactionInfo,
};
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use uuid::Uuid;

struct TestContext {
    client: Arc<RangeClient>,
    cancellation_token: CancellationToken,
    server_runtime: tokio::runtime::Runtime,
    client_runtime: tokio::runtime::Runtime,
    storage_context: rangeserver::storage::cassandra::for_testing::TestContext,
    proto_server_address: SocketAddr,
}

fn get_config(warden_address: SocketAddr, proto_server_address: SocketAddr) -> Config {
    // TODO: should be read from file!
    let region = Region {
        cloud: None,
        name: "test-region".into(),
    };
    let region_config = RegionConfig {
        warden_address: warden_address,
        epoch_publishers: HashSet::new(),
    };
    let epoch_config = EpochConfig {
        // Not used in these tests.
        proto_server_addr: "127.0.0.1:1".parse().unwrap(),
    };
    let mut config = Config {
        range_server: RangeServerConfig {
            range_maintenance_duration: time::Duration::from_secs(1),
            proto_server_addr: proto_server_address,
        },
        regions: std::collections::HashMap::new(),
        epoch: epoch_config,
    };
    config.regions.insert(region, region_config);
    config
}

fn get_server_host_info(address: SocketAddr) -> HostInfo {
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
        address,
        zone,
    }
}

async fn setup_server(
    server_socket: UdpSocket,
    cancellation_token: CancellationToken,
    warden_address: SocketAddr,
    proto_server_listener: TcpListener,
    epoch_provider: Arc<EpochProvider>,
    storage_context: &rangeserver::storage::cassandra::for_testing::TestContext,
) -> tokio::runtime::Runtime {
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let server_address = server_socket.local_addr().unwrap();
    let fast_network = Arc::new(UdpFastNetwork::new(server_socket));
    let fast_network_clone = fast_network.clone();
    runtime.spawn(async move {
        loop {
            fast_network_clone.poll();
            tokio::task::yield_now().await
        }
    });
    let storage = storage_context.cassandra.clone();

    runtime.spawn(async move {
        let config = get_config(warden_address, proto_server_listener.local_addr().unwrap());
        let host_info = get_server_host_info(server_address);
        let bg_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let server = Server::<_, _, MemTableDB>::new(
            config,
            host_info,
            storage,
            epoch_supplier,
            bg_runtime.handle().clone(),
        );
        // TODO pass in TCP stream with port 0
        // need to propagate address to client
        let res = Server::start(
            server,
            fast_network,
            cancellation_token,
            proto_server_listener,
        )
        .await
        .unwrap();
        res.await.unwrap()
    });
    runtime
}

async fn setup_client(
    cancellation_token: CancellationToken,
    server_address: SocketAddr,
) -> (Arc<RangeClient>, tokio::runtime::Runtime) {
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let fast_network = Arc::new(UdpFastNetwork::new(UdpSocket::bind("127.0.0.1:0").unwrap()));
    let fast_network_clone = fast_network.clone();
    runtime.spawn(async move {
        loop {
            fast_network_clone.poll();
            tokio::task::yield_now().await
        }
    });
    let client = RangeClient::new(
        fast_network,
        runtime.handle().clone(),
        get_server_host_info(server_address),
        cancellation_token.clone(),
    );
    return (client, runtime);
}

async fn setup() -> TestContext {
    let server_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let server_address = server_socket.local_addr().unwrap();
    let epoch_supplier = Arc::new(rangeserver::for_testing::epoch_supplier::EpochSupplier::new());
    let mock_warden = MockWarden::new();
    let warden_address = mock_warden.start().await.unwrap();
    let proto_server_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proto_server_address = proto_server_listener.local_addr().unwrap();
    let cancellation_token = CancellationToken::new();
    let storage_context: rangeserver::storage::cassandra::for_testing::TestContext =
        rangeserver::storage::cassandra::for_testing::init().await;
    let server_runtime = setup_server(
        server_socket,
        cancellation_token.clone(),
        warden_address,
        proto_server_listener,
        epoch_provider.clone(),
        &storage_context,
    )
    .await;
    let (client, client_runtime) = setup_client(cancellation_token.clone(), server_address).await;
    let range_id = FullRangeId {
        keyspace_id: storage_context.keyspace_id,
        range_id: storage_context.range_id,
    };
    let server_identity: String = "test_server".into();
    while !mock_warden.is_connected(&server_identity).await {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    mock_warden.assign(&range_id, &server_identity).await;
    // Give some delay so the RM can see the assignment and the epoch advancing.
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    epoch_supplier.set_epoch(1).await;
    TestContext {
        client,
        cancellation_token,
        server_runtime,
        client_runtime,
        storage_context,
        proto_server_address,
    }
}

async fn tear_down(context: TestContext) {
    context.cancellation_token.cancel();
    // TODO: investigate why shutdown isn't clean.
    context.server_runtime.shutdown_background();
    context.client_runtime.shutdown_background();
}

fn start_transaction() -> Arc<TransactionInfo> {
    Arc::new(TransactionInfo {
        id: Uuid::new_v4(),
        started: chrono::Utc::now(),
        overall_timeout: time::Duration::from_secs(10),
    })
}

#[tokio::test]
async fn unknown_range() {
    let context = setup().await;
    let tx = start_transaction();
    let range_id = FullRangeId {
        keyspace_id: KeyspaceId::new(Uuid::new_v4()),
        range_id: Uuid::new_v4(),
    };
    let keys = Vec::new();
    let err = context
        .client
        .get(tx, &range_id, keys)
        .await
        .expect_err("Unknown range")
        .to_flatbuf_status();
    assert!(err == flatbuf::rangeserver_flatbuffers::range_server::Status::RangeIsNotLoaded);
    tear_down(context).await
}

#[tokio::test]
async fn read_initial() {
    let context = setup().await;
    let key = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
    let tx = start_transaction();
    let range_id = FullRangeId {
        keyspace_id: context.storage_context.keyspace_id,
        range_id: context.storage_context.range_id,
    };
    let keys = vec![key];
    let vals = context
        .client
        .get(tx.clone(), &range_id, keys)
        .await
        .unwrap();
    let val = vals.get(0).unwrap();
    assert!(val.is_none());
    context
        .client
        .abort_transaction(tx, &range_id)
        .await
        .unwrap();
    tear_down(context).await
}

#[tokio::test]
async fn commit_no_writes() {
    let context = setup().await;
    let key = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
    let tx = start_transaction();
    let range_id = FullRangeId {
        keyspace_id: context.storage_context.keyspace_id,
        range_id: context.storage_context.range_id,
    };
    let keys = vec![key];
    let vals = context
        .client
        .get(tx.clone(), &range_id, keys)
        .await
        .unwrap();
    let val = vals.get(0).unwrap();
    assert!(val.is_none());
    let writes = vec![];
    let deletes = vec![];
    let prepare_ok = context
        .client
        .prepare_transaction(tx.clone(), &range_id, true, &writes, &deletes)
        .await
        .unwrap();
    context
        .client
        .commit_transaction(tx, &range_id, prepare_ok.highest_known_epoch)
        .await
        .unwrap();
    tear_down(context).await
}

#[tokio::test]
async fn read_modify_write() {
    let context = setup().await;
    let key1 = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
    let key2 = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
    let tx = start_transaction();
    let range_id = FullRangeId {
        keyspace_id: context.storage_context.keyspace_id,
        range_id: context.storage_context.range_id,
    };
    let keys = vec![key1.clone(), key2.clone()];
    let vals = context
        .client
        .get(tx.clone(), &range_id, keys)
        .await
        .unwrap();
    assert!(vals.len() == 2);
    assert!(vals.get(0).unwrap().is_none());
    assert!(vals.get(1).unwrap().is_none());
    let val1 = Bytes::from_static(b"I have a value!");
    let record1 = Record {
        key: key1.clone(),
        val: val1.clone(),
    };
    let val2 = Bytes::from_static(b"I have a different value!");
    let record2 = Record {
        key: key2.clone(),
        val: val2.clone(),
    };
    let writes = vec![record1, record2];
    let deletes = vec![];
    let prepare_ok = context
        .client
        .prepare_transaction(tx.clone(), &range_id, true, &writes, &deletes)
        .await
        .unwrap();
    context
        .client
        .commit_transaction(tx, &range_id, prepare_ok.highest_known_epoch)
        .await
        .unwrap();
    // Now read the values in a new transaction.
    let tx2 = start_transaction();
    let keys = vec![key1.clone(), key2.clone()];
    let vals = context.client.get(tx2, &range_id, keys).await.unwrap();
    assert!(vals.len() == 2);
    assert!(vals.get(0).unwrap().as_ref().unwrap().eq(&val1));
    assert!(vals.get(1).unwrap().as_ref().unwrap().eq(&val2));
    tear_down(context).await
}

#[tokio::test]
async fn test_prefetch_with_value() {
    let context = setup().await;
    let key1 = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
    let key2 = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
    let tx = start_transaction();
    let range_id = FullRangeId {
        keyspace_id: context.storage_context.keyspace_id,
        range_id: context.storage_context.range_id,
    };
    let keys = vec![key1.clone(), key2.clone()];
    let _ = context
        .client
        .get(tx.clone(), &range_id, keys)
        .await
        .unwrap();
    let val1 = Bytes::from_static(b"I have a value!");
    let record1 = Record {
        key: key1.clone(),
        val: val1.clone(),
    };
    let val2 = Bytes::from_static(b"I have a different value!");
    let record2 = Record {
        key: key2.clone(),
        val: val2.clone(),
    };
    let writes = vec![record1, record2];
    let deletes = vec![];
    let prepare_ok = context
        .client
        .prepare_transaction(tx.clone(), &range_id, true, &writes, &deletes)
        .await
        .unwrap();
    context
        .client
        .commit_transaction(tx.clone(), &range_id, prepare_ok.highest_known_epoch)
        .await
        .unwrap();
    let tx2 = start_transaction();
    let keys = vec![key1.clone(), key2.clone()];
    let addr = context.proto_server_address;
    let vals = context
        .client
        .prefetch(tx2, &range_id, keys, addr)
        .await
        .unwrap();
    assert_eq!(vals, ());
    tear_down(context).await;
}

#[tokio::test]
async fn test_prefetch_no_value() {
    let context = setup().await;
    let key1 = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
    let key2 = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
    let tx = start_transaction();
    let range_id = FullRangeId {
        keyspace_id: context.storage_context.keyspace_id,
        range_id: context.storage_context.range_id,
    };
    let keys = vec![key1.clone(), key2.clone()];
    let addr = context.proto_server_address;
    let vals = context
        .client
        .prefetch(tx, &range_id, keys, addr)
        .await
        .unwrap();
    assert_eq!(vals, ());
    tear_down(context).await;
}
