use std::time::Duration;

use proto::universe::{
    universe_client::UniverseClient, CreateKeyspaceRequest, ListKeyspacesRequest,
};
use tokio::time::sleep;
use universe::{server::run_universe_server, storage::cassandra::Cassandra};
use uuid::Uuid;

#[tokio::test]
async fn test_create_and_list_keyspace_handlers() {
    let addr = "127.0.0.1:50056";

    // Create a Cassandra CQL session
    let storage = Cassandra::new("127.0.0.1:9042".to_string()).await;

    // Start the server in a separate task
    let server_task = tokio::spawn(async move {
        run_universe_server(addr, storage).await.unwrap();
    });

    // Wait a bit for the server to start
    sleep(Duration::from_millis(100)).await;

    // Create a Universe client
    let mut client = UniverseClient::connect(format!("http://{}", addr))
        .await
        .unwrap();

    // Create a keyspace
    let keyspace_name = format!("test_keyspace_{}", Uuid::new_v4().to_string());
    let namespace = "test_namespace";
    let primary_zone = Some(proto::universe::Zone {
        region: Some(proto::universe::Region {
            cloud: Some(proto::universe::region::Cloud::OtherCloud(
                "lalakis".to_string(),
            )),
            name: "test_region".to_string(),
        }),
        name: "test_zone".to_string(),
    });
    let base_key_ranges = vec![proto::universe::KeyRangeRequest {
        lower_bound_inclusive: vec![0],
        upper_bound_exclusive: vec![10],
    }];
    // No primary zone or base key ranges for now
    let keyspace_req = CreateKeyspaceRequest {
        name: keyspace_name.to_string(),
        namespace: namespace.to_string(),
        primary_zone: primary_zone,
        base_key_ranges: base_key_ranges,
    };
    let keyspace_id = client
        .create_keyspace(keyspace_req)
        .await
        .unwrap()
        .into_inner()
        .keyspace_id;

    // List keyspaces
    let keyspaces = client
        .list_keyspaces(ListKeyspacesRequest { region: None })
        .await
        .unwrap()
        .into_inner()
        .keyspaces;

    // Check that the keyspace we created is in the list
    assert!(keyspaces.iter().any(|k| k.keyspace_id == keyspace_id));

    // Stop the server
    server_task.abort();
}
