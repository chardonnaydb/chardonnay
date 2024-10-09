use std::{
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common::{
    host_info::{self, HostInfo},
    region::{self, Region, Zone},
};
use pin_project::{pin_project, pinned_drop};
use proto::universe::universe_client::UniverseClient;
use proto::universe::universe_server::Universe;
use proto::universe::{
    CreateKeyspaceRequest, CreateKeyspaceResponse, ListKeyspacesRequest, ListKeyspacesResponse,
};
use tokio::sync::broadcast;
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream},
    Stream,
};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument};

use crate::storage::{
    cassandra::{self, Cassandra},
    Storage,
};

/// Implementation of the Universe manager.
pub struct UniverseServer<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage> UniverseServer<S> {
    /// Creates a new UniverseServer.
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

#[tonic::async_trait]
impl<S: Storage> Universe for UniverseServer<S> {
    #[instrument(skip(self))]
    async fn create_keyspace(
        &self,
        request: Request<CreateKeyspaceRequest>,
    ) -> Result<Response<CreateKeyspaceResponse>, Status> {
        debug!("Got a create_keyspace request: {:?}", request);

        let req_inner = request.into_inner();
        let keyspace_id = self
            .storage
            .create_keyspace(
                &req_inner.name,
                &req_inner.namespace,
                req_inner.primary_zone,
                req_inner.base_key_ranges,
            )
            .await
            .map_err(|e| Status::internal(format!("Failed to create keyspace: {}", e)))?;

        let response = CreateKeyspaceResponse { keyspace_id };
        Ok(Response::new(response))
    }

    #[instrument(skip(self))]
    async fn list_keyspaces(
        &self,
        request: Request<ListKeyspacesRequest>,
    ) -> Result<Response<ListKeyspacesResponse>, Status> {
        debug!("Got a list_keyspaces request: {:?}", request);

        let keyspaces = self
            .storage
            .list_keyspaces()
            .await
            .map_err(|e| Status::internal(format!("Failed to list keyspaces: {}", e)))?;

        let response = ListKeyspacesResponse { keyspaces };
        Ok(Response::new(response))
    }
}

/// Runs the Universe Manager, listening on the provided address.
///
/// # Arguments
/// - `addr`: The address for the Universe server to listen on.
///
/// # Errors
/// This function will return an error if the provided address is invalid or if there is an issue
/// starting the Universe server.
pub async fn run_universe_server<S: Storage>(
    addr: impl AsRef<str> + std::net::ToSocketAddrs,
    storage: S,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.to_socket_addrs()?.next().ok_or("Invalid address")?;

    info!("UniverseServer listening on {}", addr);

    let universe_server = UniverseServer::new(Arc::new(storage));

    tonic::transport::Server::builder()
        .add_service(proto::universe::universe_server::UniverseServer::new(
            universe_server,
        ))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cassandra::Cassandra;
    use proto::universe::{KeyRange, KeyRangeRequest};
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_run_universe_server() {
        // Use a random available port
        let addr = "127.0.0.1:0";

        // Create a Cassandra CQL session
        let storage = Cassandra::new("127.0.0.1:9042".to_string()).await;

        // Start the server in a separate task
        let server_task = tokio::spawn(async move {
            run_universe_server(addr, storage).await.unwrap();
        });

        // Wait for 1 second
        sleep(Duration::from_secs(1)).await;

        // Stop the server by aborting the task
        server_task.abort();

        // Wait for the server to shut down
        let _ = server_task.await;
    }

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
        let keyspace_name = "test_keyspace";
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
}
