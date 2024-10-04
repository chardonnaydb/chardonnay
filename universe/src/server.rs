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

/// Implementation of the Universe manager.
pub struct UniverseServer {}

#[tonic::async_trait]
impl Universe for UniverseServer {
    #[instrument(skip(self))]
    async fn create_keyspace(
        &self,
        request: Request<CreateKeyspaceRequest>,
    ) -> Result<Response<CreateKeyspaceResponse>, Status> {
        debug!("Got a create_keyspace request: {:?}", request);

        // TODO: Implement the actual logic to create a keyspace
        // For now, we'll return an empty response
        let response = CreateKeyspaceResponse {
            keyspace_id: "1234".to_string(),
        };
        Ok(Response::new(response))
    }

    #[instrument(skip(self))]
    async fn list_keyspaces(
        &self,
        request: Request<ListKeyspacesRequest>,
    ) -> Result<Response<ListKeyspacesResponse>, Status> {
        debug!("Got a list_keyspaces request: {:?}", request);

        // TODO: Implement the actual logic to list keyspaces
        // For now, we'll return an empty response
        let response = ListKeyspacesResponse {
            keyspaces: Vec::new(),
        };
        Ok(Response::new(response))
    }
}

impl UniverseServer {
    pub fn new() -> Self {
        Self {}
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
pub async fn run_universe_server(
    addr: impl AsRef<str> + std::net::ToSocketAddrs,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.to_socket_addrs()?.next().ok_or("Invalid address")?;
    let universe_server = UniverseServer::new();

    info!("UniverseServer listening on {}", addr);

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
    use tonic::transport::{channel, Channel};

    use super::run_universe_server;

    #[tokio::test]
    async fn test_create_keyspace() {
        // Create server task
        let server_task = tokio::spawn(async move {
            let addr = "[::1]:50051";
            run_universe_server(addr).await.unwrap();
        });

        // Give the server a moment to start up
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Create a client and connect to the server
        let channel = Channel::from_static("http://[::1]:50051")
            .connect()
            .await
            .unwrap();
        let mut client = proto::universe::universe_client::UniverseClient::new(channel);

        // Create a keyspace
        let request = proto::universe::CreateKeyspaceRequest {
            name: "test".to_string(),
            namespace: "test".to_string(),
            primary_zone: None,
            base_key_ranges: Vec::new(),
        };

        let response = client.create_keyspace(request).await.unwrap();
        assert_eq!(response.get_ref().keyspace_id, "1234");

        server_task.abort();
    }

    #[tokio::test]
    async fn test_list_keyspaces() {
        // Create server task
        let server_task = tokio::spawn(async move {
            let addr = "[::1]:50051";
            run_universe_server(addr).await.unwrap();
        });

        // Give the server a moment to start up
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Create a client and connect to the server
        let channel = Channel::from_static("http://[::1]:50051")
            .connect()
            .await
            .unwrap();
        let mut client = proto::universe::universe_client::UniverseClient::new(channel);

        // List keyspaces
        let request = proto::universe::ListKeyspacesRequest { region: None };

        let response = client.list_keyspaces(request).await.unwrap();
        let keyspaces = response.get_ref().keyspaces.clone();

        // Assert that the response contains the expected keyspaces
        // This assertion might need to be adjusted based on the actual implementation
        assert!(keyspaces.is_empty(), "Expected empty list of keyspaces");

        // You might want to add more specific assertions here, such as:
        // assert_eq!(keyspaces[0].name, "expected_name");
        // assert_eq!(keyspaces[0].namespace, "expected_namespace");

        server_task.abort();
    }
}
