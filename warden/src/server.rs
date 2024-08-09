use std::{
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common::{
    host_info::HostInfo,
    keyspace_id::KeyspaceId,
    region::{Region, Zone},
};
use dashmap::DashMap;
use pin_project_lite::pin_project;
use proto::warden::{warden_server::Warden, RegisterRangeServerRequest, WardenUpdate};
use tokio::sync::broadcast;
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream},
    Stream,
};
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument};

use crate::{
    assignment_computation::{AssignmentComputation, SimpleAssignmentComputation},
    persistence::RangeInfo,
};

pub type AssignmentComputationFactory = Box<
    dyn Fn(
            KeyspaceId,
            Option<Vec<RangeInfo>>,
        ) -> Arc<dyn AssignmentComputation + Sync + Send + 'static>
        + Send
        + Sync
        + 'static,
>;

/// Implementation of the Warden service.
///
/// The `WardenServer` struct maintains a map from keyspace IDs to `AssignmentComputation` instances.
/// This allows the server to handle assignment computations for multiple keyspaces independently.
pub struct WardenServer<'a> {
    // A map from keyspace id to AssignmentComputation.
    keyspace_id_to_assignment_computation:
        DashMap<String, Arc<dyn AssignmentComputation + Sync + Send + 'a>>,
    assignment_computation_factory: AssignmentComputationFactory,
}

#[tonic::async_trait]
impl Warden for WardenServer<'static> {
    type RegisterRangeServerStream = AssignmentUpdateStream<'static>;

    #[instrument(skip(self))]
    async fn register_range_server(
        &self,
        request: Request<RegisterRangeServerRequest>,
    ) -> Result<Response<Self::RegisterRangeServerStream>, Status> {
        debug!("Got a register_range_server request: {:?}", request);

        let register_request = request.into_inner();
        match register_request.range_server {
            None => {
                return Err(Status::invalid_argument(
                    "range_server field is not set in the request",
                ))
            }
            Some(range_server) => {
                info!(
                    "Registering range server: {} for keyspace: {}",
                    range_server.identity, register_request.keyspace_id
                );
                let keyspace_id = uuid::Uuid::parse_str(&register_request.keyspace_id);
                if keyspace_id.is_err() {
                    return Err(Status::invalid_argument("keyspace_id is not a valid UUID"));
                }
                // TODO(purujit): Consider adding another method to WardenServer to register new keyspaces with associated base ranges.
                let assignment_computation = self
                    .keyspace_id_to_assignment_computation
                    .entry(keyspace_id.as_ref().unwrap().to_string())
                    .or_insert_with(|| {
                        (self.assignment_computation_factory)(
                            KeyspaceId {
                                id: keyspace_id.unwrap(),
                            },
                            None,
                        )
                    })
                    .clone();

                Ok(Response::new(AssignmentUpdateStream::new(
                    assignment_computation.register_range_server(HostInfo {
                        identity: range_server.identity,
                        // todo(purujit): Get the address from the range server.
                        address: SocketAddr::from(([0, 0, 0, 0], 0)),
                        zone: Zone {
                            name: range_server.zone,
                            // TODO(purujit): Get the region from the range server.
                            region: Region {
                                cloud: None,
                                name: "".to_string(),
                            },
                        },
                    })?,
                    assignment_computation,
                )))
            }
        }
    }
}

impl<'a> WardenServer<'a> {
    pub fn new(assignment_computation_factory: AssignmentComputationFactory) -> Self {
        Self {
            keyspace_id_to_assignment_computation: DashMap::new(),
            assignment_computation_factory,
        }
    }
}

/// Runs the Warden server, listening on the provided address and using the given update callback
/// function to generate updates for registered range servers.
///
/// # Arguments
/// - `addr`: The address for the Warden server to listen on.
///
/// # Errors
/// This function will return an error if the provided address is invalid or if there is an issue
/// starting the Warden server.
pub async fn run_warden_server(
    addr: impl AsRef<str> + std::net::ToSocketAddrs,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.to_socket_addrs()?.next().ok_or("Invalid address")?;
    let warden_server = WardenServer::new(Box::new(
        |keyspace_id: KeyspaceId, base_ranges: Option<Vec<RangeInfo>>| {
            Arc::new(SimpleAssignmentComputation::new(keyspace_id, base_ranges))
        },
    ));

    info!("WardenServer listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(proto::warden::warden_server::WardenServer::new(
            warden_server,
        ))
        .serve(addr)
        .await?;

    Ok(())
}

pin_project! {
/// A stream that provides updates to assignment versions.
///
/// This stream is created by the `WardenServer` to provide updates to registered range servers.
/// It uses a `broadcast::Receiver` to receive version updates, and a callback function to
/// generate the `WardenUpdate` messages to be sent to the clients.
///
/// The stream will send a full update on the first message, and then only send incremental
/// updates after that. If the channel buffer is exhausted, the stream will return an error
/// indicating that the client should reopen the channel.
pub struct AssignmentUpdateStream<'a> {
    sent_full_update: bool,
    update_stream: BroadcastStream<i64>,
    assignment_computation: Arc<dyn AssignmentComputation + Sync + Send + 'a>,
}
}

impl<'a> AssignmentUpdateStream<'a> {
    pub fn new(
        version_update_receiver: broadcast::Receiver<i64>,
        assignment_computation: Arc<dyn AssignmentComputation + Sync + Send + 'a>,
    ) -> Self {
        AssignmentUpdateStream {
            sent_full_update: false,
            update_stream: version_update_receiver.into(),
            assignment_computation,
        }
    }
}

impl<'a> Stream for AssignmentUpdateStream<'a> {
    type Item = Result<WardenUpdate, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pinned = std::pin::pin!(self.as_mut().project().update_stream);
        match pinned.poll_next(cx) {
            Poll::Ready(update) => match update {
                Some(result) => match result {
                    Ok(version) => {
                        let update = self
                            .assignment_computation
                            .get_assignment(version, !self.sent_full_update);
                        self.sent_full_update = true;
                        match update {
                            Some(update) => Poll::Ready(Some(Ok(update))),
                            None => Poll::Ready(Some(Err(Status::internal(format!(
                                "Unexpected failure to retrieve assignment version {}",
                                version
                            ))))),
                        }
                    }
                    Err(BroadcastStreamRecvError::Lagged(n)) => {
                        Poll::Ready(Some(Err(Status::resource_exhausted(format!(
                            "Channel Buffer exhausted. Please reopen channel. Lagged by {}",
                            n
                        )))))
                    }
                },
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::warden::warden_client::WardenClient;
    use tokio::sync::broadcast::Receiver;
    use tonic::transport::Channel;

    const FULL_UPDATE_VERSION: i64 = 1;
    const PARTIAL_UPDATE_VERSION: i64 = 2;

    struct FakeMapProducer {
        full_update: WardenUpdate,
        incremental_update: WardenUpdate,
        update_sender: broadcast::Sender<i64>,
    }

    impl FakeMapProducer {
        pub fn new() -> Self {
            Self {
                full_update: WardenUpdate {
                    update: Some(proto::warden::warden_update::Update::FullAssignment(
                        proto::warden::FullAssignment {
                            version: FULL_UPDATE_VERSION,
                            range: vec![proto::warden::RangeId {
                                keyspace_id: "test_keyspace".to_string(),
                                range_id: "test_range".to_string(),
                            }],
                        },
                    )),
                },
                incremental_update: WardenUpdate {
                    update: Some(proto::warden::warden_update::Update::IncrementalAssignment(
                        proto::warden::IncrementalAssignment {
                            version: PARTIAL_UPDATE_VERSION,
                            previous_version: FULL_UPDATE_VERSION,
                            load: vec![proto::warden::RangeId {
                                keyspace_id: "test_keyspace".to_string(),
                                range_id: "test_range".to_string(),
                            }],
                            unload: vec![],
                        },
                    )),
                },
                update_sender: broadcast::channel(1).0,
            }
        }
        pub fn send_full_update(&self) -> () {
            self.update_sender.send(FULL_UPDATE_VERSION).unwrap();
        }
        pub fn send_incremental_update(&self) -> () {
            self.update_sender.send(PARTIAL_UPDATE_VERSION).unwrap();
        }
    }

    impl AssignmentComputation for FakeMapProducer {
        fn get_assignment(&self, _version: i64, full_update: bool) -> Option<WardenUpdate> {
            if full_update {
                Some(self.full_update.clone())
            } else {
                Some(self.incremental_update.clone())
            }
        }
        fn register_range_server(
            &self,
            _: common::host_info::HostInfo,
        ) -> Result<Receiver<i64>, Status> {
            Ok(self.update_sender.subscribe())
        }
    }
    #[tokio::test]
    async fn test_warden_server_startup_and_client_updates() {
        let fake_map_producer = Arc::new(FakeMapProducer::new());
        let to_move = fake_map_producer.clone();

        let warden_server = WardenServer::new(Box::new(move |_, _| to_move.clone()));

        // Start the server in a separate task
        let server_task = tokio::spawn(async move {
            let addr = "[::1]:50052".parse().unwrap();
            tonic::transport::Server::builder()
                .add_service(proto::warden::warden_server::WardenServer::new(
                    warden_server,
                ))
                .serve(addr)
                .await
                .unwrap();
        });

        // Give the server a moment to start up
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Create a client and connect to the server
        let channel = Channel::from_static("http://[::1]:50052")
            .connect()
            .await
            .unwrap();
        let mut client = WardenClient::new(channel);

        // Call register_range_server
        let request = tonic::Request::new(RegisterRangeServerRequest {
            keyspace_id: uuid::Uuid::new_v4().to_string(),
            range_server: Some(proto::warden::HostInfo {
                identity: "test_server".to_string(),
                zone: "test_zone".to_string(),
            }),
        });
        let response = client.register_range_server(request).await.unwrap();
        let mut stream = response.into_inner();

        fake_map_producer.send_full_update();

        // Verify that the client receives the full update
        let received_update = stream.message().await.unwrap().unwrap();
        assert_eq!(received_update, fake_map_producer.full_update);

        fake_map_producer.send_incremental_update();
        let received_incremental_update = stream.message().await.unwrap().unwrap();
        assert_eq!(
            received_incremental_update,
            fake_map_producer.incremental_update
        );

        // Clean up
        server_task.abort();
    }
}
