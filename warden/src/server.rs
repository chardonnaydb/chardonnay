use std::{
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use proto::warden::{warden_server::Warden, RegisterRangeServerRequest, WardenUpdate};
use tokio::sync::broadcast;
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream},
    Stream,
};
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument};

/// Implementation of the Warden service.
///
/// It receives new assignment versions via a broadcast channel and uses a callback function
/// to get the full or delta assignment updates. It sends the updates to the clients via the
/// `register_range_server` streaming gRPC method.
pub struct WardenServer {
    // TODO(purujit): Replace these two fields with a MapProducer trait that exposes these members.
    pub update_sender: broadcast::Sender<i64>,
    update_callback: fn(i64, bool) -> Option<WardenUpdate>,
}

#[tonic::async_trait]
impl Warden for WardenServer {
    type RegisterRangeServerStream = AssignmentUpdateStream;

    #[instrument(skip(self))]
    async fn register_range_server(
        &self,
        request: Request<RegisterRangeServerRequest>,
    ) -> Result<Response<Self::RegisterRangeServerStream>, Status> {
        debug!("Got a register_range_server request: {:?}", request);

        Ok(Response::new(AssignmentUpdateStream::new(
            self.update_sender.subscribe(),
            self.update_callback,
        )))
    }
}

impl WardenServer {
    pub fn new(update_callback: fn(i64, bool) -> Option<WardenUpdate>) -> Self {
        let (full_tx, _full_rx) = broadcast::channel(100);
        Self {
            update_sender: full_tx,
            update_callback,
        }
    }
}

/// Runs the Warden server, listening on the provided address and using the given update callback
/// function to generate updates for registered range servers.
///
/// # Arguments
/// - `addr`: The address for the Warden server to listen on.
/// - `update_callback`: A function that takes the current assignment version and a boolean indicating
///   whether a full or delta update is needed, and returns an optional `WardenUpdate` message.
///
/// # Errors
/// This function will return an error if the provided address is invalid or if there is an issue
/// starting the Warden server.
pub async fn run_warden_server(
    addr: impl AsRef<str> + std::net::ToSocketAddrs,
    update_callback: fn(i64, bool) -> Option<WardenUpdate>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.to_socket_addrs()?.next().ok_or("Invalid address")?;
    let warden_server = WardenServer::new(update_callback);

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
pub struct AssignmentUpdateStream {
    sent_full_update: bool,
    update_stream: BroadcastStream<i64>,
    update_callback: fn(i64, bool) -> Option<WardenUpdate>,
}
}

impl AssignmentUpdateStream {
    pub fn new(
        version_update_receiver: broadcast::Receiver<i64>,
        update_callback: fn(i64, bool) -> Option<WardenUpdate>,
    ) -> Self {
        AssignmentUpdateStream {
            sent_full_update: false,
            update_stream: version_update_receiver.into(),
            update_callback,
        }
    }
}

impl Stream for AssignmentUpdateStream {
    type Item = Result<WardenUpdate, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pinned = std::pin::pin!(self.as_mut().project().update_stream);
        match pinned.poll_next(cx) {
            Poll::Ready(update) => match update {
                Some(result) => match result {
                    Ok(version) => {
                        let update = (self.as_mut().project().update_callback)(
                            version,
                            !self.sent_full_update,
                        );
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
    use tonic::transport::Channel;

    struct FakeMapProducer {
        full_update: WardenUpdate,
        incremental_update: WardenUpdate,
    }

    impl FakeMapProducer {
        pub fn new() -> Self {
            Self {
                full_update: WardenUpdate {
                    update: Some(proto::warden::warden_update::Update::FullAssignment(
                        proto::warden::FullAssignment {
                            version: 1,
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
                            version: 2,
                            previous_version: 1,
                            load: vec![proto::warden::RangeId {
                                keyspace_id: "test_keyspace".to_string(),
                                range_id: "test_range".to_string(),
                            }],
                            unload: vec![],
                        },
                    )),
                },
            }
        }

        pub fn get_assignment(self, _version: i64, full_update: bool) -> Option<WardenUpdate> {
            if full_update {
                Some(self.full_update)
            } else {
                Some(self.incremental_update)
            }
        }
    }
    #[tokio::test]
    async fn test_warden_server_startup_and_client_updates() {
        // Set up the WardenServer
        let warden_server = WardenServer::new(|version, full_update| {
            let fake_map_producer = FakeMapProducer::new();
            fake_map_producer.get_assignment(version, full_update)
        });

        let update_sender = warden_server.update_sender.clone();

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
            range_server: Some(proto::warden::HostInfo {
                identity: "test_server".to_string(),
                zone: "test_zone".to_string(),
            }),
        });
        let response = client.register_range_server(request).await.unwrap();
        let mut stream = response.into_inner();

        update_sender.send(1).unwrap();
        update_sender.send(2).unwrap();

        // Verify that the client receives the full update
        let received_update = stream.message().await.unwrap().unwrap();
        let fake_map_producer = FakeMapProducer::new();
        assert_eq!(received_update, fake_map_producer.full_update);

        let received_incremental_update = stream.message().await.unwrap().unwrap();
        assert_eq!(
            received_incremental_update,
            fake_map_producer.incremental_update
        );

        // Clean up
        server_task.abort();
    }
}
