use flatbuffers::FlatBufferBuilder;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use bytes::Bytes;
use common::network::fast_network::FastNetwork;
use proto::epoch_publisher::epoch_publisher_server::{EpochPublisher, EpochPublisherServer};
use proto::epoch_publisher::{SetEpochRequest, SetEpochResponse};
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server as TServer, Request, Response, Status as TStatus};

use flatbuf::epoch_publisher_flatbuffers::epoch_publisher::*;

type DynamicErr = Box<dyn std::error::Error + Sync + Send + 'static>;

pub struct Server {
    epoch: AtomicUsize,
    bg_runtime: tokio::runtime::Handle,
}

struct ProtoServer {
    server: Arc<Server>,
}

#[tonic::async_trait]
impl EpochPublisher for ProtoServer {
    async fn set_epoch(
        &self,
        request: Request<SetEpochRequest>,
    ) -> Result<Response<SetEpochResponse>, TStatus> {
        let reply = SetEpochResponse {};
        let current_epoch = self.server.epoch.load(SeqCst);
        if current_epoch == 0 {
            // Can't accept the RPC, since we don't know if it is stale.
            // TODO(tamer): on startup, the publisher should sync with
            // the epoch service and read the latest epoch.
            return Err(TStatus::new(
                tonic::Code::FailedPrecondition,
                "Epoch not yet initialized",
            ));
        }
        let new_epoch = request.get_ref().epoch as usize;
        if new_epoch > current_epoch {
            let _ = self
                .server
                .epoch
                .compare_exchange(current_epoch, new_epoch, SeqCst, SeqCst);
        }
        Ok(Response::new(reply))
    }
}

impl Server {
    async fn read_epoch(
        &self,
        network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        request: ReadEpochRequest<'_>,
    ) -> Result<(), DynamicErr> {
        let mut fbb = FlatBufferBuilder::new();
        let fbb_root = match request.request_id() {
            None => ReadEpochResponse::create(
                &mut fbb,
                &ReadEpochResponseArgs {
                    request_id: None,
                    status: Status::InvalidRequestFormat,
                    epoch: 0,
                },
            ),
            Some(request_id) => {
                let epoch = self.epoch.load(SeqCst);
                let status = if epoch <= 0 {
                    Status::EpochUnknown
                } else {
                    Status::Ok
                };
                let request_id = Some(Uuidu128::create(
                    &mut fbb,
                    &Uuidu128Args {
                        lower: request_id.lower(),
                        upper: request_id.upper(),
                    },
                ));
                ReadEpochResponse::create(
                    &mut fbb,
                    &ReadEpochResponseArgs {
                        request_id,
                        status,
                        epoch: epoch as u64,
                    },
                )
            }
        };

        fbb.finish(fbb_root, None);
        self.send_response(network, sender, MessageType::ReadEpoch, fbb.finished_data())?;
        Ok(())
    }

    async fn handle_message(
        server: Arc<Self>,
        fast_network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        msg: Bytes,
    ) -> Result<(), DynamicErr> {
        // TODO: gracefully handle malformed messages instead of unwrapping and crashing.
        let msg = msg.to_vec();
        let envelope = flatbuffers::root::<RequestEnvelope>(msg.as_slice())?;
        match envelope.type_() {
            MessageType::ReadEpoch => {
                let req = flatbuffers::root::<ReadEpochRequest>(envelope.bytes().unwrap().bytes())?;
                server.read_epoch(fast_network.clone(), sender, req).await?
            }
            _ => (), // TODO: return and log unknown message type error.
        };
        Ok(())
    }

    fn send_response(
        &self,
        fast_network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        msg_type: MessageType,
        msg_payload: &[u8],
    ) -> Result<(), std::io::Error> {
        // TODO: many allocations and copies in this function.
        let mut fbb = FlatBufferBuilder::new();
        let bytes = fbb.create_vector(msg_payload);
        let fb_root = ResponseEnvelope::create(
            &mut fbb,
            &ResponseEnvelopeArgs {
                type_: msg_type,
                bytes: Some(bytes),
            },
        );
        fbb.finish(fb_root, None);
        let response = Bytes::copy_from_slice(fbb.finished_data());
        fast_network.send(sender, response)
    }

    async fn network_server_loop(
        server: Arc<Self>,
        fast_network: Arc<dyn FastNetwork>,
        cancellation_token: CancellationToken,
    ) {
        let mut network_receiver = fast_network.listen_default();
        loop {
            let () = tokio::select! {
                () = cancellation_token.cancelled() => {
                    return ()
                }
                maybe_message = network_receiver.recv() => {
                    match maybe_message {
                        None => {
                            println!("fast network closed unexpectedly!");
                            cancellation_token.cancel()
                        }
                        Some((sender, msg)) => {
                            let server = server.clone();
                            let fast_network = fast_network.clone();
                            tokio::spawn(async move{
                                let _ = Self::handle_message(server, fast_network, sender, msg).await;
                                // TODO log any error here.
                            });

                        }
                    }
                }
            };
        }
    }

    pub fn new(bg_runtime: tokio::runtime::Handle) -> Arc<Server> {
        Arc::new(Server {
            epoch: AtomicUsize::new(0),
            bg_runtime,
        })
    }

    pub fn start(
        server: Arc<Server>,
        fast_network: Arc<dyn FastNetwork>,
        cancellation_token: CancellationToken,
    ) {
        let proto_server = ProtoServer {
            server: server.clone(),
        };
        server.bg_runtime.spawn(async move {
            // TODO(tamer): make this configurable.
            let addr = SocketAddr::from_str("127.0.0.1:10010").unwrap();
            if let Err(e) = TServer::builder()
                .add_service(EpochPublisherServer::new(proto_server))
                .serve(addr)
                .await
            {
                println!("Unable to start proto server: {}", e);
            }
        });

        tokio::spawn(async move {
            let _ = Self::network_server_loop(server, fast_network, cancellation_token).await;
            println!("Network server loop exited!")
        });
    }
}
