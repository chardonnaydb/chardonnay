use common::config::Config;
use common::config::EpochPublisher as EpochPublisherConfig;
use flatbuffers::FlatBufferBuilder;
use proto::epoch::epoch_client::EpochClient;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::warn;

use bytes::Bytes;
use common::network::fast_network::FastNetwork;
use proto::epoch_publisher::epoch_publisher_server::{EpochPublisher, EpochPublisherServer};
use proto::epoch_publisher::{SetEpochRequest, SetEpochResponse};
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server as TServer, Request, Response, Status as TStatus};
use tracing::{error, info, instrument, trace};

use flatbuf::epoch_publisher_flatbuffers::epoch_publisher::*;

type DynamicErr = Box<dyn std::error::Error + Sync + Send + 'static>;

pub struct Server {
    epoch: AtomicUsize,
    config: Config,
    publisher_config: EpochPublisherConfig,
    bg_runtime: tokio::runtime::Handle,
}

struct ProtoServer {
    server: Arc<Server>,
}

#[tonic::async_trait]
impl EpochPublisher for ProtoServer {
    #[instrument(skip(self))]
    async fn set_epoch(
        &self,
        request: Request<SetEpochRequest>,
    ) -> Result<Response<SetEpochResponse>, TStatus> {
        warn!("Setting epoch");
        let reply = SetEpochResponse {};
        let current_epoch = self.server.epoch.load(SeqCst);
        if current_epoch == 0 {
            // Can't accept the RPC, since we don't know if it is stale.
            // This should never happen since we always sync the epoch before
            // enabling the server, but keeping the check defensively anyway.
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
    #[instrument(skip(self, network))]
    async fn read_epoch(
        &self,
        network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        request: ReadEpochRequest<'_>,
    ) -> Result<(), DynamicErr> {
        trace!("received read_epoch");
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
            _ => error!("Received a message of an unknown type: {:#?}", envelope),
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
        network_receiver: tokio::sync::mpsc::UnboundedReceiver<(SocketAddr, Bytes)>,
        cancellation_token: CancellationToken,
    ) {
        let mut network_receiver = network_receiver;
        loop {
            let () = tokio::select! {
                () = cancellation_token.cancelled() => {
                    return ()
                }
                maybe_message = network_receiver.recv() => {
                    match maybe_message {
                        None => {
                            error!("fast network closed unexpectedly!");
                            cancellation_token.cancel()
                        }
                        Some((sender, msg)) => {
                            let server = server.clone();
                            let fast_network = fast_network.clone();
                            tokio::spawn(async move{
                                if let Err(e) = Self::handle_message(server, fast_network, sender, msg).await {
                                    error!("error handling a network message: {}", e);
                                }
                            });

                        }
                    }
                }
            };
        }
    }

    async fn initial_epoch_sync(&self) {
        info!("initial epoch sync");
        // TODO: catch retryable errors and reconnect to epoch.
        let addr = format!("http://{}", self.config.epoch.proto_server_addr.clone());

        loop {
            match EpochClient::connect(addr.clone()).await {
                Ok(mut client) => {
                    let request = proto::epoch::ReadEpochRequest {};
                    match client.read_epoch(Request::new(request)).await {
                        Ok(response) => {
                            let epoch = response.into_inner().epoch as usize;
                            self.epoch.store(epoch, SeqCst);
                            break;
                        }
                        Err(e) => {
                            error!("failed to read epoch from epoch server: {}", e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
                Err(e) => {
                    error!("failed to connect to epoch server: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
        warn!("synced initial epoch");
    }

    pub fn new(
        config: Config,
        publisher_config: EpochPublisherConfig,
        bg_runtime: tokio::runtime::Handle,
    ) -> Arc<Server> {
        Arc::new(Server {
            epoch: AtomicUsize::new(0),
            bg_runtime,
            config,
            publisher_config,
        })
    }

    pub async fn start(
        server: Arc<Server>,
        fast_network: Arc<dyn FastNetwork>,
        runtime: tokio::runtime::Handle,
        cancellation_token: CancellationToken,
    ) {
        server.initial_epoch_sync().await;
        let ct_clone = cancellation_token.clone();
        let server_clone = server.clone();
        let (listener_tx, listener_rx) = oneshot::channel();
        runtime.spawn(async move {
            let network_receiver = fast_network.listen_default();
            info!("Listening to fast network");
            listener_tx.send(()).unwrap();
            let _ =
                Self::network_server_loop(server_clone, fast_network, network_receiver, ct_clone)
                    .await;
            info!("Network server loop exited!")
        });
        listener_rx.await.unwrap();
        let proto_server = ProtoServer {
            server: server.clone(),
        };
        let addr = server
            .publisher_config
            .backend_addr
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();
        server.bg_runtime.spawn(async move {
            // TODO(tamer): make this configurable.
            if let Err(e) = TServer::builder()
                .add_service(EpochPublisherServer::new(proto_server))
                .serve(addr)
                .await
            {
                panic!("Unable to start proto server: {}", e);
            }
        });
    }
}
