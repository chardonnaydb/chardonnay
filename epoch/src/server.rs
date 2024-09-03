use std::net::SocketAddr;
use std::sync::Arc;

use crate::publisher_set_updater::PublisherSetUpdater;
use crate::storage::Storage;
use common::config::Config;
use proto::epoch::epoch_server::{Epoch, EpochServer};
use proto::epoch::{ReadEpochRequest, ReadEpochResponse};
use std::str::FromStr;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server as TServer, Request, Response, Status as TStatus};

pub struct Server<S>
where
    S: Storage,
{
    storage: S,
    publisher_sets: Vec<Arc<PublisherSetUpdater>>,
}

struct ProtoServer<S>
where
    S: Storage,
{
    server: Arc<Server<S>>,
}

#[tonic::async_trait]
impl<S> Epoch for ProtoServer<S>
where
    S: Storage,
{
    async fn read_epoch(
        &self,
        _request: Request<ReadEpochRequest>,
    ) -> Result<Response<ReadEpochResponse>, TStatus> {
        match self.server.storage.read_latest().await {
            Err(_) => {
                // TODO: log error.
                let status = TStatus::new(tonic::Code::Internal, "Failed to read latest epoch");
                Err(status)
            }
            Ok(epoch) => {
                let reply = ReadEpochResponse { epoch };
                Ok(Response::new(reply))
            }
        }
    }
}

impl<S> Server<S>
where
    S: Storage,
{
    pub fn new(storage: S, config: Config) -> Server<S> {
        let publisher_sets: Vec<Arc<PublisherSetUpdater>> = config
            .regions
            .values()
            .map(|rc| {
                let updaters: Vec<Arc<PublisherSetUpdater>> = rc
                    .epoch_publishers
                    .iter()
                    .map(|set| Arc::new(PublisherSetUpdater::new(set.clone())))
                    .collect();
                updaters
            })
            .flatten()
            .collect();
        Server {
            storage,
            publisher_sets,
        }
    }

    pub async fn start(server: Arc<Server<S>>, cancellation_token: CancellationToken) {
        // TODO: log errors
        server.storage.initialize_epoch().await.unwrap();
        let proto_server = ProtoServer {
            server: server.clone(),
        };
        // TODO(tamer): make this configurable.
        let addr = SocketAddr::from_str("127.0.0.1:10015").unwrap();
        if let Err(e) = TServer::builder()
            .add_service(EpochServer::new(proto_server))
            .serve(addr)
            .await
        {
            panic!("Unable to start proto server: {}", e);
        }
        Server::<S>::start_update_loop(server, cancellation_token).await;
    }

    async fn start_update_loop(server: Arc<Server<S>>, cancellation_token: CancellationToken) {
        tokio::spawn(async move {
            'outer: loop {
                if cancellation_token.is_cancelled() {
                    return ();
                }
                let read_result = server.storage.read_latest().await;
                let epoch = match read_result {
                    Err(_) => continue, // TODO: log error
                    Ok(epoch) => epoch,
                };
                // Fire off a request to update the epoch at each of the sets in parallel.
                let mut join_set = JoinSet::new();
                for s in server.publisher_sets.iter() {
                    let s = s.clone();
                    join_set.spawn(async move { s.update_epoch(epoch).await });
                }

                // If all succeeded, we can advance the epoch.
                while let Some(res) = join_set.join_next().await {
                    let res = match res {
                        Err(_) => continue 'outer, // TODO: maybe log the error here
                        Ok(res) => res,
                    };
                    match res {
                        Err(_) => continue 'outer, // TODO: maybe log the error here
                        Ok(()) => (),
                    };
                }
                // If we got here then we updated all the broadcaster sets, we can advance the epoch.
                // TODO: log the error if any
                let _ = server.storage.conditional_update(epoch + 1, epoch).await;
            }
        });
    }
}
