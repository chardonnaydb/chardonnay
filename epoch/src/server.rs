use std::net::SocketAddr;
use std::sync::Arc;

use crate::storage::Storage;
use proto::epoch::epoch_server::{Epoch, EpochServer};
use proto::epoch::{ReadEpochRequest, ReadEpochResponse};
use std::str::FromStr;
use tonic::{transport::Server as TServer, Request, Response, Status as TStatus};

pub struct Server<S>
where
    S: Storage,
{
    storage: S,
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
    pub fn new(storage: S) -> Server<S> {
        Server { storage }
    }

    pub async fn start(server: Arc<Server<S>>) {
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
    }
}
