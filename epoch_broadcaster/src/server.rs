use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use proto::epoch_broadcaster::epoch_broadcaster_server::{
    EpochBroadcaster, EpochBroadcasterServer,
};
use proto::epoch_broadcaster::{SetEpochRequest, SetEpochResponse};
use tonic::{transport::Server as TServer, Request, Response, Status as TStatus};

pub struct Server {
    epoch: AtomicUsize,
    bg_runtime: tokio::runtime::Handle,
}

struct ProtoServer {
    server: Arc<Server>,
}

#[tonic::async_trait]
impl EpochBroadcaster for ProtoServer {
    async fn set_epoch(
        &self,
        request: Request<SetEpochRequest>,
    ) -> Result<Response<SetEpochResponse>, TStatus> {
        let current_epoch = self.server.epoch.load(SeqCst);
        let new_epoch = request.get_ref().epoch as usize;
        if new_epoch > current_epoch {
            let _ = self
                .server
                .epoch
                .compare_exchange(current_epoch, new_epoch, SeqCst, SeqCst);
        }
        let reply = SetEpochResponse {};

        Ok(Response::new(reply))
    }
}

pub fn new(bg_runtime: tokio::runtime::Handle) -> Arc<Server> {
    Arc::new(Server {
        epoch: AtomicUsize::new(0),
        bg_runtime,
    })
}

pub fn start(server: Arc<Server>) {
    let proto_server = ProtoServer {
        server: server.clone(),
    };
    server.bg_runtime.spawn(async move {
        // TODO(tamer): make this configurable.
        let addr = SocketAddr::from_str("127.0.0.1:10010").unwrap();
        if let Err(e) = TServer::builder()
            .add_service(EpochBroadcasterServer::new(proto_server))
            .serve(addr)
            .await
        {
            println!("Unable to start proto server: {}", e);
        }
    });
}
