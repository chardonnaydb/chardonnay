use std::{net::SocketAddr, sync::Arc};

use proto::epoch::{
    epoch_server::{Epoch, EpochServer},
    ReadEpochRequest, ReadEpochResponse,
};
use tokio::{net::TcpListener, sync::Mutex};
use tonic::{transport::Server, Request, Response, Status};

struct State {
    pub epoch: Mutex<u64>,
}

#[tonic::async_trait]
impl Epoch for State {
    async fn read_epoch(
        &self,
        _request: Request<ReadEpochRequest>,
    ) -> Result<Response<ReadEpochResponse>, Status> {
        let epoch = self.epoch.lock().await;
        let response = ReadEpochResponse { epoch: *epoch };
        Ok(Response::new(response))
    }
}

/// Mock Epoch Service.
pub struct MockEpoch {
    state: Arc<State>,
}

impl MockEpoch {
    pub fn new() -> MockEpoch {
        let state = Arc::new(State {
            epoch: Mutex::new(0),
        });

        MockEpoch { state }
    }

    pub async fn start(&self) -> Result<SocketAddr, Box<dyn std::error::Error>> {
        let state = self.state.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async {
            let svc = EpochServer::from_arc(state);
            let _ = Server::builder()
                .add_service(svc)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await;
        });
        Ok(addr)
    }

    pub async fn set_epoch(&self, epoch: u64) {
        let mut state = self.state.epoch.lock().await;
        *state = epoch
    }
}
