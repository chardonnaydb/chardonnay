use std::ops::Deref;

use common::{config::Config, host_info::HostInfo};
use proto::warden::warden_client::WardenClient;
use std::ops::DerefMut;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tonic::Request;

type warden_err = Box<dyn std::error::Error + Sync + Send + 'static>;
struct StartedState {
    stopper: CancellationToken,
}

enum State {
    NotStarted,
    Started(StartedState),
    Stopped,
}

struct WardenHandler {
    state: RwLock<State>,
    config: Config,
    host_info: HostInfo,
}

impl WardenHandler {
    async fn continuously_connect_and_register(
        host_info: HostInfo,
        config: common::config::RegionConfig,
        stop: CancellationToken,
    ) -> Result<(), warden_err> {
        loop {
            // TODO: catch retryable errors and reconnect to warden.
            let mut client = WardenClient::connect(config.warden_address.clone()).await?;
            let registration_request = proto::warden::RegisterRangeServerRequest {
                range_server: Some(proto::warden::HostInfo {
                    identity: host_info.identity.clone(),
                    zone: host_info.zone.name.clone(),
                }),
            };

            let mut stream = client
                .register_range_server(Request::new(registration_request))
                .await?
                .into_inner();

            loop {
                tokio::select! {
                    maybe_update = stream.message() => {
                        let maybe_update = maybe_update?;
                        if let None = maybe_update {
                            return Err("connection closed with warden!".into());
                        }
                        // TODO: handle updates and provide a callback mechanism.
                    }
                    () = stop.cancelled() => return Ok(())
                }
            }
        }
    }

    // If this starts correctly, returns a channel receiver that can be used to determine when
    // the warden_handler has stopped. Otherwise returns an error.
    pub async fn start(&self) -> Result<oneshot::Receiver<Result<(), warden_err>>, warden_err> {
        let mut state = self.state.write().await;
        if let State::NotStarted = state.deref_mut() {
            match self.config.regions.get(&self.host_info.zone.region) {
                None => return Err("unknown region!".into()),
                Some(config) => {
                    let (done_tx, done_rx) = oneshot::channel::<Result<(), warden_err>>();
                    let host_info = self.host_info.clone();
                    let config = config.clone();
                    let stop = CancellationToken::new();
                    let stop_clone = stop.clone();
                    *state = State::Started(StartedState { stopper: stop });
                    drop(state);
                    let task_result = tokio::spawn(async move {
                        Self::continuously_connect_and_register(
                            host_info,
                            config.clone(),
                            stop_clone,
                        )
                        .await
                    })
                    .await
                    .unwrap();
                    done_tx.send(task_result).unwrap();
                    Ok(done_rx)
                }
            }
        } else {
            Err("warden_handler can only be started once".into())
        }
    }

    pub async fn stop(self) {
        let mut state = self.state.write().await;
        let old_state = std::mem::replace(&mut *state, State::Stopped);
        if let State::Started(s) = old_state {
            s.stopper.cancel();
        }
    }
}
