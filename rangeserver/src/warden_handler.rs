use std::collections::HashSet;
use std::str::FromStr;

use common::full_range_id::FullRangeId;
use common::keyspace_id::KeyspaceId;
use common::{config::Config, host_info::HostInfo};
use proto::warden::warden_client::WardenClient;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use uuid::Uuid;

use crate::epoch_supplier::EpochSupplier;

type WardenErr = Box<dyn std::error::Error + Sync + Send + 'static>;
struct StartedState {
    stopper: CancellationToken,
    assigned_ranges: RwLock<HashSet<FullRangeId>>,
}

enum State {
    NotStarted,
    Started(Arc<StartedState>),
    Stopped,
}

pub enum WardenUpdate {
    LoadRange(FullRangeId),
    UnloadRange(FullRangeId),
}

pub struct WardenHandler {
    state: RwLock<State>,
    config: Config,
    host_info: HostInfo,
    epoch_supplier: Arc<dyn EpochSupplier>,
}

impl WardenHandler {
    pub fn new(
        config: &Config,
        host_info: &HostInfo,
        epoch_supplier: Arc<dyn EpochSupplier>,
    ) -> WardenHandler {
        WardenHandler {
            state: RwLock::new(State::NotStarted),
            config: config.clone(),
            host_info: host_info.clone(),
            epoch_supplier,
        }
    }

    fn full_range_id_from_proto(proto_range_id: &proto::warden::RangeId) -> FullRangeId {
        let keyspace_id = Uuid::from_str(proto_range_id.keyspace_id.as_str()).unwrap();
        let keyspace_id = KeyspaceId::new(keyspace_id);
        let range_id = Uuid::from_str(proto_range_id.range_id.as_str()).unwrap();
        FullRangeId {
            keyspace_id,
            range_id,
        }
    }

    fn process_warden_update(
        update: &proto::warden::WardenUpdate,
        updates_sender: &mpsc::UnboundedSender<WardenUpdate>,
        assigned_ranges: &mut HashSet<FullRangeId>,
    ) {
        let update = update.update.as_ref().unwrap();
        match update {
            proto::warden::warden_update::Update::FullAssignment(full_assignment) => {
                let new_assignment: HashSet<FullRangeId> = full_assignment
                    .range
                    .iter()
                    .map(Self::full_range_id_from_proto)
                    .collect();

                // Unload any ranges that are no longer assigned to us.
                for current_range in assigned_ranges.iter() {
                    if !new_assignment.contains(current_range) {
                        updates_sender
                            .send(WardenUpdate::UnloadRange(*current_range))
                            .unwrap();
                    }
                }

                // Load any ranges that got newly assigned to us.
                for assigned_range in &new_assignment {
                    if !assigned_ranges.contains(&assigned_range) {
                        updates_sender
                            .send(WardenUpdate::LoadRange(*assigned_range))
                            .unwrap();
                    }
                }

                assigned_ranges.clear();
                assigned_ranges.clone_from(&new_assignment);
            }
            proto::warden::warden_update::Update::IncrementalAssignment(incremental) => {
                for range_id in &incremental.load {
                    let assigned_range = Self::full_range_id_from_proto(range_id);
                    if !assigned_ranges.contains(&assigned_range) {
                        updates_sender
                            .send(WardenUpdate::LoadRange(assigned_range))
                            .unwrap();
                    }
                    assigned_ranges.insert(assigned_range);
                }

                for range_id in &incremental.unload {
                    let removed_range = Self::full_range_id_from_proto(range_id);
                    if assigned_ranges.contains(&removed_range) {
                        updates_sender
                            .send(WardenUpdate::UnloadRange(removed_range))
                            .unwrap();
                    }
                    assigned_ranges.remove(&removed_range);
                }
            }
        }
    }

    async fn continuously_connect_and_register_inner(
        host_info: HostInfo,
        config: common::config::RegionConfig,
        updates_sender: mpsc::UnboundedSender<WardenUpdate>,
        state: Arc<StartedState>,
        epoch_supplier: Arc<dyn EpochSupplier>,
    ) -> Result<(), WardenErr> {
        let addr = format!("http://{}", config.warden_address.clone());
        let epoch = epoch_supplier.read_epoch().await?;
        let mut client = WardenClient::connect(addr).await?;
        let registration_request = proto::warden::RegisterRangeServerRequest {
            range_server: Some(proto::warden::HostInfo {
                identity: host_info.identity.clone(),
                zone: host_info.zone.name.clone(),
                epoch,
            }),
        };
        let mut stream = client
            .register_range_server(Request::new(registration_request))
            .await?
            .into_inner();

        loop {
            tokio::select! {
                () = state.stopper.cancelled() => return Ok(()),
                maybe_update = stream.message() => {
                    let maybe_update = maybe_update?;
                    match maybe_update {
                        None => {
                            return Err("connection closed with warden!".into())
                        }
                        Some(update) => {
                            let mut assigned_ranges_lock = state.assigned_ranges.write().await;
                            Self::process_warden_update(&update, &updates_sender, assigned_ranges_lock.deref_mut())
                        }
                    }
                }

            }
        }
    }

    async fn continuously_connect_and_register(
        host_info: HostInfo,
        config: common::config::RegionConfig,
        updates_sender: mpsc::UnboundedSender<WardenUpdate>,
        state: Arc<StartedState>,
        epoch_supplier: Arc<dyn EpochSupplier>,
    ) -> Result<(), WardenErr> {
        loop {
            match Self::continuously_connect_and_register_inner(
                host_info.clone(),
                config.clone(),
                updates_sender.clone(),
                state.clone(),
                epoch_supplier.clone(),
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    println!("Warden Loop Error: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }
    }

    // If this starts correctly, returns a channel receiver that can be used to determine when
    // the warden_handler has stopped. Otherwise returns an error.
    pub async fn start(
        &self,
        updates_sender: mpsc::UnboundedSender<WardenUpdate>,
    ) -> Result<oneshot::Receiver<Result<(), WardenErr>>, WardenErr> {
        let mut state = self.state.write().await;
        if let State::NotStarted = state.deref_mut() {
            match self.config.regions.get(&self.host_info.zone.region) {
                None => return Err("unknown region!".into()),
                Some(config) => {
                    let (done_tx, done_rx) = oneshot::channel::<Result<(), WardenErr>>();
                    let host_info = self.host_info.clone();
                    let config = config.clone();
                    let stop = CancellationToken::new();
                    let started_state = Arc::new(StartedState {
                        stopper: stop,
                        assigned_ranges: RwLock::new(HashSet::new()),
                    });
                    *state = State::Started(started_state.clone());
                    drop(state);
                    let ec_clone = self.epoch_supplier.clone();
                    let _ = tokio::spawn(async move {
                        let exit_result = Self::continuously_connect_and_register(
                            host_info,
                            config.clone(),
                            updates_sender,
                            started_state,
                            ec_clone,
                        )
                        .await;
                        done_tx.send(exit_result).unwrap();
                    });
                    Ok(done_rx)
                }
            }
        } else {
            Err("warden_handler can only be started once".into())
        }
    }

    pub async fn stop(&self) {
        let mut state = self.state.write().await;
        let old_state = std::mem::replace(&mut *state, State::Stopped);
        if let State::Started(s) = old_state {
            s.stopper.cancel();
        }
    }

    pub async fn is_assigned(&self, range_id: &FullRangeId) -> bool {
        let state = self.state.read().await;
        match state.deref() {
            State::NotStarted => false,
            State::Stopped => false,
            State::Started(state) => {
                let assigned_ranges = state.assigned_ranges.read().await;
                (*assigned_ranges).contains(range_id)
            }
        }
    }
}
