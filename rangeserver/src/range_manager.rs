use crate::{
    epoch_provider::EpochProvider, persistence::Persistence, persistence::RangeInfo, wal::Wal,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

enum Error {
    Unknown,
}

struct LoadedState {
    range_info: RangeInfo,
    highest_known_epoch: u64,
}

enum State {
    Unloaded,
    Loading,
    Loaded(LoadedState),
}

struct RangeManager<P, E, W>
where
    P: Persistence,
    E: EpochProvider,
    W: Wal,
{
    persistence: Arc<P>,
    epoch_provider: Arc<E>,
    wal: W,
    range_id: Uuid,
    state: RwLock<State>,
}

impl<P, E, W> RangeManager<P, E, W>
where
    P: Persistence,
    E: EpochProvider,
    W: Wal,
{
    pub fn new(range_id: Uuid, persistence: Arc<P>, epoch_provider: Arc<E>, wal: W) -> Box<Self> {
        Box::new(RangeManager {
            range_id,
            persistence,
            epoch_provider,
            wal,
            state: RwLock::new(State::Unloaded),
        })
    }

    pub async fn load(&mut self) -> Result<(), Error> {
        let should_load = {
            let mut state = self.state.write().await;
            match *state {
                State::Loading | State::Loaded(_) => false,
                State::Unloaded => {
                    *state = State::Loading;
                    true
                }
            }
        };

        if !should_load {
            Ok(())
        } else {
            // TODO: handle errors and restore state to Unloaded on failures, instead of panicing.
            let range_info = self
                .persistence
                .take_ownership_and_load_range(self.range_id)
                .await
                .unwrap();
            let epoch = self.epoch_provider.read_epoch().await.unwrap();
            // Epoch read from the provider can be 1 less than the true epoch. The highest known epoch
            // of a range cannot move backward even across range load/unloads, so to maintain that guarantee
            // we just wait for the epoch to advance once.
            self.epoch_provider
                .wait_until_epoch(epoch + 1)
                .await
                .unwrap();
            // Get a new epoch lease.
            // TODO: Create a recurrent task to renew.
            let highest_known_epoch = epoch + 1;
            let new_epoch_lease_lower_bound =
                std::cmp::max(highest_known_epoch, range_info.epoch_lease.0 + 1);
            let new_epoch_lease_upper_bound = new_epoch_lease_lower_bound + 10;
            self.persistence
                .renew_epoch_lease(
                    self.range_id,
                    (new_epoch_lease_lower_bound, new_epoch_lease_upper_bound),
                    range_info.leader_sequence_number,
                )
                .await
                .unwrap();
            // TODO: apply WAL here!
            let loaded_state = LoadedState {
                range_info,
                highest_known_epoch,
            };
            let mut state = self.state.write().await;
            *state = State::Loaded(loaded_state);
            Ok(())
        }
    }
}
