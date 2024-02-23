use crate::{
    epoch_provider::EpochProvider, persistence::Persistence, persistence::RangeInfo,
    transaction_info::TransactionInfo, wal::Wal,
};
use chrono::DateTime;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use uuid::Uuid;

enum Error {
    Unknown,
    WaitDieAbort,
}

type LocalDateTime = DateTime<chrono::Local>;
struct CurrentLockHolder {
    transaction: Arc<TransactionInfo>,
    when_acquired: LocalDateTime,
    when_requested: LocalDateTime,
}

struct LockRequest {
    transaction: Arc<TransactionInfo>,
    sender: oneshot::Sender<()>,
    when_requested: LocalDateTime,
}

struct LockTable {
    current_holder: Option<CurrentLockHolder>,
    waiting_for_release: VecDeque<LockRequest>,
    waiting_to_acquire: VecDeque<LockRequest>,
}

impl LockTable {
    pub fn new() -> LockTable {
        LockTable {
            current_holder: None,
            waiting_for_release: VecDeque::new(),
            waiting_to_acquire: VecDeque::new(),
        }
    }
    pub async fn maybe_wait_for_current_holder(&mut self, tx: Arc<TransactionInfo>) {
        match &self.current_holder {
            None => (),
            Some(_) => {
                let (s, r) = oneshot::channel();
                let req = LockRequest {
                    transaction: tx.clone(),
                    sender: s,
                    when_requested: chrono::Local::now(),
                };
                self.waiting_for_release.push_back(req);
                r.await.unwrap();
            }
        }
    }

    pub async fn acquire(&mut self, tx: Arc<TransactionInfo>) -> Result<(), Error> {
        let when_requested = chrono::Local::now();
        match &self.current_holder {
            None => {
                let holder = CurrentLockHolder {
                    transaction: tx.clone(),
                    when_requested,
                    when_acquired: when_requested,
                };
                self.current_holder = Some(holder);
                Ok(())
            }
            Some(current_holder) => {
                if current_holder.transaction.id == tx.id {
                    Ok(())
                } else {
                    let highest_waiter = self
                        .waiting_to_acquire
                        .back()
                        .map_or(current_holder.transaction.id, |r| r.transaction.id);
                    if highest_waiter > tx.id {
                        // TODO: allow for skipping these checks if locks are ordered!
                        Err(Error::WaitDieAbort)
                    } else {
                        let (s, r) = oneshot::channel();
                        let req = LockRequest {
                            transaction: tx.clone(),
                            sender: s,
                            when_requested: chrono::Local::now(),
                        };
                        self.waiting_to_acquire.push_back(req);
                        r.await.unwrap();
                        Ok(())
                    }
                }
            }
        }
    }

    pub fn release(&mut self) {
        self.current_holder = None;
        while !self.waiting_for_release.is_empty() {
            let req = self.waiting_for_release.pop_front().unwrap();
            req.sender.send(()).unwrap();
        }
        match self.waiting_to_acquire.pop_front() {
            None => (),
            Some(req) => {
                let when_acquired = chrono::Local::now();
                let new_holder = CurrentLockHolder {
                    transaction: req.transaction.clone(),
                    when_requested: req.when_requested,
                    when_acquired,
                };
                self.current_holder = Some(new_holder);
                req.sender.send(()).unwrap();
            }
        }
    }
}

struct LoadedState {
    range_info: RangeInfo,
    highest_known_epoch: u64,
    lock_table: LockTable,
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
                std::cmp::max(highest_known_epoch, range_info.epoch_lease.1 + 1);
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
                lock_table: LockTable::new(),
            };
            let mut state = self.state.write().await;
            *state = State::Loaded(loaded_state);
            Ok(())
        }
    }
}
