use crate::{
    epoch_provider::EpochProvider, error::Error, key_version::KeyVersion, storage::RangeInfo,
    storage::Storage, transaction_abort_reason::TransactionAbortReason,
    transaction_info::TransactionInfo, wal::Wal,
};
use bytes::Bytes;
use chrono::DateTime;
use common::config::Config;
use common::full_range_id::FullRangeId;

use flatbuf::rangeserver_flatbuffers::range_server::*;
use std::collections::VecDeque;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

type UtcDateTime = DateTime<chrono::Utc>;
struct CurrentLockHolder {
    transaction: Arc<TransactionInfo>,
    when_acquired: UtcDateTime,
    when_requested: UtcDateTime,
}

struct LockRequest {
    transaction: Arc<TransactionInfo>,
    sender: oneshot::Sender<()>,
    when_requested: UtcDateTime,
}

// Implements transaction lock table for the range.
// Currently there is just a single lock for the entire range despite having
// "Table" in the name, but we might partition the lock to allow for more
// concurrency down the line.
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
    pub fn maybe_wait_for_current_holder(
        &mut self,
        tx: Arc<TransactionInfo>,
    ) -> oneshot::Receiver<()> {
        let (s, r) = oneshot::channel();
        match &self.current_holder {
            None => s.send(()).unwrap(),
            Some(_) => {
                let req = LockRequest {
                    transaction: tx.clone(),
                    sender: s,
                    when_requested: chrono::Utc::now(),
                };
                self.waiting_for_release.push_back(req);
            }
        };
        r
    }

    pub fn acquire(&mut self, tx: Arc<TransactionInfo>) -> Result<oneshot::Receiver<()>, Error> {
        let when_requested = chrono::Utc::now();
        let (s, r) = oneshot::channel();
        match &self.current_holder {
            None => {
                let holder = CurrentLockHolder {
                    transaction: tx.clone(),
                    when_requested,
                    when_acquired: when_requested,
                };
                self.current_holder = Some(holder);
                s.send(()).unwrap();
                Ok(r)
            }
            Some(current_holder) => {
                if current_holder.transaction.id == tx.id {
                    s.send(()).unwrap();
                    Ok(r)
                } else {
                    let highest_waiter = self
                        .waiting_to_acquire
                        .back()
                        .map_or(current_holder.transaction.id, |r| r.transaction.id);
                    if highest_waiter > tx.id {
                        // TODO: allow for skipping these checks if locks are ordered!
                        Err(Error::TransactionAborted(TransactionAbortReason::WaitDie))
                    } else {
                        let req = LockRequest {
                            transaction: tx.clone(),
                            sender: s,
                            when_requested: chrono::Utc::now(),
                        };
                        self.waiting_to_acquire.push_back(req);
                        Ok(r)
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
                let when_acquired = chrono::Utc::now();
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

    pub fn is_currently_holding(&self, tx: Arc<TransactionInfo>) -> bool {
        match &self.current_holder {
            None => false,
            Some(current) => current.transaction.id == tx.id,
        }
    }
}

struct LoadedState {
    range_info: RangeInfo,
    highest_known_epoch: u64,
    lock_table: Mutex<LockTable>,
    lease_renewal_task: tokio::task::JoinHandle<Result<(), Error>>,
}

enum State {
    Unloaded,
    Loading,
    Loaded(LoadedState),
}

pub struct RangeManager<S, E, W>
where
    S: Storage,
    E: EpochProvider,
    W: Wal,
{
    range_id: FullRangeId,
    config: Config,
    storage: Arc<S>,
    epoch_provider: Arc<E>,
    wal: Mutex<W>,
    state: Arc<RwLock<State>>,
}

pub struct GetResult {
    pub val: Option<Bytes>,
    pub leader_sequence_number: i64,
}

pub struct PrepareResult {
    pub highest_known_epoch: u64,
    pub epoch_lease: (u64, u64),
}

impl<S, E, W> RangeManager<S, E, W>
where
    S: Storage,
    E: EpochProvider,
    W: Wal,
{
    pub fn new(
        range_id: FullRangeId,
        config: Config,
        storage: Arc<S>,
        epoch_provider: Arc<E>,
        wal: W,
    ) -> Arc<Self> {
        Arc::new(RangeManager {
            range_id,
            config,
            storage,
            epoch_provider,
            wal: Mutex::new(wal),
            state: Arc::new(RwLock::new(State::Unloaded)),
        })
    }

    async fn load_inner(&self) -> Result<LoadedState, Error> {
        // TODO: handle all errors instead of panicking.
        let epoch = self
            .epoch_provider
            .read_epoch()
            .await
            .map_err(Error::from_epoch_provider_error)?;
        let range_info = self
            .storage
            .take_ownership_and_load_range(self.range_id)
            .await
            .map_err(Error::from_storage_error)?;
        // Epoch read from the provider can be 1 less than the true epoch. The highest known epoch
        // of a range cannot move backward even across range load/unloads, so to maintain that guarantee
        // we just wait for the epoch to advance once.
        self.epoch_provider
            .wait_until_epoch(epoch + 1)
            .await
            .map_err(Error::from_epoch_provider_error)?;
        // Get a new epoch lease.
        let highest_known_epoch = epoch + 1;
        let new_epoch_lease_lower_bound =
            std::cmp::max(highest_known_epoch, range_info.epoch_lease.1 + 1);
        let new_epoch_lease_upper_bound = new_epoch_lease_lower_bound + 10;
        self.storage
            .renew_epoch_lease(
                self.range_id,
                (new_epoch_lease_lower_bound, new_epoch_lease_upper_bound),
                range_info.leader_sequence_number,
            )
            .await
            .map_err(Error::from_storage_error)?;
        // Create a recurrent task to renew.
        let lease_renewal_interval = self.config.range_server.range_maintenance_duration;
        // TODO: Check on the task handle to see if it errored out.
        let lease_renewal_task = self
            .start_renew_epoch_lease_task(lease_renewal_interval)
            .await;
        // TODO: apply WAL here!
        Ok(LoadedState {
            range_info,
            highest_known_epoch,
            lock_table: Mutex::new(LockTable::new()),
            lease_renewal_task: lease_renewal_task,
        })
    }

    async fn renew_epoch_lease_task(
        range_id: FullRangeId,
        epoch_provider: Arc<E>,
        storage: Arc<S>,
        state: Arc<RwLock<State>>,
        lease_renewal_interval: std::time::Duration,
    ) -> Result<(), Error> {
        loop {
            let leader_sequence_number: u64;
            let old_lease: (u64, u64);
            let epoch = epoch_provider
                .read_epoch()
                .await
                .map_err(Error::from_epoch_provider_error)?;
            let highest_known_epoch = epoch + 1;
            if let State::Loaded(state) = state.read().await.deref() {
                old_lease = state.range_info.epoch_lease;
                leader_sequence_number = state.range_info.leader_sequence_number;
            } else {
                return Err(Error::RangeIsNotLoaded);
            }
            // TODO: If we renew too often, this could get out of hand.
            // We should probably limit the max number of epochs in the future
            // we can request a lease for.
            let new_epoch_lease_lower_bound = std::cmp::max(highest_known_epoch, old_lease.1 + 1);
            let new_epoch_lease_upper_bound = new_epoch_lease_lower_bound + 10;
            // TODO: We should handle some errors here. For example:
            // - If the error seems transient (e.g., a timeout), we should retry.
            // - If the error is something like RangeOwnershipLost, we should unload the range.
            storage
                .renew_epoch_lease(
                    range_id,
                    (new_epoch_lease_lower_bound, new_epoch_lease_upper_bound),
                    leader_sequence_number,
                )
                .await
                .map_err(Error::from_storage_error)?;

            // Update the state.
            // If our new lease continues from our old lease, merge the ranges.
            let mut new_lease = (new_epoch_lease_lower_bound, new_epoch_lease_upper_bound);
            if (new_epoch_lease_lower_bound - old_lease.1) == 1 {
                new_lease = (old_lease.0, new_epoch_lease_upper_bound);
            }
            if let State::Loaded(state) = state.write().await.deref_mut() {
                // This should never happen as only this task changes the epoch lease.
                assert_eq!(
                    state.range_info.epoch_lease, old_lease,
                    "Epoch lease changed by someone else, but only this task should be changing it!"
                );
                state.range_info.epoch_lease = new_lease;
                state.highest_known_epoch =
                    std::cmp::max(state.highest_known_epoch, highest_known_epoch);
            } else {
                return Err(Error::RangeIsNotLoaded);
            }
            // Sleep for a while before renewing the lease again.
            tokio::time::sleep(lease_renewal_interval).await;
        }
    }

    async fn start_renew_epoch_lease_task(
        &self,
        lease_renewal_interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<Result<(), Error>> {
        let range_id = self.range_id;
        let epoch_provider = self.epoch_provider.clone();
        let storage = self.storage.clone();
        let state = self.state.clone();
        let task_handle = tokio::spawn(async move {
            Self::renew_epoch_lease_task(
                range_id,
                epoch_provider,
                storage,
                state,
                lease_renewal_interval,
            )
            .await
        });
        task_handle
    }

    pub async fn load(&self) -> Result<(), Error> {
        {
            let mut state = self.state.write().await;
            match *state {
                State::Loaded(_) => return Ok(()),
                State::Loading => todo!(),
                State::Unloaded => {
                    *state = State::Loading;
                    true
                }
            }
        };
        let load_result: Result<LoadedState, Error> = self.load_inner().await;
        let mut state = self.state.write().await;
        match load_result {
            Err(e) => {
                *state = State::Unloaded;
                Err(e)
            }
            Ok(loaded_state) => {
                *state = State::Loaded(loaded_state);
                Ok(())
            }
        }
    }

    pub async fn unload(&self) {
        let mut state = self.state.write().await;
        *state = State::Unloaded;
    }

    async fn acquire_range_lock(
        &self,
        state: &LoadedState,
        tx: Arc<TransactionInfo>,
    ) -> Result<(), Error> {
        let mut lock_table = state.lock_table.lock().await;
        let receiver = lock_table.acquire(tx.clone())?;
        // TODO: allow timing out locks when transaction timeouts are implemented.
        receiver.await.unwrap();
        Ok(())
    }

    pub async fn get(&self, tx: Arc<TransactionInfo>, key: Bytes) -> Result<GetResult, Error> {
        let s = self.state.write().await;
        match s.deref() {
            State::Unloaded | State::Loading => Err(Error::RangeIsNotLoaded),
            State::Loaded(state) => {
                if !state.range_info.key_range.includes(key.clone()) {
                    return Err(Error::KeyIsOutOfRange);
                };
                self.acquire_range_lock(state, tx.clone()).await?;
                let val = self
                    .storage
                    .get(self.range_id, key.clone())
                    .await
                    .map_err(Error::from_storage_error)?;
                let get_result = GetResult {
                    val: val.clone(),
                    leader_sequence_number: state.range_info.leader_sequence_number as i64,
                };
                Ok(get_result)
            }
        }
    }

    // If prepare ever returns success, we must be able to (eventually) commit the
    // transaction no matter what, unless we get an abort call from the coordinator
    // or know for certain that the transaction aborted.
    // It is possible that prepare gets called multiple times due to retries from the
    // coordinator, so we must be able to handle that.
    pub async fn prepare(
        &self,
        tx: Arc<TransactionInfo>,
        prepare: PrepareRequest<'_>,
    ) -> Result<PrepareResult, Error> {
        let s = self.state.write().await;
        match s.deref() {
            State::Unloaded | State::Loading => return Err(Error::RangeIsNotLoaded),
            State::Loaded(state) => {
                // Sanity check that the written keys are all within this range.
                for put in prepare.puts().iter() {
                    for put in put.iter() {
                        // TODO: too much copying :(
                        let key = Bytes::copy_from_slice(put.key().unwrap().k().unwrap().bytes());
                        if !state.range_info.key_range.includes(key) {
                            return Err(Error::KeyIsOutOfRange);
                        }
                    }
                }
                for del in prepare.deletes().iter() {
                    for del in del.iter() {
                        let key = Bytes::copy_from_slice(del.k().unwrap().bytes());
                        if !state.range_info.key_range.includes(key) {
                            return Err(Error::KeyIsOutOfRange);
                        }
                    }
                }
                // Validate the transaction lock is not lost, this is essential to ensure 2PL
                // invariants still hold.
                {
                    let lock_table = state.lock_table.lock().await;
                    if prepare.has_reads() && !lock_table.is_currently_holding(tx.clone()) {
                        return Err(Error::TransactionAborted(
                            TransactionAbortReason::TransactionLockLost,
                        ));
                    }
                };
                self.acquire_range_lock(state, tx.clone()).await?;
                {
                    let mut wal = self.wal.lock().await;
                    wal.append_prepare(prepare)
                        .await
                        .map_err(Error::from_wal_error)?;
                };

                Ok(PrepareResult {
                    highest_known_epoch: state.highest_known_epoch,
                    epoch_lease: state.range_info.epoch_lease,
                })
            }
        }
    }

    pub async fn abort(
        &self,
        tx: Arc<TransactionInfo>,
        abort: AbortRequest<'_>,
    ) -> Result<(), Error> {
        let s = self.state.write().await;
        match s.deref() {
            State::Unloaded | State::Loading => return Err(Error::RangeIsNotLoaded),
            State::Loaded(state) => {
                let mut lock_table = state.lock_table.lock().await;
                if !lock_table.is_currently_holding(tx.clone()) {
                    return Ok(());
                }
                {
                    // TODO: We can skip aborting to the log if we never appended a prepare record.
                    let mut wal = self.wal.lock().await;
                    // TODO: It's possible the WAL already contains this record in case this is a retry
                    // so avoid re-inserting in that case.
                    wal.append_abort(abort)
                        .await
                        .map_err(Error::from_wal_error)?;
                }
                lock_table.release();
                Ok(())
            }
        }
    }

    // Commit *informs* the range manager of a transaction commit, it does not
    // decide the transaction outcome.
    // A call to commit can fail only for intermittent reasons, and must be
    // idempotent and safe to retry any number of times.
    pub async fn commit(
        &self,
        tx: Arc<TransactionInfo>,
        commit: CommitRequest<'_>,
    ) -> Result<(), Error> {
        let mut s = self.state.write().await;
        match s.deref_mut() {
            State::Unloaded | State::Loading => return Err(Error::RangeIsNotLoaded),
            State::Loaded(state) => {
                let mut lock_table = state.lock_table.lock().await;
                if !lock_table.is_currently_holding(tx.clone()) {
                    // it must be that we already finished committing, but perhaps the coordinator didn't
                    // realize that, so we just return success.
                    return Ok(());
                }
                state.highest_known_epoch =
                    std::cmp::max(state.highest_known_epoch, commit.epoch() as u64);
                {
                    let mut wal = self.wal.lock().await;
                    wal.append_commit(commit)
                        .await
                        .map_err(Error::from_wal_error)?;
                    // Find the corresponding prepare entry in the WAL to get the writes.
                    // This is quite inefficient, we should cache a copy in memory instead, but for now
                    // it's convenient to also test WAL iteration.
                    let prepare_record_bytes = wal
                        .find_prepare_record(tx.id.clone())
                        .await
                        .map_err(Error::from_wal_error)?
                        .unwrap();
                    let prepare_record =
                        flatbuffers::root::<PrepareRequest>(&prepare_record_bytes).unwrap();
                    let version = KeyVersion {
                        epoch: commit.epoch() as u64,
                        // TODO: version counter should be an internal counter per range.
                        // Remove from the commit message.
                        version_counter: commit.vid() as u64,
                    };
                    // TODO: we shouldn't be doing a storage operation per individual key put or delete.
                    // Instead we should write them in batches, and whenever we do multiple operations they
                    // should go in parallel not sequentially.
                    // We should also add retries in case of intermittent failures. Note that all our
                    // storage operations here are idempotent and safe to retry any number of times.
                    for put in prepare_record.puts().iter() {
                        for put in put.iter() {
                            // TODO: too much copying :(
                            let key =
                                Bytes::copy_from_slice(put.key().unwrap().k().unwrap().bytes());
                            let val = Bytes::copy_from_slice(put.value().unwrap().bytes());

                            self.storage
                                .upsert(self.range_id, key, val, version)
                                .await
                                .map_err(Error::from_storage_error)?;
                        }
                    }
                    for del in prepare_record.deletes().iter() {
                        for del in del.iter() {
                            let key = Bytes::copy_from_slice(del.k().unwrap().bytes());
                            self.storage
                                .delete(self.range_id, key, version)
                                .await
                                .map_err(Error::from_storage_error)?;
                        }
                    }
                }

                // We apply the writes to storage before releasing the lock since we send all
                // gets to storage directly. We should implement a memtable to allow us to release
                // the lock sooner.
                lock_table.release();
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use common::config::RangeServerConfig;
    use common::util;
    use core::time;
    use flatbuffers::FlatBufferBuilder;
    use uuid::Uuid;

    use super::*;
    use crate::epoch_provider::EpochProvider as EpochProviderTrait;
    use crate::for_testing::epoch_provider::EpochProvider;
    use crate::for_testing::in_memory_wal::InMemoryWal;
    use crate::storage::cassandra::Cassandra;
    use crate::transaction_info::TransactionInfo;
    type RM = RangeManager<Cassandra, EpochProvider, InMemoryWal>;

    impl RM {
        async fn abort_transaction(&self, tx: Arc<TransactionInfo>) {
            let mut fbb = FlatBufferBuilder::new();
            let transaction_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(tx.id),
            ));
            let range_id = Some(util::flatbuf::serialize_range_id(&mut fbb, &self.range_id));
            let request_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(Uuid::new_v4()),
            ));
            let fbb_root = AbortRequest::create(
                &mut fbb,
                &AbortRequestArgs {
                    request_id,
                    transaction_id,
                    range_id,
                },
            );
            fbb.finish(fbb_root, None);
            let abort_record_bytes = fbb.finished_data();
            let abort_record = flatbuffers::root::<AbortRequest>(abort_record_bytes).unwrap();
            self.abort(tx.clone(), abort_record).await.unwrap()
        }

        async fn prepare_transaction(
            &self,
            tx: Arc<TransactionInfo>,
            writes: Vec<(Bytes, Bytes)>,
            deletes: Vec<Bytes>,
            has_reads: bool,
        ) -> Result<(), Error> {
            let mut fbb = FlatBufferBuilder::new();
            let transaction_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(tx.id),
            ));
            let request_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(Uuid::new_v4()),
            ));
            let mut puts_vector = Vec::new();
            for (k, v) in writes {
                let k = Some(fbb.create_vector(k.to_vec().as_slice()));
                let key = Key::create(&mut fbb, &KeyArgs { k });
                let value = fbb.create_vector(v.to_vec().as_slice());
                puts_vector.push(Record::create(
                    &mut fbb,
                    &RecordArgs {
                        key: Some(key),
                        value: Some(value),
                    },
                ));
            }
            let puts = Some(fbb.create_vector(&puts_vector));
            let mut del_vector = Vec::new();
            for k in deletes {
                let k = Some(fbb.create_vector(k.to_vec().as_slice()));
                let key = Key::create(&mut fbb, &KeyArgs { k });
                del_vector.push(key);
            }
            let deletes = Some(fbb.create_vector(&del_vector));
            let range_id = Some(util::flatbuf::serialize_range_id(&mut fbb, &self.range_id));
            let fbb_root = PrepareRequest::create(
                &mut fbb,
                &PrepareRequestArgs {
                    request_id,
                    transaction_id,
                    range_id,
                    has_reads,
                    puts,
                    deletes,
                },
            );
            fbb.finish(fbb_root, None);
            let prepare_record_bytes = fbb.finished_data();
            let prepare_record = flatbuffers::root::<PrepareRequest>(prepare_record_bytes).unwrap();
            self.prepare(tx.clone(), prepare_record).await.map(|_| ())
        }

        async fn commit_transaction(&self, tx: Arc<TransactionInfo>) -> Result<(), Error> {
            let epoch = self.epoch_provider.read_epoch().await.unwrap();
            let mut fbb = FlatBufferBuilder::new();
            let request_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(Uuid::new_v4()),
            ));
            let transaction_id = Some(Uuidu128::create(
                &mut fbb,
                &util::flatbuf::serialize_uuid(tx.id),
            ));
            let range_id = Some(util::flatbuf::serialize_range_id(&mut fbb, &self.range_id));
            let fbb_root = CommitRequest::create(
                &mut fbb,
                &CommitRequestArgs {
                    request_id,
                    transaction_id,
                    range_id,
                    epoch,
                    vid: 0,
                },
            );
            fbb.finish(fbb_root, None);
            let commit_record_bytes = fbb.finished_data();
            let commit_record = flatbuffers::root::<CommitRequest>(commit_record_bytes).unwrap();
            self.commit(tx.clone(), commit_record).await
        }
    }

    struct TestContext {
        rm: Arc<RM>,
        storage_context: crate::storage::cassandra::tests::TestContext,
    }

    async fn init() -> TestContext {
        let epoch_provider = Arc::new(EpochProvider::new());
        let wal = Mutex::new(InMemoryWal::new());
        let storage_context: crate::storage::cassandra::tests::TestContext =
            crate::storage::cassandra::tests::init().await;
        let cassandra = storage_context.cassandra.clone();
        let range_id = FullRangeId {
            keyspace_id: storage_context.keyspace_id,
            range_id: storage_context.range_id,
        };
        let config = Config {
            range_server: RangeServerConfig {
                range_maintenance_duration: time::Duration::from_secs(1),
            },
            regions: std::collections::HashMap::new(),
        };
        let rm = Arc::new(RM {
            range_id,
            config,
            storage: cassandra,
            wal,
            epoch_provider,
            state: Arc::new(RwLock::new(State::Unloaded)),
        });
        let rm_copy = rm.clone();
        let init_handle = tokio::spawn(async move { rm_copy.load().await.unwrap() });
        let epoch_provider = rm.epoch_provider.clone();
        // Give some delay so the RM can see the epoch advancing.
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        epoch_provider.set_epoch(1).await;
        init_handle.await.unwrap();
        TestContext {
            rm,
            storage_context,
        }
    }

    fn start_transaction() -> Arc<TransactionInfo> {
        Arc::new(TransactionInfo {
            id: Uuid::new_v4(),
            started: chrono::Utc::now(),
            overall_timeout: time::Duration::from_secs(10),
        })
    }

    #[tokio::test]
    async fn basic_get_put() {
        let context = init().await;
        let rm = context.rm.clone();
        let key = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
        let tx1 = start_transaction();
        assert!(rm
            .get(tx1.clone(), key.clone())
            .await
            .unwrap()
            .val
            .is_none());
        rm.abort_transaction(tx1.clone()).await;
        let tx2 = start_transaction();
        let val = Bytes::from_static(b"I have a value!");
        rm.prepare_transaction(
            tx2.clone(),
            Vec::from([(key.clone(), val.clone())]),
            Vec::new(),
            false,
        )
        .await
        .unwrap();
        rm.commit_transaction(tx2.clone()).await.unwrap();
        let tx3 = start_transaction();
        let val_after_commit = rm.get(tx3.clone(), key.clone()).await.unwrap().val.unwrap();
        assert!(val_after_commit == val);
    }

    #[tokio::test]
    async fn test_recurring_lease_renewal() {
        let context = init().await;
        let rm = context.rm.clone();
        // Get the current lease bounds.
        let initial_lease = match rm.state.read().await.deref() {
            State::Loaded(state) => state.range_info.epoch_lease,
            _ => panic!("Range is not loaded"),
        };
        // Sleep for 2 seconds to allow the lease renewal task to run at least once.
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        let final_lease = match rm.state.read().await.deref() {
            State::Loaded(state) => state.range_info.epoch_lease,
            _ => panic!("Range is not loaded"),
        };
        // Check that the upper bound has increased.
        assert!(
            final_lease.1 > initial_lease.1,
            "Lease upper bound did not increase"
        );
    }
}
