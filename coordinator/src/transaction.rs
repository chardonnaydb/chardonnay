use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;
use common::{
    full_range_id::FullRangeId, keyspace_id::KeyspaceId,
    membership::range_assignment_oracle::RangeAssignmentOracle, transaction_info::TransactionInfo,
};
use epoch_reader::reader::EpochReader;
use uuid::Uuid;

use crate::{
    error::{Error, TransactionAbortReason},
    keyspace::Keyspace,
    rangeclient::RangeClient,
};
use tx_state_store::client::Client as TxStateStoreClient;
use tx_state_store::client::OpResult;

enum State {
    Running,
    Preparing,
    Aborted,
    Committed,
    Done,
}

pub struct Transaction {
    id: Uuid,
    transaction_info: Arc<TransactionInfo>,
    state: State,
    readset: HashSet<FullRecordKey>,
    writeset: HashMap<FullRecordKey, Bytes>,
    deleteset: HashSet<FullRecordKey>,
    range_leader_sequence_number: HashMap<Uuid, u64>,
    resolved_keyspaces: HashMap<Keyspace, KeyspaceId>,
    range_client: Arc<RangeClient>,
    range_assignment_oracle: Arc<dyn RangeAssignmentOracle>,
    epoch_reader: Arc<EpochReader>,
    tx_state_store: Arc<TxStateStoreClient>,
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct FullRecordKey {
    pub range_id: FullRangeId,
    pub key: Bytes,
}

impl Transaction {
    async fn resolve_keyspace(&self, keyspace: &Keyspace) -> Result<KeyspaceId, Error> {
        // Keyspace name to id must be stable within the same transaction, to avoid
        // scenarios in which we write different keyspaces if a keyspace is deleted
        // and then another one is created with the same name within the span of the
        // transaction.
        match self.resolved_keyspaces.get(keyspace) {
            Some(k) => return Ok(*k),
            None => (),
        };
        // TODO(tamer): implement proper resolution from universe.
        return Err(Error::KeyspaceDoesNotExist);
    }

    async fn resolve_full_record_key(
        &self,
        keyspace: &Keyspace,
        key: Bytes,
    ) -> Result<FullRecordKey, Error> {
        let keyspace_id = self.resolve_keyspace(keyspace).await?;
        let range_id = match self
            .range_assignment_oracle
            .full_range_id_of_key(keyspace_id.clone(), key.clone())
            .await
        {
            None => return Err(Error::KeyspaceDoesNotExist),
            Some(id) => id,
        };
        let full_record_key = FullRecordKey {
            key: key.clone(),
            range_id,
        };
        Ok(full_record_key)
    }

    fn check_still_running(&self) -> Result<(), Error> {
        match self.state {
            State::Running => Ok(()),
            State::Aborted => Err(Error::TransactionAborted(TransactionAbortReason::Other)),
            State::Preparing | State::Committed | State::Done => {
                Err(Error::TransactionNoLongerRunning)
            }
        }
    }

    pub async fn get(&mut self, keyspace: &Keyspace, key: Bytes) -> Result<Option<Bytes>, Error> {
        self.check_still_running()?;
        let full_record_key = self.resolve_full_record_key(keyspace, key.clone()).await?;
        // Read-your-writes.
        match self.writeset.get(&full_record_key) {
            Some(v) => return Ok(Some(v.clone())),
            None => (),
        }
        if self.deleteset.contains(&full_record_key) {
            return Ok(None);
        }
        let get_result = self
            .range_client
            .get(
                self.transaction_info.clone(),
                &full_record_key.range_id,
                vec![key.clone()],
            )
            .await
            .unwrap();
        let current_range_leader_seq_num = get_result.leader_sequence_number;
        let prev_range_leader_seq_num = match self
            .range_leader_sequence_number
            .get(&full_record_key.range_id.range_id)
        {
            None => get_result.leader_sequence_number,
            Some(s) => *s,
        };
        if current_range_leader_seq_num != prev_range_leader_seq_num {
            let _ = self.abort().await;
            return Err(Error::TransactionAborted(
                TransactionAbortReason::RangeLeadershipChanged,
            ));
        }
        self.readset.insert(full_record_key.clone());
        self.range_leader_sequence_number.insert(
            full_record_key.range_id.range_id,
            current_range_leader_seq_num,
        );
        let val = get_result.vals.get(0).unwrap().clone();
        Ok(val)
    }

    pub async fn put(&mut self, keyspace: &Keyspace, key: Bytes, val: Bytes) -> Result<(), Error> {
        self.check_still_running()?;
        let full_record_key = self.resolve_full_record_key(keyspace, key.clone()).await?;
        self.deleteset.remove(&full_record_key);
        self.writeset.insert(full_record_key, val.clone());
        Ok(())
    }

    pub async fn del(&mut self, keyspace: &Keyspace, key: Bytes) -> Result<(), Error> {
        self.check_still_running()?;
        let full_record_key = self.resolve_full_record_key(keyspace, key.clone()).await?;
        self.writeset.remove(&full_record_key);
        self.deleteset.insert(full_record_key);
        Ok(())
    }

    pub async fn abort(&mut self) -> Result<(), Error> {
        match self.state {
            State::Aborted => return Ok(()),
            _ => {
                self.check_still_running()?;
                ()
            }
        };
        // We can directly set the state to Aborted here since given Prepare
        // did not start, it cannot commit on its own without us deciding to
        // commit it.
        self.state = State::Aborted;
        // Record the abort (TODO: should be done in the background?).
        // TODO(tamer): handle errors here.
        // TODO(tamer): in parallel, inform ranges we touched of the abort so
        // they release locks quickly.
        let outcome = self
            .tx_state_store
            .try_abort_transaction(self.id)
            .await
            .unwrap();
        match outcome {
            OpResult::TransactionIsAborted => (),
            OpResult::TransactionIsCommitted(_) => {
                panic!("transaction committed without coordinator consent!")
            }
        }
        Ok(())
    }
}
