pub mod r#impl;

use crate::{error::Error, transaction_info::TransactionInfo};
use bytes::Bytes;
use flatbuf::rangeserver_flatbuffers::range_server::*;
use std::sync::Arc;
use tonic::async_trait;
use uuid::Uuid;

pub struct GetResult {
    pub val: Option<Bytes>,
    pub leader_sequence_number: i64,
}

pub struct PrepareResult {
    pub highest_known_epoch: u64,
    pub epoch_lease: (u64, u64),
}

#[async_trait]
pub trait RangeManager {
    /// Load and manage the range.
    async fn load(&self) -> Result<(), Error>;
    /// unload the range.
    async fn unload(&self);
    /// Request prefetching a key from storage and pinning to memory.
    async fn prefetch(&self, transaction_id: Uuid, key: Bytes) -> Result<(), Error>;
    /// Get the value associated with a key.
    async fn get(&self, tx: Arc<TransactionInfo>, key: Bytes) -> Result<GetResult, Error>;
    /// Run the prepare phase of two-phase commit.
    /// If prepare ever returns success, the implementation must be able to
    /// (eventually) commit the transaction no matter what, unless we get an
    /// abort call from the coordinator or know for certain that the
    /// transaction aborted.
    // It is possible that prepare gets called multiple times due to retransmits
    /// etc., so the implementation must be able to handle that.
    async fn prepare(
        &self,
        tx: Arc<TransactionInfo>,
        prepare: PrepareRequest<'_>,
    ) -> Result<PrepareResult, Error>;
    /// Abort the transaction.
    async fn abort(&self, tx: Arc<TransactionInfo>, abort: AbortRequest<'_>) -> Result<(), Error>;
    /// Run the commit phase of two-phase commit.
    /// Commit *informs* the range manager of a transaction commit, it does not
    /// decide the transaction outcome.
    /// A call to commit can fail only for intermittent reasons, and must be
    /// idempotent and safe to retry any number of times.
    async fn commit(
        &self,
        tx: Arc<TransactionInfo>,
        commit: CommitRequest<'_>,
    ) -> Result<(), Error>;
}
