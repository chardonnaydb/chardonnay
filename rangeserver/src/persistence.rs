pub mod cassandra;

use std::sync::Arc;

use crate::full_range_id::FullRangeId;
use crate::key_range::KeyRange;
use crate::key_version::KeyVersion;
use bytes::Bytes;
use thiserror::Error;
use uuid::Uuid;

type EpochLease = (u64, u64);

pub struct RangeInfo {
    pub id: Uuid,
    pub key_range: KeyRange,
    pub leader_sequence_number: u64,
    pub epoch_lease: EpochLease,
}

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Timeout Error")]
    Timeout,
    #[error("No range with this id exists in the persistence layer")]
    RangeDoesNotExist,
    #[error("Range ownership claimed by another range server")]
    RangeOwnershipLost,
    #[error("Persistence Layer error: {0}")]
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}

pub trait Persistence {
    async fn take_ownership_and_load_range(
        &self,
        range_id: FullRangeId,
    ) -> Result<RangeInfo, Error>;
    async fn renew_epoch_lease(
        &self,
        range_id: FullRangeId,
        new_lease: EpochLease,
        leader_sequence_number: u64,
    ) -> Result<(), Error>;

    // TODO: Handle deletes and tombstones in the API too.
    async fn put_versioned_record(
        &self,
        range_id: FullRangeId,
        key: Bytes,
        val: Bytes,
        version: KeyVersion,
    ) -> Result<(), Error>;

    async fn upsert(
        &self,
        range_id: FullRangeId,
        key: Bytes,
        val: Bytes,
        version: KeyVersion,
    ) -> Result<(), Error>;
    async fn delete(
        &self,
        range_id: FullRangeId,
        key: Bytes,
        version: KeyVersion,
    ) -> Result<(), Error>;
    async fn get(&self, range_id: FullRangeId, key: Bytes) -> Result<Option<Bytes>, Error>;
}
