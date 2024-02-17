pub mod cassandra;

use crate::key_range::KeyRange;
use crate::key_version::KeyVersion;
use bytes::Bytes;
use uuid::Uuid;

type EpochLease = (u64, u64);

pub struct RangeInfo {
    id: Uuid,
    key_range: KeyRange,
    leader_sequence_number: u64,
    epoch_lease: EpochLease,
}

pub enum Error {
    Timeout,
    RangeDoesNotExist,
    RangeOwnershipLost,
    Unknown,
}

pub trait Persistence {
    async fn take_ownership_and_load_range(&self, range_id: Uuid) -> Result<RangeInfo, Error>;
    async fn renew_epoch_lease(
        &self,
        range_id: Uuid,
        new_lease: EpochLease,
        leader_sequence_number: u64,
    ) -> Result<(), Error>;

    // TODO: Handle deletes and tombstones in the API too.
    async fn put_versioned_record(
        &self,
        range_id: Uuid,
        key: Bytes,
        val: Bytes,
        version: KeyVersion,
    ) -> Result<(), Error>;

    async fn upsert(
        &self,
        range_id: Uuid,
        key: Bytes,
        val: Bytes,
        version: KeyVersion,
    ) -> Result<(), Error>;
    async fn delete(&self, range_id: Uuid, key: Bytes, version: KeyVersion) -> Result<(), Error>;
    async fn get(&self, range_id: Uuid, key: Bytes) -> Result<Option<Bytes>, Error>;
}
