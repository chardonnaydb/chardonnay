pub mod cassandra;

use std::sync::Arc;

use crate::key_version::KeyVersion;
use bytes::Bytes;
use common::full_range_id::FullRangeId;
use common::key_range::KeyRange;
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

pub trait Persistence: Send + Sync + 'static {
    fn take_ownership_and_load_range(
        &self,
        range_id: FullRangeId,
    ) -> impl std::future::Future<Output = Result<RangeInfo, Error>> + Send;
    fn renew_epoch_lease(
        &self,
        range_id: FullRangeId,
        new_lease: EpochLease,
        leader_sequence_number: u64,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;

    fn upsert(
        &self,
        range_id: FullRangeId,
        key: Bytes,
        val: Bytes,
        version: KeyVersion,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
    fn delete(
        &self,
        range_id: FullRangeId,
        key: Bytes,
        version: KeyVersion,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
    fn get(
        &self,
        range_id: FullRangeId,
        key: Bytes,
    ) -> impl std::future::Future<Output = Result<Option<Bytes>, Error>> + Send;
}
