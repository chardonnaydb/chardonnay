pub mod cassandra;

use std::sync::Arc;

use common::{full_range_id::FullRangeId, key_range::KeyRange, keyspace_id::KeyspaceId};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct RangeInfo {
    pub keyspace_id: KeyspaceId,
    pub id: Uuid,
    pub key_range: KeyRange,
}

#[derive(Debug)]
pub struct RangeAssignment {
    pub range: RangeInfo,
    pub assignee: String,
}

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Persistence Layer error: {0}")]
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}
pub trait Persistence: Send + Sync + 'static {
    fn get_keyspace_range_map(
        &self,
        keyspace_id: &KeyspaceId,
    ) -> impl std::future::Future<Output = Result<Vec<RangeAssignment>, Error>> + Send;

    fn update_range_assignment(
        &self,
        range_id: &FullRangeId,
        assignee: str,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
}
