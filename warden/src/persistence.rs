pub mod cassandra;

use std::sync::Arc;

use common::{full_range_id::FullRangeId, key_range::KeyRange, keyspace_id::KeyspaceId};
use thiserror::Error;
use uuid::Uuid;

pub struct RangeInfo {
    pub id: Uuid,
    pub key_range: KeyRange,
    pub assignee: Option<String>,
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
    ) -> impl std::future::Future<Output = Result<Vec<RangeInfo>, Error>> + Send;

    fn update_range_assignment(
        &self,
        range_id: &FullRangeId,
        assignee: str,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
}
