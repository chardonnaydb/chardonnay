use proto::universe::{KeyRange, KeyRangeRequest, KeyspaceInfo, Zone};
use std::sync::Arc;
use thiserror::Error;

pub mod cassandra;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Timeout Error")]
    Timeout,
    #[error("Serialization Error: {0}")]
    DeserializationError(Arc<dyn std::error::Error + Send + Sync>),
    #[error("Storage Layer error: {0}")]
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}

pub trait Storage: Send + Sync + 'static {
    fn create_keyspace(
        &self,
        name: &str,
        namespace: &str,
        primary_zone: Option<Zone>,
        base_key_ranges: Vec<KeyRangeRequest>,
    ) -> impl std::future::Future<Output = Result<String, Error>> + Send;

    fn list_keyspaces(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<KeyspaceInfo>, Error>> + Send;
}
