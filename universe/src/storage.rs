use proto::universe::{KeyRange, KeyspaceInfo, Zone};
use std::sync::Arc;
use thiserror::Error;

pub mod cassandra;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Timeout Error")]
    Timeout,
    #[error("Keyspace already exists")]
    KeyspaceAlreadyExists,
    #[error("Storage Layer error: {}", .0.as_ref().map(|e| e.to_string()).unwrap_or_else(|| "Unknown error".to_string()))]
    InternalError(Option<Arc<dyn std::error::Error + Send + Sync>>),
}

pub trait Storage: Send + Sync + 'static {
    fn create_keyspace(
        &self,
        keyspace_id: &str,
        name: &str,
        namespace: &str,
        primary_zone: Zone,
        base_key_ranges: Vec<KeyRange>,
    ) -> impl std::future::Future<Output = Result<String, Error>> + Send;

    fn list_keyspaces(
        &self,
        region: Option<proto::universe::Region>,
    ) -> impl std::future::Future<Output = Result<Vec<KeyspaceInfo>, Error>> + Send;
}
