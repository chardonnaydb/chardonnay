pub mod cassandra;

use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Timeout Error")]
    Timeout,
    #[error("Storage Layer error: {0}")]
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}

pub struct CommitInfo {
    pub epoch: u64,
}

pub enum OpResult {
    TransactionIsAborted,
    TransactionIsCommitted(CommitInfo),
}

pub trait Storage: Send + Sync + 'static {
    fn start_transaction(
        &self,
        transaction_id: Uuid,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;

    fn abort_transaction(
        &self,
        transaction_id: Uuid,
    ) -> impl std::future::Future<Output = Result<OpResult, Error>> + Send;

    fn commit_transaction(
        &self,
        transaction_id: Uuid,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<OpResult, Error>> + Send;
}
