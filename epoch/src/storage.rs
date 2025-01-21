pub mod cassandra;
pub mod in_memory;

use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Timeout Error")]
    Timeout,
    #[error("Write failed due to condition not satisfied")]
    ConditionFailed,
    #[error("Epoch for this region has not been initialized")]
    EpochNotInitialized,
    #[error("Storage Layer error: {0}")]
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}

pub trait Storage: Send + Sync + 'static {
    /// Initializes the epoch to an initial value of 1 if it has not been already initialized.
    fn initialize_epoch(&self) -> impl std::future::Future<Output = Result<(), Error>> + Send;
    fn read_latest(&self) -> impl std::future::Future<Output = Result<u64, Error>> + Send;
    /// Sets the value of the epoch to [new_epoch], but only if the current value is
    /// [current_epoch]. Since 0 indicates an uninitialized epoch, [new_epoch] cannot be 0.
    fn conditional_update(
        &self,
        new_epoch: u64,
        current_epoch: u64,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
}
