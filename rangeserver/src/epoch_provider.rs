use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unknown")]
    Unknown,
}

pub trait EpochProvider: Send + Sync + 'static {
    // Values returned must satisfy the Global Epoch Invariant:
    // If a call returns a value e, then all subsequent calls must return a value
    // greater than or equal to e-1.
    // In particular this means that the value returned from here could be one less
    // than the true epoch.
    fn read_epoch(&self) -> impl std::future::Future<Output = Result<u64, Error>> + Send;

    fn wait_until_epoch(
        &self,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
}
