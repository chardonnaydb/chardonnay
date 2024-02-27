use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unknown")]
    Unknown,
}

pub trait EpochProvider {
    // Values returned must satisfy the Global Epoch Invariant:
    // If a call returns a value e, then all subsequent calls must return a value
    // greater than or equal to e-1.
    // In particular this means that the value returned from here could be one less
    // than the true epoch.
    async fn read_epoch(&self) -> Result<u64, Error>;

    async fn wait_until_epoch(&self, epoch: u64) -> Result<(), Error>;
}
