pub enum Error {
    UNKNOWN,
}

pub trait EpochProvider {
    async fn read_epoch(&self) -> Result<u64, Error>;

    async fn wait_until_epoch(&self, epoch: u64) -> Result<u64, Error>;
}
