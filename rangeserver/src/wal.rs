use async_trait::async_trait;
use flatbuf::rangeserver_flatbuffers::range_server::*;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("WAL needs to be sync'd")]
    NotSynced,
    #[error("Timeout")]
    Timeout,
    #[error("Unknown")]
    Unknown,
}

pub trait Iterator<'a> {
    fn next(&mut self) -> impl std::future::Future<Output = Option<LogEntry<'_>>> + Send;
    fn next_offset(&self) -> impl std::future::Future<Output = Result<u64, Error>> + Send;
}

#[async_trait]
pub trait Wal: Send + Sync + 'static {
    /// Must be called first before calling any other function on the WAL,
    /// when the WAL is first created or whenever any function returns a
    /// NotSynced error.
    async fn sync(&self) -> Result<(), Error>;
    async fn first_offset(&self) -> Result<u64, Error>;
    async fn next_offset(&self) -> Result<u64, Error>;

    async fn append_prepare(&self, entry: PrepareRequest<'_>) -> Result<(), Error>;
    async fn append_commit(&self, entry: CommitRequest<'_>) -> Result<(), Error>;
    async fn append_abort(&self, entry: AbortRequest<'_>) -> Result<(), Error>;
    async fn trim_before_offset(&self, offset: u64) -> Result<(), Error>;

    fn iterator<'a>(&'a self) -> impl Iterator + Send;
}
