use flatbuf::rangeserver_flatbuffers::range_server::*;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unknown")]
    Unknown,
}

pub trait Iterator<'a> {
    async fn next<'b>(&'b mut self) -> Option<&LogEntry<'b>>;
    async fn next_offset(&self) -> Result<u64, Error>;
}

pub trait Wal: Send + Sync + 'static {
    async fn first_offset(&self) -> Result<u64, Error>;
    async fn next_offset(&self) -> Result<u64, Error>;
    async fn append_prepare(&mut self, entry: PrepareRecord<'_>) -> Result<(), Error>;
    async fn append_commit(&mut self, entry: CommitRecord<'_>) -> Result<(), Error>;
    async fn append_abort(&mut self, entry: AbortRecord<'_>) -> Result<(), Error>;
    async fn trim_before_offset(&mut self, offset: u64) -> Result<(), Error>;
    fn iterator<'a>(&'a self) -> impl Iterator;
}
