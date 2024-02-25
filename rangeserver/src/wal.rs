use flatbuf::rangeserver_flatbuffers::range_server::*;

#[derive(Debug)]
pub enum Error {
    Unknown,
}

pub trait Iterator<'a> {
    async fn next<'b>(&'b mut self) -> Option<&LogEntry<'b>>;
    async fn next_offset(&self) -> Result<u64, Error>;
}

pub trait Wal {
    async fn first_offset(&self) -> Result<u64, Error>;
    async fn next_offset(&self) -> Result<u64, Error>;
    async fn append_prepare(&mut self, entry: PrepareRecord<'_>) -> Result<(), Error>;
    async fn append_commit(&mut self, entry: CommitRecord<'_>) -> Result<(), Error>;
    async fn append_abort(&mut self, entry: AbortRecord<'_>) -> Result<(), Error>;
    async fn trim_before_offset(&mut self, offset: u64) -> Result<(), Error>;
    fn iterator<'a>(&'a self) -> impl Iterator;
}
