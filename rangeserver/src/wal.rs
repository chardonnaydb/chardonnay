use flatbuf::rangeserver_flatbuffers::range_server::*;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unknown")]
    Unknown,
}

pub trait Iterator<'a> {
    fn next(&mut self) -> impl std::future::Future<Output = Option<&LogEntry<'_>>> + Send;
    fn next_offset(&self) -> impl std::future::Future<Output = Result<u64, Error>> + Send;
}

pub trait Wal: Send + Sync + 'static {
    fn first_offset(&self) -> impl std::future::Future<Output = Result<u64, Error>> + Send;
    fn next_offset(&self) -> impl std::future::Future<Output = Result<u64, Error>> + Send;
    fn append_prepare(
        &mut self,
        entry: PrepareRequest<'_>,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
    fn append_commit(
        &mut self,
        entry: CommitRequest<'_>,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
    fn append_abort(
        &mut self,
        entry: AbortRequest<'_>,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
    fn trim_before_offset(
        &mut self,
        offset: u64,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
    fn find_prepare_record(
        &self,
        transaction_id: Uuid,
    ) -> impl std::future::Future<Output = Result<Option<Vec<u8>>, Error>> + Send;
    fn iterator<'a>(&'a self) -> impl Iterator + Send;
}
