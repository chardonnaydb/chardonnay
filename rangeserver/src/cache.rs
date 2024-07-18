pub mod memtabledb;

use std::sync::Arc;

use bytes::Bytes;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Timeout Error")]
    Timeout,
    #[error("No space left in range cache")]
    CacheIsFull,
    #[error("Key not present in range cache")]
    KeyNotFound,
    #[error("range cache error: {0}")]
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}

// Range cache options
#[derive(Clone, Debug, Copy)]
pub struct CacheOptions {
    pub path: &'static str,   /* disk path where the db is stored */
    pub num_write_buffers: usize,   /* num memtables */
    pub write_buffer_size: u64, /* memtable size */
}

// callback from range cache to range server to garbage collect old epochs
type GCCallback = fn(epoch: u64) -> ();
pub trait Cache: Send + Sync + 'static {
    // constructor
    fn new(
        cache_options: Option<&CacheOptions>,
    ) -> impl std::future::Future<Output = Self> + Send;

    // inserts or updates a key
    fn upsert(
        &mut self,
        key: Bytes,
        val: Bytes,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;

    // deletes a key
    fn delete(
        &mut self,
        key: Bytes,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;

    // If epoch is none, returns the latest value, else
    // returns the value of latest cache epoch <= requested epoch
    fn get(
        &self,
        key: Bytes,
        epoch: Option<u64>,
    ) -> impl std::future::Future<Output = Result<(bytes::Bytes, u64), Error>> + Send;

    // clears the cache entries upto a given epoch
    fn clear(
        &mut self,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;

    // clients can register a callback to garbage collect old cache entries
    fn register_gc_callback(
        &mut self,
        cb: GCCallback,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;

}


