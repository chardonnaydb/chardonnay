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

// Not used currently, may need it for leveldb/rocksdb implementations
#[derive(Clone, Debug, Copy)]
pub struct RCOptions {
    pub path: &'static str,   /* disk path where the db is stored */
    pub num_write_buffers: usize,   /* num memtables */
    pub write_buffer_size: u64, /* memtable size */
}

// callback from range cache to range server to garbage collect old epochs
type GCCallback = fn(epoch: u64) -> ();

pub trait Cache: Send + Sync + 'static {
    // inserts or updates a key 
    // NOTE: ownership of key and val will move to the Cache
    fn upsert(
        &mut self,
        key: Bytes,
        val: Bytes,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
    // ) -> Result<(), Error>;

    // deletes a key 
    // NOTE: ownership of key will move to the Cache
    fn delete(
        &mut self,
        key: Bytes,
        epoch: u64,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
    // ) -> Result<(), Error>;

    // returns the value and epoch of a key
    fn get(
        &self,
        key: &Bytes,
        epoch: Option<u64>,
    ) -> impl std::future::Future<Output = Result<(bytes::Bytes, u64), Error>> + Send;
    // ) -> Result<(bytes::Bytes, u64), Error>;

    // fn iter(
    //     &self,
    //     epoch: Option<u64>,
    // ) -> impl Result<Iter<'_, T>, Error>;

    fn clear(
        &mut self,
        epoch: Option<u64>,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
    // ) -> Result<(), Error>;

    fn register_gc_callback(
        &mut self,
        cb: GCCallback,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
    // ) -> Result<(), Error>;

}


