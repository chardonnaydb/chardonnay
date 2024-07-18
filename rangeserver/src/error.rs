use crate::{
    epoch_provider::Error as EpochProviderError, storage::Error as StorageError,
    transaction_abort_reason::TransactionAbortReason, wal::Error as WalError, cache::Error as CacheError
};

use flatbuf::rangeserver_flatbuffers::range_server::Status;

use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum Error {
    InvalidRequestFormat,
    RangeDoesNotExist,
    RangeIsNotLoaded,
    KeyIsOutOfRange,
    RangeOwnershipLost,
    Timeout,
    UnknownTransaction,
    TransactionAborted(TransactionAbortReason),
    KeyNotFoundInCache,
    CacheIsFull,
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}

impl Error {
    pub fn from_storage_error(e: StorageError) -> Self {
        match e {
            StorageError::RangeDoesNotExist => Self::RangeDoesNotExist,
            StorageError::RangeOwnershipLost => Self::RangeOwnershipLost,
            StorageError::Timeout => Self::Timeout,
            StorageError::InternalError(_) => Self::InternalError(Arc::new(e)),
        }
    }

    pub fn from_wal_error(e: WalError) -> Self {
        match e {
            WalError::Unknown => Self::InternalError(Arc::new(e)),
        }
    }

    pub fn from_cache_error(e: CacheError) -> Self {
        match e {
            CacheError::KeyNotFound => Self::KeyNotFoundInCache,
            CacheError::CacheIsFull => Self::CacheIsFull,
            CacheError::Timeout => Self::Timeout,
            CacheError::InternalError(_) => Self::InternalError(Arc::new(e)),
        }
    }

    pub fn from_epoch_provider_error(e: EpochProviderError) -> Self {
        match e {
            EpochProviderError::Unknown => Self::InternalError(Arc::new(e)),
        }
    }

    pub fn to_flatbuf_status(&self) -> Status {
        match self {
            Self::InvalidRequestFormat => Status::InvalidRequestFormat,
            Self::RangeDoesNotExist => Status::RangeDoesNotExist,
            Self::RangeIsNotLoaded => Status::RangeIsNotLoaded,
            Self::KeyIsOutOfRange => Status::KeyIsOutOfRange,
            Self::RangeOwnershipLost => Status::RangeOwnershipLost,
            Self::Timeout => Status::Timeout,
            Self::UnknownTransaction => Status::UnknownTransaction,
            Self::TransactionAborted(_) => Status::TransactionAborted,
            Self::KeyNotFoundInCache => Status::KeyNotFoundInCache,
            Self::CacheIsFull => Status::CacheIsFull,
            Self::InternalError(_) => Status::InternalError,
        }
    }

    pub fn from_flatbuf_status(status: Status) -> Result<(), Self> {
        match status {
            Status::Ok => Ok(()),
            Status::InvalidRequestFormat => Err(Self::InvalidRequestFormat),
            Status::RangeDoesNotExist => Err(Self::RangeDoesNotExist),
            Status::RangeIsNotLoaded => Err(Self::RangeIsNotLoaded),
            Status::KeyIsOutOfRange => Err(Self::KeyIsOutOfRange),
            Status::RangeOwnershipLost => Err(Self::RangeOwnershipLost),
            Status::Timeout => Err(Self::Timeout),
            Status::UnknownTransaction => Err(Self::UnknownTransaction),
            Status::KeyNotFoundInCache => Err(Self::KeyNotFoundInCache),
            Status::CacheIsFull => Err(Self::CacheIsFull),
            Status::TransactionAborted => {
                // TODO: get the reason from the message.
                Err(Self::TransactionAborted(TransactionAbortReason::Other))
            }
            Status::InternalError => {
                // TODO: get the error from the message.
                Err(Self::InternalError(Arc::new(std::fmt::Error)))
            }
            _ => Err(Self::InternalError(Arc::new(std::fmt::Error))),
        }
    }
}
