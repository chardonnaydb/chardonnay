use crate::{
    epoch_provider::Error as EpochProviderError, storage::Error as StorageError,
    transaction_abort_reason::TransactionAbortReason, wal::Error as WalError,
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
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
    PrefetchError,
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
            Self::InternalError(_) => Status::InternalError,
            Self::PrefetchError => Status::PrefetchError,
        }
    }
}
