use crate::{
    epoch_provider::Error as EpochProviderError, persistence::Error as PersistenceError,
    transaction_abort_reason::TransactionAbortReason, wal::Error as WalError,
};

use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum Error {
    RangeDoesNotExist,
    RangeIsNotLoaded,
    KeyIsOutOfRange,
    RangeOwnershipLost,
    Timeout,
    TransactionAborted(TransactionAbortReason),
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}

impl Error {
    pub fn from_persistence_error(e: PersistenceError) -> Self {
        match e {
            PersistenceError::RangeDoesNotExist => Self::RangeDoesNotExist,
            PersistenceError::RangeOwnershipLost => Self::RangeOwnershipLost,
            PersistenceError::Timeout => Self::Timeout,
            PersistenceError::InternalError(_) => Self::InternalError(Arc::new(e)),
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
}
