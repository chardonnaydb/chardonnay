use std::sync::Arc;

use strum::Display;

#[derive(Clone, Debug, Display)]
pub enum TransactionAbortReason {
    DeadlockPrevention,
    TransactionLockLost,
    RangeLeadershipChanged,
    RangeLeaseExpired,
    RangePartitioningChanged,
    TransactionTimeout,
    PrepareFailed,
    Other,
}

#[derive(Clone, Debug)]
pub enum Error {
    KeyspaceDoesNotExist,
    TransactionNoLongerRunning,
    Timeout,
    TransactionDoneButStateUnknown,
    TransactionAborted(TransactionAbortReason),
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}
