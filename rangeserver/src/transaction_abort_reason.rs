use strum::Display;

#[derive(Clone, Debug, Display)]
pub enum TransactionAbortReason {
    WaitDie,
    TransactionLockLost,
    Other,
}
