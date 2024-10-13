use uuid::Uuid;

use crate::storage::{cassandra::Cassandra, Storage};
use common::config::Config;
use common::region::Region;

pub struct Client {
    storage: Cassandra,
}

pub type Error = crate::storage::Error;
pub type OpResult = crate::storage::OpResult;

impl Client {
    pub async fn new(config: Config, _region: Region) -> Client {
        Client {
            storage: Cassandra::new(config.cassandra.cql_addr.to_string()).await,
        }
    }

    /// Starts a new transaction with the given id.
    /// Start is idempotent: starting a previously started transaction is a no-op.
    pub async fn start_transaction(&self, id: Uuid) -> Result<(), Error> {
        self.storage.start_transaction(id).await
    }

    /// Attempt to abort a transaction.
    /// It is possible that the transaction has previously committed successfully,
    /// in which case it cannot be aborted. The OpResult returned will indicate
    /// whether the abort succeeded, or whether the transaction is committed.
    /// Aborting an already aborted transaction is a no-op.
    pub async fn try_abort_transaction(&self, id: Uuid) -> Result<OpResult, Error> {
        self.storage.abort_transaction(id).await
    }

    /// Attempt to commit a transaction.
    /// It is possible that the transaction has previously aborted, in which case
    /// it cannot be committed. The OpResult returned will indicate the commit
    /// succeeded, or whether the transaction is aborted.
    /// Committing an already committed transaction is a no-op.
    pub async fn try_commit_transaction(&self, id: Uuid, epoch: u64) -> Result<OpResult, Error> {
        self.storage.commit_transaction(id, epoch).await
    }
}
