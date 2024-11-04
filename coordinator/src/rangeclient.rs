use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use common::{
    full_range_id::FullRangeId, host_info::HostIdentity,
    membership::range_assignment_oracle::RangeAssignmentOracle, network::fast_network::FastNetwork,
    record::Record, transaction_info::TransactionInfo,
};
use rangeclient::client::{Error, GetResult, PrepareOk, RangeClient as Client};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// RangeClient abstracts away the individual rangeservers and allows users
/// to reach any range just by using the range id.
pub struct RangeClient {
    range_assignment_oracle: Arc<dyn RangeAssignmentOracle>,
    range_clients: RwLock<HashMap<HostIdentity, Arc<Client>>>,
    fast_network: Arc<dyn FastNetwork>,
    runtime: tokio::runtime::Handle,
    cancellation_token: CancellationToken,
}

// public interface
impl RangeClient {
    pub fn new(
        range_assignment_oracle: Arc<dyn RangeAssignmentOracle>,
        fast_network: Arc<dyn FastNetwork>,
        runtime: tokio::runtime::Handle,
        cancellation_token: CancellationToken,
    ) -> RangeClient {
        RangeClient {
            range_assignment_oracle,
            fast_network,
            range_clients: RwLock::new(HashMap::new()),
            runtime,
            cancellation_token,
        }
    }

    pub async fn get(
        &self,
        tx: Arc<TransactionInfo>,
        range_id: &FullRangeId,
        keys: Vec<Bytes>,
    ) -> Result<GetResult, Error> {
        let client = self.get_range_client(range_id).await?;
        client
            .get(tx, range_id, keys)
            .await
            .map_err(|e| self.handle_rangeserver_err(range_id, e))
    }

    pub async fn prepare_transaction(
        &self,
        tx: Arc<TransactionInfo>,
        range_id: &FullRangeId,
        has_reads: bool,
        writes: &[Record],
        deletes: &[Bytes],
    ) -> Result<PrepareOk, Error> {
        let client = self.get_range_client(range_id).await?;
        client
            .prepare_transaction(tx, range_id, has_reads, writes, deletes)
            .await
            .map_err(|e| self.handle_rangeserver_err(range_id, e))
    }

    pub async fn abort_transaction(
        &self,
        tx: Arc<TransactionInfo>,
        range_id: &FullRangeId,
    ) -> Result<(), Error> {
        let client = self.get_range_client(range_id).await?;
        client
            .abort_transaction(tx, range_id)
            .await
            .map_err(|e| self.handle_rangeserver_err(range_id, e))
    }

    pub async fn commit_transaction(
        &self,
        tx: Arc<TransactionInfo>,
        range_id: &FullRangeId,
        epoch: u64,
    ) -> Result<(), Error> {
        let client = self.get_range_client(range_id).await?;
        client
            .commit_transaction(tx, range_id, epoch)
            .await
            .map_err(|e| self.handle_rangeserver_err(range_id, e))
    }
}

impl RangeClient {
    async fn get_range_client(&self, range_id: &FullRangeId) -> Result<Arc<Client>, Error> {
        let host_info = match self.range_assignment_oracle.host_of_range(range_id).await {
            None => return Err(Error::RangeIsNotLoaded),
            Some(host_info) => host_info,
        };

        // Check if we already have a started client to the range server.
        let existing_client = {
            let client = {
                let range_clients = self.range_clients.read().await;
                match range_clients.get(&host_info.identity) {
                    None => None,
                    Some(c) => Some(c.clone()),
                }
            };
            match client {
                None => None,
                Some(client) => {
                    if client.host_info() == host_info && !client.is_stopped().await {
                        Some(client.clone())
                    } else {
                        None
                    }
                }
            }
        };

        let client = match existing_client {
            Some(c) => c,
            None => {
                let client = Client::new(self.fast_network.clone(), host_info.clone(), None).await;
                {
                    let mut range_clients = self.range_clients.write().await;
                    match range_clients.get(&host_info.identity) {
                        None => (),
                        Some(client) => {
                            if client.host_info() == host_info {
                                return Ok(client.clone());
                            }
                        }
                    };
                    // TODO(tamer): need to stop the old client, maybe by implementing
                    // drop on the Client struct.
                    range_clients.remove(&host_info.identity);
                    range_clients.insert(host_info.identity.clone(), client.clone());
                };
                client
            }
        };

        Client::start(
            client.clone(),
            self.runtime.clone(),
            self.cancellation_token.clone(),
        )
        .await;
        Ok(client)
    }

    fn handle_rangeserver_err(&self, range_id: &FullRangeId, error: Error) -> Error {
        match error {
            Error::RangeIsNotLoaded | Error::RangeOwnershipLost => self
                .range_assignment_oracle
                .maybe_refresh_host_of_range(range_id),
            Error::InvalidRequestFormat
            | Error::RangeDoesNotExist
            | Error::KeyIsOutOfRange
            | Error::ConnectionClosed
            | Error::Timeout
            | Error::UnknownTransaction
            | Error::CacheIsFull
            | Error::PrefetchError
            | Error::TransactionAborted(_)
            | Error::InternalError(_) => (),
        };
        error
    }
}
