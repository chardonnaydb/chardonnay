use std::sync::Arc;

use common::{
    config::Config, membership::range_assignment_oracle::RangeAssignmentOracle,
    network::fast_network::FastNetwork, region::Zone, transaction_info::TransactionInfo,
};
use epoch_reader::reader::EpochReader;
use tokio_util::sync::CancellationToken;
use tx_state_store::client::Client as TxStateStoreClient;

use crate::transaction::Transaction;

pub struct Coordinator {
    range_assignment_oracle: Arc<dyn RangeAssignmentOracle>,
    runtime: tokio::runtime::Handle,
    range_client: Arc<crate::rangeclient::RangeClient>,
    epoch_reader: Arc<EpochReader>,
    tx_state_store: Arc<TxStateStoreClient>,
}

impl Coordinator {
    pub async fn new(
        config: &Config,
        zone: Zone,
        range_assignment_oracle: Arc<dyn RangeAssignmentOracle>,
        fast_network: Arc<dyn FastNetwork>,
        runtime: tokio::runtime::Handle,
        bg_runtime: tokio::runtime::Handle,
        cancellation_token: CancellationToken,
    ) -> Coordinator {
        let range_client = Arc::new(crate::rangeclient::RangeClient::new(
            range_assignment_oracle.clone(),
            fast_network.clone(),
            runtime.clone(),
            cancellation_token.clone(),
        ));
        let tx_state_store =
            Arc::new(TxStateStoreClient::new(config.clone(), zone.region.clone()).await);
        let region_config = config.regions.get(&zone.region).unwrap();
        let publisher_set = region_config
            .epoch_publishers
            .iter()
            .find(|&s| s.zone == zone)
            .unwrap();
        let epoch_reader = Arc::new(EpochReader::new(
            fast_network.clone(),
            runtime.clone(),
            bg_runtime,
            publisher_set.clone(),
            cancellation_token.clone(),
        ));
        Coordinator {
            range_assignment_oracle,
            runtime,
            range_client,
            tx_state_store,
            epoch_reader,
        }
    }

    pub fn start_transaction(&self, transaction_info: Arc<TransactionInfo>) -> Transaction {
        //TODO(tamer): start transaction at the tx_state_store.
        Transaction::new(
            transaction_info,
            self.range_client.clone(),
            self.range_assignment_oracle.clone(),
            self.epoch_reader.clone(),
            self.tx_state_store.clone(),
            self.runtime.clone(),
        )
    }
}
