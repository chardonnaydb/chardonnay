use std::{collections::HashMap, sync::Arc};

use common::{
    config::EpochPublisherSet,
    host_info::{HostIdentity, HostInfo},
    network::fast_network::FastNetwork,
};
use epoch_publisher::{client::EpochPublisherClient, error::Error};
use std::net::ToSocketAddrs;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// EpochReader reads the latest epoch from an EpochPublisherSet.
pub struct EpochReader {
    clients: Vec<Arc<EpochPublisherClient>>,
    runtime: tokio::runtime::Handle,
    publisher_majority_count: u64,
}

impl EpochReader {
    pub fn new(
        fast_network: Arc<dyn FastNetwork>,
        runtime: tokio::runtime::Handle,
        bg_runtime: tokio::runtime::Handle,
        publisher_set: EpochPublisherSet,
        cancellation_token: CancellationToken,
    ) -> EpochReader {
        assert!(!publisher_set.publishers.is_empty());
        let clients = publisher_set
            .publishers
            .iter()
            .map(|publisher| {
                let host_info = HostInfo {
                    identity: HostIdentity {
                        name: publisher.name.clone(),
                        zone: publisher_set.zone.clone(),
                    },
                    address: publisher
                        .fast_network_addr
                        .to_socket_addrs()
                        .unwrap()
                        .next()
                        .unwrap(),

                    warden_connection_epoch: 0,
                };
                EpochPublisherClient::new(
                    fast_network.clone(),
                    runtime.clone(),
                    bg_runtime.clone(),
                    host_info,
                    cancellation_token.clone(),
                )
            })
            .collect::<Vec<_>>();
        let half_round_down = ((clients.len() as f64) / 2.0).floor() as u64;
        EpochReader {
            clients,
            runtime: runtime.clone(),
            publisher_majority_count: half_round_down + 1,
        }
    }

    pub async fn read_epoch(&self) -> Result<u64, Error> {
        // TODO(tamer): add timeout and retries.
        // Fire off a request to read the epoch from each publisher in the set in parallel.
        let mut join_set = JoinSet::new();
        for c in &self.clients {
            let client = c.clone();
            join_set.spawn_on(
                // TODO(tamer): pass timeout in as a parameter.
                async move { client.read_epoch(chrono::Duration::seconds(5)).await },
                &self.runtime,
            );
        }

        // Now see if any value is returned by a majority of publishers, and return it.
        let mut epoch_value_counts = HashMap::<u64, u64>::new();
        while let Some(res) = join_set.join_next().await {
            let res = match res {
                Err(e) => {
                    warn!("Error reading epoch from publisher: {:?}", e);
                    continue;
                }
                Ok(res) => res,
            };
            let epoch = match res {
                Err(e) => {
                    warn!("Error reading epoch from publisher: {:?}", e);
                    continue;
                }
                Ok(epoch) => epoch,
            };
            let current_count = epoch_value_counts.get(&epoch).unwrap_or(&0);
            if current_count + 1 >= self.publisher_majority_count {
                return Ok(epoch);
            }
            epoch_value_counts.insert(epoch, current_count + 1);
        }
        Err(Error::EpochUnknown)
    }
}
