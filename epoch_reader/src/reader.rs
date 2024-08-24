use std::{collections::HashMap, sync::Arc};

use common::{config::EpochPublisherSet, host_info::HostInfo, network::fast_network::FastNetwork};
use epoch_publisher::{client::EpochPublisherClient, error::Error};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

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
        publisher_set: EpochPublisherSet,
        cancellation_token: CancellationToken,
    ) -> EpochReader {
        assert!(!publisher_set.publishers.is_empty());
        let clients = publisher_set
            .publishers
            .iter()
            .map(|publisher| {
                let host_info = HostInfo {
                    identity: publisher.name.clone(),
                    address: publisher.fast_network_addr,
                    zone: publisher_set.zone.clone(),
                };
                EpochPublisherClient::new(
                    fast_network.clone(),
                    runtime.clone(),
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
        let num_publishers = self.clients.len();
        let (rx, mut tx) = mpsc::channel(num_publishers);
        // Fire off a request to read the epoch from each publisher in the set in parallel.
        for c in &self.clients {
            let client = c.clone();
            let rx = rx.clone();
            self.runtime.spawn(async move {
                let res = client.read_epoch().await;
                rx.send(res).await.unwrap();
            });
        }

        // Now see if any value is returned by a majority of publishers, and return it.
        let mut epoch_value_counts = HashMap::<u64, u64>::new();
        for _ in 0..num_publishers {
            let res = tx.recv().await;
            let res = match res {
                None => return Err(Error::EpochUnknown),
                Some(res) => res,
            };
            let epoch = match res {
                Err(_) => continue, // TODO: maybe log the error here
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