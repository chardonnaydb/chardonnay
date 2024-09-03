use common::config::EpochPublisherSet;
use proto::epoch_publisher::epoch_publisher_client::EpochPublisherClient;
use proto::epoch_publisher::SetEpochRequest;
use tokio::task::JoinSet;
use tonic::Status as TStatus;

/// PublisherSetUpdater can be used to update the epoch on EpochPublisherSet.
pub struct PublisherSetUpdater {
    publisher_set: EpochPublisherSet,
    publisher_majority_count: u64,
}

impl PublisherSetUpdater {
    pub fn new(publisher_set: EpochPublisherSet) -> PublisherSetUpdater {
        assert!(!publisher_set.publishers.is_empty());
        let half_round_down = ((publisher_set.publishers.len() as f64) / 2.0).floor() as u64;
        PublisherSetUpdater {
            publisher_set,
            publisher_majority_count: half_round_down + 1,
        }
    }
    pub async fn update_epoch(
        &self,
        epoch: u64,
    ) -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
        // Fire off a request to update the epoch at each publisher in the set in parallel.
        let mut join_set = JoinSet::new();
        for p in &self.publisher_set.publishers {
            let addr = format!("http://{}", p.backend_addr.to_string());
            join_set.spawn(async move {
                let mut client = EpochPublisherClient::connect(addr).await.map_err(|_| {
                    TStatus::new(
                        tonic::Code::FailedPrecondition,
                        "Failed to connect to epoch publisher",
                    )
                })?;
                let request = SetEpochRequest { epoch };
                client.set_epoch(request).await
            });
        }

        // Now check if it's accepted by a majority.
        let mut num_success = 0;
        while let Some(res) = join_set.join_next().await {
            let res = match res {
                Err(_) => continue, // TODO: maybe log the error here
                Ok(res) => res,
            };
            let _ = match res {
                Err(_) => continue, // TODO: maybe log the error here
                Ok(epoch) => epoch,
            };
            num_success += 1;
            if num_success >= self.publisher_majority_count {
                return Ok(());
            }
        }

        Err("failed to update epoch".into())
    }
}
