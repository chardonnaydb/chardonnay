use common::config::EpochPublisherSet;
use proto::epoch_publisher::epoch_publisher_client::EpochPublisherClient;
use proto::epoch_publisher::SetEpochRequest;
use tokio::task::JoinSet;
use tonic::Status as TStatus;
use tracing::{error, info, instrument};

/// PublisherSetUpdater can be used to update the epoch on EpochPublisherSet.
#[derive(Debug)]
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

    #[instrument]
    pub async fn update_epoch(
        &self,
        epoch: u64,
    ) -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
        info!("Updating epoch");
        //TODO(tamer): retries and timeout.

        // Fire off a request to update the epoch at each publisher in the set in parallel.
        let mut join_set = JoinSet::new();
        for p in &self.publisher_set.publishers {
            let p = p.clone();
            let addr = format!("http://{}", p.backend_addr.to_string());
            join_set.spawn(async move {
                let mut client = EpochPublisherClient::connect(addr).await.map_err(|_| {
                    error!("Failed to connect to epoch publisher {}", p.name.clone());
                    TStatus::new(
                        tonic::Code::FailedPrecondition,
                        format!("Failed to connect to epoch publisher {}", p.name.clone()),
                    )
                })?;
                let request = SetEpochRequest { epoch };
                let res = client.set_epoch(request).await;
                if let Err(e) = &res {
                    error!(
                        "Failed to set epoch on publisher {}. Error: {}",
                        p.name.clone(),
                        e
                    );
                };
                res
            });
        }

        // Now check if it's accepted by a majority.
        let mut num_success = 0;
        while let Some(res) = join_set.join_next().await {
            let res = match res {
                Err(e) => {
                    error!("Failed to set epoch on publisher. Error: {}", e);
                    continue;
                }
                Ok(res) => res,
            };
            match res {
                Err(_) => continue,
                Ok(_) => (),
            };
            num_success += 1;
            if num_success >= self.publisher_majority_count {
                return Ok(());
            }
        }
        error!("failed to update epoch on a majority of publishers");
        Err("failed to update epoch".into())
    }
}
