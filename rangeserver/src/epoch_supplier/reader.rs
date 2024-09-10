use std::sync::Arc;

use super::*;
use async_trait::async_trait;
use common::{config::EpochPublisherSet, network::fast_network::FastNetwork};
use epoch_publisher::error::Error;
use epoch_reader::reader::EpochReader;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

pub struct Reader {
    epoch_reader: EpochReader,
}

impl Reader {
    pub fn new(
        fast_network: Arc<dyn FastNetwork>,
        runtime: tokio::runtime::Handle,
        publisher_set: EpochPublisherSet,
        cancellation_token: CancellationToken,
    ) -> Self {
        Reader {
            epoch_reader: EpochReader::new(
                fast_network,
                runtime,
                publisher_set,
                cancellation_token,
            ),
        }
    }
}

#[async_trait]
impl EpochSupplier for Reader {
    async fn read_epoch(&self) -> Result<u64, Error> {
        self.epoch_reader.read_epoch().await
    }

    async fn wait_until_epoch(
        &self,
        target_epoch: u64,
        timeout: chrono::Duration,
    ) -> Result<(), Error> {
        let start_time = chrono::Local::now();
        loop {
            let current_epoch = self.read_epoch().await?;
            if current_epoch >= target_epoch {
                return Ok(());
            }
            let now = chrono::Local::now();
            if (now - start_time) > timeout {
                return Err(Error::Timeout);
            }
            // TODO(tamer): make this configurable.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
