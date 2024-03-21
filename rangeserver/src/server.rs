use std::collections::HashMap;
use std::sync::Arc;

use common::{
    config::Config, full_range_id::FullRangeId,
    membership::range_assignment_oracle::RangeAssignmentOracle,
};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    epoch_provider::EpochProvider, for_testing::in_memory_wal::InMemoryWal,
    persistence::Persistence, range_manager::RangeManager,
};
use flatbuf::rangeserver_flatbuffers::range_server::*;

pub enum Error {
    Unknown,
}

pub struct Server<P, E, O>
where
    P: Persistence,
    E: EpochProvider,
    O: RangeAssignmentOracle,
{
    identity: String,
    config: Config,
    persistence: Arc<P>,
    epoch_provider: Arc<E>,
    assignment_oracle: O,
    // TODO: parameterize the WAL implementation too.
    loaded_ranges: RwLock<HashMap<Uuid, Arc<RangeManager<P, E, InMemoryWal>>>>,
}

impl<P, E, O> Server<P, E, O>
where
    P: Persistence,
    E: EpochProvider,
    O: RangeAssignmentOracle,
{
    async fn maybe_load_and_get_range(
        &self,
        id: FullRangeId,
    ) -> Arc<RangeManager<P, E, InMemoryWal>> {
        // TODO: error handling, no panics!
        if let Some(assignee) = self.assignment_oracle.host_of_range(&id) {
            if assignee.identity != self.identity {
                panic!("range is not assigned to me!");
            }
        } else {
            panic!("range is not assigned to me!");
        }

        {
            let range_table = self.loaded_ranges.read().await;
            match (*range_table).get(&id.range_id) {
                Some(r) => return r.clone(),
                None => (),
            }
        };
        let rm = RangeManager::new(
            id,
            self.config,
            self.persistence.clone(),
            self.epoch_provider.clone(),
            InMemoryWal::new(),
        );

        rm.load().await.unwrap();
        {
            let mut range_table = self.loaded_ranges.write().await;
            (*range_table).insert(id.range_id, rm.clone());
        };
        rm.clone()
    }

    async fn get(&self, request: GetRequest<'_>) -> GetResponse {
        todo!();
    }

    async fn prepare(&self, request: PrepareRecord<'_>) -> Result<(), Error> {
        todo!();
    }
}
