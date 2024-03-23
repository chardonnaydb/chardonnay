use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

use common::util;
use common::{
    config::Config, full_range_id::FullRangeId,
    membership::range_assignment_oracle::RangeAssignmentOracle,
};
use flatbuffers::FlatBufferBuilder;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::transaction_info::TransactionInfo;
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
        id: &FullRangeId,
    ) -> Arc<RangeManager<P, E, InMemoryWal>> {
        // TODO: error handling, no panics!
        if let Some(assignee) = self.assignment_oracle.host_of_range(id) {
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
            id.clone(),
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

    async fn get<'a>(
        &self,
        fbb: &'a mut FlatBufferBuilder<'a>,
        request: GetRequest<'_>,
    ) -> GetResponse<'a> {
        let range_id = request.range_id().unwrap();
        let range_id = util::flatbuf::deserialize_range_id(&range_id).unwrap();
        let request_id = util::flatbuf::deserialize_uuid(request.request_id().unwrap());
        let rm = self.maybe_load_and_get_range(&range_id).await;
        let transaction_id = util::flatbuf::deserialize_uuid(request.transaction_id().unwrap());
        // TODO: don't create a new transaction info from the Get request. There should be a transactions table.
        let tx = Arc::new(TransactionInfo { id: transaction_id });
        let mut leader_sequence_number: i64 = 0;
        let mut reads: HashMap<Bytes, Bytes> = HashMap::new();

        // Execute the reads
        // TODO: consider providing a batch API on the RM.
        for key in request.keys().iter() {
            for key in key.iter() {
                // TODO: too much copying :(
                let key = Bytes::copy_from_slice(key.k().unwrap().bytes());
                let get_result = rm.get(tx.clone(), key.clone()).await.unwrap();
                match get_result.val {
                    None => (),
                    Some(val) => {
                        reads.insert(key, val);
                        ()
                    }
                };
                if leader_sequence_number == 0 {
                    leader_sequence_number = get_result.leader_sequence_number;
                } else if leader_sequence_number != get_result.leader_sequence_number {
                    // This can happen if the range got loaded and unloaded between gets. A transaction cannot
                    // observe two different leaders for the same range so set the sequence number to an invalid
                    // value so the coordinator knows to abort.
                    leader_sequence_number = -1;
                }
            }
        }

        // Construct the response
        let mut records_vector = Vec::new();
        for (k, v) in reads {
            let k = Some(fbb.create_vector(k.to_vec().as_slice()));
            let key = Key::create(fbb, &KeyArgs { k });
            let value = fbb.create_vector(v.to_vec().as_slice());
            records_vector.push(Record::create(
                fbb,
                &RecordArgs {
                    key: Some(key),
                    value: Some(value),
                },
            ));
        }
        let records = Some(fbb.create_vector(&records_vector));
        let request_id = Some(Uuidu128::create(
            fbb,
            &util::flatbuf::serialize_uuid(request_id),
        ));
        let fbb_root = GetResponse::create(
            fbb,
            &GetResponseArgs {
                leader_sequence_number,
                request_id,
                records,
            },
        );
        fbb.finish(fbb_root, None);
        let prepare_record_bytes = fbb.finished_data();
        flatbuffers::root::<GetResponse<'a>>(prepare_record_bytes).unwrap()
    }

    async fn prepare(&self, request: PrepareRequest<'_>) -> Result<(), Error> {
        todo!();
    }
}
