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
    epoch_provider::EpochProvider, error::Error, for_testing::in_memory_wal::InMemoryWal,
    persistence::Persistence, range_manager::RangeManager,
};
use flatbuf::rangeserver_flatbuffers::range_server::*;

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
    ) -> Result<Arc<RangeManager<P, E, InMemoryWal>>, Error> {
        if let Some(assignee) = self.assignment_oracle.host_of_range(id) {
            if assignee.identity != self.identity {
                return Err(Error::RangeIsNotLoaded);
            }
        } else {
            return Err(Error::RangeIsNotLoaded);
        }

        {
            let range_table = self.loaded_ranges.read().await;
            match (*range_table).get(&id.range_id) {
                Some(r) => return Ok(r.clone()),
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

        rm.load().await?;
        {
            let mut range_table = self.loaded_ranges.write().await;
            (*range_table).insert(id.range_id, rm.clone());
        };
        Ok(rm.clone())
    }

    async fn get_inner(
        &self,
        request: GetRequest<'_>,
    ) -> Result<(i64, HashMap<Bytes, Bytes>), Error> {
        let range_id = match request.range_id() {
            None => return Err(Error::InvalidRequestFormat),
            Some(id) => id,
        };
        let range_id = match util::flatbuf::deserialize_range_id(&range_id) {
            None => return Err(Error::InvalidRequestFormat),
            Some(id) => id,
        };
        let transaction_id = match request.transaction_id() {
            None => return Err(Error::InvalidRequestFormat),
            Some(id) => util::flatbuf::deserialize_uuid(id),
        };
        match request.request_id() {
            None => return Err(Error::InvalidRequestFormat),
            Some(_) => (),
        }
        let rm = self.maybe_load_and_get_range(&range_id).await?;
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
                let get_result = rm.get(tx.clone(), key.clone()).await?;
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
        Ok((leader_sequence_number, reads))
    }

    pub async fn get<'a>(
        &self,
        fbb: &'a mut FlatBufferBuilder<'a>,
        request: GetRequest<'_>,
    ) -> GetResponse<'a> {
        let fbb_root = match request.request_id() {
            None => GetResponse::create(
                fbb,
                &GetResponseArgs {
                    request_id: None,
                    status: Status::InvalidRequestFormat,
                    leader_sequence_number: 0,
                    records: None,
                },
            ),
            Some(req_id) => {
                let request_id = util::flatbuf::deserialize_uuid(req_id);
                let read_result = self.get_inner(request).await;

                // Construct the response
                let mut records_vector = Vec::new();
                let (status, leader_sequence_number) = match read_result {
                    Err(e) => (e.to_flatbuf_status(), -1),
                    Ok((leader_sequence_number, reads)) => {
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
                        (Status::Ok, leader_sequence_number)
                    }
                };
                let records = Some(fbb.create_vector(&records_vector));
                let request_id = Some(Uuidu128::create(
                    fbb,
                    &util::flatbuf::serialize_uuid(request_id),
                ));
                GetResponse::create(
                    fbb,
                    &GetResponseArgs {
                        request_id,
                        status,
                        leader_sequence_number,
                        records,
                    },
                )
            }
        };

        fbb.finish(fbb_root, None);
        let get_response_bytes = fbb.finished_data();
        flatbuffers::root::<GetResponse<'a>>(get_response_bytes).unwrap()
    }

    async fn prepare_inner(
        &self,
        request: PrepareRequest<'_>,
    ) -> Result<crate::range_manager::PrepareResult, Error> {
        let range_id = match request.range_id() {
            None => return Err(Error::InvalidRequestFormat),
            Some(id) => id,
        };
        let range_id = match util::flatbuf::deserialize_range_id(&range_id) {
            None => return Err(Error::InvalidRequestFormat),
            Some(id) => id,
        };
        let transaction_id = match request.transaction_id() {
            None => return Err(Error::InvalidRequestFormat),
            Some(id) => util::flatbuf::deserialize_uuid(id),
        };
        let rm = self.maybe_load_and_get_range(&range_id).await?;
        // TODO: don't create a new transaction info from the Get request. There should be a transactions table.
        let tx = Arc::new(TransactionInfo { id: transaction_id });
        rm.prepare(tx.clone(), request).await
    }

    pub async fn prepare<'a>(
        &self,
        fbb: &'a mut FlatBufferBuilder<'a>,
        request: PrepareRequest<'_>,
    ) -> PrepareResponse<'a> {
        let fbb_root = match request.request_id() {
            None => PrepareResponse::create(
                fbb,
                &PrepareResponseArgs {
                    request_id: None,
                    status: Status::InvalidRequestFormat,
                    epoch_lease: None,
                    highest_known_epoch: 0,
                },
            ),
            Some(req_id) => {
                let request_id = util::flatbuf::deserialize_uuid(req_id);

                let prepare_result = self.prepare_inner(request).await;

                // Construct the response.
                let (status, epoch_lease, highest_known_epoch) = match prepare_result {
                    Err(e) => (e.to_flatbuf_status(), None, 0),
                    Ok(prepare_result) => {
                        let epoch_lease = Some(EpochLease::create(
                            fbb,
                            &EpochLeaseArgs {
                                lower_bound_inclusive: prepare_result.epoch_lease.0,
                                upper_bound_inclusive: prepare_result.epoch_lease.1,
                            },
                        ));
                        (Status::Ok, epoch_lease, prepare_result.highest_known_epoch)
                    }
                };
                let request_id = Some(Uuidu128::create(
                    fbb,
                    &util::flatbuf::serialize_uuid(request_id),
                ));
                PrepareResponse::create(
                    fbb,
                    &PrepareResponseArgs {
                        request_id,
                        status,
                        epoch_lease,
                        highest_known_epoch,
                    },
                )
            }
        };

        fbb.finish(fbb_root, None);
        let prepare_response_bytes = fbb.finished_data();
        flatbuffers::root::<PrepareResponse<'a>>(prepare_response_bytes).unwrap()
    }

    async fn commit_inner(&self, request: CommitRequest<'_>) -> Result<(), Error> {
        let range_id = match request.range_id() {
            None => return Err(Error::InvalidRequestFormat),
            Some(id) => id,
        };
        let range_id = match util::flatbuf::deserialize_range_id(&range_id) {
            None => return Err(Error::InvalidRequestFormat),
            Some(id) => id,
        };
        let transaction_id = match request.transaction_id() {
            None => return Err(Error::InvalidRequestFormat),
            Some(id) => util::flatbuf::deserialize_uuid(id),
        };
        let rm = self.maybe_load_and_get_range(&range_id).await?;
        // TODO: don't create a new transaction info from the Get request. There should be a transactions table.
        let tx = Arc::new(TransactionInfo { id: transaction_id });
        rm.commit(tx.clone(), request).await
    }

    async fn commit<'a>(
        &self,
        fbb: &'a mut FlatBufferBuilder<'a>,
        request: CommitRequest<'_>,
    ) -> CommitResponse<'a> {
        let fbb_root = match request.request_id() {
            None => CommitResponse::create(
                fbb,
                &CommitResponseArgs {
                    request_id: None,
                    status: Status::InvalidRequestFormat,
                },
            ),
            Some(req_id) => {
                let request_id = util::flatbuf::deserialize_uuid(req_id);
                let status = match self.commit_inner(request).await {
                    Err(e) => e.to_flatbuf_status(),
                    Ok(()) => Status::Ok,
                };
                // Construct the response.
                let request_id = Some(Uuidu128::create(
                    fbb,
                    &util::flatbuf::serialize_uuid(request_id),
                ));
                CommitResponse::create(fbb, &CommitResponseArgs { request_id, status })
            }
        };
        fbb.finish(fbb_root, None);
        let commit_response_bytes = fbb.finished_data();
        flatbuffers::root::<CommitResponse<'a>>(commit_response_bytes).unwrap()
    }

    async fn abort_inner(&self, request: AbortRequest<'_>) -> Result<(), Error> {
        let range_id = match request.range_id() {
            None => return Err(Error::InvalidRequestFormat),
            Some(id) => id,
        };
        let range_id = match util::flatbuf::deserialize_range_id(&range_id) {
            None => return Err(Error::InvalidRequestFormat),
            Some(id) => id,
        };
        let transaction_id = match request.transaction_id() {
            None => return Err(Error::InvalidRequestFormat),
            Some(id) => util::flatbuf::deserialize_uuid(id),
        };
        let rm = self.maybe_load_and_get_range(&range_id).await?;
        // TODO: don't create a new transaction info from the Get request. There should be a transactions table.
        let tx = Arc::new(TransactionInfo { id: transaction_id });
        rm.abort(tx.clone(), request).await
    }

    async fn abort<'a>(
        &self,
        fbb: &'a mut FlatBufferBuilder<'a>,
        request: AbortRequest<'_>,
    ) -> AbortResponse<'a> {
        let fbb_root = match request.request_id() {
            None => AbortResponse::create(
                fbb,
                &AbortResponseArgs {
                    request_id: None,
                    status: Status::InvalidRequestFormat,
                },
            ),
            Some(req_id) => {
                let request_id = util::flatbuf::deserialize_uuid(req_id);
                let status = match self.abort_inner(request).await {
                    Err(e) => e.to_flatbuf_status(),
                    Ok(()) => Status::Ok,
                };
                // Construct the response.
                let request_id = Some(Uuidu128::create(
                    fbb,
                    &util::flatbuf::serialize_uuid(request_id),
                ));
                AbortResponse::create(fbb, &AbortResponseArgs { request_id, status })
            }
        };
        fbb.finish(fbb_root, None);
        let abort_response_bytes = fbb.finished_data();
        flatbuffers::root::<AbortResponse<'a>>(abort_response_bytes).unwrap()
    }
}
