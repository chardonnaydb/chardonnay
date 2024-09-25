use bytes::Bytes;
use common::network::fast_network::FastNetwork;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use tonic::{transport::Server as TServer, Request, Response, Status as TStatus};

use common::keyspace_id::KeyspaceId;
use common::util;
use common::{config::Config, full_range_id::FullRangeId, host_info::HostInfo};
use flatbuffers::FlatBufferBuilder;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio_util::sync::CancellationToken;

use uuid::Uuid;

use crate::range_manager::r#impl::RangeManager;
use crate::range_manager::RangeManager as RangeManagerTrait;
use crate::transaction_info::TransactionInfo;
use crate::warden_handler::WardenHandler;
use crate::{
    cache::Cache, cache::CacheOptions, epoch_supplier::EpochSupplier, error::Error,
    for_testing::in_memory_wal::InMemoryWal, storage::Storage,
};
use flatbuf::rangeserver_flatbuffers::range_server::TransactionInfo as FlatbufTransactionInfo;
use flatbuf::rangeserver_flatbuffers::range_server::*;

use proto::rangeserver::range_server_server::{RangeServer, RangeServerServer};
use proto::rangeserver::{PrefetchRequest, PrefetchResponse};

use crate::prefetching_buffer::PrefetchingBuffer;

#[derive(Clone)]
struct ProtoServer<S, C>
where
    S: Storage,
    C: Cache,
{
    parent_server: Arc<Server<S, C>>,
}

#[tonic::async_trait]
impl<S, C> RangeServer for ProtoServer<S, C>
where
    S: Storage,
    C: Cache,
{
    async fn prefetch(
        &self,
        request: Request<PrefetchRequest>, // Accept request of type PrefetchRequest
    ) -> Result<Response<PrefetchResponse>, TStatus> {
        // Return an instance of type PrefetchResponse
        println!("Got a request: {:?}", request);

        // Extract transaction_id, requested key, keyspace_id, and range_id from the request
        let transaction_id = Uuid::parse_str(&request.get_ref().transaction_id).map_err(|e| {
            TStatus::internal(format!(
                "Transaction id is not in the correct format: {:?}",
                e
            ))
        })?;

        let key = Bytes::from(request.get_ref().range_key[0].key.clone());
        let range = request.get_ref().range_key[0].range.as_ref().unwrap();
        let keyspace_id = KeyspaceId::new(Uuid::parse_str(&range.keyspace_id).map_err(|e| {
            TStatus::internal(format!("Keyspace id is not in the correct format: {:?}", e))
        })?);
        let range_id = Uuid::parse_str(&range.range_id).map_err(|e| {
            TStatus::internal(format!("Range id is not in the correct format: {:?}", e))
        })?;

        let full_range_id = FullRangeId {
            keyspace_id: keyspace_id,
            range_id: range_id,
        };

        let range_manager = self
            .parent_server
            .maybe_load_and_get_range(&full_range_id)
            .await
            .map_err(|e| TStatus::internal(format!("Failed to load range: {:?}", e)))?;

        match range_manager.prefetch(transaction_id, key).await {
            Ok(_) => {
                let reply = PrefetchResponse {
                    status: format!("Prefetch request processed successfully"),
                };
                Ok(Response::new(reply)) // Send back response
            }
            Err(_) => Err(TStatus::internal("Failed to process prefetch request")),
        }
    }
}

pub struct Server<S, C>
where
    S: Storage,
    C: Cache,
{
    config: Config,
    storage: Arc<S>,
    epoch_supplier: Arc<dyn EpochSupplier>,
    warden_handler: WardenHandler,
    bg_runtime: tokio::runtime::Handle,
    // TODO: parameterize the WAL implementation too.
    loaded_ranges: RwLock<HashMap<Uuid, Arc<RangeManager<S, InMemoryWal, C>>>>,
    transaction_table: RwLock<HashMap<Uuid, Arc<TransactionInfo>>>,
    prefetching_buffer: Arc<PrefetchingBuffer>,
}

type DynamicErr = Box<dyn std::error::Error + Sync + Send + 'static>;

impl<S, C> Server<S, C>
where
    S: Storage,
    C: Cache,
{
    pub fn new(
        config: Config,
        host_info: HostInfo,
        storage: Arc<S>,
        epoch_supplier: Arc<dyn EpochSupplier>,
        bg_runtime: tokio::runtime::Handle,
    ) -> Arc<Self> {
        let warden_handler = WardenHandler::new(&config, &host_info, epoch_supplier.clone());
        Arc::new(Server {
            config,
            storage,
            epoch_supplier,
            warden_handler,
            bg_runtime,
            loaded_ranges: RwLock::new(HashMap::new()),
            transaction_table: RwLock::new(HashMap::new()),
            prefetching_buffer: Arc::new(PrefetchingBuffer::new()),
        })
    }

    async fn maybe_start_transaction(&self, id: Uuid, info: Option<FlatbufTransactionInfo<'_>>) {
        let info = match info {
            None => return (),
            Some(info) => info,
        };
        let mut tx_table = self.transaction_table.write().await;
        match (*tx_table).get(&id) {
            Some(_) => return (),
            None => (),
        };
        let overall_timeout = core::time::Duration::from_micros(info.overall_timeout_us() as u64);
        let tx_info = Arc::new(TransactionInfo {
            id,
            started: chrono::Utc::now(), // TODO: Should be set by the client instead.
            overall_timeout,
        });
        tx_table.insert(id, tx_info);
    }

    async fn get_transaction_info(&self, id: Uuid) -> Result<Arc<TransactionInfo>, Error> {
        let tx_table = self.transaction_table.read().await;
        match (*tx_table).get(&id) {
            Some(i) => Ok(i.clone()),
            None => Err(Error::UnknownTransaction),
        }
    }

    async fn remove_transaction(&self, id: Uuid) -> () {
        let mut tx_table = self.transaction_table.write().await;
        (*tx_table).remove(&id);
    }

    async fn maybe_unload_range(&self, id: &FullRangeId) {
        let rm = {
            let mut range_table = self.loaded_ranges.write().await;
            (*range_table).remove(&id.range_id)
        };
        match rm {
            None => (),
            Some(r) => r.unload().await,
        }
    }

    async fn maybe_load_and_get_range_inner(
        &self,
        id: &FullRangeId,
    ) -> Result<Arc<RangeManager<S, InMemoryWal, C>>, Error> {
        {
            // Fast path when range has already been loaded.
            let range_table = self.loaded_ranges.read().await;
            match (*range_table).get(&id.range_id) {
                Some(r) => {
                    r.load().await?;
                    return Ok(r.clone());
                }
                None => (),
            }
        };

        if !self.warden_handler.is_assigned(id).await {
            return Err(Error::RangeIsNotLoaded);
        }

        let rm = {
            let mut range_table = self.loaded_ranges.write().await;
            match (range_table).get(&id.range_id) {
                Some(r) => {
                    r.load().await?;
                    r.clone()
                }
                None => {
                    let rm = RangeManager::new(
                        id.clone(),
                        self.config.clone(),
                        self.storage.clone(),
                        self.epoch_supplier.clone(),
                        InMemoryWal::new(),
                        C::new(CacheOptions::default()).await,
                        self.prefetching_buffer.clone(),
                        self.bg_runtime.clone(),
                    );
                    (range_table).insert(id.range_id, rm.clone());
                    drop(range_table);
                    rm.load().await?;
                    rm.clone()
                }
            }
        };
        Ok(rm.clone())
    }

    async fn maybe_load_and_get_range(
        &self,
        id: &FullRangeId,
    ) -> Result<Arc<RangeManager<S, InMemoryWal, C>>, Error> {
        let res = self.maybe_load_and_get_range_inner(id).await;
        match res {
            Ok(_) => (),
            Err(_) => {
                // An RM load can only be attempted once, so remove from table
                // to force creating a fresh one if the range is still assigned
                // to us.
                let mut range_table = self.loaded_ranges.write().await;
                let remove = match range_table.get(&id.range_id) {
                    None => false,
                    Some(r) => !r.is_unloaded().await,
                };
                if remove {
                    range_table.remove(&id.range_id);
                }
            }
        };
        res
    }

    fn send_response(
        &self,
        fast_network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        msg_type: MessageType,
        msg_payload: &[u8],
    ) -> Result<(), std::io::Error> {
        // TODO: many allocations and copies in this function.
        let mut fbb = FlatBufferBuilder::new();
        let bytes = fbb.create_vector(msg_payload);
        let fb_root = ResponseEnvelope::create(
            &mut fbb,
            &ResponseEnvelopeArgs {
                type_: msg_type,
                bytes: Some(bytes),
            },
        );
        fbb.finish(fb_root, None);
        let response = Bytes::copy_from_slice(fbb.finished_data());
        fast_network.send(sender, response)
    }

    async fn get_inner(
        &self,
        request: GetRequest<'_>,
    ) -> Result<(i64, Vec<(Bytes, Option<Bytes>)>), Error> {
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
        self.maybe_start_transaction(transaction_id, request.transaction_info())
            .await;
        let rm = self.maybe_load_and_get_range(&range_id).await?;
        let tx = self.get_transaction_info(transaction_id).await?;
        let mut leader_sequence_number: i64 = 0;
        let mut reads = Vec::new();

        // Execute the reads
        // TODO: consider providing a batch API on the RM.
        for key in request.keys().iter() {
            for key in key.iter() {
                // TODO: too much copying :(
                let key = Bytes::copy_from_slice(key.k().unwrap().bytes());
                let get_result = rm.get(tx.clone(), key.clone()).await?;
                match get_result.val {
                    None => {
                        reads.push((key, None));
                    }
                    Some(val) => {
                        reads.push((key, Some(val)));
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

    async fn get(
        &self,
        network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        request: GetRequest<'_>,
    ) -> Result<(), DynamicErr> {
        let mut fbb = FlatBufferBuilder::new();
        let fbb_root = match request.request_id() {
            None => GetResponse::create(
                &mut fbb,
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
                            let key = Key::create(&mut fbb, &KeyArgs { k });
                            let value = match v {
                                None => None,
                                Some(v) => Some(fbb.create_vector(v.to_vec().as_slice())),
                            };
                            records_vector.push(Record::create(
                                &mut fbb,
                                &RecordArgs {
                                    key: Some(key),
                                    value: value,
                                },
                            ));
                        }
                        (Status::Ok, leader_sequence_number)
                    }
                };
                let records = Some(fbb.create_vector(&records_vector));
                let request_id = Some(Uuidu128::create(
                    &mut fbb,
                    &util::flatbuf::serialize_uuid(request_id),
                ));
                GetResponse::create(
                    &mut fbb,
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
        self.send_response(network, sender, MessageType::Get, fbb.finished_data())?;
        Ok(())
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
        let tx = self.get_transaction_info(transaction_id).await?;
        rm.prepare(tx.clone(), request).await
    }

    async fn prepare(
        &self,
        network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        request: PrepareRequest<'_>,
    ) -> Result<(), DynamicErr> {
        let mut fbb = FlatBufferBuilder::new();
        let fbb_root = match request.request_id() {
            None => PrepareResponse::create(
                &mut fbb,
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
                            &mut fbb,
                            &EpochLeaseArgs {
                                lower_bound_inclusive: prepare_result.epoch_lease.0,
                                upper_bound_inclusive: prepare_result.epoch_lease.1,
                            },
                        ));
                        (Status::Ok, epoch_lease, prepare_result.highest_known_epoch)
                    }
                };
                let request_id = Some(Uuidu128::create(
                    &mut fbb,
                    &util::flatbuf::serialize_uuid(request_id),
                ));
                PrepareResponse::create(
                    &mut fbb,
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
        self.send_response(network, sender, MessageType::Prepare, fbb.finished_data())?;
        Ok(())
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
        let tx = self.get_transaction_info(transaction_id).await?;
        rm.commit(tx.clone(), request).await?;
        self.remove_transaction(transaction_id).await;
        Ok(())
    }

    pub async fn commit(
        &self,
        network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        request: CommitRequest<'_>,
    ) -> Result<(), DynamicErr> {
        let mut fbb = FlatBufferBuilder::new();
        let fbb_root = match request.request_id() {
            None => CommitResponse::create(
                &mut fbb,
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
                    &mut fbb,
                    &util::flatbuf::serialize_uuid(request_id),
                ));
                CommitResponse::create(&mut fbb, &CommitResponseArgs { request_id, status })
            }
        };
        fbb.finish(fbb_root, None);
        self.send_response(network, sender, MessageType::Commit, fbb.finished_data())?;
        Ok(())
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
        let tx = self.get_transaction_info(transaction_id).await?;
        rm.abort(tx.clone(), request).await?;
        self.remove_transaction(transaction_id).await;
        Ok(())
    }

    pub async fn abort(
        &self,
        network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        request: AbortRequest<'_>,
    ) -> Result<(), DynamicErr> {
        let mut fbb = FlatBufferBuilder::new();
        let fbb_root = match request.request_id() {
            None => AbortResponse::create(
                &mut fbb,
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
                    &mut fbb,
                    &util::flatbuf::serialize_uuid(request_id),
                ));
                AbortResponse::create(&mut fbb, &AbortResponseArgs { request_id, status })
            }
        };
        fbb.finish(fbb_root, None);
        self.send_response(network, sender, MessageType::Abort, fbb.finished_data())?;
        Ok(())
    }

    async fn warden_update_loop(
        server: Arc<Self>,
        mut receiver: UnboundedReceiver<crate::warden_handler::WardenUpdate>,
        cancellation_token: CancellationToken,
    ) -> Result<(), DynamicErr> {
        loop {
            let () = tokio::select! {
                () = cancellation_token.cancelled() => {
                    server.warden_handler.stop().await;
                    return Ok(())
                }
                maybe_update = receiver.recv() => {
                    match maybe_update {
                        None => {
                            return Err("connection closed with warden handler!".into());
                        }
                        Some(update) => {
                            match &update {
                                crate::warden_handler::WardenUpdate::LoadRange(id) => {

                                    let id = id.clone();
                                    let server = server.clone();
                                    tokio::spawn (async move
                                        {
                                            // TODO: handle errors here
                                            server.maybe_load_and_get_range(&id).await
                                        });
                                }
                                crate::warden_handler::WardenUpdate::UnloadRange(id) => {
                                    server.maybe_unload_range(id).await
                                }
                            }
                        }
                    }
                }

            };
        }
    }

    async fn handle_message(
        server: Arc<Self>,
        fast_network: Arc<dyn FastNetwork>,
        sender: SocketAddr,
        msg: Bytes,
    ) -> Result<(), DynamicErr> {
        // TODO: gracefully handle malformed messages instead of unwrapping and crashing.
        let msg = msg.to_vec();
        let envelope = flatbuffers::root::<RequestEnvelope>(msg.as_slice())?;
        match envelope.type_() {
            MessageType::Get => {
                let get_msg = flatbuffers::root::<GetRequest>(envelope.bytes().unwrap().bytes())?;
                server.get(fast_network.clone(), sender, get_msg).await?
            }
            MessageType::Prepare => {
                let prepare_msg =
                    flatbuffers::root::<PrepareRequest>(envelope.bytes().unwrap().bytes())?;
                server
                    .prepare(fast_network.clone(), sender, prepare_msg)
                    .await?
            }
            MessageType::Abort => {
                let abort_msg =
                    flatbuffers::root::<AbortRequest>(envelope.bytes().unwrap().bytes())?;
                server
                    .abort(fast_network.clone(), sender, abort_msg)
                    .await?
            }
            MessageType::Commit => {
                let commit_msg =
                    flatbuffers::root::<CommitRequest>(envelope.bytes().unwrap().bytes())?;
                server
                    .commit(fast_network.clone(), sender, commit_msg)
                    .await?
            }
            _ => (), // TODO: return and log unknown message type error.
        };
        Ok(())
    }

    async fn network_server_loop(
        server: Arc<Self>,
        fast_network: Arc<dyn FastNetwork>,
        cancellation_token: CancellationToken,
    ) {
        let mut network_receiver = fast_network.listen_default();
        loop {
            let () = tokio::select! {
                () = cancellation_token.cancelled() => {
                    return ()
                }
                maybe_message = network_receiver.recv() => {
                    match maybe_message {
                        None => {
                            println!("fast network closed unexpectedly!");
                            cancellation_token.cancel()
                        }
                        Some((sender, msg)) => {
                            let server = server.clone();
                            let fast_network = fast_network.clone();
                            tokio::spawn(async move{
                                let _ = Self::handle_message(server, fast_network, sender, msg).await;
                                // TODO log any error here.
                            });

                        }
                    }
                }
            };
        }
    }

    pub async fn start(
        server: Arc<Self>,
        fast_network: Arc<dyn FastNetwork>,
        cancellation_token: CancellationToken,
        proto_server_listener: TcpListener,
    ) -> Result<oneshot::Receiver<Result<(), DynamicErr>>, DynamicErr> {
        let (warden_s, warden_r) = mpsc::unbounded_channel();
        let server_clone = server.clone();
        let cancellation_token_for_warden_loop = cancellation_token.clone();
        server.bg_runtime.spawn(async move {
            let _ = Self::warden_update_loop(
                server_clone,
                warden_r,
                cancellation_token_for_warden_loop,
            )
            .await;
            println!("Warden update loop exited!")
        });

        let prefetch = ProtoServer {
            parent_server: server.clone(),
        };

        // Spawn the gRPC server as a separate task
        server.bg_runtime.spawn(async move {
            if let Err(e) = TServer::builder()
                .add_service(RangeServerServer::new(prefetch))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    proto_server_listener,
                ))
                .await
            {
                println!("Server error: {}", e);
            }
        });

        let server_ref = server.clone();
        let res = server
            .bg_runtime
            .spawn(async move { server_ref.warden_handler.start(warden_s).await })
            .await??;

        tokio::spawn(async move {
            let _ = Self::network_server_loop(server, fast_network, cancellation_token).await;
            println!("Network server loop exited!")
        });
        Ok(res)
    }
}

#[cfg(test)]
pub mod tests {
    use crate::cache::memtabledb::MemTableDB;
    use crate::epoch_supplier::EpochSupplier as Trait;
    use common::config::{CassandraConfig, EpochConfig, RangeServerConfig, RegionConfig};
    use common::network::for_testing::udp_fast_network::UdpFastNetwork;
    use common::region::{Region, Zone};
    use core::time;
    use std::collections::HashSet;
    use std::net::UdpSocket;

    use super::*;

    use crate::for_testing::epoch_supplier::EpochSupplier;
    use crate::for_testing::mock_warden::MockWarden;
    use crate::storage::cassandra::Cassandra;
    type Server = super::Server<Cassandra, MemTableDB>;

    impl Server {
        async fn is_assigned(&self, range_id: &FullRangeId) -> bool {
            let range_table = self.loaded_ranges.read().await;
            range_table.contains_key(&range_id.range_id)
        }
    }

    struct TestContext {
        server: Arc<Server>,
        fast_network: Arc<dyn FastNetwork>,
        identity: String,
        mock_warden: MockWarden,
        storage_context: crate::storage::cassandra::for_testing::TestContext,
        proto_server_listener: TcpListener,
    }

    async fn init() -> TestContext {
        let fast_network = Arc::new(UdpFastNetwork::new(UdpSocket::bind("127.0.0.1:0").unwrap()));
        let epoch_supplier = Arc::new(EpochSupplier::new());
        let storage_context: crate::storage::cassandra::for_testing::TestContext =
            crate::storage::cassandra::for_testing::init().await;
        let cassandra = storage_context.cassandra.clone();
        let mock_warden = MockWarden::new();
        let warden_address = mock_warden.start(None).await.unwrap();
        let proto_server_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let region = Region {
            cloud: None,
            name: "test-region".into(),
        };
        let zone = Zone {
            region: region.clone(),
            name: "a".into(),
        };
        let region_config = RegionConfig {
            warden_address: warden_address,
            epoch_publishers: HashSet::new(),
        };
        let epoch_config = EpochConfig {
            // Not used in these tests.
            proto_server_addr: "127.0.0.1:50052".parse().unwrap(),
        };
        let mut config = Config {
            range_server: RangeServerConfig {
                range_maintenance_duration: time::Duration::from_secs(1),
                proto_server_port: 50054,
                fast_network_port: 50055,
                // proto_server_addr: proto_server_listener.local_addr().unwrap(),
            },
            cassandra: CassandraConfig {
                cql_addr: "127.0.0.1:9042".parse().unwrap(),
            },
            regions: std::collections::HashMap::new(),
            epoch: epoch_config,
        };
        config.regions.insert(region, region_config);
        let identity: String = "test_server".into();
        let host_info = HostInfo {
            identity: identity.clone(),
            address: "127.0.0.1:50054".parse().unwrap(),
            zone,
            warden_connection_epoch: epoch_supplier.read_epoch().await.unwrap(),
        };
        let server = Server::new(
            config,
            host_info,
            cassandra,
            epoch_supplier,
            tokio::runtime::Handle::current().clone(),
        );

        // Give some delay so the mock warden starts.
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        TestContext {
            server,
            fast_network,
            identity,
            storage_context,
            mock_warden,
            proto_server_listener,
        }
    }

    #[tokio::test]
    async fn range_server_connects_to_warden() {
        let context = init().await;
        let cancellation_token = CancellationToken::new();
        let proto_server_listener = context.proto_server_listener;
        let ch = Server::start(
            context.server.clone(),
            context.fast_network.clone(),
            cancellation_token.clone(),
            proto_server_listener,
        )
        .await
        .unwrap();
        while !context.mock_warden.is_connected(&context.identity).await {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
        // Disconnect from warden -- should reconnect automatically.
        context.mock_warden.disconnect(&context.identity).await;
        assert!(!context.mock_warden.is_connected(&context.identity).await);
        while !context.mock_warden.is_connected(&context.identity).await {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
        cancellation_token.cancel();
        ch.await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn incremental_load_unload() {
        let context = init().await;
        let cancellation_token = CancellationToken::new();
        let proto_server_listener = context.proto_server_listener;
        let range_id = FullRangeId {
            keyspace_id: context.storage_context.keyspace_id,
            range_id: context.storage_context.range_id,
        };

        let ch = Server::start(
            context.server.clone(),
            context.fast_network.clone(),
            cancellation_token.clone(),
            proto_server_listener,
        )
        .await
        .unwrap();
        while !context.mock_warden.is_connected(&context.identity).await {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
        assert!(!(context.server.warden_handler.is_assigned(&range_id).await));
        assert!(!context.server.is_assigned(&range_id).await);
        context
            .mock_warden
            .assign(&range_id, &context.identity)
            .await;
        // Yield so server can process the update.
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert!((context.server.warden_handler.is_assigned(&range_id).await));
        assert!(context.server.is_assigned(&range_id).await);
        context.mock_warden.unassign(&range_id).await;
        while context.server.warden_handler.is_assigned(&range_id).await {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
        assert!(!context.server.is_assigned(&range_id).await);
        cancellation_token.cancel();
        ch.await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn initial_warden_update() {
        let context = init().await;
        let cancellation_token = CancellationToken::new();
        let proto_server_listener = context.proto_server_listener;
        let range_id = FullRangeId {
            keyspace_id: context.storage_context.keyspace_id,
            range_id: context.storage_context.range_id,
        };
        context
            .mock_warden
            .assign(&range_id, &context.identity)
            .await;

        let ch = Server::start(
            context.server.clone(),
            context.fast_network.clone(),
            cancellation_token.clone(),
            proto_server_listener,
        )
        .await
        .unwrap();
        while !context.mock_warden.is_connected(&context.identity).await {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        assert!((context.server.warden_handler.is_assigned(&range_id).await));
        assert!(context.server.is_assigned(&range_id).await);
        context.mock_warden.unassign(&range_id).await;
        // Yield so server can process the update.
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert!(!(context.server.warden_handler.is_assigned(&range_id).await));
        assert!(!context.server.is_assigned(&range_id).await);
        cancellation_token.cancel();
        ch.await.unwrap().unwrap()
    }
}
