use bytes::Bytes;
use common::network::fast_network::FastNetwork;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::Prefix;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use tonic::{transport::Server as TServer, Request, Response, Status as TStatus};

use common::util;
use common::{config::Config, full_range_id::FullRangeId, host_info::HostInfo};
use flatbuffers::FlatBufferBuilder;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use uuid::Uuid;

use crate::transaction_info::TransactionInfo;
use crate::warden_handler::WardenHandler;
use crate::{
    epoch_provider::EpochProvider, error::Error, for_testing::in_memory_wal::InMemoryWal,
    range_manager::RangeManager, storage::Storage,
};
use flatbuf::rangeserver_flatbuffers::range_server::TransactionInfo as FlatbufTransactionInfo;
use flatbuf::rangeserver_flatbuffers::range_server::*;

use proto::rangeserver::range_server_server::{RangeServer, RangeServerServer};
use proto::rangeserver::{PrefetchRequest, PrefetchResponse};

use crate::prefetching_buffer::PrefetchingBuffer;

pub mod rangeserver {
    include!("../../proto/target/rangeserver/rangeserver.rs");
}

#[derive(Clone, Debug, Default)]
struct ProtoServer {
    buffer: Arc<Mutex<PrefetchingBuffer>>,
}

#[tonic::async_trait]
impl RangeServer for ProtoServer {
    async fn prefetch(
        &self,
        request: Request<PrefetchRequest>, // Accept request of type PrefetchRequest
    ) -> Result<Response<PrefetchResponse>, TStatus> {
        // Return an instance of type PrefetchResponse
        println!("Got a request: {:?}", request);

        // TODO: Where do we get transaction_id from?
        let transaction_id = Uuid::new_v4();

        // Extract requested key from the request
        let key = Bytes::from(request.get_ref().range_key[0].key.clone());

        // Call process_prefetch_request
        // TODO: Don't lock the entire buffer. Use separate locks for separate parts
        let mut buffer = self.buffer.lock().await;
        match buffer.process_prefetch_request(transaction_id, key).await {
            Ok(_) => {
                let reply = PrefetchResponse {
                    status: format!("Prefetch request processed successfully"),
                };
                Ok(Response::new(reply)) // Send back response
            }
            Err(_) => {
                Err(TStatus::internal("Failed to process prefetch request")) // Handle error
            }
        }
    }
}

pub struct Server<S, E>
where
    S: Storage,
    E: EpochProvider,
{
    config: Config,
    storage: Arc<S>,
    epoch_provider: Arc<E>,
    warden_handler: WardenHandler,
    bg_runtime: tokio::runtime::Handle,
    // TODO: parameterize the WAL implementation too.
    loaded_ranges: RwLock<HashMap<Uuid, Arc<RangeManager<S, E, InMemoryWal>>>>,
    transaction_table: RwLock<HashMap<Uuid, Arc<TransactionInfo>>>,
}

type DynamicErr = Box<dyn std::error::Error + Sync + Send + 'static>;

impl<S, E> Server<S, E>
where
    S: Storage,
    E: EpochProvider,
{
    pub fn new(
        config: Config,
        host_info: HostInfo,
        storage: Arc<S>,
        epoch_provider: Arc<E>,
        bg_runtime: tokio::runtime::Handle,
    ) -> Arc<Self> {
        let warden_handler = WardenHandler::new(&config, &host_info);
        Arc::new(Server {
            config,
            storage,
            epoch_provider,
            warden_handler,
            bg_runtime,
            loaded_ranges: RwLock::new(HashMap::new()),
            transaction_table: RwLock::new(HashMap::new()),
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

    async fn maybe_load_and_get_range(
        &self,
        id: &FullRangeId,
    ) -> Result<Arc<RangeManager<S, E, InMemoryWal>>, Error> {
        {
            // Fast path when range has already been loaded.
            let range_table = self.loaded_ranges.read().await;
            match (*range_table).get(&id.range_id) {
                Some(r) => return Ok(r.clone()),
                None => (),
            }
        };

        if !self.warden_handler.is_assigned(id).await {
            return Err(Error::RangeIsNotLoaded);
        }

        let rm = {
            let mut range_table = self.loaded_ranges.write().await;
            match (range_table).get(&id.range_id) {
                Some(r) => r.clone(),
                None => {
                    let rm = RangeManager::new(
                        id.clone(),
                        self.config.clone(),
                        self.storage.clone(),
                        self.epoch_provider.clone(),
                        InMemoryWal::new(),
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
        self.maybe_start_transaction(transaction_id, request.transaction_info())
            .await;
        let rm = self.maybe_load_and_get_range(&range_id).await?;
        let tx = self.get_transaction_info(transaction_id).await?;
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
                            let value = fbb.create_vector(v.to_vec().as_slice());
                            records_vector.push(Record::create(
                                &mut fbb,
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
        self.send_response(network, sender, MessageType::Abort, fbb.finished_data())?;
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
                        tokio::spawn(async move{
                            let _ = Self::handle_message(server, fast_network, sender, msg).await;
                            // TODO log any error here.
                        });

                    }
                }
            }
        };
    }

    pub async fn start(
        server: Arc<Self>,
        fast_network: Arc<dyn FastNetwork>,
        cancellation_token: CancellationToken,
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

        // Pull the gRPC server address and define the service
        let addr = server
            .config
            .range_server
            .proto_server_addr
            .parse()
            .unwrap();

        // Create buffer to hold prefetch requests
        let mut prefetching_buffer = PrefetchingBuffer {
            prefetch_store: BTreeMap::new(),
            key_state: HashMap::new(),
            transaction_keys: HashMap::new(),
            key_transactions: HashMap::new(),
            key_state_watcher: HashMap::new(),
            key_state_sender: HashMap::new(),
        };
        let buffer = Arc::new(Mutex::new(prefetching_buffer));
        let prefetch = ProtoServer { buffer };

        // Spawn the gRPC server as a separate task
        server.bg_runtime.spawn(async move {
            if let Err(e) = TServer::builder()
                .add_service(RangeServerServer::new(prefetch))
                .serve(addr)
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
            let _ = Self::network_server_loop(server, fast_network, cancellation_token);
            println!("Network server loop exited!")
        });
        Ok(res)
    }
}

#[cfg(test)]
pub mod tests {
    use common::config::{RangeServerConfig, RegionConfig};
    use common::network::for_testing::udp_fast_network::UdpFastNetwork;
    use common::region::{Region, Zone};
    use core::time;
    use std::net::UdpSocket;

    use super::*;

    use crate::for_testing::epoch_provider::EpochProvider;
    use crate::for_testing::mock_warden::MockWarden;
    use crate::storage::cassandra::Cassandra;
    type Server = super::Server<Cassandra, EpochProvider>;

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
        storage_context: crate::storage::cassandra::tests::TestContext,
    }

    async fn init() -> TestContext {
        let fast_network = Arc::new(UdpFastNetwork::new(UdpSocket::bind("127.0.0.1:0").unwrap()));
        let epoch_provider = Arc::new(EpochProvider::new());
        let storage_context: crate::storage::cassandra::tests::TestContext =
            crate::storage::cassandra::tests::init().await;
        let cassandra = storage_context.cassandra.clone();
        let mock_warden = MockWarden::new();
        let warden_address = mock_warden.start().await.unwrap();
        let region = Region {
            cloud: None,
            name: "test-region".into(),
        };
        let zone = Zone {
            region: region.clone(),
            name: "a".into(),
        };
        let region_config = RegionConfig {
            warden_address: warden_address.to_string(),
        };
        let mut config = Config {
            range_server: RangeServerConfig {
                range_maintenance_duration: time::Duration::from_secs(1),
                proto_server_addr: String::from("127.0.0.1:50051"),
            },
            regions: std::collections::HashMap::new(),
        };
        config.regions.insert(region, region_config);
        let identity: String = "test_server".into();
        let host_info = HostInfo {
            identity: identity.clone(),
            address: "127.0.0.1:10001".parse().unwrap(),
            zone,
        };
        let server = Server::new(
            config,
            host_info,
            cassandra,
            epoch_provider,
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
        }
    }

    #[tokio::test]
    async fn range_server_connects_to_warden() {
        let context = init().await;
        let cancellation_token = CancellationToken::new();
        let ch = Server::start(
            context.server.clone(),
            context.fast_network.clone(),
            cancellation_token.clone(),
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
        let range_id = FullRangeId {
            keyspace_id: context.storage_context.keyspace_id,
            range_id: context.storage_context.range_id,
        };
        let ch = Server::start(
            context.server.clone(),
            context.fast_network.clone(),
            cancellation_token.clone(),
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
        // Yield so server can process the update.
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert!(!(context.server.warden_handler.is_assigned(&range_id).await));
        assert!(!context.server.is_assigned(&range_id).await);
        cancellation_token.cancel();
        ch.await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn initial_warden_update() {
        let context = init().await;
        let cancellation_token = CancellationToken::new();
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
