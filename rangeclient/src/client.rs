use bytes::Bytes;
use common::network::fast_network::FastNetwork;
use common::util;
use common::{full_range_id::FullRangeId, host_info::HostInfo, record::Record};
use flatbuf::rangeserver_flatbuffers::range_server::TransactionInfo as FlatbufTransactionInfo;
use flatbuf::rangeserver_flatbuffers::range_server::*;
use flatbuffers::FlatBufferBuilder;
use rangeserver::error::Error as RangeServerError;
use rangeserver::transaction_info::TransactionInfo;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// Provides an async rpc interface to a specific range server.
pub struct RangeClient {
    fast_network: Arc<dyn FastNetwork>,
    range_server_info: HostInfo,
    // TODO: make more typeful and store more information to e.g. allow timing out.
    outstanding_requests: Mutex<HashMap<Uuid, oneshot::Sender<Bytes>>>,
}

impl RangeClient {
    pub fn new(
        fast_network: Arc<dyn FastNetwork>,
        host_info: HostInfo,
        cancellation_token: CancellationToken,
    ) -> Arc<RangeClient> {
        let rc = Arc::new(RangeClient {
            fast_network,
            range_server_info: host_info,
            outstanding_requests: Mutex::new(HashMap::new()),
        });

        let rc_clone = rc.clone();
        tokio::spawn(async move {
            let _ = Self::network_server_loop(rc_clone, cancellation_token);
            println!("Network server loop exited!")
        });
        rc
    }

    pub async fn get(
        self,
        tx: Arc<TransactionInfo>,
        range_id: &FullRangeId,
        keys: Vec<Bytes>,
    ) -> Result<Vec<Record>, RangeServerError> {
        // TODO: gracefully handle malformed messages instead of unwrapping and crashing.
        // TODO: too much copying :(
        let req_id = Uuid::new_v4();
        let mut fbb = FlatBufferBuilder::new();
        let transaction_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(tx.id),
        ));
        let range_id = Some(util::flatbuf::serialize_range_id(&mut fbb, &range_id));
        let request_id = Some(Uuidu128::create(
            &mut fbb,
            &util::flatbuf::serialize_uuid(req_id),
        ));
        // TODO: only supply the transaction info on the first request to the RS.
        let transaction_info = Some(FlatbufTransactionInfo::create(
            &mut fbb,
            &TransactionInfoArgs {
                overall_timeout_us: tx.overall_timeout.as_micros() as u32,
            },
        ));
        let mut keys_vector = Vec::new();
        for key in keys {
            let k = Some(fbb.create_vector(key.to_vec().as_slice()));
            let key = Key::create(&mut fbb, &KeyArgs { k });
            keys_vector.push(key)
        }
        let keys = Some(fbb.create_vector(&keys_vector));
        let fbb_root = GetRequest::create(
            &mut fbb,
            &GetRequestArgs {
                request_id,
                transaction_id,
                range_id,
                transaction_info,
                keys,
            },
        );
        fbb.finish(fbb_root, None);
        let (tx, rx) = oneshot::channel();
        {
            let mut outstanding_requests = self.outstanding_requests.lock().await;
            outstanding_requests.insert(req_id, tx);
        }
        let get_record_bytes = Bytes::copy_from_slice(fbb.finished_data());
        self.fast_network
            .send(self.range_server_info.address, get_record_bytes)
            .unwrap();
        let response = rx.await.unwrap();
        let msg = response.to_vec();
        let envelope = flatbuffers::root::<ResponseEnvelope>(msg.as_slice()).unwrap();
        match envelope.type_() {
            MessageType::Get => {
                let response_msg =
                    flatbuffers::root::<GetResponse>(envelope.bytes().unwrap().bytes()).unwrap();
                let () = rangeserver::error::Error::from_flatbuf_status(response_msg.status())?;
                let mut result = Vec::new();
                for record in response_msg.records().iter() {
                    for rec in record.iter() {
                        let key = Bytes::copy_from_slice(rec.key().unwrap().k().unwrap().bytes());
                        let val = Bytes::copy_from_slice(rec.value().unwrap().bytes());
                        result.push(Record { key, val });
                    }
                }
                return Ok(result);
            }
            _ => return Err(RangeServerError::InvalidRequestFormat),
        }
    }

    fn get_request_id_from_response(msg: Bytes) -> Uuid {
        let msg = msg.to_vec();
        let envelope = flatbuffers::root::<ResponseEnvelope>(msg.as_slice()).unwrap();
        let req_id = match envelope.type_() {
            MessageType::Get => {
                let msg =
                    flatbuffers::root::<GetResponse>(envelope.bytes().unwrap().bytes()).unwrap();
                msg.request_id()
            }
            MessageType::Prepare => {
                let msg = flatbuffers::root::<PrepareResponse>(envelope.bytes().unwrap().bytes())
                    .unwrap();
                msg.request_id()
            }
            MessageType::Abort => {
                let msg =
                    flatbuffers::root::<AbortResponse>(envelope.bytes().unwrap().bytes()).unwrap();
                msg.request_id()
            }
            MessageType::Commit => {
                let msg =
                    flatbuffers::root::<CommitResponse>(envelope.bytes().unwrap().bytes()).unwrap();
                msg.request_id()
            }
            _ => panic!("unknown response message type"), // TODO: return and log unknown message type error.
        };
        common::util::flatbuf::deserialize_uuid(req_id.unwrap())
    }

    async fn network_server_loop(client: Arc<Self>, cancellation_token: CancellationToken) {
        let mut network_receiver = client
            .fast_network
            .register(client.range_server_info.address);
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
                    Some(msg) => {
                        let req_id = Self::get_request_id_from_response(msg.clone());
                        let mut outstanding_requests = client.outstanding_requests.lock().await;
                        let sender = outstanding_requests.remove(&req_id).unwrap();
                        sender.send(msg).unwrap()
                    }
                }
            }
        };
    }
}
