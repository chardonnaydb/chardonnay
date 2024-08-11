use std::{collections::HashMap, sync::Arc};

use crate::error::Error;
use bytes::Bytes;
use common::{host_info::HostInfo, network::fast_network::FastNetwork};
use flatbuf::epoch_publisher_flatbuffers::epoch_publisher::*;
use flatbuffers::FlatBufferBuilder;
use tokio::sync::{oneshot, Mutex};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Provides an async rpc interface to a specific epoch publisher.
pub struct EpochPublisherClient {
    fast_network: Arc<dyn FastNetwork>,
    range_server_info: HostInfo,
    // TODO: make more typeful and store more information to e.g. allow timing out.
    outstanding_requests: Mutex<HashMap<Uuid, oneshot::Sender<Bytes>>>,
}

pub fn serialize_uuid(uuid: Uuid) -> Uuidu128Args {
    let uint128 = uuid.to_u128_le();
    let lower = uint128 as u64;
    let upper = (uint128 >> 64) as u64;
    Uuidu128Args { lower, upper }
}

pub fn deserialize_uuid(uuidf: Uuidu128<'_>) -> Uuid {
    let res: u128 = ((uuidf.upper() as u128) << 64) | (uuidf.lower() as u128);
    Uuid::from_u128_le(res)
}

impl EpochPublisherClient {
    pub fn new(
        fast_network: Arc<dyn FastNetwork>,
        runtime: tokio::runtime::Handle,
        host_info: HostInfo,
        cancellation_token: CancellationToken,
    ) -> Arc<EpochPublisherClient> {
        let pc = Arc::new(EpochPublisherClient {
            fast_network,
            range_server_info: host_info,
            outstanding_requests: Mutex::new(HashMap::new()),
        });

        let pc_clone = pc.clone();
        runtime.spawn(async move {
            let _ = Self::network_loop(pc_clone, cancellation_token).await;
            println!("Network loop exited!")
        });
        pc
    }

    pub async fn read_epoch(&self) -> Result<u64, Error> {
        // TODO: gracefully handle malformed messages instead of unwrapping and crashing.
        // TODO: too much copying :(
        let req_id = Uuid::new_v4();
        let mut fbb = FlatBufferBuilder::new();
        let request_id = Some(Uuidu128::create(&mut fbb, &serialize_uuid(req_id)));
        let fbb_root = ReadEpochRequest::create(&mut fbb, &ReadEpochRequestArgs { request_id });
        fbb.finish(fbb_root, None);
        let (tx, rx) = oneshot::channel();
        {
            let mut outstanding_requests = self.outstanding_requests.lock().await;
            outstanding_requests.insert(req_id, tx);
        }
        let get_request_bytes = Bytes::copy_from_slice(fbb.finished_data());
        let mut envelope_fbb = FlatBufferBuilder::new();
        let request_bytes =
            self.create_msg_envelope(&mut envelope_fbb, MessageType::ReadEpoch, get_request_bytes);
        self.fast_network
            .send(
                self.range_server_info.address,
                Bytes::copy_from_slice(request_bytes),
            )
            .unwrap();
        let response = rx.await.unwrap();
        let msg = response.to_vec();
        let envelope = flatbuffers::root::<ResponseEnvelope>(msg.as_slice()).unwrap();
        match envelope.type_() {
            MessageType::ReadEpoch => {
                let response_msg =
                    flatbuffers::root::<ReadEpochResponse>(envelope.bytes().unwrap().bytes())
                        .unwrap();
                let () = Error::from_flatbuf_status(response_msg.status())?;
                return Ok(response_msg.epoch());
            }
            _ => return Err(Error::InvalidResponseFormat),
        }
    }

    async fn network_loop(client: Arc<Self>, cancellation_token: CancellationToken) {
        let mut network_receiver = client
            .fast_network
            .register(client.range_server_info.address);
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

    fn get_request_id_from_response(msg: Bytes) -> Uuid {
        let msg = msg.to_vec();
        let envelope = flatbuffers::root::<ResponseEnvelope>(msg.as_slice()).unwrap();
        let req_id = match envelope.type_() {
            MessageType::ReadEpoch => {
                let msg = flatbuffers::root::<ReadEpochResponse>(envelope.bytes().unwrap().bytes())
                    .unwrap();
                msg.request_id()
            }
            _ => panic!("unknown response message type"), // TODO: return and log unknown message type error.
        };
        deserialize_uuid(req_id.unwrap())
    }

    fn create_msg_envelope<'a>(
        &self,
        fbb: &'a mut FlatBufferBuilder<'a>,
        msg_type: MessageType,
        bytes: Bytes,
    ) -> &'a [u8] {
        let bytes = fbb.create_vector(bytes.to_vec().as_slice());
        let fbb_root = RequestEnvelope::create(
            fbb,
            &RequestEnvelopeArgs {
                type_: msg_type,
                bytes: Some(bytes),
            },
        );
        fbb.finish(fbb_root, None);
        fbb.finished_data()
    }
}
