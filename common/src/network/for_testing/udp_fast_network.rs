use crate::network::fast_network::FastNetwork as Trait;
use std::{
    collections::HashMap,
    net::{SocketAddr, UdpSocket},
    sync::RwLock,
};

use bytes::Bytes;
use tokio::sync::mpsc;

pub struct UdpFastNetwork {
    socket: UdpSocket,
    listeners: RwLock<HashMap<SocketAddr, mpsc::UnboundedSender<Bytes>>>,
}

impl UdpFastNetwork {
    pub fn new(socket: UdpSocket) -> UdpFastNetwork {
        socket.set_nonblocking(true).unwrap();
        UdpFastNetwork {
            socket,
            listeners: RwLock::new(HashMap::new()),
        }
    }
}

impl Trait for UdpFastNetwork {
    fn send(&self, to: SocketAddr, payload: Bytes) -> Result<(), std::io::Error> {
        self.socket.send_to(payload.to_vec().as_slice(), to)?;
        Ok(())
    }

    fn listen_default(&self) -> mpsc::UnboundedReceiver<(SocketAddr, Bytes)> {
        // TODO(tamer): properly implement.
        let (_, r) = mpsc::unbounded_channel();
        r
    }

    fn register(&self, from: SocketAddr) -> mpsc::UnboundedReceiver<Bytes> {
        let (s, r) = mpsc::unbounded_channel();
        let mut listeners = self.listeners.write().unwrap();
        listeners.insert(from, s);
        r
    }

    fn poll(&self) -> bool {
        let mut buf = [0; 1024];
        match self.socket.recv_from(&mut buf) {
            Ok((len, sender_addr)) => {
                let bytes = Bytes::copy_from_slice(&buf[0..len]);
                let listeners = self.listeners.read().unwrap();
                match listeners.get(&sender_addr) {
                    None => (), // TODO: provide default listener
                    Some(s) => s.send(bytes).unwrap(),
                };
                return true;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => return false,
            Err(e) => panic!("UdpFastNetwork encountered IO error: {e}"),
        }
    }
}
