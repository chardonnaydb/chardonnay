use crate::network::fast_network::FastNetwork as Trait;
use std::{
    collections::HashMap,
    net::{SocketAddr, UdpSocket},
    sync::RwLock,
};

use bytes::Bytes;
use tokio::sync::mpsc;

enum DefaultHandler {
    NotRegistered,
    Registered(mpsc::UnboundedSender<(SocketAddr, Bytes)>),
}
pub struct UdpFastNetwork {
    socket: UdpSocket,
    listeners: RwLock<HashMap<SocketAddr, mpsc::UnboundedSender<Bytes>>>,
    default_handler: RwLock<DefaultHandler>,
}

impl UdpFastNetwork {
    pub fn new(socket: UdpSocket) -> UdpFastNetwork {
        socket.set_nonblocking(true).unwrap();
        UdpFastNetwork {
            socket,
            listeners: RwLock::new(HashMap::new()),
            default_handler: RwLock::new(DefaultHandler::NotRegistered),
        }
    }
}

impl Trait for UdpFastNetwork {
    fn send(&self, to: SocketAddr, payload: Bytes) -> Result<(), std::io::Error> {
        self.socket.send_to(payload.to_vec().as_slice(), to)?;
        Ok(())
    }

    fn listen_default(&self) -> mpsc::UnboundedReceiver<(SocketAddr, Bytes)> {
        let (s, r) = mpsc::unbounded_channel();
        let mut default_handler = self.default_handler.write().unwrap();
        *default_handler = DefaultHandler::Registered(s);
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
                let default_listener = self.default_handler.read().unwrap();
                match listeners.get(&sender_addr) {
                    None => {
                        match &*default_listener {
                            DefaultHandler::NotRegistered => (), // perhaps log something here?
                            DefaultHandler::Registered(s) => s.send((sender_addr, bytes)).unwrap(),
                        }
                    }
                    Some(s) => s.send(bytes).unwrap(),
                };
                return true;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => return false,
            Err(e) => panic!("UdpFastNetwork encountered IO error: {e}"),
        }
    }
}
