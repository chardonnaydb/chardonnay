use std::net::SocketAddr;

use bytes::Bytes;
use tokio::sync::mpsc;
// Busy-polled network endpoint.
pub trait FastNetwork: Send + Sync + 'static {
    fn send(&self, to: SocketAddr, payload: Bytes) -> Result<(), std::io::Error>;
    // Listen for messages sent from a specific SocketAddr.
    fn register(&self, from: SocketAddr) -> mpsc::UnboundedReceiver<Bytes>;
    // Reads one message from the network (if any) and delivers it to all the
    // relevant listeners. Returns true if something was read from the network,
    // and false otherwise.
    fn poll(&self) -> bool;
}
