use std::net::SocketAddr;

#[derive(Clone, Debug)]
pub struct HostInfo {
    pub identity: String,
    pub address: SocketAddr,
}
