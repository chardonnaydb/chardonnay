use std::net::SocketAddr;

#[derive(Clone, Debug)]
pub struct HostInfo {
    pub identity: String,
    pub address: SocketAddr,
    pub zone: crate::region::Zone,
    pub warden_connection_epoch: u64,
}
