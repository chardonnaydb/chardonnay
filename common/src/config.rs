use crate::region::Region;
use core::time;
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Clone, Debug)]
pub struct RangeServerConfig {
    pub range_maintenance_duration: time::Duration,
    pub proto_server_addr: SocketAddr,
}

#[derive(Clone, Debug)]
pub struct RegionConfig {
    pub warden_address: String,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub range_server: RangeServerConfig,
    pub regions: HashMap<Region, RegionConfig>,
}
