use crate::region::{Region, Zone};
use core::time;
use derivative::Derivative;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EpochConfig {
    pub proto_server_addr: SocketAddr,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct EpochPublisher {
    pub name: String,
    pub backend_addr: SocketAddr,
    pub fast_network_addr: SocketAddr,
}

#[derive(Derivative, Serialize, Deserialize)]
#[derivative(Clone, Debug, Eq, PartialEq, Hash)]
pub struct EpochPublisherSet {
    pub name: String,
    pub zone: Zone,
    #[derivative(Hash = "ignore")]
    pub publishers: HashSet<EpochPublisher>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RangeServerConfig {
    pub range_maintenance_duration: time::Duration,
    pub proto_server_port: u32,
    pub fast_network_port: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegionConfig {
    pub warden_address: SocketAddr,
    pub epoch_publishers: HashSet<EpochPublisherSet>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub range_server: RangeServerConfig,
    pub epoch: EpochConfig,
    pub regions: HashMap<Region, RegionConfig>,
}
