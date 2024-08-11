use crate::region::{Region, Zone};
use core::time;
use derivative::Derivative;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

#[derive(Clone, Debug)]
pub struct EpochConfig {
    pub proto_server_addr: SocketAddr,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct EpochPublisher {
    pub name: String,
    pub backend_addr: SocketAddr,
    pub fast_network_addr: SocketAddr,
}

#[derive(Derivative)]
#[derivative(Clone, Debug, Eq, PartialEq, Hash)]
pub struct EpochPublisherSet {
    pub name: String,
    pub zone: Zone,
    #[derivative(Hash = "ignore")]
    pub publishers: HashSet<EpochPublisher>,
}

#[derive(Clone, Debug)]
pub struct RangeServerConfig {
    pub range_maintenance_duration: time::Duration,
    pub proto_server_addr: SocketAddr,
}

#[derive(Clone, Debug)]
pub struct RegionConfig {
    pub warden_address: SocketAddr,
    pub epoch_publishers: HashSet<EpochPublisherSet>,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub range_server: RangeServerConfig,
    pub epoch: EpochConfig,
    pub regions: HashMap<Region, RegionConfig>,
}
