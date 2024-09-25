use crate::region::{Region, Zone};
use core::time;
use derivative::Derivative;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    net::SocketAddr,
    str::FromStr,
};

/// Represents a host and port combination.
#[derive(Clone, Debug)]
pub struct HostPort {
    /// The hostname or IP address.
    pub host: String,
    /// The port number.
    pub port: u16,
}

impl HostPort {
    pub fn to_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl fmt::Display for HostPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl FromStr for HostPort {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err("Invalid HostPort format".to_string());
        }
        let host = parts[0].to_string();
        let port = parts[1].parse::<u16>().map_err(|e| e.to_string())?;
        Ok(HostPort { host, port })
    }
}

impl Serialize for HostPort {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for HostPort {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(serde::de::Error::custom("Invalid HostPort format"));
        }
        let host = parts[0].to_string();
        let port = parts[1].parse().map_err(serde::de::Error::custom)?;
        Ok(HostPort { host, port })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EpochConfig {
    pub proto_server_addr: SocketAddr,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CassandraConfig {
    // Can't be SocketAddr because we want to use a DNS name.
    pub cql_addr: HostPort,
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
    pub cassandra: CassandraConfig,
    pub regions: HashMap<Region, RegionConfig>,
}
