use core::time;
#[derive(Clone, Copy, Debug)]
pub struct RangeServerConfig {
    pub range_maintenance_duration: time::Duration,
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub range_server: RangeServerConfig,
}
