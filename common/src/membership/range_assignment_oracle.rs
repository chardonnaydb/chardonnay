use crate::{full_range_id::FullRangeId, host_info::HostInfo};
use bytes::Bytes;
use uuid::Uuid;
pub trait RangeAssignmentOracle: Send + Sync + 'static {
    fn full_range_id_of_key(&self, keyspace_id: Uuid, key: Bytes) -> Option<FullRangeId>;
    fn host_of_range(&self, range_id: &FullRangeId) -> Option<HostInfo>;
    // TODO: provide APIs for key ranges / scans
}
