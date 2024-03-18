use crate::{full_range_id::FullRangeId, host_info::HostInfo, keyspace_id::KeyspaceId};
use bytes::Bytes;
pub trait RangeAssignmentOracle: Send + Sync + 'static {
    fn full_range_id_of_key(&self, keyspace_id: KeyspaceId, key: Bytes) -> Option<FullRangeId>;
    fn host_of_range(&self, range_id: &FullRangeId) -> Option<HostInfo>;
    // TODO: provide APIs for key ranges / scans
}
