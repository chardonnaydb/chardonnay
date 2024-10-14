use crate::{full_range_id::FullRangeId, host_info::HostInfo, keyspace_id::KeyspaceId};
use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait RangeAssignmentOracle: Send + Sync + 'static {
    async fn full_range_id_of_key(
        &self,
        keyspace_id: KeyspaceId,
        key: Bytes,
    ) -> Option<FullRangeId>;
    async fn host_of_range(&self, range_id: &FullRangeId) -> Option<HostInfo>;
    // TODO: provide APIs for key ranges / scans
}
