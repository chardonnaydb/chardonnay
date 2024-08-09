use common::{host_info::HostInfo, keyspace_id::KeyspaceId};
use proto::warden::WardenUpdate;
use tokio::sync::broadcast::Receiver;
use tonic::Status;

use crate::persistence::{RangeAssignment, RangeInfo};

pub trait AssignmentComputation {
    fn register_range_server(&self, host_info: HostInfo) -> Result<Receiver<i64>, Status>;
    fn get_assignment(&self, version: i64, full_update: bool) -> Option<WardenUpdate>;
}
pub struct SimpleAssignmentComputation {
    keyspace_id: KeyspaceId,
    base_ranges: Option<Vec<RangeInfo>>,
    range_assignments: Option<Vec<RangeAssignment>>,
}

impl SimpleAssignmentComputation {
    pub fn new(keyspace_id: KeyspaceId, base_ranges: Option<Vec<RangeInfo>>) -> Self {
        Self {
            keyspace_id,
            // TODO(purujit): Initialize base_ranges and range_assignments from database.
            base_ranges,
            range_assignments: None,
        }
    }
}

impl AssignmentComputation for SimpleAssignmentComputation {
    fn register_range_server(&self, _host_info: HostInfo) -> Result<Receiver<i64>, Status> {
        todo!()
    }

    fn get_assignment(&self, _version: i64, _full_update: bool) -> Option<WardenUpdate> {
        todo!()
    }
}
