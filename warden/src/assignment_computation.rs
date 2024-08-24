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
    base_ranges: Option<Vec<RangeInfo>>,
    range_assignments: Option<Vec<RangeAssignment>>,
}

impl SimpleAssignmentComputation {
    pub fn new() -> Self {
        Self {
            // TODO(purujit): Initialize base_ranges and range_assignments from database.
            base_ranges: None,
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
