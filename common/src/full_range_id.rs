use uuid::Uuid;

use crate::keyspace_id::KeyspaceId;

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct FullRangeId {
    pub keyspace_id: KeyspaceId,
    pub range_id: Uuid,
}
