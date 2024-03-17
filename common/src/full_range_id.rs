use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
pub struct FullRangeId {
    pub keyspace_id: Uuid,
    pub range_id: Uuid,
}
