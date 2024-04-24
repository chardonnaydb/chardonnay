use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct KeyspaceId {
    pub id: Uuid,
}

impl KeyspaceId {
    pub fn new(id: Uuid) -> KeyspaceId {
        KeyspaceId { id }
    }
}
