pub mod cassandra;

use std::sync::Arc;

use common::{full_range_id::FullRangeId, key_range::KeyRange, keyspace_id::KeyspaceId};
use scylla::{query::Query, statement::SerialConsistency, Session, SessionBuilder};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct RangeInfo {
    pub keyspace_id: KeyspaceId,
    pub id: Uuid,
    pub key_range: KeyRange,
}

#[derive(Debug)]
pub struct RangeAssignment {
    pub range: RangeInfo,
    pub assignee: String,
}

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Persistence Layer error: {0}")]
    InternalError(Arc<dyn std::error::Error + Send + Sync>),
}

#[async_trait::async_trait]
pub trait Persistence: Send + Sync + 'static {
    async fn get_keyspace_range_map(
        &self,
        keyspace_id: &KeyspaceId,
    ) -> Result<Vec<RangeAssignment>, Error>;

    async fn update_range_assignment(
        &self,
        range_id: &FullRangeId,
        assignee: String,
    ) -> Result<(), Error>;

    async fn insert_new_ranges(&self, ranges: &Vec<RangeInfo>) -> Result<(), Error>;
}

pub struct PersistenceImpl {
    session: Arc<Session>,
}

impl PersistenceImpl {
    pub async fn new(known_node: String) -> Self {
        let session = Arc::new(
            SessionBuilder::new()
                .known_node(known_node)
                .build()
                .await
                .unwrap(),
        );
        Self { session }
    }
}

static INSERT_INTO_RANGE_LEASE_QUERY: &str = r#"
  INSERT INTO chardonnay.range_leases(range_id, key_lower_bound_inclusive, key_upper_bound_exclusive)
    VALUES (?, ?, ?)
    IF NOT EXISTS
"#;

#[async_trait::async_trait]
impl Persistence for PersistenceImpl {
    async fn get_keyspace_range_map(
        &self,
        keyspace_id: &KeyspaceId,
    ) -> Result<Vec<RangeAssignment>, Error> {
        todo!();
    }

    async fn update_range_assignment(
        &self,
        range_id: &FullRangeId,
        assignee: String,
    ) -> Result<(), Error> {
        todo!();
    }

    async fn insert_new_ranges(&self, ranges: &Vec<RangeInfo>) -> Result<(), Error> {
        for range in ranges {
            let mut query = Query::new(INSERT_INTO_RANGE_LEASE_QUERY);
            query.set_serial_consistency(Some(SerialConsistency::Serial));
            if let Err(err) = self
                .session
                .query(
                    query,
                    (
                        range.id,
                        range
                            .key_range
                            .lower_bound_inclusive
                            .clone()
                            .map_or(vec![], |v| v.to_vec()),
                        range
                            .key_range
                            .upper_bound_exclusive
                            .clone()
                            .map_or(vec![], |v| v.to_vec()),
                    ),
                )
                .await
            {
                return Err(Error::InternalError(Arc::new(err)));
            }
        }
        Ok(())
    }
}
