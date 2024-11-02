pub mod cassandra;

use std::sync::Arc;

use common::{full_range_id::FullRangeId, key_range::KeyRange, keyspace_id::KeyspaceId};
use scylla::{
    query::Query, statement::SerialConsistency, FromRow, SerializeRow, Session, SessionBuilder,
};
use thiserror::Error;
use tracing::info;
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

#[derive(Debug, FromRow, SerializeRow)]
struct SerializedRangeAssignment {
    keyspace_id: Uuid,
    range_id: Uuid,
    key_lower_bound_inclusive: Option<Vec<u8>>,
    key_upper_bound_exclusive: Option<Vec<u8>>,
    assignee: String,
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

    async fn update_range_assignments(
        &self,
        version: i64,
        assignments: Vec<RangeAssignment>,
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

// TODO(purujit): To prevent writes from different Warden servers from clobbering each other,
// we will ultimately want the Warden to acquire a leader lease,
// we can then wrap this write into a Cassandra Lightweight Transaction that checks against
// the lease's sequence number.
static INSERT_OR_UPDATE_RANGE_ASSIGNMENT_QUERY: &str = r#"
  INSERT INTO chardonnay.range_map(keyspace_id, range_id, key_lower_bound_inclusive, key_upper_bound_exclusive, assignee)
  VALUES (?, ?, ?, ?, ?)
  "#;

#[async_trait::async_trait]
impl Persistence for PersistenceImpl {
    async fn get_keyspace_range_map(
        &self,
        keyspace_id: &KeyspaceId,
    ) -> Result<Vec<RangeAssignment>, Error> {
        todo!();
    }

    async fn update_range_assignments(
        &self,
        version: i64,
        assignments: Vec<RangeAssignment>,
    ) -> Result<(), Error> {
        let prepared = self
            .session
            .prepare(INSERT_OR_UPDATE_RANGE_ASSIGNMENT_QUERY)
            .await
            .map_err(|op| Error::InternalError(Arc::new(op)))?;
        info!("Writing assignments for version: {}", version);
        for assignment in assignments {
            let assignment = SerializedRangeAssignment {
                keyspace_id: assignment.range.keyspace_id.id,
                range_id: assignment.range.id,
                key_lower_bound_inclusive: assignment
                    .range
                    .key_range
                    .lower_bound_inclusive
                    .map(|b| b.to_vec()),
                key_upper_bound_exclusive: assignment
                    .range
                    .key_range
                    .upper_bound_exclusive
                    .map(|b| b.to_vec()),
                assignee: assignment.assignee,
            };
            self.session
                .execute(&prepared, assignment)
                .await
                .map_err(|op| Error::InternalError(Arc::new(op)))?;
        }
        info!("Finished writing assignments for version: {}", version);
        Ok(())
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
