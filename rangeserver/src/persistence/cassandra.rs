use super::*;
use bytes::Bytes;
use scylla::frame::response::cql_to_rust::FromCqlVal;
use scylla::macros::FromUserType;
use scylla::macros::IntoUserType;
use scylla::transport::errors::DbError;
use scylla::transport::errors::QueryError;
use scylla::{FromRow, Session, ValueList};
use uuid::Uuid;

pub struct Cassandra {
    session: Session,
}

#[derive(Debug, FromUserType, IntoUserType)]
struct CqlEpochRange {
    lower_bound_inclusive: i64,
    upper_bound_inclusive: i64,
}

#[derive(Debug, FromRow, ValueList)]
struct CqlRangeLease {
    range_id: Uuid,
    epoch_lease: CqlEpochRange,
    key_lower_bound_inclusive: Option<Vec<u8>>,
    key_upper_bound_exclusive: Option<Vec<u8>>,
    leader_sequence_number: i64,
    safe_snapshot_epochs: CqlEpochRange,
}

#[derive(Debug, FromRow)]
struct CqlVal {
    value: Option<Vec<u8>>,
}

impl CqlRangeLease {
    fn key_range(&self) -> KeyRange {
        let lower_bound_inclusive = match &self.key_lower_bound_inclusive {
            None => None,
            Some(bytes) => Some(Bytes::copy_from_slice(&bytes)),
        };

        let upper_bound_exclusive = match &self.key_upper_bound_exclusive {
            None => None,
            Some(bytes) => Some(Bytes::copy_from_slice(&bytes)),
        };
        KeyRange {
            lower_bound_inclusive,
            upper_bound_exclusive,
        }
    }
}

static GET_RANGE_LEASE_QUERY: &str = r#"
  SELECT * FROM chardonnay.range_leases
    WHERE range_id = ?;
"#;

static ACQUIRE_RANGE_LEASE_QUERY: &str = r#"
  UPDATE chardonnay.range_leases SET leader_sequence_number = ?
    WHERE range_id = ? 
    IF leader_sequence_number = ? 
"#;

static RENEW_EPOCH_LEASE_QUERY: &str = r#"
  UPDATE chardonnay.range_leases SET epoch_lease = ?
    WHERE range_id = ? 
    IF leader_sequence_number = ? 
"#;

static UPSERT_QUERY: &str = r#"
  INSERT INTO chardonnay.records (range_id, key, value, epoch) 
    VALUES (?, ?, ?, ?) 
    USING TIMESTAMP ?
"#;

static DELETE_QUERY: &str = r#"
  DELETE FROM chardonnay.records
    USING TIMESTAMP ?
    WHERE range_id = ?
    AND key = ?
"#;

static GET_QUERY: &str = r#"
  SELECT value from chardonnay.records
  WHERE range_id = ? AND key = ?
"#;

fn scylla_query_error_to_persistence_error(qe: QueryError) -> Error {
    match qe {
        QueryError::TimeoutError | QueryError::DbError(DbError::WriteTimeout { .. }, _) => {
            Error::Timeout
        }
        _ => {
            // TODO: It is essential to correctly categorize timeout errors, since these could indicate an operation
            // might still succeed and require extra care in dealing with. Having a catch-all is bad since we might
            // break if a new timeout variant is added.
            Error::InternalError(Arc::new(qe))
        }
    }
}

impl Cassandra {
    async fn get_range_lease(&self, range_id: Uuid) -> Result<CqlRangeLease, Error> {
        let rows = self
            .session
            .query(GET_RANGE_LEASE_QUERY, (range_id,))
            .await
            .map_err(scylla_query_error_to_persistence_error)?
            .rows;

        let res = match rows {
            None => Err(Error::RangeDoesNotExist),
            Some(mut rows) => {
                if rows.len() != 1 {
                    panic!("found multiple ranges with the same id!");
                } else {
                    let row = rows.pop().unwrap();
                    let row = row.into_typed::<CqlRangeLease>().unwrap();
                    Ok(row)
                }
            }
        };
        res
    }
}

impl Persistence for Cassandra {
    async fn take_ownership_and_load_range(&self, range_id: Uuid) -> Result<RangeInfo, Error> {
        let cql_lease = self.get_range_lease(range_id).await?;

        let prev_leader_sequence_number = cql_lease.leader_sequence_number;
        let new_leader_sequence_number = prev_leader_sequence_number + 1;
        let _ = self
            .session
            .query(
                ACQUIRE_RANGE_LEASE_QUERY,
                (
                    new_leader_sequence_number,
                    cql_lease.range_id,
                    prev_leader_sequence_number,
                ),
            )
            .await
            .map_err(scylla_query_error_to_persistence_error)?;

        // We must read the lease again after we've taken ownership to ensure we get its most up-to-date info.
        // Otherwise, the previous owner could have updated the lease between looking it up and owning it.
        let cql_lease = self.get_range_lease(range_id).await?;
        if cql_lease.leader_sequence_number != new_leader_sequence_number {
            Err(Error::RangeOwnershipLost)
        } else {
            Ok(RangeInfo {
                id: range_id,
                leader_sequence_number: new_leader_sequence_number as u64,
                epoch_lease: (
                    cql_lease.epoch_lease.lower_bound_inclusive as u64,
                    cql_lease.epoch_lease.upper_bound_inclusive as u64,
                ),
                key_range: cql_lease.key_range(),
            })
        }
    }

    async fn renew_epoch_lease(
        &self,
        range_id: Uuid,
        (llb, lup): EpochLease,
        leader_sequence_number: u64,
    ) -> Result<(), Error> {
        let cql_epoch_range = CqlEpochRange {
            lower_bound_inclusive: llb as i64,
            upper_bound_inclusive: lup as i64,
        };
        let _ = self
            .session
            .query(
                RENEW_EPOCH_LEASE_QUERY,
                (cql_epoch_range, range_id, leader_sequence_number as i64),
            )
            .await
            .map_err(scylla_query_error_to_persistence_error)?;
        // Scylla and cassandra have different ways of communicating whether the conditional statement actually
        // took effect or not. To support both, we just do a serial read and see what actually happened.
        // We should revisit this approach at some point though.
        let cql_lease = self.get_range_lease(range_id).await?;
        if cql_lease.leader_sequence_number != (leader_sequence_number as i64) {
            Err(Error::RangeOwnershipLost)
        } else {
            Ok(())
        }
    }

    async fn put_versioned_record(
        &self,
        _range_id: Uuid,
        _key: Bytes,
        _val: Bytes,
        _version: KeyVersion,
    ) -> Result<(), Error> {
        todo!();
    }

    async fn upsert(
        &self,
        range_id: Uuid,
        key: Bytes,
        val: Bytes,
        version: KeyVersion,
    ) -> Result<(), Error> {
        let _ = self
            .session
            .query(
                UPSERT_QUERY,
                (
                    range_id,
                    key.to_vec(),
                    val.to_vec(),
                    version.epoch as i64,
                    version.epoch as i64,
                ),
            )
            .await
            .map_err(scylla_query_error_to_persistence_error)?;
        Ok(())
    }

    async fn delete(&self, range_id: Uuid, key: Bytes, version: KeyVersion) -> Result<(), Error> {
        let _ = self
            .session
            .query(DELETE_QUERY, (version.epoch as i64, range_id, key.to_vec()))
            .await
            .map_err(scylla_query_error_to_persistence_error)?;
        Ok(())
    }

    async fn get(&self, range_id: Uuid, key: Bytes) -> Result<Option<Bytes>, Error> {
        let rows = self
            .session
            .query(GET_QUERY, (range_id, key.to_vec()))
            .await
            .map_err(scylla_query_error_to_persistence_error)?
            .rows;

        match rows {
            None => Ok(None),
            Some(mut rows) => {
                if rows.len() == 0 {
                    Ok(None)
                } else if rows.len() != 1 {
                    panic!("found multiple rows with the same id!");
                } else {
                    let row = rows.pop().unwrap();
                    let row = row.into_typed::<CqlVal>().unwrap();
                    Ok(row.value.map(|v| Bytes::copy_from_slice(&v)))
                }
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use scylla::SessionBuilder;

    pub const TEST_RANGE_UUID: &str = "40fc4bf4-a79c-4740-ad29-a61bace5c251";
    impl Cassandra {
        async fn create_test() -> Cassandra {
            let session = SessionBuilder::new()
                .known_node("127.0.0.1:9042".to_string())
                .build()
                .await
                .unwrap();
            Cassandra { session }
        }
    }

    pub async fn init() -> Cassandra {
        let cassandra = Cassandra::create_test().await;
        let cql_range = CqlRangeLease {
            range_id: Uuid::parse_str(TEST_RANGE_UUID).unwrap(),
            leader_sequence_number: 0,
            key_lower_bound_inclusive: None,
            key_upper_bound_exclusive: None,
            epoch_lease: CqlEpochRange {
                lower_bound_inclusive: 0,
                upper_bound_inclusive: 0,
            },
            safe_snapshot_epochs: CqlEpochRange {
                lower_bound_inclusive: 1,
                upper_bound_inclusive: 0,
            },
        };
        cassandra
            .session
            .query("INSERT INTO chardonnay.range_leases (range_id, leader_sequence_number, epoch_lease, safe_snapshot_epochs) VALUES (?, ?, ?, ?) IF NOT EXISTS", (cql_range.range_id, cql_range.leader_sequence_number, cql_range.epoch_lease, cql_range.safe_snapshot_epochs))
            .await
            .unwrap();
        cassandra
    }

    #[tokio::test]
    async fn basic_take_ownership_and_renew_lease() {
        let cassandra = init().await;
        let range_info = cassandra
            .take_ownership_and_load_range(Uuid::parse_str(TEST_RANGE_UUID).unwrap())
            .await
            .unwrap();
        cassandra
            .renew_epoch_lease(range_info.id, (0, 1), range_info.leader_sequence_number)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn basic_crud() {
        let cassandra = init().await;
        let range_id = Uuid::parse_str(TEST_RANGE_UUID).unwrap();
        let key = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());

        assert!(cassandra
            .get(range_id, key.clone())
            .await
            .unwrap()
            .is_none());

        let epoch2_val = Bytes::from_static(b"C");
        cassandra
            .upsert(
                range_id,
                key.clone(),
                epoch2_val.clone(),
                KeyVersion {
                    epoch: 2,
                    version_counter: 0,
                },
            )
            .await
            .unwrap();

        let read_val = cassandra.get(range_id, key.clone()).await.unwrap().unwrap();
        assert!(read_val == epoch2_val);

        // a write with a lower epoch should be a no-op.
        let epoch1_val = Bytes::from_static(b"A");
        cassandra
            .upsert(
                range_id,
                key.clone(),
                epoch1_val.clone(),
                KeyVersion {
                    epoch: 1,
                    version_counter: 0,
                },
            )
            .await
            .unwrap();

        let read_val = cassandra.get(range_id, key.clone()).await.unwrap().unwrap();
        assert!(read_val == epoch2_val);

        // but a higher epoch should override existing value.
        let epoch3_val = Bytes::from_static(b"D");
        cassandra
            .upsert(
                range_id,
                key.clone(),
                epoch3_val.clone(),
                KeyVersion {
                    epoch: 3,
                    version_counter: 0,
                },
            )
            .await
            .unwrap();

        let read_val = cassandra.get(range_id, key.clone()).await.unwrap().unwrap();
        assert!(read_val == epoch3_val);

        // a delete with a lower epoch should be a no-op.
        cassandra
            .delete(
                range_id,
                key.clone(),
                KeyVersion {
                    epoch: 2,
                    version_counter: 0,
                },
            )
            .await
            .unwrap();

        let read_val = cassandra.get(range_id, key.clone()).await.unwrap().unwrap();
        assert!(read_val == epoch3_val);

        // but a higher epoch should work.
        cassandra
            .delete(
                range_id,
                key.clone(),
                KeyVersion {
                    epoch: 4,
                    version_counter: 0,
                },
            )
            .await
            .unwrap();

        assert!(cassandra
            .get(range_id, key.clone())
            .await
            .unwrap()
            .is_none());
    }
}
