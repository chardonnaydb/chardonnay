use super::*;
use bytes::Bytes;
use scylla::frame::response::cql_to_rust::FromCqlVal;
use scylla::macros::FromUserType;
use scylla::{FromRow, Session, SessionBuilder};
use uuid::Uuid;

pub struct Cassandra {
    session: Session,
}

#[derive(Debug, FromUserType)]
struct CqlEpochRange {
    lower_bound_inclusive: i64,
    upper_bound_inclusive: i64,
}

#[derive(Debug, FromRow)]
struct CqlRangeLease {
    range_id: Uuid,
    leader_sequence_number: i64,
    key_lower_bound_inclusive: Option<Vec<u8>>,
    key_upper_bound_exclusive: Option<Vec<u8>>,
    epoch_lease: CqlEpochRange,
    safe_snapshot_epochs: CqlEpochRange,
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

impl Cassandra {
    async fn create_test() -> Cassandra {
        let session = SessionBuilder::new()
            .known_node("127.0.0.1:9042".to_string())
            .build()
            .await
            .unwrap();
        Cassandra { session }
    }

    async fn get_range_lease(&self, range_id: Uuid) -> Result<CqlRangeLease, Error> {
        //TODO proper error handling instead of crashing.
        let rows = self
            .session
            .query(GET_RANGE_LEASE_QUERY, (range_id,))
            .await
            .unwrap()
            .rows;

        let res = match rows {
            None => Err(Error::RangeDoesNotExist),
            Some(mut rows) => {
                if rows.len() != 1 {
                    panic!("found multiple ranges with the same id!");
                } else {
                    let row = rows.pop().unwrap().into_typed::<CqlRangeLease>().unwrap();
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
        let done_or_err = self
            .session
            .query(
                ACQUIRE_RANGE_LEASE_QUERY,
                (prev_leader_sequence_number, new_leader_sequence_number),
            )
            .await;
        let res = match done_or_err {
            // TODO: break down error handling here.
            Err(_) => Err(Error::RangeOwnershipLost),
            Ok(_) => {
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
        };
        res
    }

    async fn renew_epoch_lease(
        &self,
        _range_id: Uuid,
        _new_lease: EpochLease,
        _leader_sequence_number: u64,
    ) -> Result<(), Error> {
        Err(Error::Unknown)
    }

    async fn put_versioned_record(
        &self,
        _range_id: Uuid,
        _key: Bytes,
        _val: Bytes,
        _version: KeyVersion,
    ) -> Result<(), Error> {
        Err(Error::Unknown)
    }

    async fn upsert(
        &self,
        _range_id: Uuid,
        _key: Bytes,
        _val: Bytes,
        _version: KeyVersion,
    ) -> Result<(), Error> {
        Err(Error::Unknown)
    }

    async fn delete(
        &self,
        _range_id: Uuid,
        _key: Bytes,
        _version: KeyVersion,
    ) -> Result<(), Error> {
        Err(Error::Unknown)
    }

    async fn get(&self, _range_id: Uuid, _key: Bytes) -> Result<Option<Bytes>, Error> {
        Err(Error::Unknown)
    }
}
