use super::*;
use chrono::DateTime;
use scylla::query::Query;
use scylla::statement::SerialConsistency;
use scylla::transport::errors::DbError;
use scylla::transport::errors::QueryError;
use scylla::Session;
use scylla::SessionBuilder;

type UtcDateTime = DateTime<chrono::Utc>;

pub struct Cassandra {
    session: Session,
    region: String,
}

static INITIALIZE_EPOCH_QUERY: &str = r#"
  INSERT INTO chardonnay.epoch (region, epoch, timestamp) 
    VALUES (?, 1, ?) 
    IF NOT EXISTS
"#;

static GET_LATEST_EPOCH_QUERY: &str = r#"
  SELECT epoch, timestamp FROM chardonnay.epoch
    WHERE region = ?;
"#;

static UPDATE_EPOCH_QUERY: &str = r#"
  UPDATE chardonnay.epoch SET epoch = ?, timestamp = ?
    WHERE region = ? 
    IF epoch = ? 
"#;

fn scylla_query_error_to_storage_error(qe: QueryError) -> Error {
    match qe {
        QueryError::TimeoutError | QueryError::DbError(DbError::WriteTimeout { .. }, _) => {
            Error::Timeout
        }
        _ => Error::InternalError(Arc::new(qe)),
    }
}

impl Cassandra {
    pub async fn new(known_node: String, region: String) -> Cassandra {
        let session = SessionBuilder::new()
            .known_node(known_node)
            .build()
            .await
            .unwrap();
        Cassandra { session, region }
    }
}

impl Storage for Cassandra {
    async fn initialize_epoch(&self) -> Result<(), Error> {
        let utcnow = chrono::Utc::now();
        let mut query = Query::new(INITIALIZE_EPOCH_QUERY);
        query.set_serial_consistency(Some(SerialConsistency::Serial));
        let _ = self
            .session
            .query(query, (self.region.clone(), utcnow))
            .await
            .map_err(scylla_query_error_to_storage_error)?;
        Ok(())
    }

    async fn read_latest(&self) -> Result<u64, Error> {
        let mut query = Query::new(GET_LATEST_EPOCH_QUERY);
        query.set_serial_consistency(Some(SerialConsistency::Serial));
        let rows = self
            .session
            .query(query, (self.region.clone(),))
            .await
            .map_err(scylla_query_error_to_storage_error)?
            .rows;

        match rows {
            None => Err(Error::EpochNotInitialized),
            Some(mut rows) => {
                if rows.len() != 1 {
                    panic!("found multiple epoch rows for the same region!");
                } else {
                    let row = rows.pop().unwrap();
                    let (epoch, _) = row.into_typed::<(i64, UtcDateTime)>().unwrap();
                    Ok(epoch as u64)
                }
            }
        }
    }

    async fn conditional_update(&self, new_epoch: u64, current_epoch: u64) -> Result<(), Error> {
        if new_epoch < current_epoch {
            return Err(Error::ConditionFailed);
        }
        let utcnow = chrono::Utc::now();
        let mut query = Query::new(UPDATE_EPOCH_QUERY);
        query.set_serial_consistency(Some(SerialConsistency::Serial));
        let query_result = self
            .session
            .query(
                query,
                (
                    new_epoch as i64,
                    utcnow,
                    self.region.clone(),
                    current_epoch as i64,
                ),
            )
            .await
            .map_err(scylla_query_error_to_storage_error)?
            .rows;
        let res = match query_result {
            None => panic!("no results results from a LWT"),
            Some(mut rows) => {
                if rows.len() != 1 {
                    panic!("found multiple results from a LWT");
                } else {
                    let row = rows.pop().unwrap();
                    let applied = row.columns[0].as_ref().unwrap().as_boolean().unwrap();
                    if applied {
                        Ok(())
                    } else {
                        Err(Error::ConditionFailed)
                    }
                }
            }
        };
        res
    }
}
