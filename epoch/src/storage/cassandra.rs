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

        let res = match rows {
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
        };
        res
    }

    async fn conditional_update(&self, new_epoch: u64, current_epoch: u64) -> Result<(), Error> {
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

#[cfg(test)]
pub mod tests {
    use uuid::Uuid;

    use super::*;

    impl Cassandra {
        async fn create_test(test_region: String) -> Cassandra {
            Cassandra::new("127.0.0.1:9042".to_string(), test_region).await
        }

        async fn cleanup(&self) {
            let mut query = Query::new("DELETE FROM chardonnay.epoch WHERE region = ?");
            query.set_serial_consistency(Some(SerialConsistency::Serial));
            let _ = self.session.query(query, (self.region.clone(),)).await;
        }
    }

    #[tokio::test]
    async fn initialize_read_update() {
        let region_name = Uuid::new_v4().to_string();
        let cassandra = Cassandra::create_test(region_name).await;
        cassandra.initialize_epoch().await.unwrap();
        let epoch = cassandra.read_latest().await.unwrap();
        assert!(epoch == 1);
        cassandra.conditional_update(3, 1).await.unwrap();
        let epoch = cassandra.read_latest().await.unwrap();
        assert!(epoch == 3);
        cassandra.cleanup().await;
    }

    #[tokio::test]
    async fn initialize_condition_failed() {
        let region_name = Uuid::new_v4().to_string();
        let cassandra = Cassandra::create_test(region_name).await;
        cassandra.initialize_epoch().await.unwrap();
        let epoch = cassandra.read_latest().await.unwrap();
        assert!(epoch == 1);
        let err = cassandra
            .conditional_update(3, 2)
            .await
            .expect_err("expecting condition failed");
        match err {
            Error::ConditionFailed => (),
            _ => assert!(false),
        }
        cassandra.cleanup().await;
    }
}
