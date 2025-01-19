use epoch::storage::cassandra::Cassandra;
use epoch::storage::Error;
use epoch::storage::Storage;
use scylla::query::Query;
use scylla::statement::SerialConsistency;
use scylla::SessionBuilder;
use test_case::test_case;
use uuid::Uuid;

const CASSANDRA_KNOWN_NODE: &str = "127.0.0.1:9042";

// We need to wrap the implementation in an enum like this since the Storage trait is not "object
// safe" and therefore does not support dynamic dispatch.
enum StorageTestCase {
    Cassandra {
        cass: Option<Cassandra>,
        region: String,
    },
}

impl StorageTestCase {
    fn new_cassandra() -> StorageTestCase {
        StorageTestCase::Cassandra {
            cass: None,
            region: String::new(),
        }
    }

    async fn setup(&mut self) {
        match self {
            StorageTestCase::Cassandra { cass, region } => {
                *region = Uuid::new_v4().to_string();
                *cass =
                    Some(Cassandra::new(CASSANDRA_KNOWN_NODE.to_string(), region.clone()).await);
            }
        }
    }

    async fn teardown(self) {
        match self {
            StorageTestCase::Cassandra { cass: _, region } => {
                let session = SessionBuilder::new()
                    .known_node(CASSANDRA_KNOWN_NODE)
                    .build()
                    .await
                    .unwrap();
                let mut query = Query::new("DELETE FROM chardonnay.epoch WHERE region = ?");
                query.set_serial_consistency(Some(SerialConsistency::Serial));
                let _ = session.query(query, (region.clone(),)).await;
            }
        }
    }
}

impl Storage for StorageTestCase {
    async fn initialize_epoch(&self) -> Result<(), Error> {
        match self {
            StorageTestCase::Cassandra { cass, .. } => {
                cass.as_ref().unwrap().initialize_epoch().await
            }
        }
    }

    async fn read_latest(&self) -> Result<u64, Error> {
        match self {
            StorageTestCase::Cassandra { cass, .. } => cass.as_ref().unwrap().read_latest().await,
        }
    }

    async fn conditional_update(&self, new_epoch: u64, current_epoch: u64) -> Result<(), Error> {
        match self {
            StorageTestCase::Cassandra { cass, .. } => {
                cass.as_ref()
                    .unwrap()
                    .conditional_update(new_epoch, current_epoch)
                    .await
            }
        }
    }
}

#[tokio::test]
#[test_case(StorageTestCase::new_cassandra() ; "Cassandra")]
async fn initialize_read_update(mut storage: StorageTestCase) {
    storage.setup().await;

    storage.initialize_epoch().await.unwrap();
    let epoch = storage.read_latest().await.unwrap();
    assert_eq!(epoch, 1);
    storage
        .conditional_update(/*new_epoch=*/ 3, /*current_epoch=*/ 1)
        .await
        .unwrap();
    let epoch = storage.read_latest().await.unwrap();
    assert_eq!(epoch, 3);

    storage.teardown().await;
}

#[tokio::test]
#[test_case(StorageTestCase::new_cassandra() ; "Cassandra")]
async fn initialize_condition_failed(mut storage: StorageTestCase) {
    storage.setup().await;

    storage.initialize_epoch().await.unwrap();
    let epoch = storage.read_latest().await.unwrap();
    assert_eq!(epoch, 1);
    let err = storage
        .conditional_update(/*new_epoch=*/ 3, /*current_epoch=*/ 2)
        .await
        .expect_err("conditional update should fail if current epoch does not match");
    match err {
        Error::ConditionFailed => (),
        _ => panic!("unexpected error: {:?}", err),
    }

    storage.teardown().await;
}

#[tokio::test]
#[test_case(StorageTestCase::new_cassandra() ; "Cassandra")]
async fn epoch_increases_monotonically(mut storage: StorageTestCase) {
    storage.setup().await;

    storage.initialize_epoch().await.unwrap();
    let epoch = storage.read_latest().await.unwrap();
    assert_eq!(epoch, 1);
    let err = storage
        .conditional_update(/*new_epoch=*/ 0, /*current_epoch=*/ 1)
        .await
        .expect_err("conditional update should not decrease the epoch");
    match err {
        Error::ConditionFailed => (),
        _ => panic!("unexpected error: {:?}", err),
    }

    storage.teardown().await;
}
