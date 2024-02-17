use super::*;
use scylla::{Session, SessionBuilder};

pub struct Cassandra {
    session: Session,
}

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

impl Persistence for Cassandra {
    async fn take_ownership_and_load_range(&self, _range_id: Uuid) -> Result<RangeInfo, Error> {
        Err(Error::Unknown)
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
