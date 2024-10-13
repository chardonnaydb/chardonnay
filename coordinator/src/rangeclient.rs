use std::sync::Arc;

use bytes::Bytes;
use common::{
    full_range_id::FullRangeId, membership::range_assignment_oracle::RangeAssignmentOracle,
    record::Record, transaction_info::TransactionInfo,
};
use rangeclient::client::{Error, GetResult, PrepareOk};

/// RangeClient abstracts away the individual rangeservers and allows users
/// to reach any range just by using the range id.
pub struct RangeClient {
    range_assignment_oracle: Arc<dyn RangeAssignmentOracle>,
}

impl RangeClient {
    pub async fn get(
        &self,
        _tx: Arc<TransactionInfo>,
        _range_id: &FullRangeId,
        _keys: Vec<Bytes>,
    ) -> Result<GetResult, Error> {
        panic!("unimplemented")
    }

    pub async fn prepare_transaction(
        &self,
        _tx: Arc<TransactionInfo>,
        _range_id: &FullRangeId,
        _has_reads: bool,
        _writes: &[Record],
        _deletes: &[Bytes],
    ) -> Result<PrepareOk, Error> {
        panic!("unimplemented")
    }
}
