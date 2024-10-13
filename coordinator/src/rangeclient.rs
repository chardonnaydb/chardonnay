use std::sync::Arc;

use bytes::Bytes;
use common::{
    full_range_id::FullRangeId, membership::range_assignment_oracle::RangeAssignmentOracle,
    transaction_info::TransactionInfo,
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
        tx: Arc<TransactionInfo>,
        range_id: &FullRangeId,
        keys: Vec<Bytes>,
    ) -> Result<GetResult, Error> {
        panic!("unimplemented")
    }
}
