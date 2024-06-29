#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct EpochLease {
    pub lower_bound_inclusive: u64,
    pub upper_bound_inclusive: u64,
}
