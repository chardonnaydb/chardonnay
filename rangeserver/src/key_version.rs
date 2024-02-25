#[derive(Clone, Copy)]
pub struct KeyVersion {
    pub epoch: u64,
    pub version_counter: u64,
}
