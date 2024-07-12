use bytes::Bytes;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct Record {
    pub key: Bytes,
    pub val: Bytes,
}
