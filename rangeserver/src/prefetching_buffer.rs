use bytes::Bytes;
use std::collections::BTreeMap;

pub struct PrefetchingBuffer {
    pub prefetch_store: BTreeMap<Bytes, Bytes>,
}

impl PrefetchingBuffer {
    // Insert a key into the BTree (TBU with Result<(), err> output)
    fn insert(&mut self, key: Bytes, val: Bytes) {
        let _ = &mut self.prefetch_store.insert(key, val);
    }

    // get a key from the BTree
    fn get(&self, key: Bytes) -> Option<&Bytes> {
        self.prefetch_store.get(&key)
    }
}
