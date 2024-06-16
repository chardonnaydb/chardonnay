use bytes::Bytes;
use flatbuf::rangeserver_flatbuffers::range_server::Key;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use uuid::Uuid;

pub enum KeyState {
    Fetched,   // Key has been fetched and is in the BTree
    Loading,   // Key has been requested and is being fetched
    Requested, // Key has been requested but fetching has not been started
}

pub struct PrefetchingBuffer {
    pub prefetch_store: BTreeMap<Bytes, Bytes>, // stores key / value
    pub key_state: HashMap<Bytes, KeyState>,    // stores key -> current fetch state
    pub transaction_keys: HashMap<Uuid, BTreeSet<Bytes>>, // stores transaction_id -> set of requested keys
}

impl PrefetchingBuffer {
    // TODO: fix result type
    fn process_prefetch_request(&mut self, transaction_id: Uuid, key: Bytes) -> Result<(), ()> {
        // Log that this transaction is requesting this key
        self.add_to_transaction_keys(transaction_id, key.clone());

        // Check if key has already been requested by another transaction
        // If not, add to key_state with fetch state requested
        self.key_state
            .entry(key.clone())
            .or_insert(KeyState::Requested);

        match self.key_state.get(&key) {
            Some(KeyState::Fetched) => (),   // return ok
            Some(KeyState::Loading) => (),   // wait for fetch to complete
            Some(KeyState::Requested) => (), // start fetch
            None => (),
        }

        Ok(())
    }

    /// Adds a transaction and its requested key to the transaction_key hashmap.
    /// If the transaction already appeats in the map, it adds the requested key
    /// to the set of requested keys.
    /// TODO: Add error checking and return
    fn add_to_transaction_keys(&mut self, transaction_id: Uuid, key: Bytes) {
        let transaction_set = self.transaction_keys.get_mut(&transaction_id);
        match transaction_set {
            Some(s) => {
                let _ = s.insert(key);
            } // Transaction has already requested keys, so add key to existing set
            // TODO: consider if transaction repeatedly requests the same key
            None => {
                // Transaction has not requested any keys, so create new set and add to transaction_keys
                let mut key_set = BTreeSet::new();
                key_set.insert(key);
                self.transaction_keys.insert(transaction_id, key_set);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_key() {
        let mut prefetching_buffer = PrefetchingBuffer {
            prefetch_store: BTreeMap::new(),
            key_state: HashMap::new(),
            transaction_keys: HashMap::new(),
        };

        let fake_id = Uuid::new_v4();
        let fake_key = Bytes::from("testing!");

        prefetching_buffer.add_to_transaction_keys(fake_id, fake_key);

        let other_key = prefetching_buffer.transaction_keys.get(&fake_id);
        match other_key {
            Some(s) => assert!(s.contains(&Bytes::from("testing!"))),
            None => panic!("Set did not contain a value it should have contained"),
        }
    }
}
