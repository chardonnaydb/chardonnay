use bytes::Bytes;
use flatbuf::rangeserver_flatbuffers::range_server::Key;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use tokio::sync::{watch, Mutex};
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum KeyState {
    Fetched,   // Key has been fetched and is in the BTree
    Loading,   // Key has been requested and is being fetched
    Requested, // Key has been requested but fetching has not been started
}

#[derive(Default, Debug)]
pub struct PrefetchingBuffer {
    pub prefetch_store: BTreeMap<Bytes, Bytes>, // stores key / value
    pub key_state: HashMap<Bytes, KeyState>,    // stores key -> current fetch state
    pub transaction_keys: HashMap<Uuid, BTreeSet<Bytes>>, // stores transaction_id -> set of requested keys
    pub key_transactions: HashMap<Bytes, BTreeSet<Uuid>>, // stores key -> set of transaction_ids
    pub key_state_watcher: HashMap<Bytes, watch::Receiver<KeyState>>, // key -> receiver for state changes
    pub key_state_sender: HashMap<Bytes, watch::Sender<KeyState>>, // key -> sender for state changes
}

impl PrefetchingBuffer {
    // TODO: fix result type
    // TODO: fix clones
    // TODO: Error checking
    pub async fn process_prefetch_request(
        &mut self,
        transaction_id: Uuid,
        key: Bytes,
    ) -> Result<(), ()> {
        // Log that this transaction is requesting this key
        self.add_to_transaction_keys(transaction_id, key.clone());
        // Log that this key is being requested by this transaction
        self.add_to_key_transactions(transaction_id, key.clone());

        // Check if key has already been requested by another transaction
        // If not, add to key_state with fetch state requested
        self.key_state
            .entry(key.clone())
            .or_insert(KeyState::Requested);

        match self.key_state.get(&key) {
            Some(KeyState::Fetched) => {
                println!("Returning");
                self.print_buffer();
                return Ok(());
            } // return ok
            Some(KeyState::Loading) => {
                // wait for fetch to complete
                println!("Fetch is loading");
                if let Some(receiver) = self.key_state_watcher.get(&key) {
                    let mut receiver = receiver.clone();
                    while receiver.changed().await.is_ok() {
                        if *receiver.borrow() == KeyState::Fetched {
                            println!("Fetch is done");
                            self.print_buffer();
                            return Ok(()); // return ok once complete
                        }
                    }
                }
            }
            Some(KeyState::Requested) => {
                println!("Requesting fetch");
                self.fetch(key).await; // start fetch
                println!("Fetch complete");
                self.print_buffer();
                return Ok(()); // return ok once complete
            }
            None => (),
        }

        Err(()) // If we got to here, an error occured
    }

    /// Adds a transaction and its requested key to the transaction_key hashmap.
    /// If the transaction already appears in the map, it adds the requested key
    /// to the set of requested keys.
    /// TODO: Add error checking and return
    fn add_to_transaction_keys(&mut self, transaction_id: Uuid, key: Bytes) {
        let transaction_set = self.transaction_keys.get_mut(&transaction_id);
        match transaction_set {
            Some(s) => {
                // Transaction has already requested keys, so add key to existing set
                let _ = s.insert(key);
            }
            // TODO: consider if transaction repeatedly requests the same key
            None => {
                // Transaction has not requested any keys, so create new set and add to transaction_keys
                let mut key_set = BTreeSet::new();
                key_set.insert(key);
                self.transaction_keys.insert(transaction_id, key_set);
            }
        }
    }

    /// Adds a key and its requesting transactions to the key_transaction hashmap.
    /// If the key already appears in the map, it adds the requesting transaction
    /// to the set of requesting transactions.
    /// TODO: Add error checking and return
    fn add_to_key_transactions(&mut self, transaction_id: Uuid, key: Bytes) {
        let key_set = self.key_transactions.get_mut(&key);
        match key_set {
            Some(s) => {
                // Key has already been previously requested, so add transaction_id to existing set
                let _ = s.insert(transaction_id);
            }
            // TODO: consider if transaction repeatedly requests the same key
            None => {
                // Key has not been requested by any transaction, so create new set and add to key_transactions
                let mut transaction_set = BTreeSet::new();
                transaction_set.insert(transaction_id);
                self.key_transactions.insert(key, transaction_set);
            }
        }
    }

    async fn fetch(&mut self, key: Bytes) {
        // Update key_state to reflect beginning of fetch
        self.key_state.insert(key.clone(), KeyState::Loading);
        // Create a watch channel for this key if it does not exist
        if !self.key_state_watcher.contains_key(&key) {
            let (tx, rx) = watch::channel(KeyState::Loading);
            self.key_state_sender.insert(key.clone(), tx);
            self.key_state_watcher.insert(key.clone(), rx);
        }
        // TODO: Read key from the database
        let value = Bytes::from("dummy value for now");
        // Check if key is in BTree. If not, add it
        self.prefetch_store.entry(key.clone()).or_insert(value);
        // Update key_state to reflect fetch completion
        self.key_state.insert(key.clone(), KeyState::Fetched);
        // Notify all watchers of the state change
        if let Some(sender) = self.key_state_sender.get(&key) {
            let _ = sender.send(KeyState::Fetched);
        }
    }

    // For debugging
    fn print_buffer(&self) {
        println!("prefetch_store: {:?}", self.prefetch_store);
        println!("key_state: {:?}", self.key_state);
        println!("transaction_keys: {:?}", self.transaction_keys);
        println!("key_transactions: {:?}", self.key_transactions);
        println!("key_state_watcher: {:?}", self.key_state_watcher);
        println!("key_state_sender: {:?}", self.key_state_sender);
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
            key_transactions: HashMap::new(),
            key_state_watcher: HashMap::new(),
            key_state_sender: HashMap::new(),
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
