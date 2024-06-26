use bytes::Bytes;
use common::keyspace_id::KeyspaceId;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use uuid::Uuid;

use crate::storage::cassandra::*;
use crate::storage::Error;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum KeyState {
    Fetched,   // Key has been fetched and is in the BTree
    Loading,   // Key has been requested and is being fetched
    Requested, // Key has been requested but fetching has not been started
}

#[derive(Default, Debug)]
pub struct PrefetchingBuffer {
    pub prefetch_store: Arc<Mutex<BTreeMap<Bytes, Bytes>>>, // stores key / value
    pub key_state: Arc<Mutex<HashMap<Bytes, KeyState>>>,    // stores key -> current fetch state
    pub transaction_keys: Arc<Mutex<HashMap<Uuid, BTreeSet<Bytes>>>>, // stores transaction_id -> set of requested keys
    pub key_transactions: Arc<Mutex<HashMap<Bytes, BTreeSet<Uuid>>>>, // stores key -> set of transaction_ids
    pub key_state_watcher: Arc<Mutex<HashMap<Bytes, watch::Receiver<KeyState>>>>, // key -> receiver for state changes
    pub key_state_sender: Arc<Mutex<HashMap<Bytes, watch::Sender<KeyState>>>>, // key -> sender for state changes
}

impl PrefetchingBuffer {
    pub fn new() -> Self {
        PrefetchingBuffer {
            prefetch_store: Arc::new(Mutex::new(BTreeMap::new())),
            key_state: Arc::new(Mutex::new(HashMap::new())),
            transaction_keys: Arc::new(Mutex::new(HashMap::new())),
            key_transactions: Arc::new(Mutex::new(HashMap::new())),
            key_state_watcher: Arc::new(Mutex::new(HashMap::new())),
            key_state_sender: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    // TODO: fix result type
    // TODO: fix clones
    // TODO: Error checking
    pub async fn process_prefetch_request(
        &self,
        transaction_id: Uuid,
        key: Bytes,
    ) -> Option<KeyState> {
        {
            let mut transaction_keys = self.transaction_keys.lock().await;
            let mut key_transactions = self.key_transactions.lock().await;
            // Log that this transaction is requesting this key
            self.add_to_transaction_keys(&mut transaction_keys, transaction_id, key.clone());
            // Log that this key is being requested by this transaction
            self.add_to_key_transactions(&mut key_transactions, transaction_id, key.clone());
        }

        {
            let mut key_state = self.key_state.lock().await;
            // Check if key has already been requested by another transaction
            // If not, add to key_state with fetch state requested
            key_state.entry(key.clone()).or_insert(KeyState::Requested);

            // TODO: This can be refactored to just return key_state.get(&key)
            match key_state.get(&key) {
                Some(KeyState::Fetched) => {
                    println!("Returning");
                    self.print_buffer();
                    // return Ok(());
                    return Some(KeyState::Fetched);
                } // return ok
                Some(KeyState::Loading) => {
                    // wait for fetch to complete
                    println!("Fetch is loading");
                    // This was moved to range_manager. TODO: Check with Tamer if this is appropriate design pattern
                    // if let Some(receiver) = self.key_state_watcher.lock().await.get(&key) {
                    //     let mut receiver = receiver.clone();
                    //     while receiver.changed().await.is_ok() {
                    //         if *receiver.borrow() == KeyState::Fetched {
                    //             println!("Fetch is done");
                    //             self.print_buffer();
                    //             return Some(KeyState::Fetched); // return ok once complete
                    //         }
                    //     }
                    // }
                    return Some(KeyState::Loading);
                }
                Some(KeyState::Requested) => {
                    println!("Requesting fetch");
                    self.print_buffer();
                    return Some(KeyState::Requested); // return ok once complete
                }
                None => None,
            }
        }
    }

    /// Adds a transaction and its requested key to the transaction_key hashmap.
    /// If the transaction already appears in the map, it adds the requested key
    /// to the set of requested keys.
    /// TODO: Add error checking and return
    fn add_to_transaction_keys(
        &self,
        transaction_keys: &mut HashMap<Uuid, BTreeSet<Bytes>>,
        transaction_id: Uuid,
        key: Bytes,
    ) {
        let transaction_set = transaction_keys.get_mut(&transaction_id);
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
                transaction_keys.insert(transaction_id, key_set);
            }
        }
    }

    /// Adds a key and its requesting transactions to the key_transaction hashmap.
    /// If the key already appears in the map, it adds the requesting transaction
    /// to the set of requesting transactions.
    /// TODO: Add error checking and return
    fn add_to_key_transactions(
        &self,
        key_transactions: &mut HashMap<Bytes, BTreeSet<Uuid>>,
        transaction_id: Uuid,
        key: Bytes,
    ) {
        let key_set = key_transactions.get_mut(&key);
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
                key_transactions.insert(key, transaction_set);
            }
        }
    }

    /// Logs that a key is currently being fetched from the database
    /// The key has to have been requested (and marked as Requested) prior
    /// to calling this function
    /// TODO: Return value and error checking
    pub async fn initiate_fetch(&self, key: Bytes) -> Result<(), ()> {
        {
            let mut key_state = self.key_state.lock().await;
            // Update key_state to reflect beginning of fetch
            match key_state.insert(key.clone(), KeyState::Loading) {
                Some(_) => return Ok(()),
                // We shouldn't be initiating fetch if the key was never requested in the first place
                None => return Err(()),
            };
        }
        // This was moved to range_manager. TODO: Check with Tamer if this is appropriate design pattern
        // {
        //     let mut key_state_watcher = self.key_state_watcher.lock().await;
        //     // Create a watch channel for this key if it does not exist
        //     if !key_state_watcher.contains_key(&key) {
        //         let (tx, rx) = watch::channel(KeyState::Loading);
        //         {
        //             let mut key_state_sender = self.key_state_sender.lock().await;
        //             key_state_sender.insert(key.clone(), tx);
        //         }
        //         key_state_watcher.insert(key.clone(), rx);
        //     }
        // }
        // Ok(())
    }

    /// Once fetch from database is complete, adds they key value to the Btree
    /// and updates the key state to Fetched. Notifies any waiting requesters
    /// that the fetch is complete
    pub async fn fetch_complete(&self, key: Bytes, value: Bytes) {
        let mut prefetch_store = self.prefetch_store.lock().await;
        let mut key_state = self.key_state.lock().await;
        // let key_state_sender = self.key_state_sender.lock().await;
        // Check if key is in BTree. If not, add it
        prefetch_store.entry(key.clone()).or_insert(value);
        // Update key_state to reflect fetch completion
        key_state.insert(key.clone(), KeyState::Fetched);
        // Notify all watchers of the state change
        // This was moved to range_manager. TODO: Check with Tamer if this is appropriate design pattern
        // if let Some(sender) = key_state_sender.get(&key) {
        //     let _ = sender.send(KeyState::Fetched);
        // }
    }

    /// Once transaction is complete, transaction calls this function to undo prefetch
    /// request and update BTree if needed
    /// TODO: fix result type
    /// TODO: fix clones
    /// TODO: Error checking
    pub async fn process_transaction_complete(
        &mut self,
        transaction_id: Uuid,
        key: Bytes,
        value: Bytes,
    ) -> Result<(), ()> {
        {
            let mut transaction_keys = self.transaction_keys.lock().await;
            let mut key_transactions = self.key_transactions.lock().await;
            // Log that this transaction is done requesting this key
            self.remove_from_transaction_keys(&mut transaction_keys, transaction_id, key.clone());
            // Log that this key is no longer being requested by this transaction
            self.remove_from_key_transactions(&mut key_transactions, transaction_id, key.clone());
        }

        let mut prefetch_store = self.prefetch_store.lock().await;
        let mut key_state = self.key_state.lock().await;
        // If key is still being requested by a different transaction, update BTree with latest value
        if self.key_transactions.lock().await.contains_key(&key) {
            let _ = prefetch_store.insert(key, value);
        } else {
            // If key is not being requested by any transactions, remove from BTree
            let _ = prefetch_store.remove(&key);
            let _ = key_state.remove(&key);
        }
        Ok(())
    }

    /// Removes a requested key from the transaction_key hashmap.
    /// If the transaction has no more requested keys, it removes the transaction
    /// from the hashmap
    /// TODO: Add error checking and return
    fn remove_from_transaction_keys(
        &self,
        transaction_keys: &mut HashMap<Uuid, BTreeSet<Bytes>>,
        transaction_id: Uuid,
        key: Bytes,
    ) {
        let transaction_set = transaction_keys.get_mut(&transaction_id);
        match transaction_set {
            Some(s) => {
                // Remove key from transaction's request list
                let _ = s.remove(&key);
                // If the transaction is no longer requesting keys, remove the entire transaction
                if s.is_empty() {
                    let _ = transaction_keys.remove(&transaction_id);
                }
            }
            None => {
                // Transaction has not requested any keys, so there is an error
                // TODO: fix panic
                panic!("Remove request for a transaction that is not currently logged");
            }
        }
    }

    /// Removes a transaction from the key_transaction hashmap.
    /// If the key is no longer requested by any transactions it removes the key
    /// from the hashmap
    /// TODO: Add error checking and return
    fn remove_from_key_transactions(
        &self,
        key_transactions: &mut HashMap<Bytes, BTreeSet<Uuid>>,
        transaction_id: Uuid,
        key: Bytes,
    ) {
        let key_set = key_transactions.get_mut(&key);
        match key_set {
            Some(s) => {
                // Remove transaction_id from existing set
                let _ = s.remove(&transaction_id);
                // If the key is no longer being requested by any transaction, remove the entire key
                if s.is_empty() {
                    let _ = key_transactions.remove(&key);
                }
            }
            None => {
                // Key has not been requested by any transactions, so there is an error
                // TODO: fix panic
                panic!("Remove request for a key that is not currently logged");
            }
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
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    async fn test_add_to_transaction_keys() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_id = Uuid::new_v4();
        let fake_key = Bytes::from("testing!");

        {
            let mut transaction_keys = prefetching_buffer.transaction_keys.lock().await;
            prefetching_buffer.add_to_transaction_keys(&mut transaction_keys, fake_id, fake_key);
        }

        let transaction_keys = prefetching_buffer.transaction_keys.lock().await;
        let other_key = transaction_keys.get(&fake_id);
        match other_key {
            Some(s) => assert!(s.contains(&Bytes::from("testing!"))),
            None => panic!("Set did not contain a value it should have contained"),
        }
    }

    #[tokio::test]
    async fn test_add_to_key_transactions() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_id = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        {
            let mut key_transactions = prefetching_buffer.key_transactions.lock().await;
            prefetching_buffer.add_to_key_transactions(
                &mut key_transactions,
                fake_id,
                fake_key.clone(),
            );
        }

        let key_transactions = prefetching_buffer.key_transactions.lock().await;
        let other_transaction = key_transactions.get(&fake_key);
        match other_transaction {
            Some(s) => {
                assert!(s.contains(&Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap()))
            }
            None => panic!("Set did not contain a value it should have contained"),
        }
    }

    #[tokio::test]
    async fn test_process_prefetch_request() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_id = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        // Test return for a brand new key
        assert_eq!(
            prefetching_buffer
                .process_prefetch_request(fake_id, fake_key.clone())
                .await,
            Some(KeyState::Requested)
        );

        // Test proper function of update key_state
        assert_eq!(
            prefetching_buffer.initiate_fetch(fake_key.clone()).await,
            Ok(())
        );

        // Test transition to loading status for the key
        assert_eq!(
            prefetching_buffer
                .process_prefetch_request(fake_id, fake_key.clone())
                .await,
            Some(KeyState::Loading)
        );

        let fake_value = Bytes::from("testing value");
        prefetching_buffer
            .fetch_complete(fake_key.clone(), fake_value.clone())
            .await;

        assert_eq!(
            prefetching_buffer
                .process_prefetch_request(fake_id.clone(), fake_key.clone())
                .await,
            Some(KeyState::Fetched)
        );

        let value = prefetching_buffer.prefetch_store.lock().await;
        let bind = value.get(&fake_key.clone()).unwrap();

        assert_eq!(*bind, fake_value);
    }
}
