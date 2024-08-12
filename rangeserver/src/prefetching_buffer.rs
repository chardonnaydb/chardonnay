use bytes::Bytes;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use tokio::sync::{watch, Mutex};
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum KeyState {
    Fetched,        // Key has been fetched and is in the BTree
    Loading(u64),   // Key has been requested and is being fetched
    Requested(u64), // Key has been requested but fetching has not been started
}

#[derive(Debug)]
struct State {
    pub prefetch_store: BTreeMap<Bytes, Bytes>, // stores key / value
    pub key_state: HashMap<Bytes, KeyState>,    // stores key -> current fetch state
    pub transaction_keys: HashMap<Uuid, BTreeSet<Bytes>>, // stores transaction_id -> set of requested keys
    pub key_transactions: HashMap<Bytes, BTreeSet<Uuid>>, // stores key -> set of transaction_ids
    pub key_state_watcher: HashMap<Bytes, watch::Receiver<KeyState>>, // key -> receiver for state changes
    pub key_state_sender: HashMap<Bytes, watch::Sender<KeyState>>, // key -> sender for state changes
    pub fetch_sequence_number: u64,
}

pub struct PrefetchingBuffer {
    state: Mutex<State>,
}

impl PrefetchingBuffer {
    pub fn new() -> Self {
        PrefetchingBuffer {
            state: Mutex::new(State {
                prefetch_store: BTreeMap::new(),
                key_state: HashMap::new(),
                transaction_keys: HashMap::new(),
                key_transactions: HashMap::new(),
                key_state_watcher: HashMap::new(),
                key_state_sender: HashMap::new(),
                fetch_sequence_number: 0,
            }),
        }
    }

    /// Returns the value for a key from the prefetch_store
    pub async fn get_from_buffer(&self, key: Bytes) -> Result<Option<Bytes>, ()> {
        let cur_state = self.state.lock().await;
        if let Some(KeyState::Fetched) = cur_state.key_state.get(&key.clone()) {
            let val = cur_state.prefetch_store.get(&key);
            if let Some(value) = val {
                return Ok(Some(value.clone()));
            } else {
                return Err(());
            }
        } else {
            return Ok(None);
        }
    }

    /// Processes prefetch request by adding transaction / key request to
    /// the appropriate data structures and returns the state of they key,
    /// which is either Requested, Loading, or Fetched. If Loading, the
    /// process waits for another transaction to complete the fetch
    pub async fn process_prefetch_request(&self, transaction_id: Uuid, key: Bytes) -> KeyState {
        let mut cur_state = self.state.lock().await;
        // Log that this transaction is requesting this key
        self.add_to_transaction_keys(&mut cur_state.transaction_keys, transaction_id, key.clone());
        // Log that this key is being requested by this transaction
        self.add_to_key_transactions(&mut cur_state.key_transactions, transaction_id, key.clone());

        // Check if key has already been requested by another transaction
        // If not, add to key_state with fetch state Requested
        if !cur_state.key_state.contains_key(&key.clone()) {
            cur_state.fetch_sequence_number += 1;
            let sequence_number = cur_state.fetch_sequence_number;
            cur_state
                .key_state
                .insert(key.clone(), KeyState::Requested(sequence_number));
        }

        let key_state = cur_state.key_state.get(&key).cloned().unwrap();
        match key_state {
            KeyState::Fetched => return KeyState::Fetched,
            KeyState::Loading(n) => {
                drop(cur_state);
                // wait for fetch to complete
                let key_state_watcher = self.state.lock().await.key_state_watcher.clone();
                if let Some(receiver) = key_state_watcher.get(&key) {
                    let mut receiver = receiver.clone();
                    while receiver.changed().await.is_ok() {
                        if *receiver.borrow() == KeyState::Fetched {
                            return KeyState::Fetched;
                        } else {
                            // The fetch failed and was returned to KeyState::Requested
                            // Return KeyState::Requested so that the caller can try again
                            let mut cur_state = self.state.lock().await;
                            self.change_keystate_to_loading(key, n, &mut cur_state)
                                .await
                                .unwrap();
                            return KeyState::Requested(n);
                        }
                    }
                }
                return KeyState::Loading(n);
            }
            KeyState::Requested(n) => {
                // Update keystate to loading while holding the lock to avoid race condition
                // let mut cur_state = self.state.lock().await;
                self.change_keystate_to_loading(key, n, &mut cur_state)
                    .await
                    .unwrap();
                return KeyState::Requested(n);
            }
        }
    }

    /// Adds a transaction and its requested key to the transaction_keys hashmap.
    /// If the transaction already appears in the map, it adds the requested key
    /// to the set of requested keys.
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
            None => {
                // Transaction has not previously requested any keys, so create new set and add
                // to transaction_keys
                let mut key_set = BTreeSet::new();
                key_set.insert(key);
                transaction_keys.insert(transaction_id, key_set);
            }
        }
    }

    /// Adds a key and its requesting transactions to the key_transactions hashmap.
    /// If the key already appears in the map, it adds the requesting transaction
    /// to the set of requesting transactions.
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
    /// to calling this function, and the calling function should then
    /// proceed to fetching the key
    async fn change_keystate_to_loading(
        &self,
        key: Bytes,
        fetch_sequence_number: u64,
        state: &mut State,
    ) -> Result<(), ()> {
        {
            // Update key_state to reflect beginning of fetch
            match state
                .key_state
                .insert(key.clone(), KeyState::Loading(fetch_sequence_number))
            {
                Some(_) => {
                    // Create a watch channel for this key if it does not exist
                    if !state.key_state_watcher.contains_key(&key) {
                        let (tx, rx) = watch::channel(KeyState::Loading(fetch_sequence_number));
                        state.key_state_sender.insert(key.clone(), tx);
                        state.key_state_watcher.insert(key.clone(), rx);
                    }
                    return Ok(());
                }
                // We shouldn't be initiating fetch if the key was never requested in the first place
                None => return Err(()),
            }
        }
    }

    /// Once fetch from database is complete, adds the key value to the Btree
    /// and updates the key state to Fetched. Notifies any waiting requesters
    /// that the fetch is complete
    pub async fn fetch_complete(
        &self,
        key: Bytes,
        value: Option<Bytes>,
        fetch_sequence_number: u64,
    ) {
        let mut cur_state = self.state.lock().await;
        if let Some(KeyState::Loading(n)) = cur_state.key_state.get(&key.clone()) {
            // If the current loading number is not equal to input fetch_sequence_number
            // this means another later transaction deleted and changed the key
            if *n == fetch_sequence_number {
                // Update/add key to BTree
                cur_state.prefetch_store.insert(key.clone(), value.unwrap());
                // Update key_state to reflect fetch completion
                cur_state.key_state.insert(key.clone(), KeyState::Fetched);
                // Notify all watchers of the state change
                if let Some(sender) = cur_state.key_state_sender.get(&key) {
                    let _ = sender.send(KeyState::Fetched);
                }
            }
        }
    }

    /// If a fetch fails, change the key_state back from Loading back to Requested
    /// and notify waiting transactions so that they can try the fetch again
    pub async fn fetch_failed(&self, key: Bytes, fetch_sequence_number: u64) {
        let mut cur_state = self.state.lock().await;
        if let Some(KeyState::Loading(n)) = cur_state.key_state.get(&key.clone()) {
            // If the current loading number is not equal to input fetch_sequence_number
            // this means another later transaction deleted and changed the key
            if *n == fetch_sequence_number {
                cur_state
                    .key_state
                    .insert(key.clone(), KeyState::Requested(fetch_sequence_number));
                if let Some(sender) = cur_state.key_state_sender.get(&key) {
                    let _ = sender.send(KeyState::Requested(fetch_sequence_number));
                }
            }
        }
    }

    /// Marks a transaction as being complete by updating bookkeeping to remove
    /// the request from the data structures
    pub async fn process_transaction_complete(&self, transaction_id: Uuid) {
        let mut cur_state = self.state.lock().await;
        let keys = cur_state.transaction_keys.get(&transaction_id).cloned();

        // Loop over set of transaction keys to call remove key transactions (accept only transaction id)
        if let Some(keys) = keys {
            for key in keys {
                // Log that this key is no longer being requested by this transaction
                self.remove_from_key_transactions(
                    &mut cur_state.key_transactions,
                    transaction_id,
                    key.clone(),
                );
                // If the key is no longer requested by any transaction, also delete it from the BTree
                if !cur_state.key_transactions.contains_key(&key) {
                    self.evict_key(key.clone(), &mut cur_state).await;
                }
            }
        }
        // Remove transaction from transaction_key
        let _ = cur_state.transaction_keys.remove(&transaction_id);
    }

    /// Update the prefetch_store and update the key_state
    pub async fn upsert(&self, key: Bytes, value: Bytes) {
        let mut cur_state = self.state.lock().await;
        // If key is in key_state, it is being requested by a transaction
        if cur_state.key_state.contains_key(&key.clone()) {
            let _ = cur_state.prefetch_store.insert(key.clone(), value);
            // If the key is still loading in a prefetch, change to fetched
            if let KeyState::Loading(_) = cur_state.key_state.get(&key).unwrap() {
                cur_state.key_state.insert(key.clone(), KeyState::Fetched);
                // Notify all watchers of the state change
                if let Some(sender) = cur_state.key_state_sender.get(&key) {
                    let _ = sender.send(KeyState::Fetched);
                }
            }
        }
    }

    /// If a transaction deletes a key, it must also be deleted from the buffer
    /// This function deletes a key from prefetch_store and updates the key_state
    /// to fetched (i.e. the buffer reflects the state of the database)
    pub async fn delete(&self, key: Bytes) {
        let mut cur_state = self.state.lock().await;
        if cur_state.key_state.contains_key(&key.clone()) {
            let _ = cur_state.prefetch_store.remove(&key.clone());
            // If the key is still loading in a prefetch, change to fetched
            if let KeyState::Loading(_) = cur_state.key_state.get(&key).unwrap() {
                cur_state.key_state.insert(key.clone(), KeyState::Fetched);
                // Notify all watchers of the state change
                if let Some(sender) = cur_state.key_state_sender.get(&key) {
                    let _ = sender.send(KeyState::Fetched);
                }
            }
        }
    }

    /// If no transactions are  requesting a key, it can be flushed from the buffer
    /// This function deletes a key from prefetch_store and the key_state
    /// It needs be called with the lock held
    async fn evict_key(&self, key: Bytes, state: &mut State) {
        let _ = state.prefetch_store.remove(&key);
        let _ = state.key_state.remove(&key); // might be None
    }

    /// Removes a transaction from the key_transaction hashmap.
    /// If the key is no longer requested by any transactions it removes the key
    /// from the hashmap
    fn remove_from_key_transactions(
        &self,
        key_transactions: &mut HashMap<Bytes, BTreeSet<Uuid>>,
        transaction_id: Uuid,
        key: Bytes,
    ) {
        let key_set = key_transactions.get_mut(&key);
        if let Some(s) = key_set {
            // Remove transaction_id from existing set
            let _ = s.remove(&transaction_id);
            // If the key is no longer being requested by any transaction, remove the entire key
            if s.is_empty() {
                let _ = key_transactions.remove(&key);
            }
        }
        // If key_set is None, then no transactions requested this key, so we do nothing
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::task::Wake;
    use std::task::{Context, Poll, Waker};

    #[tokio::test]
    async fn test_add_to_transaction_keys() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_transaction = Uuid::new_v4();
        let fake_key = Bytes::from("testing!");

        {
            let mut cur_state = prefetching_buffer.state.lock().await;
            prefetching_buffer.add_to_transaction_keys(
                &mut cur_state.transaction_keys,
                fake_transaction,
                fake_key,
            );
        }

        let cur_state = prefetching_buffer.state.lock().await;
        let other_key = cur_state.transaction_keys.get(&fake_transaction);
        match other_key {
            Some(s) => assert!(s.contains(&Bytes::from("testing!"))),
            None => panic!("Set did not contain a value it should have contained"),
        }
    }

    #[tokio::test]
    async fn test_add_to_key_transactions() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_transaction = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        {
            let mut cur_state = prefetching_buffer.state.lock().await;
            prefetching_buffer.add_to_key_transactions(
                &mut cur_state.key_transactions,
                fake_transaction,
                fake_key.clone(),
            );
        }

        let cur_state = prefetching_buffer.state.lock().await;
        let other_transaction = cur_state.key_transactions.get(&fake_key);
        match other_transaction {
            Some(s) => {
                assert!(s.contains(&Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap()))
            }
            None => panic!("Set did not contain a value it should have contained"),
        }
    }

    struct DummyWaker;

    impl Wake for DummyWaker {
        fn wake(self: Arc<Self>) {}
    }

    fn create_dummy_waker() -> Waker {
        let arc = Arc::new(DummyWaker);
        Waker::from(arc)
    }

    #[tokio::test]
    async fn test_process_prefetch_request() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_transaction = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        // Test return for a brand new key - should be KeyState::Requested
        assert_eq!(
            prefetching_buffer
                .process_prefetch_request(fake_transaction, fake_key.clone())
                .await,
            KeyState::Requested(1)
        );
    }

    #[tokio::test]
    async fn automatic_update_to_loading() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_transaction = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        let _ = prefetching_buffer
            .process_prefetch_request(fake_transaction, fake_key.clone())
            .await;

        // Test that key is automatically updated to Loading
        {
            let cur_state = prefetching_buffer.state.lock().await;
            let val = cur_state.key_state.get(&fake_key.clone()).unwrap();
            assert_eq!(*val, KeyState::Loading(1));
        }
    }

    #[tokio::test]
    async fn watcher_wait_for_load() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_transaction = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        let _ = prefetching_buffer
            .process_prefetch_request(fake_transaction, fake_key.clone())
            .await;

        // Check that asking for the same key waits until the key is loaded
        let other_fake_transaction =
            Uuid::from_str("c68c05e4-4edd-4f96-ae25-20761a4653c5").unwrap();
        let pending_future =
            prefetching_buffer.process_prefetch_request(other_fake_transaction, fake_key.clone());

        // Create a dummy waker
        let waker = create_dummy_waker();
        // Create a context with the waker
        let mut context = Context::from_waker(&waker);
        // Pin the future to the stack
        let mut pending_future = Box::pin(pending_future);

        // Check that the future is pending
        assert_eq!(
            Future::poll(Pin::as_mut(&mut pending_future), &mut context),
            Poll::Pending
        );
    }

    #[tokio::test]
    async fn watcher_change_to_fetched() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_transaction = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        let _ = prefetching_buffer
            .process_prefetch_request(fake_transaction, fake_key.clone())
            .await;

        // Check that asking for the same key waits until the key is loaded
        let other_fake_transaction =
            Uuid::from_str("c68c05e4-4edd-4f96-ae25-20761a4653c5").unwrap();
        let pending_future =
            prefetching_buffer.process_prefetch_request(other_fake_transaction, fake_key.clone());

        // Create a dummy waker
        let waker = create_dummy_waker();
        // Create a context with the waker
        let mut context = Context::from_waker(&waker);
        // Pin the future to the stack
        let mut pending_future = Box::pin(pending_future);

        // advance poll
        let _ = Future::poll(Pin::as_mut(&mut pending_future), &mut context);

        // Add value to BTree for this key
        let fake_value = Some(Bytes::from("testing value"));
        let cur_state = prefetching_buffer.state.lock().await;
        let n = cur_state.fetch_sequence_number;
        drop(cur_state);
        let _ = prefetching_buffer
            .fetch_complete(fake_key.clone(), fake_value.clone(), n)
            .await;

        // Check that the future is now marked as ready
        assert_eq!(
            Future::poll(Pin::as_mut(&mut pending_future), &mut context),
            Poll::Ready(KeyState::Fetched)
        );
    }

    #[tokio::test]
    async fn mark_as_fetch() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_transaction = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        let _ = prefetching_buffer
            .process_prefetch_request(fake_transaction, fake_key.clone())
            .await;

        // Check that asking for the same key waits until the key is loaded
        let other_fake_transaction =
            Uuid::from_str("c68c05e4-4edd-4f96-ae25-20761a4653c5").unwrap();
        let _ =
            prefetching_buffer.process_prefetch_request(other_fake_transaction, fake_key.clone());

        // Add value to BTree for this key
        let fake_value = Some(Bytes::from("testing value"));
        let cur_state = prefetching_buffer.state.lock().await;
        let n = cur_state.fetch_sequence_number;
        drop(cur_state);
        let _ = prefetching_buffer
            .fetch_complete(fake_key.clone(), fake_value.clone(), n)
            .await;

        // Check that processing a new request for the same key returns it as fetched
        assert_eq!(
            prefetching_buffer
                .process_prefetch_request(fake_transaction.clone(), fake_key.clone())
                .await,
            KeyState::Fetched
        );
    }

    #[tokio::test]
    async fn pull_from_prefetch_store() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_transaction = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        let _ = prefetching_buffer
            .process_prefetch_request(fake_transaction, fake_key.clone())
            .await;

        // Check that asking for the same key waits until the key is loaded
        let other_fake_transaction =
            Uuid::from_str("c68c05e4-4edd-4f96-ae25-20761a4653c5").unwrap();
        let _ =
            prefetching_buffer.process_prefetch_request(other_fake_transaction, fake_key.clone());

        // Add value to BTree for this key
        let fake_value = Some(Bytes::from("testing value"));
        let cur_state = prefetching_buffer.state.lock().await;
        let n = cur_state.fetch_sequence_number;
        drop(cur_state);
        let _ = prefetching_buffer
            .fetch_complete(fake_key.clone(), fake_value.clone(), n)
            .await;

        // Check that the value in the BTree for that key is as expected
        let cur_state = prefetching_buffer.state.lock().await;
        let value_from_tree = cur_state.prefetch_store.get(&fake_key.clone()).unwrap();

        assert_eq!(*value_from_tree, fake_value.unwrap());
    }

    #[tokio::test]
    async fn test_process_buffer_update() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_transaction = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        let _ = prefetching_buffer
            .process_prefetch_request(fake_transaction, fake_key.clone())
            .await;

        // Check that asking for the same key waits until the key is loaded
        let other_fake_transaction =
            Uuid::from_str("c68c05e4-4edd-4f96-ae25-20761a4653c5").unwrap();
        let pending_future =
            prefetching_buffer.process_prefetch_request(other_fake_transaction, fake_key.clone());

        // Create a dummy waker
        let waker = create_dummy_waker();
        // Create a context with the waker
        let mut context = Context::from_waker(&waker);
        // Pin the future to the stack
        let mut pending_future = Box::pin(pending_future);

        // advance poll
        let _ = Future::poll(Pin::as_mut(&mut pending_future), &mut context);

        // Add value to BTree for this key
        let fake_value = Some(Bytes::from("testing value"));
        let cur_state = prefetching_buffer.state.lock().await;
        let n = cur_state.fetch_sequence_number;
        drop(cur_state);
        let _ = prefetching_buffer
            .fetch_complete(fake_key.clone(), fake_value.clone(), n)
            .await;

        // Process transaction is complete for fake_transaction with a new value
        // The value for fake_key should be updated in this process
        let _ = Future::poll(Pin::as_mut(&mut pending_future), &mut context);

        // Update buffer
        let new_value = Bytes::from("updated testing value");
        prefetching_buffer
            .upsert(fake_key.clone(), new_value.clone())
            .await;

        let cur_state = prefetching_buffer.state.lock().await;
        assert_eq!(
            cur_state.prefetch_store.get(&fake_key.clone()).unwrap(),
            &new_value
        );
    }

    #[tokio::test]
    async fn test_process_transaction_delete() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_transaction = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        let _ = prefetching_buffer
            .process_prefetch_request(fake_transaction, fake_key.clone())
            .await;

        // Check that asking for the same key waits until the key is loaded
        let other_fake_transaction =
            Uuid::from_str("c68c05e4-4edd-4f96-ae25-20761a4653c5").unwrap();
        let pending_future =
            prefetching_buffer.process_prefetch_request(other_fake_transaction, fake_key.clone());

        // Create a dummy waker
        let waker = create_dummy_waker();
        // Create a context with the waker
        let mut context = Context::from_waker(&waker);
        // Pin the future to the stack
        let mut pending_future = Box::pin(pending_future);

        // advance poll
        let _ = Future::poll(Pin::as_mut(&mut pending_future), &mut context);

        // Add value to BTree for this key
        let fake_value = Some(Bytes::from("testing value"));
        let cur_state = prefetching_buffer.state.lock().await;
        let n = cur_state.fetch_sequence_number;
        drop(cur_state);
        let _ = prefetching_buffer
            .fetch_complete(fake_key.clone(), fake_value.clone(), n)
            .await;

        // Process transaction is complete for fake_transaction with a new value
        // The value for fake_key should be updated in this process
        let _ = Future::poll(Pin::as_mut(&mut pending_future), &mut context);

        // Transaction deletes key
        prefetching_buffer.delete(fake_key.clone()).await;

        let cur_state = prefetching_buffer.state.lock().await;
        assert_eq!(cur_state.prefetch_store.get(&fake_key.clone()), None);
    }

    #[tokio::test]
    async fn test_transaction_complete() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_transaction = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        let _ = prefetching_buffer
            .process_prefetch_request(fake_transaction, fake_key.clone())
            .await;

        // Check that asking for the same key waits until the key is loaded
        let other_fake_transaction =
            Uuid::from_str("c68c05e4-4edd-4f96-ae25-20761a4653c5").unwrap();
        let pending_future =
            prefetching_buffer.process_prefetch_request(other_fake_transaction, fake_key.clone());

        // Create a dummy waker
        let waker = create_dummy_waker();
        // Create a context with the waker
        let mut context = Context::from_waker(&waker);
        // Pin the future to the stack
        let mut pending_future = Box::pin(pending_future);

        // advance poll
        let _ = Future::poll(Pin::as_mut(&mut pending_future), &mut context);

        // Add value to BTree for this key
        let fake_value = Some(Bytes::from("testing value"));
        let cur_state = prefetching_buffer.state.lock().await;
        let n = cur_state.fetch_sequence_number;
        drop(cur_state);
        let _ = prefetching_buffer
            .fetch_complete(fake_key.clone(), fake_value.clone(), n)
            .await;

        // Process transaction is complete for fake_transaction with a new value
        // The value for fake_key should be updated in this process
        let _ = Future::poll(Pin::as_mut(&mut pending_future), &mut context);

        // Update buffer
        let new_value = Bytes::from("updated testing value");
        let _ = prefetching_buffer
            .upsert(fake_key.clone(), new_value.clone())
            .await;

        // Remove prefetch request
        prefetching_buffer
            .process_transaction_complete(fake_transaction)
            .await;

        // other_fake_transaction is still requesting the key, so buffer
        // should still contain the new value
        {
            let cur_state = prefetching_buffer.state.lock().await;
            let value_from_tree = cur_state.prefetch_store.get(&fake_key.clone()).unwrap();

            assert_eq!(value_from_tree, &new_value);
        }

        // Check that Keystate still contains the value
        {
            let cur_state = prefetching_buffer.state.lock().await;
            let value_from_map = cur_state.key_state.get(&fake_key.clone()).unwrap();

            assert_eq!(value_from_map, &KeyState::Fetched);
        }

        // Check that transaction_keys does not contain the transaction
        {
            let cur_state = prefetching_buffer.state.lock().await;
            let value_from_tree = cur_state.transaction_keys.get(&fake_transaction.clone());

            assert_eq!(value_from_tree, None);
        }

        // Check that key_transactions does contain the key and other fake transaction
        {
            let cur_state = prefetching_buffer.state.lock().await;
            let value_from_tree = cur_state.key_transactions.get(&fake_key.clone()).unwrap();

            assert!(value_from_tree.contains(&other_fake_transaction));
        }
    }

    #[tokio::test]
    async fn buffer_value_update() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_transaction = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        let _ = prefetching_buffer
            .process_prefetch_request(fake_transaction, fake_key.clone())
            .await;

        // Check that asking for the same key waits until the key is loaded
        let other_fake_transaction =
            Uuid::from_str("c68c05e4-4edd-4f96-ae25-20761a4653c5").unwrap();

        let pending_future =
            prefetching_buffer.process_prefetch_request(other_fake_transaction, fake_key.clone());

        // Create a dummy waker
        let waker = create_dummy_waker();
        // Create a context with the waker
        let mut context = Context::from_waker(&waker);
        // Pin the future to the stack
        let mut pending_future = Box::pin(pending_future);

        // advance poll
        let _ = Future::poll(Pin::as_mut(&mut pending_future), &mut context);

        // Add value to BTree for this key
        let fake_value = Some(Bytes::from("testing value"));
        let cur_state = prefetching_buffer.state.lock().await;
        let n = cur_state.fetch_sequence_number;
        drop(cur_state);
        let _ = prefetching_buffer
            .fetch_complete(fake_key.clone(), fake_value.clone(), n)
            .await;

        // Process transaction is complete for fake_transaction with a new value
        // The value for fake_key should be updated in this process
        let _ = Future::poll(Pin::as_mut(&mut pending_future), &mut context);

        // Update buffer
        let new_value = Bytes::from("updated testing value");
        let _ = prefetching_buffer
            .upsert(fake_key.clone(), new_value.clone())
            .await;

        // Remove prefetch request
        let _ = prefetching_buffer
            .process_transaction_complete(fake_transaction)
            .await;

        // Check that value has been updated in the Btree
        let cur_state = prefetching_buffer.state.lock().await;
        let value_from_tree = cur_state.prefetch_store.get(&fake_key.clone()).unwrap();

        assert_eq!(*value_from_tree, new_value);
    }

    #[tokio::test]
    async fn test_buffer_cleanup() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_transaction = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        let _ = prefetching_buffer
            .process_prefetch_request(fake_transaction, fake_key.clone())
            .await;

        // Check that asking for the same key waits until the key is loaded
        let other_fake_transaction =
            Uuid::from_str("c68c05e4-4edd-4f96-ae25-20761a4653c5").unwrap();
        let pending_future =
            prefetching_buffer.process_prefetch_request(other_fake_transaction, fake_key.clone());

        // Create a dummy waker
        let waker = create_dummy_waker();
        // Create a context with the waker
        let mut context = Context::from_waker(&waker);
        // Pin the future to the stack
        let mut pending_future = Box::pin(pending_future);

        // advance poll
        let _ = Future::poll(Pin::as_mut(&mut pending_future), &mut context);

        // Add value to BTree for this key
        let fake_value = Some(Bytes::from("testing value"));
        let cur_state = prefetching_buffer.state.lock().await;
        let n = cur_state.fetch_sequence_number;
        drop(cur_state);
        let _ = prefetching_buffer
            .fetch_complete(fake_key.clone(), fake_value.clone(), n)
            .await;

        // Process transaction is complete for fake_transaction with a new value
        // The value for fake_key should be updated in this process
        let _ = Future::poll(Pin::as_mut(&mut pending_future), &mut context);

        // Update buffer
        let new_value = Bytes::from("updated testing value");
        let _ = prefetching_buffer
            .upsert(fake_key.clone(), new_value.clone())
            .await;

        // Remove prefetch request
        let _ = prefetching_buffer
            .process_transaction_complete(fake_transaction)
            .await;

        // Process transaction is complete for other_fake_transaction
        // fake_key should be removed from the BTree in this process given no other
        // transactions are requesting the key
        let _ = prefetching_buffer
            .process_transaction_complete(other_fake_transaction)
            .await;

        // Check that BTree does not contain the value
        {
            let cur_state = prefetching_buffer.state.lock().await;
            let value_from_tree = cur_state.prefetch_store.get(&fake_key.clone());

            assert_eq!(value_from_tree, None);
        }

        // Check that Keystate does not contain the value
        {
            let cur_state = prefetching_buffer.state.lock().await;
            let value_from_map = cur_state.key_state.get(&fake_key.clone());

            assert_eq!(value_from_map, None);
        }

        // Check that transaction_keys does not contain the transaction
        {
            let cur_state = prefetching_buffer.state.lock().await;
            let value_from_tree = cur_state.transaction_keys.get(&fake_transaction.clone());

            assert_eq!(value_from_tree, None);
        }

        // Check that key_transactions does not contain the key
        {
            let cur_state = prefetching_buffer.state.lock().await;
            let value_from_tree = cur_state.key_transactions.get(&fake_key.clone());

            assert_eq!(value_from_tree, None);
        }
    }

    #[tokio::test]
    async fn fetch_failure() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_transaction = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        let _ = prefetching_buffer
            .process_prefetch_request(fake_transaction, fake_key.clone())
            .await;

        // Check that asking for the same key waits until the key is loaded
        let other_fake_transaction =
            Uuid::from_str("c68c05e4-4edd-4f96-ae25-20761a4653c5").unwrap();
        let pending_future =
            prefetching_buffer.process_prefetch_request(other_fake_transaction, fake_key.clone());

        // Create a dummy waker
        let waker = create_dummy_waker();
        // Create a context with the waker
        let mut context = Context::from_waker(&waker);
        // Pin the future to the stack
        let mut pending_future = Box::pin(pending_future);

        // advance poll
        let _ = Future::poll(Pin::as_mut(&mut pending_future), &mut context);

        // Fetch failed from database
        let cur_state = prefetching_buffer.state.lock().await;
        let n = cur_state.fetch_sequence_number;
        drop(cur_state);
        let _ = prefetching_buffer.fetch_failed(fake_key.clone(), n).await;

        // Check that the future is now marked as loading
        assert_eq!(
            Future::poll(Pin::as_mut(&mut pending_future), &mut context),
            Poll::Ready(KeyState::Requested(n))
        );
    }

    #[tokio::test]
    async fn fetch_failure_loading() {
        let prefetching_buffer = PrefetchingBuffer::new();

        let fake_transaction = Uuid::from_str("fae86b67-36dd-41fa-a201-f18d3051bca5").unwrap();
        let fake_key = Bytes::from("testing!");

        let _ = prefetching_buffer
            .process_prefetch_request(fake_transaction, fake_key.clone())
            .await;

        // Check that asking for the same key waits until the key is loaded
        let other_fake_transaction =
            Uuid::from_str("c68c05e4-4edd-4f96-ae25-20761a4653c5").unwrap();
        let pending_future =
            prefetching_buffer.process_prefetch_request(other_fake_transaction, fake_key.clone());

        // Create a dummy waker
        let waker = create_dummy_waker();
        // Create a context with the waker
        let mut context = Context::from_waker(&waker);
        // Pin the future to the stack
        let mut pending_future = Box::pin(pending_future);

        // advance poll
        let _ = Future::poll(Pin::as_mut(&mut pending_future), &mut context);

        // Fetch failed from database
        let cur_state = prefetching_buffer.state.lock().await;
        let n = cur_state.fetch_sequence_number;
        drop(cur_state);
        let _ = prefetching_buffer.fetch_failed(fake_key.clone(), n).await;

        // Check that the future is now marked as loading
        let _ = Future::poll(Pin::as_mut(&mut pending_future), &mut context);

        // Test that key is automatically updated to Loading
        {
            let cur_state = prefetching_buffer.state.lock().await;
            let val = cur_state.key_state.get(&fake_key.clone()).unwrap();
            assert_eq!(*val, KeyState::Loading(n));
        }
    }
}
