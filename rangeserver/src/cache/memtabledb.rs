use skiplist::OrderedSkipList;
use std::cmp::Ordering;
use tokio::sync::RwLock;
use std::mem;
use bytes::Bytes;
use std::sync::Arc;
use crate::cache::{Cache, Error, GCCallback, CacheOptions};
use std::collections::HashMap;
use std::vec::Vec;
use std::thread;
use std::sync::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::cmp::min;
use std::cmp::max;
use core::ops::Bound::Included;

// memtable status changes from FLUSHED->MUTABLE->IMMUTABLE->FLUSHED
#[derive(Clone, Debug, PartialEq)]
pub enum MemTableStatus {
    FLUSHED,    // also initial status
    MUTABLE,
    IMMUTABLE,
}

#[derive(Clone, Debug, Eq)]
pub struct MemTableEntry {
    key: Bytes,
    val: Bytes,
    epoch: u64, 
    deleted: bool,
}

impl MemTableEntry {
    pub fn new(key: Bytes, val: Bytes, epoch: u64, deleted: bool, ) -> Self {
        MemTableEntry { key, val, epoch, deleted }
    }
}

impl PartialOrd for MemTableEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MemTableEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        let key_comp = self.key.cmp(&other.key);
        match key_comp {
            Ordering::Less | Ordering::Greater => key_comp,
            Ordering::Equal => other.epoch.cmp(&self.epoch),
        }
    }
}

impl PartialEq for MemTableEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.epoch == other.epoch
    }
}

#[derive(Debug)]
pub struct MemTable {
    id: usize,
    size_in_bytes: u64,
    status: MemTableStatus,
    latest_epoch: u64,
    oldest_epoch: u64,
    entries: OrderedSkipList<MemTableEntry>,
}

#[derive(Debug)]
pub struct MemTableDB {
    cache_options: CacheOptions,
    memtables: Vec<MemTable>,
    mut_memtable_idx: usize,
    latest_epoch: u64,
    gc_callback: Option<GCCallback>,
}

impl MemTableDB {
    pub async fn new(cache_options: Option<&CacheOptions>) -> Self {

        let mut opts = CacheOptions{
            path: "",
            num_write_buffers: 2,
            write_buffer_size: 2<<29,
         };

        match cache_options {
            Some(opt) => {opts = *opt},
            None => {},
        }

        let mut mt_db = MemTableDB {
            cache_options: opts.clone(),
            memtables: Vec::<MemTable>::new(), 
            mut_memtable_idx: 0,
            latest_epoch: 0,
            gc_callback: None,
        };

        for i in 0..opts.num_write_buffers {
            let mt = MemTable { 
                id: i,
                size_in_bytes: 0,
                status: MemTableStatus::FLUSHED,
                latest_epoch: 0,
                oldest_epoch: 0,
                // TODO: init with capacity
                entries: OrderedSkipList::<MemTableEntry>::new(),};
            mt_db.memtables.push(mt);
        }
        // change the status of first memtable as mutable
        mt_db.memtables[mt_db.mut_memtable_idx].status = MemTableStatus::MUTABLE;
        mt_db
    }

    async fn upsert_or_delete(
        &mut self,
        key: bytes::Bytes,
        epoch: u64,
        deleted: bool,
        val: Option<bytes::Bytes>) -> Result<(), Error> {

            // sanity checks
            assert!(self.mut_memtable_idx >= 0 && self.mut_memtable_idx < self.cache_options.num_write_buffers, 
                "mut_memtable_idx is out of range ({})", self.mut_memtable_idx);
            
            let mut val_match = Bytes::new();
            let mut val_len = 0; 
            match val{
                Some(v) => {
                    val_len = v.len();
                    val_match = v;
                },
                None => assert!(deleted == true, "upsert api called without value argument"),
            }

            let entry_size: u64 = key.len() as u64 + (val_len as u64) + (mem::size_of::<u64>() as u64) + (mem::size_of::<bool>() as u64);
            let entry = MemTableEntry { 
                key, 
                val: val_match, 
                epoch, 
                deleted };
            
            assert!(self.memtables[self.mut_memtable_idx].status == MemTableStatus::MUTABLE, 
                "active memtable is not mutable ({}) ({:?})", self.mut_memtable_idx, self.memtables[self.mut_memtable_idx].status);

            // println!("Memtable size {} entry size {}", memtable_wg.size_in_bytes, entry_size);
            if self.memtables[self.mut_memtable_idx].size_in_bytes + entry_size > self.cache_options.write_buffer_size {
                let old_id= self.mut_memtable_idx;
                let new_id = (self.mut_memtable_idx+1) % self.cache_options.num_write_buffers;

                if self.memtables[new_id].status != MemTableStatus::FLUSHED {
                    println!("No free memtables {} {:?}", new_id, self.memtables[new_id].status);
                    return Err(Error::CacheIsFull);
                }

                self.mut_memtable_idx = new_id;
                self.memtables[new_id].status = MemTableStatus::MUTABLE;
                self.memtables[old_id].status = MemTableStatus::IMMUTABLE;
                self.memtables[new_id].oldest_epoch = epoch;
                println!("Switched memtable from {} to {}", old_id, new_id);

                // trigger garbage collection on flushed memtable
                match self.gc_callback {
                    Some(cb) => cb(self.memtables[self.mut_memtable_idx].latest_epoch),
                    None => println!("GC callback is not set"),
                }
            }
            self.memtables[self.mut_memtable_idx].entries.insert(entry);
            self.memtables[self.mut_memtable_idx].size_in_bytes += entry_size;
            self.memtables[self.mut_memtable_idx].latest_epoch = max(self.memtables[self.mut_memtable_idx].latest_epoch, epoch);
            self.latest_epoch = epoch;

            Ok(())
        }
}

impl Cache for MemTableDB {

    async fn upsert(
        &mut self,
        key: bytes::Bytes,
        val: bytes::Bytes,
        epoch: u64,
        ) -> Result<(), Error> {
            self.upsert_or_delete(key, epoch, false, Some(val)).await
    }

    async fn delete(
        &mut self, 
        key: bytes::Bytes,
        epoch: u64,
    ) -> Result<(), Error> {
        self.upsert_or_delete(key, epoch, true, None).await
    }

    async fn get(
        &self, 
        key: bytes::Bytes,
        epoch: Option<u64>,
    ) -> Result<(bytes::Bytes, u64), Error> {

        // sanity checks
        assert!(self.mut_memtable_idx >= 0 && self.mut_memtable_idx < self.cache_options.num_write_buffers, 
            "mut_memtable_idx is out of range ({})", self.mut_memtable_idx);
    
        let mut epoch_internal = 0;
        match epoch {
            Some(e) => {
                epoch_internal = min(e, self.latest_epoch);
            },
            None => {
                epoch_internal = self.latest_epoch;
            },
        }

        for i in 0..self.cache_options.num_write_buffers {
            let idx = (self.mut_memtable_idx + self.cache_options.num_write_buffers - i) % self.cache_options.num_write_buffers; // mut_id is usize, should not become negative
            println!("GET-DBG: key {:?} memtable_idx {}, epoch {}, oldest_epoch {}, latest_epoch {}", key, idx, epoch_internal, self.memtables[idx].oldest_epoch, self.memtables[idx].latest_epoch);
            if self.memtables[idx].status == MemTableStatus::FLUSHED {
                break
            }
            if self.memtables[idx].oldest_epoch <= epoch_internal && epoch_internal <= self.memtables[idx].latest_epoch {
                let start = MemTableEntry{
                    key: key.clone(),
                    val: Vec::new().into(),
                    epoch: epoch_internal,
                    deleted: false};
                let mut end = start.clone();
                end.epoch = 0;

                let result = self.memtables[idx].entries.range(Included(&start), Included(&end)).next();

                match result {
                    Some(entry) => { 
                        println!("Found the entry {:?}", entry);
                        if entry.deleted {
                            return Err(Error::KeyNotFound);
                        } else {
                            return Ok((entry.val.clone(), entry.epoch));
                        }
                    }
                    None => {
                        println!("Entry not found in memtable {}", idx);
                        // continue checking other memtables
                    }
                }
            }
        }
        Err(Error::KeyNotFound)
    }

    // async fn get_range() -> Result<LoadedState, Error> {
        
    // }

    // async fn get_iterator() -> Result<LoadedState, Error> {
        
    // }

    async fn clear(
        &mut self,
        epoch: u64,
    ) -> Result<(), Error> {

        // sanity checks
        assert!(self.mut_memtable_idx >= 0 && self.mut_memtable_idx < self.cache_options.num_write_buffers, 
            "mut_memtable_idx is out of range ({})", self.mut_memtable_idx);
        
        for i in 1..self.cache_options.num_write_buffers {
            let id = (self.mut_memtable_idx + self.cache_options.num_write_buffers - i) % self.cache_options.num_write_buffers; // mut_id is usize, should not become negative
            if self.memtables[id].status == MemTableStatus::IMMUTABLE && epoch >= self.memtables[id].latest_epoch {
                self.memtables[id].status = MemTableStatus::FLUSHED;
                self.memtables[id].size_in_bytes = 0;
                self.memtables[id].entries.clear();
                println!("Changing memtable {} state from IMMUTABLE to {:?}", id, self.memtables[id].status);
            }
        }
        Ok(())
    }

    async fn register_gc_callback(
        &mut self,
        cb: GCCallback,
    ) -> Result<(), Error>{
        self.gc_callback = Some(cb);
        println!("registered gc callback");
        Ok(())
    }
}


pub mod for_testing {
    use super::*;

    pub const KV_SIZE: u64 = 30;
    pub const MEMTABLE_SIZE: u64 = 2<<9;

    static cache_options: CacheOptions = CacheOptions{
        path: "",
        num_write_buffers: 2, 
        write_buffer_size: MEMTABLE_SIZE,
    };

    impl MemTableDB {
        async fn create_test() -> Arc<RwLock<MemTableDB>> {
            Arc::new(RwLock::new(MemTableDB::new(Some(&cache_options)).await))
        }
    }
    pub struct TestContext {
        pub mt_db: Arc<RwLock<MemTableDB>>,
    }

    pub async fn init() -> TestContext {
        let mt_db = MemTableDB::create_test().await;

        TestContext {
            mt_db: mt_db
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use for_testing::*;

    async fn fill(mt_db: &Arc<RwLock<MemTableDB>>, num_keys: u64, clear: bool) {
        
        let mut data = vec![0u8; 10];
        for i in 0..num_keys {
            let key = Bytes::from(format!("k{}", i));
            data[0] = i as u8;
            let value = Bytes::from(data.clone());
            let epoch = i;
            println!("FILL: insert key {:?} value_size {:?} epoch {:?}", key, value.len(), epoch);
            {
                // let mut mt_db_wg = mt_db.write().await;
                match mt_db.write().await.upsert(key.clone(), value.clone(), epoch).await {
                    Err(e) => assert!(false, "insert result not ok! {}", e),
                    Ok(_) => {},
                }
            }
            {
                println!("FILL: get key {:?} epoch {:?}", key, epoch);
                match mt_db.read().await.get(key, None).await {
                    Err(e) => assert!(false, "get after insert returned error! {}", e),
                    Ok((val, ep)) => {
                        assert!(*val == value, "get after insert returned wrong value! expected {:?} got {:?}", value, val);
                        assert!(ep == epoch, "get after insert returned wrong epoch! expected {:?} got {:?}", epoch, ep);
                    }
                }
            }

            // trigger a gc cleanup periodically 
            if clear && i%10 == 0 {
                // let mut mt_db_wg = mt_db.write().await;
                match mt_db.write().await.clear(epoch).await {
                    Ok(_) => println!("Clear() succeeded"),
                    Err(e) => assert!(false, "Clear() failed {}", e),
                }
            }
        }
    }

    #[tokio::test]
    async fn insert_get() {
        let context = init().await;
        let mut mt_db = context.mt_db.clone();
        
        let mut num_keys: u64 = 0;
        num_keys = mt_db.read().await.cache_options.write_buffer_size/for_testing::KV_SIZE - 1;
        fill(&mt_db, num_keys, false).await;
        println!("Fill complete >>>>>>!");

        // Using a closure as a callback
        let gc_callback = |epoch: u64| {
            println!("GC callback was called for epoch: {}", epoch);
        };
        let _ = mt_db.write().await.register_gc_callback(gc_callback);

        let mut epoch = 0;
        let mut data = vec![0u8; 10];
        for i in 0..num_keys {
            let key = Bytes::from(format!("k{}", i));
            data[0] = i as u8;
            let value = Bytes::from(data.clone());
            epoch = i;

            match mt_db.read().await.get(key, Some(epoch)).await {
                Err(e) => assert!(false, "get after insert returned error! {}", e),
                Ok((val, ep)) => {
                    assert!(val == value, "get after insert returned wrong value! expected {:?} got {:?}", value, val);
                    assert!(ep == epoch, "get after insert returned wrong epoch! expected {:?} got {:?}", epoch, ep);
                }
            }
        }
    }

    #[tokio::test]
    async fn update_get() {
        let context = init().await;
        let mut mt_db = context.mt_db.clone();
        
        let num_keys = mt_db.read().await.cache_options.write_buffer_size/for_testing::KV_SIZE - 1;
        fill(&mt_db, num_keys, false).await;
        println!("Fill complete >>>>>>!");

        // Using a closure as a callback
        let gc_callback = |epoch: u64| {
            println!("GC callback was called for epoch: {}", epoch);
        };
        let _ = mt_db.write().await.register_gc_callback(gc_callback);

        let mut data = vec![0u8; 10];

        // modify a key
        let key = Bytes::from("k1");
        data[0] = 127 as u8;
        let epoch = num_keys;
        let value = Bytes::from(data.clone());
        
        match mt_db.write().await.upsert(key.clone(), value.clone(), epoch).await {
            Err(e) => assert!(false, "insert returned error! {}", e),
            Ok(_) => {}
        }

        // read older epoch
        match mt_db.read().await.get(key.clone(), Some(epoch-1)).await {
            Err(e) => assert!(false, "get after insert returned error! {}", e),
            Ok((val, ep)) => {
                assert!(val != value, "get after insert returned wrong value! expected {:?} got {:?}", value, val);
                assert!(ep == 1, "get after insert returned wrong epoch! expected {:?} got {:?}", epoch, ep);
            }
        }

        // read latest
        match mt_db.read().await.get(key.clone(), None).await {
            Err(e) => assert!(false, "get after insert returned error! {}", e),
            Ok((val, ep)) => {
                assert!(val == value, "get after insert returned wrong value! expected {:?} got {:?}", value, val);
                assert!(ep == epoch, "get after insert returned wrong epoch! expected {:?} got {:?}", epoch, ep);
            }
        };

        // read future
        match mt_db.read().await.get(key.clone(), Some(epoch+1)).await {
            Err(e) => assert!(false, "get after insert returned error! {}", e),
            Ok((val, ep)) => {
                assert!(val == value, "get after insert returned wrong value! expected {:?} got {:?}", value, val);
                assert!(ep == epoch, "get after insert returned wrong epoch! expected {:?} got {:?}", epoch, ep);
            }
        };
    }

    #[tokio::test]
    async fn delete() {
        let context = init().await;
        let mut mt_db = context.mt_db.clone();
        
        let num_keys = mt_db.read().await.cache_options.write_buffer_size/for_testing::KV_SIZE - 1;
        fill(&mt_db, num_keys, false).await;
        println!("Fill complete >>>>>>!");

        // Using a closure as a callback
        let gc_callback = |epoch: u64| {
            println!("GC callback was called for epoch: {}", epoch);
        };
        let _ = mt_db.write().await.register_gc_callback(gc_callback);

        let mut data = vec![0u8; 10];

        // modify a key
        let key = Bytes::from("k1");
        data[0] = 1 as u8;
        let value = Bytes::from(data.clone());
        let epoch = num_keys;
        
        match mt_db.write().await.delete(key.clone(), epoch).await {
            Err(e) => assert!(false, "delete returned error! {}", e),
            Ok(_) => {}
        }

        // // read older epoch
        match mt_db.read().await.get(key.clone(), Some(epoch-1)).await {
            Err(e) => assert!(false, "get after insert returned error! {}", e),
            Ok((val, ep)) => {
                assert!(val == value, "get after insert returned wrong value! expected {:?} got {:?}", value, val);
                assert!(ep == 1, "get after insert returned wrong epoch! expected {:?} got {:?}", epoch, ep);
            }
        }

        // read latest
        match mt_db.read().await.get(key.clone(), None).await {
            Err(e) => {},
            Ok(_) => assert!(false, "get after delete returned a value!"),
        };
    }

    fn gc_callback(epoch: u64) {
        println!("GC callback was called for epoch: {}", epoch);
    }

    #[tokio::test]
    async fn clear() {
        let context = init().await;
        let mut mt_db = context.mt_db.clone();

        let _ = mt_db.write().await.register_gc_callback(gc_callback);
        
        // fill two memtables
        let num_keys = 100*(mt_db.read().await.cache_options.write_buffer_size/for_testing::KV_SIZE - 1);
        fill(&mt_db, num_keys, true).await;
        println!("Fill complete >>>>>>!");

        match mt_db.read().await.get(Bytes::from(format!("k{}", 0)), None).await {
            Err(e) => assert!(true, "key should not be found! {}", e),
            Ok((val, ep)) => assert!(false, "should not return any value! {:?} {}", val, ep),
        };
    }

    #[tokio::test]
    async fn concurrent() {
        let context = init().await;
        let mut mt_db = context.mt_db.clone();

        // bump up the memtable size
        mt_db.write().await.cache_options.write_buffer_size = 2<<21;

        #[derive(Clone, Debug)]
        struct ThreadData {
            // mt_db: MemTableDB,
            num_keys: usize,
            num_epochs: usize,
            values: HashMap<Bytes, Vec<Bytes>>,
        }

        let mut td = ThreadData{
            // mt_db: mt_db,
            num_keys: 10, 
            num_epochs: 40000, 
            values: HashMap::new(),};
        
        for k in 0..td.num_keys {
            let key = Bytes::from(format!("k{}", k));
            let mut epoch: Vec<Bytes> = Vec::new();
            for e in 0..td.num_epochs {
                let val = Bytes::from(format!("k{}e{}", k, e));
                epoch.push(val);
            }
            td.values.insert(key, epoch);
        }

        let global_epoch = Arc::new(Mutex::new(0));

        // fill the 0th epoch values
        for i in 0..td.num_keys {
            // let mut key_str = format!("k{}", i);
            let key = Bytes::from(format!("k{}", i));
            let epoch = *(global_epoch.lock().unwrap());
            match td.values.get(&key) {
                None => {},
                Some(v) => {
                    match mt_db.write().await.upsert(key.clone(), v[epoch].clone(), epoch as u64).await {
                        Err(e) => assert!(false, "insert result not ok! {}", e),
                        Ok(_) => {},
                    }
                    // println!("Key {:?}, value {:?}", key, v)
                },
            }
        }
        *global_epoch.lock().unwrap() += 1;
        
        let mut handles = Vec::new();
        let arc_td = Arc::new(td);
        let wr_td = Arc::clone(&arc_td);
        // let arc_mt_db = Arc::new(RwLock::new(mt_db));
        let mt_db_wr = Arc::clone(&mt_db);
        let global_epoch_wr = Arc::clone(&global_epoch);
        
        // spawn a single writer thread
        
        let wr_handle = tokio::spawn(async move {
            let mut curr_epoch = *global_epoch_wr.lock().unwrap();
            for ep in curr_epoch..wr_td.num_epochs {
                for k in 0..wr_td.num_keys {
                    // println!("key {}, epoch {}", k, ep);
                    let key = Bytes::from(format!("k{}", k));
                    let v = wr_td.values.get(&key).unwrap();
                    // let mut mt_db_wg = mt_db_wr.write().unwrap();
                    // let mut mt_db_wg = mt_db_wr.write();
                    match mt_db_wr.write().await.upsert(key.clone(), v[ep].clone(), ep as u64).await {
                        Err(e) => assert!(false, "insert result not ok! {}", e),
                        Ok(_) => {},
                    }
                    // drop(mt_db_wg);
                }
                *global_epoch_wr.lock().unwrap() += 1;
            }
            // println!("INSERT DONE");
        });

        handles.push(wr_handle);

        // spawn 10 concurrent readers
        for _ in 0..10 {
            let rd_td = Arc::clone(&arc_td);
            let mt_db_rd = Arc::clone(&mt_db);
            let global_epoch_rd = Arc::clone(&global_epoch);

            let rd_handle = tokio::spawn(async move {
                let mut rng = StdRng::from_entropy();
                for i in 0..10000 {
                    let rand_epoch = rng.gen_range(0..*global_epoch_rd.lock().unwrap()) as u64;
                    let rand_key = rng.gen_range(0..rd_td.num_keys);
                    let key = Bytes::from(format!("k{}", rand_key));
                    println!("key {:?}, epoch {}", key, rand_epoch);

                    // let mt_db_rg = mt_db_rd.read().unwrap();
                    // let mt_db_rg = mt_db_rd.read();
                    match mt_db_rd.read().await.get(key.clone(), Some(rand_epoch)).await {
                        Err(e) => assert!(false, "get returned error! {}", e),
                        Ok((val, ep)) => {
                            match rd_td.values.get(&key) {
                                None => {},
                                Some(v) => {
                                    println!("Get result requested epoch {}, get_epoch {}", rand_epoch, ep);
                                    assert!(*val == v[ep as usize], "get returned wrong value! expected {:?} got {:?}", v[ep as usize], val);
                                    assert!(ep <= rand_epoch, "get returned wrong epoch! expected {:?} got {:?}", rand_epoch, ep);
                                }
                            }
                        }
                    }
                    // drop(mt_db_rg)
                }
            });
            handles.push(rd_handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }
}

