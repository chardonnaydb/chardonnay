use skiplist::OrderedSkipList;
use std::cmp::Ordering;
// use std::sync::RwLock;
// use parking_lot::RwLock;
use tokio::sync::RwLock;
use std::mem;
use bytes::Bytes;
use std::sync::Arc;
use crate::cache::{Cache, Error, GCCallback, RCOptions};
use std::collections::HashMap;
use std::vec::Vec;
use std::thread;
use std::sync::Mutex;
// use rand::Rng;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::cmp::min;

// memtable status changes from FLUSHED->MUTABLE->IMMUTABLE->FLUSHED
#[derive(Debug, PartialEq)]
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

#[derive(Debug, Clone)]
pub struct MemTableDB {
    rc_options: RCOptions,
    memtables: Vec<Arc<RwLock<MemTable>>>,
    mutable_id: usize,
    latest_epoch: u64,
    gc_callback: Option<GCCallback>,
}

impl MemTableDB {
    pub async fn new(rc_options: Option<&RCOptions>) -> Self {

        let mut opts = RCOptions{
            path: "",
            num_write_buffers: 2,
            write_buffer_size: 2<<29,
         };

        match rc_options {
            Some(opt) => {opts = *opt},
            None => {},
        }

        let mut mt_db = MemTableDB {
            rc_options: opts.clone(),
            memtables: Vec::<Arc<RwLock<MemTable>>>::new(), 
            mutable_id: 0,
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
            mt_db.memtables.push(Arc::new(RwLock::new(mt)));
        }
        // change the status of first memtable as mutable
        // let mut memtable_wg = mt_db.memtables[mt_db.mutable_id].write().unwrap();
        let mut memtable_wg = mt_db.memtables[mt_db.mutable_id].write().await;
        memtable_wg.status = MemTableStatus::MUTABLE;
        drop(memtable_wg);
        mt_db
    }

    async fn upsert_or_delete(
        &mut self,
        key: bytes::Bytes,
        epoch: u64,
        deleted: bool,
        val: Option<bytes::Bytes>) -> Result<(), Error> {

            // sanity checks
            assert!(self.mutable_id >= 0 && self.mutable_id < self.rc_options.num_write_buffers, 
                "mutable_id is out of range ({})", self.mutable_id);
            
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
            let entry = MemTableEntry { key: key, val: val_match, epoch: epoch, deleted: deleted };
            
            // let mut memtable_wg = self.memtables[self.mutable_id].write().unwrap();
            let mut memtable_wg = self.memtables[self.mutable_id].write().await;
            assert!(memtable_wg.status == MemTableStatus::MUTABLE, 
                "active memtable is not mutable ({}) ({:?})", self.mutable_id, memtable_wg.status);

            // println!("Memtable size {} entry size {}", memtable_wg.size_in_bytes, entry_size);
            let mut flush_id: i32 = -1;
            if memtable_wg.size_in_bytes + entry_size <= self.rc_options.write_buffer_size {
                memtable_wg.entries.insert(entry);
                memtable_wg.size_in_bytes += entry_size;
                memtable_wg.latest_epoch = epoch;
                self.latest_epoch = epoch;
                drop(memtable_wg);
            } else {
                flush_id = self.mutable_id as i32;
                let new_id = (self.mutable_id+1) % self.rc_options.num_write_buffers;

                // let mut new_memtable_wg = self.memtables[new_id].write().unwrap();
                let mut new_memtable_wg = self.memtables[new_id].write().await;
                assert!(new_memtable_wg.status == MemTableStatus::FLUSHED, 
                    "no free memtables {} {:?}", new_id, new_memtable_wg.status);
                // Err(Error::OutOfSpace)

                // TODO: these two operations need to be atomic
                self.mutable_id = (self.mutable_id+1) % self.rc_options.num_write_buffers;
                new_memtable_wg.status = MemTableStatus::MUTABLE;
                memtable_wg.status = MemTableStatus::IMMUTABLE;

                // trigger garbage collection on flushed memtable
                match self.gc_callback {
                    Some(cb) => cb(memtable_wg.latest_epoch),
                    None => println!("INFO: GC callback is not set {:?}", self.gc_callback),
                }
                drop(memtable_wg);

                new_memtable_wg.entries.insert(entry);
                new_memtable_wg.size_in_bytes += entry_size;
                new_memtable_wg.latest_epoch = epoch;
                new_memtable_wg.oldest_epoch = epoch;
                self.latest_epoch = epoch;
                drop(new_memtable_wg);

                println!("Switched memtable from {} to {}", flush_id, new_id);
            }
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

            // sanity checks
            assert!(self.mutable_id >= 0 && self.mutable_id < self.rc_options.num_write_buffers, 
                "mutable_id is out of range ({})", self.mutable_id);
                        
            self.upsert_or_delete(key, epoch, false, Some(val)).await
    }

    async fn delete(
        &mut self, 
        key: bytes::Bytes,
        epoch: u64,
    ) -> Result<(), Error> {

        // sanity checks
        assert!(self.mutable_id >= 0 && self.mutable_id < self.rc_options.num_write_buffers, 
            "mutable_id is out of range ({})", self.mutable_id);
        
        self.upsert_or_delete(key, epoch, true, None).await
    }

    async fn get(
        &self, 
        key: &bytes::Bytes,
        epoch: Option<u64>,
    ) -> Result<(bytes::Bytes, u64), Error> {

        // sanity checks
        assert!(self.mutable_id >= 0 && self.mutable_id < self.rc_options.num_write_buffers, 
            "mutable_id is out of range ({})", self.mutable_id);
    
        let mut epoch_internal = 0;
        match epoch {
            Some(e) => {
                epoch_internal = min(e, self.latest_epoch);
            },
            None => {
                epoch_internal = self.latest_epoch;
            },
        }
        
        let mut_id = self.mutable_id; // take a snapshot of mutable_id, may change

        for i in 0..self.rc_options.num_write_buffers {
            let id = (mut_id + self.rc_options.num_write_buffers - i) % self.rc_options.num_write_buffers; // mut_id is usize, should not become negative
            // let memtable_rg = self.memtables[id].read().unwrap();
            let memtable_rg = self.memtables[id].read().await;
            println!("GET-DBG: id {}, epoch {}, oldest_epoch {}, latest_epoch {}", id, epoch_internal, memtable_rg.oldest_epoch, memtable_rg.latest_epoch);
            if memtable_rg.status == MemTableStatus::FLUSHED {
                break
            }
            if memtable_rg.oldest_epoch <= epoch_internal && epoch_internal <= memtable_rg.latest_epoch {                
                match memtable_rg.entries.iter().find(|&&ref entry| entry.key == key && entry.epoch <= epoch_internal)  {
                    Some(entry) => { 
                        println!("Found the entry {:?}", entry);
                        if entry.deleted {
                            return Err(Error::KeyNotFound);
                        } else {
                            return Ok((entry.val.clone(), entry.epoch));
                        }
                    }
                    None => {
                        println!("Entry not found in memtable {}", id);
                        return Err(Error::KeyNotFound);
                    }
                }
            }
            drop(memtable_rg);
        }
        Err(Error::KeyNotFound)
    }

    // async fn get_range() -> Result<LoadedState, Error> {
        
    // }

    // async fn get_iterator() -> Result<LoadedState, Error> {
        
    // }

    async fn clear(
        &mut self,
        epoch: Option<u64>,
    ) -> Result<(), Error> {

        // sanity checks
        assert!(self.mutable_id >= 0 && self.mutable_id < self.rc_options.num_write_buffers, 
            "mutable_id is out of range ({})", self.mutable_id);
        
        let mut_id = self.mutable_id; // take a snapshot of mutable_id, may change

        match epoch {
            Some(e) => {
                for i in 1..self.rc_options.num_write_buffers {
                    let id = (mut_id + self.rc_options.num_write_buffers - i) % self.rc_options.num_write_buffers; // mut_id is usize, should not become negative
                    // let memtable_rg = self.memtables[id].write().unwrap();
                    let memtable_rg = self.memtables[id].write().await;
                    if memtable_rg.status == MemTableStatus::IMMUTABLE && e >= memtable_rg.latest_epoch {
                        drop(memtable_rg);
                        // let mut memtable_wg = self.memtables[id].write().unwrap();
                        let mut memtable_wg = self.memtables[id].write().await;
                        memtable_wg.status = MemTableStatus::FLUSHED;
                        memtable_wg.size_in_bytes = 0;
                        memtable_wg.entries.clear();
                        println!("Changing memtable {} state from IMMUTABLE to {:?}", id, memtable_wg.status);
                        drop(memtable_wg);
                    }
                    // drop(memtable_rg);
                }
                Ok(())
            },
            None => panic!("epoch value not provided!"),
        }

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

    static rc_options: RCOptions = RCOptions{
        path: "",
        num_write_buffers: 2, 
        write_buffer_size: MEMTABLE_SIZE,
    };
    // static mut gc_epoch: u64 = 0;

    impl MemTableDB {
        async fn create_test() -> Arc<RwLock<MemTableDB>> {
            Arc::new(RwLock::new(MemTableDB::new(Some(&rc_options)).await))
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

    // const KV_SIZE: u64 = 30;
    // const MEMTABLE_SIZE: u64 = 2<<9;

    // static rc_options: RCOptions = RCOptions{
    //     path: "",
    //     num_write_buffers: 2, 
    //     write_buffer_size: MEMTABLE_SIZE,
    // };
    // // static mut gc_epoch: u64 = 0;

    // impl MemTableDB {
    //     async fn create_test() -> Arc<RwLock<MemTableDB>> {
    //         Arc::new(RwLock::new(MemTableDB::new(Some(&rc_options)).await))
    //     }
    // }

    // pub struct TestContext {
    //     pub mt_db: Arc<RwLock<MemTableDB>>,
    // }

    // pub async fn init() -> TestContext {
    //     let mt_db = MemTableDB::create_test().await;

    //     TestContext {
    //         mt_db: mt_db
    //     }
    // }

    async fn fill(mt_db: &Arc<RwLock<MemTableDB>>, num_keys: u64, clear: bool) {
        
        let mut data = vec![0u8; 10];
        for i in 0..num_keys {
            // let mut key_str = format!("k{}", i);
            let key = Bytes::from(format!("k{}", i));
            data[0] = i as u8;
            let value = Bytes::from(data.clone());
            let epoch = i;
            println!("insert key {:?} value_size {:?} epoch {:?}", key, value.len(), epoch);
            {
                let mut mt_db_wg = mt_db.write().await;
                match mt_db_wg.upsert(key.clone(), value.clone(), epoch).await {
                    Err(e) => assert!(false, "insert result not ok! {}", e),
                    Ok(_) => {},
                }
            }
            {
                println!("get key {:?} epoch {:?}", key, epoch);
                let mut mt_db_rg = mt_db.read().await;
                match mt_db_rg.get(&key, None).await {
                    Err(e) => assert!(false, "get after insert returned error! {}", e),
                    Ok((val, ep)) => {
                        assert!(*val == value, "get after insert returned wrong value! expected {:?} got {:?}", value, val);
                        assert!(ep == epoch, "get after insert returned wrong epoch! expected {:?} got {:?}", epoch, ep);
                    }
                }
            }

            // trigger a gc cleanup periodically 
            if clear && i%10 == 0 {
                let mut mt_db_wg = mt_db.write().await;
                match mt_db_wg.clear(Some(epoch)).await {
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
        num_keys = mt_db.read().await.rc_options.write_buffer_size/for_testing::KV_SIZE - 1;
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

            match mt_db.read().await.get(&key, Some(epoch)).await {
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
        
        let num_keys = mt_db.read().await.rc_options.write_buffer_size/for_testing::KV_SIZE - 1;
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
        let epoch = num_keys + 1;
        let value = Bytes::from(data.clone());
        
        match mt_db.write().await.upsert(key.clone(), value.clone(), epoch).await {
            Err(e) => assert!(false, "insert returned error! {}", e),
            Ok(_) => {}
        }

        // read older epoch
        match mt_db.read().await.get(&key, Some(epoch-1)).await {
            Err(e) => assert!(false, "get after insert returned error! {}", e),
            Ok((val, ep)) => {
                assert!(val != value, "get after insert returned wrong value! expected {:?} got {:?}", value, val);
                assert!(ep == 1, "get after insert returned wrong epoch! expected {:?} got {:?}", epoch, ep);
            }
        }

        // read latest
        match mt_db.read().await.get(&key, None).await {
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
        
        let num_keys = mt_db.read().await.rc_options.write_buffer_size/for_testing::KV_SIZE - 1;
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
        let epoch = num_keys + 1;
        
        match mt_db.write().await.delete(key.clone(), epoch).await {
            Err(e) => assert!(false, "delete returned error! {}", e),
            Ok(_) => {}
        }

        // read older epoch
        match mt_db.read().await.get(&key, Some(epoch-1)).await {
            Err(e) => assert!(false, "get after insert returned error! {}", e),
            Ok((val, ep)) => {
                assert!(val == value, "get after insert returned wrong value! expected {:?} got {:?}", value, val);
                assert!(ep == 1, "get after insert returned wrong epoch! expected {:?} got {:?}", epoch, ep);
            }
        }

        // read latest
        match mt_db.read().await.get(&key, None).await {
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
        let num_keys = 100*(mt_db.read().await.rc_options.write_buffer_size/for_testing::KV_SIZE - 1);
        fill(&mt_db, num_keys, true).await;
        println!("Fill complete >>>>>>!");

        match mt_db.read().await.get(&Bytes::from(format!("k{}", 0)), None).await {
            Err(e) => assert!(true, "key should not be found! {}", e),
            Ok((val, ep)) => assert!(false, "should not return any value! {:?} {}", val, ep),
        };
    }

    #[tokio::test]
    async fn concurrent_insert_get() {
        let context = init().await;
        let mut mt_db = context.mt_db.clone();

        // bump up the memtable size
        mt_db.write().await.rc_options.write_buffer_size = 2<<21;

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
                    match mt_db_rd.read().await.get(&key, Some(rand_epoch)).await {
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

