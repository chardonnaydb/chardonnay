use super::Error;
use super::Storage;
use std::sync::atomic;

#[derive(Debug)]
pub struct InMemoryEpochStorage {
    // The current epoch, where 0 means that the epoch hasn't been initialized yet.
    epoch: atomic::AtomicU64,
}

impl InMemoryEpochStorage {
    pub fn new() -> InMemoryEpochStorage {
        InMemoryEpochStorage {
            epoch: atomic::AtomicU64::new(0),
        }
    }
}

impl Storage for InMemoryEpochStorage {
    async fn initialize_epoch(&self) -> Result<(), Error> {
        // Try to initialize the epoch. If it's already been initialized, the compare exchange will
        // fail, which is ok. Relaxed ordering is sufficient since there is no dependent data.
        self.epoch
            .compare_exchange(0, 1, atomic::Ordering::Relaxed, atomic::Ordering::Relaxed)
            .ok();
        Ok(())
    }

    async fn read_latest(&self) -> Result<u64, Error> {
        // Relaxed ordering is sufficient since there is no dependent data.
        let epoch = self.epoch.load(atomic::Ordering::Relaxed);
        if epoch == 0 {
            return Err(Error::EpochNotInitialized);
        }
        Ok(epoch)
    }

    async fn conditional_update(&self, new_epoch: u64, current_epoch: u64) -> Result<(), Error> {
        if new_epoch < current_epoch {
            return Err(Error::ConditionFailed);
        }
        // Relaxed ordering is sufficient since there is no dependent data.
        match self.epoch.compare_exchange(
            current_epoch,
            new_epoch,
            atomic::Ordering::Relaxed,
            atomic::Ordering::Relaxed,
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::ConditionFailed),
        }
    }
}
