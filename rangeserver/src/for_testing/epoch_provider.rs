use crate::epoch_provider::EpochProvider as Trait;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use tokio::sync::oneshot;
use tokio::sync::Mutex;

struct EpochWaiter {
    epoch: u64,
    sender: oneshot::Sender<()>,
}

impl Ord for EpochWaiter {
    fn cmp(&self, other: &Self) -> Ordering {
        self.epoch.cmp(&other.epoch)
    }
}

impl PartialEq for EpochWaiter {
    fn eq(&self, other: &Self) -> bool {
        self.epoch == other.epoch
    }
}

impl Eq for EpochWaiter {}

impl PartialOrd for EpochWaiter {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct EpochProvider {
    epoch: u64,
    waiters: Mutex<BinaryHeap<EpochWaiter>>,
}

impl EpochProvider {
    pub async fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
        let mut waiters = self.waiters.lock().await;
        while let Some(w) = waiters.peek() {
            if w.epoch > epoch {
                break;
            }
            let w = waiters.pop().unwrap();
            w.sender.send(()).unwrap();
        }
    }
}

impl Trait for EpochProvider {
    async fn read_epoch(&self) -> Result<u64, crate::epoch_provider::Error> {
        Ok(self.epoch)
    }

    async fn wait_until_epoch(&self, epoch: u64) -> Result<(), crate::epoch_provider::Error> {
        if self.epoch >= epoch {
            return Ok(());
        };
        let (s, r) = oneshot::channel();
        let mut waiters = self.waiters.lock().await;
        waiters.push(EpochWaiter { epoch, sender: s });
        r.await.unwrap();
        Ok(())
    }
}
