use crate::epoch_supplier::EpochSupplier as Trait;
use async_trait::async_trait;
use epoch_publisher::error::Error;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::RwLock;
use tokio::sync::oneshot;

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

struct State {
    epoch: u64,
    waiters: BinaryHeap<EpochWaiter>,
}
pub struct EpochSupplier {
    state: RwLock<State>,
}

impl EpochSupplier {
    pub fn new() -> EpochSupplier {
        EpochSupplier {
            state: RwLock::new(State {
                epoch: 0,
                waiters: BinaryHeap::new(),
            }),
        }
    }
    pub async fn set_epoch(&self, epoch: u64) {
        let mut state = self.state.write().unwrap();
        state.epoch = epoch;
        while let Some(w) = state.waiters.peek() {
            if w.epoch > epoch {
                break;
            }
            let w = state.waiters.pop().unwrap();
            w.sender.send(()).unwrap();
        }
    }
}

#[async_trait]
impl Trait for EpochSupplier {
    async fn read_epoch(&self) -> Result<u64, Error> {
        let state = self.state.read().unwrap();
        Ok(state.epoch)
    }

    async fn wait_until_epoch(&self, epoch: u64, _timeout: chrono::Duration) -> Result<(), Error> {
        let (s, r) = oneshot::channel();
        {
            let mut state = self.state.write().unwrap();
            if state.epoch >= epoch {
                return Ok(());
            };

            state.waiters.push(EpochWaiter { epoch, sender: s });
        }
        r.await.unwrap();
        Ok(())
    }
}
