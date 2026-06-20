use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::watch;

#[derive(Clone)]
pub struct ConsolePublisher {
    version: Arc<AtomicU64>,
    tx: watch::Sender<u64>,
}

impl Default for ConsolePublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsolePublisher {
    pub fn new() -> Self {
        let (tx, _) = watch::channel(0);
        Self {
            version: Arc::new(AtomicU64::new(0)),
            tx,
        }
    }

    pub fn current_version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    pub fn mark_changed(&self) -> u64 {
        let version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        self.tx.send_replace(version);
        version
    }

    pub fn advance_to(&self, target: u64) -> u64 {
        let previous = self.version.fetch_max(target, Ordering::SeqCst);
        let version = previous.max(target);
        if previous < target {
            self.tx.send_replace(target);
        }
        version
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.tx.subscribe()
    }
}
