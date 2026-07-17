use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::watch;

#[derive(Clone)]
pub struct ConsolePublisher {
    version: Arc<AtomicU64>,
    tx: watch::Sender<u64>,
    pending: Arc<Mutex<PendingInvalidations>>,
}

#[derive(Debug, Default)]
struct PendingInvalidations {
    full: bool,
    branches: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConsoleInvalidationBatch {
    pub version: u64,
    pub full: bool,
    pub branches: BTreeSet<String>,
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
            pending: Arc::new(Mutex::new(PendingInvalidations::default())),
        }
    }

    pub fn current_version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    pub fn mark_changed(&self) -> u64 {
        self.pending
            .lock()
            .expect("console invalidation lock poisoned")
            .full = true;
        self.publish_change()
    }

    pub(crate) fn mark_full_at_least(&self, target: u64) -> u64 {
        self.pending
            .lock()
            .expect("console invalidation lock poisoned")
            .full = true;
        self.advance_to(target)
    }

    pub(crate) fn mark_branch_changed(&self, branch: impl Into<String>) -> u64 {
        self.pending
            .lock()
            .expect("console invalidation lock poisoned")
            .branches
            .insert(branch.into());
        self.publish_change()
    }

    pub(crate) fn mark_branches_changed(&self, branches: impl IntoIterator<Item = String>) -> u64 {
        self.pending
            .lock()
            .expect("console invalidation lock poisoned")
            .branches
            .extend(branches);
        self.publish_change()
    }

    fn publish_change(&self) -> u64 {
        let version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        self.tx.send_replace(version);
        version
    }

    pub(crate) fn take_invalidations(&self) -> ConsoleInvalidationBatch {
        let mut pending = self
            .pending
            .lock()
            .expect("console invalidation lock poisoned");
        ConsoleInvalidationBatch {
            version: self.current_version(),
            full: std::mem::take(&mut pending.full),
            branches: std::mem::take(&mut pending.branches),
        }
    }

    pub(crate) fn restore_invalidations(&self, batch: ConsoleInvalidationBatch) {
        let mut pending = self
            .pending
            .lock()
            .expect("console invalidation lock poisoned");
        pending.full |= batch.full;
        pending.branches.extend(batch.branches);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_invalidations_are_coalesced_and_restorable() {
        let publisher = ConsolePublisher::new();

        publisher.mark_branch_changed("main");
        publisher.mark_branch_changed("main");
        publisher.mark_branch_changed("worker");
        let batch = publisher.take_invalidations();

        assert_eq!(batch.version, 3);
        assert!(!batch.full);
        assert_eq!(
            batch.branches,
            BTreeSet::from(["main".to_owned(), "worker".to_owned()])
        );
        publisher.restore_invalidations(batch.clone());
        assert_eq!(publisher.take_invalidations(), batch);
    }

    #[test]
    fn full_invalidation_preserves_dirty_branches() {
        let publisher = ConsolePublisher::new();

        publisher.mark_branch_changed("main");
        publisher.mark_changed();
        let batch = publisher.take_invalidations();

        assert_eq!(batch.version, 2);
        assert!(batch.full);
        assert_eq!(batch.branches, BTreeSet::from(["main".to_owned()]));
    }

    #[test]
    fn full_invalidation_can_advance_without_incrementing_an_existing_version() {
        let publisher = ConsolePublisher::new();
        publisher.mark_branch_changed("main");

        assert_eq!(publisher.mark_full_at_least(1), 1);
        let batch = publisher.take_invalidations();
        assert!(batch.full);
        assert_eq!(batch.branches, BTreeSet::from(["main".to_owned()]));

        assert_eq!(publisher.mark_full_at_least(4), 4);
        assert_eq!(publisher.current_version(), 4);
        assert!(publisher.take_invalidations().full);
    }
}
