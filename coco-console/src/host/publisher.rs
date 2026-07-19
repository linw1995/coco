use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{broadcast, watch};

const NODE_CREATION_CHANNEL_CAPACITY: usize = 1_024;

#[derive(Clone)]
pub struct ConsolePublisher {
    version: Arc<AtomicU64>,
    tx: watch::Sender<u64>,
    node_tx: broadcast::Sender<ConsoleNodeCreated>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConsoleNodeCreated {
    pub source_version: u64,
    pub node_id: String,
}

impl Default for ConsolePublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsolePublisher {
    pub fn new() -> Self {
        let (tx, _) = watch::channel(0);
        let (node_tx, _) = broadcast::channel(NODE_CREATION_CHANNEL_CAPACITY);
        Self {
            version: Arc::new(AtomicU64::new(0)),
            tx,
            node_tx,
            pending: Arc::new(Mutex::new(PendingInvalidations::default())),
        }
    }

    pub fn current_version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    pub fn mark_changed(&self) -> u64 {
        self.update_and_publish(
            |pending| pending.full = true,
            |publisher| publisher.publish_change(),
        )
    }

    pub(crate) fn mark_full_at_least(&self, target: u64) -> u64 {
        self.update_and_publish(
            |pending| pending.full = true,
            |publisher| publisher.advance_to_unlocked(target),
        )
    }

    pub(crate) fn mark_branch_changed(&self, branch: impl Into<String>) -> u64 {
        self.update_and_publish(
            |pending| {
                pending.branches.insert(branch.into());
            },
            |publisher| publisher.publish_change(),
        )
    }

    pub(crate) fn mark_branches_changed(&self, branches: impl IntoIterator<Item = String>) -> u64 {
        self.update_and_publish(
            |pending| pending.branches.extend(branches),
            |publisher| publisher.publish_change(),
        )
    }

    pub(crate) fn mark_node_created(&self, node_id: impl Into<String>) -> u64 {
        self.mark_changed_with_node(|_| {}, node_id)
    }

    pub(crate) fn mark_branch_and_node_changed(
        &self,
        branch: impl Into<String>,
        node_id: impl Into<String>,
    ) -> u64 {
        self.mark_changed_with_node(
            |pending| {
                pending.branches.insert(branch.into());
            },
            node_id,
        )
    }

    pub(crate) fn mark_branches_and_node_changed(
        &self,
        branches: impl IntoIterator<Item = String>,
        node_id: impl Into<String>,
    ) -> u64 {
        self.mark_changed_with_node(|pending| pending.branches.extend(branches), node_id)
    }

    fn mark_changed_with_node(
        &self,
        update: impl FnOnce(&mut PendingInvalidations),
        node_id: impl Into<String>,
    ) -> u64 {
        let version = self.update_and_publish(update, |publisher| publisher.publish_change());
        let _ = self.node_tx.send(ConsoleNodeCreated {
            source_version: version,
            node_id: node_id.into(),
        });
        version
    }

    fn update_and_publish(
        &self,
        update: impl FnOnce(&mut PendingInvalidations),
        publish: impl FnOnce(&Self) -> u64,
    ) -> u64 {
        self.update_and_publish_with_hook(update, || {}, publish)
    }

    fn update_and_publish_with_hook(
        &self,
        update: impl FnOnce(&mut PendingInvalidations),
        before_publish: impl FnOnce(),
        publish: impl FnOnce(&Self) -> u64,
    ) -> u64 {
        let mut pending = self
            .pending
            .lock()
            .expect("console invalidation lock poisoned");
        update(&mut pending);
        before_publish();
        let version = publish(self);
        drop(pending);
        version
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
        let pending = self
            .pending
            .lock()
            .expect("console invalidation lock poisoned");
        let version = self.advance_to_unlocked(target);
        drop(pending);
        version
    }

    fn advance_to_unlocked(&self, target: u64) -> u64 {
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

    pub(crate) fn subscribe_node_creations(&self) -> broadcast::Receiver<ConsoleNodeCreated> {
        self.node_tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::sync::TryLockError;

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

    #[test]
    fn invalidation_cannot_be_taken_before_its_version_is_published() {
        let publisher = ConsolePublisher::new();
        let dirty = Arc::new(Barrier::new(2));
        let allow_publish = Arc::new(Barrier::new(2));
        let marker = {
            let publisher = publisher.clone();
            let dirty = dirty.clone();
            let allow_publish = allow_publish.clone();
            std::thread::spawn(move || {
                publisher.update_and_publish_with_hook(
                    |pending| {
                        pending.branches.insert("main".to_owned());
                    },
                    || {
                        dirty.wait();
                        allow_publish.wait();
                    },
                    |publisher| publisher.publish_change(),
                )
            })
        };
        dirty.wait();
        assert_eq!(publisher.current_version(), 0);
        assert!(matches!(
            publisher.pending.try_lock(),
            Err(TryLockError::WouldBlock)
        ));

        allow_publish.wait();
        assert_eq!(marker.join().unwrap(), 1);
        let batch = publisher.take_invalidations();

        assert_eq!(batch.version, 1);
        assert!(!batch.full);
        assert_eq!(batch.branches, BTreeSet::from(["main".to_owned()]));
    }

    #[tokio::test]
    async fn node_creation_uses_the_same_version_as_its_invalidation() {
        let publisher = ConsolePublisher::new();
        let mut nodes = publisher.subscribe_node_creations();

        let version = publisher.mark_branch_and_node_changed("main", "node-1");

        assert_eq!(version, 1);
        assert_eq!(publisher.current_version(), 1);
        assert_eq!(
            nodes.recv().await.unwrap(),
            ConsoleNodeCreated {
                source_version: 1,
                node_id: "node-1".to_owned(),
            }
        );
        assert_eq!(
            publisher.take_invalidations().branches,
            BTreeSet::from(["main".to_owned()])
        );
    }
}
