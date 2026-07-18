use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use coco_mem::GraphMutationReceipt;
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConsoleInvalidationBatch {
    pub version: u64,
    pub full: bool,
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
        self.update_and_publish(
            |pending| pending.full = true,
            |publisher| publisher.publish_change(),
        )
    }

    pub(crate) fn notify_durable_change(&self) -> u64 {
        self.update_and_publish(|_| {}, |publisher| publisher.publish_change())
    }

    pub(crate) fn notify_durable_change_at_least(&self, target: u64) -> u64 {
        self.update_and_publish(|_| {}, |publisher| publisher.advance_to_unlocked(target))
    }

    #[cfg(test)]
    pub(crate) fn mark_full_at_least(&self, target: u64) -> u64 {
        self.update_and_publish(
            |pending| pending.full = true,
            |publisher| publisher.advance_to_unlocked(target),
        )
    }

    pub(crate) fn publish_graph_mutation<T>(&self, receipt: &GraphMutationReceipt<T>) -> u64 {
        self.update_and_publish(
            |pending| pending.full |= !receipt.exact,
            |publisher| publisher.publish_change(),
        )
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
        }
    }

    pub(crate) fn restore_invalidations(&self, batch: ConsoleInvalidationBatch) {
        let mut pending = self
            .pending
            .lock()
            .expect("console invalidation lock poisoned");
        pending.full |= batch.full;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::sync::TryLockError;

    #[test]
    fn durable_wakeups_have_constant_size_state() {
        let publisher = ConsolePublisher::new();

        publisher.notify_durable_change();
        publisher.notify_durable_change();
        publisher.notify_durable_change();
        let batch = publisher.take_invalidations();

        assert_eq!(batch.version, 3);
        assert!(!batch.full);
        publisher.restore_invalidations(batch.clone());
        assert_eq!(publisher.take_invalidations(), batch);
    }

    #[test]
    fn full_invalidation_is_restorable() {
        let publisher = ConsolePublisher::new();

        publisher.mark_changed();
        let batch = publisher.take_invalidations();

        assert_eq!(batch.version, 1);
        assert!(batch.full);
        publisher.restore_invalidations(batch);
        assert!(publisher.take_invalidations().full);
    }

    #[test]
    fn full_invalidation_can_advance_without_incrementing_an_existing_version() {
        let publisher = ConsolePublisher::new();
        publisher.notify_durable_change();

        assert_eq!(publisher.mark_full_at_least(1), 1);
        assert!(publisher.take_invalidations().full);

        assert_eq!(publisher.mark_full_at_least(4), 4);
        assert_eq!(publisher.current_version(), 4);
        assert!(publisher.take_invalidations().full);
    }

    #[test]
    fn durable_wakeup_can_advance_without_requesting_full_reconciliation() {
        let publisher = ConsolePublisher::new();

        assert_eq!(publisher.notify_durable_change_at_least(4), 4);
        assert_eq!(publisher.current_version(), 4);
        assert!(!publisher.take_invalidations().full);
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
                    |_| {},
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
    }
}
