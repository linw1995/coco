use tokio::sync::watch;

#[derive(Clone)]
pub struct ConsolePublisher {
    source_dirty: watch::Sender<u64>,
}

impl Default for ConsolePublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsolePublisher {
    pub fn new() -> Self {
        let (source_dirty, _) = watch::channel(0);
        Self { source_dirty }
    }

    pub(crate) fn mark_source_dirty(&self) -> u64 {
        let mut generation = 0;
        self.source_dirty.send_modify(|current| {
            *current = current.wrapping_add(1);
            generation = *current;
        });
        generation
    }

    pub(crate) fn subscribe_source_changes(&self) -> watch::Receiver<u64> {
        self.source_dirty.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn source_changes_are_coalesced_into_dirty_wakeups() {
        let publisher = ConsolePublisher::new();
        let mut changes = publisher.subscribe_source_changes();
        changes.borrow_and_update();

        assert_eq!(publisher.mark_source_dirty(), 1);
        assert_eq!(publisher.mark_source_dirty(), 2);
        changes.changed().await.unwrap();
        assert_eq!(*changes.borrow_and_update(), 2);
    }
}
