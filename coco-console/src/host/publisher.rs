use tokio::sync::watch;

#[derive(Clone)]
pub struct ConsolePublisher {
    source_dirty: watch::Sender<u64>,
    jobs_changed: watch::Sender<u64>,
}

impl Default for ConsolePublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsolePublisher {
    pub fn new() -> Self {
        let (source_dirty, _) = watch::channel(0);
        let (jobs_changed, _) = watch::channel(0);
        Self {
            source_dirty,
            jobs_changed,
        }
    }
}

pub fn mark_source_dirty(publisher: &ConsolePublisher) -> u64 {
    let mut generation = 0;
    publisher.source_dirty.send_modify(|current| {
        *current = current.wrapping_add(1);
        generation = *current;
    });
    generation
}

pub fn subscribe_source_changes(publisher: &ConsolePublisher) -> watch::Receiver<u64> {
    publisher.source_dirty.subscribe()
}

pub fn mark_jobs_changed(publisher: &ConsolePublisher) -> u64 {
    let mut generation = 0;
    publisher.jobs_changed.send_modify(|current| {
        *current = current.wrapping_add(1);
        generation = *current;
    });
    generation
}

pub fn subscribe_job_changes(publisher: &ConsolePublisher) -> watch::Receiver<u64> {
    publisher.jobs_changed.subscribe()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn source_changes_are_coalesced_into_dirty_wakeups() {
        let publisher = ConsolePublisher::new();
        let mut changes = subscribe_source_changes(&publisher);
        changes.borrow_and_update();

        assert_eq!(mark_source_dirty(&publisher), 1);
        assert_eq!(mark_source_dirty(&publisher), 2);
        changes.changed().await.unwrap();
        assert_eq!(*changes.borrow_and_update(), 2);
    }

    #[tokio::test]
    async fn job_changes_are_published_independently() {
        let publisher = ConsolePublisher::new();
        let mut changes = subscribe_job_changes(&publisher);
        changes.borrow_and_update();

        assert_eq!(mark_jobs_changed(&publisher), 1);
        changes.changed().await.unwrap();
        assert_eq!(*changes.borrow_and_update(), 1);
        assert_eq!(*subscribe_source_changes(&publisher).borrow(), 0);
    }
}
