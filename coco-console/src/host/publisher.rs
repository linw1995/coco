use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::broadcast;

const NODE_CREATION_CHANNEL_CAPACITY: usize = 1_024;

#[derive(Clone)]
pub struct ConsolePublisher {
    version: Arc<AtomicU64>,
    node_tx: broadcast::Sender<ConsoleNodeCreated>,
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
        let (node_tx, _) = broadcast::channel(NODE_CREATION_CHANNEL_CAPACITY);
        Self {
            version: Arc::new(AtomicU64::new(0)),
            node_tx,
        }
    }

    pub fn current_version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    pub(crate) fn mark_node_created(&self, node_id: impl Into<String>) -> u64 {
        let source_version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.node_tx.send(ConsoleNodeCreated {
            source_version,
            node_id: node_id.into(),
        });
        source_version
    }

    pub fn advance_to(&self, target: u64) -> u64 {
        self.version.fetch_max(target, Ordering::SeqCst).max(target)
    }

    pub(crate) fn subscribe_node_creations(&self) -> broadcast::Receiver<ConsoleNodeCreated> {
        self.node_tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn node_creation_advances_version_and_publishes_identity() {
        let publisher = ConsolePublisher::new();
        let mut events = publisher.subscribe_node_creations();

        assert_eq!(publisher.mark_node_created("node-1"), 1);
        assert_eq!(
            events.recv().await.unwrap(),
            ConsoleNodeCreated {
                source_version: 1,
                node_id: "node-1".to_owned(),
            }
        );
    }

    #[test]
    fn advance_to_never_moves_source_version_backwards() {
        let publisher = ConsolePublisher::new();

        assert_eq!(publisher.advance_to(4), 4);
        assert_eq!(publisher.advance_to(2), 4);
        assert_eq!(publisher.current_version(), 4);
    }
}
