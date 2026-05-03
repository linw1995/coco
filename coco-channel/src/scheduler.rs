use std::time::Duration;

use async_trait::async_trait;

use crate::{ChannelRuntime, InboundMessage, MessageHandler, Result};

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_CLAIM_LIMIT: usize = 16;

#[async_trait]
pub trait SchedulerTaskSource: Send + Sync {
    async fn claim_due(&self, limit: usize) -> Result<Vec<InboundMessage>>;
}

#[derive(Debug)]
pub struct SchedulerChannel<S> {
    source: S,
    poll_interval: Duration,
    claim_limit: usize,
}

impl<S> SchedulerChannel<S> {
    pub fn new(source: S) -> Self {
        Self {
            source,
            poll_interval: DEFAULT_POLL_INTERVAL,
            claim_limit: DEFAULT_CLAIM_LIMIT,
        }
    }

    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub fn with_claim_limit(mut self, claim_limit: usize) -> Self {
        self.claim_limit = claim_limit;
        self
    }
}

#[async_trait]
impl<S> ChannelRuntime for SchedulerChannel<S>
where
    S: SchedulerTaskSource + Send,
{
    async fn run<H>(self, handler: &H) -> Result<()>
    where
        H: MessageHandler,
    {
        loop {
            let messages = self.source.claim_due(self.claim_limit).await?;
            for message in messages {
                tracing::info!(
                    branch = %message.conversation_id,
                    task_id = ?message.source_message_id,
                    "dispatching scheduled inbound prompt"
                );
                handler.handle(message).await?;
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::{OutboundMessage, Result};

    #[derive(Default)]
    struct StaticSource;

    #[async_trait]
    impl SchedulerTaskSource for StaticSource {
        async fn claim_due(&self, _limit: usize) -> Result<Vec<InboundMessage>> {
            Ok(vec![InboundMessage::scheduler(
                "main",
                "scheduler",
                "nightly",
                "Run nightly review",
            )])
        }
    }

    #[derive(Default)]
    struct RecordingHandler {
        messages: Arc<Mutex<Vec<InboundMessage>>>,
    }

    #[async_trait]
    impl MessageHandler for RecordingHandler {
        async fn handle(&self, message: InboundMessage) -> Result<OutboundMessage> {
            self.messages.lock().unwrap().push(message);
            Ok(OutboundMessage {
                text: "handled".to_owned(),
            })
        }
    }

    #[tokio::test]
    async fn scheduler_channel_dispatches_claimed_messages() {
        let channel =
            SchedulerChannel::new(StaticSource).with_poll_interval(Duration::from_secs(60));
        let handler = RecordingHandler::default();

        let result = tokio::time::timeout(Duration::from_millis(50), channel.run(&handler)).await;

        assert!(result.is_err());
        assert_eq!(
            handler.messages.lock().unwrap().as_slice(),
            &[InboundMessage::scheduler(
                "main",
                "scheduler",
                "nightly",
                "Run nightly review"
            )]
        );
    }
}
