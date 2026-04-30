use async_trait::async_trait;

use crate::{InboundMessage, OutboundMessage, Result};

#[async_trait]
pub trait MessageHandler: Send + Sync {
    async fn handle(&self, message: InboundMessage) -> Result<OutboundMessage>;
}

#[async_trait]
pub trait ChannelRuntime: Send {
    async fn run<H>(self, handler: &H) -> Result<()>
    where
        H: MessageHandler;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoHandler;

    #[async_trait]
    impl MessageHandler for EchoHandler {
        async fn handle(&self, message: InboundMessage) -> Result<OutboundMessage> {
            Ok(OutboundMessage { text: message.text })
        }
    }

    #[tokio::test]
    async fn message_handler_returns_outbound_message() {
        let response = EchoHandler
            .handle(InboundMessage::cli("conversation", "sender", "hello"))
            .await
            .unwrap();

        assert_eq!(response.text, "hello");
    }
}
