mod error;
mod message;
mod runtime;
#[cfg(feature = "telegram")]
pub mod telegram;

pub use error::{BoxError, Error, Result};
pub use message::{
    ChannelInboundMessage, ChannelKind, InboundMessage, OutboundMessage, TelegramImageAttachment,
    TelegramInboundMessage,
};
pub use runtime::{ChannelRuntime, MessageHandler};

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn init_test_logging() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::DEBUG)
        .with_test_writer()
        .try_init();
}
