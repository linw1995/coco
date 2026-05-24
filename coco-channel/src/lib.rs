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
