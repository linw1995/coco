mod engine;
mod error;
mod message;
mod resolver;
mod service;

pub use engine::{ConversationEngine, LlmConversationEngine};
pub use error::{BranchResolveError, EngineError, Error};
pub use message::{ChannelKind, InboundMessage, OutboundMessage};
pub use resolver::{BranchResolver, FixedBranchResolver};
pub use service::CoreService;

#[cfg(test)]
mod tests;
