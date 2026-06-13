mod engine;
mod error;
mod message;
mod resolver;
mod service;
mod skill;

pub use coco_channel::{
    ChannelKind, InboundMessage, OutboundMessage, TelegramImageAttachment, TelegramInboundMessage,
    TelegramVoiceAttachment,
};
pub use coco_llm::coco_mem::JobStatus;
pub use engine::{BranchLockGuard, ConversationEngine, JobStatusSnapshot, SYSTEM_EVENT_QUEUE};
pub use error::{BranchResolveError, EngineError, Error};
pub use message::{
    BatchPromptRequest, BatchPromptResult, BranchPromptFailure, BranchPromptOutcome,
    BranchPromptRequest, BranchPromptResult, BranchPromptStatus, BranchPromptSuccess,
};
pub use resolver::{BranchResolver, FixedBranchResolver};
pub use service::CoreService;
pub use skill::{CoreSkillSearchExecutor, SkillMatch, SkillSearchResult};

#[cfg(test)]
mod tests;
