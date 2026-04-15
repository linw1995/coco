mod engine;
mod error;
mod message;
mod resolver;
mod service;
mod skill;

pub use coco_llm::coco_mem::JobStatus;
pub use engine::{ConversationEngine, JobStatusSnapshot};
pub use error::{BranchResolveError, EngineError, Error};
pub use message::{
    BatchPromptRequest, BatchPromptResult, BranchPromptFailure, BranchPromptOutcome,
    BranchPromptRequest, BranchPromptResult, BranchPromptStatus, BranchPromptSuccess, ChannelKind,
    InboundMessage, OutboundMessage,
};
pub use resolver::{BranchResolver, FixedBranchResolver};
pub use service::CoreService;
pub use skill::{CoreSkillToolExecutor, SkillMatch, SkillSearchResult};

#[cfg(test)]
mod tests;
