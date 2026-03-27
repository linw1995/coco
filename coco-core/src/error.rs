use coco_llm::coco_mem::StoreError;
use snafu::prelude::*;

use crate::ChannelKind;

#[derive(Debug, Snafu, Clone, PartialEq, Eq)]
pub enum BranchResolveError {
    #[snafu(display("{message}"))]
    ResolveFailed { message: String },
}

#[derive(Debug, Snafu, Clone, PartialEq, Eq)]
pub enum EngineError {
    #[snafu(display("Missing session for branch {branch:?}"))]
    SessionMissing { branch: String },

    #[snafu(display("{message}"))]
    EngineFailed { message: String },
}

impl From<coco_llm::Error> for EngineError {
    fn from(error: coco_llm::Error) -> Self {
        match error {
            coco_llm::Error::MissingAnchor { branch } => Self::SessionMissing { branch },
            coco_llm::Error::Memory {
                source: StoreError::BranchNotFound { name },
            } => Self::SessionMissing { branch: name },
            coco_llm::Error::Memory {
                source: StoreError::MissingSessionAnchor { branch },
            } => Self::SessionMissing { branch },
            other => Self::EngineFailed {
                message: other.to_string(),
            },
        }
    }
}

#[derive(Debug, Snafu, Clone, PartialEq, Eq)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display(
        "Branch resolution failed for channel {channel_kind} conversation {conversation_id:?}: {source}"
    ))]
    BranchResolveFailed {
        channel_kind: ChannelKind,
        conversation_id: String,
        source: BranchResolveError,
    },

    #[snafu(display("Missing session for branch {branch:?}"))]
    MissingSession { branch: String },

    #[snafu(display("Engine failed for branch {branch:?}: {source}"))]
    ConversationFailed { branch: String, source: EngineError },

    #[snafu(display("Invalid input: {message}"))]
    InvalidInput { message: String },
}
