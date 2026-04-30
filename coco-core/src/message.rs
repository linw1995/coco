use std::fmt;

use coco_llm::MergeParentRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelKind {
    Cli,
    Telegram,
    Discord,
}

impl ChannelKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Telegram => "telegram",
            Self::Discord => "discord",
        }
    }
}

impl fmt::Display for ChannelKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InboundMessage {
    pub channel_kind: ChannelKind,
    pub conversation_id: String,
    pub sender_id: String,
    pub text: String,
}

impl InboundMessage {
    pub fn cli(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self::new(ChannelKind::Cli, conversation_id, sender_id, text)
    }

    pub fn telegram(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self::new(ChannelKind::Telegram, conversation_id, sender_id, text)
    }

    pub fn discord(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self::new(ChannelKind::Discord, conversation_id, sender_id, text)
    }

    fn new(
        channel_kind: ChannelKind,
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            channel_kind,
            conversation_id: conversation_id.into(),
            sender_id: sender_id.into(),
            text: text.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutboundMessage {
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchPromptRequest {
    pub branch: String,
    pub prompt: String,
    pub merge_parents: Vec<MergeParentRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchPromptRequest {
    pub items: Vec<BranchPromptRequest>,
    pub max_concurrency: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchPromptStatus {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchPromptSuccess {
    pub execution_id: Option<String>,
    pub response_node_id: String,
    pub branch_head: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchPromptFailure {
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchPromptResult {
    Succeeded(BranchPromptSuccess),
    Failed(BranchPromptFailure),
}

impl BranchPromptResult {
    pub fn status(&self) -> BranchPromptStatus {
        match self {
            Self::Succeeded(_) => BranchPromptStatus::Succeeded,
            Self::Failed(_) => BranchPromptStatus::Failed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchPromptOutcome {
    pub branch: String,
    pub result: BranchPromptResult,
}

impl BranchPromptOutcome {
    pub fn succeeded(
        branch: impl Into<String>,
        execution_id: Option<String>,
        response_node_id: impl Into<String>,
        branch_head: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            branch: branch.into(),
            result: BranchPromptResult::Succeeded(BranchPromptSuccess {
                execution_id,
                response_node_id: response_node_id.into(),
                branch_head: branch_head.into(),
                text: text.into(),
            }),
        }
    }

    pub fn failed(branch: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            branch: branch.into(),
            result: BranchPromptResult::Failed(BranchPromptFailure {
                error: error.into(),
            }),
        }
    }

    pub fn status(&self) -> BranchPromptStatus {
        self.result.status()
    }

    pub fn success(&self) -> Option<&BranchPromptSuccess> {
        match &self.result {
            BranchPromptResult::Succeeded(result) => Some(result),
            BranchPromptResult::Failed(_) => None,
        }
    }

    pub fn failure(&self) -> Option<&BranchPromptFailure> {
        match &self.result {
            BranchPromptResult::Succeeded(_) => None,
            BranchPromptResult::Failed(result) => Some(result),
        }
    }

    pub fn execution_id(&self) -> Option<&str> {
        self.success()
            .and_then(|result| result.execution_id.as_deref())
    }

    pub fn response_node_id(&self) -> Option<&str> {
        self.success()
            .map(|result| result.response_node_id.as_str())
    }

    pub fn branch_head(&self) -> Option<&str> {
        self.success().map(|result| result.branch_head.as_str())
    }

    pub fn text(&self) -> Option<&str> {
        self.success().map(|result| result.text.as_str())
    }

    pub fn error(&self) -> Option<&str> {
        self.failure().map(|result| result.error.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchPromptResult {
    pub outcomes: Vec<BranchPromptOutcome>,
}
