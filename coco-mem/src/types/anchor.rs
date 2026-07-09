use serde::{Deserialize, Serialize};

use super::{SessionAnchor, SessionAnchorPatch};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Anchor {
    pub merge_parents: Vec<MergeParent>,
    pub payload: AnchorPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MergeParent {
    Merge { node_id: String },
    Shadow { node_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AnchorPayload {
    Session(Box<SessionAnchor>),
    SessionPatch(SessionAnchorPatch),
    Prompt(PromptAnchor),
    SkillInvocation(SkillInvocationAnchor),
    SkillResult(SkillResultAnchor),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnchorPayloadKind {
    Session,
    SessionPatch,
    Prompt,
    SkillInvocation,
    SkillResult,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PromptAnchor {
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<PromptAttachment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PromptAttachment {
    Image(PromptImageAttachment),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptImageAttachment {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillInvocationAnchor {
    pub skill_name: String,
    pub mode: SkillInvocationMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SkillInvocationMode {
    InheritContext,
    Handoff { prompt: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillResultAnchor {
    pub skill_name: String,
    pub output: String,
}

impl MergeParent {
    pub fn merge(node_id: impl Into<String>) -> Self {
        Self::Merge {
            node_id: node_id.into(),
        }
    }

    pub fn shadow(node_id: impl Into<String>) -> Self {
        Self::Shadow {
            node_id: node_id.into(),
        }
    }

    pub fn node_id(&self) -> &str {
        match self {
            Self::Merge { node_id } | Self::Shadow { node_id } => node_id,
        }
    }

    pub fn is_shadow(&self) -> bool {
        matches!(self, Self::Shadow { .. })
    }
}

impl Anchor {
    pub fn session(merge_parents: Vec<MergeParent>, anchor: SessionAnchor) -> Self {
        Self {
            merge_parents,
            payload: AnchorPayload::Session(Box::new(anchor)),
        }
    }

    pub fn session_patch(merge_parents: Vec<MergeParent>, patch: SessionAnchorPatch) -> Self {
        Self {
            merge_parents,
            payload: AnchorPayload::SessionPatch(patch),
        }
    }

    pub fn prompt(merge_parents: Vec<MergeParent>, anchor: PromptAnchor) -> Self {
        Self {
            merge_parents,
            payload: AnchorPayload::Prompt(anchor),
        }
    }

    pub fn skill_invocation(
        merge_parents: Vec<MergeParent>,
        anchor: SkillInvocationAnchor,
    ) -> Self {
        Self {
            merge_parents,
            payload: AnchorPayload::SkillInvocation(anchor),
        }
    }

    pub fn skill_result(merge_parents: Vec<MergeParent>, anchor: SkillResultAnchor) -> Self {
        Self {
            merge_parents,
            payload: AnchorPayload::SkillResult(anchor),
        }
    }

    pub fn merge_parents(&self) -> &[MergeParent] {
        &self.merge_parents
    }

    pub fn merge_parent_node_ids(&self) -> Vec<&str> {
        self.merge_parents
            .iter()
            .map(MergeParent::node_id)
            .collect()
    }

    pub fn payload_kind(&self) -> AnchorPayloadKind {
        self.payload.kind()
    }

    pub fn as_session(&self) -> Option<&SessionAnchor> {
        match &self.payload {
            AnchorPayload::Session(anchor) => Some(anchor.as_ref()),
            AnchorPayload::SessionPatch(_)
            | AnchorPayload::Prompt(_)
            | AnchorPayload::SkillInvocation(_)
            | AnchorPayload::SkillResult(_) => None,
        }
    }

    pub fn as_session_patch(&self) -> Option<&SessionAnchorPatch> {
        match &self.payload {
            AnchorPayload::SessionPatch(patch) => Some(patch),
            AnchorPayload::Session(_)
            | AnchorPayload::Prompt(_)
            | AnchorPayload::SkillInvocation(_)
            | AnchorPayload::SkillResult(_) => None,
        }
    }

    pub fn as_prompt(&self) -> Option<&PromptAnchor> {
        match &self.payload {
            AnchorPayload::Session(_)
            | AnchorPayload::SessionPatch(_)
            | AnchorPayload::SkillInvocation(_)
            | AnchorPayload::SkillResult(_) => None,
            AnchorPayload::Prompt(anchor) => Some(anchor),
        }
    }

    pub fn as_skill_invocation(&self) -> Option<&SkillInvocationAnchor> {
        match &self.payload {
            AnchorPayload::Session(_)
            | AnchorPayload::SessionPatch(_)
            | AnchorPayload::Prompt(_)
            | AnchorPayload::SkillResult(_) => None,
            AnchorPayload::SkillInvocation(anchor) => Some(anchor),
        }
    }

    pub fn as_skill_result(&self) -> Option<&SkillResultAnchor> {
        match &self.payload {
            AnchorPayload::Session(_)
            | AnchorPayload::SessionPatch(_)
            | AnchorPayload::Prompt(_)
            | AnchorPayload::SkillInvocation(_) => None,
            AnchorPayload::SkillResult(anchor) => Some(anchor),
        }
    }
}

impl AnchorPayload {
    pub fn kind(&self) -> AnchorPayloadKind {
        match self {
            Self::Session(_) => AnchorPayloadKind::Session,
            Self::SessionPatch(_) => AnchorPayloadKind::SessionPatch,
            Self::Prompt(_) => AnchorPayloadKind::Prompt,
            Self::SkillInvocation(_) => AnchorPayloadKind::SkillInvocation,
            Self::SkillResult(_) => AnchorPayloadKind::SkillResult,
        }
    }
}

impl AnchorPayloadKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::SessionPatch => "session_patch",
            Self::Prompt => "prompt",
            Self::SkillInvocation => "skill_invocation",
            Self::SkillResult => "skill_result",
        }
    }
}
