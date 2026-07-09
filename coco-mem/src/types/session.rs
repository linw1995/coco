use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{SkillRuntimeContext, Tool};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionAnchor {
    pub role: SessionRole,
    #[serde(default)]
    pub provider_profile: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    pub model: String,
    pub tools: Vec<Tool>,
    pub system_prompt: String,
    pub prompt: String,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub additional_params: Option<Value>,
    #[serde(default)]
    pub enable_coco_shim: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_skill: Option<SkillRuntimeContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PauseReason {
    Merged { merged_anchor_id: String },
    Closed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SessionState {
    Active,
    Attached {
        target_branch: String,
        base_head_id: String,
    },
    Paused {
        target_branch: String,
        reason: PauseReason,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionRole {
    Orchestrator,
    Runner,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SessionAnchorPatch {
    pub role: Option<SessionRole>,
    pub provider_profile: Option<Option<String>>,
    pub provider: Option<Option<String>>,
    pub model: Option<String>,
    pub tools: Option<Vec<Tool>>,
    pub system_prompt: Option<String>,
    pub temperature: Option<Option<f64>>,
    pub max_tokens: Option<Option<u64>>,
    pub additional_params: Option<Option<Value>>,
    pub enable_coco_shim: Option<bool>,
}

impl PauseReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Merged { .. } => "merged",
            Self::Closed => "closed",
        }
    }

    pub fn merged_anchor_id(&self) -> Option<&str> {
        match self {
            Self::Merged { merged_anchor_id } => Some(merged_anchor_id),
            Self::Closed => None,
        }
    }
}

impl SessionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Attached { .. } => "attached",
            Self::Paused { .. } => "paused",
        }
    }

    pub fn target_branch(&self) -> Option<&str> {
        match self {
            Self::Active => None,
            Self::Attached { target_branch, .. } | Self::Paused { target_branch, .. } => {
                Some(target_branch)
            }
        }
    }

    pub fn base_head_id(&self) -> Option<&str> {
        match self {
            Self::Attached { base_head_id, .. } => Some(base_head_id),
            Self::Active | Self::Paused { .. } => None,
        }
    }

    pub fn pause_reason(&self) -> Option<&PauseReason> {
        match self {
            Self::Paused { reason, .. } => Some(reason),
            Self::Active | Self::Attached { .. } => None,
        }
    }
}

impl SessionAnchor {
    pub fn apply_patch(&self, patch: &SessionAnchorPatch) -> Self {
        Self {
            role: patch.role.unwrap_or(self.role),
            provider_profile: patch
                .provider_profile
                .clone()
                .unwrap_or_else(|| self.provider_profile.clone()),
            provider: patch
                .provider
                .clone()
                .unwrap_or_else(|| self.provider.clone()),
            model: patch.model.clone().unwrap_or_else(|| self.model.clone()),
            tools: patch.tools.clone().unwrap_or_else(|| self.tools.clone()),
            system_prompt: patch
                .system_prompt
                .clone()
                .unwrap_or_else(|| self.system_prompt.clone()),
            prompt: self.prompt.clone(),
            temperature: patch.temperature.unwrap_or(self.temperature),
            max_tokens: patch.max_tokens.unwrap_or(self.max_tokens),
            additional_params: patch
                .additional_params
                .clone()
                .unwrap_or_else(|| self.additional_params.clone()),
            enable_coco_shim: patch.enable_coco_shim.unwrap_or(self.enable_coco_shim),
            active_skill: self.active_skill.clone(),
        }
    }
}

impl SessionRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Orchestrator => "orchestrator",
            Self::Runner => "runner",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "orchestrator" => Some(Self::Orchestrator),
            "runner" => Some(Self::Runner),
            _ => None,
        }
    }
}
