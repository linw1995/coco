use std::collections::BTreeMap;

use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{SessionAnchor, SessionAnchorPatch, SessionRole, Tool};

/// Preset configuration for creating sessions and rebasing runtime defaults.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Preset {
    pub role: SessionRole,
    pub provider_profile: String,
    pub model: String,
    #[serde(default)]
    pub tools: Vec<Tool>,
    pub system_prompt: String,
    #[serde(default)]
    pub prompt: String,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub max_tokens: Option<u64>,
    #[serde(default)]
    pub additional_params: Option<Value>,
    #[serde(default)]
    pub enable_coco_shim: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PresetVersion {
    pub version: u64,
    pub created_at: Timestamp,
    #[serde(flatten)]
    pub config: Preset,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct PresetRecord {
    pub name: String,
    pub current_version: u64,
    #[serde(default)]
    pub versions: BTreeMap<u64, PresetVersion>,
}

impl Preset {
    /// Builds the runtime patch used by session rebase.
    ///
    /// The durable user prompt is intentionally excluded: changing it requires
    /// a new session anchor rather than patching the current one.
    pub fn to_session_anchor_patch(&self) -> SessionAnchorPatch {
        SessionAnchorPatch {
            role: Some(self.role),
            provider_profile: Some(Some(self.provider_profile.clone())),
            provider: Some(None),
            model: Some(self.model.clone()),
            tools: Some(self.tools.clone()),
            system_prompt: Some(self.system_prompt.clone()),
            temperature: Some(self.temperature),
            max_tokens: Some(self.max_tokens),
            additional_params: Some(self.additional_params.clone()),
            enable_coco_shim: Some(self.enable_coco_shim),
        }
    }

    pub fn apply_to_session_anchor(&self, anchor: &SessionAnchor) -> SessionAnchor {
        SessionAnchor {
            role: self.role,
            provider_profile: Some(self.provider_profile.clone()),
            provider: None,
            model: self.model.clone(),
            tools: self.tools.clone(),
            system_prompt: self.system_prompt.clone(),
            prompt: self.prompt.clone(),
            temperature: self.temperature,
            max_tokens: self.max_tokens,
            additional_params: self.additional_params.clone(),
            enable_coco_shim: self.enable_coco_shim,
            active_skill: anchor.active_skill.clone(),
        }
    }
}

impl PresetVersion {
    pub fn new(version: u64, config: Preset) -> Self {
        Self {
            version,
            created_at: Timestamp::now(),
            config,
        }
    }

    pub fn to_preset(&self) -> Preset {
        self.config.clone()
    }
}

impl PresetRecord {
    pub fn new(name: impl Into<String>, config: Preset) -> Self {
        let version = PresetVersion::new(1, config);
        let current_version = version.version;
        let mut versions = BTreeMap::new();
        versions.insert(current_version, version);

        Self {
            name: name.into(),
            current_version,
            versions,
        }
    }

    pub fn current(&self) -> Option<&PresetVersion> {
        self.versions.get(&self.current_version)
    }

    pub fn current_preset(&self) -> Option<Preset> {
        self.current().map(PresetVersion::to_preset)
    }

    pub fn update(&mut self, config: Preset) -> Option<&PresetVersion> {
        self.current()?;
        let next_version = self.versions.keys().next_back().copied().unwrap_or(0) + 1;
        let next = PresetVersion::new(next_version, config);

        self.current_version = next_version;
        self.versions.insert(next_version, next);
        self.current()
    }

    pub fn rollback(&mut self, target_version: u64) -> Option<&PresetVersion> {
        let target = self.versions.get(&target_version)?.to_preset();
        let next_version = self.versions.keys().next_back().copied().unwrap_or(0) + 1;
        let next = PresetVersion::new(next_version, target);

        self.current_version = next_version;
        self.versions.insert(next_version, next);
        self.current()
    }
}
