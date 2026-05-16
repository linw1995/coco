use std::collections::BTreeMap;

use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Represents a node in the memory graph, similar to a git commit
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Node {
    pub id: String,
    pub parent: String,
    pub created_at: Timestamp,
    pub role: Role,
    pub metadata: Option<NodeMetadata>,
    pub kind: Kind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewNode {
    pub parent: String,
    pub role: Role,
    pub metadata: Option<NodeMetadata>,
    pub kind: Kind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Role {
    User,
    System,
    LLM,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Kind {
    Anchor(Anchor),
    ToolUse(ManyOrOne<ToolUse>),
    ToolResult(ManyOrOne<ToolResult>),
    Text(String),
    Failure(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ManyOrOne<T> {
    One(T),
    Many(Vec<T>),
}

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
pub enum JobStatus {
    Queued,
    Running,
    Finished,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionRole {
    Orchestrator,
    Runner,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// A persisted execution job bound to a branch.
pub struct Job {
    pub job_id: String,
    pub created_at: Timestamp,
    #[serde(default)]
    pub finished_at: Option<Timestamp>,
    pub branch: String,
    /// The node where this job starts execution.
    ///
    /// For prompt-based jobs this is the detached prompt anchor. For resume-style
    /// jobs this can be any existing node that should continue execution.
    pub base: String,
    pub status: JobStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewMessageQueueItem {
    pub queue: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MessageQueueItem {
    pub message_id: String,
    pub queue: String,
    pub created_at: Timestamp,
    pub payload: Value,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderProfile {
    pub provider: String,
    #[serde(default)]
    pub secrets: BTreeMap<String, String>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub default_model: Option<String>,
    #[serde(default)]
    #[serde(flatten)]
    pub spec: ProviderSpec,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderSpec {
    #[serde(flatten)]
    pub gpt: GptProviderSpec,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GptProviderSpec {
    #[serde(default)]
    pub reasoning_level: Option<String>,
    #[serde(default)]
    pub service_tier: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillScript {
    pub path: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillRuntimeContext {
    pub name: String,
    pub handoff: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillVersionSpec {
    pub description: String,
    pub body: String,
    #[serde(default)]
    pub scripts: Vec<SkillScript>,
    #[serde(default)]
    pub enable_coco_shim: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillUpdatePatch {
    pub description: Option<String>,
    pub body: Option<String>,
    pub scripts: Option<Vec<SkillScript>>,
    pub enable_coco_shim: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillVersion {
    #[serde(default)]
    pub id: String,
    pub version: u64,
    pub created_at: Timestamp,
    pub description: String,
    pub body: String,
    #[serde(default)]
    pub scripts: Vec<SkillScript>,
    #[serde(default)]
    pub enable_coco_shim: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillRecord {
    pub name: String,
    pub current_version: u64,
    #[serde(default)]
    pub versions: BTreeMap<u64, SkillVersion>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillGroups {
    #[serde(default)]
    pub orchestrator: BTreeMap<String, SkillRecord>,
    #[serde(default)]
    pub runner: BTreeMap<String, SkillRecord>,
}

impl SkillUpdatePatch {
    pub fn is_empty(&self) -> bool {
        self.description.is_none()
            && self.body.is_none()
            && self.scripts.is_none()
            && self.enable_coco_shim.is_none()
    }
}

impl SkillVersion {
    pub fn new(version: u64, spec: SkillVersionSpec) -> Self {
        Self {
            id: compute_skill_version_id(
                &spec.description,
                &spec.body,
                &spec.scripts,
                spec.enable_coco_shim,
            ),
            version,
            created_at: Timestamp::now(),
            description: spec.description,
            body: spec.body,
            scripts: spec.scripts,
            enable_coco_shim: spec.enable_coco_shim,
        }
    }

    pub fn expected_id(&self) -> String {
        compute_skill_version_id(
            &self.description,
            &self.body,
            &self.scripts,
            self.enable_coco_shim,
        )
    }

    pub fn normalize_id(&mut self) {
        if self.id.is_empty() {
            self.id = self.expected_id();
        }
    }

    pub fn id_matches_content(&self) -> bool {
        self.id == self.expected_id()
    }
}

impl SkillRecord {
    pub fn new(name: impl Into<String>, spec: SkillVersionSpec) -> Self {
        let version = SkillVersion::new(1, spec);
        let current_version = version.version;
        let mut versions = BTreeMap::new();
        versions.insert(current_version, version);

        Self {
            name: name.into(),
            current_version,
            versions,
        }
    }

    pub fn current(&self) -> Option<&SkillVersion> {
        self.versions.get(&self.current_version)
    }

    pub fn update(&mut self, patch: &SkillUpdatePatch) -> Option<&SkillVersion> {
        let current = self.current()?.clone();
        let next_version = self.versions.keys().next_back().copied().unwrap_or(0) + 1;
        let description = patch.description.clone().unwrap_or(current.description);
        let body = patch.body.clone().unwrap_or(current.body);
        let scripts = patch.scripts.clone().unwrap_or(current.scripts);
        let enable_coco_shim = patch.enable_coco_shim.unwrap_or(current.enable_coco_shim);
        let next = SkillVersion {
            id: compute_skill_version_id(&description, &body, &scripts, enable_coco_shim),
            version: next_version,
            created_at: Timestamp::now(),
            description,
            body,
            scripts,
            enable_coco_shim,
        };

        self.current_version = next_version;
        self.versions.insert(next_version, next);
        self.current()
    }

    pub fn rollback(&mut self, target_version: u64) -> Option<&SkillVersion> {
        let target = self.versions.get(&target_version)?.clone();
        let next_version = self.versions.keys().next_back().copied().unwrap_or(0) + 1;
        let next = SkillVersion {
            id: target.id,
            version: next_version,
            created_at: Timestamp::now(),
            description: target.description,
            body: target.body,
            scripts: target.scripts,
            enable_coco_shim: target.enable_coco_shim,
        };

        self.current_version = next_version;
        self.versions.insert(next_version, next);
        self.current()
    }
}

impl SkillGroups {
    pub fn is_empty(&self) -> bool {
        self.orchestrator.is_empty() && self.runner.is_empty()
    }

    pub fn for_role(&self, role: SessionRole) -> &BTreeMap<String, SkillRecord> {
        match role {
            SessionRole::Orchestrator => &self.orchestrator,
            SessionRole::Runner => &self.runner,
        }
    }

    pub fn for_role_mut(&mut self, role: SessionRole) -> &mut BTreeMap<String, SkillRecord> {
        match role {
            SessionRole::Orchestrator => &mut self.orchestrator,
            SessionRole::Runner => &mut self.runner,
        }
    }
}

pub fn default_skill_groups() -> SkillGroups {
    let mut groups = SkillGroups::default();
    groups.orchestrator.insert(
        "coco-orchestrator".to_owned(),
        SkillRecord::new(
            "coco-orchestrator",
            SkillVersionSpec {
                description:
                    "Guide an orchestrator session through CoCo branch and prompt workflows."
                        .to_owned(),
                body: include_str!("default_skills/coco-orchestrator.md")
                    .trim()
                    .to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        ),
    );
    groups.orchestrator.insert(
        "new-skill".to_owned(),
        SkillRecord::new(
            "new-skill",
            SkillVersionSpec {
                description: "Create or update dynamic CoCo skills through the skill add workflow."
                    .to_owned(),
                body: include_str!("default_skills/new-skill.md")
                    .trim()
                    .to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        ),
    );
    groups.orchestrator.insert(
        "cronjob".to_owned(),
        SkillRecord::new(
            "cronjob",
            SkillVersionSpec {
                description: "Manage host crontab entries that submit CoCo prompts.".to_owned(),
                body: include_str!("default_skills/cronjob.md").trim().to_owned(),
                scripts: vec![
                    SkillScript {
                        path: "scripts/cronjob_add.py".to_owned(),
                        content: include_str!("default_skills/cronjob/scripts/cronjob_add.py")
                            .to_owned(),
                    },
                    SkillScript {
                        path: "scripts/cronjob_run.py".to_owned(),
                        content: include_str!("default_skills/cronjob/scripts/cronjob_run.py")
                            .to_owned(),
                    },
                    SkillScript {
                        path: "scripts/cronjob_crontab.py".to_owned(),
                        content: include_str!("default_skills/cronjob/scripts/cronjob_crontab.py")
                            .to_owned(),
                    },
                ],
                enable_coco_shim: true,
            },
        ),
    );
    groups.runner.insert(
        "coco-runner".to_owned(),
        SkillRecord::new(
            "coco-runner",
            SkillVersionSpec {
                description:
                    "Guide a runner session through the CoCo commands available in runner scope."
                        .to_owned(),
                body: include_str!("default_skills/coco-runner.md")
                    .trim()
                    .to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        ),
    );
    groups.runner.insert(
        "telegram".to_owned(),
        SkillRecord::new(
            "telegram",
            SkillVersionSpec {
                description:
                    "Send, reply to, and edit Telegram messages through the Telegram Bot API."
                        .to_owned(),
                body: include_str!("default_skills/telegram.md").trim().to_owned(),
                scripts: vec![
                    SkillScript {
                        path: "scripts/telegram_send.py".to_owned(),
                        content: include_str!("default_skills/telegram/scripts/telegram_send.py")
                            .to_owned(),
                    },
                    SkillScript {
                        path: "scripts/telegram_edit.py".to_owned(),
                        content: include_str!("default_skills/telegram/scripts/telegram_edit.py")
                            .to_owned(),
                    },
                ],
                enable_coco_shim: true,
            },
        ),
    );
    groups
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PromptAnchor {
    pub prompt: String,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionMetadata {
    pub execution_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderMetadata {
    pub call_id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct BackendMetadata {
    pub execution_id: Option<String>,
    // Provider-specific metadata such as rig's optional call_id should stay at
    // the metadata boundary instead of leaking into domain payload types.
    pub call_id: Option<String>,
}

pub type NodeMetadata = ManyOrOne<BackendMetadata>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackendMetadataBuilder {
    execution_id: Option<String>,
    call_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolUse {
    pub id: String,
    pub name: String,
    pub input: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolResult {
    pub id: String,
    pub output: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

impl<T> ManyOrOne<T> {
    pub fn one(value: T) -> Self {
        Self::One(value)
    }

    pub fn many(values: Vec<T>) -> Self {
        Self::from_items(values)
    }

    pub fn from_items(mut items: Vec<T>) -> Self {
        if items.len() == 1 {
            Self::One(items.pop().expect("items length is one"))
        } else {
            Self::Many(items)
        }
    }

    pub fn items(&self) -> &[T] {
        match self {
            Self::One(item) => std::slice::from_ref(item),
            Self::Many(items) => items,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items().iter()
    }

    pub fn first(&self) -> Option<&T> {
        self.items().first()
    }

    pub fn as_one(&self) -> Option<&T> {
        match self {
            Self::One(item) => Some(item),
            Self::Many(_) => None,
        }
    }
}

impl<T> From<T> for ManyOrOne<T> {
    fn from(value: T) -> Self {
        Self::one(value)
    }
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

impl Job {
    pub fn new(
        job_id: impl Into<String>,
        branch: impl Into<String>,
        base: impl Into<String>,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            created_at: Timestamp::now(),
            finished_at: None,
            branch: branch.into(),
            base: base.into(),
            status: JobStatus::Queued,
        }
    }
}

impl MessageQueueItem {
    pub fn new(queue: impl Into<String>, payload: Value, created_at: Timestamp) -> Self {
        let queue = queue.into();
        let message_id = compute_message_queue_item_id(&queue, &payload, &created_at);

        Self {
            message_id,
            queue,
            created_at,
            payload,
        }
    }
}

impl ExecutionMetadata {
    pub fn new(execution_id: String) -> Self {
        Self { execution_id }
    }
}

impl ProviderMetadata {
    pub fn new(call_id: Option<String>) -> Self {
        Self { call_id }
    }
}

impl BackendMetadata {
    pub fn builder() -> BackendMetadataBuilder {
        BackendMetadataBuilder::default()
    }

    pub fn from_parts(
        execution: Option<&ExecutionMetadata>,
        provider: Option<&ProviderMetadata>,
    ) -> Option<NodeMetadata> {
        Self::builder()
            .maybe_execution(execution)
            .maybe_provider(provider)
            .build()
    }
}

impl BackendMetadataBuilder {
    pub fn execution(mut self, metadata: &ExecutionMetadata) -> Self {
        self.execution_id = Some(metadata.execution_id.clone());
        self
    }

    pub fn maybe_execution(self, metadata: Option<&ExecutionMetadata>) -> Self {
        match metadata {
            Some(metadata) => self.execution(metadata),
            None => self,
        }
    }

    pub fn provider(mut self, metadata: &ProviderMetadata) -> Self {
        self.call_id = metadata.call_id.clone();
        self
    }

    pub fn maybe_provider(self, metadata: Option<&ProviderMetadata>) -> Self {
        match metadata {
            Some(metadata) => self.provider(metadata),
            None => self,
        }
    }

    pub fn build(self) -> Option<NodeMetadata> {
        if self.execution_id.is_none() && self.call_id.is_none() {
            return None;
        }

        Some(NodeMetadata::one(BackendMetadata {
            execution_id: self.execution_id,
            call_id: self.call_id,
        }))
    }

    pub fn build_item(self) -> Option<BackendMetadata> {
        if self.execution_id.is_none() && self.call_id.is_none() {
            return None;
        }

        Some(BackendMetadata {
            execution_id: self.execution_id,
            call_id: self.call_id,
        })
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

impl Node {
    pub fn new(
        parent: String,
        role: Role,
        metadata: Option<NodeMetadata>,
        kind: Kind,
        created_at: Timestamp,
    ) -> Self {
        let id = compute_node_id(&parent, &role, metadata.as_ref(), &kind, &created_at);

        Self {
            id,
            parent,
            created_at,
            role,
            metadata,
            kind,
        }
    }

    pub fn is_root(&self) -> bool {
        self.parent.is_empty()
    }
}

impl Kind {
    pub fn tool_use(tool_use: ToolUse) -> Self {
        Self::ToolUse(ManyOrOne::one(tool_use))
    }

    pub fn tool_uses(tool_uses: Vec<ToolUse>) -> Self {
        Self::ToolUse(ManyOrOne::many(tool_uses))
    }

    pub fn tool_use_items(tool_uses: Vec<ToolUse>) -> Self {
        Self::ToolUse(ManyOrOne::from_items(tool_uses))
    }

    pub fn tool_result(tool_result: ToolResult) -> Self {
        Self::ToolResult(ManyOrOne::one(tool_result))
    }

    pub fn tool_results(tool_results: Vec<ToolResult>) -> Self {
        Self::ToolResult(ManyOrOne::many(tool_results))
    }

    pub fn tool_result_items(tool_results: Vec<ToolResult>) -> Self {
        Self::ToolResult(ManyOrOne::from_items(tool_results))
    }

    pub fn as_tool_uses(&self) -> Option<&ManyOrOne<ToolUse>> {
        match self {
            Self::ToolUse(tool_uses) => Some(tool_uses),
            Self::Anchor(_) | Self::ToolResult(_) | Self::Text(_) | Self::Failure(_) => None,
        }
    }

    pub fn as_tool_results(&self) -> Option<&ManyOrOne<ToolResult>> {
        match self {
            Self::ToolResult(tool_results) => Some(tool_results),
            Self::Anchor(_) | Self::ToolUse(_) | Self::Text(_) | Self::Failure(_) => None,
        }
    }
}

#[derive(Serialize)]
struct NodeHashPayload<'a> {
    parent: &'a str,
    role: &'a Role,
    metadata: Option<&'a NodeMetadata>,
    kind: &'a Kind,
    created_at: &'a Timestamp,
}

#[derive(Serialize)]
struct MessageQueueItemHashPayload<'a> {
    queue: &'a str,
    payload: &'a Value,
    created_at: &'a Timestamp,
}

// Skill revision ids are content-addressed so builtin revisions remain stable across stores.
#[derive(Serialize)]
struct SkillVersionHashPayload<'a> {
    description: &'a str,
    body: &'a str,
    scripts: &'a [SkillScript],
    enable_coco_shim: bool,
}

fn compute_node_id(
    parent: &str,
    role: &Role,
    metadata: Option<&NodeMetadata>,
    kind: &Kind,
    created_at: &Timestamp,
) -> String {
    let payload = serde_json::to_vec(&NodeHashPayload {
        parent,
        role,
        metadata,
        kind,
        created_at,
    })
    .expect("node hash payload should serialize");

    let mut hasher = Sha256::new();
    hasher.update(format!("node {}\0", payload.len()).as_bytes());
    hasher.update(&payload);

    hex_encode(&hasher.finalize())
}

fn compute_message_queue_item_id(queue: &str, payload: &Value, created_at: &Timestamp) -> String {
    let payload = serde_json::to_vec(&MessageQueueItemHashPayload {
        queue,
        payload,
        created_at,
    })
    .expect("message queue item hash payload should serialize");

    let mut hasher = Sha256::new();
    hasher.update(format!("message_queue_item {}\0", payload.len()).as_bytes());
    hasher.update(&payload);

    hex_encode(&hasher.finalize())
}

fn compute_skill_version_id(
    description: &str,
    body: &str,
    scripts: &[SkillScript],
    enable_coco_shim: bool,
) -> String {
    let payload = serde_json::to_vec(&SkillVersionHashPayload {
        description,
        body,
        scripts,
        enable_coco_shim,
    })
    .expect("skill version hash payload should serialize");

    let mut hasher = Sha256::new();
    hasher.update(format!("skill_version {}\0", payload.len()).as_bytes());
    hasher.update(&payload);

    hex_encode(&hasher.finalize())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }

    encoded
}

#[cfg(test)]
mod tests {
    use super::{
        Anchor, BackendMetadata, ExecutionMetadata, Kind, MergeParent, NewNode, Node, NodeMetadata,
        PauseReason, Preset, PromptAnchor, ProviderMetadata, Role, SessionAnchor,
        SessionAnchorPatch, SessionRole, SessionState, SkillInvocationAnchor, SkillInvocationMode,
        SkillResultAnchor, SkillScript, SkillVersion, SkillVersionSpec, Tool, ToolResult, ToolUse,
    };
    use jiff::Timestamp;
    use serde_json::json;

    fn fixed_timestamp() -> Timestamp {
        "2026-03-25T09:10:11Z".parse().unwrap()
    }

    fn make_text_node(parent: &str, text: &str, created_at: Timestamp) -> Node {
        Node::new(
            parent.to_owned(),
            Role::User,
            None,
            Kind::Text(text.to_owned()),
            created_at,
        )
    }

    fn make_failure_node(parent: &str, text: &str, created_at: Timestamp) -> Node {
        Node::new(
            parent.to_owned(),
            Role::System,
            None,
            Kind::Failure(text.to_owned()),
            created_at,
        )
    }

    fn make_session_anchor() -> SessionAnchor {
        SessionAnchor {
            role: SessionRole::Orchestrator,
            provider_profile: None,
            provider: Some("openai".to_owned()),
            model: "gpt-5.4".to_owned(),
            tools: vec![Tool {
                name: "search".to_owned(),
                description: "Search docs".to_owned(),
                input_schema: json!({"type": "object"}),
            }],
            system_prompt: "system".to_owned(),
            prompt: "prompt".to_owned(),
            temperature: Some(0.1),
            max_tokens: Some(128),
            additional_params: Some(json!({"service_tier": "default"})),
            enable_coco_shim: false,
            active_skill: None,
        }
    }

    fn make_prompt_anchor() -> PromptAnchor {
        PromptAnchor {
            prompt: "merge prompt".to_owned(),
        }
    }

    fn make_skill_invocation_anchor() -> SkillInvocationAnchor {
        SkillInvocationAnchor {
            skill_name: "find-skills".to_owned(),
            mode: SkillInvocationMode::Handoff {
                prompt: "find a skill for this task".to_owned(),
            },
        }
    }

    fn make_skill_result_anchor() -> SkillResultAnchor {
        SkillResultAnchor {
            skill_name: "find-skills".to_owned(),
            output: "child result".to_owned(),
        }
    }

    #[test]
    fn node_id_is_stable_for_same_payload() {
        let left = make_text_node("parent", "hello", fixed_timestamp());
        let right = make_text_node("parent", "hello", fixed_timestamp());

        assert_eq!(left.id, right.id);
    }

    #[test]
    fn node_id_changes_when_created_at_changes() {
        let left = make_text_node("parent", "hello", "2026-03-25T09:10:11Z".parse().unwrap());
        let right = make_text_node("parent", "hello", "2026-03-25T09:10:12Z".parse().unwrap());

        assert_ne!(left.id, right.id);
    }

    #[test]
    fn node_id_changes_when_anchor_variant_changes() {
        let left = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::session(vec![], make_session_anchor())),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::prompt(
                vec![MergeParent::merge("merge-a")],
                make_prompt_anchor(),
            )),
            fixed_timestamp(),
        );

        assert_ne!(left.id, right.id);
    }

    #[test]
    fn node_id_changes_when_prompt_anchor_merge_parents_change() {
        let left = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::prompt(
                vec![MergeParent::merge("merge-a")],
                make_prompt_anchor(),
            )),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::prompt(
                vec![MergeParent::merge("merge-b")],
                make_prompt_anchor(),
            )),
            fixed_timestamp(),
        );

        assert_ne!(left.id, right.id);
    }

    #[test]
    fn node_id_changes_when_session_anchor_merge_parents_change() {
        let left = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::session(
                vec![MergeParent::merge("merge-a")],
                make_session_anchor(),
            )),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::session(
                vec![MergeParent::merge("merge-b")],
                make_session_anchor(),
            )),
            fixed_timestamp(),
        );

        assert_ne!(left.id, right.id);
    }

    #[test]
    fn node_id_changes_when_skill_invocation_anchor_mode_changes() {
        let left = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::skill_invocation(
                vec![MergeParent::merge("merge-a")],
                make_skill_invocation_anchor(),
            )),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::skill_invocation(
                vec![MergeParent::merge("merge-a")],
                SkillInvocationAnchor {
                    mode: SkillInvocationMode::InheritContext,
                    ..make_skill_invocation_anchor()
                },
            )),
            fixed_timestamp(),
        );

        assert_ne!(left.id, right.id);
    }

    #[test]
    fn node_id_changes_when_skill_result_anchor_output_changes() {
        let left = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::skill_result(
                vec![MergeParent::merge("merge-a")],
                make_skill_result_anchor(),
            )),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::skill_result(
                vec![MergeParent::merge("merge-a")],
                SkillResultAnchor {
                    output: "different result".to_owned(),
                    ..make_skill_result_anchor()
                },
            )),
            fixed_timestamp(),
        );

        assert_ne!(left.id, right.id);
    }

    #[test]
    fn node_id_changes_when_skill_result_anchor_merge_parents_change() {
        let left = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::skill_result(
                vec![MergeParent::merge("merge-a")],
                make_skill_result_anchor(),
            )),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::skill_result(
                vec![MergeParent::merge("merge-b")],
                make_skill_result_anchor(),
            )),
            fixed_timestamp(),
        );

        assert_ne!(left.id, right.id);
    }

    #[test]
    fn node_id_changes_when_execution_metadata_changes() {
        let left = Node::new(
            "parent".to_owned(),
            Role::User,
            BackendMetadata::builder()
                .execution(&ExecutionMetadata::new("execution-1".to_owned()))
                .build(),
            Kind::Text("hello".to_owned()),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::User,
            BackendMetadata::builder()
                .execution(&ExecutionMetadata::new("execution-2".to_owned()))
                .build(),
            Kind::Text("hello".to_owned()),
            fixed_timestamp(),
        );

        assert_ne!(left.id, right.id);
    }

    #[test]
    fn node_id_changes_when_text_kind_changes_to_failure_kind() {
        let left = make_text_node("parent", "hello", fixed_timestamp());
        let right = make_failure_node("parent", "hello", fixed_timestamp());

        assert_ne!(left.id, right.id);
    }

    #[test]
    fn node_id_is_lower_hex_sha256() {
        let node = make_text_node("parent", "hello", fixed_timestamp());

        assert_eq!(node.id.len(), 64);
        assert!(node.id.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(node.id, node.id.to_ascii_lowercase());
    }

    #[test]
    fn node_round_trip_preserves_id_and_created_at() {
        let node = Node::new(
            "parent".to_owned(),
            Role::LLM,
            BackendMetadata::builder()
                .provider(&ProviderMetadata::new(Some("call-1".to_owned())))
                .build(),
            Kind::tool_result(ToolResult {
                id: "call-1".to_owned(),
                output: "ok".to_owned(),
            }),
            fixed_timestamp(),
        );

        let encoded = serde_json::to_string(&node).unwrap();
        let decoded: Node = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded.id, node.id);
        assert_eq!(decoded.parent, node.parent);
        assert_eq!(decoded.created_at, node.created_at);
    }

    #[test]
    fn skill_version_id_is_stable_for_same_content() {
        let spec = SkillVersionSpec {
            description: "description".to_owned(),
            body: "body".to_owned(),
            scripts: vec![SkillScript {
                path: "scripts/run.py".to_owned(),
                content: "print('ok')".to_owned(),
            }],
            enable_coco_shim: true,
        };
        let left = SkillVersion::new(1, spec.clone());
        let right = SkillVersion::new(2, spec);

        assert_eq!(left.id, right.id);
        assert!(left.id_matches_content());
    }

    #[test]
    fn skill_version_id_changes_when_content_changes() {
        let left = SkillVersion::new(
            1,
            SkillVersionSpec {
                description: "description".to_owned(),
                body: "body".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        );
        let right = SkillVersion::new(
            1,
            SkillVersionSpec {
                description: "description".to_owned(),
                body: "different".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        );

        assert_ne!(left.id, right.id);
    }

    #[test]
    fn skill_version_id_is_lower_hex_sha256() {
        let version = SkillVersion::new(
            1,
            SkillVersionSpec {
                description: "description".to_owned(),
                body: "body".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: false,
            },
        );

        assert_eq!(version.id.len(), 64);
        assert!(version.id.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(version.id, version.id.to_ascii_lowercase());
    }

    #[test]
    fn tool_use_payload_accepts_one_or_many_items() {
        let one: Kind = serde_json::from_value(serde_json::json!({
            "ToolUse": {
                "id": "tool-call-1",
                "name": "exec_command",
                "input": { "cmd": "pwd" }
            }
        }))
        .unwrap();
        let many: Kind = serde_json::from_value(serde_json::json!({
            "ToolUse": [
                {
                    "id": "tool-call-1",
                    "name": "exec_command",
                    "input": { "cmd": "pwd" }
                },
                {
                    "id": "tool-call-2",
                    "name": "exec_command",
                    "input": { "cmd": "ls" }
                }
            ]
        }))
        .unwrap();

        assert_eq!(one.as_tool_uses().unwrap().iter().count(), 1);
        assert_eq!(many.as_tool_uses().unwrap().iter().count(), 2);
        assert_eq!(many.as_tool_uses().unwrap().items()[0].id, "tool-call-1");
    }

    #[test]
    fn many_tool_node_metadata_round_trip_preserves_call_ids() {
        let node = Node::new(
            "parent".to_owned(),
            Role::LLM,
            Some(NodeMetadata::many(vec![
                BackendMetadata {
                    execution_id: Some("execution-1".to_owned()),
                    call_id: Some("call-1".to_owned()),
                },
                BackendMetadata {
                    execution_id: Some("execution-1".to_owned()),
                    call_id: Some("call-2".to_owned()),
                },
            ])),
            Kind::tool_uses(vec![
                ToolUse {
                    id: "tool-call-1".to_owned(),
                    name: "exec_command".to_owned(),
                    input: json!({"cmd": "pwd"}),
                },
                ToolUse {
                    id: "tool-call-2".to_owned(),
                    name: "exec_command".to_owned(),
                    input: json!({"cmd": "ls"}),
                },
            ]),
            fixed_timestamp(),
        );

        let encoded = serde_json::to_string(&node).unwrap();
        let decoded: Node = serde_json::from_str(&encoded).unwrap();
        let metadata = decoded.metadata.expect("expected node metadata");
        let items = metadata.items();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].call_id.as_deref(), Some("call-1"));
        assert_eq!(items[1].call_id.as_deref(), Some("call-2"));
    }

    #[test]
    fn failure_node_round_trip_preserves_message() {
        let node = make_failure_node("parent", "rate limited", fixed_timestamp());

        let encoded = serde_json::to_string(&node).unwrap();
        let decoded: Node = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded.id, node.id);
        assert!(matches!(
            decoded.kind,
            Kind::Failure(message) if message == "rate limited"
        ));
    }

    #[test]
    fn new_node_carries_only_unpersisted_fields() {
        let node = NewNode {
            parent: "parent".to_owned(),
            role: Role::System,
            metadata: BackendMetadata::builder()
                .execution(&ExecutionMetadata::new("execution-1".to_owned()))
                .build(),
            kind: Kind::Anchor(Anchor::prompt(
                vec![MergeParent::merge("merge-a"), MergeParent::merge("merge-b")],
                make_prompt_anchor(),
            )),
        };

        let encoded = serde_json::to_value(&node).unwrap();

        assert!(encoded.get("id").is_none());
        assert!(encoded.get("created_at").is_none());
    }

    #[test]
    fn session_anchor_round_trip_preserves_prompt() {
        let node = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::session(
                vec![MergeParent::merge("merge-a"), MergeParent::merge("merge-b")],
                make_session_anchor(),
            )),
            fixed_timestamp(),
        );

        let encoded = serde_json::to_string(&node).unwrap();
        let decoded: Node = serde_json::from_str(&encoded).unwrap();
        let Kind::Anchor(anchor) = decoded.kind else {
            panic!("expected anchor node");
        };
        let session_anchor = anchor.as_session().expect("expected session anchor");

        assert_eq!(session_anchor.prompt, "prompt");
        assert_eq!(session_anchor.role, SessionRole::Orchestrator);
        assert_eq!(anchor.merge_parent_node_ids(), ["merge-a", "merge-b"]);
    }

    #[test]
    fn prompt_anchor_round_trip_preserves_merge_parents() {
        let node = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::prompt(
                vec![MergeParent::merge("merge-a"), MergeParent::merge("merge-b")],
                make_prompt_anchor(),
            )),
            fixed_timestamp(),
        );

        let encoded = serde_json::to_string(&node).unwrap();
        let decoded: Node = serde_json::from_str(&encoded).unwrap();
        let Kind::Anchor(anchor) = decoded.kind else {
            panic!("expected anchor node");
        };
        let prompt_anchor = anchor.as_prompt().expect("expected prompt anchor");

        assert_eq!(anchor.merge_parent_node_ids(), ["merge-a", "merge-b"]);
        assert_eq!(prompt_anchor.prompt, "merge prompt");
    }

    #[test]
    fn prompt_anchor_round_trip_preserves_shadow_merge_parent_kind() {
        let node = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::prompt(
                vec![MergeParent::shadow("shadow-a")],
                make_prompt_anchor(),
            )),
            fixed_timestamp(),
        );

        let encoded = serde_json::to_string(&node).unwrap();
        let decoded: Node = serde_json::from_str(&encoded).unwrap();
        let Kind::Anchor(anchor) = decoded.kind else {
            panic!("expected anchor node");
        };

        assert_eq!(
            anchor.merge_parents(),
            [MergeParent::shadow("shadow-a")].as_slice()
        );
    }

    #[test]
    fn skill_result_anchor_round_trip_preserves_fields() {
        let node = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::skill_result(
                vec![MergeParent::merge("merge-a")],
                make_skill_result_anchor(),
            )),
            fixed_timestamp(),
        );

        let encoded = serde_json::to_string(&node).unwrap();
        let decoded: Node = serde_json::from_str(&encoded).unwrap();
        let Kind::Anchor(anchor) = decoded.kind else {
            panic!("expected anchor node");
        };
        let skill_result = anchor
            .as_skill_result()
            .expect("expected skill result anchor");

        assert_eq!(anchor.merge_parent_node_ids(), ["merge-a"]);
        assert_eq!(skill_result.skill_name, "find-skills");
        assert_eq!(skill_result.output, "child result");
    }

    #[test]
    fn skill_invocation_anchor_round_trip_preserves_fields() {
        let node = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::skill_invocation(
                vec![MergeParent::merge("merge-a")],
                make_skill_invocation_anchor(),
            )),
            fixed_timestamp(),
        );

        let encoded = serde_json::to_string(&node).unwrap();
        let decoded: Node = serde_json::from_str(&encoded).unwrap();
        let Kind::Anchor(anchor) = decoded.kind else {
            panic!("expected anchor node");
        };
        let invocation = anchor
            .as_skill_invocation()
            .expect("expected skill invocation anchor");

        assert_eq!(anchor.merge_parent_node_ids(), ["merge-a"]);
        assert_eq!(invocation.skill_name, "find-skills");
        assert_eq!(
            invocation.mode,
            SkillInvocationMode::Handoff {
                prompt: "find a skill for this task".to_owned()
            }
        );
    }

    #[test]
    fn node_metadata_round_trip_preserves_execution_id() {
        let node = Node::new(
            "parent".to_owned(),
            Role::User,
            BackendMetadata::builder()
                .execution(&ExecutionMetadata::new("execution-1".to_owned()))
                .provider(&ProviderMetadata::new(Some("call-1".to_owned())))
                .build(),
            Kind::Text("hello".to_owned()),
            fixed_timestamp(),
        );

        let encoded = serde_json::to_string(&node).unwrap();
        let decoded: Node = serde_json::from_str(&encoded).unwrap();
        let metadata = decoded
            .metadata
            .expect("expected node metadata")
            .as_one()
            .expect("expected single node metadata")
            .clone();

        assert_eq!(metadata.execution_id.as_deref(), Some("execution-1"));
        assert_eq!(metadata.call_id.as_deref(), Some("call-1"));
    }

    #[test]
    fn backend_metadata_builder_returns_none_when_empty() {
        assert_eq!(BackendMetadata::builder().build(), None);
    }

    #[test]
    fn session_anchor_patch_updates_selected_fields() {
        let updated = make_session_anchor().apply_patch(&SessionAnchorPatch {
            role: Some(SessionRole::Runner),
            provider_profile: Some(Some("anthropic-main".to_owned())),
            provider: Some(Some("anthropic".to_owned())),
            model: Some("claude-sonnet-4-20250514".to_owned()),
            tools: Some(vec![]),
            system_prompt: Some("new system".to_owned()),
            temperature: Some(None),
            max_tokens: Some(Some(256)),
            additional_params: Some(Some(json!({"service_tier": "priority"}))),
            enable_coco_shim: Some(true),
        });

        assert_eq!(updated.role, SessionRole::Runner);
        assert_eq!(updated.provider_profile.as_deref(), Some("anthropic-main"));
        assert_eq!(updated.provider.as_deref(), Some("anthropic"));
        assert_eq!(updated.model, "claude-sonnet-4-20250514");
        assert!(updated.tools.is_empty());
        assert_eq!(updated.system_prompt, "new system");
        assert_eq!(updated.prompt, "prompt");
        assert_eq!(updated.temperature, None);
        assert_eq!(updated.max_tokens, Some(256));
        assert!(updated.enable_coco_shim);
        assert_eq!(
            updated.additional_params,
            Some(json!({"service_tier": "priority"}))
        );
    }

    #[test]
    fn preset_applies_session_and_role_settings() {
        let updated = Preset {
            role: SessionRole::Runner,
            provider_profile: "anthropic-main".to_owned(),
            model: "claude-sonnet-4-20250514".to_owned(),
            tools: vec![],
            system_prompt: "new system".to_owned(),
            prompt: "new prompt".to_owned(),
            temperature: Some(0.2),
            max_tokens: Some(256),
            additional_params: Some(json!({"service_tier": "priority"})),
            enable_coco_shim: true,
        }
        .apply_to_session_anchor(&make_session_anchor());

        assert_eq!(updated.role, SessionRole::Runner);
        assert_eq!(updated.provider_profile.as_deref(), Some("anthropic-main"));
        assert_eq!(updated.provider, None);
        assert_eq!(updated.model, "claude-sonnet-4-20250514");
        assert!(updated.tools.is_empty());
        assert_eq!(updated.system_prompt, "new system");
        assert_eq!(updated.prompt, "new prompt");
        assert_eq!(updated.temperature, Some(0.2));
        assert_eq!(updated.max_tokens, Some(256));
        assert_eq!(
            updated.additional_params,
            Some(json!({"service_tier": "priority"}))
        );
        assert!(updated.enable_coco_shim);
    }

    #[test]
    fn session_state_round_trip_preserves_paused_reason() {
        let state = SessionState::Paused {
            target_branch: "base".to_owned(),
            reason: PauseReason::Merged {
                merged_anchor_id: "anchor-1".to_owned(),
            },
        };

        let encoded = serde_json::to_string(&state).unwrap();
        let decoded: SessionState = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, state);
    }

    #[test]
    fn session_role_parse_accepts_known_values() {
        assert_eq!(
            SessionRole::parse("orchestrator"),
            Some(SessionRole::Orchestrator)
        );
        assert_eq!(SessionRole::parse("runner"), Some(SessionRole::Runner));
        assert_eq!(SessionRole::parse("unknown"), None);
    }
}
