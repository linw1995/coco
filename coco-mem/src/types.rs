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
    pub metadata: Option<BackendMetadata>,
    pub kind: Kind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewNode {
    pub parent: String,
    pub role: Role,
    pub metadata: Option<BackendMetadata>,
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
    ToolUse(ToolUse),
    ToolResult(ToolResult),
    Text(String),
    Failure(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Anchor {
    pub merge_parents: Vec<String>,
    pub payload: AnchorPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AnchorPayload {
    Session(SessionAnchor),
    Prompt(PromptAnchor),
    SkillResult(SkillResultAnchor),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionAnchor {
    pub role: SessionRole,
    pub provider: String,
    pub model: String,
    pub tools: Vec<Tool>,
    pub system_prompt: String,
    pub prompt: String,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub additional_params: Option<Value>,
    #[serde(default)]
    pub enable_coco_shim: bool,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SessionAnchorPatch {
    pub role: Option<SessionRole>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub tools: Option<Vec<Tool>>,
    pub system_prompt: Option<String>,
    pub prompt: Option<String>,
    pub temperature: Option<Option<f64>>,
    pub max_tokens: Option<Option<u64>>,
    pub additional_params: Option<Option<Value>>,
    pub enable_coco_shim: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillVersionSpec {
    pub description: String,
    pub body: String,
    #[serde(default)]
    pub enable_coco_shim: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillUpdatePatch {
    pub description: Option<String>,
    pub body: Option<String>,
    pub enable_coco_shim: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillVersion {
    pub version: u64,
    pub created_at: Timestamp,
    pub description: String,
    pub body: String,
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
        self.description.is_none() && self.body.is_none() && self.enable_coco_shim.is_none()
    }
}

impl SkillVersion {
    pub fn new(version: u64, spec: SkillVersionSpec) -> Self {
        Self {
            version,
            created_at: Timestamp::now(),
            description: spec.description,
            body: spec.body,
            enable_coco_shim: spec.enable_coco_shim,
        }
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
        let next = SkillVersion {
            version: next_version,
            created_at: Timestamp::now(),
            description: patch.description.clone().unwrap_or(current.description),
            body: patch.body.clone().unwrap_or(current.body),
            enable_coco_shim: patch.enable_coco_shim.unwrap_or(current.enable_coco_shim),
        };

        self.current_version = next_version;
        self.versions.insert(next_version, next);
        self.current()
    }

    pub fn rollback(&mut self, target_version: u64) -> Option<&SkillVersion> {
        let target = self.versions.get(&target_version)?.clone();
        let next_version = self.versions.keys().next_back().copied().unwrap_or(0) + 1;
        let next = SkillVersion {
            version: next_version,
            created_at: Timestamp::now(),
            description: target.description,
            body: target.body,
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
                description: "Guide an orchestrator session through CoCo branch and prompt workflows."
                    .to_owned(),
                body: r#"
# CoCo Orchestrator Workflow

Use the injected `coco` command through `bash` whenever you need branch-aware session workflow control.

Useful commands:
- `coco session list`
- `coco session get --branch <branch>`
- `coco session show <ref>`
- `coco session fork --branch <branch> --from-ref <ref>`
- `coco session pr --branch <branch> --target-branch <branch>`
- `coco session feedback --branch <branch> --prompt "<text>"`
- `coco session merge --branch <branch> --target-branch <branch> --prompt "<text>"`
- `coco prompt --branch <branch> "<text>"`
- `coco prompt status --job <job>`
- `coco prompt branch-status --job <job> --branch <branch>`

Guidelines:
- Prefer `coco` over editing store files directly.
- Use orchestrator sessions for coordination, branching, and merge decisions.
- Hand off bounded implementation work to runner sessions when orchestration is no longer needed.
"#
                .trim()
                .to_owned(),
                enable_coco_shim: true,
            },
        ),
    );
    groups.runner.insert(
        "coco-runner".to_owned(),
        SkillRecord::new(
            "coco-runner",
            SkillVersionSpec {
                description: "Guide a runner session through the CoCo commands available in runner scope."
                    .to_owned(),
                body: r#"
# CoCo Runner Workflow

Use the injected `coco` command through `bash` for runner-safe visibility and status inspection.

Useful commands:
- `coco session list`
- `coco session get --branch <branch>`
- `coco session graph`
- `coco session show <ref>`
- `coco prompt status --job <job>`
- `coco prompt branch-status --job <job> --branch <branch>`

Guidelines:
- Runner-scoped `coco` is read-oriented and intentionally hides write entrypoints.
- Use runner sessions for isolated execution, inspection, and handoff preparation.
- If you need workflow mutations such as create, merge, feedback, or prompt submission, hand back to an orchestrator session.
"#
                .trim()
                .to_owned(),
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillResultAnchor {
    pub tool_id: String,
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

impl Anchor {
    pub fn session(merge_parents: Vec<String>, anchor: SessionAnchor) -> Self {
        Self {
            merge_parents,
            payload: AnchorPayload::Session(anchor),
        }
    }

    pub fn prompt(merge_parents: Vec<String>, anchor: PromptAnchor) -> Self {
        Self {
            merge_parents,
            payload: AnchorPayload::Prompt(anchor),
        }
    }

    pub fn skill_result(merge_parents: Vec<String>, anchor: SkillResultAnchor) -> Self {
        Self {
            merge_parents,
            payload: AnchorPayload::SkillResult(anchor),
        }
    }

    pub fn merge_parents(&self) -> &[String] {
        &self.merge_parents
    }

    pub fn as_session(&self) -> Option<&SessionAnchor> {
        match &self.payload {
            AnchorPayload::Session(anchor) => Some(anchor),
            AnchorPayload::Prompt(_) | AnchorPayload::SkillResult(_) => None,
        }
    }

    pub fn as_prompt(&self) -> Option<&PromptAnchor> {
        match &self.payload {
            AnchorPayload::Session(_) | AnchorPayload::SkillResult(_) => None,
            AnchorPayload::Prompt(anchor) => Some(anchor),
        }
    }

    pub fn as_skill_result(&self) -> Option<&SkillResultAnchor> {
        match &self.payload {
            AnchorPayload::Session(_) | AnchorPayload::Prompt(_) => None,
            AnchorPayload::SkillResult(anchor) => Some(anchor),
        }
    }
}

impl SessionAnchor {
    pub fn apply_patch(&self, patch: &SessionAnchorPatch) -> Self {
        Self {
            role: patch.role.unwrap_or(self.role),
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
            prompt: patch.prompt.clone().unwrap_or_else(|| self.prompt.clone()),
            temperature: patch.temperature.unwrap_or(self.temperature),
            max_tokens: patch.max_tokens.unwrap_or(self.max_tokens),
            additional_params: patch
                .additional_params
                .clone()
                .unwrap_or_else(|| self.additional_params.clone()),
            enable_coco_shim: patch.enable_coco_shim.unwrap_or(self.enable_coco_shim),
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
    ) -> Option<Self> {
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

    pub fn build(self) -> Option<BackendMetadata> {
        if self.execution_id.is_none() && self.call_id.is_none() {
            return None;
        }

        Some(BackendMetadata {
            execution_id: self.execution_id,
            call_id: self.call_id,
        })
    }
}

impl Node {
    pub fn new(
        parent: String,
        role: Role,
        metadata: Option<BackendMetadata>,
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

#[derive(Serialize)]
struct NodeHashPayload<'a> {
    parent: &'a str,
    role: &'a Role,
    metadata: Option<&'a BackendMetadata>,
    kind: &'a Kind,
    created_at: &'a Timestamp,
}

fn compute_node_id(
    parent: &str,
    role: &Role,
    metadata: Option<&BackendMetadata>,
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
        Anchor, BackendMetadata, ExecutionMetadata, Kind, NewNode, Node, PauseReason, PromptAnchor,
        ProviderMetadata, Role, SessionAnchor, SessionAnchorPatch, SessionRole, SessionState,
        SkillResultAnchor, Tool, ToolResult,
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
            provider: "openai".to_owned(),
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
        }
    }

    fn make_prompt_anchor() -> PromptAnchor {
        PromptAnchor {
            prompt: "merge prompt".to_owned(),
        }
    }

    fn make_skill_result_anchor() -> SkillResultAnchor {
        SkillResultAnchor {
            tool_id: "tool-call-1".to_owned(),
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
                vec!["merge-a".to_owned()],
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
                vec!["merge-a".to_owned()],
                make_prompt_anchor(),
            )),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::prompt(
                vec!["merge-b".to_owned()],
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
                vec!["merge-a".to_owned()],
                make_session_anchor(),
            )),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::session(
                vec!["merge-b".to_owned()],
                make_session_anchor(),
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
                vec!["merge-a".to_owned()],
                make_skill_result_anchor(),
            )),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::skill_result(
                vec!["merge-a".to_owned()],
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
                vec!["merge-a".to_owned()],
                make_skill_result_anchor(),
            )),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::skill_result(
                vec!["merge-b".to_owned()],
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
            Kind::ToolResult(ToolResult {
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
                vec!["merge-a".to_owned(), "merge-b".to_owned()],
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
                vec!["merge-a".to_owned(), "merge-b".to_owned()],
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
        assert_eq!(anchor.merge_parents(), ["merge-a", "merge-b"]);
    }

    #[test]
    fn prompt_anchor_round_trip_preserves_merge_parents() {
        let node = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::prompt(
                vec!["merge-a".to_owned(), "merge-b".to_owned()],
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

        assert_eq!(anchor.merge_parents, vec!["merge-a", "merge-b"]);
        assert_eq!(prompt_anchor.prompt, "merge prompt");
    }

    #[test]
    fn skill_result_anchor_round_trip_preserves_fields() {
        let node = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::skill_result(
                vec!["merge-a".to_owned()],
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

        assert_eq!(anchor.merge_parents, vec!["merge-a"]);
        assert_eq!(skill_result.tool_id, "tool-call-1");
        assert_eq!(skill_result.skill_name, "find-skills");
        assert_eq!(skill_result.output, "child result");
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
        let metadata = decoded.metadata.expect("expected node metadata");

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
            provider: Some("anthropic".to_owned()),
            model: Some("claude-sonnet-4-20250514".to_owned()),
            tools: Some(vec![]),
            system_prompt: Some("new system".to_owned()),
            prompt: Some("new prompt".to_owned()),
            temperature: Some(None),
            max_tokens: Some(Some(256)),
            additional_params: Some(Some(json!({"service_tier": "priority"}))),
            enable_coco_shim: Some(true),
        });

        assert_eq!(updated.role, SessionRole::Runner);
        assert_eq!(updated.provider, "anthropic");
        assert_eq!(updated.model, "claude-sonnet-4-20250514");
        assert!(updated.tools.is_empty());
        assert_eq!(updated.system_prompt, "new system");
        assert_eq!(updated.prompt, "new prompt");
        assert_eq!(updated.temperature, None);
        assert_eq!(updated.max_tokens, Some(256));
        assert!(updated.enable_coco_shim);
        assert_eq!(
            updated.additional_params,
            Some(json!({"service_tier": "priority"}))
        );
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
