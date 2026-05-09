mod runtime_bridge;
mod skill;
mod skill_tool;
mod tool_definition;
mod unified_exec_tool;

use std::collections::{BTreeMap, HashMap};
#[cfg(test)]
use std::ffi::OsStr;
use std::path::PathBuf;
use std::str::FromStr;
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use coco_mem::{
    Anchor, AnchorPayload, BackendMetadata, BranchStore, ExecutionMetadata, Kind, MemoryStore,
    MergeParent, NewNode, NodeStore, PauseReason, PromptAnchor, ProviderMetadata, Role,
    RuntimeStore, SessionAnchor, SessionRole, SessionState, SessionStore, SkillResultAnchor,
    StoreError, Tool, ToolResult, ToolUse,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::IntoError;
use snafu::prelude::*;
use tokio::sync::{Mutex, OwnedMutexGuard};

pub use skill::{
    ExecutorError, SearchSkillToolRequest, SkillResultEvent, SkillToolExecutionResult,
    SkillToolExecutor, SkillToolRequest, SkillToolRunResult, UseSkillToolRequest,
};

pub use coco_mem;
pub use coco_mem::SessionAnchorPatch as SessionConfigPatch;
pub use runtime_bridge::LlmRuntimeBridge;
pub use tool_definition::builtin_tool_definition;

pub const COCO_SESSION_BRANCH_ENV: &str = "COCO_BRANCH";
pub const COCO_SESSION_ROLE_ENV: &str = "COCO_SESSION_ROLE";
pub const COCO_STORE_PATH_ENV: &str = "COCO_STORE_PATH";
pub const COCO_CLI_RUNTIME_SOCKET_ENV: &str = "COCO_CLI_RUNTIME_SOCKET";
pub const COCO_COMMAND_SHIM_MODE_ENV: &str = "COCO_COMMAND_SHIM_MODE";
pub const COCO_PARENT_TOOL_USE_ID_ENV: &str = "COCO_PARENT_TOOL_USE_ID";
pub const COCO_SKILL_NAME_ENV: &str = "COCO_SKILL_NAME";
pub const COCO_SKILL_DIR_ENV: &str = "COCO_SKILL_DIR";
pub const COCO_SKILL_PERSIST_DIR_ENV: &str = "COCO_SKILL_PERSIST_DIR";
pub const COCO_SKILL_PERSIST_ROOT_ENV: &str = "COCO_SKILL_PERSIST_ROOT";

pub type CompletionMessage = rig::completion::message::Message;
pub type CompletionToolCall = rig::completion::message::ToolCall;
pub type CompletionToolDefinition = rig::completion::ToolDefinition;

#[derive(Debug, Clone, Copy)]
pub struct StepContext<'a> {
    pub session: &'a ResolvedSession,
    pub request: &'a ResolvedCompletionRequest,
    pub prompt: &'a CompletionMessage,
    pub history: &'a [CompletionMessage],
    pub tool_definitions: &'a [CompletionToolDefinition],
}

#[cfg(test)]
async fn with_process_env_async<T, F, Fut>(entries: &[(&str, Option<&OsStr>)], run: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let previous: Vec<_> = entries
        .iter()
        .map(|(name, _)| ((*name).to_owned(), std::env::var_os(name)))
        .collect();

    for (name, value) in entries {
        match value {
            Some(value) => unsafe { std::env::set_var(name, value) },
            None => unsafe { std::env::remove_var(name) },
        }
    }

    let output = run().await;

    for (name, value) in previous {
        match value {
            Some(value) => unsafe { std::env::set_var(name, value) },
            None => unsafe { std::env::remove_var(name) },
        }
    }

    output
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    OpenAi,
    Anthropic,
    ChatGpt,
}

impl Provider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Anthropic => "anthropic",
            Self::ChatGpt => "chatgpt",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "openai" => Ok(Self::OpenAi),
            "anthropic" => Ok(Self::Anthropic),
            "chatgpt" => Ok(Self::ChatGpt),
            _ => UnknownProviderSnafu {
                provider: value.to_owned(),
            }
            .fail(),
        }
    }
}

impl FromStr for Provider {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub branch: String,
    pub merge_parents: Vec<MergeParent>,
    pub provider_profile: Option<String>,
    pub role: SessionRole,
    pub provider: Provider,
    pub model: String,
    pub system_prompt: String,
    pub prompt: String,
    pub tools: Vec<Tool>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub additional_params: Option<Value>,
    pub enable_coco_shim: bool,
}

#[derive(Debug, Clone)]
pub struct PromptRequest {
    pub branch: String,
    pub prompt: String,
    pub merge_parents: Vec<MergeParent>,
    pub session_patch: Option<SessionConfigPatch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchSession {
    pub branch: String,
    pub anchor_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequest {
    pub branch: String,
    pub target_branch: String,
    pub base_head_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMerge {
    pub branch: String,
    pub target_branch: String,
    pub source_head_id: String,
    pub merged_anchor_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionFeedback {
    pub branch: String,
    pub target_branch: String,
    pub base_head_id: String,
    pub source_anchor_id: String,
    pub feedback_anchor_id: String,
}

#[derive(Debug, Clone)]
pub struct CompletionRequest {
    pub branch: String,
    pub origin: CompletionOrigin,
    pub input: CompletionInput,
    pub overrides: CompletionOverrides,
    pub active_skill_runtime: Option<ActiveSkillRuntimeContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum CompletionOrigin {
    #[default]
    BranchHead,
    Reference(String),
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum CompletionInput {
    #[default]
    Continue,
    Prompt {
        text: String,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<Box<SessionConfigPatch>>,
    },
}

#[derive(Debug, Clone, Default)]
pub struct CompletionOverrides {
    pub provider: Option<Provider>,
    pub model: Option<String>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub additional_params: Option<Value>,
}

fn completion_origin_kind(origin: &CompletionOrigin) -> &'static str {
    match origin {
        CompletionOrigin::BranchHead => "branch_head",
        CompletionOrigin::Reference(_) => "reference",
    }
}

fn completion_input_kind(input: &CompletionInput) -> &'static str {
    match input {
        CompletionInput::Continue => "continue",
        CompletionInput::Prompt { .. } => "prompt",
    }
}

fn completion_input_merge_parent_count(input: &CompletionInput) -> usize {
    match input {
        CompletionInput::Continue => 0,
        CompletionInput::Prompt { merge_parents, .. } => merge_parents.len(),
    }
}

fn completion_input_has_session_patch(input: &CompletionInput) -> bool {
    match input {
        CompletionInput::Continue => false,
        CompletionInput::Prompt { session_patch, .. } => session_patch.is_some(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionResult {
    pub branch: String,
    pub anchor_id: String,
    pub execution_id: String,
    pub response_node_id: String,
    pub branch_head: String,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct SessionModelConfig {
    pub role: SessionRole,
    pub provider_profile: Option<String>,
    pub provider: Provider,
    pub model: String,
    pub secrets: BTreeMap<String, String>,
    pub base_url: Option<String>,
    pub system_prompt: String,
    pub tools: Vec<Tool>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub additional_params: Option<Value>,
    pub enable_coco_shim: bool,
}

#[derive(Debug, Clone)]
pub struct ResolvedSession {
    pub branch: String,
    pub anchor_id: String,
    pub config: SessionModelConfig,
    pub provider_history: Vec<rig::completion::message::Message>,
    pub tool_runtime_env: ToolRuntimeEnv,
}

#[derive(Debug, Clone)]
pub struct ResolvedCompletionRequest {
    pub branch: String,
    pub provider: Provider,
    pub model: String,
    pub secrets: BTreeMap<String, String>,
    pub base_url: Option<String>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub additional_params: Option<Value>,
    runtime: RuntimeCapabilities,
    trace_node_appender: Option<TraceNodeAppenderHandle>,
}

#[derive(Debug, Clone)]
pub struct ProviderRuntimeConfig {
    pub provider: Provider,
    pub secrets: BTreeMap<String, String>,
    pub base_url: Option<String>,
    pub default_model: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ToolInvocationContext {
    pub(crate) persisted_tool_use_node_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ToolExecutionOutcome {
    ToolResult {
        provider_output: String,
    },
    SkillResult {
        skill_name: String,
        merge_parent: String,
        provider_output: String,
    },
}

impl ToolExecutionOutcome {
    pub fn tool_result(provider_output: impl Into<String>) -> Self {
        Self::ToolResult {
            provider_output: provider_output.into(),
        }
    }

    pub fn skill_result(
        skill_name: impl Into<String>,
        merge_parent: impl Into<String>,
        provider_output: impl Into<String>,
    ) -> Self {
        Self::SkillResult {
            skill_name: skill_name.into(),
            merge_parent: merge_parent.into(),
            provider_output: provider_output.into(),
        }
    }

    pub fn provider_output(&self) -> &str {
        match self {
            Self::ToolResult { provider_output } => provider_output,
            Self::SkillResult {
                provider_output, ..
            } => provider_output,
        }
    }

    pub fn into_backend_event(self, tool_id: String, call_id: Option<String>) -> BackendEvent {
        let event = match self {
            Self::ToolResult { provider_output } => BackendEventPayload::ToolResult(ToolResult {
                id: tool_id,
                call_id: call_id.clone(),
                output: provider_output,
            }),
            Self::SkillResult {
                skill_name,
                merge_parent,
                provider_output,
            } => BackendEventPayload::SkillResult(SkillResultEvent {
                tool_id,
                skill_name,
                merge_parent,
                output: provider_output,
            }),
        };

        BackendEvent::new(event).with_metadata(Some(ProviderMetadata::new(call_id)))
    }
}

#[derive(Debug)]
struct StoreNodeAppender<S> {
    store: S,
    head_id: StdMutex<String>,
}

#[derive(Clone)]
struct TraceNodeAppenderHandle {
    inner: Arc<dyn TraceNodeAppender>,
}

trait TraceNodeAppender: Send + Sync {
    /// Appends a store node to the current trace tail.
    fn append(
        &self,
        role: Role,
        metadata: Option<BackendMetadata>,
        kind: Kind,
    ) -> std::result::Result<String, BackendError>;
}

impl TraceNodeAppenderHandle {
    fn new(inner: Arc<dyn TraceNodeAppender>) -> TraceNodeAppenderHandle {
        Self { inner }
    }

    fn append(
        &self,
        role: Role,
        metadata: Option<BackendMetadata>,
        kind: Kind,
    ) -> std::result::Result<String, BackendError> {
        self.inner.append(role, metadata, kind)
    }
}

impl std::fmt::Debug for TraceNodeAppenderHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TraceNodeAppenderHandle(..)")
    }
}

impl<S> TraceNodeAppender for StoreNodeAppender<S>
where
    S: NodeStore + Send + Sync,
{
    fn append(
        &self,
        role: Role,
        metadata: Option<BackendMetadata>,
        kind: Kind,
    ) -> std::result::Result<String, BackendError> {
        let mut head_id = self
            .head_id
            .lock()
            .expect("trace node appender lock poisoned");
        let node_id = self
            .store
            .append(NewNode {
                parent: head_id.clone(),
                role,
                metadata,
                kind,
            })
            .map_err(|source| BackendError::failed(source.to_string()))?;
        *head_id = node_id.clone();
        Ok(node_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CocoCliRuntimeRequest {
    pub args: Vec<String>,
    pub stdin: Vec<u8>,
    pub branch_env: Option<String>,
    pub session_role: Option<SessionRole>,
    pub store_path_env: Option<String>,
    pub parent_tool_use_id_env: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CocoCliRuntimeResponse {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Snafu, Clone, PartialEq, Eq)]
pub enum UnifiedExecCliBridgeError {
    #[snafu(display("coco-cli runtime bridge is unavailable"))]
    Unavailable,
}

#[async_trait]
pub trait UnifiedExecCliBridge: Send + Sync {
    fn is_available(&self) -> bool {
        true
    }

    async fn execute_coco_cli(
        &self,
        request: CocoCliRuntimeRequest,
    ) -> std::result::Result<CocoCliRuntimeResponse, UnifiedExecCliBridgeError>;
}

#[derive(Debug)]
struct UnavailableUnifiedExecCliBridge;

#[async_trait]
impl UnifiedExecCliBridge for UnavailableUnifiedExecCliBridge {
    fn is_available(&self) -> bool {
        false
    }

    async fn execute_coco_cli(
        &self,
        _request: CocoCliRuntimeRequest,
    ) -> std::result::Result<CocoCliRuntimeResponse, UnifiedExecCliBridgeError> {
        Err(UnifiedExecCliBridgeError::Unavailable)
    }
}

#[derive(Clone)]
pub struct UnifiedExecCliBridgeHandle {
    inner: Arc<dyn UnifiedExecCliBridge>,
}

impl UnifiedExecCliBridgeHandle {
    pub fn new(inner: Arc<dyn UnifiedExecCliBridge>) -> Self {
        Self { inner }
    }

    pub fn unavailable() -> Self {
        Self::new(Arc::new(UnavailableUnifiedExecCliBridge))
    }

    pub fn is_available(&self) -> bool {
        self.inner.is_available()
    }

    pub async fn execute_coco_cli(
        &self,
        request: CocoCliRuntimeRequest,
    ) -> std::result::Result<CocoCliRuntimeResponse, UnifiedExecCliBridgeError> {
        self.inner.execute_coco_cli(request).await
    }
}

impl std::fmt::Debug for UnifiedExecCliBridgeHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("UnifiedExecCliBridgeHandle(..)")
    }
}

impl Default for UnifiedExecCliBridgeHandle {
    fn default() -> Self {
        Self::unavailable()
    }
}

#[derive(Debug)]
struct UnavailableSkillToolExecutor;

#[async_trait]
impl SkillToolExecutor for UnavailableSkillToolExecutor {
    async fn search_skill_tool(
        &self,
        _request: SearchSkillToolRequest,
    ) -> std::result::Result<String, ExecutorError> {
        Err(ExecutorError::Unavailable)
    }

    async fn execute_skill_tool(
        &self,
        _request: UseSkillToolRequest,
    ) -> std::result::Result<SkillToolExecutionResult, ExecutorError> {
        Err(ExecutorError::Unavailable)
    }
}

#[derive(Clone)]
pub struct SkillToolExecutorHandle {
    inner: Arc<dyn SkillToolExecutor>,
}

impl SkillToolExecutorHandle {
    pub fn new(inner: Arc<dyn SkillToolExecutor>) -> Self {
        Self { inner }
    }

    pub fn unavailable() -> Self {
        Self::new(Arc::new(UnavailableSkillToolExecutor))
    }

    pub async fn search_skill_tool(
        &self,
        request: SearchSkillToolRequest,
    ) -> std::result::Result<String, ExecutorError> {
        self.inner.search_skill_tool(request).await
    }

    pub async fn execute_skill_tool(
        &self,
        request: UseSkillToolRequest,
    ) -> std::result::Result<SkillToolExecutionResult, ExecutorError> {
        self.inner.execute_skill_tool(request).await
    }
}

impl std::fmt::Debug for SkillToolExecutorHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SkillToolExecutorHandle(..)")
    }
}

impl Default for SkillToolExecutorHandle {
    fn default() -> Self {
        Self::unavailable()
    }
}

#[derive(Clone)]
pub struct ActiveSkillRuntimeContext {
    pub name: String,
    pub directory: PathBuf,
    pub persistent_directory: PathBuf,
}

impl std::fmt::Debug for ActiveSkillRuntimeContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActiveSkillRuntimeContext")
            .field("name", &self.name)
            .field("directory", &self.directory)
            .field("persistent_directory", &self.persistent_directory)
            .finish()
    }
}

#[derive(Clone)]
pub struct ToolRuntimeEnv {
    pub session_branch: String,
    pub session_role: SessionRole,
    pub current_skill_name: Option<String>,
    pub active_skill: Option<ActiveSkillRuntimeContext>,
    pub store_path: Option<PathBuf>,
    pub enable_coco_shim: bool,
    pub cli_bridge: UnifiedExecCliBridgeHandle,
    pub skill_executor: SkillToolExecutorHandle,
}

impl std::fmt::Debug for ToolRuntimeEnv {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ToolRuntimeEnv")
            .field("session_branch", &self.session_branch)
            .field("session_role", &self.session_role)
            .field("current_skill_name", &self.current_skill_name)
            .field("active_skill", &self.active_skill)
            .field("store_path", &self.store_path)
            .field("enable_coco_shim", &self.enable_coco_shim)
            .field("cli_bridge", &self.cli_bridge)
            .field("skill_executor", &self.skill_executor)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum BackendEventPayload {
    AssistantText(String),
    ToolUse(ToolUse),
    ToolResult(ToolResult),
    SkillResult(SkillResultEvent),
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackendEvent {
    pub metadata: Option<ProviderMetadata>,
    pub event: BackendEventPayload,
}

impl BackendEvent {
    pub fn new(event: BackendEventPayload) -> Self {
        Self {
            metadata: None,
            event,
        }
    }

    pub fn with_metadata(mut self, metadata: Option<ProviderMetadata>) -> Self {
        self.metadata = metadata;
        self
    }
}

impl From<BackendEventPayload> for BackendEvent {
    fn from(event: BackendEventPayload) -> Self {
        Self::new(event)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackendStep {
    pub execution: ExecutionMetadata,
    pub events: Vec<BackendEvent>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackendRun {
    pub steps: Vec<BackendStep>,
    pub outcome: BackendOutcome,
    pub head: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackendTurn {
    pub message: CompletionMessage,
    pub events: Vec<BackendEvent>,
    pub tool_calls: Vec<CompletionToolCall>,
    pub final_text: Option<String>,
    pub trace_persisted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BackendOutcome {
    Succeeded { text: String },
    Failed { message: String },
}

impl BackendRun {
    pub fn succeeded_with_steps(text: impl Into<String>, steps: Vec<BackendStep>) -> Self {
        Self {
            steps,
            outcome: BackendOutcome::Succeeded { text: text.into() },
            head: None,
        }
    }

    pub fn succeeded(text: impl Into<String>, events: Vec<BackendEvent>) -> Self {
        Self::succeeded_with_steps(
            text,
            vec![BackendStep {
                execution: ExecutionMetadata::new(format!("execution-{}", nanoid::nanoid!())),
                events,
            }],
        )
    }

    pub fn failed_with_steps(message: impl Into<String>, steps: Vec<BackendStep>) -> Self {
        Self {
            steps,
            outcome: BackendOutcome::Failed {
                message: message.into(),
            },
            head: None,
        }
    }

    pub fn failed(message: impl Into<String>, events: Vec<BackendEvent>) -> Self {
        Self::failed_with_steps(
            message,
            vec![BackendStep {
                execution: ExecutionMetadata::new(format!("execution-{}", nanoid::nanoid!())),
                events,
            }],
        )
    }

    pub fn with_head(mut self, head: Option<String>) -> Self {
        self.head = head;
        self
    }
}

impl BackendTurn {
    #[cfg(test)]
    fn finished(text: impl Into<String>) -> Self {
        let text = text.into();
        Self {
            message: CompletionMessage::assistant(text.clone()),
            events: vec![BackendEventPayload::AssistantText(text.clone()).into()],
            tool_calls: vec![],
            final_text: Some(text),
            trace_persisted: false,
        }
    }

    fn from_assistant_choice(
        message_id: Option<String>,
        choice: rig::OneOrMany<rig::message::AssistantContent>,
    ) -> Self {
        Self {
            message: CompletionMessage::Assistant {
                id: message_id,
                content: choice.clone(),
            },
            events: backend_events_from_choice(&choice),
            tool_calls: tool_calls_from_choice(&choice),
            final_text: assistant_text_from_choice(&choice),
            trace_persisted: false,
        }
    }
}

#[derive(Debug, Snafu, Clone, PartialEq, Eq)]
pub enum BackendError {
    #[snafu(display("{message}"))]
    Failed { message: String },

    #[snafu(display("{message}"))]
    UnifiedExecTool { message: String },
}

impl BackendError {
    fn failed(message: impl Into<String>) -> Self {
        Self::Failed {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendFailureContext {
    pub branch: String,
    pub execution_id: String,
    pub error_node_id: String,
    pub retry_from_node_id: String,
}

#[async_trait]
pub trait CompletionBackend: Send + Sync {
    async fn step(&self, _ctx: StepContext<'_>) -> std::result::Result<BackendTurn, BackendError> {
        Err(BackendError::failed("backend step is not implemented"))
    }

    async fn complete(
        &self,
        session: ResolvedSession,
        request: ResolvedCompletionRequest,
    ) -> std::result::Result<BackendRun, BackendError> {
        CompletionRunner::new(session, request)
            .await?
            .run(self)
            .await
    }
}

type BranchLockTable = Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>;
type WorkflowLock = Arc<Mutex<()>>;

#[derive(Debug, Clone)]
pub struct RuntimeCapabilities {
    pub unified_exec_cli_bridge: UnifiedExecCliBridgeHandle,
    pub skill_tool_executor: SkillToolExecutorHandle,
    unified_exec_sessions: unified_exec_tool::UnifiedExecSessionStoreHandle,
}

impl Default for RuntimeCapabilities {
    fn default() -> Self {
        Self {
            unified_exec_cli_bridge: UnifiedExecCliBridgeHandle::default(),
            skill_tool_executor: SkillToolExecutorHandle::default(),
            unified_exec_sessions: unified_exec_tool::session_store(),
        }
    }
}

pub struct LlmService<B = RigBackend, S = MemoryStore> {
    store: S,
    backend: B,
    provider_configs: HashMap<String, ProviderRuntimeConfig>,
    runtime: RuntimeCapabilities,
    branch_locks: BranchLockTable,
    workflow_lock: WorkflowLock,
}

pub struct LlmServiceBuilder<B, S> {
    store: S,
    backend: B,
    provider_configs: HashMap<String, ProviderRuntimeConfig>,
    unified_exec_cli_bridge: Option<UnifiedExecCliBridgeHandle>,
    skill_tool_executor: Option<SkillToolExecutorHandle>,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Memory operation failed: {source}"))]
    Memory {
        #[snafu(source(from(coco_mem::StoreError, Box::new)))]
        source: Box<coco_mem::StoreError>,
    },

    #[snafu(display("Branch {branch:?} has no session anchor"))]
    MissingAnchor { branch: String },

    #[snafu(display("Anchor {anchor_id:?} is not a conversation anchor"))]
    InvalidAnchor { anchor_id: String },

    #[snafu(display("Unknown provider {provider:?}"))]
    UnknownProvider { provider: String },

    #[snafu(display("Provider profile {profile:?} is not configured"))]
    ProviderProfileNotConfigured { profile: String },

    #[snafu(display("Session {branch:?} is not attached to a target branch"))]
    SessionNotAttached { branch: String },

    #[snafu(display(
        "Session {branch:?} target branch mismatch: expected {expected:?}, got {actual:?}"
    ))]
    TargetBranchMismatch {
        branch: String,
        expected: String,
        actual: String,
    },

    #[snafu(display(
        "Feedback source {source_anchor_id:?} must not move behind base head {base_head_id:?} on target branch {target_branch:?}"
    ))]
    FeedbackSourceNotAhead {
        target_branch: String,
        base_head_id: String,
        source_anchor_id: String,
    },

    #[snafu(display("Backend call failed: {source}"))]
    Backend {
        source: BackendError,
        context: Box<BackendFailureContext>,
    },

    #[snafu(display("Failed to clean up temporary use_skill branch {branch:?}: {source}"))]
    UseSkillCleanup { branch: String, source: Box<Error> },

    #[snafu(display(
        "use_skill workflow failed and cleanup of temporary branch {branch:?} also failed: workflow={workflow}; cleanup={cleanup}"
    ))]
    UseSkillWorkflowFailedCleanup {
        branch: String,
        workflow: Box<Error>,
        cleanup: Box<Error>,
    },
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
struct ResolvedContext {
    active_anchor_id: String,
    session_anchor: SessionAnchor,
    nodes: Vec<coco_mem::Node>,
}

#[derive(Debug, Clone, Default)]
pub struct RigBackend;

impl LlmService<RigBackend, MemoryStore> {
    pub fn with_store(store: MemoryStore) -> Self {
        Self::new(store, RigBackend)
    }
}

impl<B, S> LlmServiceBuilder<B, S> {
    pub fn with_provider_configs(
        mut self,
        provider_configs: HashMap<String, ProviderRuntimeConfig>,
    ) -> Self {
        self.provider_configs = provider_configs;
        self
    }

    pub fn with_unified_exec_cli_bridge(mut self, bridge: UnifiedExecCliBridgeHandle) -> Self {
        self.unified_exec_cli_bridge = Some(bridge);
        self
    }

    pub fn with_skill_tool_executor(mut self, executor: Arc<dyn SkillToolExecutor>) -> Self {
        self.skill_tool_executor = Some(SkillToolExecutorHandle::new(executor));
        self
    }

    pub fn build(self) -> LlmService<B, S> {
        LlmService {
            store: self.store,
            backend: self.backend,
            provider_configs: self.provider_configs,
            runtime: RuntimeCapabilities {
                unified_exec_cli_bridge: self.unified_exec_cli_bridge.unwrap_or_default(),
                skill_tool_executor: self.skill_tool_executor.unwrap_or_default(),
                unified_exec_sessions: unified_exec_tool::session_store(),
            },
            branch_locks: Arc::new(Mutex::new(HashMap::new())),
            workflow_lock: Arc::new(Mutex::new(())),
        }
    }
}

impl<B, S> LlmService<B, S> {
    pub fn builder(store: S, backend: B) -> LlmServiceBuilder<B, S> {
        LlmServiceBuilder {
            store,
            backend,
            provider_configs: HashMap::new(),
            unified_exec_cli_bridge: None,
            skill_tool_executor: None,
        }
    }

    pub fn new(store: S, backend: B) -> Self {
        Self::builder(store, backend).build()
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    async fn lock_branch(&self, branch: &str) -> OwnedMutexGuard<()> {
        let branch_lock = {
            let mut locks = self.branch_locks.lock().await;
            locks
                .entry(branch.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };

        branch_lock.lock_owned().await
    }

    async fn lock_workflow(&self) -> OwnedMutexGuard<()> {
        self.workflow_lock.clone().lock_owned().await
    }

    async fn lock_branch_pair(&self, left: &str, right: &str) -> Vec<OwnedMutexGuard<()>> {
        let mut branches = vec![left.to_owned()];
        if left != right {
            branches.push(right.to_owned());
            branches.sort();
        }

        let mut guards = Vec::with_capacity(branches.len());
        for branch in branches {
            guards.push(self.lock_branch(&branch).await);
        }
        guards
    }

    pub async fn cleanup_runtime_processes(&self) -> usize {
        let cleaned_session_count = self.runtime.unified_exec_sessions.remove_all().await;
        tracing::info!(cleaned_session_count, "cleaned runtime processes");
        cleaned_session_count
    }
}

impl<B, S> LlmService<B, S>
where
    S: SessionStore,
{
    pub async fn rebase_session(&self, branch: &str, patch: SessionConfigPatch) -> Result<String> {
        let _guard = self.lock_branch(branch).await;
        let has_tool_patch = patch.tools.is_some();
        let has_model_patch = patch.model.is_some();
        let anchor_id = self
            .store
            .rebase_session(branch, &patch)
            .context(MemorySnafu)?;
        tracing::info!(
            branch = %branch,
            anchor_id = %anchor_id,
            has_tool_patch,
            has_model_patch,
            "rebased session"
        );
        Ok(anchor_id)
    }

    pub async fn rebase_session_system_prompt(
        &self,
        branch: &str,
        patch: SessionConfigPatch,
        system_prompt: &str,
    ) -> Result<String> {
        let _guard = self.lock_branch(branch).await;
        let has_tool_patch = patch.tools.is_some();
        let has_model_patch = patch.model.is_some();
        let anchor_id = self
            .store
            .rebase_session_system_prompt(branch, &patch, system_prompt)
            .context(MemorySnafu)?;
        tracing::info!(
            branch = %branch,
            anchor_id = %anchor_id,
            has_tool_patch,
            has_model_patch,
            "rebased session with system prompt"
        );
        Ok(anchor_id)
    }
}

impl<B, S> LlmService<B, S>
where
    S: NodeStore + BranchStore,
{
    pub async fn create_session(&self, config: SessionConfig) -> Result<BranchSession> {
        let _guard = self.lock_branch(&config.branch).await;
        let branch = config.branch.clone();
        let role = config.role;
        let configured_provider = config.provider;
        let model = config.model.clone();
        let logged_provider_profile = config.provider_profile.clone();
        let tool_count = config.tools.len();
        let merge_parent_count = config.merge_parents.len();
        let root_id = self.store.root_id();
        let provider_profile = config.provider_profile;
        let provider = provider_profile
            .is_none()
            .then(|| config.provider.as_str().to_owned());
        let merge_parents = normalize_merge_parents(config.merge_parents, &root_id);
        let anchor_id = self
            .store
            .append(NewNode {
                parent: root_id,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(
                    merge_parents,
                    SessionAnchor {
                        role: config.role,
                        provider_profile,
                        provider,
                        model: config.model,
                        tools: config.tools,
                        system_prompt: config.system_prompt,
                        prompt: config.prompt,
                        temperature: config.temperature,
                        max_tokens: config.max_tokens,
                        additional_params: config.additional_params,
                        enable_coco_shim: config.enable_coco_shim,
                        active_skill: None,
                    },
                )),
            })
            .context(MemorySnafu)?;
        self.store
            .fork(&config.branch, &anchor_id)
            .context(MemorySnafu)?;

        tracing::info!(
            branch = %branch,
            anchor_id = %anchor_id,
            role = ?role,
            provider = %configured_provider.as_str(),
            model = %model,
            provider_profile = ?logged_provider_profile,
            tool_count,
            merge_parent_count,
            "created session"
        );
        Ok(BranchSession {
            branch: config.branch,
            anchor_id,
        })
    }

    pub fn fork(&self, branch: impl Into<String>, from_ref: &str) -> Result<String> {
        let branch = branch.into();
        let anchor_id = self.store.fork(&branch, from_ref).context(MemorySnafu)?;
        tracing::info!(
            branch = %branch,
            from_ref = %from_ref,
            anchor_id = %anchor_id,
            "forked session branch"
        );
        Ok(anchor_id)
    }
}

impl<B, S> LlmService<B, S>
where
    S: BranchStore,
{
    pub async fn delete_session_branch(&self, branch: &str) -> Result<()> {
        let _guard = self.lock_branch(branch).await;
        self.store.delete_branch(branch).context(MemorySnafu)?;
        let cleaned_session_count = self
            .runtime
            .unified_exec_sessions
            .remove_branch(branch)
            .await;
        tracing::info!(branch, cleaned_session_count, "deleted session branch");
        Ok(())
    }
}

impl<B, S> LlmService<B, S>
where
    S: BranchStore + SessionStore,
{
    pub async fn open_pull_request(
        &self,
        branch: &str,
        target_branch: &str,
    ) -> Result<PullRequest> {
        let _workflow = self.lock_workflow().await;
        let _guards = self.lock_branch_pair(branch, target_branch).await;
        let base_head_id = self
            .store
            .get_branch_head(target_branch)
            .context(MemorySnafu)?;
        self.store
            .set_session_state(
                branch,
                None,
                SessionState::Attached {
                    target_branch: target_branch.to_owned(),
                    base_head_id: base_head_id.clone(),
                },
            )
            .context(MemorySnafu)?;

        tracing::info!(
            branch = %branch,
            target_branch = %target_branch,
            base_head_id = %base_head_id,
            "opened session pull request"
        );
        Ok(PullRequest {
            branch: branch.to_owned(),
            target_branch: target_branch.to_owned(),
            base_head_id,
        })
    }
}

impl<B, S> LlmService<B, S>
where
    B: CompletionBackend,
    S: NodeStore + BranchStore + SessionStore + RuntimeStore + Clone + Send + Sync + 'static,
{
    pub async fn prompt(&self, request: PromptRequest) -> Result<CompletionResult> {
        let _guard = self.lock_branch(&request.branch).await;
        tracing::info!(
            branch = %request.branch,
            merge_parent_count = request.merge_parents.len(),
            has_session_patch = request.session_patch.is_some(),
            "received prompt request"
        );
        self.run_locked(CompletionRequest {
            branch: request.branch,
            origin: CompletionOrigin::BranchHead,
            input: CompletionInput::Prompt {
                text: request.prompt,
                merge_parents: request.merge_parents,
                session_patch: request.session_patch.map(Box::new),
            },
            overrides: CompletionOverrides::default(),
            active_skill_runtime: None,
        })
        .await
    }

    pub async fn run(&self, request: CompletionRequest) -> Result<CompletionResult> {
        let _guard = self.lock_branch(&request.branch).await;
        tracing::debug!(
            branch = %request.branch,
            origin = completion_origin_kind(&request.origin),
            input = completion_input_kind(&request.input),
            "received completion request"
        );
        self.run_locked(request).await
    }

    async fn run_locked(&self, request: CompletionRequest) -> Result<CompletionResult> {
        let branch = request.branch.clone();
        let origin_kind = completion_origin_kind(&request.origin);
        let input_kind = completion_input_kind(&request.input);
        let merge_parent_count = completion_input_merge_parent_count(&request.input);
        let has_session_patch = completion_input_has_session_patch(&request.input);
        let original_head = self
            .store
            .get_branch_head(&request.branch)
            .context(MemorySnafu)?;
        let reference_id = match &request.origin {
            CompletionOrigin::BranchHead => original_head.clone(),
            CompletionOrigin::Reference(reference) => self.resolve_reference_id(reference)?,
        };
        let retry_from_node_id = match &request.input {
            CompletionInput::Continue => reference_id.clone(),
            CompletionInput::Prompt {
                text,
                merge_parents,
                session_patch,
            } => self.append_prompt_anchor_to_parent_with_session_patch(
                &reference_id,
                text,
                merge_parents,
                session_patch.as_deref(),
            )?,
        };
        let mut session =
            self.resolve_session_from_reference(&request.branch, &retry_from_node_id)?;
        if request.active_skill_runtime.is_some() {
            session.tool_runtime_env.active_skill = request.active_skill_runtime.clone();
        }
        tracing::info!(
            branch = %branch,
            original_head = %original_head,
            reference_id = %reference_id,
            retry_from_node_id = %retry_from_node_id,
            anchor_id = %session.anchor_id,
            origin = origin_kind,
            input = input_kind,
            merge_parent_count,
            has_session_patch,
            provider = %session.config.provider.as_str(),
            model = %session.config.model,
            tool_count = session.config.tools.len(),
            "starting completion"
        );
        let trace_node_appender = TraceNodeAppenderHandle::new(Arc::new(StoreNodeAppender {
            store: self.store.clone(),
            head_id: StdMutex::new(retry_from_node_id.clone()),
        }));
        let resolved = self.resolve_request(&session, request.clone(), Some(trace_node_appender));
        match self
            .backend
            .complete(session.clone(), resolved.clone())
            .await
        {
            Ok(run) => {
                let BackendRun {
                    steps,
                    outcome,
                    head,
                } = run;
                match outcome {
                    BackendOutcome::Succeeded { text } => {
                        let (last_execution_id, steps) =
                            normalize_backend_steps(steps, Some(text.clone()));
                        let response_node_id = match resolved.trace_node_appender.as_ref() {
                            Some(trace_node_appender) => match head.as_deref() {
                                Some(head_id) => self
                                    .validate_terminal_text_with_trace_node_appender(
                                        &resolved.branch,
                                        &last_execution_id,
                                        head_id,
                                        &text,
                                    )?,
                                None => self.persist_backend_steps_with_trace_node_appender(
                                    &resolved.branch,
                                    &retry_from_node_id,
                                    trace_node_appender,
                                    &steps,
                                )?,
                            },
                            None => self
                                .append_backend_steps(retry_from_node_id.clone(), &steps)
                                .context(MemorySnafu)?,
                        };
                        self.move_branch_head(&resolved.branch, &original_head, &response_node_id)?;

                        tracing::info!(
                            branch = %resolved.branch,
                            execution_id = %last_execution_id,
                            response_node_id = %response_node_id,
                            branch_head = %response_node_id,
                            step_count = steps.len(),
                            "completion succeeded"
                        );
                        Ok(CompletionResult {
                            branch: resolved.branch,
                            anchor_id: session.anchor_id,
                            execution_id: last_execution_id,
                            response_node_id: response_node_id.clone(),
                            branch_head: response_node_id,
                            text,
                        })
                    }
                    BackendOutcome::Failed { message } => {
                        let (execution_id, steps) = normalize_backend_steps(steps, None);
                        let error_node_id = match resolved.trace_node_appender.as_ref() {
                            Some(trace_node_appender) => {
                                head.clone().map_or_else(
                                    || {
                                        self.persist_backend_steps_with_trace_node_appender(
                                            &resolved.branch,
                                            &retry_from_node_id,
                                            trace_node_appender,
                                            &steps,
                                        )
                                    },
                                    Ok,
                                )?;
                                self.append_failure_with_trace_node_appender(
                                    &resolved.branch,
                                    &retry_from_node_id,
                                    trace_node_appender,
                                    &execution_id,
                                    &message,
                                )?
                            }
                            None => self.append_failure_node(
                                self.append_backend_steps(retry_from_node_id.clone(), &steps)
                                    .context(MemorySnafu)?,
                                &execution_id,
                                &message,
                            )?,
                        };
                        self.move_branch_head(
                            &resolved.branch,
                            &original_head,
                            &retry_from_node_id,
                        )?;
                        tracing::warn!(
                            branch = %resolved.branch,
                            execution_id = %execution_id,
                            error_node_id = %error_node_id,
                            retry_from_node_id = %retry_from_node_id,
                            step_count = steps.len(),
                            error = %message,
                            "completion failed with backend outcome"
                        );
                        Err(BackendSnafu {
                            context: Box::new(BackendFailureContext {
                                branch: resolved.branch,
                                execution_id,
                                error_node_id,
                                retry_from_node_id,
                            }),
                        }
                        .into_error(BackendError::failed(message)))
                    }
                }
            }
            Err(source) => {
                let execution_id = format!("execution-{}", nanoid::nanoid!());
                let error_node_id = match resolved.trace_node_appender.as_ref() {
                    Some(trace_node_appender) => self.append_failure_with_trace_node_appender(
                        &resolved.branch,
                        &retry_from_node_id,
                        trace_node_appender,
                        &execution_id,
                        &source.to_string(),
                    )?,
                    None => self.append_failure_node(
                        retry_from_node_id.clone(),
                        &execution_id,
                        &source.to_string(),
                    )?,
                };
                self.move_branch_head(&resolved.branch, &original_head, &retry_from_node_id)?;
                tracing::error!(
                    branch = %resolved.branch,
                    execution_id = %execution_id,
                    error_node_id = %error_node_id,
                    retry_from_node_id = %retry_from_node_id,
                    error = %source,
                    "completion backend returned error"
                );
                Err(BackendSnafu {
                    context: Box::new(BackendFailureContext {
                        branch: resolved.branch,
                        execution_id,
                        error_node_id,
                        retry_from_node_id,
                    }),
                }
                .into_error(source))
            }
        }
    }
}

impl<B, S> LlmService<B, S>
where
    S: NodeStore + RuntimeStore,
{
    fn resolve_session_from_reference(
        &self,
        branch: &str,
        reference: &str,
    ) -> Result<ResolvedSession> {
        let context = self.resolve_context(reference)?;
        self.session_from_context(branch, context)
    }
}

fn resolve_session_provider(
    anchor: &SessionAnchor,
    config: Option<&ProviderRuntimeConfig>,
) -> Result<Provider> {
    if let Some(config) = config {
        return Ok(config.provider);
    }

    let Some(provider) = anchor.provider.as_deref() else {
        return UnknownProviderSnafu {
            provider: "<missing>".to_owned(),
        }
        .fail();
    };
    Provider::parse(provider)
}

fn resolve_session_model(anchor: &SessionAnchor, config: Option<&ProviderRuntimeConfig>) -> String {
    if !anchor.model.is_empty() {
        return anchor.model.clone();
    }

    config
        .and_then(|config| config.default_model.clone())
        .unwrap_or_default()
}

impl<B, S> LlmService<B, S>
where
    S: NodeStore + BranchStore + RuntimeStore,
{
    pub fn append_prompt_job_base(
        &self,
        branch: &str,
        prompt: &str,
        merge_parents: &[MergeParent],
        session_patch: Option<&SessionConfigPatch>,
    ) -> Result<String> {
        let parent_id = self.store.get_branch_head(branch).context(MemorySnafu)?;
        self.append_prompt_anchor_to_parent_with_session_patch(
            &parent_id,
            prompt,
            merge_parents,
            session_patch,
        )
    }
}

impl<B, S> LlmService<B, S>
where
    S: NodeStore + BranchStore,
{
    fn append_prompt_anchor_to_branch(
        &self,
        branch: &str,
        prompt: &str,
        merge_parents: &[MergeParent],
    ) -> Result<String> {
        let original_head = self.store.get_branch_head(branch).context(MemorySnafu)?;
        let anchor_id =
            self.append_prompt_anchor_to_parent(&original_head, prompt, merge_parents)?;
        self.store
            .set_branch_head(branch, &original_head, &anchor_id)
            .context(MemorySnafu)?;

        Ok(anchor_id)
    }
}

impl<B, S> LlmService<B, S>
where
    S: NodeStore + RuntimeStore,
{
    fn append_prompt_anchor_to_parent_with_session_patch(
        &self,
        parent_id: &str,
        prompt: &str,
        merge_parents: &[MergeParent],
        session_patch: Option<&SessionConfigPatch>,
    ) -> Result<String> {
        let prompt_parent_id = match session_patch {
            Some(patch) => {
                self.resolve_context(parent_id)?;
                self.store
                    .append(NewNode {
                        parent: parent_id.to_owned(),
                        role: Role::System,
                        metadata: None,
                        kind: Kind::Anchor(Anchor::session_patch(vec![], patch.clone())),
                    })
                    .context(MemorySnafu)?
            }
            None => parent_id.to_owned(),
        };
        self.append_prompt_anchor_to_parent(&prompt_parent_id, prompt, merge_parents)
    }
}

impl<B, S> LlmService<B, S>
where
    S: NodeStore,
{
    fn append_prompt_anchor_to_parent(
        &self,
        parent_id: &str,
        prompt: &str,
        merge_parents: &[MergeParent],
    ) -> Result<String> {
        let mut normalized_parents = Vec::new();
        for merge_parent in merge_parents {
            let node_id = merge_parent.node_id();
            if node_id != parent_id
                && !normalized_parents
                    .iter()
                    .any(|parent: &MergeParent| parent.node_id() == node_id)
            {
                normalized_parents.push(merge_parent.clone());
            }
        }
        self.store
            .append(NewNode {
                parent: parent_id.to_owned(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    normalized_parents,
                    PromptAnchor {
                        prompt: prompt.to_owned(),
                    },
                )),
            })
            .context(MemorySnafu)
    }

    fn append_backend_events(
        &self,
        parent_id: String,
        execution: &ExecutionMetadata,
        events: &[BackendEvent],
    ) -> std::result::Result<String, StoreError> {
        let mut parent_id = parent_id;
        for event in events {
            let (role, metadata, kind) =
                persisted_node_from_backend_event(event.clone(), execution);
            parent_id = self.store.append(NewNode {
                parent: parent_id,
                role,
                metadata,
                kind,
            })?;
        }

        Ok(parent_id)
    }

    fn append_backend_steps(
        &self,
        mut parent_id: String,
        steps: &[BackendStep],
    ) -> std::result::Result<String, StoreError> {
        for step in steps {
            parent_id = self.append_backend_events(parent_id, &step.execution, &step.events)?;
        }

        Ok(parent_id)
    }

    fn persist_backend_steps_with_trace_node_appender(
        &self,
        branch: &str,
        retry_from_node_id: &str,
        trace_node_appender: &TraceNodeAppenderHandle,
        steps: &[BackendStep],
    ) -> Result<String> {
        let mut head_id = retry_from_node_id.to_owned();
        for step in steps {
            if step.events.is_empty() {
                continue;
            }
            head_id = append_backend_events(trace_node_appender, &step.execution, &step.events)
                .context(BackendSnafu {
                    context: Box::new(BackendFailureContext {
                        branch: branch.to_owned(),
                        execution_id: step.execution.execution_id.clone(),
                        error_node_id: retry_from_node_id.to_owned(),
                        retry_from_node_id: retry_from_node_id.to_owned(),
                    }),
                })?;
        }

        Ok(head_id)
    }

    fn append_failure_with_trace_node_appender(
        &self,
        branch: &str,
        retry_from_node_id: &str,
        trace_node_appender: &TraceNodeAppenderHandle,
        execution_id: &str,
        message: &str,
    ) -> Result<String> {
        trace_node_appender
            .append(
                Role::System,
                BackendMetadata::builder()
                    .execution(&ExecutionMetadata::new(execution_id.to_owned()))
                    .build(),
                Kind::Failure(message.to_owned()),
            )
            .context(BackendSnafu {
                context: Box::new(BackendFailureContext {
                    branch: branch.to_owned(),
                    execution_id: execution_id.to_owned(),
                    error_node_id: retry_from_node_id.to_owned(),
                    retry_from_node_id: retry_from_node_id.to_owned(),
                }),
            })
    }

    fn validate_terminal_text_with_trace_node_appender(
        &self,
        branch: &str,
        execution_id: &str,
        head: &str,
        text: &str,
    ) -> Result<String> {
        let node = self.store.get_node(head).context(MemorySnafu)?;
        if matches!(&node.kind, Kind::Text(last_text) if last_text == text) {
            return Ok(head.to_owned());
        }

        Err(BackendSnafu {
            context: Box::new(BackendFailureContext {
                branch: branch.to_owned(),
                execution_id: execution_id.to_owned(),
                error_node_id: head.to_owned(),
                retry_from_node_id: head.to_owned(),
            }),
        }
        .into_error(BackendError::failed(format!(
            "backend returned head {head:?} without terminal assistant text {text:?}"
        ))))
    }

    fn append_failure_node(
        &self,
        parent_id: String,
        execution_id: &str,
        message: &str,
    ) -> Result<String> {
        self.store
            .append(NewNode {
                parent: parent_id,
                role: Role::System,
                metadata: BackendMetadata::builder()
                    .execution(&ExecutionMetadata::new(execution_id.to_owned()))
                    .build(),
                kind: Kind::Failure(message.to_owned()),
            })
            .context(MemorySnafu)
    }
}

impl<B, S> LlmService<B, S>
where
    S: BranchStore,
{
    fn move_branch_head(&self, branch: &str, current_head: &str, next_head: &str) -> Result<()> {
        if current_head == next_head {
            return Ok(());
        }

        self.store
            .set_branch_head(branch, current_head, next_head)
            .context(MemorySnafu)
    }
}

impl<B, S> LlmService<B, S>
where
    S: NodeStore + BranchStore + SessionStore,
{
    pub async fn merge_session(
        &self,
        branch: &str,
        target_branch: Option<&str>,
        prompt: &str,
    ) -> Result<SessionMerge> {
        let _workflow = self.lock_workflow().await;
        let _branch_guard = self.lock_branch(branch).await;
        let resolved_target_branch = self.resolve_target_branch(branch, target_branch)?;
        let _target_guard = if resolved_target_branch == branch {
            None
        } else {
            Some(self.lock_branch(&resolved_target_branch).await)
        };

        let source_head_id = self.store.get_branch_head(branch).context(MemorySnafu)?;
        let merge_parents = vec![MergeParent::merge(source_head_id.clone())];
        let merged_anchor_id =
            self.append_prompt_anchor_to_branch(&resolved_target_branch, prompt, &merge_parents)?;
        self.store
            .set_session_state(
                branch,
                None,
                SessionState::Paused {
                    target_branch: resolved_target_branch.clone(),
                    reason: PauseReason::Merged {
                        merged_anchor_id: merged_anchor_id.clone(),
                    },
                },
            )
            .context(MemorySnafu)?;

        tracing::info!(
            branch = %branch,
            target_branch = %resolved_target_branch,
            source_head_id = %source_head_id,
            merged_anchor_id = %merged_anchor_id,
            "merged session"
        );
        Ok(SessionMerge {
            branch: branch.to_owned(),
            target_branch: resolved_target_branch,
            source_head_id,
            merged_anchor_id,
        })
    }

    pub async fn apply_feedback(
        &self,
        branch: &str,
        prompt: &str,
        from_ref: Option<&str>,
    ) -> Result<SessionFeedback> {
        let _workflow = self.lock_workflow().await;
        let _branch_guard = self.lock_branch(branch).await;
        let (target_branch, base_head_id) = self.attached_state(branch)?;
        let _target_guard = if target_branch == branch {
            None
        } else {
            Some(self.lock_branch(&target_branch).await)
        };

        let source_anchor_id = self.resolve_reference_id(from_ref.unwrap_or(&target_branch))?;
        self.ensure_ref_visible_on_branch(&target_branch, &source_anchor_id)?;
        if source_anchor_id != base_head_id {
            match self.store.log(&base_head_id, &source_anchor_id) {
                Ok(_) => {}
                Err(StoreError::RefsNotConnected { .. }) => {
                    return FeedbackSourceNotAheadSnafu {
                        target_branch,
                        base_head_id,
                        source_anchor_id,
                    }
                    .fail();
                }
                Err(source) => {
                    return Err(Error::Memory {
                        source: Box::new(source),
                    });
                }
            }
        }

        let merge_parents = vec![MergeParent::merge(source_anchor_id.clone())];
        let feedback_anchor_id =
            self.append_prompt_anchor_to_branch(branch, prompt, &merge_parents)?;
        self.store
            .set_session_state(
                branch,
                None,
                SessionState::Attached {
                    target_branch: target_branch.clone(),
                    base_head_id: source_anchor_id.clone(),
                },
            )
            .context(MemorySnafu)?;

        tracing::info!(
            branch = %branch,
            target_branch = %target_branch,
            source_anchor_id = %source_anchor_id,
            feedback_anchor_id = %feedback_anchor_id,
            "applied session feedback"
        );
        Ok(SessionFeedback {
            branch: branch.to_owned(),
            target_branch,
            base_head_id: source_anchor_id.clone(),
            source_anchor_id,
            feedback_anchor_id,
        })
    }
}

impl<B, S> LlmService<B, S>
where
    S: NodeStore + RuntimeStore,
{
    #[cfg(test)]
    fn resolve_session(&self, branch: &str) -> Result<ResolvedSession> {
        let context = self.resolve_context(branch)?;
        self.session_from_context(branch, context)
    }
}

fn normalize_merge_parents(
    merge_parents: Vec<MergeParent>,
    primary_parent: &str,
) -> Vec<MergeParent> {
    let mut normalized = Vec::new();
    for merge_parent in merge_parents {
        let node_id = merge_parent.node_id();
        if node_id != primary_parent
            && !normalized
                .iter()
                .any(|parent: &MergeParent| parent.node_id() == node_id)
        {
            normalized.push(merge_parent);
        }
    }
    normalized
}

impl<B, S> LlmService<B, S>
where
    S: RuntimeStore,
{
    fn session_from_context(
        &self,
        branch: &str,
        context: ResolvedContext,
    ) -> Result<ResolvedSession> {
        let provider_history = rig_messages_from_nodes(&context.nodes);

        let provider_config = context
            .session_anchor
            .provider_profile
            .as_deref()
            .map(|name| {
                self.provider_configs
                    .get(name)
                    .ok_or_else(|| Error::ProviderProfileNotConfigured {
                        profile: name.to_owned(),
                    })
            })
            .transpose()?;
        let provider = resolve_session_provider(&context.session_anchor, provider_config)?;
        let model = resolve_session_model(&context.session_anchor, provider_config);
        let secrets = provider_config
            .map(|config| config.secrets.clone())
            .unwrap_or_default();
        let base_url = provider_config.and_then(|config| config.base_url.clone());

        Ok(ResolvedSession {
            branch: branch.to_owned(),
            anchor_id: context.active_anchor_id,
            config: SessionModelConfig {
                role: context.session_anchor.role,
                provider_profile: context.session_anchor.provider_profile.clone(),
                provider,
                model,
                secrets,
                base_url,
                system_prompt: context.session_anchor.system_prompt.clone(),
                tools: context.session_anchor.tools.clone(),
                temperature: context.session_anchor.temperature,
                max_tokens: context.session_anchor.max_tokens,
                additional_params: context.session_anchor.additional_params.clone(),
                enable_coco_shim: context.session_anchor.enable_coco_shim,
            },
            provider_history,
            tool_runtime_env: ToolRuntimeEnv {
                session_branch: branch.to_owned(),
                session_role: context.session_anchor.role,
                current_skill_name: current_skill_name_from_prompt(&context.session_anchor.prompt),
                active_skill: None,
                store_path: self.store.runtime_store_path(),
                enable_coco_shim: context.session_anchor.enable_coco_shim,
                cli_bridge: self.runtime.unified_exec_cli_bridge.clone(),
                skill_executor: self.runtime.skill_tool_executor.clone(),
            },
        })
    }
}

impl<B, S> LlmService<B, S>
where
    S: NodeStore,
{
    fn resolve_context(&self, reference: &str) -> Result<ResolvedContext> {
        let mut ordered = Vec::new();
        for node in self.store.ancestry(reference).context(MemorySnafu)? {
            let is_context_start = is_provider_context_start(&node);
            ordered.push(node);
            if is_context_start {
                break;
            }
        }
        ordered.reverse();

        let mut state: Option<ResolvedContext> = None;

        for index in 0..ordered.len() {
            let node = &ordered[index];
            let next = ordered.get(index + 1);
            if should_skip_inherited_use_skill_tool_use(node, next) {
                continue;
            }
            self.apply_node_to_context(reference, &mut state, node)?;
        }

        state.context(MissingAnchorSnafu {
            branch: reference.to_owned(),
        })
    }

    fn apply_node_to_context(
        &self,
        reference: &str,
        state: &mut Option<ResolvedContext>,
        node: &coco_mem::Node,
    ) -> Result<()> {
        match &node.kind {
            Kind::Anchor(anchor) => match &anchor.payload {
                AnchorPayload::Session(session_anchor) => {
                    if let Some(context) = state.as_mut() {
                        context.session_anchor = session_anchor.as_ref().clone();
                        context.nodes.push(node.clone());
                        context.active_anchor_id = node.id.clone();
                    } else {
                        *state = Some(ResolvedContext {
                            active_anchor_id: node.id.clone(),
                            session_anchor: session_anchor.as_ref().clone(),
                            nodes: vec![node.clone()],
                        });
                    }
                }
                AnchorPayload::SessionPatch(patch) => {
                    let Some(context) = state.as_mut() else {
                        return MissingAnchorSnafu {
                            branch: reference.to_owned(),
                        }
                        .fail();
                    };

                    context.session_anchor = context.session_anchor.apply_patch(patch);
                    context.nodes.push(node.clone());
                    context.active_anchor_id = node.id.clone();
                }
                AnchorPayload::Prompt(_) => {
                    let Some(context) = state.as_mut() else {
                        return MissingAnchorSnafu {
                            branch: reference.to_owned(),
                        }
                        .fail();
                    };

                    context.nodes.push(node.clone());
                    context.active_anchor_id = node.id.clone();
                }
                AnchorPayload::SkillResult(_) => {
                    let Some(context) = state.as_mut() else {
                        return MissingAnchorSnafu {
                            branch: reference.to_owned(),
                        }
                        .fail();
                    };

                    context.nodes.push(node.clone());
                    context.active_anchor_id = node.id.clone();
                }
            },
            Kind::Text(_) => {
                let Some(context) = state.as_mut() else {
                    return Ok(());
                };

                context.nodes.push(node.clone());
            }
            Kind::ToolUse(_) => {
                let Some(context) = state.as_mut() else {
                    return Ok(());
                };
                context.nodes.push(node.clone());
            }
            Kind::ToolResult(_) => {
                let Some(context) = state.as_mut() else {
                    return Ok(());
                };
                context.nodes.push(node.clone());
            }
            Kind::Failure(_) => {
                let Some(context) = state.as_mut() else {
                    return Ok(());
                };
                context.nodes.push(node.clone());
            }
        }

        Ok(())
    }
}

fn should_skip_inherited_use_skill_tool_use(
    node: &coco_mem::Node,
    next: Option<&coco_mem::Node>,
) -> bool {
    is_use_skill_tool_use(node) && next.is_some_and(is_skill_execution_anchor)
}

fn is_provider_context_start(node: &coco_mem::Node) -> bool {
    matches!(
        &node.kind,
        Kind::Anchor(anchor)
            if anchor.as_session().is_some_and(is_context_start_session)
    )
}

fn is_context_start_session(session: &SessionAnchor) -> bool {
    !is_skill_execution_prompt(&session.prompt)
        || session
            .active_skill
            .as_ref()
            .is_some_and(|active_skill| active_skill.handoff.is_some())
}

fn is_use_skill_tool_use(node: &coco_mem::Node) -> bool {
    node.kind.as_tool_uses().is_some_and(|tool_uses| {
        tool_uses
            .iter()
            .any(|tool_use| tool_use.name == "use_skill")
    })
}

fn is_skill_execution_anchor(node: &coco_mem::Node) -> bool {
    match &node.kind {
        Kind::Anchor(anchor) => {
            anchor
                .as_prompt()
                .is_some_and(|prompt| is_skill_execution_prompt(&prompt.prompt))
                || anchor
                    .as_session()
                    .is_some_and(|session| is_skill_execution_prompt(&session.prompt))
        }
        _ => false,
    }
}

fn is_skill_execution_prompt(prompt: &str) -> bool {
    current_skill_name_from_prompt(prompt).is_some()
}

fn current_skill_name_from_prompt(prompt: &str) -> Option<String> {
    prompt
        .strip_prefix("You are executing the skill `")
        .and_then(|rest| rest.split_once('`'))
        .map(|(name, _)| name.to_owned())
        .filter(|name| !name.is_empty())
}

impl<B, S> LlmService<B, S> {
    fn resolve_request(
        &self,
        session: &ResolvedSession,
        request: CompletionRequest,
        trace_node_appender: Option<TraceNodeAppenderHandle>,
    ) -> ResolvedCompletionRequest {
        let provider = request
            .overrides
            .provider
            .unwrap_or(session.config.provider);
        let uses_session_provider = provider == session.config.provider;
        ResolvedCompletionRequest {
            branch: request.branch,
            provider,
            model: request
                .overrides
                .model
                .unwrap_or_else(|| session.config.model.clone()),
            secrets: if uses_session_provider {
                session.config.secrets.clone()
            } else {
                BTreeMap::new()
            },
            base_url: uses_session_provider
                .then(|| session.config.base_url.clone())
                .flatten(),
            temperature: request.overrides.temperature.or(session.config.temperature),
            max_tokens: request.overrides.max_tokens.or(session.config.max_tokens),
            additional_params: request
                .overrides
                .additional_params
                .or_else(|| session.config.additional_params.clone()),
            runtime: self.runtime.clone(),
            trace_node_appender,
        }
    }
}

impl<B, S> LlmService<B, S>
where
    S: SessionStore,
{
    fn resolve_target_branch(&self, branch: &str, explicit_target: Option<&str>) -> Result<String> {
        let state = self.store.get_session_state(branch).context(MemorySnafu)?;

        if let Some(target_branch) = explicit_target {
            match state {
                SessionState::Attached {
                    target_branch: expected,
                    ..
                }
                | SessionState::Paused {
                    target_branch: expected,
                    ..
                } if !expected.is_empty() && expected != target_branch => {
                    return TargetBranchMismatchSnafu {
                        branch: branch.to_owned(),
                        expected,
                        actual: target_branch.to_owned(),
                    }
                    .fail();
                }
                _ => return Ok(target_branch.to_owned()),
            }
        }

        match state {
            SessionState::Attached { target_branch, .. }
            | SessionState::Paused { target_branch, .. }
                if !target_branch.is_empty() =>
            {
                Ok(target_branch)
            }
            SessionState::Active | SessionState::Attached { .. } | SessionState::Paused { .. } => {
                SessionNotAttachedSnafu {
                    branch: branch.to_owned(),
                }
                .fail()
            }
        }
    }

    fn attached_state(&self, branch: &str) -> Result<(String, String)> {
        match self.store.get_session_state(branch).context(MemorySnafu)? {
            SessionState::Attached {
                target_branch,
                base_head_id,
            } => Ok((target_branch, base_head_id)),
            SessionState::Active | SessionState::Paused { .. } => SessionNotAttachedSnafu {
                branch: branch.to_owned(),
            }
            .fail(),
        }
    }
}

impl<B, S> LlmService<B, S>
where
    S: NodeStore,
{
    fn resolve_reference_id(&self, reference: &str) -> Result<String> {
        self.store
            .ancestry(reference)
            .context(MemorySnafu)
            .map(|nodes| {
                nodes
                    .into_iter()
                    .next()
                    .expect("ancestry should always include the head node")
                    .id
            })
    }

    fn ensure_ref_visible_on_branch(&self, branch: &str, node_id: &str) -> Result<()> {
        self.store
            .log(node_id, branch)
            .context(MemorySnafu)
            .map(|_| ())
    }
}

const DEFAULT_AGENT_MAX_TURNS: usize = 100;

struct RuntimeToolSet {
    definitions: Vec<CompletionToolDefinition>,
    tools: HashMap<String, RuntimeTool>,
}

enum RuntimeTool {
    ExecCommand(unified_exec_tool::UnifiedExecToolRuntime),
    WriteStdin(unified_exec_tool::UnifiedExecToolRuntime),
    SearchSkill(skill_tool::SkillToolRuntime),
    UseSkill(skill_tool::SkillToolRuntime),
}

impl RuntimeTool {
    fn definition(&self) -> CompletionToolDefinition {
        match self {
            Self::ExecCommand(tool) | Self::WriteStdin(tool) => tool.tool_definition(),
            Self::SearchSkill(tool) | Self::UseSkill(tool) => tool.tool_definition(),
        }
    }

    async fn execute(
        &self,
        args: String,
        invocation: ToolInvocationContext,
    ) -> std::result::Result<ToolExecutionOutcome, rig::tool::ToolError> {
        match self {
            Self::ExecCommand(tool) | Self::WriteStdin(tool) => {
                tool.execute(args, invocation).await
            }
            Self::SearchSkill(tool) | Self::UseSkill(tool) => tool.execute(args, invocation).await,
        }
    }
}

impl RuntimeToolSet {
    fn definition_list(&self) -> Vec<CompletionToolDefinition> {
        self.definitions.clone()
    }

    async fn execute(
        &self,
        tool_name: &str,
        args: String,
        invocation: ToolInvocationContext,
    ) -> std::result::Result<ToolExecutionOutcome, BackendError> {
        let tool = self.tools.get(tool_name).ok_or_else(|| {
            BackendError::failed(format!("unsupported runtime tool {tool_name:?}"))
        })?;
        match tool.execute(args, invocation).await {
            Ok(outcome) => Ok(outcome),
            Err(source) => Ok(ToolExecutionOutcome::tool_result(source.to_string())),
        }
    }
}

fn build_runtime_tools(
    session: &ResolvedSession,
    workspace_root: std::path::PathBuf,
    exec_sessions: unified_exec_tool::UnifiedExecSessionStoreHandle,
) -> std::result::Result<RuntimeToolSet, BackendError> {
    let mut definitions = Vec::with_capacity(session.config.tools.len());
    let mut tools = HashMap::with_capacity(session.config.tools.len());

    for tool in &session.config.tools {
        let runtime_tool = match tool.name.as_str() {
            "exec_command" => RuntimeTool::ExecCommand(unified_exec_tool::runtime_with_sessions(
                tool.clone(),
                workspace_root.clone(),
                session.tool_runtime_env.clone(),
                unified_exec_tool::UnifiedExecToolKind::ExecCommand,
                exec_sessions.clone(),
            )),
            "write_stdin" => RuntimeTool::WriteStdin(unified_exec_tool::runtime_with_sessions(
                tool.clone(),
                workspace_root.clone(),
                session.tool_runtime_env.clone(),
                unified_exec_tool::UnifiedExecToolKind::WriteStdin,
                exec_sessions.clone(),
            )),
            "search_skill" => RuntimeTool::SearchSkill(skill_tool::search_runtime(
                tool.clone(),
                workspace_root.clone(),
                session.tool_runtime_env.clone(),
            )),
            "use_skill" => RuntimeTool::UseSkill(skill_tool::run_runtime(
                tool.clone(),
                workspace_root.clone(),
                session.tool_runtime_env.clone(),
            )),
            other => {
                return Err(BackendError::failed(format!(
                    "unsupported tool {other:?}; only \"exec_command\", \"write_stdin\", \"search_skill\", and \"use_skill\" are implemented"
                )));
            }
        };
        definitions.push(runtime_tool.definition());
        tools.insert(tool.name.clone(), runtime_tool);
    }

    Ok(RuntimeToolSet { definitions, tools })
}

fn build_runtime_tools_for_session(
    session: &ResolvedSession,
    runtime: &RuntimeCapabilities,
) -> std::result::Result<RuntimeToolSet, BackendError> {
    let workspace_root = unified_exec_tool::resolve_workspace_root().map_err(|source| {
        BackendError::UnifiedExecTool {
            message: source.to_string(),
        }
    })?;
    build_runtime_tools(
        session,
        workspace_root,
        runtime.unified_exec_sessions.clone(),
    )
}

fn configure_completion_request_builder<M>(
    mut builder: rig::completion::CompletionRequestBuilder<M>,
    session: &ResolvedSession,
    request: &ResolvedCompletionRequest,
    tool_definitions: &[CompletionToolDefinition],
) -> rig::completion::CompletionRequestBuilder<M>
where
    M: rig::completion::CompletionModel,
{
    if !session.config.system_prompt.is_empty() {
        builder = builder.preamble(session.config.system_prompt.clone());
    }
    if !tool_definitions.is_empty() {
        builder = builder.tools(tool_definitions.to_vec());
    }
    if let Some(temperature) = request.temperature {
        builder = builder.temperature(temperature);
    }
    if let Some(max_tokens) = request.max_tokens {
        builder = builder.max_tokens(max_tokens);
    }
    if let Some(additional_params) = request.additional_params.clone() {
        builder = builder.additional_params(additional_params);
    }
    builder
}

fn assistant_text_from_choice(
    choice: &rig::OneOrMany<rig::message::AssistantContent>,
) -> Option<String> {
    let text = choice
        .iter()
        .filter_map(|item| match item {
            rig::message::AssistantContent::Text(text) => Some(text.text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    (!text.is_empty()).then_some(text)
}

fn tool_calls_from_choice(
    choice: &rig::OneOrMany<rig::message::AssistantContent>,
) -> Vec<CompletionToolCall> {
    choice
        .iter()
        .filter_map(|item| match item {
            rig::message::AssistantContent::ToolCall(tool_call) => Some(tool_call.clone()),
            _ => None,
        })
        .collect()
}

fn tool_result_message(
    tool_results: Vec<rig::completion::message::UserContent>,
) -> CompletionMessage {
    CompletionMessage::User {
        content: rig::OneOrMany::many(tool_results).expect("there is atleast one tool result"),
    }
}

fn rig_tool_call_content(
    id: impl Into<String>,
    call_id: Option<String>,
    name: impl Into<String>,
    arguments: serde_json::Value,
) -> rig::message::AssistantContent {
    match call_id {
        Some(call_id) => {
            rig::message::AssistantContent::tool_call_with_call_id(id, call_id, name, arguments)
        }
        None => rig::message::AssistantContent::tool_call(id, name, arguments),
    }
}

fn rig_tool_result_content(
    id: impl Into<String>,
    call_id: Option<String>,
    content: rig::OneOrMany<rig::completion::message::ToolResultContent>,
) -> rig::completion::message::UserContent {
    match call_id {
        Some(call_id) => {
            rig::completion::message::UserContent::tool_result_with_call_id(id, call_id, content)
        }
        None => rig::completion::message::UserContent::tool_result(id, content),
    }
}

fn provider_metadata(call_id: Option<String>) -> Option<ProviderMetadata> {
    Some(ProviderMetadata::new(call_id))
}

fn persisted_node_from_backend_event(
    envelope: BackendEvent,
    execution: &ExecutionMetadata,
) -> (Role, Option<BackendMetadata>, Kind) {
    let metadata = BackendMetadata::builder()
        .execution(execution)
        .maybe_provider(envelope.metadata.as_ref())
        .build();

    match envelope.event {
        BackendEventPayload::AssistantText(text) => (Role::LLM, metadata, Kind::Text(text)),
        BackendEventPayload::ToolUse(mut tool_use) => {
            if tool_use.call_id.is_none() {
                tool_use.call_id = envelope
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.call_id.clone());
            }
            (Role::LLM, metadata, Kind::tool_use(tool_use))
        }
        BackendEventPayload::ToolResult(mut tool_result) => {
            if tool_result.call_id.is_none() {
                tool_result.call_id = envelope
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.call_id.clone());
            }
            (Role::User, metadata, Kind::tool_result(tool_result))
        }
        BackendEventPayload::SkillResult(skill_result) => (
            Role::System,
            metadata,
            Kind::Anchor(Anchor::skill_result(
                vec![MergeParent::merge(skill_result.merge_parent)],
                SkillResultAnchor {
                    tool_id: skill_result.tool_id,
                    skill_name: skill_result.skill_name,
                    output: skill_result.output,
                },
            )),
        ),
    }
}

fn append_backend_event(
    trace_node_appender: &TraceNodeAppenderHandle,
    execution: &ExecutionMetadata,
    event: BackendEvent,
) -> std::result::Result<String, BackendError> {
    let (role, metadata, kind) = persisted_node_from_backend_event(event, execution);
    trace_node_appender.append(role, metadata, kind)
}

fn append_backend_events(
    trace_node_appender: &TraceNodeAppenderHandle,
    execution: &ExecutionMetadata,
    events: &[BackendEvent],
) -> std::result::Result<String, BackendError> {
    let mut events = events.iter();
    let first = events
        .next()
        .expect("append_backend_events requires at least one event");
    let mut head_id = append_backend_event(trace_node_appender, execution, first.clone())?;
    for event in events {
        head_id = append_backend_event(trace_node_appender, execution, event.clone())?;
    }

    Ok(head_id)
}

fn rig_messages_from_nodes(nodes: &[coco_mem::Node]) -> Vec<rig::completion::message::Message> {
    let mut builder = ProviderHistoryBuilder::default();

    for node in nodes {
        builder.apply_node(node);
    }

    builder.finish()
}

#[derive(Default)]
struct ProviderHistoryBuilder {
    messages: Vec<rig::completion::message::Message>,
    assistant_contents: Vec<rig::completion::message::AssistantContent>,
    assistant_execution_id: Option<String>,
    tool_results: Vec<rig::completion::message::UserContent>,
    tool_result_execution_id: Option<String>,
}

impl ProviderHistoryBuilder {
    fn apply_node(&mut self, node: &coco_mem::Node) {
        match &node.kind {
            Kind::Anchor(anchor) => match &anchor.payload {
                AnchorPayload::Session(session) => self.push_session_prompt(&session.prompt),
                AnchorPayload::SessionPatch(_) => {}
                AnchorPayload::Prompt(prompt) => self.push_user_text(&prompt.prompt),
                AnchorPayload::SkillResult(result) => self.push_tool_result(
                    node.metadata.as_ref(),
                    &result.tool_id,
                    None,
                    &result.output,
                ),
            },
            Kind::Text(text) => match node.role {
                Role::User => self.push_user_text(text),
                Role::LLM => self.push_assistant_text(node.metadata.as_ref(), text),
                Role::System => {}
            },
            Kind::ToolUse(tool_uses) => {
                for tool_use in tool_uses.iter() {
                    self.push_tool_use(node.metadata.as_ref(), tool_use);
                }
            }
            Kind::ToolResult(tool_results) => {
                for tool_result in tool_results.iter() {
                    self.push_tool_result(
                        node.metadata.as_ref(),
                        &tool_result.id,
                        tool_result.call_id.as_deref(),
                        &tool_result.output,
                    );
                }
            }
            Kind::Failure(_) => {}
        }
    }

    fn push_session_prompt(&mut self, prompt: &str) {
        if prompt.is_empty() {
            return;
        }

        self.push_user_text(prompt);
    }

    fn push_user_text(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }

        self.flush_assistant_contents();
        self.flush_tool_results();
        self.messages
            .push(rig::completion::message::Message::user(text.to_owned()));
    }

    fn push_assistant_text(&mut self, metadata: Option<&BackendMetadata>, text: &str) {
        self.flush_tool_results();
        let execution_id = metadata.and_then(|metadata| metadata.execution_id.clone());
        if !self.assistant_contents.is_empty() && self.assistant_execution_id != execution_id {
            self.flush_assistant_contents();
        }
        self.assistant_execution_id = execution_id;
        self.assistant_contents
            .push(rig::message::AssistantContent::text(text.to_owned()));
    }

    fn push_tool_use(&mut self, metadata: Option<&BackendMetadata>, tool_use: &ToolUse) {
        self.flush_tool_results();
        let execution_id = metadata.and_then(|metadata| metadata.execution_id.clone());
        if !self.assistant_contents.is_empty() && self.assistant_execution_id != execution_id {
            self.flush_assistant_contents();
        }
        self.assistant_execution_id = execution_id;
        let call_id = tool_use
            .call_id
            .clone()
            .or_else(|| metadata.and_then(|metadata| metadata.call_id.clone()));
        self.assistant_contents.push(rig_tool_call_content(
            tool_use.id.clone(),
            call_id,
            tool_use.name.clone(),
            tool_use.input.clone(),
        ));
    }

    fn push_tool_result(
        &mut self,
        metadata: Option<&BackendMetadata>,
        id: &str,
        call_id: Option<&str>,
        output: &str,
    ) {
        self.flush_assistant_contents();
        let execution_id = metadata.and_then(|metadata| metadata.execution_id.clone());
        if !self.tool_results.is_empty() && self.tool_result_execution_id != execution_id {
            self.flush_tool_results();
        }
        self.tool_result_execution_id = execution_id;
        let content = rig::OneOrMany::one(rig::completion::message::ToolResultContent::text(
            output.to_owned(),
        ));
        let call_id = call_id
            .map(str::to_owned)
            .or_else(|| metadata.and_then(|metadata| metadata.call_id.clone()));
        self.tool_results
            .push(rig_tool_result_content(id.to_owned(), call_id, content));
    }

    fn finish(mut self) -> Vec<rig::completion::message::Message> {
        self.flush_assistant_contents();
        self.flush_tool_results();
        self.messages
    }

    fn flush_assistant_contents(&mut self) {
        if self.assistant_contents.is_empty() {
            return;
        }

        self.messages
            .push(rig::completion::message::Message::Assistant {
                id: None,
                content: rig::OneOrMany::many(std::mem::take(&mut self.assistant_contents))
                    .expect("assistant content buffer is non-empty"),
            });
        self.assistant_execution_id = None;
    }

    fn flush_tool_results(&mut self) {
        if self.tool_results.is_empty() {
            return;
        }

        self.messages.push(rig::completion::message::Message::User {
            content: rig::OneOrMany::many(std::mem::take(&mut self.tool_results))
                .expect("tool result buffer is non-empty"),
        });
        self.tool_result_execution_id = None;
    }
}

fn push_assistant_text_event(buffer: &mut Vec<String>, events: &mut Vec<BackendEvent>) {
    if !buffer.is_empty() {
        events.push(BackendEvent::new(BackendEventPayload::AssistantText(
            buffer.join("\n"),
        )));
        buffer.clear();
    }
}

fn backend_events_from_choice(
    choice: &rig::OneOrMany<rig::message::AssistantContent>,
) -> Vec<BackendEvent> {
    let mut events = Vec::new();
    let mut text_buffer = Vec::new();

    for item in choice.iter() {
        match item {
            rig::message::AssistantContent::Text(text) => {
                text_buffer.push(text.text.clone());
            }
            rig::message::AssistantContent::ToolCall(tool_call) => {
                push_assistant_text_event(&mut text_buffer, &mut events);
                events.push(
                    BackendEvent::new(BackendEventPayload::ToolUse(ToolUse {
                        id: tool_call.id.clone(),
                        call_id: tool_call.call_id.clone(),
                        name: tool_call.function.name.clone(),
                        input: tool_call.function.arguments.clone(),
                    }))
                    .with_metadata(provider_metadata(tool_call.call_id.clone())),
                );
            }
            rig::message::AssistantContent::Reasoning(_)
            | rig::message::AssistantContent::Image(_) => {}
        }
    }

    push_assistant_text_event(&mut text_buffer, &mut events);
    events
}

fn normalize_backend_steps(
    mut steps: Vec<BackendStep>,
    final_text: Option<String>,
) -> (String, Vec<BackendStep>) {
    if steps.is_empty() {
        steps.push(BackendStep {
            execution: ExecutionMetadata::new(format!("execution-{}", nanoid::nanoid!())),
            events: vec![],
        });
    }

    let last_step = steps.last_mut().expect("steps is non-empty");

    if let Some(text) = final_text
        && !matches!(
            last_step.events.last(),
            Some(last_event)
                if matches!(&last_event.event, BackendEventPayload::AssistantText(last_text) if last_text == &text)
        )
    {
        last_step
            .events
            .push(BackendEventPayload::AssistantText(text).into());
    }

    (last_step.execution.execution_id.clone(), steps)
}

struct CompletionRunner {
    session: ResolvedSession,
    request: ResolvedCompletionRequest,
    prompt: CompletionMessage,
    history: Vec<CompletionMessage>,
    runtime_tools: RuntimeToolSet,
    tool_definitions: Vec<CompletionToolDefinition>,
    head: Option<String>,
    tool_use_node_ids: HashMap<String, String>,
    steps: Vec<BackendStep>,
    pending_step: Option<BackendStep>,
}

struct StepState {
    execution: ExecutionMetadata,
    step_events: Vec<BackendEvent>,
}

struct ToolCallExecution {
    event: BackendEvent,
    tool_result: rig::completion::message::UserContent,
}

enum RunControl {
    Continue,
    Completed(BackendRun),
}

impl CompletionRunner {
    async fn new(
        session: ResolvedSession,
        request: ResolvedCompletionRequest,
    ) -> std::result::Result<Self, BackendError> {
        let history = session.provider_history.clone();
        let Some((prompt, history)) = history.split_last() else {
            return Err(BackendError::failed("completion requires history"));
        };
        let runtime_tools = build_runtime_tools_for_session(&session, &request.runtime)?;
        let tool_definitions = runtime_tools.definition_list();

        Ok(Self {
            session,
            request,
            prompt: prompt.clone(),
            history: history.to_vec(),
            runtime_tools,
            tool_definitions,
            head: None,
            tool_use_node_ids: HashMap::new(),
            steps: vec![],
            pending_step: None,
        })
    }

    fn step_context(&self) -> StepContext<'_> {
        StepContext {
            session: &self.session,
            request: &self.request,
            prompt: &self.prompt,
            history: &self.history,
            tool_definitions: &self.tool_definitions,
        }
    }

    fn next_execution_id() -> String {
        format!("execution-{}", nanoid::nanoid!())
    }

    fn begin_step(&mut self) -> StepState {
        StepState {
            execution: self
                .pending_step
                .as_ref()
                .map(|step| step.execution.clone())
                .unwrap_or_else(|| ExecutionMetadata::new(Self::next_execution_id())),
            step_events: self
                .pending_step
                .take()
                .map(|step| step.events)
                .unwrap_or_default(),
        }
    }

    async fn execute_step<B>(&self, backend: &B) -> std::result::Result<BackendTurn, BackendError>
    where
        B: CompletionBackend + ?Sized,
    {
        backend.step(self.step_context()).await
    }

    fn record_turn_events(
        &mut self,
        execution: &ExecutionMetadata,
        step_events: &mut Vec<BackendEvent>,
        turn: &BackendTurn,
    ) -> std::result::Result<(), BackendError> {
        if let Some(trace_node_appender) = self.request.trace_node_appender.as_ref()
            && !turn.trace_persisted
            && !turn.events.is_empty()
        {
            let mut head_id = None;
            for event in &turn.events {
                let node_id = append_backend_event(trace_node_appender, execution, event.clone())?;
                if let BackendEventPayload::ToolUse(tool_use) = &event.event {
                    self.tool_use_node_ids
                        .insert(tool_use.id.clone(), node_id.clone());
                }
                head_id = Some(node_id);
            }
            self.head = head_id;
        }
        step_events.extend(turn.events.clone());
        Ok(())
    }

    fn fail_current_run(&mut self, state: StepState, error: BackendError) -> BackendRun {
        self.steps.push(BackendStep {
            execution: state.execution,
            events: state.step_events,
        });
        BackendRun::failed_with_steps(error.to_string(), std::mem::take(&mut self.steps))
            .with_head(self.head.take())
    }

    fn complete_terminal_step(
        &mut self,
        state: StepState,
        turn: BackendTurn,
    ) -> std::result::Result<BackendRun, BackendError> {
        let text = turn.final_text.ok_or_else(|| {
            BackendError::failed("completion response did not include assistant text")
        })?;
        if let Some(trace_node_appender) = self.request.trace_node_appender.as_ref()
            && !turn.trace_persisted
            && !matches!(
                turn.events.last(),
                Some(last_event)
                    if matches!(&last_event.event, BackendEventPayload::AssistantText(last_text) if last_text == &text)
            )
        {
            self.head = Some(append_backend_event(
                trace_node_appender,
                &state.execution,
                BackendEventPayload::AssistantText(text.clone()).into(),
            )?);
        }
        self.steps.push(BackendStep {
            execution: state.execution,
            events: state.step_events,
        });
        Ok(
            BackendRun::succeeded_with_steps(text, std::mem::take(&mut self.steps))
                .with_head(self.head.take()),
        )
    }

    async fn execute_tool_call(
        &mut self,
        next_execution: &ExecutionMetadata,
        tool_call: CompletionToolCall,
    ) -> std::result::Result<ToolCallExecution, BackendError> {
        let tool_use_node_id = self.tool_use_node_ids.remove(&tool_call.id);
        if tool_call.function.name == "use_skill" && tool_use_node_id.is_none() {
            return Err(BackendError::failed(format!(
                "missing persisted tool_use node for {:?}",
                tool_call.id
            )));
        }
        let args = serde_json::to_string(&tool_call.function.arguments)
            .map_err(|source| BackendError::failed(source.to_string()))?;
        let outcome = self
            .runtime_tools
            .execute(
                &tool_call.function.name,
                args,
                ToolInvocationContext {
                    persisted_tool_use_node_id: tool_use_node_id,
                },
            )
            .await?;
        let provider_output = outcome.provider_output().to_owned();
        let call_id = tool_call.call_id.clone();
        let event = outcome.into_backend_event(tool_call.id.clone(), call_id.clone());
        if let Some(trace_node_appender) = self.request.trace_node_appender.as_ref() {
            self.head = Some(append_backend_event(
                trace_node_appender,
                next_execution,
                event.clone(),
            )?);
        }

        Ok(ToolCallExecution {
            event,
            tool_result: rig_tool_result_content(
                tool_call.id,
                call_id,
                rig::OneOrMany::one(rig::completion::message::ToolResultContent::text(
                    provider_output,
                )),
            ),
        })
    }

    async fn advance_with_tool_calls(
        &mut self,
        state: StepState,
        turn: BackendTurn,
    ) -> std::result::Result<RunControl, BackendError> {
        self.history.push(self.prompt.clone());
        self.history.push(turn.message);

        let next_execution = ExecutionMetadata::new(Self::next_execution_id());
        let mut tool_results = Vec::with_capacity(turn.tool_calls.len());
        let mut next_events = Vec::with_capacity(turn.tool_calls.len());
        for tool_call in turn.tool_calls {
            let execution = self.execute_tool_call(&next_execution, tool_call).await?;
            next_events.push(execution.event);
            tool_results.push(execution.tool_result);
        }

        self.steps.push(BackendStep {
            execution: state.execution,
            events: state.step_events,
        });
        // Tool calls are intermediate outcomes. The parent run must continue from the tool result
        // until it produces its own terminal text or failure.
        self.pending_step = Some(BackendStep {
            execution: next_execution,
            events: next_events,
        });
        self.prompt = tool_result_message(tool_results);
        Ok(RunControl::Continue)
    }

    async fn finish_step(
        &mut self,
        state: StepState,
        turn: BackendTurn,
    ) -> std::result::Result<RunControl, BackendError> {
        if turn.tool_calls.is_empty() {
            return self
                .complete_terminal_step(state, turn)
                .map(RunControl::Completed);
        }

        self.advance_with_tool_calls(state, turn).await
    }

    fn max_turn_failure(&mut self) -> BackendRun {
        if let Some(pending_step) = self.pending_step.take() {
            self.steps.push(pending_step);
        }

        BackendRun::failed_with_steps(
            format!("MaxTurnError: (reached max turn limit: {DEFAULT_AGENT_MAX_TURNS})"),
            std::mem::take(&mut self.steps),
        )
        .with_head(self.head.take())
    }

    async fn run<B>(mut self, backend: &B) -> std::result::Result<BackendRun, BackendError>
    where
        B: CompletionBackend + ?Sized,
    {
        for _ in 0..DEFAULT_AGENT_MAX_TURNS {
            let mut state = self.begin_step();
            let turn = match self.execute_step(backend).await {
                Ok(turn) => turn,
                Err(source) => return Ok(self.fail_current_run(state, source)),
            };

            self.record_turn_events(&state.execution, &mut state.step_events, &turn)?;

            match self.finish_step(state, turn).await? {
                RunControl::Continue => {}
                RunControl::Completed(run) => return Ok(run),
            }
        }

        Ok(self.max_turn_failure())
    }
}

async fn send_completion_turn<M>(
    model: M,
    ctx: StepContext<'_>,
) -> std::result::Result<BackendTurn, BackendError>
where
    M: rig::completion::CompletionModel,
{
    let builder = model
        .completion_request(ctx.prompt.clone())
        .messages(ctx.history.to_vec());
    let response = configure_completion_request_builder(
        builder,
        ctx.session,
        ctx.request,
        ctx.tool_definitions,
    )
    .send()
    .await
    .map_err(|source| BackendError::failed(source.to_string()))?;

    Ok(BackendTurn::from_assistant_choice(
        response.message_id,
        response.choice,
    ))
}

fn resolve_provider_api_key(
    provider: Provider,
    provider_env: &'static str,
    secrets: &BTreeMap<String, String>,
) -> std::result::Result<String, BackendError> {
    if let Some(env) = secrets.get("api_key") {
        return std::env::var(env).map_err(|_| {
            BackendError::failed(format!(
                "missing API key for provider {} in environment variable {}",
                provider.as_str(),
                env
            ))
        });
    }

    let generic = std::env::var("COCO_API_KEY").ok();
    let provider_specific = std::env::var(provider_env).ok();

    generic.or(provider_specific).ok_or_else(|| {
        BackendError::failed(format!(
            "missing API key for provider {}",
            provider.as_str()
        ))
    })
}

fn resolve_chatgpt_auth(
    secrets: &BTreeMap<String, String>,
) -> std::result::Result<rig::providers::chatgpt::ChatGPTAuth, BackendError> {
    if let Some(env) = secrets.get("access_token")
        && let Some(access_token) = resolve_secret_env(
            env,
            format!("missing access token for provider chatgpt in environment variable {env}"),
        )?
    {
        return Ok(rig::providers::chatgpt::ChatGPTAuth::AccessToken {
            access_token,
            account_id: resolve_chatgpt_account_id(secrets)?,
        });
    }

    if resolve_env_value("COCO_API_KEY").is_some() {
        return Err(BackendError::failed(
            "COCO_API_KEY must not be set when provider is chatgpt",
        ));
    }

    match resolve_env_value("CHATGPT_ACCESS_TOKEN") {
        Some(access_token) => Ok(rig::providers::chatgpt::ChatGPTAuth::AccessToken {
            access_token,
            account_id: resolve_chatgpt_account_id(secrets)?,
        }),
        None => Ok(rig::providers::chatgpt::ChatGPTAuth::OAuth),
    }
}

fn resolve_chatgpt_account_id(
    secrets: &BTreeMap<String, String>,
) -> std::result::Result<Option<String>, BackendError> {
    Ok(resolve_optional_secret(secrets, "account_id")?
        .or_else(|| resolve_env_value("CHATGPT_ACCOUNT_ID")))
}

fn resolve_optional_secret(
    secrets: &BTreeMap<String, String>,
    key: &str,
) -> std::result::Result<Option<String>, BackendError> {
    secrets
        .get(key)
        .map(|env| {
            resolve_secret_env(
                env,
                format!("missing secret {key} in environment variable {env}"),
            )
        })
        .transpose()
        .map(Option::flatten)
}

fn resolve_secret_env(
    name: &str,
    missing_message: String,
) -> std::result::Result<Option<String>, BackendError> {
    std::env::var(name)
        .map(|value| (!value.trim().is_empty()).then_some(value))
        .map_err(|_| BackendError::failed(missing_message))
}

fn resolve_env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn resolve_base_url(provider: Provider, custom_base_url: Option<&str>) -> Option<String> {
    custom_base_url.map(str::to_owned).or_else(|| {
        std::env::var("COCO_BASE_URL")
            .ok()
            .or_else(|| match provider {
                Provider::OpenAi => std::env::var("OPENAI_BASE_URL").ok(),
                Provider::Anthropic => None,
                Provider::ChatGpt => std::env::var("CHATGPT_API_BASE")
                    .ok()
                    .or_else(|| std::env::var("OPENAI_CHATGPT_API_BASE").ok()),
            })
    })
}

#[async_trait]
impl CompletionBackend for RigBackend {
    async fn step(&self, ctx: StepContext<'_>) -> std::result::Result<BackendTurn, BackendError> {
        use rig::client::CompletionClient;
        use rig::providers::{anthropic, chatgpt, openai};

        match ctx.request.provider {
            Provider::OpenAi => {
                let api_key = resolve_provider_api_key(
                    ctx.request.provider,
                    "OPENAI_API_KEY",
                    &ctx.request.secrets,
                )?;
                let mut builder = openai::Client::builder().api_key(&api_key);
                if let Some(base_url) =
                    resolve_base_url(ctx.request.provider, ctx.request.base_url.as_deref())
                {
                    builder = builder.base_url(&base_url);
                }
                let client = builder
                    .build()
                    .map_err(|source| BackendError::failed(source.to_string()))?;
                send_completion_turn(client.completion_model(&ctx.request.model), ctx).await
            }
            Provider::Anthropic => {
                let api_key = resolve_provider_api_key(
                    ctx.request.provider,
                    "ANTHROPIC_API_KEY",
                    &ctx.request.secrets,
                )?;
                let mut builder = anthropic::Client::builder().api_key(api_key);
                if let Some(base_url) =
                    resolve_base_url(ctx.request.provider, ctx.request.base_url.as_deref())
                {
                    builder = builder.base_url(&base_url);
                }
                let client = builder
                    .build()
                    .map_err(|source| BackendError::failed(source.to_string()))?;
                send_completion_turn(client.completion_model(&ctx.request.model), ctx).await
            }
            Provider::ChatGpt => {
                let mut builder =
                    chatgpt::Client::builder().api_key(resolve_chatgpt_auth(&ctx.request.secrets)?);
                if let Some(base_url) =
                    resolve_base_url(ctx.request.provider, ctx.request.base_url.as_deref())
                {
                    builder = builder.base_url(base_url);
                }
                let client = builder
                    .build()
                    .map_err(|source| BackendError::failed(source.to_string()))?;
                send_completion_turn(client.completion_model(&ctx.request.model), ctx).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::VecDeque;
    use std::ffi::OsStr;
    use std::time::Duration;

    use coco_mem::{BranchStore, MemoryStore, NodeStore, SessionStore};
    use tokio::sync::Barrier;

    type RecordedCalls = Arc<Mutex<Vec<(ResolvedSession, ResolvedCompletionRequest)>>>;
    type FakeTurnQueue =
        Arc<Mutex<HashMap<String, VecDeque<std::result::Result<BackendTurn, BackendError>>>>>;
    type FakeRunQueue =
        Arc<Mutex<HashMap<String, VecDeque<std::result::Result<BackendRun, BackendError>>>>>;

    #[derive(Clone)]
    struct FakeBackend {
        turns: FakeTurnQueue,
        runs: FakeRunQueue,
        barrier: Option<Arc<Barrier>>,
        calls: RecordedCalls,
    }

    impl FakeBackend {
        fn with_responses(entries: &[(&str, &[std::result::Result<&str, BackendError>])]) -> Self {
            let turns = entries
                .iter()
                .map(|(branch, responses)| {
                    (
                        (*branch).to_owned(),
                        responses
                            .iter()
                            .map(|response| {
                                response
                                    .as_ref()
                                    .map(|text| BackendTurn::finished((*text).to_owned()))
                                    .map_err(Clone::clone)
                            })
                            .collect(),
                    )
                })
                .collect();

            Self {
                turns: Arc::new(Mutex::new(turns)),
                runs: Arc::new(Mutex::new(HashMap::new())),
                barrier: None,
                calls: Arc::new(Mutex::new(vec![])),
            }
        }

        fn with_turns(
            entries: Vec<(&str, Vec<std::result::Result<BackendTurn, BackendError>>)>,
        ) -> Self {
            let turns = entries
                .into_iter()
                .map(|(branch, responses)| (branch.to_owned(), responses.into()))
                .collect();

            Self {
                turns: Arc::new(Mutex::new(turns)),
                runs: Arc::new(Mutex::new(HashMap::new())),
                barrier: None,
                calls: Arc::new(Mutex::new(vec![])),
            }
        }

        fn with_completions(
            entries: &[(&str, &[std::result::Result<BackendRun, BackendError>])],
        ) -> Self {
            let runs = entries
                .iter()
                .map(|(branch, responses)| {
                    (
                        (*branch).to_owned(),
                        responses.iter().cloned().collect::<VecDeque<_>>(),
                    )
                })
                .collect();

            Self {
                turns: Arc::new(Mutex::new(HashMap::new())),
                runs: Arc::new(Mutex::new(runs)),
                barrier: None,
                calls: Arc::new(Mutex::new(vec![])),
            }
        }

        fn with_barrier(branches: &[(&str, &str)], barrier: Arc<Barrier>) -> Self {
            let turns = branches
                .iter()
                .map(|(branch, response)| {
                    (
                        (*branch).to_owned(),
                        VecDeque::from([Ok(BackendTurn::finished((*response).to_owned()))]),
                    )
                })
                .collect();

            Self {
                turns: Arc::new(Mutex::new(turns)),
                runs: Arc::new(Mutex::new(HashMap::new())),
                barrier: Some(barrier),
                calls: Arc::new(Mutex::new(vec![])),
            }
        }
    }

    #[async_trait]
    impl CompletionBackend for FakeBackend {
        async fn step(
            &self,
            ctx: StepContext<'_>,
        ) -> std::result::Result<BackendTurn, BackendError> {
            self.calls
                .lock()
                .await
                .push((ctx.session.clone(), ctx.request.clone()));

            if let Some(barrier) = &self.barrier {
                barrier.wait().await;
            }

            let mut turns = self.turns.lock().await;
            let queue = turns
                .get_mut(&ctx.request.branch)
                .expect("missing fake backend response queue");
            let next = queue.pop_front().expect("missing fake backend response");
            drop(turns);

            tokio::time::sleep(Duration::from_millis(5)).await;
            next
        }

        async fn complete(
            &self,
            session: ResolvedSession,
            request: ResolvedCompletionRequest,
        ) -> std::result::Result<BackendRun, BackendError> {
            let has_run_queue = {
                let runs = self.runs.lock().await;
                runs.contains_key(&request.branch)
            };
            if !has_run_queue {
                return CompletionRunner::new(session, request)
                    .await?
                    .run(self)
                    .await;
            }

            self.calls.lock().await.push((session, request.clone()));

            if let Some(barrier) = &self.barrier {
                barrier.wait().await;
            }

            let mut runs = self.runs.lock().await;
            let queue = runs
                .get_mut(&request.branch)
                .expect("missing fake backend completion queue");
            let next = queue
                .pop_front()
                .expect("missing fake backend completion response");
            drop(runs);

            tokio::time::sleep(Duration::from_millis(5)).await;
            next
        }
    }

    #[derive(Debug)]
    struct FakeSkillExecutor;

    #[async_trait]
    impl SkillToolExecutor for FakeSkillExecutor {
        async fn search_skill_tool(
            &self,
            _request: SearchSkillToolRequest,
        ) -> std::result::Result<String, ExecutorError> {
            Ok(r#"{"skills":[]}"#.to_owned())
        }

        async fn execute_skill_tool(
            &self,
            request: UseSkillToolRequest,
        ) -> std::result::Result<SkillToolExecutionResult, ExecutorError> {
            let response_node_id = request.parent_tool_use_id.clone();
            Ok(SkillToolExecutionResult {
                result: SkillToolRunResult {
                    text: format!("Executed {}", request.skill_name),
                },
                response_node_id,
            })
        }
    }

    #[derive(Clone)]
    struct StreamingBackend {
        calls: RecordedCalls,
    }

    impl StreamingBackend {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(vec![])),
            }
        }
    }

    #[async_trait]
    impl CompletionBackend for StreamingBackend {
        async fn complete(
            &self,
            session: ResolvedSession,
            request: ResolvedCompletionRequest,
        ) -> std::result::Result<BackendRun, BackendError> {
            let skill_merge_parent = session.anchor_id.clone();
            self.calls.lock().await.push((session, request.clone()));
            let trace_node_appender = request
                .trace_node_appender
                .clone()
                .expect("streaming backend requires trace node appender");

            let step_one_events = vec![backend_event(
                BackendEventPayload::ToolUse(ToolUse {
                    id: "tool-call-1".to_owned(),
                    call_id: None,
                    name: "use_skill".to_owned(),
                    input: serde_json::json!({"name": "find-skills"}),
                }),
                Some("execution-step-1"),
                Some("tool-call-1"),
            )];
            append_backend_events(
                &trace_node_appender,
                &execution("execution-step-1"),
                &step_one_events,
            )?;

            tokio::time::sleep(Duration::from_millis(5)).await;

            let step_two_result = backend_event(
                BackendEventPayload::SkillResult(SkillResultEvent {
                    tool_id: "tool-call-1".to_owned(),
                    skill_name: "find-skills".to_owned(),
                    merge_parent: skill_merge_parent,
                    output: "delegated output".to_owned(),
                }),
                Some("execution-step-2"),
                Some("tool-call-1"),
            );
            append_backend_event(
                &trace_node_appender,
                &execution("execution-step-2"),
                step_two_result.clone(),
            )?;

            tokio::time::sleep(Duration::from_millis(5)).await;

            let step_two_text: BackendEvent =
                BackendEventPayload::AssistantText("done".to_owned()).into();
            let head = Some(append_backend_event(
                &trace_node_appender,
                &execution("execution-step-2"),
                step_two_text.clone(),
            )?);

            Ok(BackendRun::succeeded_with_steps(
                "done",
                vec![
                    BackendStep {
                        execution: ExecutionMetadata::new("execution-step-1".to_owned()),
                        events: step_one_events,
                    },
                    BackendStep {
                        execution: ExecutionMetadata::new("execution-step-2".to_owned()),
                        events: vec![step_two_result, step_two_text],
                    },
                ],
            )
            .with_head(head))
        }
    }

    #[derive(Clone)]
    struct StreamingFailBackend;

    #[async_trait]
    impl CompletionBackend for StreamingFailBackend {
        async fn complete(
            &self,
            _session: ResolvedSession,
            request: ResolvedCompletionRequest,
        ) -> std::result::Result<BackendRun, BackendError> {
            let trace_node_appender = request
                .trace_node_appender
                .clone()
                .expect("streaming backend requires trace node appender");

            let step_one_events = vec![backend_event(
                BackendEventPayload::ToolUse(ToolUse {
                    id: "tool-call-1".to_owned(),
                    call_id: None,
                    name: "exec_command".to_owned(),
                    input: serde_json::json!({"cmd": "printf 'hello' > trace.txt"}),
                }),
                Some("execution-step-1"),
                Some("tool-call-1"),
            )];
            append_backend_events(
                &trace_node_appender,
                &execution("execution-step-1"),
                &step_one_events,
            )?;

            tokio::time::sleep(Duration::from_millis(5)).await;

            let step_two_events = vec![
                backend_event(
                    BackendEventPayload::ToolResult(ToolResult {
                        id: "tool-call-1".to_owned(),
                        call_id: None,
                        output: "exit_status: 0\nstdout:\n\nstderr:\n".to_owned(),
                    }),
                    Some("execution-step-2"),
                    Some("tool-call-1"),
                ),
                BackendEventPayload::AssistantText("trying again".to_owned()).into(),
            ];
            let head = append_backend_events(
                &trace_node_appender,
                &execution("execution-step-2"),
                &step_two_events,
            )?;

            Ok(BackendRun::failed_with_steps(
                "MaxTurnError: (reached max turn limit: 8)",
                vec![
                    BackendStep {
                        execution: ExecutionMetadata::new("execution-step-1".to_owned()),
                        events: step_one_events,
                    },
                    BackendStep {
                        execution: ExecutionMetadata::new("execution-step-2".to_owned()),
                        events: step_two_events,
                    },
                ],
            )
            .with_head(Some(head)))
        }
    }

    #[derive(Clone)]
    struct StreamingInvalidTerminalTextBackend;

    #[async_trait]
    impl CompletionBackend for StreamingInvalidTerminalTextBackend {
        async fn complete(
            &self,
            _session: ResolvedSession,
            request: ResolvedCompletionRequest,
        ) -> std::result::Result<BackendRun, BackendError> {
            let trace_node_appender = request
                .trace_node_appender
                .clone()
                .expect("streaming backend requires trace node appender");

            let step_one_events = vec![backend_event(
                BackendEventPayload::ToolUse(ToolUse {
                    id: "tool-call-1".to_owned(),
                    call_id: None,
                    name: "exec_command".to_owned(),
                    input: serde_json::json!({"cmd": "printf done"}),
                }),
                Some("execution-step-1"),
                Some("tool-call-1"),
            )];
            append_backend_events(
                &trace_node_appender,
                &execution("execution-step-1"),
                &step_one_events,
            )?;

            let step_two_events = vec![backend_event(
                BackendEventPayload::ToolResult(ToolResult {
                    id: "tool-call-1".to_owned(),
                    call_id: None,
                    output: "stdout: done".to_owned(),
                }),
                Some("execution-step-2"),
                Some("tool-call-1"),
            )];
            let head = append_backend_events(
                &trace_node_appender,
                &execution("execution-step-2"),
                &step_two_events,
            )?;

            Ok(BackendRun::succeeded_with_steps(
                "done",
                vec![
                    BackendStep {
                        execution: ExecutionMetadata::new("execution-step-1".to_owned()),
                        events: step_one_events,
                    },
                    BackendStep {
                        execution: ExecutionMetadata::new("execution-step-2".to_owned()),
                        events: step_two_events,
                    },
                ],
            )
            .with_head(Some(head)))
        }
    }

    fn session_config(branch: &str) -> SessionConfig {
        SessionConfig {
            branch: branch.to_owned(),
            merge_parents: vec![],
            provider_profile: None,
            role: SessionRole::Orchestrator,
            provider: Provider::OpenAi,
            model: "gpt-4.1-mini".to_owned(),
            system_prompt: "You are helpful.".to_owned(),
            prompt: "Conversation start.".to_owned(),
            tools: vec![],
            temperature: Some(0.2),
            max_tokens: Some(64),
            additional_params: None,
            enable_coco_shim: false,
        }
    }

    fn request(branch: &str) -> CompletionRequest {
        CompletionRequest {
            branch: branch.to_owned(),
            origin: CompletionOrigin::BranchHead,
            input: CompletionInput::Continue,
            overrides: CompletionOverrides::default(),
            active_skill_runtime: None,
        }
    }

    fn prompt_request(branch: &str, prompt: &str) -> PromptRequest {
        PromptRequest {
            branch: branch.to_owned(),
            prompt: prompt.to_owned(),
            merge_parents: vec![],
            session_patch: None,
        }
    }

    fn session_patch() -> SessionConfigPatch {
        SessionConfigPatch {
            role: None,
            provider_profile: None,
            provider: None,
            model: None,
            tools: None,
            temperature: None,
            max_tokens: None,
            additional_params: None,
            enable_coco_shim: None,
        }
    }

    fn exec_command_tool() -> Tool {
        crate::builtin_tool_definition("exec_command").expect("builtin tool should exist")
    }

    fn text_messages_from_provider_history(
        messages: &[rig::completion::message::Message],
    ) -> Vec<(Role, String)> {
        let mut text_messages = Vec::new();

        for message in messages {
            match message {
                rig::completion::message::Message::User { content } => {
                    text_messages.extend(content.iter().filter_map(|content| match content {
                        rig::completion::message::UserContent::Text(text) => {
                            Some((Role::User, text.text.clone()))
                        }
                        rig::completion::message::UserContent::ToolResult(_)
                        | rig::completion::message::UserContent::Image(_)
                        | rig::completion::message::UserContent::Audio(_)
                        | rig::completion::message::UserContent::Video(_)
                        | rig::completion::message::UserContent::Document(_) => None,
                    }));
                }
                rig::completion::message::Message::Assistant { content, .. } => {
                    text_messages.extend(content.iter().filter_map(|content| match content {
                        rig::completion::message::AssistantContent::Text(text) => {
                            Some((Role::LLM, text.text.clone()))
                        }
                        rig::completion::message::AssistantContent::ToolCall(_)
                        | rig::completion::message::AssistantContent::Reasoning(_)
                        | rig::completion::message::AssistantContent::Image(_) => None,
                    }));
                }
                rig::completion::message::Message::System { .. } => {}
            }
        }

        text_messages
    }

    #[test]
    fn current_skill_name_from_prompt_extracts_skill_context() {
        let prompt = "You are executing the skill `catgirl-role` on an isolated child branch.";

        assert_eq!(
            current_skill_name_from_prompt(prompt),
            Some("catgirl-role".to_owned())
        );
        assert_eq!(current_skill_name_from_prompt("regular prompt"), None);
    }

    #[tokio::test]
    async fn handoff_use_skill_waits_for_sibling_results_before_continuing() {
        let use_skill = rig_tool_call_content(
            "tool-call-1",
            Some("call-1".to_owned()),
            "use_skill",
            serde_json::json!({"name": "fast-rust", "handoff": "Review the diff."}),
        );
        let search_skill = rig_tool_call_content(
            "tool-call-2",
            Some("call-2".to_owned()),
            "search_skill",
            serde_json::json!({"query": "rust"}),
        );
        let turn = BackendTurn::from_assistant_choice(
            Some("assistant-1".to_owned()),
            rig::OneOrMany::many(vec![use_skill, search_skill])
                .expect("assistant choice should be non-empty"),
        );
        let backend = FakeBackend::with_turns(vec![(
            "main",
            vec![Ok(turn), Ok(BackendTurn::finished("done"))],
        )]);
        let store = MemoryStore::new();
        let service = LlmService::builder(store.clone(), backend)
            .with_skill_tool_executor(Arc::new(FakeSkillExecutor))
            .build();
        service
            .create_session(SessionConfig {
                tools: vec![
                    crate::builtin_tool_definition("use_skill").unwrap(),
                    crate::builtin_tool_definition("search_skill").unwrap(),
                ],
                ..session_config("main")
            })
            .await
            .unwrap();

        let result = service
            .prompt(prompt_request("main", "delegate and continue"))
            .await
            .unwrap();

        assert_eq!(result.text, "done");
        let session = service.resolve_session("main").unwrap();
        assert!(matches!(
            &session.provider_history[3],
            rig::completion::message::Message::User { content }
                if {
                    let tool_results = content.iter().collect::<Vec<_>>();
                    tool_results.len() == 2
                        && matches!(
                            tool_results[0],
                            rig::completion::message::UserContent::ToolResult(tool_result)
                                if tool_result.id == "tool-call-1"
                                    && tool_result.call_id.as_deref() == Some("call-1")
                        )
                        && matches!(
                            tool_results[1],
                            rig::completion::message::UserContent::ToolResult(tool_result)
                                if tool_result.id == "tool-call-2"
                                    && tool_result.call_id.as_deref() == Some("call-2")
                        )
                }
        ));
    }

    fn metadata(execution_id: Option<&str>, call_id: Option<&str>) -> Option<BackendMetadata> {
        let execution =
            execution_id.map(|execution_id| ExecutionMetadata::new(execution_id.to_owned()));
        let provider = call_id.map(|call_id| ProviderMetadata::new(Some(call_id.to_owned())));

        BackendMetadata::builder()
            .maybe_execution(execution.as_ref())
            .maybe_provider(provider.as_ref())
            .build()
    }

    fn context_node(role: Role, metadata: Option<BackendMetadata>, kind: Kind) -> coco_mem::Node {
        let store = MemoryStore::new();
        let id = store
            .append(NewNode {
                parent: store.root_id(),
                role,
                metadata,
                kind,
            })
            .expect("test node should be appended");
        store.get_node(&id).expect("test node should exist")
    }

    fn kind_has_tool_use(kind: &Kind, expected_id: &str, expected_name: &str) -> bool {
        kind.as_tool_uses().is_some_and(|tool_uses| {
            tool_uses
                .iter()
                .any(|tool_use| tool_use.id == expected_id && tool_use.name == expected_name)
        })
    }

    fn kind_has_tool_result(kind: &Kind, expected_id: &str, expected_output: &str) -> bool {
        kind.as_tool_results().is_some_and(|tool_results| {
            tool_results.iter().any(|tool_result| {
                tool_result.id == expected_id && tool_result.output == expected_output
            })
        })
    }

    fn execution(execution_id: &str) -> ExecutionMetadata {
        ExecutionMetadata::new(execution_id.to_owned())
    }

    fn backend_event(
        event: BackendEventPayload,
        _execution_id: Option<&str>,
        call_id: Option<&str>,
    ) -> BackendEvent {
        BackendEvent::new(event)
            .with_metadata(call_id.map(|call_id| ProviderMetadata::new(Some(call_id.to_owned()))))
    }

    #[test]
    fn provider_parse_accepts_chatgpt() {
        assert_eq!(Provider::parse("chatgpt").unwrap(), Provider::ChatGpt);
        assert_eq!(Provider::ChatGpt.as_str(), "chatgpt");
    }

    fn secrets(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[tokio::test]
    async fn resolve_chatgpt_auth_uses_access_token_when_configured() {
        with_process_env_async(
            &[
                ("COCO_API_KEY", None),
                ("CHATGPT_ACCESS_TOKEN", Some(OsStr::new("chatgpt-token"))),
                ("CHATGPT_ACCOUNT_ID", Some(OsStr::new("acct-123"))),
            ],
            || async {
                match resolve_chatgpt_auth(&BTreeMap::new()).unwrap() {
                    rig::providers::chatgpt::ChatGPTAuth::AccessToken {
                        access_token,
                        account_id,
                    } => {
                        assert_eq!(access_token, "chatgpt-token");
                        assert_eq!(account_id.as_deref(), Some("acct-123"));
                    }
                    rig::providers::chatgpt::ChatGPTAuth::OAuth => {
                        panic!("expected access token auth")
                    }
                }
            },
        )
        .await;
    }

    #[tokio::test]
    async fn resolve_chatgpt_auth_defaults_to_oauth_without_token() {
        with_process_env_async(
            &[
                ("COCO_API_KEY", None),
                ("CHATGPT_ACCESS_TOKEN", None),
                ("CHATGPT_ACCOUNT_ID", None),
            ],
            || async {
                assert!(matches!(
                    resolve_chatgpt_auth(&BTreeMap::new()).unwrap(),
                    rig::providers::chatgpt::ChatGPTAuth::OAuth
                ));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn resolve_chatgpt_auth_defaults_to_oauth_with_empty_default_token() {
        with_process_env_async(
            &[
                ("COCO_API_KEY", None),
                ("CHATGPT_ACCESS_TOKEN", Some(OsStr::new(""))),
                ("CHATGPT_ACCOUNT_ID", Some(OsStr::new(""))),
            ],
            || async {
                assert!(matches!(
                    resolve_chatgpt_auth(&BTreeMap::new()).unwrap(),
                    rig::providers::chatgpt::ChatGPTAuth::OAuth
                ));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn resolve_chatgpt_auth_rejects_coco_api_key() {
        with_process_env_async(
            &[
                ("COCO_API_KEY", Some(OsStr::new("generic-key"))),
                ("CHATGPT_ACCESS_TOKEN", None),
                ("CHATGPT_ACCOUNT_ID", None),
            ],
            || async {
                let error = resolve_chatgpt_auth(&BTreeMap::new()).unwrap_err();
                assert_eq!(
                    error.to_string(),
                    "COCO_API_KEY must not be set when provider is chatgpt"
                );
            },
        )
        .await;
    }

    #[tokio::test]
    async fn resolve_chatgpt_auth_uses_oauth_when_custom_env_access_token_is_empty() {
        with_process_env_async(
            &[
                ("COCO_API_KEY", None),
                ("COCO_CHATGPT_ACCESS_TOKEN", Some(OsStr::new(""))),
                ("COCO_CHATGPT_ACCOUNT_ID", Some(OsStr::new(""))),
                ("CHATGPT_ACCESS_TOKEN", None),
                ("CHATGPT_ACCOUNT_ID", None),
            ],
            || async {
                assert!(matches!(
                    resolve_chatgpt_auth(&secrets(&[
                        ("access_token", "COCO_CHATGPT_ACCESS_TOKEN"),
                        ("account_id", "COCO_CHATGPT_ACCOUNT_ID"),
                    ]))
                    .unwrap(),
                    rig::providers::chatgpt::ChatGPTAuth::OAuth
                ));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn resolve_chatgpt_auth_reports_missing_custom_env_access_token() {
        with_process_env_async(
            &[
                ("COCO_API_KEY", None),
                ("COCO_CHATGPT_ACCESS_TOKEN", None),
                ("CHATGPT_ACCESS_TOKEN", None),
            ],
            || async {
                let error = resolve_chatgpt_auth(&secrets(&[(
                    "access_token",
                    "COCO_CHATGPT_ACCESS_TOKEN",
                )]))
                .unwrap_err();
                assert_eq!(
                    error.to_string(),
                    "missing access token for provider chatgpt in environment variable COCO_CHATGPT_ACCESS_TOKEN"
                );
            },
        )
        .await;
    }

    #[tokio::test]
    async fn resolve_chatgpt_auth_uses_custom_env_access_token() {
        with_process_env_async(
            &[
                ("COCO_API_KEY", Some(OsStr::new("generic-key"))),
                (
                    "COCO_CHATGPT_ACCESS_TOKEN",
                    Some(OsStr::new("profile-token")),
                ),
                ("COCO_CHATGPT_ACCOUNT_ID", Some(OsStr::new("profile-acct"))),
                ("CHATGPT_ACCOUNT_ID", Some(OsStr::new("default-acct"))),
            ],
            || async {
                match resolve_chatgpt_auth(&secrets(&[
                    ("access_token", "COCO_CHATGPT_ACCESS_TOKEN"),
                    ("account_id", "COCO_CHATGPT_ACCOUNT_ID"),
                ]))
                .unwrap()
                {
                    rig::providers::chatgpt::ChatGPTAuth::AccessToken {
                        access_token,
                        account_id,
                    } => {
                        assert_eq!(access_token, "profile-token");
                        assert_eq!(account_id.as_deref(), Some("profile-acct"));
                    }
                    rig::providers::chatgpt::ChatGPTAuth::OAuth => {
                        panic!("expected access token auth")
                    }
                }
            },
        )
        .await;
    }

    #[tokio::test]
    async fn resolve_provider_api_key_prefers_custom_env() {
        with_process_env_async(
            &[
                ("COCO_API_KEY", Some(OsStr::new("generic-key"))),
                ("OPENAI_API_KEY", Some(OsStr::new("openai-key"))),
                ("COCO_WORK_OPENAI_API_KEY", Some(OsStr::new("work-key"))),
            ],
            || async {
                assert_eq!(
                    resolve_provider_api_key(
                        Provider::OpenAi,
                        "OPENAI_API_KEY",
                        &secrets(&[("api_key", "COCO_WORK_OPENAI_API_KEY")])
                    )
                    .unwrap(),
                    "work-key"
                );
            },
        )
        .await;
    }

    #[tokio::test]
    async fn resolve_provider_api_key_reports_missing_custom_env() {
        with_process_env_async(
            &[
                ("COCO_API_KEY", Some(OsStr::new("generic-key"))),
                ("COCO_WORK_OPENAI_API_KEY", None),
            ],
            || async {
                let error = resolve_provider_api_key(
                    Provider::OpenAi,
                    "OPENAI_API_KEY",
                    &secrets(&[("api_key", "COCO_WORK_OPENAI_API_KEY")]),
                )
                .unwrap_err();
                assert_eq!(
                    error.to_string(),
                    "missing API key for provider openai in environment variable COCO_WORK_OPENAI_API_KEY"
                );
            },
        )
        .await;
    }

    #[tokio::test]
    async fn resolve_base_url_reads_chatgpt_specific_env() {
        with_process_env_async(
            &[
                ("COCO_BASE_URL", None),
                (
                    "CHATGPT_API_BASE",
                    Some(OsStr::new("https://chatgpt.example.test")),
                ),
                ("OPENAI_CHATGPT_API_BASE", None),
            ],
            || async {
                assert_eq!(
                    resolve_base_url(Provider::ChatGpt, None).as_deref(),
                    Some("https://chatgpt.example.test")
                );
            },
        )
        .await;
    }

    #[test]
    fn resolve_base_url_prefers_profile_base_url() {
        assert_eq!(
            resolve_base_url(Provider::OpenAi, Some("https://profile.example.test")).as_deref(),
            Some("https://profile.example.test")
        );
    }

    #[tokio::test]
    async fn complete_persists_execution_metadata_on_assistant_node() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[("main", &[Ok("hello")])]);
        let service = LlmService::new(store.clone(), backend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();
        let result = service
            .prompt(prompt_request("main", "Say hello"))
            .await
            .unwrap();

        let ancestry = store.ancestry("main").unwrap();
        let assistant = &ancestry[0];
        let prompt = &ancestry[1];

        assert_eq!(assistant.role, Role::LLM);
        assert!(matches!(&assistant.kind, Kind::Text(text) if text == "hello"));
        assert_eq!(
            assistant.metadata.as_ref().unwrap().execution_id.as_deref(),
            Some(result.execution_id.as_str())
        );

        assert!(matches!(
            &prompt.kind,
            Kind::Anchor(anchor) if matches!(anchor.payload, AnchorPayload::Prompt(_))
        ));
    }

    #[tokio::test]
    async fn create_session_resolves_session_anchor_merge_parents() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[]);
        let service = LlmService::new(store.clone(), backend);
        let main_session = service
            .create_session(session_config("main"))
            .await
            .unwrap();
        let main_head = store.get_branch_head("main").unwrap();

        let draft_session = service
            .create_session(SessionConfig {
                branch: "draft".to_owned(),
                merge_parents: vec![MergeParent::merge(main_head)],
                provider_profile: None,
                role: SessionRole::Orchestrator,
                provider: Provider::OpenAi,
                model: "gpt-4.1-mini".to_owned(),
                system_prompt: "You are helpful.".to_owned(),
                prompt: "Start from main.".to_owned(),
                tools: vec![],
                temperature: Some(0.2),
                max_tokens: Some(64),
                additional_params: None,
                enable_coco_shim: false,
            })
            .await
            .unwrap();

        let anchor = store.get_node(&draft_session.anchor_id).unwrap();
        let Kind::Anchor(anchor) = anchor.kind else {
            panic!("expected anchor node");
        };
        assert!(anchor.as_session().is_some());
        assert_eq!(
            anchor.merge_parent_node_ids(),
            [main_session.anchor_id.as_str()]
        );
    }

    #[tokio::test]
    async fn create_session_with_provider_profile_persists_only_profile_name() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[]);
        let service = LlmService::new(store.clone(), backend);
        let mut config = session_config("main");
        config.provider_profile = Some("work-openai".to_owned());

        let session = service.create_session(config).await.unwrap();

        let anchor = store.get_node(&session.anchor_id).unwrap();
        let Kind::Anchor(anchor) = anchor.kind else {
            panic!("expected anchor node");
        };
        let session_anchor = anchor.as_session().expect("expected session anchor");
        assert_eq!(
            session_anchor.provider_profile.as_deref(),
            Some("work-openai")
        );
        assert_eq!(session_anchor.provider, None);
    }

    #[tokio::test]
    async fn second_turn_uses_previous_assistant_text_in_history() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[("main", &[Ok("first"), Ok("second")])]);
        let service = LlmService::new(store, backend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();

        service
            .prompt(prompt_request("main", "round one"))
            .await
            .unwrap();
        let result = service
            .prompt(prompt_request("main", "round two"))
            .await
            .unwrap();

        assert_eq!(result.text, "second");
        let session = service.resolve_session("main").unwrap();
        assert_eq!(
            text_messages_from_provider_history(&session.provider_history),
            vec![
                (Role::User, "Conversation start.".to_owned()),
                (Role::User, "round one".to_owned()),
                (Role::LLM, "first".to_owned()),
                (Role::User, "round two".to_owned()),
                (Role::LLM, "second".to_owned()),
            ]
        );
    }

    async fn skill_child_context_fixture(
        handoff: Option<String>,
    ) -> (LlmService<FakeBackend, MemoryStore>, String) {
        let store = MemoryStore::new();
        let service = LlmService::new(store.clone(), FakeBackend::with_responses(&[]));
        let base_session = service
            .create_session(session_config("main"))
            .await
            .unwrap();
        let prompt_id = store
            .append(NewNode {
                parent: base_session.anchor_id.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![],
                    PromptAnchor {
                        prompt: "Delegate twice.".to_owned(),
                    },
                )),
            })
            .unwrap();
        let use_skill_id = store
            .append(NewNode {
                parent: prompt_id,
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-call-1".to_owned(),
                    call_id: None,
                    name: "use_skill".to_owned(),
                    input: serde_json::json!({"name": "fast-rust"}),
                }),
            })
            .unwrap();
        let child_config = session_config("child");
        let child_prompt = "You are executing the skill `fast-rust` on an isolated child branch.";
        let child_anchor_id = store
            .append(NewNode {
                parent: use_skill_id,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(
                    vec![],
                    SessionAnchor {
                        role: SessionRole::Runner,
                        provider_profile: child_config.provider_profile,
                        provider: Some(child_config.provider.as_str().to_owned()),
                        model: child_config.model,
                        tools: child_config.tools,
                        system_prompt: child_config.system_prompt,
                        prompt: child_prompt.to_owned(),
                        temperature: child_config.temperature,
                        max_tokens: child_config.max_tokens,
                        additional_params: child_config.additional_params,
                        enable_coco_shim: child_config.enable_coco_shim,
                        active_skill: Some(coco_mem::SkillRuntimeContext {
                            name: "fast-rust".to_owned(),
                            handoff,
                        }),
                    },
                )),
            })
            .unwrap();
        store.fork("child", &child_anchor_id).unwrap();

        (service, child_prompt.to_owned())
    }

    #[tokio::test]
    async fn context_reconstruction_orders_skill_prompt_after_inherited_history() {
        let (service, child_prompt) = skill_child_context_fixture(None).await;
        let session = service.resolve_session("child").unwrap();

        assert_eq!(
            text_messages_from_provider_history(&session.provider_history),
            vec![
                (Role::User, "Conversation start.".to_owned()),
                (Role::User, "Delegate twice.".to_owned()),
                (Role::User, child_prompt.to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn context_reconstruction_skips_use_skill_that_created_skill_session() {
        let (service, _) = skill_child_context_fixture(None).await;
        let session = service.resolve_session("child").unwrap();

        assert_eq!(
            session
                .provider_history
                .iter()
                .filter(|message| {
                    matches!(
                        message,
                        rig::completion::message::Message::Assistant { content, .. }
                            if content.iter().any(|content| matches!(
                                content,
                                rig::completion::message::AssistantContent::ToolCall(_)
                            ))
                    )
                })
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn context_reconstruction_can_hide_parent_history_for_handoff_skill_child() {
        let (service, child_prompt) =
            skill_child_context_fixture(Some("Review the bounded diff.".to_owned())).await;
        let session = service.resolve_session("child").unwrap();

        assert_eq!(
            text_messages_from_provider_history(&session.provider_history),
            vec![(Role::User, child_prompt)]
        );
    }

    #[tokio::test]
    async fn failed_completion_persists_failure_kind_but_not_prompt_history() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_completions(&[(
            "main",
            &[Ok(BackendRun::failed("rate limited", vec![]))],
        )]);
        let service = LlmService::new(store.clone(), backend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();
        let err = service
            .prompt(prompt_request("main", "retry me"))
            .await
            .unwrap_err();

        let (execution_id, error_node_id, retry_from_node_id) = match err {
            Error::Backend {
                source: BackendError::Failed { .. },
                context,
                ..
            } => (
                context.execution_id,
                context.error_node_id,
                context.retry_from_node_id,
            ),
            other => panic!("expected backend error, got {other:?}"),
        };

        let failure = store.get_node(&error_node_id).unwrap();
        assert_eq!(failure.role, Role::System);
        assert!(matches!(&failure.kind, Kind::Failure(text) if text == "rate limited"));
        assert_eq!(
            failure
                .metadata
                .as_ref()
                .unwrap()
                .execution_id
                .as_ref()
                .unwrap(),
            &execution_id
        );
        assert_eq!(store.get_branch_head("main").unwrap(), retry_from_node_id);

        let session = service.resolve_session("main").unwrap();
        assert_eq!(
            text_messages_from_provider_history(&session.provider_history),
            vec![
                (Role::User, "Conversation start.".to_owned()),
                (Role::User, "retry me".to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn prompt_keeps_session_config_without_importing_merge_parent_history() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[
            ("main", &[Ok("main answer"), Ok("merge answer")]),
            ("draft", &[Ok("draft answer")]),
        ]);
        let service = LlmService::new(store.clone(), backend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();
        let main_result = service
            .prompt(prompt_request("main", "main question"))
            .await
            .unwrap();
        service
            .fork("draft", &main_result.response_node_id)
            .unwrap();
        service
            .prompt(prompt_request("draft", "draft question"))
            .await
            .unwrap();
        let draft_head = store.get_branch_head("draft").unwrap();

        let result = service
            .prompt(PromptRequest {
                branch: "main".to_owned(),
                prompt: "merge them".to_owned(),
                merge_parents: vec![MergeParent::merge(draft_head)],
                session_patch: None,
            })
            .await
            .unwrap();
        let session = service.resolve_session("main").unwrap();
        assert_eq!(result.text, "merge answer");
        assert_eq!(session.config.model, "gpt-4.1-mini");
        assert_eq!(session.config.system_prompt, "You are helpful.");
        assert_eq!(
            text_messages_from_provider_history(&session.provider_history),
            vec![
                (Role::User, "Conversation start.".to_owned()),
                (Role::User, "main question".to_owned()),
                (Role::LLM, "main answer".to_owned()),
                (Role::User, "merge them".to_owned()),
                (Role::LLM, "merge answer".to_owned()),
            ]
        );

        let ancestry = service.store().ancestry("main").unwrap();
        assert!(matches!(
            &ancestry[0].kind,
            Kind::Text(text) if text == "merge answer"
        ));
        assert_eq!(ancestry[0].id, result.response_node_id);
        assert_eq!(ancestry[1].id, result.anchor_id);
        assert!(matches!(
            &ancestry[1].kind,
            Kind::Anchor(anchor) if matches!(anchor.payload, AnchorPayload::Prompt(_))
        ));
    }

    #[tokio::test]
    async fn prompt_with_session_patch_appends_patch_anchor_without_truncating_history() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[
            ("main", &[Ok("main answer")]),
            ("runner", &[Ok("runner answer")]),
        ]);
        let calls = backend.calls.clone();
        let service = LlmService::new(store.clone(), backend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();
        service
            .prompt(PromptRequest {
                branch: "main".to_owned(),
                prompt: "keep this".to_owned(),
                merge_parents: vec![],
                session_patch: None,
            })
            .await
            .unwrap();
        let main_head = store.get_branch_head("main").unwrap();
        service.fork("runner", &main_head).unwrap();
        let exec_tool = builtin_tool_definition("exec_command").unwrap();

        let result = service
            .prompt(PromptRequest {
                branch: "runner".to_owned(),
                prompt: "run date".to_owned(),
                merge_parents: vec![],
                session_patch: Some(SessionConfigPatch {
                    role: Some(SessionRole::Runner),
                    tools: Some(vec![exec_tool.clone()]),
                    ..SessionConfigPatch::default()
                }),
            })
            .await
            .unwrap();

        let calls = calls.lock().await;
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[1].0.config.role, SessionRole::Runner);
        assert_eq!(calls[1].0.config.tools, vec![exec_tool.clone()]);
        assert_eq!(
            text_messages_from_provider_history(&calls[1].0.provider_history),
            vec![
                (Role::User, "Conversation start.".to_owned()),
                (Role::User, "keep this".to_owned()),
                (Role::LLM, "main answer".to_owned()),
                (Role::User, "run date".to_owned()),
            ]
        );
        drop(calls);

        let ancestry = store.ancestry("runner").unwrap();
        assert!(matches!(
            &ancestry[0].kind,
            Kind::Text(text) if text == "runner answer"
        ));
        assert_eq!(ancestry[1].id, result.anchor_id);
        assert!(matches!(
            &ancestry[1].kind,
            Kind::Anchor(anchor) if matches!(anchor.payload, AnchorPayload::Prompt(_))
        ));
        let Kind::Anchor(anchor) = &ancestry[2].kind else {
            panic!("expected session patch anchor");
        };
        let patch = anchor
            .as_session_patch()
            .expect("expected session patch anchor");
        assert_eq!(patch.role, Some(SessionRole::Runner));
        assert_eq!(patch.tools, Some(vec![exec_tool]));
        assert_eq!(ancestry[2].parent, main_head);
    }

    #[tokio::test]
    async fn prompt_advances_branch_head_to_completion_node() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[("main", &[Ok("prompted")])]);
        let service = LlmService::new(store.clone(), backend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();

        let result = service
            .prompt(prompt_request("main", "new prompt"))
            .await
            .unwrap();

        assert_eq!(
            store.get_branch_head("main").unwrap(),
            result.response_node_id
        );

        let ancestry = store.ancestry("main").unwrap();
        assert_eq!(ancestry[0].id, result.response_node_id);
        assert!(matches!(&ancestry[0].kind, Kind::Text(text) if text == "prompted"));
        assert_eq!(ancestry[1].id, result.anchor_id);
        assert!(matches!(
            &ancestry[1].kind,
            Kind::Anchor(anchor) if matches!(anchor.payload, AnchorPayload::Prompt(_))
        ));
    }

    #[tokio::test]
    async fn prompt_uses_prompt_anchor_history() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[("main", &[Ok("prompted")])]);
        let service = LlmService::new(store.clone(), backend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();
        let prompt_result = service
            .prompt(prompt_request("main", "new prompt"))
            .await
            .unwrap();
        assert_eq!(prompt_result.text, "prompted");
        assert_eq!(
            store.get_branch_head("main").unwrap(),
            prompt_result.response_node_id
        );

        let ancestry = store.ancestry("main").unwrap();
        assert!(matches!(&ancestry[0].kind, Kind::Text(text) if text == "prompted"));
        assert!(matches!(
            &ancestry[1].kind,
            Kind::Anchor(anchor) if matches!(anchor.payload, AnchorPayload::Prompt(_))
        ));
        assert_ne!(ancestry[1].role, Role::User);

        let session = service.resolve_session("main").unwrap();
        assert_eq!(
            text_messages_from_provider_history(&session.provider_history),
            vec![
                (Role::User, "Conversation start.".to_owned()),
                (Role::User, "new prompt".to_owned()),
                (Role::LLM, "prompted".to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn complete_can_retry_after_prompt_failure() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_completions(&[(
            "main",
            &[
                Ok(BackendRun::failed("rate limited", vec![])),
                Ok(BackendRun::succeeded(
                    "recovered",
                    vec![BackendEventPayload::AssistantText("recovered".to_owned()).into()],
                )),
            ],
        )]);
        let service = LlmService::new(store.clone(), backend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();
        let err = service
            .prompt(prompt_request("main", "retry prompt"))
            .await
            .unwrap_err();
        let (error_node_id, retry_from_node_id) = match err {
            Error::Backend {
                source: BackendError::Failed { .. },
                context,
                ..
            } => (context.error_node_id, context.retry_from_node_id),
            other => panic!("expected backend error, got {other:?}"),
        };
        let failure = store.get_node(&error_node_id).unwrap();
        assert_eq!(failure.role, Role::System);
        assert!(matches!(&failure.kind, Kind::Failure(text) if text == "rate limited"));
        let prompt_anchor_id = retry_from_node_id.clone();
        assert_eq!(store.get_branch_head("main").unwrap(), retry_from_node_id);

        let session = service.resolve_session("main").unwrap();
        assert_eq!(
            text_messages_from_provider_history(&session.provider_history),
            vec![
                (Role::User, "Conversation start.".to_owned()),
                (Role::User, "retry prompt".to_owned()),
            ]
        );

        let recovered = service.run(request("main")).await.unwrap();
        assert_eq!(recovered.text, "recovered");
        assert_eq!(recovered.anchor_id, prompt_anchor_id);
        let recovered_node = store.get_node(&recovered.response_node_id).unwrap();
        assert_eq!(recovered_node.parent, prompt_anchor_id);
    }

    #[tokio::test]
    async fn run_can_continue_from_historical_reference() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[("main", &[Ok("first"), Ok("resumed")])]);
        let service = LlmService::new(store.clone(), backend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();
        let first = service.run(request("main")).await.unwrap();

        let resumed = service
            .run(CompletionRequest {
                branch: "main".to_owned(),
                origin: CompletionOrigin::Reference(first.response_node_id.clone()),
                input: CompletionInput::Continue,
                overrides: CompletionOverrides::default(),
                active_skill_runtime: None,
            })
            .await
            .unwrap();

        assert_eq!(resumed.text, "resumed");
        assert_eq!(
            store.get_branch_head("main").unwrap(),
            resumed.response_node_id
        );

        let resumed_node = store.get_node(&resumed.response_node_id).unwrap();
        assert_eq!(resumed_node.parent, first.response_node_id);
    }

    #[tokio::test]
    async fn run_can_start_from_historical_reference_with_prompt() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[("main", &[Ok("old head"), Ok("new head")])]);
        let service = LlmService::new(store.clone(), backend);
        let session = service
            .create_session(session_config("main"))
            .await
            .unwrap();
        let old_head = service.run(request("main")).await.unwrap();

        let resumed = service
            .run(CompletionRequest {
                branch: "main".to_owned(),
                origin: CompletionOrigin::Reference(session.anchor_id.clone()),
                input: CompletionInput::Prompt {
                    text: "resume from base".to_owned(),
                    merge_parents: vec![],
                    session_patch: None,
                },
                overrides: CompletionOverrides::default(),
                active_skill_runtime: None,
            })
            .await
            .unwrap();

        assert_eq!(resumed.text, "new head");
        assert_eq!(
            store.get_branch_head("main").unwrap(),
            resumed.response_node_id
        );
        assert_ne!(old_head.response_node_id, resumed.response_node_id);

        let prompt_anchor = store.get_node(&resumed.anchor_id).unwrap();
        assert_eq!(prompt_anchor.parent, session.anchor_id);
        assert!(matches!(
            &prompt_anchor.kind,
            Kind::Anchor(anchor)
                if matches!(
                    &anchor.payload,
                    AnchorPayload::Prompt(prompt_anchor) if prompt_anchor.prompt == "resume from base"
                )
        ));
    }

    #[tokio::test]
    async fn different_branches_can_complete_concurrently() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_barrier(
            &[("main", "main"), ("draft", "draft")],
            Arc::new(Barrier::new(2)),
        );
        let service = Arc::new(LlmService::new(store.clone(), backend));
        let main_session = service
            .create_session(session_config("main"))
            .await
            .unwrap();
        service.fork("draft", &main_session.anchor_id).unwrap();

        let main_service = service.clone();
        let draft_service = service.clone();
        let main = tokio::spawn(async move { main_service.run(request("main")).await });
        let draft = tokio::spawn(async move { draft_service.run(request("draft")).await });

        let main_result = main.await.unwrap().unwrap();
        let draft_result = draft.await.unwrap().unwrap();

        assert_eq!(main_result.text, "main");
        assert_eq!(draft_result.text, "draft");
        assert_ne!(
            service.store().get_branch_head("main").unwrap(),
            service.store().get_branch_head("draft").unwrap()
        );
    }

    #[tokio::test]
    async fn open_pull_request_uses_target_head_as_base() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[]);
        let service = LlmService::new(store.clone(), backend);
        let base_session = service
            .create_session(session_config("base"))
            .await
            .unwrap();
        service
            .create_session(session_config("main"))
            .await
            .unwrap();
        let review_anchor_id = store
            .append(NewNode {
                parent: base_session.anchor_id.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![],
                    PromptAnchor {
                        prompt: "review".to_owned(),
                    },
                )),
            })
            .unwrap();
        store
            .set_branch_head("base", &base_session.anchor_id, &review_anchor_id)
            .unwrap();

        let pr = service.open_pull_request("main", "base").await.unwrap();

        assert_eq!(pr.target_branch, "base");
        assert_eq!(pr.base_head_id, review_anchor_id);
        assert_eq!(
            store.get_session_state("main").unwrap(),
            SessionState::Attached {
                target_branch: "base".to_owned(),
                base_head_id: pr.base_head_id,
            }
        );
    }

    #[tokio::test]
    async fn merge_session_appends_target_prompt_anchor_and_pauses_source() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[]);
        let service = LlmService::new(store.clone(), backend);
        service
            .create_session(session_config("base"))
            .await
            .unwrap();
        service
            .create_session(session_config("main"))
            .await
            .unwrap();
        let pr = service.open_pull_request("main", "base").await.unwrap();
        let source_head_id = store.get_branch_head("main").unwrap();

        let merged = service
            .merge_session("main", None, "handoff to base")
            .await
            .unwrap();

        assert_eq!(merged.branch, "main");
        assert_eq!(merged.target_branch, "base");
        assert_eq!(merged.source_head_id, source_head_id);
        assert_ne!(merged.merged_anchor_id, pr.base_head_id);

        let merged_anchor = store.get_node(&merged.merged_anchor_id).unwrap();
        let Kind::Anchor(anchor) = merged_anchor.kind else {
            panic!("expected anchor node");
        };
        let prompt_anchor = anchor.as_prompt().expect("expected prompt anchor");
        assert_eq!(merged_anchor.parent, pr.base_head_id);
        assert_eq!(prompt_anchor.prompt, "handoff to base");
        assert_eq!(anchor.merge_parent_node_ids(), [source_head_id.as_str()]);
        assert_eq!(
            store.get_branch_head("base").unwrap(),
            merged.merged_anchor_id
        );
        assert_eq!(
            store.get_session_state("main").unwrap(),
            SessionState::Paused {
                target_branch: "base".to_owned(),
                reason: PauseReason::Merged {
                    merged_anchor_id: merged.merged_anchor_id,
                },
            }
        );
    }

    #[tokio::test]
    async fn feedback_appends_session_prompt_anchor_and_advances_base_head() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[]);
        let service = LlmService::new(store.clone(), backend);
        let base_session = service
            .create_session(session_config("base"))
            .await
            .unwrap();
        let main_session = service
            .create_session(session_config("main"))
            .await
            .unwrap();
        service.open_pull_request("main", "base").await.unwrap();

        let base_feedback_id = store
            .append(NewNode {
                parent: base_session.anchor_id.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![],
                    PromptAnchor {
                        prompt: "base feedback".to_owned(),
                    },
                )),
            })
            .unwrap();
        store
            .set_branch_head("base", &base_session.anchor_id, &base_feedback_id)
            .unwrap();

        let feedback = service
            .apply_feedback("main", "address review comments", None)
            .await
            .unwrap();

        assert_eq!(feedback.target_branch, "base");
        assert_eq!(feedback.base_head_id, base_feedback_id);
        assert_eq!(feedback.source_anchor_id, base_feedback_id);
        let feedback_anchor = store.get_node(&feedback.feedback_anchor_id).unwrap();
        let Kind::Anchor(anchor) = feedback_anchor.kind else {
            panic!("expected anchor node");
        };
        let prompt_anchor = anchor.as_prompt().expect("expected prompt anchor");
        assert_eq!(feedback_anchor.parent, main_session.anchor_id);
        assert_eq!(prompt_anchor.prompt, "address review comments");
        assert_eq!(anchor.merge_parent_node_ids(), [base_feedback_id.as_str()]);
        assert_eq!(
            store.get_branch_head("main").unwrap(),
            feedback.feedback_anchor_id
        );
        assert_eq!(
            store.get_session_state("main").unwrap(),
            SessionState::Attached {
                target_branch: "base".to_owned(),
                base_head_id: base_feedback_id,
            }
        );
    }

    #[tokio::test]
    async fn feedback_rejects_source_behind_attached_base_head() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[]);
        let service = LlmService::new(store.clone(), backend);
        let base_session = service
            .create_session(session_config("base"))
            .await
            .unwrap();
        service
            .create_session(session_config("main"))
            .await
            .unwrap();
        let newer_feedback_id = store
            .append(NewNode {
                parent: base_session.anchor_id.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![],
                    PromptAnchor {
                        prompt: "new review".to_owned(),
                    },
                )),
            })
            .unwrap();
        store
            .set_branch_head("base", &base_session.anchor_id, &newer_feedback_id)
            .unwrap();
        service.open_pull_request("main", "base").await.unwrap();

        let err = service
            .apply_feedback("main", "stale feedback", Some(&base_session.anchor_id))
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::FeedbackSourceNotAhead {
                target_branch,
                base_head_id,
                source_anchor_id,
            } if target_branch == "base"
                && base_head_id == newer_feedback_id
                && source_anchor_id == base_session.anchor_id
        ));
    }

    #[tokio::test]
    async fn rebase_session_changes_defaults_for_future_turns() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[("main", &[Ok("hello"), Ok("updated")])]);
        let calls = backend.calls.clone();
        let service = LlmService::new(store, backend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();
        service
            .prompt(prompt_request("main", "before rebase"))
            .await
            .unwrap();

        service
            .rebase_session(
                "main",
                SessionConfigPatch {
                    provider: Some(Some("anthropic".to_owned())),
                    model: Some("claude-sonnet-4-20250514".to_owned()),
                    temperature: Some(None),
                    max_tokens: Some(Some(128)),
                    additional_params: Some(Some(serde_json::json!({"service_tier": "priority"}))),
                    ..session_patch()
                },
            )
            .await
            .unwrap();

        let result = service
            .prompt(prompt_request("main", "after rebase"))
            .await
            .unwrap();

        assert_eq!(result.text, "updated");
        let session = service.resolve_session("main").unwrap();
        assert_eq!(session.config.provider, Provider::Anthropic);
        assert_eq!(session.config.model, "claude-sonnet-4-20250514");
        assert_eq!(session.config.system_prompt, "You are helpful.");
        assert_eq!(session.config.temperature, None);
        assert_eq!(session.config.max_tokens, Some(128));
        assert_eq!(
            session.config.additional_params,
            Some(serde_json::json!({"service_tier": "priority"}))
        );

        let calls = calls.lock().await;
        let last = calls.last().expect("expected backend call");
        assert_eq!(last.0.config.system_prompt, "You are helpful.");
        assert_eq!(last.1.provider, Provider::Anthropic);
        assert_eq!(last.1.model, "claude-sonnet-4-20250514");
        assert_eq!(last.1.temperature, None);
        assert_eq!(last.1.max_tokens, Some(128));
        assert_eq!(
            last.1.additional_params,
            Some(serde_json::json!({"service_tier": "priority"}))
        );
    }

    #[tokio::test]
    async fn rebase_session_system_prompt_rebuilds_session_anchor() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[("main", &[Ok("updated")])]);
        let calls = backend.calls.clone();
        let service = LlmService::new(store.clone(), backend);
        let session = service
            .create_session(session_config("main"))
            .await
            .unwrap();

        let new_head = service
            .rebase_session_system_prompt("main", session_patch(), "You are strict.")
            .await
            .unwrap();

        assert_ne!(new_head, session.anchor_id);
        let ancestry = store.ancestry("main").unwrap();
        let Kind::Anchor(anchor) = &ancestry[0].kind else {
            panic!("expected rebuilt session anchor");
        };
        let session_anchor = anchor
            .as_session()
            .expect("expected rebuilt session anchor");
        assert_eq!(session_anchor.system_prompt, "You are strict.");

        let result = service
            .prompt(prompt_request("main", "after rebase"))
            .await
            .unwrap();

        assert_eq!(result.text, "updated");
        let calls = calls.lock().await;
        let last = calls.last().expect("expected backend call");
        assert_eq!(last.0.config.system_prompt, "You are strict.");
    }

    #[tokio::test]
    async fn rebase_session_keeps_sibling_branch_defaults_unchanged() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_responses(&[
            ("main", &[Ok("main"), Ok("main updated")]),
            ("draft", &[Ok("draft")]),
        ]);
        let calls = backend.calls.clone();
        let service = LlmService::new(store, backend);
        let main_session = service
            .create_session(session_config("main"))
            .await
            .unwrap();
        service.fork("draft", &main_session.anchor_id).unwrap();
        service
            .rebase_session(
                "main",
                SessionConfigPatch {
                    model: Some("gpt-5.4".to_owned()),
                    ..session_patch()
                },
            )
            .await
            .unwrap();

        service
            .prompt(prompt_request("draft", "draft prompt"))
            .await
            .unwrap();
        service
            .prompt(prompt_request("main", "main prompt"))
            .await
            .unwrap();

        let calls = calls.lock().await;
        let draft_call = calls
            .iter()
            .find(|(_, request)| request.branch == "draft")
            .expect("expected draft call");
        let main_call = calls
            .iter()
            .rev()
            .find(|(_, request)| request.branch == "main")
            .expect("expected main call");
        assert_eq!(draft_call.1.model, "gpt-4.1-mini");
        assert_eq!(main_call.1.model, "gpt-5.4");
    }

    #[tokio::test]
    async fn prompt_persists_tool_trace_before_final_assistant_text() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_completions(&[(
            "main",
            &[Ok(BackendRun::succeeded_with_steps(
                "done",
                vec![
                    BackendStep {
                        execution: ExecutionMetadata::new("execution-step-1".to_owned()),
                        events: vec![backend_event(
                            BackendEventPayload::ToolUse(ToolUse {
                                id: "tool-call-1".to_owned(),
                                call_id: None,
                                name: "exec_command".to_owned(),
                                input: serde_json::json!({"cmd": "rg --files"}),
                            }),
                            Some("execution-step-1"),
                            Some("tool-call-1"),
                        )],
                    },
                    BackendStep {
                        execution: ExecutionMetadata::new("execution-step-2".to_owned()),
                        events: vec![
                            backend_event(
                                BackendEventPayload::ToolResult(ToolResult {
                                    id: "tool-call-1".to_owned(),
                                    call_id: None,
                                    output: "Cargo.toml".to_owned(),
                                }),
                                Some("execution-step-2"),
                                Some("tool-call-1"),
                            ),
                            BackendEventPayload::AssistantText("done".to_owned()).into(),
                        ],
                    },
                ],
            ))],
        )]);
        let service = LlmService::new(store.clone(), backend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();

        let result = service
            .prompt(prompt_request("main", "list files"))
            .await
            .unwrap();

        let ancestry = store.ancestry("main").unwrap();
        let assistant = &ancestry[0];
        let tool_result = &ancestry[1];
        let tool_use = &ancestry[2];

        assert_eq!(assistant.role, Role::LLM);
        assert!(matches!(&assistant.kind, Kind::Text(text) if text == "done"));
        assert_eq!(
            assistant.metadata.as_ref().unwrap().execution_id.as_deref(),
            Some(result.execution_id.as_str())
        );

        assert_eq!(tool_result.role, Role::User);
        assert!(kind_has_tool_result(
            &tool_result.kind,
            "tool-call-1",
            "Cargo.toml"
        ));
        assert_eq!(
            tool_result
                .metadata
                .as_ref()
                .unwrap()
                .execution_id
                .as_deref(),
            Some("execution-step-2")
        );

        assert_eq!(tool_use.role, Role::LLM);
        assert!(tool_use.kind.as_tool_uses().is_some_and(|tool_uses| {
            tool_uses.iter().any(|tool_use| {
                tool_use.id == "tool-call-1"
                    && tool_use.name == "exec_command"
                    && tool_use.input == serde_json::json!({"cmd": "rg --files"})
            })
        }));
        assert_eq!(
            tool_use.metadata.as_ref().unwrap().execution_id.as_deref(),
            Some("execution-step-1")
        );
    }

    #[tokio::test]
    async fn prompt_rejects_streamed_trace_head_without_terminal_text() {
        let store = MemoryStore::new();
        let service = LlmService::new(store.clone(), StreamingInvalidTerminalTextBackend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();
        let original_head = store.get_branch_head("main").unwrap();

        let error = service
            .prompt(prompt_request("main", "finish the streamed reply"))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("without terminal assistant text")
        );
        assert_eq!(store.get_branch_head("main").unwrap(), original_head);
    }

    #[tokio::test]
    async fn streamed_tool_trace_preserves_event_timestamps() {
        let store = MemoryStore::new();
        let service = LlmService::new(store.clone(), StreamingBackend::new());
        service
            .create_session(session_config("main"))
            .await
            .unwrap();

        let result = service
            .prompt(prompt_request("main", "list files"))
            .await
            .unwrap();

        let ancestry = store.ancestry("main").unwrap();
        let assistant = &ancestry[0];
        let skill_result = &ancestry[1];
        let tool_use = &ancestry[2];

        assert_eq!(assistant.id, result.response_node_id);
        assert!(tool_use.created_at < skill_result.created_at);
        assert!(skill_result.created_at < assistant.created_at);
        assert!(kind_has_tool_use(
            &tool_use.kind,
            "tool-call-1",
            "use_skill"
        ));
        let Kind::Anchor(anchor) = &skill_result.kind else {
            panic!("expected skill result anchor");
        };
        let persisted = anchor
            .as_skill_result()
            .expect("expected skill result payload");
        assert_eq!(persisted.tool_id, "tool-call-1");
        assert_eq!(persisted.skill_name, "find-skills");
        assert_eq!(persisted.output, "delegated output");
        assert_eq!(anchor.merge_parent_node_ids(), [tool_use.parent.as_str()]);
        assert!(matches!(&assistant.kind, Kind::Text(text) if text == "done"));
    }

    #[tokio::test]
    async fn skill_result_event_persists_anchor_without_handoff_state_transition() {
        let store = MemoryStore::new();
        let service = LlmService::new(store.clone(), FakeBackend::with_completions(&[]));
        let base_session = service
            .create_session(session_config("main"))
            .await
            .unwrap();
        service.fork("draft", &base_session.anchor_id).unwrap();
        let draft_head = store.get_branch_head("draft").unwrap();
        assert_eq!(draft_head, base_session.anchor_id);
        store
            .set_session_state(
                "draft",
                None,
                SessionState::Attached {
                    target_branch: "main".to_owned(),
                    base_head_id: base_session.anchor_id.clone(),
                },
            )
            .unwrap();
        let backend = FakeBackend::with_completions(&[(
            "main",
            &[Ok(BackendRun::succeeded_with_steps(
                "base done",
                vec![BackendStep {
                    execution: ExecutionMetadata::new("execution-step-1".to_owned()),
                    events: vec![
                        backend_event(
                            BackendEventPayload::SkillResult(SkillResultEvent {
                                tool_id: "tool-call-1".to_owned(),
                                skill_name: "find-skills".to_owned(),
                                merge_parent: draft_head.clone(),
                                output: "Skill handoff".to_owned(),
                            }),
                            Some("execution-step-1"),
                            Some("tool-call-1"),
                        ),
                        BackendEventPayload::AssistantText("base done".to_owned()).into(),
                    ],
                }],
            ))],
        )]);
        let service = LlmService::new(store.clone(), backend);

        let result = service
            .prompt(prompt_request("main", "use delegated skill"))
            .await
            .unwrap();

        let ancestry = store.ancestry("main").unwrap();
        let assistant = &ancestry[0];
        let handoff_anchor = &ancestry[1];

        assert!(matches!(&assistant.kind, Kind::Text(text) if text == "base done"));
        let Kind::Anchor(anchor) = &handoff_anchor.kind else {
            panic!("expected handoff skill result anchor");
        };
        let skill_result = anchor
            .as_skill_result()
            .expect("expected skill result anchor");
        assert_eq!(skill_result.tool_id, "tool-call-1");
        assert_eq!(skill_result.skill_name, "find-skills");
        assert_eq!(skill_result.output, "Skill handoff");
        assert_eq!(anchor.merge_parent_node_ids(), [draft_head.as_str()]);
        assert_eq!(
            store.get_session_state("draft").unwrap(),
            SessionState::Attached {
                target_branch: "main".to_owned(),
                base_head_id: base_session.anchor_id.clone(),
            }
        );
        assert_eq!(assistant.parent, handoff_anchor.id);
        assert_eq!(result.text, "base done");
    }

    #[tokio::test]
    async fn resolve_session_keeps_tool_entries_but_text_history_stays_text_only() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_completions(&[(
            "main",
            &[Ok(BackendRun::succeeded_with_steps(
                "done",
                vec![
                    BackendStep {
                        execution: ExecutionMetadata::new("execution-step-1".to_owned()),
                        events: vec![backend_event(
                            BackendEventPayload::ToolUse(ToolUse {
                                id: "tool-call-1".to_owned(),
                                call_id: None,
                                name: "exec_command".to_owned(),
                                input: serde_json::json!({"cmd": "rg --files"}),
                            }),
                            Some("execution-step-1"),
                            Some("tool-call-1"),
                        )],
                    },
                    BackendStep {
                        execution: ExecutionMetadata::new("execution-step-2".to_owned()),
                        events: vec![
                            backend_event(
                                BackendEventPayload::ToolResult(ToolResult {
                                    id: "tool-call-1".to_owned(),
                                    call_id: None,
                                    output: "Cargo.toml".to_owned(),
                                }),
                                Some("execution-step-2"),
                                Some("tool-call-1"),
                            ),
                            BackendEventPayload::AssistantText("done".to_owned()).into(),
                        ],
                    },
                ],
            ))],
        )]);
        let service = LlmService::new(store, backend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();
        service
            .prompt(prompt_request("main", "list files"))
            .await
            .unwrap();

        let session = service.resolve_session("main").unwrap();
        assert_eq!(
            text_messages_from_provider_history(&session.provider_history),
            vec![
                (Role::User, "Conversation start.".to_owned()),
                (Role::User, "list files".to_owned()),
                (Role::LLM, "done".to_owned()),
            ]
        );
        assert!(matches!(
            &session.provider_history[2],
            rig::completion::message::Message::Assistant { content, .. }
                if matches!(
                    content.first_ref(),
                    rig::completion::message::AssistantContent::ToolCall(tool_call)
                        if tool_call.id == "tool-call-1"
                )
        ));
        assert!(matches!(
            &session.provider_history[3],
            rig::completion::message::Message::User { content }
                if matches!(
                    content.first_ref(),
                    rig::completion::message::UserContent::ToolResult(tool_result)
                        if tool_result.id == "tool-call-1"
                )
        ));
    }

    #[test]
    fn rig_messages_from_nodes_groups_tool_calls_and_results_by_turn() {
        let messages = rig_messages_from_nodes(&[
            context_node(Role::User, None, Kind::Text("list files".to_owned())),
            context_node(
                Role::LLM,
                metadata(Some("execution-1"), None),
                Kind::Text("checking".to_owned()),
            ),
            context_node(
                Role::LLM,
                metadata(Some("execution-1"), Some("call-1")),
                Kind::tool_use(ToolUse {
                    id: "tool-call-1".to_owned(),
                    call_id: None,
                    name: "exec_command".to_owned(),
                    input: serde_json::json!({"cmd": "ls"}),
                }),
            ),
            context_node(
                Role::LLM,
                metadata(Some("execution-1"), Some("call-2")),
                Kind::tool_use(ToolUse {
                    id: "tool-call-2".to_owned(),
                    call_id: None,
                    name: "exec_command".to_owned(),
                    input: serde_json::json!({"cmd": "pwd"}),
                }),
            ),
            context_node(
                Role::User,
                metadata(Some("execution-1"), Some("call-1")),
                Kind::tool_result(ToolResult {
                    id: "tool-call-1".to_owned(),
                    call_id: None,
                    output: "Cargo.toml".to_owned(),
                }),
            ),
            context_node(
                Role::User,
                metadata(Some("execution-1"), Some("call-2")),
                Kind::tool_result(ToolResult {
                    id: "tool-call-2".to_owned(),
                    call_id: None,
                    output: "/tmp".to_owned(),
                }),
            ),
            context_node(
                Role::LLM,
                metadata(Some("execution-2"), None),
                Kind::Text("done".to_owned()),
            ),
        ]);

        assert_eq!(messages.len(), 4);
        assert!(matches!(
            &messages[0],
            rig::completion::message::Message::User { content }
                if matches!(
                    content.first_ref(),
                    rig::completion::message::UserContent::Text(text) if text.text == "list files"
                )
        ));
        assert!(matches!(
            &messages[1],
            rig::completion::message::Message::Assistant { content, .. }
                if {
                    let items = content.iter().collect::<Vec<_>>();
                    items.len() == 3
                    && matches!(
                        items[0],
                        rig::completion::message::AssistantContent::Text(text)
                            if text.text == "checking"
                    )
                    && matches!(
                        items[1],
                        rig::completion::message::AssistantContent::ToolCall(tool_call)
                            if tool_call.id == "tool-call-1"
                                && tool_call.call_id.as_deref() == Some("call-1")
                    )
                    && matches!(
                        items[2],
                        rig::completion::message::AssistantContent::ToolCall(tool_call)
                            if tool_call.id == "tool-call-2"
                                && tool_call.call_id.as_deref() == Some("call-2")
                    )
                }
        ));
        assert!(matches!(
            &messages[2],
            rig::completion::message::Message::User { content }
                if {
                    let items = content.iter().collect::<Vec<_>>();
                    items.len() == 2
                    && matches!(
                        items[0],
                        rig::completion::message::UserContent::ToolResult(tool_result)
                            if tool_result.id == "tool-call-1"
                                && tool_result.call_id.as_deref() == Some("call-1")
                    )
                    && matches!(
                        items[1],
                        rig::completion::message::UserContent::ToolResult(tool_result)
                            if tool_result.id == "tool-call-2"
                                && tool_result.call_id.as_deref() == Some("call-2")
                    )
                }
        ));
        assert!(matches!(
            &messages[3],
            rig::completion::message::Message::Assistant { content, .. }
                if matches!(
                    content.first_ref(),
                    rig::completion::message::AssistantContent::Text(text) if text.text == "done"
                )
        ));
    }

    #[test]
    fn rig_messages_from_nodes_preserves_call_id() {
        let messages = rig_messages_from_nodes(&[
            context_node(
                Role::LLM,
                metadata(Some("execution-1"), Some("call-legacy")),
                Kind::tool_use(ToolUse {
                    id: "tool-call-legacy".to_owned(),
                    call_id: None,
                    name: "exec_command".to_owned(),
                    input: serde_json::json!({"cmd": "pwd"}),
                }),
            ),
            context_node(
                Role::User,
                metadata(Some("execution-1"), Some("call-legacy")),
                Kind::tool_result(ToolResult {
                    id: "tool-call-legacy".to_owned(),
                    call_id: None,
                    output: "/tmp".to_owned(),
                }),
            ),
        ]);

        assert!(matches!(
            &messages[0],
            rig::completion::message::Message::Assistant { content, .. }
                if matches!(
                    content.first_ref(),
                    rig::completion::message::AssistantContent::ToolCall(tool_call)
                        if tool_call.id == "tool-call-legacy"
                            && tool_call.call_id.as_deref() == Some("call-legacy")
                )
        ));
        assert!(matches!(
            &messages[1],
            rig::completion::message::Message::User { content }
                if matches!(
                    content.first_ref(),
                    rig::completion::message::UserContent::ToolResult(tool_result)
                        if tool_result.id == "tool-call-legacy"
                            && tool_result.call_id.as_deref() == Some("call-legacy")
                )
        ));
    }

    #[test]
    fn rig_messages_from_nodes_uses_payload_call_id_without_metadata_call_id() {
        let messages = rig_messages_from_nodes(&[
            context_node(
                Role::LLM,
                metadata(Some("execution-1"), None),
                Kind::tool_use(ToolUse {
                    id: "tool-call-payload".to_owned(),
                    call_id: Some("call-payload".to_owned()),
                    name: "exec_command".to_owned(),
                    input: serde_json::json!({"cmd": "pwd"}),
                }),
            ),
            context_node(
                Role::User,
                metadata(Some("execution-1"), None),
                Kind::tool_result(ToolResult {
                    id: "tool-call-payload".to_owned(),
                    call_id: Some("call-payload".to_owned()),
                    output: "/tmp".to_owned(),
                }),
            ),
        ]);

        assert!(matches!(
            &messages[0],
            rig::completion::message::Message::Assistant { content, .. }
                if matches!(
                    content.first_ref(),
                    rig::completion::message::AssistantContent::ToolCall(tool_call)
                        if tool_call.call_id.as_deref() == Some("call-payload")
                )
        ));
        assert!(matches!(
            &messages[1],
            rig::completion::message::Message::User { content }
                if matches!(
                    content.first_ref(),
                    rig::completion::message::UserContent::ToolResult(tool_result)
                        if tool_result.call_id.as_deref() == Some("call-payload")
                )
        ));
    }

    #[test]
    fn rig_messages_from_nodes_omits_call_id_when_absent() {
        let messages = rig_messages_from_nodes(&[
            context_node(
                Role::LLM,
                metadata(Some("execution-1"), None),
                Kind::tool_use(ToolUse {
                    id: "tool-call-legacy".to_owned(),
                    call_id: None,
                    name: "exec_command".to_owned(),
                    input: serde_json::json!({"cmd": "pwd"}),
                }),
            ),
            context_node(
                Role::User,
                metadata(Some("execution-1"), None),
                Kind::tool_result(ToolResult {
                    id: "tool-call-legacy".to_owned(),
                    call_id: None,
                    output: "/tmp".to_owned(),
                }),
            ),
        ]);

        assert!(matches!(
            &messages[0],
            rig::completion::message::Message::Assistant { content, .. }
                if matches!(
                    content.first_ref(),
                    rig::completion::message::AssistantContent::ToolCall(tool_call)
                        if tool_call.id == "tool-call-legacy"
                            && tool_call.call_id.is_none()
                )
        ));
        assert!(matches!(
            &messages[1],
            rig::completion::message::Message::User { content }
                if matches!(
                    content.first_ref(),
                    rig::completion::message::UserContent::ToolResult(tool_result)
                        if tool_result.id == "tool-call-legacy"
                            && tool_result.call_id.is_none()
                )
        ));
    }

    #[test]
    fn backend_events_from_choice_preserves_optional_call_id() {
        let choice = rig::OneOrMany::many(vec![
            rig::message::AssistantContent::tool_call(
                "tool-call-1",
                "exec_command",
                serde_json::json!({"cmd": "pwd"}),
            ),
            rig::message::AssistantContent::tool_call_with_call_id(
                "tool-call-2",
                "call-2".to_owned(),
                "exec_command",
                serde_json::json!({"cmd": "ls"}),
            ),
        ])
        .expect("assistant choice should be non-empty");

        let events = backend_events_from_choice(&choice);

        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            BackendEvent {
                metadata: Some(ProviderMetadata { call_id }),
                event: BackendEventPayload::ToolUse(ToolUse { id, .. }),
            } if id == "tool-call-1" && call_id.is_none()
        ));
        assert!(matches!(
            &events[1],
            BackendEvent {
                metadata: Some(ProviderMetadata { call_id }),
                event: BackendEventPayload::ToolUse(ToolUse { id, .. }),
            } if id == "tool-call-2" && call_id.as_deref() == Some("call-2")
        ));
    }

    #[tokio::test]
    async fn multi_step_completion_uses_distinct_execution_ids_per_completion_call() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_completions(&[(
            "main",
            &[Ok(BackendRun::succeeded_with_steps(
                "done",
                vec![
                    BackendStep {
                        execution: ExecutionMetadata::new("execution-step-1".to_owned()),
                        events: vec![
                            backend_event(
                                BackendEventPayload::ToolUse(ToolUse {
                                    id: "tool-call-1".to_owned(),
                                    call_id: None,
                                    name: "exec_command".to_owned(),
                                    input: serde_json::json!({"cmd": "ls"}),
                                }),
                                Some("execution-step-1"),
                                Some("tool-call-1"),
                            ),
                            backend_event(
                                BackendEventPayload::ToolResult(ToolResult {
                                    id: "tool-call-1".to_owned(),
                                    call_id: None,
                                    output: "Cargo.toml".to_owned(),
                                }),
                                Some("execution-step-1"),
                                Some("tool-call-1"),
                            ),
                        ],
                    },
                    BackendStep {
                        execution: ExecutionMetadata::new("execution-step-2".to_owned()),
                        events: vec![BackendEventPayload::AssistantText("done".to_owned()).into()],
                    },
                ],
            ))],
        )]);
        let service = LlmService::new(store.clone(), backend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();

        let result = service
            .prompt(prompt_request("main", "list files"))
            .await
            .unwrap();

        let ancestry = store.ancestry("main").unwrap();
        let assistant = &ancestry[0];
        let tool_result = &ancestry[1];
        let tool_use = &ancestry[2];

        assert_eq!(
            assistant.metadata.as_ref().unwrap().execution_id.as_deref(),
            Some("execution-step-2")
        );
        assert_eq!(
            tool_result
                .metadata
                .as_ref()
                .unwrap()
                .execution_id
                .as_deref(),
            Some("execution-step-1")
        );
        assert_eq!(
            tool_use.metadata.as_ref().unwrap().execution_id.as_deref(),
            Some("execution-step-1")
        );
        assert_eq!(result.execution_id, "execution-step-2");

        let session = service.resolve_session("main").unwrap();
        assert_eq!(session.provider_history.len(), 5);
        assert!(matches!(
            &session.provider_history[2],
            rig::completion::message::Message::Assistant { content, .. }
                if matches!(
                    content.first_ref(),
                    rig::completion::message::AssistantContent::ToolCall(tool_call)
                        if tool_call.id == "tool-call-1"
                )
        ));
        assert!(matches!(
            &session.provider_history[3],
            rig::completion::message::Message::User { content }
                if matches!(
                    content.first_ref(),
                    rig::completion::message::UserContent::ToolResult(tool_result)
                        if tool_result.id == "tool-call-1"
                )
        ));
        assert!(matches!(
            &session.provider_history[4],
            rig::completion::message::Message::Assistant { content, .. }
                if matches!(
                    content.first_ref(),
                    rig::completion::message::AssistantContent::Text(text) if text.text == "done"
                )
        ));
    }

    #[tokio::test]
    async fn failed_completion_persists_partial_trace_as_orphan_chain() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_completions(&[(
            "main",
            &[Ok(BackendRun::failed_with_steps(
                "MaxTurnError: (reached max turn limit: 8)",
                vec![
                    BackendStep {
                        execution: ExecutionMetadata::new("execution-step-1".to_owned()),
                        events: vec![backend_event(
                            BackendEventPayload::ToolUse(ToolUse {
                                id: "tool-call-1".to_owned(),
                                call_id: None,
                                name: "exec_command".to_owned(),
                                input: serde_json::json!({"cmd": "printf 'hello' > trace.txt"}),
                            }),
                            Some("execution-step-1"),
                            Some("tool-call-1"),
                        )],
                    },
                    BackendStep {
                        execution: ExecutionMetadata::new("execution-step-2".to_owned()),
                        events: vec![
                            backend_event(
                                BackendEventPayload::ToolResult(ToolResult {
                                    id: "tool-call-1".to_owned(),
                                    call_id: None,
                                    output: "exit_status: 0\nstdout:\n\nstderr:\n".to_owned(),
                                }),
                                Some("execution-step-2"),
                                Some("tool-call-1"),
                            ),
                            BackendEventPayload::AssistantText("trying again".to_owned()).into(),
                        ],
                    },
                ],
            ))],
        )]);
        let service = LlmService::new(store.clone(), backend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();

        let err = service
            .prompt(prompt_request("main", "keep going"))
            .await
            .unwrap_err();
        let context = match err {
            Error::Backend { context, .. } => context,
            other => panic!("expected backend error, got {other:?}"),
        };

        assert_eq!(
            store.get_branch_head("main").unwrap(),
            context.retry_from_node_id
        );

        let failure = store.get_node(&context.error_node_id).unwrap();
        assert!(matches!(
            &failure.kind,
            Kind::Failure(text) if text == "MaxTurnError: (reached max turn limit: 8)"
        ));

        let assistant = store.get_node(&failure.parent).unwrap();
        assert!(matches!(&assistant.kind, Kind::Text(text) if text == "trying again"));

        let tool_result = store.get_node(&assistant.parent).unwrap();
        assert!(kind_has_tool_result(
            &tool_result.kind,
            "tool-call-1",
            "exit_status: 0\nstdout:\n\nstderr:\n"
        ));

        let tool_use = store.get_node(&tool_result.parent).unwrap();
        assert!(kind_has_tool_use(
            &tool_use.kind,
            "tool-call-1",
            "exec_command"
        ));
        assert_eq!(tool_use.parent, context.retry_from_node_id);

        let session = service.resolve_session("main").unwrap();
        assert_eq!(
            text_messages_from_provider_history(&session.provider_history),
            vec![
                (Role::User, "Conversation start.".to_owned()),
                (Role::User, "keep going".to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn streamed_failure_keeps_branch_head_at_retry_point() {
        let store = MemoryStore::new();
        let service = LlmService::new(store.clone(), StreamingFailBackend);
        service
            .create_session(session_config("main"))
            .await
            .unwrap();

        let err = service
            .prompt(prompt_request("main", "keep going"))
            .await
            .unwrap_err();
        let context = match err {
            Error::Backend { context, .. } => context,
            other => panic!("expected backend error, got {other:?}"),
        };

        assert_eq!(
            store.get_branch_head("main").unwrap(),
            context.retry_from_node_id
        );

        let failure = store.get_node(&context.error_node_id).unwrap();
        let assistant = store.get_node(&failure.parent).unwrap();
        let tool_result = store.get_node(&assistant.parent).unwrap();
        let tool_use = store.get_node(&tool_result.parent).unwrap();

        assert!(tool_use.created_at < tool_result.created_at);
        assert!(tool_result.created_at < assistant.created_at);
        assert!(assistant.created_at < failure.created_at);
        assert_eq!(tool_use.parent, context.retry_from_node_id);
    }

    #[tokio::test]
    async fn exec_command_runtime_allows_writes_within_configured_workspace() {
        let temp_root = tempfile::tempdir().unwrap();
        let runtime = unified_exec_tool::runtime_tool(
            exec_command_tool(),
            temp_root.path().to_path_buf(),
            ToolRuntimeEnv {
                session_branch: "main".to_owned(),
                session_role: SessionRole::Orchestrator,
                current_skill_name: None,
                active_skill: None,
                store_path: None,
                enable_coco_shim: false,
                cli_bridge: UnifiedExecCliBridgeHandle::default(),
                skill_executor: SkillToolExecutorHandle::default(),
            },
        );
        let output = with_process_env_async(
            &[("COCO_EXEC_SANDBOX", Some(OsStr::new("off")))],
            || async {
                runtime
                    .call(format!(
                        r#"{{"cmd":"printf 'hello' > trace.txt; cat trace.txt","workdir":"{}"}}"#,
                        temp_root.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();
        assert!(output.contains("exit_status: 0"));
        assert!(output.contains("stdout:\nhello"));
        assert_eq!(
            std::fs::read_to_string(temp_root.path().join("trace.txt")).unwrap(),
            "hello"
        );
    }
}
