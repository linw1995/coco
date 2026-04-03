mod bash_tool;
mod tool_definition;

use std::collections::HashMap;
#[cfg(test)]
use std::ffi::OsStr;
use std::path::PathBuf;
use std::str::FromStr;
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::Arc;

use async_trait::async_trait;
use coco_mem::{
    Anchor, AnchorPayload, Kind, MemoryStore, NewNode, NodeMetadata, PauseReason, PromptAnchor,
    Role, SessionAnchor, SessionState, Store, StoreError, Tool, ToolResult, ToolUse,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::IntoError;
use snafu::prelude::*;
use tokio::sync::{Mutex, OwnedMutexGuard};

pub use coco_mem;
pub use coco_mem::SessionAnchorPatch as SessionConfigPatch;
pub use tool_definition::builtin_tool_definition;

pub const COCO_SESSION_BRANCH_ENV: &str = "COCO_BRANCH";
pub const COCO_STORE_PATH_ENV: &str = "COCO_STORE_PATH";
pub const COCO_CLI_RUNTIME_SOCKET_ENV: &str = "COCO_CLI_RUNTIME_SOCKET";

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
}

impl Provider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Anthropic => "anthropic",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "openai" => Ok(Self::OpenAi),
            "anthropic" => Ok(Self::Anthropic),
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
    pub merge_parents: Vec<String>,
    pub provider: Provider,
    pub model: String,
    pub system_prompt: String,
    pub prompt: String,
    pub tools: Vec<Tool>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub additional_params: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct PromptRequest {
    pub branch: String,
    pub prompt: String,
    pub merge_parents: Vec<String>,
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
    pub provider: Option<Provider>,
    pub model: Option<String>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub additional_params: Option<Value>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageRole {
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationMessage {
    pub role: MessageRole,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConversationEntry {
    Message(ConversationMessage),
    ToolUse(ToolUse),
    ToolResult(ToolResult),
}

#[derive(Debug, Clone, PartialEq)]
struct TrackedConversationEntry {
    execution_id: Option<String>,
    entry: ConversationEntry,
}

#[derive(Debug, Clone)]
pub struct SessionModelConfig {
    pub provider: Provider,
    pub model: String,
    pub system_prompt: String,
    pub tools: Vec<Tool>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub additional_params: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct ResolvedSession {
    pub branch: String,
    pub anchor_id: String,
    pub config: SessionModelConfig,
    pub conversation: Vec<ConversationEntry>,
    pub provider_history: Vec<rig::completion::message::Message>,
    pub bash_tool_context: BashToolContext,
}

#[derive(Debug, Clone)]
pub struct ResolvedCompletionRequest {
    pub branch: String,
    pub provider: Provider,
    pub model: String,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub additional_params: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CocoCliRuntimeRequest {
    pub args: Vec<String>,
    pub stdin: Vec<u8>,
    pub branch_env: Option<String>,
    pub store_path_env: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CocoCliRuntimeResponse {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Snafu, Clone, PartialEq, Eq)]
pub enum BashToolCliBridgeError {
    #[snafu(display("coco-cli runtime bridge is unavailable"))]
    Unavailable,
}

#[async_trait]
pub trait BashToolCliBridge: Send + Sync {
    async fn execute_coco_cli(
        &self,
        request: CocoCliRuntimeRequest,
    ) -> std::result::Result<CocoCliRuntimeResponse, BashToolCliBridgeError>;
}

#[derive(Clone)]
pub struct BashToolCliBridgeHandle {
    inner: Arc<dyn BashToolCliBridge>,
}

impl BashToolCliBridgeHandle {
    pub fn new(inner: Arc<dyn BashToolCliBridge>) -> Self {
        Self { inner }
    }

    pub async fn execute_coco_cli(
        &self,
        request: CocoCliRuntimeRequest,
    ) -> std::result::Result<CocoCliRuntimeResponse, BashToolCliBridgeError> {
        self.inner.execute_coco_cli(request).await
    }
}

impl std::fmt::Debug for BashToolCliBridgeHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BashToolCliBridgeHandle(..)")
    }
}

#[derive(Debug, Clone)]
pub struct BashToolContext {
    pub session_branch: String,
    pub store_path: Option<PathBuf>,
    pub cli_bridge: Option<BashToolCliBridgeHandle>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BackendEvent {
    AssistantText(String),
    ToolUse(ToolUse),
    ToolResult(ToolResult),
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackendStep {
    pub execution_id: String,
    pub events: Vec<BackendEvent>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackendRun {
    pub steps: Vec<BackendStep>,
    pub outcome: BackendOutcome,
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
        }
    }

    pub fn succeeded(text: impl Into<String>, events: Vec<BackendEvent>) -> Self {
        Self::succeeded_with_steps(
            text,
            vec![BackendStep {
                execution_id: format!("execution-{}", nanoid::nanoid!()),
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
        }
    }

    pub fn failed(message: impl Into<String>, events: Vec<BackendEvent>) -> Self {
        Self::failed_with_steps(
            message,
            vec![BackendStep {
                execution_id: format!("execution-{}", nanoid::nanoid!()),
                events,
            }],
        )
    }
}

#[derive(Debug, Snafu, Clone, PartialEq, Eq)]
pub enum BackendError {
    #[snafu(display("{message}"))]
    Failed { message: String },

    #[snafu(display("{message}"))]
    BashTool { message: String },
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
    async fn complete(
        &self,
        session: ResolvedSession,
        request: ResolvedCompletionRequest,
    ) -> std::result::Result<BackendRun, BackendError>;
}

type BranchLockTable = Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>;
type WorkflowLock = Arc<Mutex<()>>;

pub struct LlmService<B = RigBackend, S = MemoryStore>
where
    S: Store,
{
    store: S,
    backend: B,
    bash_tool_cli_bridge: Option<BashToolCliBridgeHandle>,
    branch_locks: BranchLockTable,
    workflow_lock: WorkflowLock,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Memory operation failed: {source}"))]
    Memory { source: coco_mem::StoreError },

    #[snafu(display("Branch {branch:?} has no session anchor"))]
    MissingAnchor { branch: String },

    #[snafu(display("Anchor {anchor_id:?} is not a conversation anchor"))]
    InvalidAnchor { anchor_id: String },

    #[snafu(display("Unknown provider {provider:?}"))]
    UnknownProvider { provider: String },

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
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
struct ResolvedContext {
    active_anchor_id: String,
    session_anchor: SessionAnchor,
    tail_entries: Vec<TrackedConversationEntry>,
}

#[derive(Debug, Clone, Default)]
pub struct RigBackend;

impl LlmService<RigBackend, MemoryStore> {
    pub fn with_store(store: MemoryStore) -> Self {
        Self::new(store, RigBackend)
    }
}

impl<B, S> LlmService<B, S>
where
    B: CompletionBackend,
    S: Store,
{
    pub fn new(store: S, backend: B) -> Self {
        Self {
            store,
            backend,
            bash_tool_cli_bridge: None,
            branch_locks: Arc::new(Mutex::new(HashMap::new())),
            workflow_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn with_bash_tool_cli_bridge(mut self, bridge: BashToolCliBridgeHandle) -> Self {
        self.bash_tool_cli_bridge = Some(bridge);
        self
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub async fn rebase_session(&self, branch: &str, patch: SessionConfigPatch) -> Result<String> {
        let _guard = self.lock_branch(branch).await;
        self.store
            .rebase_session(branch, &patch)
            .context(MemorySnafu)
    }

    pub async fn create_session(&self, config: SessionConfig) -> Result<BranchSession> {
        let _guard = self.lock_branch(&config.branch).await;
        let root_id = self.store.root_id();
        let merge_parents = config
            .merge_parents
            .iter()
            .map(|reference| {
                self.store
                    .ancestry(reference)
                    .map(|nodes| {
                        nodes
                            .into_iter()
                            .next()
                            .expect("ancestry should always include the head node")
                            .id
                    })
                    .context(MemorySnafu)
            })
            .collect::<Result<Vec<_>>>()?;
        let anchor_id = self
            .store
            .append(NewNode {
                parent: root_id,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(
                    merge_parents,
                    SessionAnchor {
                        provider: config.provider.as_str().to_owned(),
                        model: config.model,
                        tools: config.tools,
                        system_prompt: config.system_prompt,
                        prompt: config.prompt,
                        temperature: config.temperature,
                        max_tokens: config.max_tokens,
                        additional_params: config.additional_params,
                    },
                )),
            })
            .context(MemorySnafu)?;
        self.store
            .fork(&config.branch, &anchor_id)
            .context(MemorySnafu)?;

        Ok(BranchSession {
            branch: config.branch,
            anchor_id,
        })
    }

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

        Ok(PullRequest {
            branch: branch.to_owned(),
            target_branch: target_branch.to_owned(),
            base_head_id,
        })
    }

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
        let merged_anchor_id = self.append_prompt_anchor_to_branch(
            &resolved_target_branch,
            prompt,
            std::slice::from_ref(&source_head_id),
        )?;
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
                Err(source) => return Err(Error::Memory { source }),
            }
        }

        let feedback_anchor_id = self.append_prompt_anchor_to_branch(
            branch,
            prompt,
            std::slice::from_ref(&source_anchor_id),
        )?;
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

        Ok(SessionFeedback {
            branch: branch.to_owned(),
            target_branch,
            base_head_id: source_anchor_id.clone(),
            source_anchor_id,
            feedback_anchor_id,
        })
    }

    pub async fn prompt(&self, request: PromptRequest) -> Result<CompletionResult> {
        let _guard = self.lock_branch(&request.branch).await;
        self.append_prompt_anchor_to_branch(
            &request.branch,
            &request.prompt,
            &request.merge_parents,
        )?;
        self.complete_locked(CompletionRequest {
            branch: request.branch,
            provider: None,
            model: None,
            temperature: None,
            max_tokens: None,
            additional_params: None,
        })
        .await
    }

    fn append_prompt_anchor_to_branch(
        &self,
        branch: &str,
        prompt: &str,
        merge_parents: &[String],
    ) -> Result<String> {
        let original_head = self.store.get_branch_head(branch).context(MemorySnafu)?;
        let merge_parents = merge_parents
            .iter()
            .map(|reference| {
                self.store
                    .ancestry(reference)
                    .map(|nodes| {
                        nodes
                            .into_iter()
                            .next()
                            .expect("ancestry should always include the head node")
                            .id
                    })
                    .context(MemorySnafu)
            })
            .collect::<Result<Vec<_>>>()?;
        let anchor_id = self
            .store
            .append(NewNode {
                parent: original_head.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    merge_parents,
                    PromptAnchor {
                        prompt: prompt.to_owned(),
                    },
                )),
            })
            .context(MemorySnafu)?;
        self.store
            .set_branch_head(branch, &original_head, &anchor_id)
            .context(MemorySnafu)?;

        Ok(anchor_id)
    }

    pub fn fork(&self, branch: impl Into<String>, from_ref: &str) -> Result<String> {
        let branch = branch.into();
        self.store.fork(&branch, from_ref).context(MemorySnafu)
    }

    pub async fn complete(&self, request: CompletionRequest) -> Result<CompletionResult> {
        let _guard = self.lock_branch(&request.branch).await;
        self.complete_locked(request).await
    }

    async fn complete_locked(&self, request: CompletionRequest) -> Result<CompletionResult> {
        let original_head = self
            .store
            .get_branch_head(&request.branch)
            .context(MemorySnafu)?;
        let session = self.resolve_session(&request.branch)?;
        let resolved = self.resolve_request(&session, request.clone());

        match self
            .backend
            .complete(session.clone(), resolved.clone())
            .await
        {
            Ok(run) => match run.outcome {
                BackendOutcome::Succeeded { text } => {
                    let (last_execution_id, steps) =
                        normalize_backend_steps(run.steps, Some(text.clone()));
                    let parent_id = self
                        .append_backend_steps(original_head.clone(), &steps)
                        .context(MemorySnafu)?;
                    self.store
                        .set_branch_head(&resolved.branch, &original_head, &parent_id)
                        .context(MemorySnafu)?;

                    Ok(CompletionResult {
                        branch: resolved.branch,
                        anchor_id: session.anchor_id,
                        execution_id: last_execution_id,
                        response_node_id: parent_id.clone(),
                        branch_head: parent_id,
                        text,
                    })
                }
                BackendOutcome::Failed { message } => {
                    let (execution_id, steps) = normalize_backend_steps(run.steps, None);
                    let partial_history_tail = self
                        .append_backend_steps(original_head.clone(), &steps)
                        .context(MemorySnafu)?;
                    let error_node_id = self
                        .store
                        .append(NewNode {
                            parent: partial_history_tail,
                            role: Role::System,
                            metadata: Some(NodeMetadata::execution(execution_id.clone())),
                            kind: Kind::Failure(message.clone()),
                        })
                        .context(MemorySnafu)?;
                    Err(BackendSnafu {
                        context: Box::new(BackendFailureContext {
                            branch: resolved.branch,
                            execution_id,
                            error_node_id,
                            retry_from_node_id: original_head,
                        }),
                    }
                    .into_error(BackendError::failed(message)))
                }
            },
            Err(source) => {
                let execution_id = format!("execution-{}", nanoid::nanoid!());
                let error_node_id = self
                    .store
                    .append(NewNode {
                        parent: original_head.clone(),
                        role: Role::System,
                        metadata: Some(NodeMetadata::execution(execution_id.clone())),
                        kind: Kind::Failure(source.to_string()),
                    })
                    .context(MemorySnafu)?;
                Err(BackendSnafu {
                    context: Box::new(BackendFailureContext {
                        branch: resolved.branch,
                        execution_id,
                        error_node_id,
                        retry_from_node_id: original_head,
                    }),
                }
                .into_error(source))
            }
        }
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

    fn append_backend_events(
        &self,
        parent_id: String,
        metadata: &NodeMetadata,
        events: &[BackendEvent],
    ) -> std::result::Result<String, StoreError> {
        let mut parent_id = parent_id;
        for event in events {
            let (role, kind) = match event.clone() {
                BackendEvent::AssistantText(text) => (Role::LLM, Kind::Text(text)),
                BackendEvent::ToolUse(tool_use) => (Role::LLM, Kind::ToolUse(tool_use)),
                BackendEvent::ToolResult(tool_result) => {
                    (Role::User, Kind::ToolResult(tool_result))
                }
            };
            parent_id = self.store.append(NewNode {
                parent: parent_id,
                role,
                metadata: Some(metadata.clone()),
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
            let metadata = NodeMetadata::execution(step.execution_id.clone());
            parent_id = self.append_backend_events(parent_id, &metadata, &step.events)?;
        }

        Ok(parent_id)
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

    fn resolve_session(&self, branch: &str) -> Result<ResolvedSession> {
        let context = self.resolve_context(branch)?;
        let mut conversation = Vec::new();
        let mut tracked_entries = Vec::new();
        if !context.session_anchor.prompt.is_empty() {
            let entry = ConversationEntry::Message(ConversationMessage {
                role: MessageRole::User,
                text: context.session_anchor.prompt.clone(),
            });
            conversation.push(entry.clone());
            tracked_entries.push(TrackedConversationEntry {
                execution_id: None,
                entry,
            });
        }
        conversation.extend(context.tail_entries.iter().map(|entry| entry.entry.clone()));
        tracked_entries.extend(context.tail_entries);

        Ok(ResolvedSession {
            branch: branch.to_owned(),
            anchor_id: context.active_anchor_id,
            config: SessionModelConfig {
                provider: Provider::parse(&context.session_anchor.provider)?,
                model: context.session_anchor.model.clone(),
                system_prompt: context.session_anchor.system_prompt.clone(),
                tools: context.session_anchor.tools.clone(),
                temperature: context.session_anchor.temperature,
                max_tokens: context.session_anchor.max_tokens,
                additional_params: context.session_anchor.additional_params.clone(),
            },
            conversation,
            provider_history: rig_messages_from_tracked_entries(&tracked_entries),
            bash_tool_context: BashToolContext {
                session_branch: branch.to_owned(),
                store_path: self.store.runtime_store_path(),
                cli_bridge: self.bash_tool_cli_bridge.clone(),
            },
        })
    }

    fn resolve_context(&self, reference: &str) -> Result<ResolvedContext> {
        let ordered: Vec<_> = self
            .store
            .ancestry(reference)
            .context(MemorySnafu)?
            .into_iter()
            .rev()
            .collect();

        let mut state: Option<ResolvedContext> = None;

        for node in ordered {
            match &node.kind {
                Kind::Anchor(anchor) => match &anchor.payload {
                    AnchorPayload::Session(session_anchor) => {
                        state = Some(ResolvedContext {
                            active_anchor_id: node.id.clone(),
                            session_anchor: session_anchor.clone(),
                            tail_entries: vec![],
                        });
                    }
                    AnchorPayload::Prompt(prompt_anchor) => {
                        let Some(context) = state.as_mut() else {
                            return MissingAnchorSnafu {
                                branch: reference.to_owned(),
                            }
                            .fail();
                        };

                        if !prompt_anchor.prompt.is_empty() {
                            context.tail_entries.push(TrackedConversationEntry {
                                execution_id: None,
                                entry: ConversationEntry::Message(ConversationMessage {
                                    role: MessageRole::User,
                                    text: prompt_anchor.prompt.clone(),
                                }),
                            });
                        }
                        context.active_anchor_id = node.id.clone();
                    }
                },
                Kind::Text(text) => {
                    let Some(context) = state.as_mut() else {
                        continue;
                    };

                    let role = match node.role {
                        Role::User => Some(MessageRole::User),
                        Role::LLM => Some(MessageRole::Assistant),
                        Role::System => None,
                    };
                    if let Some(role) = role {
                        context.tail_entries.push(TrackedConversationEntry {
                            execution_id: node
                                .metadata
                                .as_ref()
                                .and_then(|metadata| metadata.execution_id.clone()),
                            entry: ConversationEntry::Message(ConversationMessage {
                                role,
                                text: text.clone(),
                            }),
                        });
                    }
                }
                Kind::ToolUse(tool_use) => {
                    let Some(context) = state.as_mut() else {
                        continue;
                    };
                    context.tail_entries.push(TrackedConversationEntry {
                        execution_id: node
                            .metadata
                            .as_ref()
                            .and_then(|metadata| metadata.execution_id.clone()),
                        entry: ConversationEntry::ToolUse(tool_use.clone()),
                    });
                }
                Kind::ToolResult(tool_result) => {
                    let Some(context) = state.as_mut() else {
                        continue;
                    };
                    context.tail_entries.push(TrackedConversationEntry {
                        execution_id: node
                            .metadata
                            .as_ref()
                            .and_then(|metadata| metadata.execution_id.clone()),
                        entry: ConversationEntry::ToolResult(tool_result.clone()),
                    });
                }
                Kind::Failure(_) => {}
            }
        }

        state.context(MissingAnchorSnafu {
            branch: reference.to_owned(),
        })
    }

    fn resolve_request(
        &self,
        session: &ResolvedSession,
        request: CompletionRequest,
    ) -> ResolvedCompletionRequest {
        ResolvedCompletionRequest {
            branch: request.branch,
            provider: request.provider.unwrap_or(session.config.provider),
            model: request
                .model
                .unwrap_or_else(|| session.config.model.clone()),
            temperature: request.temperature.or(session.config.temperature),
            max_tokens: request.max_tokens.or(session.config.max_tokens),
            additional_params: request
                .additional_params
                .or_else(|| session.config.additional_params.clone()),
        }
    }

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

fn build_runtime_tool_set(
    session: &ResolvedSession,
    workspace_root: std::path::PathBuf,
) -> std::result::Result<rig::tool::ToolSet, BackendError> {
    let runtime_tools = session
        .config
        .tools
        .iter()
        .map(|tool| match tool.name.as_str() {
            "bash" => Ok(bash_tool::runtime_tool(
                tool.clone(),
                workspace_root.clone(),
                session.bash_tool_context.clone(),
            )),
            other => Err(BackendError::failed(format!(
                "unsupported tool {other:?}; only \"bash\" is implemented"
            ))),
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(rig::tool::ToolSet::from_tools_boxed(runtime_tools))
}

fn configure_completion_request_builder<M>(
    mut builder: rig::completion::CompletionRequestBuilder<M>,
    session: &ResolvedSession,
    request: &ResolvedCompletionRequest,
    tool_definitions: Vec<rig::completion::ToolDefinition>,
) -> rig::completion::CompletionRequestBuilder<M>
where
    M: rig::completion::CompletionModel,
{
    if !session.config.system_prompt.is_empty() {
        builder = builder.preamble(session.config.system_prompt.clone());
    }
    if !tool_definitions.is_empty() {
        builder = builder.tools(tool_definitions);
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
) -> Vec<rig::completion::message::ToolCall> {
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
) -> rig::completion::message::Message {
    rig::completion::message::Message::User {
        content: rig::OneOrMany::many(tool_results).expect("there is atleast one tool result"),
    }
}

fn rig_messages_from_tracked_entries(
    entries: &[TrackedConversationEntry],
) -> Vec<rig::completion::message::Message> {
    fn flush_assistant_contents(
        messages: &mut Vec<rig::completion::message::Message>,
        assistant_contents: &mut Vec<rig::completion::message::AssistantContent>,
        assistant_execution_id: &mut Option<String>,
    ) {
        if assistant_contents.is_empty() {
            return;
        }
        messages.push(rig::completion::message::Message::Assistant {
            id: None,
            content: rig::OneOrMany::many(std::mem::take(assistant_contents))
                .expect("assistant content buffer is non-empty"),
        });
        *assistant_execution_id = None;
    }

    fn flush_tool_results(
        messages: &mut Vec<rig::completion::message::Message>,
        tool_results: &mut Vec<rig::completion::message::UserContent>,
        tool_result_execution_id: &mut Option<String>,
    ) {
        if tool_results.is_empty() {
            return;
        }
        messages.push(rig::completion::message::Message::User {
            content: rig::OneOrMany::many(std::mem::take(tool_results))
                .expect("tool result buffer is non-empty"),
        });
        *tool_result_execution_id = None;
    }

    let mut messages = Vec::new();
    let mut assistant_contents = Vec::new();
    let mut assistant_execution_id = None;
    let mut tool_results = Vec::new();
    let mut tool_result_execution_id = None;

    for tracked_entry in entries {
        match &tracked_entry.entry {
            ConversationEntry::Message(message) => match message.role {
                MessageRole::User => {
                    flush_assistant_contents(
                        &mut messages,
                        &mut assistant_contents,
                        &mut assistant_execution_id,
                    );
                    flush_tool_results(
                        &mut messages,
                        &mut tool_results,
                        &mut tool_result_execution_id,
                    );
                    messages.push(rig::completion::message::Message::user(
                        message.text.clone(),
                    ));
                }
                MessageRole::Assistant => {
                    flush_tool_results(
                        &mut messages,
                        &mut tool_results,
                        &mut tool_result_execution_id,
                    );
                    if !assistant_contents.is_empty()
                        && assistant_execution_id != tracked_entry.execution_id
                    {
                        flush_assistant_contents(
                            &mut messages,
                            &mut assistant_contents,
                            &mut assistant_execution_id,
                        );
                    }
                    assistant_execution_id = tracked_entry.execution_id.clone();
                    assistant_contents
                        .push(rig::message::AssistantContent::text(message.text.clone()));
                }
            },
            ConversationEntry::ToolUse(tool_use) => {
                flush_tool_results(
                    &mut messages,
                    &mut tool_results,
                    &mut tool_result_execution_id,
                );
                if !assistant_contents.is_empty()
                    && assistant_execution_id != tracked_entry.execution_id
                {
                    flush_assistant_contents(
                        &mut messages,
                        &mut assistant_contents,
                        &mut assistant_execution_id,
                    );
                }
                assistant_execution_id = tracked_entry.execution_id.clone();
                assistant_contents.push(rig::message::AssistantContent::tool_call(
                    tool_use.id.clone(),
                    tool_use.name.clone(),
                    tool_use.input.clone(),
                ));
            }
            ConversationEntry::ToolResult(tool_result) => {
                flush_assistant_contents(
                    &mut messages,
                    &mut assistant_contents,
                    &mut assistant_execution_id,
                );
                if !tool_results.is_empty()
                    && tool_result_execution_id != tracked_entry.execution_id
                {
                    flush_tool_results(
                        &mut messages,
                        &mut tool_results,
                        &mut tool_result_execution_id,
                    );
                }
                tool_result_execution_id = tracked_entry.execution_id.clone();
                tool_results.push(rig::completion::message::UserContent::tool_result(
                    tool_result.id.clone(),
                    rig::OneOrMany::one(rig::completion::message::ToolResultContent::text(
                        tool_result.output.clone(),
                    )),
                ));
            }
        }
    }

    flush_assistant_contents(
        &mut messages,
        &mut assistant_contents,
        &mut assistant_execution_id,
    );
    flush_tool_results(
        &mut messages,
        &mut tool_results,
        &mut tool_result_execution_id,
    );

    messages
}

fn push_assistant_text_event(buffer: &mut Vec<String>, events: &mut Vec<BackendEvent>) {
    if !buffer.is_empty() {
        events.push(BackendEvent::AssistantText(buffer.join("\n")));
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
                events.push(BackendEvent::ToolUse(ToolUse {
                    id: tool_call.id.clone(),
                    name: tool_call.function.name.clone(),
                    input: tool_call.function.arguments.clone(),
                }));
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
            execution_id: format!("execution-{}", nanoid::nanoid!()),
            events: vec![],
        });
    }

    let last_step = steps.last_mut().expect("steps is non-empty");

    if let Some(text) = final_text
        && !matches!(
            last_step.events.last(),
            Some(BackendEvent::AssistantText(last_text)) if last_text == &text
        )
    {
        last_step.events.push(BackendEvent::AssistantText(text));
    }

    (last_step.execution_id.clone(), steps)
}

async fn execute_completion_loop<M>(
    model: M,
    session: &ResolvedSession,
    request: &ResolvedCompletionRequest,
    prompt: rig::completion::message::Message,
    mut history: Vec<rig::completion::message::Message>,
    toolset: rig::tool::ToolSet,
) -> std::result::Result<BackendRun, BackendError>
where
    M: rig::completion::CompletionModel,
{
    fn next_execution_id() -> String {
        format!("execution-{}", nanoid::nanoid!())
    }

    let tool_definitions = toolset
        .get_tool_definitions()
        .await
        .map_err(|source| BackendError::failed(source.to_string()))?;
    let mut steps = Vec::new();
    let mut pending_tool_result_events = Vec::new();
    let mut prompt = prompt;

    for _ in 0..DEFAULT_AGENT_MAX_TURNS {
        let execution_id = next_execution_id();
        let builder = model
            .completion_request(prompt.clone())
            .messages(history.clone());
        let response = configure_completion_request_builder(
            builder,
            session,
            request,
            tool_definitions.clone(),
        )
        .send()
        .await;
        let response = match response {
            Ok(response) => response,
            Err(source) => {
                steps.push(BackendStep {
                    execution_id,
                    events: vec![],
                });
                return Ok(BackendRun::failed_with_steps(source.to_string(), steps));
            }
        };

        let mut step_events = std::mem::take(&mut pending_tool_result_events);
        let choice_events = backend_events_from_choice(&response.choice);
        let tool_calls = tool_calls_from_choice(&response.choice);
        let final_text = assistant_text_from_choice(&response.choice);

        step_events.extend(choice_events);

        if tool_calls.is_empty() {
            let text = final_text.ok_or_else(|| {
                BackendError::failed("completion response did not include assistant text")
            })?;
            steps.push(BackendStep {
                execution_id,
                events: step_events,
            });
            return Ok(BackendRun::succeeded_with_steps(text, steps));
        }

        history.push(prompt);
        history.push(rig::completion::message::Message::Assistant {
            id: response.message_id,
            content: response.choice,
        });

        let mut tool_results = Vec::with_capacity(tool_calls.len());
        for tool_call in tool_calls {
            let args = serde_json::to_string(&tool_call.function.arguments)
                .map_err(|source| BackendError::failed(source.to_string()))?;
            let output = match toolset.call(&tool_call.function.name, args).await {
                Ok(output) => output,
                Err(source) => source.to_string(),
            };
            pending_tool_result_events.push(BackendEvent::ToolResult(ToolResult {
                id: tool_call.id.clone(),
                output: output.clone(),
            }));
            tool_results.push(rig::completion::message::UserContent::tool_result(
                tool_call.id,
                rig::OneOrMany::one(rig::completion::message::ToolResultContent::text(output)),
            ));
        }
        steps.push(BackendStep {
            execution_id,
            events: step_events,
        });
        prompt = tool_result_message(tool_results);
    }

    if !pending_tool_result_events.is_empty() {
        steps.push(BackendStep {
            execution_id: next_execution_id(),
            events: pending_tool_result_events,
        });
    }

    Ok(BackendRun::failed_with_steps(
        format!("MaxTurnError: (reached max turn limit: {DEFAULT_AGENT_MAX_TURNS})"),
        steps,
    ))
}

#[async_trait]
impl CompletionBackend for RigBackend {
    async fn complete(
        &self,
        session: ResolvedSession,
        request: ResolvedCompletionRequest,
    ) -> std::result::Result<BackendRun, BackendError> {
        use rig::client::CompletionClient;
        use rig::providers::{anthropic, openai};

        fn resolve_api_key(provider: Provider) -> std::result::Result<String, BackendError> {
            let generic = std::env::var("COCO_API_KEY").ok();
            let provider_specific = match provider {
                Provider::OpenAi => std::env::var("OPENAI_API_KEY").ok(),
                Provider::Anthropic => std::env::var("ANTHROPIC_API_KEY").ok(),
            };

            generic.or(provider_specific).ok_or_else(|| {
                BackendError::failed(format!(
                    "missing API key for provider {}",
                    provider.as_str()
                ))
            })
        }

        fn resolve_base_url(provider: Provider) -> Option<String> {
            std::env::var("COCO_BASE_URL")
                .ok()
                .or_else(|| match provider {
                    Provider::OpenAi => std::env::var("OPENAI_BASE_URL").ok(),
                    Provider::Anthropic => None,
                })
        }

        let workspace_root = match bash_tool::resolve_workspace_root() {
            Ok(path) => path,
            Err(source) => {
                return BashToolSnafu {
                    message: source.to_string(),
                }
                .fail();
            }
        };
        let toolset = build_runtime_tool_set(&session, workspace_root)?;
        let history = session.provider_history.clone();
        let Some((prompt, history)) = history.split_last() else {
            return Err(BackendError::failed("completion requires history"));
        };
        let prompt = prompt.clone();
        let history = history.to_vec();

        match request.provider {
            Provider::OpenAi => {
                let api_key = resolve_api_key(request.provider)?;
                let mut builder = openai::Client::builder().api_key(&api_key);
                if let Some(base_url) = resolve_base_url(request.provider) {
                    builder = builder.base_url(&base_url);
                }
                let client = builder
                    .build()
                    .map_err(|source| BackendError::failed(source.to_string()))?;
                execute_completion_loop(
                    client.completion_model(&request.model),
                    &session,
                    &request,
                    prompt.clone(),
                    history.clone(),
                    toolset,
                )
                .await
            }
            Provider::Anthropic => {
                let api_key = resolve_api_key(request.provider)?;
                let mut builder = anthropic::Client::builder().api_key(api_key);
                if let Some(base_url) = resolve_base_url(request.provider) {
                    builder = builder.base_url(&base_url);
                }
                let client = builder
                    .build()
                    .map_err(|source| BackendError::failed(source.to_string()))?;
                execute_completion_loop(
                    client.completion_model(&request.model),
                    &session,
                    &request,
                    prompt,
                    history,
                    toolset,
                )
                .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::VecDeque;
    use std::time::Duration;

    use coco_mem::MemoryStore;
    use tokio::sync::Barrier;

    type RecordedCalls = Arc<Mutex<Vec<(ResolvedSession, ResolvedCompletionRequest)>>>;
    type FakeResponseQueue =
        Arc<Mutex<HashMap<String, VecDeque<std::result::Result<BackendRun, BackendError>>>>>;

    #[derive(Clone)]
    struct FakeBackend {
        responses: FakeResponseQueue,
        barrier: Option<Arc<Barrier>>,
        calls: RecordedCalls,
    }

    impl FakeBackend {
        fn with_responses(entries: &[(&str, &[std::result::Result<&str, BackendError>])]) -> Self {
            let responses = entries
                .iter()
                .map(|(branch, responses)| {
                    (
                        (*branch).to_owned(),
                        responses
                            .iter()
                            .map(|response| {
                                response
                                    .as_ref()
                                    .map(|text| {
                                        BackendRun::succeeded(
                                            (*text).to_owned(),
                                            vec![BackendEvent::AssistantText((*text).to_owned())],
                                        )
                                    })
                                    .map_err(Clone::clone)
                            })
                            .collect(),
                    )
                })
                .collect();

            Self {
                responses: Arc::new(Mutex::new(responses)),
                barrier: None,
                calls: Arc::new(Mutex::new(vec![])),
            }
        }

        fn with_completions(
            entries: &[(&str, &[std::result::Result<BackendRun, BackendError>])],
        ) -> Self {
            let responses = entries
                .iter()
                .map(|(branch, responses)| {
                    (
                        (*branch).to_owned(),
                        responses.iter().cloned().collect::<VecDeque<_>>(),
                    )
                })
                .collect();

            Self {
                responses: Arc::new(Mutex::new(responses)),
                barrier: None,
                calls: Arc::new(Mutex::new(vec![])),
            }
        }

        fn with_barrier(branches: &[(&str, &str)], barrier: Arc<Barrier>) -> Self {
            let responses = branches
                .iter()
                .map(|(branch, response)| {
                    (
                        (*branch).to_owned(),
                        VecDeque::from([Ok(BackendRun::succeeded(
                            (*response).to_owned(),
                            vec![BackendEvent::AssistantText((*response).to_owned())],
                        ))]),
                    )
                })
                .collect();

            Self {
                responses: Arc::new(Mutex::new(responses)),
                barrier: Some(barrier),
                calls: Arc::new(Mutex::new(vec![])),
            }
        }
    }

    #[async_trait]
    impl CompletionBackend for FakeBackend {
        async fn complete(
            &self,
            session: ResolvedSession,
            request: ResolvedCompletionRequest,
        ) -> std::result::Result<BackendRun, BackendError> {
            self.calls.lock().await.push((session, request.clone()));

            if let Some(barrier) = &self.barrier {
                barrier.wait().await;
            }

            let mut responses = self.responses.lock().await;
            let queue = responses
                .get_mut(&request.branch)
                .expect("missing fake backend response queue");
            let next = queue.pop_front().expect("missing fake backend response");
            drop(responses);

            tokio::time::sleep(Duration::from_millis(5)).await;
            next
        }
    }

    fn session_config(branch: &str) -> SessionConfig {
        SessionConfig {
            branch: branch.to_owned(),
            merge_parents: vec![],
            provider: Provider::OpenAi,
            model: "gpt-4.1-mini".to_owned(),
            system_prompt: "You are helpful.".to_owned(),
            prompt: "Conversation start.".to_owned(),
            tools: vec![],
            temperature: Some(0.2),
            max_tokens: Some(64),
            additional_params: None,
        }
    }

    fn request(branch: &str) -> CompletionRequest {
        CompletionRequest {
            branch: branch.to_owned(),
            provider: None,
            model: None,
            temperature: None,
            max_tokens: None,
            additional_params: None,
        }
    }

    fn prompt_request(branch: &str, prompt: &str) -> PromptRequest {
        PromptRequest {
            branch: branch.to_owned(),
            prompt: prompt.to_owned(),
            merge_parents: vec![],
        }
    }

    fn session_patch() -> SessionConfigPatch {
        SessionConfigPatch {
            provider: None,
            model: None,
            system_prompt: None,
            prompt: None,
            tools: None,
            temperature: None,
            max_tokens: None,
            additional_params: None,
        }
    }

    fn bash_tool() -> Tool {
        Tool {
            name: "bash".to_owned(),
            description: "Run a bash command".to_owned(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "command": {"type": "string"},
                    "workdir": {"type": "string"},
                    "timeout_ms": {"type": "integer"}
                },
                "required": ["command"]
            }),
        }
    }

    fn text_messages_from_entries(entries: &[ConversationEntry]) -> Vec<ConversationMessage> {
        entries
            .iter()
            .filter_map(|entry| match entry {
                ConversationEntry::Message(message) => Some(message.clone()),
                ConversationEntry::ToolUse(_) | ConversationEntry::ToolResult(_) => None,
            })
            .collect()
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

        let draft_session = service
            .create_session(SessionConfig {
                branch: "draft".to_owned(),
                merge_parents: vec!["main".to_owned()],
                provider: Provider::OpenAi,
                model: "gpt-4.1-mini".to_owned(),
                system_prompt: "You are helpful.".to_owned(),
                prompt: "Start from main.".to_owned(),
                tools: vec![],
                temperature: Some(0.2),
                max_tokens: Some(64),
                additional_params: None,
            })
            .await
            .unwrap();

        let anchor = store.get_node(&draft_session.anchor_id).unwrap();
        let Kind::Anchor(anchor) = anchor.kind else {
            panic!("expected anchor node");
        };
        assert!(anchor.as_session().is_some());
        assert_eq!(anchor.merge_parents, vec![main_session.anchor_id]);
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
            text_messages_from_entries(&session.conversation),
            vec![
                ConversationMessage {
                    role: MessageRole::User,
                    text: "Conversation start.".to_owned(),
                },
                ConversationMessage {
                    role: MessageRole::User,
                    text: "round one".to_owned(),
                },
                ConversationMessage {
                    role: MessageRole::Assistant,
                    text: "first".to_owned(),
                },
                ConversationMessage {
                    role: MessageRole::User,
                    text: "round two".to_owned(),
                },
                ConversationMessage {
                    role: MessageRole::Assistant,
                    text: "second".to_owned(),
                },
            ]
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
            text_messages_from_entries(&session.conversation),
            vec![
                ConversationMessage {
                    role: MessageRole::User,
                    text: "Conversation start.".to_owned(),
                },
                ConversationMessage {
                    role: MessageRole::User,
                    text: "retry me".to_owned(),
                },
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
        let service = LlmService::new(store, backend);
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

        let result = service
            .prompt(PromptRequest {
                branch: "main".to_owned(),
                prompt: "merge them".to_owned(),
                merge_parents: vec!["draft".to_owned()],
            })
            .await
            .unwrap();
        let session = service.resolve_session("main").unwrap();
        assert_eq!(result.text, "merge answer");
        assert_eq!(session.config.model, "gpt-4.1-mini");
        assert_eq!(session.config.system_prompt, "You are helpful.");
        assert_eq!(
            text_messages_from_entries(&session.conversation),
            vec![
                ConversationMessage {
                    role: MessageRole::User,
                    text: "Conversation start.".to_owned(),
                },
                ConversationMessage {
                    role: MessageRole::User,
                    text: "main question".to_owned(),
                },
                ConversationMessage {
                    role: MessageRole::Assistant,
                    text: "main answer".to_owned(),
                },
                ConversationMessage {
                    role: MessageRole::User,
                    text: "merge them".to_owned(),
                },
                ConversationMessage {
                    role: MessageRole::Assistant,
                    text: "merge answer".to_owned(),
                },
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
            text_messages_from_entries(&session.conversation),
            vec![
                ConversationMessage {
                    role: MessageRole::User,
                    text: "Conversation start.".to_owned(),
                },
                ConversationMessage {
                    role: MessageRole::User,
                    text: "new prompt".to_owned(),
                },
                ConversationMessage {
                    role: MessageRole::Assistant,
                    text: "prompted".to_owned(),
                },
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
                    vec![BackendEvent::AssistantText("recovered".to_owned())],
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
            text_messages_from_entries(&session.conversation),
            vec![
                ConversationMessage {
                    role: MessageRole::User,
                    text: "Conversation start.".to_owned(),
                },
                ConversationMessage {
                    role: MessageRole::User,
                    text: "retry prompt".to_owned(),
                },
            ]
        );

        let recovered = service.complete(request("main")).await.unwrap();
        assert_eq!(recovered.text, "recovered");
        assert_eq!(recovered.anchor_id, prompt_anchor_id);
        let recovered_node = store.get_node(&recovered.response_node_id).unwrap();
        assert_eq!(recovered_node.parent, prompt_anchor_id);
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
        let main = tokio::spawn(async move { main_service.complete(request("main")).await });
        let draft = tokio::spawn(async move { draft_service.complete(request("draft")).await });

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
        assert_eq!(anchor.merge_parents(), [source_head_id.as_str()]);
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
        assert_eq!(anchor.merge_parents(), [base_feedback_id.as_str()]);
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
                    provider: Some("anthropic".to_owned()),
                    model: Some("claude-sonnet-4-20250514".to_owned()),
                    system_prompt: Some("You are strict.".to_owned()),
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
        assert_eq!(session.config.system_prompt, "You are strict.");
        assert_eq!(session.config.temperature, None);
        assert_eq!(session.config.max_tokens, Some(128));
        assert_eq!(
            session.config.additional_params,
            Some(serde_json::json!({"service_tier": "priority"}))
        );

        let calls = calls.lock().await;
        let last = calls.last().expect("expected backend call");
        assert_eq!(last.0.config.system_prompt, "You are strict.");
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
                        execution_id: "execution-step-1".to_owned(),
                        events: vec![BackendEvent::ToolUse(ToolUse {
                            id: "tool-call-1".to_owned(),
                            name: "bash".to_owned(),
                            input: serde_json::json!({"command": "rg --files"}),
                        })],
                    },
                    BackendStep {
                        execution_id: "execution-step-2".to_owned(),
                        events: vec![
                            BackendEvent::ToolResult(ToolResult {
                                id: "tool-call-1".to_owned(),
                                output: "Cargo.toml".to_owned(),
                            }),
                            BackendEvent::AssistantText("done".to_owned()),
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
        assert!(matches!(
            &tool_result.kind,
            Kind::ToolResult(ToolResult { id, output })
                if id == "tool-call-1" && output == "Cargo.toml"
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
        assert!(matches!(
            &tool_use.kind,
            Kind::ToolUse(ToolUse { id, name, input })
                if id == "tool-call-1"
                    && name == "bash"
                    && input == &serde_json::json!({"command": "rg --files"})
        ));
        assert_eq!(
            tool_use.metadata.as_ref().unwrap().execution_id.as_deref(),
            Some("execution-step-1")
        );
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
                        execution_id: "execution-step-1".to_owned(),
                        events: vec![BackendEvent::ToolUse(ToolUse {
                            id: "tool-call-1".to_owned(),
                            name: "bash".to_owned(),
                            input: serde_json::json!({"command": "rg --files"}),
                        })],
                    },
                    BackendStep {
                        execution_id: "execution-step-2".to_owned(),
                        events: vec![
                            BackendEvent::ToolResult(ToolResult {
                                id: "tool-call-1".to_owned(),
                                output: "Cargo.toml".to_owned(),
                            }),
                            BackendEvent::AssistantText("done".to_owned()),
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
            text_messages_from_entries(&session.conversation),
            vec![
                ConversationMessage {
                    role: MessageRole::User,
                    text: "Conversation start.".to_owned(),
                },
                ConversationMessage {
                    role: MessageRole::User,
                    text: "list files".to_owned(),
                },
                ConversationMessage {
                    role: MessageRole::Assistant,
                    text: "done".to_owned(),
                },
            ]
        );
        assert_eq!(
            session.conversation,
            vec![
                ConversationEntry::Message(ConversationMessage {
                    role: MessageRole::User,
                    text: "Conversation start.".to_owned(),
                }),
                ConversationEntry::Message(ConversationMessage {
                    role: MessageRole::User,
                    text: "list files".to_owned(),
                }),
                ConversationEntry::ToolUse(ToolUse {
                    id: "tool-call-1".to_owned(),
                    name: "bash".to_owned(),
                    input: serde_json::json!({"command": "rg --files"}),
                }),
                ConversationEntry::ToolResult(ToolResult {
                    id: "tool-call-1".to_owned(),
                    output: "Cargo.toml".to_owned(),
                }),
                ConversationEntry::Message(ConversationMessage {
                    role: MessageRole::Assistant,
                    text: "done".to_owned(),
                }),
            ]
        );
    }

    #[test]
    fn rig_messages_from_entries_groups_tool_calls_and_results_by_turn() {
        let messages = rig_messages_from_tracked_entries(&[
            TrackedConversationEntry {
                execution_id: None,
                entry: ConversationEntry::Message(ConversationMessage {
                    role: MessageRole::User,
                    text: "list files".to_owned(),
                }),
            },
            TrackedConversationEntry {
                execution_id: Some("execution-1".to_owned()),
                entry: ConversationEntry::Message(ConversationMessage {
                    role: MessageRole::Assistant,
                    text: "checking".to_owned(),
                }),
            },
            TrackedConversationEntry {
                execution_id: Some("execution-1".to_owned()),
                entry: ConversationEntry::ToolUse(ToolUse {
                    id: "tool-call-1".to_owned(),
                    name: "bash".to_owned(),
                    input: serde_json::json!({"command": "ls"}),
                }),
            },
            TrackedConversationEntry {
                execution_id: Some("execution-1".to_owned()),
                entry: ConversationEntry::ToolUse(ToolUse {
                    id: "tool-call-2".to_owned(),
                    name: "bash".to_owned(),
                    input: serde_json::json!({"command": "pwd"}),
                }),
            },
            TrackedConversationEntry {
                execution_id: Some("execution-1".to_owned()),
                entry: ConversationEntry::ToolResult(ToolResult {
                    id: "tool-call-1".to_owned(),
                    output: "Cargo.toml".to_owned(),
                }),
            },
            TrackedConversationEntry {
                execution_id: Some("execution-1".to_owned()),
                entry: ConversationEntry::ToolResult(ToolResult {
                    id: "tool-call-2".to_owned(),
                    output: "/tmp".to_owned(),
                }),
            },
            TrackedConversationEntry {
                execution_id: Some("execution-2".to_owned()),
                entry: ConversationEntry::Message(ConversationMessage {
                    role: MessageRole::Assistant,
                    text: "done".to_owned(),
                }),
            },
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
                    )
                    && matches!(
                        items[2],
                        rig::completion::message::AssistantContent::ToolCall(tool_call)
                            if tool_call.id == "tool-call-2"
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
                    )
                    && matches!(
                        items[1],
                        rig::completion::message::UserContent::ToolResult(tool_result)
                            if tool_result.id == "tool-call-2"
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

    #[tokio::test]
    async fn multi_step_completion_uses_distinct_execution_ids_per_completion_call() {
        let store = MemoryStore::new();
        let backend = FakeBackend::with_completions(&[(
            "main",
            &[Ok(BackendRun::succeeded_with_steps(
                "done",
                vec![
                    BackendStep {
                        execution_id: "execution-step-1".to_owned(),
                        events: vec![
                            BackendEvent::ToolUse(ToolUse {
                                id: "tool-call-1".to_owned(),
                                name: "bash".to_owned(),
                                input: serde_json::json!({"command": "ls"}),
                            }),
                            BackendEvent::ToolResult(ToolResult {
                                id: "tool-call-1".to_owned(),
                                output: "Cargo.toml".to_owned(),
                            }),
                        ],
                    },
                    BackendStep {
                        execution_id: "execution-step-2".to_owned(),
                        events: vec![BackendEvent::AssistantText("done".to_owned())],
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
                        execution_id: "execution-step-1".to_owned(),
                        events: vec![BackendEvent::ToolUse(ToolUse {
                            id: "tool-call-1".to_owned(),
                            name: "bash".to_owned(),
                            input: serde_json::json!({"command": "printf 'hello' > trace.txt"}),
                        })],
                    },
                    BackendStep {
                        execution_id: "execution-step-2".to_owned(),
                        events: vec![
                            BackendEvent::ToolResult(ToolResult {
                                id: "tool-call-1".to_owned(),
                                output: "exit_status: 0\nstdout:\n\nstderr:\n".to_owned(),
                            }),
                            BackendEvent::AssistantText("trying again".to_owned()),
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
        assert!(matches!(
            &tool_result.kind,
            Kind::ToolResult(ToolResult { id, output, .. })
                if id == "tool-call-1"
                    && output == "exit_status: 0\nstdout:\n\nstderr:\n"
        ));

        let tool_use = store.get_node(&tool_result.parent).unwrap();
        assert!(matches!(
            &tool_use.kind,
            Kind::ToolUse(ToolUse { id, name, .. })
                if id == "tool-call-1" && name == "bash"
        ));
        assert_eq!(tool_use.parent, context.retry_from_node_id);

        let session = service.resolve_session("main").unwrap();
        assert_eq!(
            text_messages_from_entries(&session.conversation),
            vec![
                ConversationMessage {
                    role: MessageRole::User,
                    text: "Conversation start.".to_owned(),
                },
                ConversationMessage {
                    role: MessageRole::User,
                    text: "keep going".to_owned(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn bash_tool_runtime_allows_writes_within_configured_workspace() {
        let temp_root = tempfile::tempdir().unwrap();
        let runtime = bash_tool::runtime_tool(
            bash_tool(),
            temp_root.path().to_path_buf(),
            BashToolContext {
                session_branch: "main".to_owned(),
                store_path: None,
                cli_bridge: None,
            },
        );
        let previous = std::env::var_os("COCO_BASH_SANDBOX");
        unsafe {
            std::env::set_var("COCO_BASH_SANDBOX", "off");
        }
        let output = runtime
            .call(format!(
                r#"{{"command":"printf 'hello' > trace.txt; cat trace.txt","workdir":"{}"}}"#,
                temp_root.path().display()
            ))
            .await;
        match previous {
            Some(value) => unsafe {
                std::env::set_var("COCO_BASH_SANDBOX", value);
            },
            None => unsafe {
                std::env::remove_var("COCO_BASH_SANDBOX");
            },
        }
        let output = output.unwrap();
        assert!(output.contains("exit_status: 0"));
        assert!(output.contains("stdout:\nhello"));
        assert_eq!(
            std::fs::read_to_string(temp_root.path().join("trace.txt")).unwrap(),
            "hello"
        );
    }
}
