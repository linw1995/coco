use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use coco_mem::{
    Anchor, AnchorPayload, Kind, MemoryStore, NewNode, NodeMetadata, PauseReason, PromptAnchor,
    Role, SessionAnchor, SessionState, Store, StoreError, Tool,
};
use serde_json::Value;
use snafu::IntoError;
use snafu::prelude::*;
use tokio::sync::{Mutex, OwnedMutexGuard};

pub use coco_mem;
pub use coco_mem::SessionAnchorPatch as SessionConfigPatch;

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

#[derive(Debug, Clone)]
pub struct SessionSnapshot {
    pub branch: String,
    pub anchor_id: String,
    pub session_anchor_id: String,
    pub provider: Provider,
    pub model: String,
    pub system_prompt: String,
    pub tools: Vec<Tool>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub additional_params: Option<Value>,
    pub history: Vec<ConversationMessage>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendCompletion {
    pub text: String,
}

#[derive(Debug, Snafu, Clone, PartialEq, Eq)]
pub enum BackendError {
    #[snafu(display("{message}"))]
    Failed { message: String },
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
        session: SessionSnapshot,
        request: ResolvedCompletionRequest,
    ) -> std::result::Result<BackendCompletion, BackendError>;
}

type BranchLockTable = Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>;
type WorkflowLock = Arc<Mutex<()>>;

pub struct LlmService<B = RigBackend, S = MemoryStore>
where
    S: Store,
{
    store: S,
    backend: B,
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
    session_anchor_id: String,
    session_anchor: SessionAnchor,
    tail_history: Vec<ConversationMessage>,
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
            branch_locks: Arc::new(Mutex::new(HashMap::new())),
            workflow_lock: Arc::new(Mutex::new(())),
        }
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
        let execution_id = format!("execution-{}", nanoid::nanoid!());
        let metadata = NodeMetadata::execution(execution_id.clone());

        match self
            .backend
            .complete(session.clone(), resolved.clone())
            .await
        {
            Ok(completion) => {
                let response_text = completion.text;
                let response_node_id = self
                    .store
                    .append(NewNode {
                        parent: original_head.clone(),
                        role: Role::LLM,
                        metadata: Some(metadata.clone()),
                        kind: Kind::Text(response_text.clone()),
                    })
                    .context(MemorySnafu)?;
                self.store
                    .set_branch_head(&resolved.branch, &original_head, &response_node_id)
                    .context(MemorySnafu)?;

                Ok(CompletionResult {
                    branch: resolved.branch,
                    anchor_id: session.anchor_id,
                    execution_id,
                    response_node_id: response_node_id.clone(),
                    branch_head: response_node_id,
                    text: response_text,
                })
            }
            Err(source) => {
                let message = source.to_string();
                let error_node_id = self
                    .store
                    .append(NewNode {
                        parent: original_head.clone(),
                        role: Role::System,
                        metadata: Some(metadata),
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

    fn resolve_session(&self, branch: &str) -> Result<SessionSnapshot> {
        let context = self.resolve_context(branch)?;
        let mut history = Vec::new();
        if !context.session_anchor.prompt.is_empty() {
            history.push(ConversationMessage {
                role: MessageRole::User,
                text: context.session_anchor.prompt.clone(),
            });
        }
        history.extend(context.tail_history);

        Ok(SessionSnapshot {
            branch: branch.to_owned(),
            anchor_id: context.active_anchor_id,
            session_anchor_id: context.session_anchor_id,
            provider: Provider::parse(&context.session_anchor.provider)?,
            model: context.session_anchor.model.clone(),
            system_prompt: context.session_anchor.system_prompt.clone(),
            tools: context.session_anchor.tools.clone(),
            temperature: context.session_anchor.temperature,
            max_tokens: context.session_anchor.max_tokens,
            additional_params: context.session_anchor.additional_params.clone(),
            history,
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
                            session_anchor_id: node.id.clone(),
                            session_anchor: session_anchor.clone(),
                            tail_history: vec![],
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
                            context.tail_history.push(ConversationMessage {
                                role: MessageRole::User,
                                text: prompt_anchor.prompt.clone(),
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
                        context.tail_history.push(ConversationMessage {
                            role,
                            text: text.clone(),
                        });
                    }
                }
                Kind::Failure(_) | Kind::ToolUse(_) | Kind::ToolResult(_) => {}
            }
        }

        state.context(MissingAnchorSnafu {
            branch: reference.to_owned(),
        })
    }

    fn resolve_request(
        &self,
        session: &SessionSnapshot,
        request: CompletionRequest,
    ) -> ResolvedCompletionRequest {
        ResolvedCompletionRequest {
            branch: request.branch,
            provider: request.provider.unwrap_or(session.provider),
            model: request.model.unwrap_or_else(|| session.model.clone()),
            temperature: request.temperature.or(session.temperature),
            max_tokens: request.max_tokens.or(session.max_tokens),
            additional_params: request
                .additional_params
                .or_else(|| session.additional_params.clone()),
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

#[async_trait]
impl CompletionBackend for RigBackend {
    async fn complete(
        &self,
        session: SessionSnapshot,
        request: ResolvedCompletionRequest,
    ) -> std::result::Result<BackendCompletion, BackendError> {
        use rig::client::CompletionClient;
        use rig::completion::CompletionModel;
        use rig::completion::message::Message;
        use rig::message::AssistantContent;
        use rig::providers::{anthropic, openai};

        fn resolve_api_key(provider: Provider) -> std::result::Result<String, BackendError> {
            let generic = std::env::var("COCO_API_KEY").ok();
            let provider_specific = match provider {
                Provider::OpenAi => std::env::var("OPENAI_API_KEY").ok(),
                Provider::Anthropic => std::env::var("ANTHROPIC_API_KEY").ok(),
            };

            generic
                .or(provider_specific)
                .ok_or_else(|| BackendError::Failed {
                    message: format!("missing API key for provider {}", provider.as_str()),
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

        fn to_messages(history: &[ConversationMessage]) -> Vec<Message> {
            history
                .iter()
                .map(|message| match message.role {
                    MessageRole::User => Message::user(message.text.clone()),
                    MessageRole::Assistant => Message::assistant(message.text.clone()),
                })
                .collect()
        }

        fn extract_text<T>(response: rig::completion::CompletionResponse<T>) -> String {
            response
                .choice
                .iter()
                .filter_map(|content| match content {
                    AssistantContent::Text(text) => Some(text.text.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n")
        }

        let history = to_messages(&session.history);
        let Some((prompt, history)) = history.split_last() else {
            return Err(BackendError::Failed {
                message: "completion requires history".to_owned(),
            });
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
                let client = builder.build().map_err(|source| BackendError::Failed {
                    message: source.to_string(),
                })?;
                let model = client.completion_model(&request.model);
                let request = model
                    .completion_request(prompt.clone())
                    .messages(history)
                    .preamble(session.system_prompt)
                    .temperature_opt(request.temperature)
                    .max_tokens_opt(request.max_tokens)
                    .additional_params_opt(request.additional_params)
                    .build();
                let response =
                    model
                        .completion(request)
                        .await
                        .map_err(|source| BackendError::Failed {
                            message: source.to_string(),
                        })?;

                Ok(BackendCompletion {
                    text: extract_text(response),
                })
            }
            Provider::Anthropic => {
                let api_key = resolve_api_key(request.provider)?;
                let mut builder = anthropic::Client::builder().api_key(api_key);
                if let Some(base_url) = resolve_base_url(request.provider) {
                    builder = builder.base_url(&base_url);
                }
                let client = builder.build().map_err(|source| BackendError::Failed {
                    message: source.to_string(),
                })?;
                let model = client.completion_model(&request.model);
                let request = model
                    .completion_request(prompt)
                    .messages(history)
                    .preamble(session.system_prompt)
                    .temperature_opt(request.temperature)
                    .max_tokens_opt(request.max_tokens)
                    .additional_params_opt(request.additional_params)
                    .build();
                let response =
                    model
                        .completion(request)
                        .await
                        .map_err(|source| BackendError::Failed {
                            message: source.to_string(),
                        })?;

                Ok(BackendCompletion {
                    text: extract_text(response),
                })
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

    type RecordedCalls = Arc<Mutex<Vec<(SessionSnapshot, ResolvedCompletionRequest)>>>;
    type FakeResponseQueue =
        Arc<Mutex<HashMap<String, VecDeque<std::result::Result<String, BackendError>>>>>;

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
                                    .map(|text| (*text).to_owned())
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

        fn with_barrier(branches: &[(&str, &str)], barrier: Arc<Barrier>) -> Self {
            let responses = branches
                .iter()
                .map(|(branch, response)| {
                    (
                        (*branch).to_owned(),
                        VecDeque::from([Ok((*response).to_owned())]),
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
            session: SessionSnapshot,
            request: ResolvedCompletionRequest,
        ) -> std::result::Result<BackendCompletion, BackendError> {
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
            next.map(|text| BackendCompletion { text })
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
            session.history,
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
        let backend = FakeBackend::with_responses(&[(
            "main",
            &[Err(BackendError::Failed {
                message: "rate limited".to_owned(),
            })],
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
            session.history,
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
        let main_session = service
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
        assert_eq!(session.session_anchor_id, main_session.anchor_id);
        assert_eq!(session.model, "gpt-4.1-mini");
        assert_eq!(session.system_prompt, "You are helpful.");
        assert_eq!(
            session.history,
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
            session.history,
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
        let backend = FakeBackend::with_responses(&[(
            "main",
            &[
                Err(BackendError::Failed {
                    message: "rate limited".to_owned(),
                }),
                Ok("recovered"),
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
            session.history,
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
        assert_eq!(session.provider, Provider::Anthropic);
        assert_eq!(session.model, "claude-sonnet-4-20250514");
        assert_eq!(session.system_prompt, "You are strict.");
        assert_eq!(session.temperature, None);
        assert_eq!(session.max_tokens, Some(128));
        assert_eq!(
            session.additional_params,
            Some(serde_json::json!({"service_tier": "priority"}))
        );

        let calls = calls.lock().await;
        let last = calls.last().expect("expected backend call");
        assert_eq!(last.0.system_prompt, "You are strict.");
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
}
