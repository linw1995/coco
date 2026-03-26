use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use coco_mem::{
    Anchor, AnchorPayload, Kind, NewNode, NodeMetadata, PromptAnchor, Role, SessionAnchor,
    SharedStore, Tool as MemoryTool,
};
use serde_json::Value;
use snafu::prelude::*;
use tokio::sync::{Mutex, OwnedMutexGuard};

pub use coco_mem::SessionAnchorPatch as SessionConfigPatch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    OpenAi,
    Anthropic,
}

impl Provider {
    fn as_str(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Anthropic => "anthropic",
        }
    }

    fn parse(value: &str) -> Result<Self> {
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

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub branch: String,
    pub merge_parents: Vec<String>,
    pub provider: Provider,
    pub model: String,
    pub system_prompt: String,
    pub prompt: String,
    pub tools: Vec<MemoryTool>,
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
    pub tools: Vec<MemoryTool>,
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
    Failed { message: String, retryable: bool },
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

pub struct LlmService<B = RigBackend> {
    store: SharedStore,
    backend: B,
    branch_locks: BranchLockTable,
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

impl LlmService<RigBackend> {
    pub fn with_store(store: SharedStore) -> Self {
        Self::new(store, RigBackend)
    }
}

impl<B> LlmService<B>
where
    B: CompletionBackend,
{
    pub fn new(store: SharedStore, backend: B) -> Self {
        Self {
            store,
            backend,
            branch_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn store(&self) -> &SharedStore {
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
            .fork(config.branch.clone(), &anchor_id)
            .context(MemorySnafu)?;

        Ok(BranchSession {
            branch: config.branch,
            anchor_id,
        })
    }

    pub async fn prompt(&self, request: PromptRequest) -> Result<CompletionResult> {
        let _guard = self.lock_branch(&request.branch).await;
        self.append_prompt_anchor(&request)?;
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

    fn append_prompt_anchor(&self, config: &PromptRequest) -> Result<String> {
        let original_head = self
            .store
            .get_branch_head(&config.branch)
            .context(MemorySnafu)?;
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
                parent: original_head.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    merge_parents,
                    PromptAnchor {
                        prompt: config.prompt.clone(),
                    },
                )),
            })
            .context(MemorySnafu)?;
        self.store
            .set_branch_head(&config.branch, &original_head, &anchor_id)
            .context(MemorySnafu)?;

        Ok(anchor_id)
    }

    pub fn fork(&self, branch: impl Into<String>, from_ref: &str) -> Result<String> {
        self.store
            .fork(branch.into(), from_ref)
            .context(MemorySnafu)
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
                        kind: Kind::Text(message.clone()),
                    })
                    .context(MemorySnafu)?;
                Err(Error::Backend {
                    source,
                    context: Box::new(BackendFailureContext {
                        branch: resolved.branch,
                        execution_id,
                        error_node_id,
                        retry_from_node_id: original_head,
                    }),
                })
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
                Kind::ToolUse(_) | Kind::ToolResult(_) => {}
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
}

#[async_trait]
impl CompletionBackend for RigBackend {
    async fn complete(
        &self,
        session: SessionSnapshot,
        request: ResolvedCompletionRequest,
    ) -> std::result::Result<BackendCompletion, BackendError> {
        use rig::client::{CompletionClient, ProviderClient};
        use rig::completion::CompletionModel;
        use rig::completion::message::Message;
        use rig::message::AssistantContent;
        use rig::providers::{anthropic, openai};

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
                retryable: false,
            });
        };
        let prompt = prompt.clone();
        let history = history.to_vec();

        match request.provider {
            Provider::OpenAi => {
                let client = openai::Client::from_env();
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
                            retryable: false,
                        })?;

                Ok(BackendCompletion {
                    text: extract_text(response),
                })
            }
            Provider::Anthropic => {
                let client = anthropic::Client::from_env();
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
                            retryable: false,
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

    use coco_mem::SharedStore;
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
        let store = SharedStore::new();
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
        let store = SharedStore::new();
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
        let store = SharedStore::new();
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
    async fn failed_completion_persists_system_text_but_not_prompt_history() {
        let store = SharedStore::new();
        let backend = FakeBackend::with_responses(&[(
            "main",
            &[Err(BackendError::Failed {
                message: "rate limited".to_owned(),
                retryable: true,
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
        assert!(matches!(&failure.kind, Kind::Text(text) if text == "rate limited"));
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
        let store = SharedStore::new();
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
        let store = SharedStore::new();
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
        let store = SharedStore::new();
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
        let store = SharedStore::new();
        let backend = FakeBackend::with_responses(&[(
            "main",
            &[
                Err(BackendError::Failed {
                    message: "rate limited".to_owned(),
                    retryable: true,
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
        assert!(matches!(&failure.kind, Kind::Text(text) if text == "rate limited"));
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
        let store = SharedStore::new();
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
    async fn rebase_session_changes_defaults_for_future_turns() {
        let store = SharedStore::new();
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
        let store = SharedStore::new();
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
