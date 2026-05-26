use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use coco_llm::coco_mem::{
    Anchor, BackendMetadata, BranchStore, ExecutionMetadata, JobStatus, JobStore, Kind,
    MemoryStore, MessageQueueStore, NewNode, NodeStore, PromptAnchor, ProviderMetadata, Role,
    SessionRole, SessionStore, SkillInvocationAnchor, SkillInvocationMode, SkillScript, SkillStore,
    SkillVersionSpec, ToolResult, ToolUse,
};
use coco_llm::{
    BackendError, BackendEventPayload, BackendTurn, CompletionBackend, CompletionMessage,
    LlmService, Provider, SessionConfig, SessionConfigPatch, StepContext, builtin_tool_definition,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::time::{Duration, sleep};

use crate::engine::SYSTEM_EVENT_QUEUE;
use crate::{
    BatchPromptRequest, BranchPromptRequest, BranchPromptStatus, BranchResolveError,
    BranchResolver, ChannelKind, ConversationEngine, CoreService, EngineError, Error,
    FixedBranchResolver, InboundMessage, TelegramImageAttachment,
};

type FakeResponseQueue =
    Arc<Mutex<HashMap<String, VecDeque<std::result::Result<BackendTurn, BackendError>>>>>;

fn append_skill_invocation_node<S>(store: &S, parent: &str, skill_name: &str) -> String
where
    S: NodeStore,
{
    store
        .append(NewNode {
            parent: parent.to_owned(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::skill_invocation(
                vec![],
                SkillInvocationAnchor {
                    skill_name: skill_name.to_owned(),
                    mode: SkillInvocationMode::InheritContext,
                },
            )),
        })
        .unwrap()
}

fn skill_runtime_contains_body(path: &Path, expected_body: &str) -> bool {
    let Ok(entries) = fs::read_dir(path) else {
        return false;
    };
    entries.filter_map(Result::ok).any(|entry| {
        fs::read_to_string(entry.path().join("SKILL.md")).is_ok_and(|body| body == expected_body)
    })
}

#[derive(Debug)]
struct FailingResolver;

impl BranchResolver for FailingResolver {
    fn resolve_branch(
        &self,
        _message: &InboundMessage,
    ) -> std::result::Result<String, BranchResolveError> {
        Err(BranchResolveError::ResolveFailed {
            message: "resolver failed".to_owned(),
        })
    }
}

#[derive(Debug, Clone)]
struct FakeBackend {
    responses: FakeResponseQueue,
}

impl FakeBackend {
    fn finished_turn(text: &str) -> BackendTurn {
        BackendTurn {
            message: CompletionMessage::assistant(text.to_owned()),
            events: vec![],
            tool_calls: vec![],
            final_text: Some(text.to_owned()),
            trace_persisted: false,
        }
    }

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
                                .map(|text| Self::finished_turn(text))
                                .map_err(Clone::clone)
                        })
                        .collect(),
                )
            })
            .collect();

        Self {
            responses: Arc::new(Mutex::new(responses)),
        }
    }
}

#[async_trait]
impl CompletionBackend for FakeBackend {
    async fn step(&self, ctx: StepContext<'_>) -> std::result::Result<BackendTurn, BackendError> {
        let mut responses = self.responses.lock().unwrap();
        let queue = responses
            .get_mut(&ctx.request.branch)
            .expect("missing fake backend queue");
        queue.pop_front().expect("missing fake backend response")
    }
}

#[derive(Debug, Clone)]
struct BlockingBackend {
    started: Arc<Notify>,
    release: Arc<Notify>,
    calls: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
struct AnyBranchBackend {
    calls: Arc<AsyncMutex<Vec<String>>>,
    provider_contexts: Arc<AsyncMutex<Vec<String>>>,
    text: String,
}

impl AnyBranchBackend {
    fn new(text: &str) -> Self {
        Self {
            calls: Arc::new(AsyncMutex::new(Vec::new())),
            provider_contexts: Arc::new(AsyncMutex::new(Vec::new())),
            text: text.to_owned(),
        }
    }
}

#[async_trait]
impl CompletionBackend for AnyBranchBackend {
    async fn step(&self, ctx: StepContext<'_>) -> std::result::Result<BackendTurn, BackendError> {
        self.calls.lock().await.push(ctx.request.branch.clone());
        self.provider_contexts
            .lock()
            .await
            .push(format!("{:?}\n{:?}", ctx.history, ctx.prompt));
        Ok(FakeBackend::finished_turn(&self.text))
    }
}

#[derive(Debug, Clone)]
struct AlwaysFailBackend {
    calls: Arc<AsyncMutex<Vec<String>>>,
}

impl AlwaysFailBackend {
    fn new() -> Self {
        Self {
            calls: Arc::new(AsyncMutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl CompletionBackend for AlwaysFailBackend {
    async fn step(&self, ctx: StepContext<'_>) -> std::result::Result<BackendTurn, BackendError> {
        self.calls.lock().await.push(ctx.request.branch.clone());
        Err(BackendError::Failed {
            message: "backend failed".to_owned(),
        })
    }
}

#[derive(Debug, Clone)]
struct MissingFinalTextBackend {
    calls: Arc<AsyncMutex<Vec<String>>>,
}

impl MissingFinalTextBackend {
    fn new() -> Self {
        Self {
            calls: Arc::new(AsyncMutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl CompletionBackend for MissingFinalTextBackend {
    async fn step(&self, ctx: StepContext<'_>) -> std::result::Result<BackendTurn, BackendError> {
        self.calls.lock().await.push(ctx.request.branch.clone());
        Ok(BackendTurn {
            message: CompletionMessage::assistant("thinking"),
            events: vec![BackendEventPayload::AssistantText("thinking".to_owned()).into()],
            tool_calls: vec![],
            final_text: None,
            trace_persisted: false,
        })
    }
}

#[derive(Debug, Clone)]
struct ModelGateBackend {
    calls: Arc<AsyncMutex<Vec<String>>>,
    accepted_model: String,
}

impl ModelGateBackend {
    fn new(accepted_model: &str) -> Self {
        Self {
            calls: Arc::new(AsyncMutex::new(Vec::new())),
            accepted_model: accepted_model.to_owned(),
        }
    }
}

#[async_trait]
impl CompletionBackend for ModelGateBackend {
    async fn step(&self, ctx: StepContext<'_>) -> std::result::Result<BackendTurn, BackendError> {
        let model = ctx.session.config.model.clone();
        self.calls.lock().await.push(model.clone());
        if model == self.accepted_model {
            Ok(FakeBackend::finished_turn("recovered"))
        } else {
            Err(BackendError::Failed {
                message: format!("model {model} failed"),
            })
        }
    }
}

#[async_trait]
impl CompletionBackend for BlockingBackend {
    async fn step(&self, _ctx: StepContext<'_>) -> std::result::Result<BackendTurn, BackendError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_waiters();
        self.release.notified().await;
        Ok(FakeBackend::finished_turn("done"))
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
        prompt: "".to_owned(),
        tools: vec![],
        temperature: None,
        max_tokens: None,
        additional_params: None,
        enable_coco_shim: false,
    }
}

fn submit_prompt_job(store: &MemoryStore, branch: &str, prompt: &str) -> coco_llm::coco_mem::Job {
    let parent = store.get_branch_head(branch).unwrap();
    let prompt_anchor_id = store
        .append(NewNode {
            parent,
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                vec![],
                PromptAnchor {
                    prompt: prompt.to_owned(),
                    attachments: vec![],
                },
            )),
        })
        .unwrap();
    store.submit_job(branch, &prompt_anchor_id).unwrap()
}

#[tokio::test]
async fn core_service_routes_message_to_engine() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[("main", &[Ok("hello from llm")])]);
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let service = CoreService::new(
        FixedBranchResolver::new("main"),
        ConversationEngine::new(llm),
    );

    let response = service
        .handle_message(InboundMessage::cli("conversation", "user", "hello"))
        .await
        .unwrap();

    assert_eq!(response.text, "hello from llm");
}

#[tokio::test]
async fn core_service_telegram_prompt_requires_completing_request_before_reply() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[("main", &[Ok("Telegram reply sent.")])]);
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let service = CoreService::new(
        FixedBranchResolver::new("main"),
        ConversationEngine::new(llm),
    );

    service
        .handle_message(InboundMessage::telegram_with_message_id(
            "chat-1",
            "user-1",
            "message-1",
            "Create a runner skill and inspect the upstream history.",
        ))
        .await
        .unwrap();

    let ancestry = store.ancestry("main").unwrap();
    let prompt = match &ancestry[1].kind {
        Kind::Anchor(anchor) => &anchor.as_prompt().expect("expected prompt anchor").prompt,
        _ => panic!("expected prompt anchor"),
    };
    assert!(prompt.contains("Treat the incoming message as the user's actual request"));
    assert!(prompt.contains("Complete the requested work using any needed tools or skills"));
    assert!(prompt.contains("do not delegate the user task itself"));
    assert!(prompt.contains("you may send one progress update through the `telegram` skill"));
    assert!(prompt.contains("then continue working"));
    assert!(prompt.contains("for the final user-facing reply only after"));
    assert!(prompt.contains("the final reply is that first message"));
    assert!(prompt.contains("Do not use the `telegram` skill merely to acknowledge"));
    assert!(prompt.contains("Do not finish after an acknowledgement-only Telegram reply"));
    assert!(prompt.contains("include a concise multi-task summary"));
    assert!(prompt.contains("chat_id: chat-1"));
    assert!(prompt.contains("reply_to_message_id: message-1"));
}

#[tokio::test]
async fn core_service_telegram_prompt_includes_image_attachments() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[("main", &[Ok("Telegram reply sent.")])]);
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    let mut config = session_config("main");
    config.tools = vec![builtin_tool_definition("load_image").unwrap()];
    llm.create_session(config).await.unwrap();
    let service = CoreService::new(
        FixedBranchResolver::new("main"),
        ConversationEngine::new(llm),
    );

    service
        .handle_message(InboundMessage::telegram_with_message_id_and_images(
            "chat-1",
            "user-1",
            "message-1",
            "",
            vec![TelegramImageAttachment::from_parts(
                "file-id",
                Some("unique-id".to_owned()),
                Some(1280),
                Some(960),
                Some(200_000),
            )],
        ))
        .await
        .unwrap();

    let ancestry = store.ancestry("main").unwrap();
    let prompt = match &ancestry[1].kind {
        Kind::Anchor(anchor) => &anchor.as_prompt().expect("expected prompt anchor").prompt,
        _ => panic!("expected prompt anchor"),
    };
    assert!(prompt.contains("Incoming image attachments:"));
    assert!(prompt.contains("file_id=file-id"));
    assert!(prompt.contains("file_unique_id=unique-id"));
    assert!(prompt.contains("width=1280"));
    assert!(prompt.contains("height=960"));
    assert!(prompt.contains("file_size=200000"));
    assert!(prompt.contains("No text caption was provided."));
    assert!(prompt.contains("load_image"));
    assert!(prompt.contains("telegram_download.py"));

    let attachments = &match &ancestry[1].kind {
        Kind::Anchor(anchor) => anchor.as_prompt().expect("expected prompt anchor"),
        _ => panic!("expected prompt anchor"),
    }
    .attachments;
    assert!(attachments.is_empty());
}

#[tokio::test]
async fn core_service_telegram_prompt_omits_load_image_when_tool_is_unavailable() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[("main", &[Ok("Telegram reply sent.")])]);
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let service = CoreService::new(
        FixedBranchResolver::new("main"),
        ConversationEngine::new(llm),
    );

    service
        .handle_message(InboundMessage::telegram_with_message_id_and_images(
            "chat-1",
            "user-1",
            "message-1",
            "Describe this image.",
            vec![TelegramImageAttachment::from_parts(
                "file-id", None, None, None, None,
            )],
        ))
        .await
        .unwrap();

    let ancestry = store.ancestry("main").unwrap();
    let prompt = match &ancestry[1].kind {
        Kind::Anchor(anchor) => &anchor.as_prompt().expect("expected prompt anchor").prompt,
        _ => panic!("expected prompt anchor"),
    };
    assert!(prompt.contains("telegram_download.py"));
    assert!(prompt.contains("tools available in this session"));
    assert!(!prompt.contains("load_image"));
}

#[tokio::test]
async fn core_service_returns_missing_session() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[]);
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    let service = CoreService::new(
        FixedBranchResolver::new("main"),
        ConversationEngine::new(llm),
    );

    let error = service
        .handle_message(InboundMessage::cli("conversation", "user", "hello"))
        .await
        .unwrap_err();

    assert!(matches!(error, Error::MissingSession { branch } if branch == "main"));
}

#[tokio::test]
async fn core_service_returns_branch_resolution_error_context() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[]);
    let llm = Arc::new(LlmService::new(store, backend));
    let service = CoreService::new(FailingResolver, ConversationEngine::new(llm));

    let error = service
        .handle_message(InboundMessage::telegram("chat-1", "user-1", "hello"))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        Error::BranchResolveFailed {
            channel_kind: ChannelKind::Telegram,
            conversation_id,
            source: BranchResolveError::ResolveFailed { .. },
        } if conversation_id == "chat-1"
    ));
}

#[tokio::test]
async fn llm_engine_calls_prompt_and_returns_text() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[("main", &[Ok("hello from llm")])]);
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);

    let response = engine.reply("main", "hello").await.unwrap();

    assert_eq!(response, "hello from llm");
    let jobs = store.list_jobs().unwrap();
    assert_eq!(jobs.len(), 1);
    let job_id = jobs.keys().next().unwrap();
    let persisted_job = jobs.get(job_id).unwrap();
    let job = engine.get_job(job_id).unwrap();
    assert_eq!(job.status, JobStatus::Finished);
    assert_eq!(job.finished_at, persisted_job.finished_at);
    assert!(job.finished_at.is_some());
}

#[tokio::test]
async fn llm_engine_prompt_session_patch_appends_patch_anchor() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[("runner", &[Ok("runner done")])]);
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    let main_session = llm.create_session(session_config("main")).await.unwrap();
    llm.fork("runner", &main_session.anchor_id).unwrap();
    let exec_tool = builtin_tool_definition("exec_command").unwrap();
    let engine = ConversationEngine::new(llm);

    let response = engine
        .reply_with_session_patch(
            "runner",
            "run date",
            vec![],
            Some(SessionConfigPatch {
                role: Some(SessionRole::Runner),
                tools: Some(vec![exec_tool.clone()]),
                ..SessionConfigPatch::default()
            }),
        )
        .await
        .unwrap();

    assert_eq!(response, "runner done");
    let ancestry = store.ancestry("runner").unwrap();
    assert!(matches!(
        &ancestry[0].kind,
        Kind::Text(text) if text == "runner done"
    ));
    assert!(matches!(&ancestry[1].kind, Kind::Anchor(anchor) if anchor.as_prompt().is_some()));
    let Kind::Anchor(anchor) = &ancestry[2].kind else {
        panic!("expected session patch anchor");
    };
    let patch = anchor
        .as_session_patch()
        .expect("expected session patch anchor");
    assert_eq!(patch.role, Some(SessionRole::Runner));
    assert_eq!(patch.tools, Some(vec![exec_tool]));
    assert_eq!(ancestry[2].parent, main_session.anchor_id);
}

#[tokio::test]
async fn llm_engine_rejects_second_active_job_on_same_branch() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[]);
    let llm = Arc::new(LlmService::new(store, backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);

    let first = engine.submit_job("main", "hello", vec![]).await.unwrap();
    let error = engine
        .submit_job("main", "world", vec![])
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        EngineError::EngineFailed { message }
            if message.contains("active prompt job") && message.contains(first.job_id.as_str())
    ));
}

#[tokio::test]
async fn llm_engine_branch_lock_uses_service_lock() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[]);
    let llm = Arc::new(LlmService::new(store, backend));
    let engine = ConversationEngine::new(llm.clone());

    let guard = llm.lock_branch_scope("main").await;
    let started = Arc::new(Notify::new());
    let acquired = Arc::new(Notify::new());
    let waiter = tokio::spawn({
        let engine = engine.clone();
        let started = started.clone();
        let acquired = acquired.clone();
        async move {
            started.notify_one();
            let _guard = engine.lock_branch("main").await;
            acquired.notify_one();
        }
    });

    started.notified().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), acquired.notified())
            .await
            .is_err()
    );

    drop(guard);
    tokio::time::timeout(Duration::from_secs(1), acquired.notified())
        .await
        .unwrap();
    waiter.await.unwrap();
}

#[tokio::test]
async fn llm_engine_coalesces_duplicate_drive_job_calls() {
    let store = MemoryStore::new();
    let backend = BlockingBackend {
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let started = backend.started.clone();
    let release = backend.release.clone();
    let calls = backend.calls.clone();
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    let job = engine.submit_job("main", "hello", vec![]).await.unwrap();

    let first = tokio::spawn({
        let engine = engine.clone();
        let job_id = job.job_id.clone();
        async move { engine.drive_job(&job_id).await }
    });

    started.notified().await;

    let second = tokio::spawn({
        let engine = engine.clone();
        let job_id = job.job_id.clone();
        async move { engine.drive_job(&job_id).await }
    });

    sleep(Duration::from_millis(20)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    release.notify_waiters();

    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(first.status, JobStatus::Finished);
    assert_eq!(second.status, JobStatus::Finished);
    assert_eq!(first.head, second.head);
}

#[tokio::test]
async fn llm_engine_join_job_waits_for_idle_job_without_starting_it() {
    let store = MemoryStore::new();
    let backend = BlockingBackend {
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let calls = backend.calls.clone();
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    let job = engine.submit_job("main", "hello", vec![]).await.unwrap();

    let result =
        tokio::time::timeout(Duration::from_millis(20), engine.join_job(&job.job_id)).await;

    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.get_job(&job.job_id).unwrap().status,
        JobStatus::Queued
    );
}

#[tokio::test]
async fn llm_engine_join_job_waits_for_later_driver_notification() {
    let store = MemoryStore::new();
    let backend = BlockingBackend {
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let started = backend.started.clone();
    let release = backend.release.clone();
    let calls = backend.calls.clone();
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    let job = engine.submit_job("main", "hello", vec![]).await.unwrap();

    let joiner = tokio::spawn({
        let engine = engine.clone();
        let job_id = job.job_id.clone();
        async move { engine.join_job(&job_id).await }
    });
    sleep(Duration::from_millis(20)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!joiner.is_finished());

    let driver = tokio::spawn({
        let engine = engine.clone();
        let job_id = job.job_id.clone();
        async move { engine.drive_job(&job_id).await }
    });
    started.notified().await;
    release.notify_waiters();

    let driven = driver.await.unwrap().unwrap();
    let joined = joiner.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(driven.status, JobStatus::Finished);
    assert_eq!(joined.status, JobStatus::Finished);
    assert_eq!(driven.head, joined.head);
}

#[tokio::test]
async fn llm_engine_join_job_observes_driver_from_another_engine_instance() {
    let store = MemoryStore::new();
    let backend = BlockingBackend {
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let started = backend.started.clone();
    let release = backend.release.clone();
    let calls = backend.calls.clone();
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let join_engine = ConversationEngine::new(llm.clone());
    let drive_engine = ConversationEngine::new(llm);
    let job = join_engine
        .submit_job("main", "hello", vec![])
        .await
        .unwrap();

    let joiner = tokio::spawn({
        let engine = join_engine.clone();
        let job_id = job.job_id.clone();
        async move { engine.join_job(&job_id).await }
    });
    sleep(Duration::from_millis(20)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!joiner.is_finished());

    let driver = tokio::spawn({
        let job_id = job.job_id.clone();
        async move { drive_engine.drive_job(&job_id).await }
    });
    started.notified().await;
    release.notify_waiters();

    let driven = driver.await.unwrap().unwrap();
    let joined = joiner.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(driven.status, JobStatus::Finished);
    assert_eq!(joined.status, JobStatus::Finished);
    assert_eq!(driven.head, joined.head);
}

#[tokio::test]
async fn llm_engine_join_job_waits_for_inflight_job() {
    let store = MemoryStore::new();
    let backend = BlockingBackend {
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let started = backend.started.clone();
    let release = backend.release.clone();
    let calls = backend.calls.clone();
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    let job = engine.submit_job("main", "hello", vec![]).await.unwrap();

    let driver = tokio::spawn({
        let engine = engine.clone();
        let job_id = job.job_id.clone();
        async move { engine.drive_job(&job_id).await }
    });
    started.notified().await;

    let joiner = tokio::spawn({
        let engine = engine.clone();
        let job_id = job.job_id.clone();
        async move { engine.join_job(&job_id).await }
    });
    sleep(Duration::from_millis(20)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!joiner.is_finished());

    release.notify_waiters();
    let driven = driver.await.unwrap().unwrap();
    let joined = joiner.await.unwrap().unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(driven.status, JobStatus::Finished);
    assert_eq!(joined.status, JobStatus::Finished);
    assert_eq!(driven.head, joined.head);
}

#[tokio::test]
async fn llm_engine_drive_job_returns_snapshot_after_backend_failure() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[(
        "main",
        &[Err(BackendError::Failed {
            message: "backend failed".to_owned(),
        })],
    )]);
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    let job = engine.submit_job("main", "hello", vec![]).await.unwrap();

    let snapshot = engine.drive_job(&job.job_id).await.unwrap();

    assert_eq!(snapshot.status, JobStatus::Running);
    assert_eq!(snapshot.branch, "main");
    assert_eq!(snapshot.work_branch, "main");
    let events = store.list_queue_messages(SYSTEM_EVENT_QUEUE).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].queue, SYSTEM_EVENT_QUEUE);
    let payload = &events[0].payload;
    assert_eq!(payload["type"], "llm.backend_failure.recovery_requested");
    assert_eq!(payload["version"], 1);
    assert_eq!(payload["data"]["job_id"], job.job_id);
    assert_eq!(payload["data"]["root_branch"], "main");
    assert_eq!(payload["data"]["work_branch"], "main");
    assert_eq!(payload["data"]["failed_branch"], "main");
    assert_eq!(payload["data"]["base_node_id"], job.base);
    assert_eq!(payload["data"]["message"], "backend failed");
    assert_eq!(
        payload["dedupe_key"],
        format!("llm.backend_failure:{}:main:{}", job.job_id, job.base)
    );
}

#[tokio::test]
async fn llm_engine_retries_job_from_before_failure_node() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[("main", &[Ok("recovered")])]);
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    let job = engine.submit_job("main", "hello", vec![]).await.unwrap();
    let current_head = store.get_branch_head("main").unwrap();
    let failure_id = store
        .append(NewNode {
            parent: job.base.clone(),
            role: Role::System,
            metadata: BackendMetadata::builder().build(),
            kind: Kind::Failure("transient backend outage".to_owned()),
        })
        .unwrap();
    store
        .set_branch_head("main", &current_head, &failure_id)
        .unwrap();

    let recovered = engine.drive_job(&job.job_id).await.unwrap();

    assert_eq!(recovered.status, JobStatus::Finished);
    assert_eq!(recovered.work_branch, "main");
    assert_eq!(
        store.get_node(&recovered.head).unwrap().kind,
        Kind::Text("recovered".to_owned())
    );
}

#[tokio::test]
async fn llm_engine_retries_disconnected_rebased_job_with_latest_branch_session() {
    let store = MemoryStore::new();
    let backend = ModelGateBackend::new("good-model");
    let calls = backend.calls.clone();
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    let mut config = session_config("main");
    config.model = "bad-model".to_owned();
    llm.create_session(config).await.unwrap();
    let engine = ConversationEngine::new(llm.clone());
    let job = engine.submit_job("main", "hello", vec![]).await.unwrap();

    let failed = engine.drive_job(&job.job_id).await.unwrap();
    assert_eq!(failed.status, JobStatus::Running);
    assert_eq!(failed.head, job.base);

    llm.rebase_session(
        "main",
        SessionConfigPatch {
            model: Some("good-model".to_owned()),
            ..SessionConfigPatch::default()
        },
    )
    .await
    .unwrap();

    let recovered = engine.drive_job(&job.job_id).await.unwrap();

    assert_eq!(recovered.status, JobStatus::Finished);
    assert_eq!(
        store.get_node(&recovered.head).unwrap().kind,
        Kind::Text("recovered".to_owned())
    );
    assert_eq!(
        calls.lock().await.as_slice(),
        ["bad-model".to_owned(), "good-model".to_owned()]
    );
}

#[tokio::test]
async fn llm_engine_retrying_failure_node_does_not_enqueue_duplicate_recovery_event() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[(
        "main",
        &[Err(BackendError::Failed {
            message: "backend still failed".to_owned(),
        })],
    )]);
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    let job = engine.submit_job("main", "hello", vec![]).await.unwrap();
    let current_head = store.get_branch_head("main").unwrap();
    let failure_id = store
        .append(NewNode {
            parent: job.base.clone(),
            role: Role::System,
            metadata: BackendMetadata::builder().build(),
            kind: Kind::Failure("transient backend outage".to_owned()),
        })
        .unwrap();
    store
        .set_branch_head("main", &current_head, &failure_id)
        .unwrap();

    let snapshot = engine.drive_job(&job.job_id).await.unwrap();

    assert_eq!(snapshot.status, JobStatus::Running);
    assert!(
        store
            .list_queue_messages(SYSTEM_EVENT_QUEUE)
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn llm_engine_finishes_job_after_unrecoverable_resume_error() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[]);
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    let job = engine.submit_job("main", "hello", vec![]).await.unwrap();
    let job_id = job.job_id.clone();

    store.delete_branch("main").unwrap();
    let error = engine.drive_job(&job_id).await.unwrap_err();

    assert!(matches!(error, EngineError::SessionMissing { branch } if branch == "main"));
    assert_eq!(store.get_job(&job_id).unwrap().status, JobStatus::Finished);
    assert!(engine.active_branch_prompt_job("main").unwrap().is_none());
}

#[tokio::test]
async fn llm_engine_keeps_recovery_branch_as_current_work_until_it_recovers_root_branch() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[
        (
            "main",
            &[Err(BackendError::Failed {
                message: "main failed".to_owned(),
            })],
        ),
        (
            "recovery-b",
            &[Err(BackendError::Failed {
                message: "recovery b failed".to_owned(),
            })],
        ),
        ("recovery-c", &[Ok("recovered by c")]),
    ]);
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    let job = engine.submit_job("main", "hello", vec![]).await.unwrap();

    let failed_a = engine.drive_job(&job.job_id).await.unwrap();
    assert_eq!(failed_a.status, JobStatus::Running);
    assert_eq!(failed_a.branch, "main");
    assert_eq!(failed_a.work_branch, "main");
    let first_event = store
        .list_queue_messages(SYSTEM_EVENT_QUEUE)
        .unwrap()
        .pop()
        .expect("first recovery event should exist");
    let retry_from_a = first_event.payload["data"]["retry_from_node_id"]
        .as_str()
        .expect("first event should include retry node")
        .to_owned();

    engine.service().fork("recovery-b", &retry_from_a).unwrap();
    let on_b = engine
        .set_job_work_branch(&job.job_id, "main", "recovery-b")
        .unwrap();
    assert_eq!(on_b.status, JobStatus::Running);
    assert_eq!(on_b.branch, "main");
    assert_eq!(on_b.work_branch, "recovery-b");

    let failed_b = engine.drive_job(&job.job_id).await.unwrap();
    assert_eq!(failed_b.status, JobStatus::Running);
    assert_eq!(failed_b.work_branch, "recovery-b");
    let events = store.list_queue_messages(SYSTEM_EVENT_QUEUE).unwrap();
    assert_eq!(events.len(), 2);
    let second_event = events.last().unwrap();
    assert_eq!(second_event.payload["data"]["root_branch"], "main");
    assert_eq!(second_event.payload["data"]["work_branch"], "recovery-b");
    assert_eq!(second_event.payload["data"]["failed_branch"], "recovery-b");
    assert_eq!(second_event.payload["data"]["message"], "recovery b failed");
    let retry_from_b = second_event.payload["data"]["retry_from_node_id"]
        .as_str()
        .expect("second event should include retry node")
        .to_owned();

    engine.service().fork("recovery-c", &retry_from_b).unwrap();
    let on_c = engine
        .set_job_work_branch(&job.job_id, "recovery-b", "recovery-c")
        .unwrap();
    assert_eq!(on_c.status, JobStatus::Running);
    assert_eq!(on_c.work_branch, "recovery-c");

    let recovered = engine.drive_job(&job.job_id).await.unwrap();

    assert_eq!(recovered.status, JobStatus::Finished);
    assert_eq!(recovered.branch, "main");
    assert_eq!(recovered.work_branch, "main");
    assert_eq!(store.get_branch_head("main").unwrap(), recovered.head);
    assert_eq!(
        store.get_node(&recovered.head).unwrap().kind,
        Kind::Text("recovered by c".to_owned())
    );
    assert!(engine.active_branch_prompt_job("main").unwrap().is_none());
    assert!(
        engine
            .active_branch_prompt_job("recovery-b")
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .active_branch_prompt_job("recovery-c")
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn llm_engine_finishes_job_when_recovery_restore_fails() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[
        (
            "main",
            &[Err(BackendError::Failed {
                message: "main failed".to_owned(),
            })],
        ),
        ("recovery-b", &[Ok("recovered by b")]),
    ]);
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    let job = engine.submit_job("main", "hello", vec![]).await.unwrap();
    let failed = engine.drive_job(&job.job_id).await.unwrap();
    let event = store
        .list_queue_messages(SYSTEM_EVENT_QUEUE)
        .unwrap()
        .pop()
        .expect("recovery event should exist");
    let retry_from = event.payload["data"]["retry_from_node_id"]
        .as_str()
        .expect("event should include retry node");

    engine.service().fork("recovery-b", retry_from).unwrap();
    engine
        .set_job_work_branch(&job.job_id, &failed.work_branch, "recovery-b")
        .unwrap();
    store.delete_branch("main").unwrap();

    let recovered = engine.drive_job(&job.job_id).await.unwrap();

    assert_eq!(recovered.status, JobStatus::Finished);
    assert_eq!(recovered.branch, "main");
    assert_eq!(recovered.work_branch, "recovery-b");
    assert!(engine.active_branch_prompt_job("main").unwrap().is_none());
    assert!(
        engine
            .active_branch_prompt_job("recovery-b")
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn llm_engine_maps_missing_session_error() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[]);
    let llm = Arc::new(LlmService::new(store, backend));
    let engine = ConversationEngine::new(llm);

    let error = engine.reply("main", "hello").await.unwrap_err();

    assert!(matches!(error, EngineError::SessionMissing { branch } if branch == "main"));
}

#[tokio::test]
async fn llm_engine_reply_surfaces_backend_failure_message() {
    let store = MemoryStore::new();
    let backend = AlwaysFailBackend::new();
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);

    let error = engine.reply("main", "hello").await.unwrap_err();
    match error {
        EngineError::EngineFailed { message } => {
            assert!(
                message.contains("waiting for recovery"),
                "actual message: {message}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
    let events = store.list_queue_messages(SYSTEM_EVENT_QUEUE).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].payload["type"],
        "llm.backend_failure.recovery_requested"
    );
    assert!(engine.active_branch_prompt_job("main").unwrap().is_none());
}

#[tokio::test]
async fn llm_engine_reply_rejects_intermediate_text_without_terminal_response() {
    let store = MemoryStore::new();
    let backend = MissingFinalTextBackend::new();
    let llm = Arc::new(LlmService::new(store, backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);

    let error = engine.reply("main", "hello").await.unwrap_err();
    match error {
        EngineError::EngineFailed { message } => {
            assert!(
                message.contains("waiting for recovery"),
                "actual message: {message}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn core_service_rejects_empty_message_content() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[]);
    let llm = Arc::new(LlmService::new(store, backend));
    let service = CoreService::new(
        FixedBranchResolver::new("main"),
        ConversationEngine::new(llm),
    );

    let error = service
        .handle_message(InboundMessage::cli("cli", "cli", "   "))
        .await
        .unwrap_err();

    assert!(
        matches!(error, Error::InvalidInput { message } if message == "message content is empty")
    );
}

#[tokio::test]
async fn core_service_handles_batch_prompt_across_multiple_branches() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[
        ("main", &[Ok("main done")]),
        ("draft", &[Ok("draft done")]),
    ]);
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    llm.create_session(session_config("main")).await.unwrap();
    llm.create_session(session_config("draft")).await.unwrap();
    let service = CoreService::new(
        FixedBranchResolver::new("main"),
        ConversationEngine::new(llm),
    );

    let result = service
        .handle_batch_prompt(BatchPromptRequest {
            items: vec![
                BranchPromptRequest {
                    branch: "main".to_owned(),
                    prompt: "hello".to_owned(),
                    attachments: vec![],
                    merge_parents: vec![],
                },
                BranchPromptRequest {
                    branch: "draft".to_owned(),
                    prompt: "world".to_owned(),
                    attachments: vec![],
                    merge_parents: vec![],
                },
            ],
            max_concurrency: 2,
        })
        .await
        .unwrap();

    assert_eq!(result.outcomes.len(), 2);
    assert_eq!(result.outcomes[0].branch, "main");
    assert_eq!(result.outcomes[0].status(), BranchPromptStatus::Succeeded);
    assert_eq!(result.outcomes[0].text(), Some("main done"));
    assert_eq!(result.outcomes[1].branch, "draft");
    assert_eq!(result.outcomes[1].status(), BranchPromptStatus::Succeeded);
    assert_eq!(result.outcomes[1].text(), Some("draft done"));
    let jobs = store.list_jobs().unwrap();
    assert_eq!(jobs.len(), 2);
    assert!(jobs.values().all(|job| job.status == JobStatus::Finished));
}

#[tokio::test]
async fn core_service_batch_prompt_reports_per_branch_failures() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[("main", &[Ok("main done")])]);
    let llm = Arc::new(LlmService::new(store, backend));
    llm.create_session(session_config("main")).await.unwrap();
    let service = CoreService::new(
        FixedBranchResolver::new("main"),
        ConversationEngine::new(llm),
    );

    let result = service
        .handle_batch_prompt(BatchPromptRequest {
            items: vec![
                BranchPromptRequest {
                    branch: "main".to_owned(),
                    prompt: "hello".to_owned(),
                    attachments: vec![],
                    merge_parents: vec![],
                },
                BranchPromptRequest {
                    branch: "missing".to_owned(),
                    prompt: "world".to_owned(),
                    attachments: vec![],
                    merge_parents: vec![],
                },
            ],
            max_concurrency: 2,
        })
        .await
        .unwrap();

    assert_eq!(result.outcomes[0].status(), BranchPromptStatus::Succeeded);
    assert_eq!(result.outcomes[1].status(), BranchPromptStatus::Failed);
    assert!(
        result.outcomes[1]
            .error()
            .is_some_and(|error| error.contains("Missing session"))
    );
}

#[tokio::test]
async fn core_service_batch_prompt_rejects_duplicate_branch() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[]);
    let llm = Arc::new(LlmService::new(store, backend));
    let service = CoreService::new(
        FixedBranchResolver::new("main"),
        ConversationEngine::new(llm),
    );

    let error = service
        .handle_batch_prompt(BatchPromptRequest {
            items: vec![
                BranchPromptRequest {
                    branch: "main".to_owned(),
                    prompt: "hello".to_owned(),
                    attachments: vec![],
                    merge_parents: vec![],
                },
                BranchPromptRequest {
                    branch: "main".to_owned(),
                    prompt: "world".to_owned(),
                    attachments: vec![],
                    merge_parents: vec![],
                },
            ],
            max_concurrency: 2,
        })
        .await
        .unwrap_err();

    assert!(
        matches!(error, Error::InvalidInput { message } if message == "batch prompt branch \"main\" is duplicated")
    );
}

#[tokio::test]
async fn llm_engine_resumes_running_job_from_nodes_after_restart() {
    let store = MemoryStore::new();
    let setup_backend = FakeBackend::with_responses(&[]);
    let setup_llm = Arc::new(LlmService::new(store.clone(), setup_backend));
    setup_llm
        .create_session(session_config("main"))
        .await
        .unwrap();
    let original_head = store.get_branch_head("main").unwrap();
    submit_prompt_job(&store, "main", "keep going");
    store
        .set_job_status(
            store
                .list_jobs()
                .unwrap()
                .keys()
                .next()
                .expect("job should exist"),
            JobStatus::Queued,
            JobStatus::Running,
        )
        .unwrap();

    let job = store.list_jobs().unwrap();
    let job = job.values().next().unwrap().clone();
    store
        .set_branch_head("main", &original_head, &job.base)
        .unwrap();
    let tool_use_id = store
        .append(NewNode {
            parent: job.base.clone(),
            role: Role::LLM,
            metadata: BackendMetadata::builder()
                .execution(&ExecutionMetadata::new("execution-step-1".to_owned()))
                .provider(&ProviderMetadata::new(Some("tool-call-1".to_owned())))
                .build(),
            kind: Kind::tool_use(ToolUse {
                id: "tool-call-1".to_owned(),
                name: "exec_command".to_owned(),
                input: serde_json::json!({"cmd": "printf 'hello' > trace.txt"}),
            }),
        })
        .unwrap();
    store
        .set_branch_head("main", &job.base, &tool_use_id)
        .unwrap();
    let tool_result_id = store
        .append(NewNode {
            parent: tool_use_id.clone(),
            role: Role::User,
            metadata: BackendMetadata::builder()
                .execution(&ExecutionMetadata::new("execution-step-2".to_owned()))
                .provider(&ProviderMetadata::new(Some("tool-call-1".to_owned())))
                .build(),
            kind: Kind::tool_result(ToolResult {
                id: "tool-call-1".to_owned(),
                output: "exit_status: 0\nstdout:\n\nstderr:\n".to_owned(),
            }),
        })
        .unwrap();
    store
        .set_branch_head("main", &tool_use_id, &tool_result_id)
        .unwrap();

    let resumed_backend = FakeBackend::with_responses(&[("main", &[Ok("recovered")])]);
    let resumed_llm = Arc::new(LlmService::new(store.clone(), resumed_backend));
    let resumed_engine = ConversationEngine::new(resumed_llm);

    resumed_engine.resume_incomplete_jobs().await.unwrap();

    let stored_job = resumed_engine.get_job(&job.job_id).unwrap();
    assert_eq!(stored_job.status, JobStatus::Finished);
    assert_eq!(store.get_branch_head("main").unwrap(), stored_job.head);
}

#[tokio::test]
async fn llm_engine_executes_skill_and_cleans_up_child_branch() {
    let store = MemoryStore::new();
    let backend = AnyBranchBackend::new("child result");
    let calls = backend.calls.clone();
    let provider_contexts = backend.provider_contexts.clone();
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    let base_session = llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    store
        .add_skill(
            SessionRole::Runner,
            "fast-rust",
            SkillVersionSpec {
                description: "Review Rust changes.".to_owned(),
                body: "# Fast Rust".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        )
        .unwrap();
    let caller_task = "Review the inherited task from the parent prompt.";
    let caller_prompt_id = store
        .append(NewNode {
            parent: base_session.anchor_id.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                vec![],
                PromptAnchor {
                    prompt: caller_task.to_owned(),
                    attachments: vec![],
                },
            )),
        })
        .unwrap();
    let invocation_id = append_skill_invocation_node(&store, &caller_prompt_id, "fast-rust");

    let result = engine
        .execute_skill_invocation(
            Path::new("."),
            "main",
            SessionRole::Orchestrator,
            &invocation_id,
            "fast-rust",
            None,
        )
        .await
        .unwrap();

    assert_eq!(result.text, "child result");
    let response_node = store.get_node(&result.response_node_id).unwrap();
    assert!(matches!(
        response_node.kind,
        Kind::Text(ref text) if text == "child result"
    ));

    let calls = calls.lock().await;
    assert_eq!(calls.len(), 1);
    let child_branch = calls[0].clone();
    drop(calls);

    let branch_error = store.get_branch_head(&child_branch).unwrap_err();
    assert!(matches!(
        branch_error,
        coco_llm::coco_mem::StoreError::BranchNotFound { name } if name == child_branch
    ));
    let state_error = store.get_session_state(&child_branch).unwrap_err();
    assert!(matches!(
        state_error,
        coco_llm::coco_mem::StoreError::BranchNotFound { name } if name == child_branch
    ));
    assert!(store.list_jobs().unwrap().is_empty());

    let children = store.list_children(&invocation_id).unwrap();
    let child_session_anchor = children
        .iter()
        .find_map(|node| match &node.kind {
            Kind::Anchor(anchor) => anchor.as_session().map(|session| (node, session)),
            _ => None,
        })
        .expect("child execution should persist a child session anchor");
    assert_eq!(child_session_anchor.0.parent, invocation_id);
    assert_eq!(child_session_anchor.1.role, SessionRole::Runner);
    assert!(child_session_anchor.1.enable_coco_shim);
    assert!(
        child_session_anchor
            .1
            .prompt
            .contains("You are executing the skill `fast-rust`")
    );
    assert!(
        !child_session_anchor
            .1
            .prompt
            .contains("Additional task from caller:")
    );
    let child_session_children = store.list_children(&child_session_anchor.0.id).unwrap();
    assert!(!child_session_children.iter().any(|node| matches!(
        &node.kind,
        Kind::Anchor(anchor) if anchor.as_prompt().is_some()
    )));

    let provider_contexts = provider_contexts.lock().await;
    assert_eq!(provider_contexts.len(), 1);
    assert!(provider_contexts[0].contains(caller_task));
    assert!(!provider_contexts[0].contains("skill_invocation"));
}

#[tokio::test]
async fn llm_engine_materializes_store_skill_scripts() {
    let store = MemoryStore::new();
    store
        .add_skill(
            SessionRole::Orchestrator,
            "scripted-skill",
            SkillVersionSpec {
                description: "Run a uv single-file script.".to_owned(),
                body: "# Scripted Skill".to_owned(),
                scripts: vec![SkillScript {
                    path: "scripts/inspect.py".to_owned(),
                    content: "print('inspect')\n".to_owned(),
                }],
                enable_coco_shim: true,
            },
        )
        .unwrap();

    let backend = AnyBranchBackend::new("script result");
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    let base_session = llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    let invocation_id =
        append_skill_invocation_node(&store, &base_session.anchor_id, "scripted-skill");

    engine
        .execute_skill_invocation(
            Path::new("."),
            "main",
            SessionRole::Orchestrator,
            &invocation_id,
            "scripted-skill",
            None,
        )
        .await
        .unwrap();

    let children = store.list_children(&invocation_id).unwrap();
    let child_session_anchor = children
        .iter()
        .find_map(|node| match &node.kind {
            Kind::Anchor(anchor) => anchor.as_session().map(|session| (node, session)),
            _ => None,
        })
        .expect("child execution should persist a child session anchor");
    let active_skill = child_session_anchor
        .1
        .active_skill
        .as_ref()
        .expect("scripted skill should persist identity");

    assert_eq!(active_skill.name, "scripted-skill");
    let expected_persistent_directory = Path::new(".")
        .join(".coco-workspace")
        .join("skills")
        .join("orchestrator")
        .join("scripted-skill")
        .join("data");
    assert!(expected_persistent_directory.exists());
    assert!(
        child_session_anchor
            .1
            .prompt
            .contains("uv run --script \"$COCO_SKILL_DIR/scripts/inspect.py\"")
    );
    assert!(
        child_session_anchor
            .1
            .prompt
            .contains("Use $COCO_SKILL_PERSIST_DIR")
    );
}

#[tokio::test]
async fn llm_engine_cleans_up_skill_runtime_when_materialization_fails() {
    let runtime_root = std::env::temp_dir().join("coco").join("skill-sessions");
    let body = format!("# Bad Scripted Skill {}", nanoid::nanoid!());
    let store = MemoryStore::new();
    store
        .add_skill(
            SessionRole::Orchestrator,
            "bad-scripted-skill",
            SkillVersionSpec {
                description: "Has an invalid script path.".to_owned(),
                body: body.clone(),
                scripts: vec![SkillScript {
                    path: "../escape.py".to_owned(),
                    content: "print('escape')\n".to_owned(),
                }],
                enable_coco_shim: true,
            },
        )
        .unwrap();

    let backend = AnyBranchBackend::new("should not run");
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    let base_session = llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    let invocation_id =
        append_skill_invocation_node(&store, &base_session.anchor_id, "bad-scripted-skill");

    let error = engine
        .execute_skill_invocation(
            Path::new("."),
            "main",
            SessionRole::Orchestrator,
            &invocation_id,
            "bad-scripted-skill",
            None,
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("invalid skill script path"));
    assert!(!skill_runtime_contains_body(&runtime_root, &body));
}

#[tokio::test]
async fn llm_engine_cleans_up_child_branch_when_skill_fails() {
    let store = MemoryStore::new();
    let backend = AlwaysFailBackend::new();
    let calls = backend.calls.clone();
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    let base_session = llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    store
        .add_skill(
            SessionRole::Runner,
            "fast-rust",
            SkillVersionSpec {
                description: "Review Rust changes.".to_owned(),
                body: "# Fast Rust".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: false,
            },
        )
        .unwrap();
    let invocation_id = append_skill_invocation_node(&store, &base_session.anchor_id, "fast-rust");

    let error = engine
        .execute_skill_invocation(
            Path::new("."),
            "main",
            SessionRole::Orchestrator,
            &invocation_id,
            "fast-rust",
            None,
        )
        .await
        .unwrap_err();

    assert!(matches!(error, EngineError::EngineFailed { .. }));
    let calls = calls.lock().await;
    assert_eq!(calls.len(), 1);
    let child_branch = calls[0].clone();
    drop(calls);

    let branch_error = store.get_branch_head(&child_branch).unwrap_err();
    assert!(matches!(
        branch_error,
        coco_llm::coco_mem::StoreError::BranchNotFound { name } if name == child_branch
    ));
    assert!(store.list_jobs().unwrap().is_empty());
}
