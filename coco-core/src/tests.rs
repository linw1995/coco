use std::collections::{HashMap, VecDeque};
use std::ffi::OsStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use coco_llm::coco_mem::{
    Anchor, BackendMetadata, BranchStore, ExecutionMetadata, JobStatus, JobStore, Kind,
    MemoryStore, NewNode, NodeStore, PromptAnchor, ProviderMetadata, Role, SessionRole,
    SessionStore, ToolResult, ToolUse,
};
use coco_llm::{
    BackendError, BackendEventPayload, BackendTurn, CompletionBackend, CompletionMessage,
    LlmService, Provider, SessionConfig, StepContext,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::time::{Duration, sleep};

use crate::{
    BatchPromptRequest, BranchPromptRequest, BranchPromptStatus, BranchResolveError,
    BranchResolver, ChannelKind, ConversationEngine, CoreService, EngineError, Error,
    FixedBranchResolver, InboundMessage,
};

type FakeResponseQueue =
    Arc<Mutex<HashMap<String, VecDeque<std::result::Result<BackendTurn, BackendError>>>>>;

fn append_use_skill_node<S>(store: &S, parent: &str, skill_name: &str) -> String
where
    S: NodeStore,
{
    store
        .append(NewNode {
            parent: parent.to_owned(),
            role: Role::LLM,
            metadata: BackendMetadata::builder()
                .execution(&ExecutionMetadata::new("execution-use-skill".to_owned()))
                .provider(&ProviderMetadata::new(Some("tool-call-1".to_owned())))
                .build(),
            kind: Kind::ToolUse(ToolUse {
                id: "tool-call-1".to_owned(),
                name: "use_skill".to_owned(),
                input: serde_json::json!({ "name": skill_name }),
            }),
        })
        .unwrap()
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
    text: String,
}

impl AnyBranchBackend {
    fn new(text: &str) -> Self {
        Self {
            calls: Arc::new(AsyncMutex::new(Vec::new())),
            text: text.to_owned(),
        }
    }
}

#[async_trait]
impl CompletionBackend for AnyBranchBackend {
    async fn step(&self, ctx: StepContext<'_>) -> std::result::Result<BackendTurn, BackendError> {
        self.calls.lock().await.push(ctx.request.branch.clone());
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
                },
            )),
        })
        .unwrap();
    store.submit_job(branch, &prompt_anchor_id).unwrap()
}

async fn with_env_async<T, F, Fut>(entries: &[(&str, Option<&OsStr>)], run: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    static ENV_LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();

    let _guard = ENV_LOCK.get_or_init(|| AsyncMutex::new(())).lock().await;
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

fn write_skill(root: &std::path::Path, relative_dir: &str, body: &str) {
    let dir = root.join(relative_dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("SKILL.md"), body).unwrap();
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

    assert_eq!(snapshot.status, JobStatus::Finished);
    assert_eq!(snapshot.branch, "main");
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
    let llm = Arc::new(LlmService::new(store, backend));
    llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);

    let error = engine.reply("main", "hello").await.unwrap_err();
    match error {
        EngineError::EngineFailed { message } => {
            assert!(
                message.contains("backend failed"),
                "actual message: {message}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
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
                message.contains("completion response did not include assistant text"),
                "actual message: {message}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn core_service_rejects_empty_message_text() {
    let store = MemoryStore::new();
    let backend = FakeBackend::with_responses(&[]);
    let llm = Arc::new(LlmService::new(store, backend));
    let service = CoreService::new(
        FixedBranchResolver::new("main"),
        ConversationEngine::new(llm),
    );

    let error = service
        .handle_message(InboundMessage::discord("channel", "user", "   "))
        .await
        .unwrap_err();

    assert!(matches!(error, Error::InvalidInput { message } if message == "message text is empty"));
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
                    merge_parents: vec![],
                },
                BranchPromptRequest {
                    branch: "draft".to_owned(),
                    prompt: "world".to_owned(),
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
                    merge_parents: vec![],
                },
                BranchPromptRequest {
                    branch: "missing".to_owned(),
                    prompt: "world".to_owned(),
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
                    merge_parents: vec![],
                },
                BranchPromptRequest {
                    branch: "main".to_owned(),
                    prompt: "world".to_owned(),
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
            kind: Kind::ToolUse(ToolUse {
                id: "tool-call-1".to_owned(),
                name: "bash".to_owned(),
                input: serde_json::json!({"command": "printf 'hello' > trace.txt"}),
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
            kind: Kind::ToolResult(ToolResult {
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
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    let base_session = llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    let root = tempfile::tempdir().unwrap();
    write_skill(
        root.path(),
        "fast-rust",
        r#"---
name: "fast-rust"
description: "Review Rust changes."
session_role: "runner"
enable_coco_shim: true
---

# Fast Rust
"#,
    );
    let path_env = std::env::join_paths([root.path()]).unwrap();
    let tool_use_id = append_use_skill_node(&store, &base_session.anchor_id, "fast-rust");

    let result = with_env_async(
        &[("COCO_SKILLS_DIRS", Some(path_env.as_os_str()))],
        || async {
            engine
                .execute_skill(
                    root.path(),
                    "main",
                    SessionRole::Orchestrator,
                    &tool_use_id,
                    "fast-rust",
                )
                .await
        },
    )
    .await
    .unwrap();

    assert_eq!(result.result.text, "child result");

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

    let children = store.list_children(&tool_use_id).unwrap();
    let child_session_anchor = children
        .iter()
        .find_map(|node| match &node.kind {
            Kind::Anchor(anchor) => anchor.as_session().map(|session| (node, session)),
            _ => None,
        })
        .expect("child execution should persist a child session anchor");
    assert_eq!(child_session_anchor.0.parent, tool_use_id);
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
}

#[tokio::test]
async fn llm_engine_cleans_up_child_branch_when_skill_fails() {
    let store = MemoryStore::new();
    let backend = AlwaysFailBackend::new();
    let calls = backend.calls.clone();
    let llm = Arc::new(LlmService::new(store.clone(), backend));
    let base_session = llm.create_session(session_config("main")).await.unwrap();
    let engine = ConversationEngine::new(llm);
    let root = tempfile::tempdir().unwrap();
    write_skill(
        root.path(),
        "fast-rust",
        r#"---
name: "fast-rust"
description: "Review Rust changes."
---

# Fast Rust
"#,
    );
    let path_env = std::env::join_paths([root.path()]).unwrap();
    let tool_use_id = append_use_skill_node(&store, &base_session.anchor_id, "fast-rust");

    let error = with_env_async(
        &[("COCO_SKILLS_DIRS", Some(path_env.as_os_str()))],
        || async {
            engine
                .execute_skill(
                    root.path(),
                    "main",
                    SessionRole::Orchestrator,
                    &tool_use_id,
                    "fast-rust",
                )
                .await
        },
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
