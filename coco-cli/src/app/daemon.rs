use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use coco_channel::{
    ChannelRuntime, InboundMessage, MessageHandler, OutboundMessage, TelegramImageAttachment,
};
use coco_channel::{Error as ChannelError, telegram::TelegramChannel};
use coco_console::{ConsoleConfig, ConsolePublisher, ConsoleServerHandle, start_console_server};
use coco_core::{
    ConversationEngine, CoreService, EngineError, FixedBranchResolver, SYSTEM_EVENT_QUEUE,
};
use coco_llm::{
    CocoCliRuntimeRequest, CocoCliRuntimeResponse, CompletionBackend, LlmService, Provider,
    SessionConfig,
};
use coco_mem::{
    Anchor, AnchorPayload, JobStatus, Kind, MessageQueueItem, SessionRole, Store, StoreError,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use snafu::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, Notify};

use super::{
    config::{ChannelConfigs, ProviderProfiles, resolve_channel_secret},
    prompt::{PROMPT_JOB_QUEUE, QueuedPromptRequest, queue_prompt_job_request},
    run_forwarded_with_services,
    runtime::{ForwardedRuntimeInputs, RuntimeServices},
    session::resolve_session_config,
};
use crate::{
    Result,
    cli::{CliSessionRole, CliTool, DaemonCommand, DaemonSubcommand, SessionCreateCommand},
    error::{
        BindDaemonSocketSnafu, ChannelSnafu, ConsoleSnafu, CoreEngineSnafu, JoinChannelTaskSnafu,
        JoinDaemonServerSnafu, JoinMessageQueueTaskSnafu, LlmSnafu, ResolveDaemonSocketRootSnafu,
        ServeDaemonSocketSnafu, StoreSnafu,
    },
};

const DEFAULT_SESSION_BRANCH: &str = "main";
const BUILTIN_DAY_BRANCH: &str = "day";
const DEFAULT_SYSTEM_PROMPT: &str = "You are CoCo. An AI copilot";
const DAY_SYSTEM_PROMPT: &str = r#"You are CoCo Day, the built-in system event branch.

Your only job is to consume CoCo system events and turn them into concrete recovery work.
When you receive an LLM backend failure recovery event, inspect the event payload and run the `recovery` skill from this `day` branch through the injected `coco` command. The failed `work_branch` in the event is the target to inspect or repair, not the branch executing recovery. Do not create or select another recovery branch.

Use the `compact` skill when a target branch has accumulated enough anchors or history that future recovery is likely to exceed context budget. Compact with session graph inspection and `coco session handoff`."#;
const DEFAULT_MAX_TOKENS: u64 = 32_000;
const TELEGRAM_INBOUND_QUEUE: &str = "telegram.inbound";
const PROMPT_JOB_QUEUE_IDLE_DELAY: Duration = Duration::from_secs(1);
const TELEGRAM_QUEUE_IDLE_DELAY: Duration = Duration::from_secs(1);
const ACTIVE_JOB_JOIN_INTERVAL: Duration = Duration::from_secs(30);

pub(crate) struct CocoCliDaemonServerHandle<B, S> {
    socket_path: PathBuf,
    llm: Arc<LlmService<B, S>>,
    tasks: DaemonTaskGroup,
}

pub(crate) struct DaemonServerOptions<'a> {
    pub channel_configs: &'a ChannelConfigs,
    pub console_config: Option<ConsoleConfig>,
    pub console_publisher: Option<ConsolePublisher>,
}

pub(super) async fn run_daemon_command<B, S>(
    command: DaemonCommand,
    shared_store: &S,
    llm: &Arc<LlmService<B, S>>,
    provider_profiles: &ProviderProfiles,
    channel_configs: &ChannelConfigs,
    console_publisher: Option<ConsolePublisher>,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    let socket_path = resolve_daemon_socket_path(match &command.command {
        DaemonSubcommand::Serve(command) => command.socket.as_deref(),
    })?;
    let shared_engine = Arc::new(ConversationEngine::new(llm.clone()));
    let console_config = match (&command.command, console_publisher.as_ref()) {
        (DaemonSubcommand::Serve(command), Some(_)) if !command.no_console => Some(ConsoleConfig {
            addr: command.console_addr,
        }),
        _ => None,
    };
    let server = match command.command {
        DaemonSubcommand::Serve(_) => {
            ensure_initial_session(shared_store, llm, provider_profiles).await?;
            start_daemon_server(
                &socket_path,
                shared_store,
                llm,
                provider_profiles,
                &shared_engine,
                DaemonServerOptions {
                    channel_configs,
                    console_config,
                    console_publisher,
                },
            )?
        }
    };
    spawn_resume_incomplete_jobs(shared_engine);
    server.wait().await?;
    Ok(None)
}

pub(crate) async fn ensure_initial_session<B, S>(
    shared_store: &S,
    llm: &Arc<LlmService<B, S>>,
    provider_profiles: &ProviderProfiles,
) -> Result<()>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    if shared_store
        .list_session_states()
        .context(StoreSnafu)?
        .is_empty()
    {
        let config = resolve_session_config(default_session_create_command(), provider_profiles)?;
        tracing::info!(
            branch = %config.branch,
            max_tokens = config.max_tokens,
            tool_count = config.tools.len(),
            "creating default session on empty store"
        );
        llm.create_session(config).await.context(LlmSnafu)?;
    }

    ensure_builtin_day_session(shared_store, llm, provider_profiles).await?;
    Ok(())
}

async fn ensure_builtin_day_session<B, S>(
    shared_store: &S,
    llm: &Arc<LlmService<B, S>>,
    provider_profiles: &ProviderProfiles,
) -> Result<()>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    if builtin_day_session_is_valid(shared_store)? {
        return Ok(());
    }

    match shared_store.get_branch_head(BUILTIN_DAY_BRANCH) {
        Ok(_) => {
            tracing::warn!(
                branch = BUILTIN_DAY_BRANCH,
                "replacing invalid builtin day session"
            );
            shared_store
                .delete_branch(BUILTIN_DAY_BRANCH)
                .context(StoreSnafu)?;
        }
        Err(StoreError::BranchNotFound { .. }) => {}
        Err(source) => return Err(source).context(StoreSnafu),
    }

    let config = match resolve_session_config(day_session_create_command(), provider_profiles) {
        Ok(config) => config,
        Err(error) => {
            let Some(config) = derive_day_session_config(shared_store)? else {
                return Err(error);
            };
            config
        }
    };
    tracing::info!(
        branch = %config.branch,
        max_tokens = config.max_tokens,
        tool_count = config.tools.len(),
        "creating builtin day session"
    );
    llm.create_session(config).await.context(LlmSnafu)?;
    Ok(())
}

fn builtin_day_session_is_valid(store: &impl Store) -> Result<bool> {
    let head = match store.get_branch_head(BUILTIN_DAY_BRANCH) {
        Ok(head) => head,
        Err(StoreError::BranchNotFound { .. }) => return Ok(false),
        Err(source) => return Err(source).context(StoreSnafu),
    };
    match store.get_session_state(BUILTIN_DAY_BRANCH) {
        Ok(_) => {}
        Err(StoreError::BranchNotFound { .. }) => return Ok(false),
        Err(source) => return Err(source).context(StoreSnafu),
    }

    let ancestry = store.ancestry(&head).context(StoreSnafu)?;
    let Some(session) = ancestry.into_iter().find_map(|node| {
        let Kind::Anchor(Anchor {
            payload: AnchorPayload::Session(session),
            ..
        }) = node.kind
        else {
            return None;
        };
        Some(session)
    }) else {
        return Ok(false);
    };

    let expected_tools = default_builtin_day_tools()
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    let actual_tools = session
        .tools
        .iter()
        .map(|tool| tool.name.clone())
        .collect::<Vec<_>>();
    Ok(session.role == SessionRole::Orchestrator
        && session.system_prompt == DAY_SYSTEM_PROMPT
        && session.enable_coco_shim
        && actual_tools == expected_tools)
}

fn derive_day_session_config(store: &impl Store) -> Result<Option<SessionConfig>> {
    let mut branches = store
        .list_session_states()
        .context(StoreSnafu)?
        .into_keys()
        .filter(|branch| branch != BUILTIN_DAY_BRANCH)
        .collect::<Vec<_>>();
    branches.sort();
    if let Some(index) = branches
        .iter()
        .position(|branch| branch == DEFAULT_SESSION_BRANCH)
    {
        branches.swap(0, index);
    }

    for branch in branches {
        let head = store.get_branch_head(&branch).context(StoreSnafu)?;
        let ancestry = store.ancestry(&head).context(StoreSnafu)?;
        let Some(session_anchor) = ancestry.into_iter().find_map(|node| {
            let Kind::Anchor(anchor) = &node.kind else {
                return None;
            };
            let AnchorPayload::Session(session) = &anchor.payload else {
                return None;
            };
            Some(session.clone())
        }) else {
            continue;
        };
        let provider = session_anchor
            .provider
            .as_deref()
            .and_then(|provider| Provider::parse(provider).ok())
            .unwrap_or(Provider::OpenAi);
        return Ok(Some(SessionConfig {
            branch: BUILTIN_DAY_BRANCH.to_owned(),
            merge_parents: vec![],
            provider_profile: session_anchor.provider_profile,
            provider,
            model: session_anchor.model,
            role: SessionRole::Orchestrator,
            system_prompt: DAY_SYSTEM_PROMPT.to_owned(),
            prompt: String::new(),
            tools: default_builtin_day_tools(),
            temperature: session_anchor.temperature,
            max_tokens: session_anchor.max_tokens.or(Some(DEFAULT_MAX_TOKENS)),
            additional_params: session_anchor.additional_params,
            enable_coco_shim: true,
        }));
    }

    Ok(None)
}

fn default_builtin_day_tools() -> Vec<coco_mem::Tool> {
    ["exec_command", "write_stdin", "search_skill"]
        .into_iter()
        .map(|name| coco_llm::builtin_tool_definition(name).expect("builtin day tool should exist"))
        .collect()
}

fn default_session_create_command() -> SessionCreateCommand {
    SessionCreateCommand {
        branch: DEFAULT_SESSION_BRANCH.to_owned(),
        role: CliSessionRole::Orchestrator,
        provider_profile: None,
        system_prompt: DEFAULT_SYSTEM_PROMPT.to_owned(),
        prompt: String::new(),
        temperature: None,
        max_tokens: Some(DEFAULT_MAX_TOKENS),
        additional_params: None,
        tools: vec![
            CliTool::ExecCommand,
            CliTool::WriteStdin,
            CliTool::SearchSkill,
            CliTool::LoadImage,
        ],
        enable_coco_shim: true,
        disable_coco_shim: false,
    }
}

fn day_session_create_command() -> SessionCreateCommand {
    SessionCreateCommand {
        branch: BUILTIN_DAY_BRANCH.to_owned(),
        role: CliSessionRole::Orchestrator,
        provider_profile: None,
        system_prompt: DAY_SYSTEM_PROMPT.to_owned(),
        prompt: String::new(),
        temperature: None,
        max_tokens: Some(DEFAULT_MAX_TOKENS),
        additional_params: None,
        tools: vec![
            CliTool::ExecCommand,
            CliTool::WriteStdin,
            CliTool::SearchSkill,
        ],
        enable_coco_shim: true,
        disable_coco_shim: false,
    }
}

pub(crate) async fn resume_incomplete_jobs<B, S>(engine: &ConversationEngine<B, S>) -> Result<()>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    engine
        .resume_incomplete_jobs()
        .await
        .context(crate::error::CoreEngineSnafu)
}

fn spawn_resume_incomplete_jobs<B, S>(engine: Arc<ConversationEngine<B, S>>)
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    tokio::spawn(async move {
        if let Err(error) = resume_incomplete_jobs(engine.as_ref()).await {
            tracing::error!(
                error = %error,
                "failed to resume incomplete prompt jobs on daemon startup"
            );
        }
    });
}

pub fn resolve_default_daemon_socket_path() -> Result<PathBuf> {
    // This default path is only a convenience for explicitly selected daemon
    // mode. Because the socket is OS-scoped rather than project-scoped, callers
    // must opt in with `--daemon-socket` or `COCO_DAEMON_SOCKET` before a
    // command will talk to it.
    let current_dir = std::env::current_dir().context(ResolveDaemonSocketRootSnafu)?;
    let runtime_root = match std::env::var_os("XDG_RUNTIME_DIR") {
        Some(path) => {
            let path = PathBuf::from(path);
            if path.is_absolute() {
                path
            } else {
                current_dir.join(path)
            }
        }
        None => std::env::temp_dir(),
    };
    Ok(canonicalize_existing_path(&runtime_root)
        .join("coco")
        .join("coco-daemon.sock"))
}

pub(crate) fn resolve_daemon_socket_path(socket_path: Option<&Path>) -> Result<PathBuf> {
    match socket_path {
        Some(path) => Ok(path.to_path_buf()),
        None => resolve_default_daemon_socket_path(),
    }
}

pub(crate) fn start_daemon_server<B, S>(
    socket_path: &Path,
    shared_store: &S,
    llm: &Arc<LlmService<B, S>>,
    provider_profiles: &ProviderProfiles,
    shared_engine: &Arc<ConversationEngine<B, S>>,
    options: DaemonServerOptions<'_>,
) -> Result<CocoCliDaemonServerHandle<B, S>>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent).context(BindDaemonSocketSnafu {
            path: socket_path.to_path_buf(),
        })?;
    }
    if socket_path.exists() {
        std::fs::remove_file(socket_path).context(BindDaemonSocketSnafu {
            path: socket_path.to_path_buf(),
        })?;
    }

    let listener = UnixListener::bind(socket_path).context(BindDaemonSocketSnafu {
        path: socket_path.to_path_buf(),
    })?;
    tracing::info!(socket_path = %socket_path.display(), "daemon socket bound");
    let socket_path = socket_path.to_path_buf();
    let shared_store = shared_store.clone();
    let llm = llm.clone();
    let provider_profiles = provider_profiles.clone();
    let shared_engine = shared_engine.clone();
    let console = match options.console_config {
        Some(config) => Some(
            start_console_server(
                config,
                shared_store.clone(),
                options
                    .console_publisher
                    .expect("console publisher should exist when console is enabled"),
            )
            .context(ConsoleSnafu)?,
        ),
        None => None,
    };
    if let Some(console) = &console {
        tracing::info!(addr = %console.addr(), "coco console listening");
    }
    let channel_task = start_channel_task(options.channel_configs, &shared_store, &shared_engine)?;
    let message_queue_task = start_message_queue_task(&shared_store, &shared_engine);
    let handle_llm = llm.clone();

    let socket_task = start_socket_task(
        listener,
        shared_store,
        llm,
        provider_profiles,
        shared_engine,
    );

    Ok(CocoCliDaemonServerHandle {
        socket_path,
        llm: handle_llm,
        tasks: DaemonTaskGroup {
            socket_task: Some(socket_task),
            console,
            channel_task,
            message_queue_task: Some(message_queue_task),
        },
    })
}

fn start_socket_task<B, S>(
    listener: UnixListener,
    shared_store: S,
    llm: Arc<LlmService<B, S>>,
    provider_profiles: ProviderProfiles,
    shared_engine: Arc<ConversationEngine<B, S>>,
) -> tokio::task::JoinHandle<Result<()>>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    tokio::spawn(async move {
        serve_daemon_socket(
            listener,
            shared_store,
            llm,
            provider_profiles,
            shared_engine,
        )
        .await
    })
}

async fn serve_daemon_socket<B, S>(
    listener: UnixListener,
    shared_store: S,
    llm: Arc<LlmService<B, S>>,
    provider_profiles: ProviderProfiles,
    shared_engine: Arc<ConversationEngine<B, S>>,
) -> Result<()>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    let mut clients = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted.context(ServeDaemonSocketSnafu)?;
                clients.spawn(handle_daemon_client(
                    stream,
                    shared_store.clone(),
                    llm.clone(),
                    provider_profiles.clone(),
                    shared_engine.clone(),
                ));
            }
            Some(result) = clients.join_next(), if !clients.is_empty() => {
                if let Err(source) = result {
                    tracing::warn!(error = %source, "daemon client task failed");
                }
            }
        }
    }
}

async fn handle_daemon_client<B, S>(
    mut stream: UnixStream,
    shared_store: S,
    llm: Arc<LlmService<B, S>>,
    provider_profiles: ProviderProfiles,
    shared_engine: Arc<ConversationEngine<B, S>>,
) where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    let response = handle_client(
        &mut stream,
        &shared_store,
        &llm,
        &provider_profiles,
        &shared_engine,
    )
    .await;
    let payload = encode_runtime_response(&response);
    if let Err(error) = stream.write_all(&payload).await {
        tracing::warn!(
            error = %error,
            exit_code = response.exit_code,
            "failed to write daemon client response"
        );
    } else {
        tracing::debug!(
            exit_code = response.exit_code,
            stdout_bytes = response.stdout.len(),
            stderr_bytes = response.stderr.len(),
            "handled daemon client request"
        );
    }
}

fn start_message_queue_task<B, S>(
    shared_store: &S,
    shared_engine: &Arc<ConversationEngine<B, S>>,
) -> tokio::task::JoinHandle<Result<()>>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    let prompt_worker =
        PromptJobMessageQueueWorker::new(shared_store.clone(), shared_engine.as_ref().clone());
    let system_event_worker = SystemEventMessageQueueWorker::new(shared_store.clone());
    tokio::spawn(async move {
        tokio::try_join!(prompt_worker.run(), system_event_worker.run())?;
        Ok(())
    })
}

fn start_channel_task<B, S>(
    channel_configs: &ChannelConfigs,
    shared_store: &S,
    shared_engine: &Arc<ConversationEngine<B, S>>,
) -> Result<Option<tokio::task::JoinHandle<Result<()>>>>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    let Some(config) = channel_configs
        .telegram
        .as_ref()
        .filter(|config| config.enabled)
    else {
        return Ok(None);
    };

    let token = resolve_channel_secret("telegram", &config.token)?;
    tracing::info!(
        branch = %config.branch,
        poll_timeout_secs = config.poll_timeout_secs,
        allowed_chat_count = config.allowed_chat_ids.len(),
        "starting telegram channel"
    );
    let channel = TelegramChannel::from_config(coco_channel::telegram::TelegramChannelConfig {
        token,
        poll_timeout_secs: config.poll_timeout_secs,
        allowed_chat_ids: config.allowed_chat_ids.clone(),
    })
    .context(ChannelSnafu)?;
    let notify = Arc::new(Notify::new());
    let handler = TelegramMessageQueuePublisher::new(shared_store.clone(), notify.clone());
    let worker = TelegramMessageQueueWorker::new(
        config.branch.clone(),
        shared_store.clone(),
        shared_engine.as_ref().clone(),
        notify,
    );

    Ok(Some(tokio::spawn(async move {
        tokio::select! {
            channel_result = channel.run(&handler) => channel_result.context(ChannelSnafu),
            worker_result = worker.run() => worker_result,
        }
    })))
}

#[derive(Debug, Serialize, Deserialize)]
struct QueuedTelegramMessage {
    chat_id: String,
    sender_id: String,
    source_message_id: Option<String>,
    text: String,
    #[serde(default)]
    image_attachments: Vec<QueuedTelegramImageAttachment>,
}

#[derive(Debug, Serialize, Deserialize)]
struct QueuedTelegramImageAttachment {
    file_id: String,
    file_unique_id: Option<String>,
    width: Option<u32>,
    height: Option<u32>,
    file_size: Option<u64>,
}

#[derive(Debug, Snafu)]
enum TelegramQueuePayloadError {
    #[snafu(display("Failed to decode Telegram queue payload: {source}"))]
    Decode { source: serde_json::Error },
}

#[derive(Debug, Snafu)]
enum PromptJobQueuePayloadError {
    #[snafu(display("Failed to decode prompt job queue payload: {source}"))]
    DecodePromptJob { source: serde_json::Error },
}

#[derive(Debug, Snafu)]
enum SystemEventQueuePayloadError {
    #[snafu(display("Failed to decode system event queue payload: {source}"))]
    DecodeSystemEvent { source: serde_json::Error },

    #[snafu(display("Unsupported system event {event_type:?} version {version}"))]
    UnsupportedEvent { event_type: String, version: u64 },
}

#[derive(Debug, Deserialize)]
struct SystemEventEnvelope {
    #[serde(rename = "type")]
    event_type: String,
    version: u64,
    #[serde(default)]
    dedupe_key: Option<String>,
    data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct LlmBackendFailureRecoveryRequested {
    #[serde(default)]
    dedupe_key: String,
    job_id: String,
    root_branch: String,
    work_branch: String,
    failed_branch: String,
    base_node_id: String,
    execution_id: String,
    error_node_id: String,
    retry_from_node_id: String,
    message: String,
}

#[derive(Debug)]
enum SystemEvent {
    LlmBackendFailureRecoveryRequested(LlmBackendFailureRecoveryRequested),
}

struct SystemEventMessageQueueWorker<S> {
    store: S,
}

impl<S> SystemEventMessageQueueWorker<S> {
    fn new(store: S) -> Self {
        Self { store }
    }

    async fn run(self) -> Result<()>
    where
        S: Store + Clone + Send + Sync + 'static,
    {
        loop {
            if self.drain_once().await? == 0 {
                tokio::time::sleep(PROMPT_JOB_QUEUE_IDLE_DELAY).await;
            }
        }
    }

    async fn drain_once(&self) -> Result<usize>
    where
        S: Store + Clone + Send + Sync + 'static,
    {
        let mut handled = 0;
        while let Some(item) = self
            .store
            .peek_message(SYSTEM_EVENT_QUEUE)
            .context(StoreSnafu)?
        {
            match decode_system_event_message(item.payload.clone()) {
                Ok(event) => {
                    if !self.queue_prompt_job_for_event(&item, event)? {
                        return Ok(handled);
                    }
                    self.store
                        .dequeue_message(SYSTEM_EVENT_QUEUE)
                        .context(StoreSnafu)?;
                }
                Err(error) => {
                    self.store
                        .dequeue_message(SYSTEM_EVENT_QUEUE)
                        .context(StoreSnafu)?;
                    tracing::error!(
                        message_id = %item.message_id,
                        queue = SYSTEM_EVENT_QUEUE,
                        error = %error,
                        "discarded invalid system event queue message"
                    );
                }
            }
            handled += 1;
        }
        Ok(handled)
    }

    fn queue_prompt_job_for_event(
        &self,
        item: &MessageQueueItem,
        event: SystemEvent,
    ) -> Result<bool>
    where
        S: Store,
    {
        let route = route_system_event(&event);
        match self.store.get_branch_head(route.branch) {
            Ok(_) => {}
            Err(StoreError::BranchNotFound { .. }) => {
                tracing::warn!(
                    message_id = %item.message_id,
                    queue = SYSTEM_EVENT_QUEUE,
                    branch = route.branch,
                    "kept system event because target branch is missing"
                );
                return Ok(false);
            }
            Err(source) => return Err(source).context(StoreSnafu),
        }

        let request = materialize_system_event_prompt_job(route, &event);
        if self.prompt_job_request_exists(&request.job_id)? {
            tracing::debug!(
                message_id = %item.message_id,
                queue = SYSTEM_EVENT_QUEUE,
                branch = route.branch,
                job_id = %request.job_id,
                "skipped duplicate system event prompt job"
            );
            return Ok(true);
        }

        queue_prompt_job_request(&self.store, request)?;
        tracing::info!(
            message_id = %item.message_id,
            queue = SYSTEM_EVENT_QUEUE,
            branch = route.branch,
            "queued system event prompt job"
        );
        Ok(true)
    }

    fn prompt_job_request_exists(&self, job_id: &str) -> Result<bool>
    where
        S: Store,
    {
        match self.store.get_job(job_id) {
            Ok(_) => return Ok(true),
            Err(StoreError::PromptJobNotFound { .. }) => {}
            Err(source) => return Err(source).context(StoreSnafu),
        }

        Ok(self
            .store
            .list_queue_messages(PROMPT_JOB_QUEUE)
            .context(StoreSnafu)?
            .into_iter()
            .filter_map(|item| decode_prompt_job_message(item.payload).ok())
            .any(|request| request.job_id == job_id))
    }
}

struct PromptJobMessageQueueWorker<B, S> {
    store: S,
    engine: ConversationEngine<B, S>,
    active_job_joins: Mutex<HashMap<String, Instant>>,
}

impl<B, S> PromptJobMessageQueueWorker<B, S> {
    fn new(store: S, engine: ConversationEngine<B, S>) -> Self {
        Self {
            store,
            engine,
            active_job_joins: Mutex::new(HashMap::new()),
        }
    }

    async fn run(self) -> Result<()>
    where
        B: CompletionBackend + 'static,
        S: Store + Clone + Send + Sync + 'static,
    {
        loop {
            self.drain_once().await?;
        }
    }

    async fn drain_once(&self) -> Result<()>
    where
        B: CompletionBackend + 'static,
        S: Store + Clone + Send + Sync + 'static,
    {
        while let Some(item) = self.peek_prompt_queue_head()? {
            match decode_prompt_job_message(item.payload.clone()) {
                Ok(request) => {
                    match self
                        .handle_prompt_request_queue_head(&item, &request)
                        .await?
                    {
                        PromptQueueHeadResult::Continue => {}
                        PromptQueueHeadResult::Wait(duration) => {
                            tokio::time::sleep(duration).await;
                            return Ok(());
                        }
                    }
                }
                Err(_) => {
                    let Some(item) = self
                        .store
                        .dequeue_message(PROMPT_JOB_QUEUE)
                        .context(StoreSnafu)?
                    else {
                        continue;
                    };
                    self.handle_item(item).await;
                }
            }
        }
        tokio::time::sleep(PROMPT_JOB_QUEUE_IDLE_DELAY).await;
        Ok(())
    }

    fn peek_prompt_queue_head(&self) -> Result<Option<MessageQueueItem>>
    where
        S: Store,
    {
        self.store
            .peek_message(PROMPT_JOB_QUEUE)
            .context(StoreSnafu)
    }

    async fn handle_prompt_request_queue_head(
        &self,
        item: &MessageQueueItem,
        request: &QueuedPromptRequest,
    ) -> Result<PromptQueueHeadResult>
    where
        B: CompletionBackend + 'static,
        S: Store + Clone + Send + Sync + 'static,
    {
        match self.store.get_branch_head(&request.branch) {
            Ok(_) => {}
            Err(coco_mem::StoreError::BranchNotFound { .. }) => {
                if self
                    .store
                    .dequeue_message(PROMPT_JOB_QUEUE)
                    .context(StoreSnafu)?
                    .is_some()
                {
                    tracing::warn!(
                        message_id = %item.message_id,
                        queue = PROMPT_JOB_QUEUE,
                        job_id = %request.job_id,
                        branch = %request.branch,
                        "discarded queued prompt job request for missing branch"
                    );
                }
                return Ok(PromptQueueHeadResult::Continue);
            }
            Err(error) => return Err(error).context(StoreSnafu),
        }

        match self.store.get_job(&request.job_id) {
            Ok(_) => {
                if self
                    .store
                    .dequeue_message(PROMPT_JOB_QUEUE)
                    .context(StoreSnafu)?
                    .is_some()
                {
                    tracing::warn!(
                        message_id = %item.message_id,
                        queue = PROMPT_JOB_QUEUE,
                        job_id = %request.job_id,
                        branch = %request.branch,
                        "discarded duplicate queued prompt job request"
                    );
                }
                return Ok(PromptQueueHeadResult::Continue);
            }
            Err(coco_mem::StoreError::PromptJobNotFound { .. }) => {}
            Err(error) => return Err(error).context(StoreSnafu),
        }

        if let Some(active_job) = self
            .engine
            .active_branch_prompt_job(&request.branch)
            .context(CoreEngineSnafu)?
        {
            return self
                .wait_for_active_branch_job(item, request, active_job)
                .await;
        }

        let guard = self.engine.lock_branch(&request.branch).await;
        let Some(item) = self.peek_prompt_queue_head()? else {
            return Ok(PromptQueueHeadResult::Continue);
        };
        let request = match decode_prompt_job_message(item.payload.clone()) {
            Ok(request) => request,
            Err(_) => {
                let Some(item) = self
                    .store
                    .dequeue_message(PROMPT_JOB_QUEUE)
                    .context(StoreSnafu)?
                else {
                    return Ok(PromptQueueHeadResult::Continue);
                };
                self.handle_item(item).await;
                return Ok(PromptQueueHeadResult::Continue);
            }
        };

        match self.store.get_branch_head(&request.branch) {
            Ok(_) => {}
            Err(coco_mem::StoreError::BranchNotFound { .. }) => {
                if self
                    .store
                    .dequeue_message(PROMPT_JOB_QUEUE)
                    .context(StoreSnafu)?
                    .is_some()
                {
                    tracing::warn!(
                        message_id = %item.message_id,
                        queue = PROMPT_JOB_QUEUE,
                        job_id = %request.job_id,
                        branch = %request.branch,
                        "discarded queued prompt job request for missing branch"
                    );
                }
                return Ok(PromptQueueHeadResult::Continue);
            }
            Err(error) => return Err(error).context(StoreSnafu),
        }

        match self.store.get_job(&request.job_id) {
            Ok(_) => {
                if self
                    .store
                    .dequeue_message(PROMPT_JOB_QUEUE)
                    .context(StoreSnafu)?
                    .is_some()
                {
                    tracing::warn!(
                        message_id = %item.message_id,
                        queue = PROMPT_JOB_QUEUE,
                        job_id = %request.job_id,
                        branch = %request.branch,
                        "discarded duplicate queued prompt job request"
                    );
                }
                return Ok(PromptQueueHeadResult::Continue);
            }
            Err(coco_mem::StoreError::PromptJobNotFound { .. }) => {}
            Err(error) => return Err(error).context(StoreSnafu),
        }

        if let Some(active_job) = self
            .engine
            .active_branch_prompt_job(&request.branch)
            .context(CoreEngineSnafu)?
        {
            drop(guard);
            return self
                .wait_for_active_branch_job(&item, &request, active_job)
                .await;
        }
        let Some(item) = self
            .store
            .dequeue_message(PROMPT_JOB_QUEUE)
            .context(StoreSnafu)?
        else {
            return Ok(PromptQueueHeadResult::Continue);
        };
        self.handle_item(item).await;
        Ok(PromptQueueHeadResult::Continue)
    }

    async fn wait_for_active_branch_job(
        &self,
        item: &MessageQueueItem,
        request: &QueuedPromptRequest,
        active_job: coco_mem::Job,
    ) -> Result<PromptQueueHeadResult>
    where
        B: CompletionBackend + 'static,
        S: Store + Clone + Send + Sync + 'static,
    {
        if active_job_is_waiting_for_recovery(&self.store, &active_job).context(StoreSnafu)? {
            tracing::debug!(
                message_id = %item.message_id,
                queue = PROMPT_JOB_QUEUE,
                branch = %request.branch,
                job_id = %request.job_id,
                active_job_id = %active_job.job_id,
                active_job_status = ?active_job.status,
                wait_ms = ACTIVE_JOB_JOIN_INTERVAL.as_millis(),
                "active branch prompt job is waiting for recovery; queued request will wait"
            );
            return Ok(PromptQueueHeadResult::Wait(ACTIVE_JOB_JOIN_INTERVAL));
        }

        if let ActiveJobJoinDecision::Backoff(duration) =
            self.active_job_join_decision(&active_job).await
        {
            tracing::debug!(
                message_id = %item.message_id,
                queue = PROMPT_JOB_QUEUE,
                branch = %request.branch,
                job_id = %request.job_id,
                active_job_id = %active_job.job_id,
                active_job_status = ?active_job.status,
                wait_ms = duration.as_millis(),
                "active branch prompt job was joined recently; queued request will wait"
            );
            return Ok(PromptQueueHeadResult::Wait(duration));
        }

        tracing::info!(
            message_id = %item.message_id,
            queue = PROMPT_JOB_QUEUE,
            branch = %request.branch,
            job_id = %request.job_id,
            active_job_id = %active_job.job_id,
            active_job_status = ?active_job.status,
            "waiting for active branch prompt job before handling queued request"
        );

        Ok(match self.engine.join_job(&active_job.job_id).await {
            Ok(snapshot) if matches!(snapshot.status, JobStatus::Finished) => {
                PromptQueueHeadResult::Continue
            }
            Ok(snapshot) => {
                tracing::info!(
                    message_id = %item.message_id,
                    queue = PROMPT_JOB_QUEUE,
                    branch = %request.branch,
                    job_id = %request.job_id,
                    active_job_id = %active_job.job_id,
                    active_job_status = ?snapshot.status,
                    "active branch prompt job still blocks queued request"
                );
                PromptQueueHeadResult::Wait(ACTIVE_JOB_JOIN_INTERVAL)
            }
            Err(error) => {
                tracing::warn!(
                    message_id = %item.message_id,
                    queue = PROMPT_JOB_QUEUE,
                    branch = %request.branch,
                    job_id = %request.job_id,
                    active_job_id = %active_job.job_id,
                    error = %error,
                    "failed to wait for active branch prompt job before materializing queued request"
                );
                PromptQueueHeadResult::Wait(ACTIVE_JOB_JOIN_INTERVAL)
            }
        })
    }

    async fn active_job_join_decision(&self, active_job: &coco_mem::Job) -> ActiveJobJoinDecision {
        let mut active_job_joins = self.active_job_joins.lock().await;
        active_job_join_decision_with_backoff(&mut active_job_joins, active_job)
    }

    async fn handle_item(&self, item: MessageQueueItem)
    where
        B: CompletionBackend + 'static,
        S: Store + Clone + Send + Sync + 'static,
    {
        let job = match decode_prompt_job_message(item.payload) {
            Ok(job) => job,
            Err(error) => {
                tracing::error!(
                    message_id = %item.message_id,
                    queue = PROMPT_JOB_QUEUE,
                    error = %error,
                    "discarded invalid prompt job queue message"
                );
                return;
            }
        };

        tracing::info!(
            message_id = %item.message_id,
            queue = PROMPT_JOB_QUEUE,
            job_id = %job.job_id,
            branch = %job.branch,
            "handling queued prompt job request"
        );
        self.handle_prompt_request(&item.message_id, job).await;
    }

    async fn handle_prompt_request(&self, message_id: &str, request: QueuedPromptRequest)
    where
        B: CompletionBackend + 'static,
        S: Store + Clone + Send + Sync + 'static,
    {
        if let Err(error) = self.try_handle_prompt_request(&request).await {
            if is_missing_branch_error(&error) {
                tracing::warn!(
                    message_id = %message_id,
                    queue = PROMPT_JOB_QUEUE,
                    job_id = %request.job_id,
                    branch = %request.branch,
                    error = %error,
                    "discarded queued prompt job request for missing branch"
                );
                return;
            }

            if is_prompt_job_already_exists_error(&error) {
                tracing::warn!(
                    message_id = %message_id,
                    queue = PROMPT_JOB_QUEUE,
                    job_id = %request.job_id,
                    branch = %request.branch,
                    error = %error,
                    "discarded duplicate queued prompt job request"
                );
                return;
            }

            tracing::warn!(
                message_id = %message_id,
                queue = PROMPT_JOB_QUEUE,
                job_id = %request.job_id,
                branch = %request.branch,
                error = %error,
                "discarded queued prompt job request after submission failure"
            );
        }
    }

    async fn try_handle_prompt_request(&self, request: &QueuedPromptRequest) -> Result<()>
    where
        B: CompletionBackend + 'static,
        S: Store + Clone + Send + Sync + 'static,
    {
        let job = self
            .engine
            .submit_job_with_id_and_session_patch(
                &request.job_id,
                &request.branch,
                &request.prompt,
                request.merge_parents.clone(),
                request.session_patch.clone(),
            )
            .await
            .context(CoreEngineSnafu)?;
        spawn_queued_prompt_job_driver(self.engine.clone(), job.job_id);
        Ok(())
    }
}

struct TelegramMessageQueuePublisher<S> {
    store: S,
    notify: Arc<Notify>,
}

impl<S> TelegramMessageQueuePublisher<S> {
    fn new(store: S, notify: Arc<Notify>) -> Self {
        Self { store, notify }
    }
}

#[async_trait]
impl<S> MessageHandler for TelegramMessageQueuePublisher<S>
where
    S: Store + Clone + Send + Sync + 'static,
{
    async fn handle(&self, message: InboundMessage) -> coco_channel::Result<OutboundMessage> {
        let item = self
            .store
            .enqueue_message(TELEGRAM_INBOUND_QUEUE, encode_telegram_message(&message))
            .map_err(ChannelError::handler)?;
        let conversation_id = message.conversation_id().to_owned();
        let sender_id = message.sender_id().to_owned();
        tracing::info!(
            message_id = %item.message_id,
            queue = TELEGRAM_INBOUND_QUEUE,
            conversation_id = %conversation_id,
            sender_id = %sender_id,
            "queued telegram inbound message"
        );
        self.notify.notify_one();
        Ok(OutboundMessage {
            text: "queued telegram inbound message".to_owned(),
        })
    }
}

struct TelegramMessageQueueWorker<B, S> {
    branch: String,
    store: S,
    engine: ConversationEngine<B, S>,
    service: CoreService<FixedBranchResolver, B, S>,
    notify: Arc<Notify>,
}

impl<B, S> TelegramMessageQueueWorker<B, S> {
    fn new(
        branch: impl Into<String>,
        store: S,
        engine: ConversationEngine<B, S>,
        notify: Arc<Notify>,
    ) -> Self {
        let branch = branch.into();
        let service = CoreService::new(FixedBranchResolver::new(branch.clone()), engine.clone());
        Self {
            branch,
            store,
            engine,
            service,
            notify,
        }
    }

    async fn run(self) -> Result<()>
    where
        B: CompletionBackend + 'static,
        S: Store + Clone + Send + Sync + 'static,
    {
        loop {
            if self.drain_once().await? == 0 {
                tokio::select! {
                    () = self.notify.notified() => {}
                    () = tokio::time::sleep(TELEGRAM_QUEUE_IDLE_DELAY) => {}
                }
            }
        }
    }

    async fn drain_once(&self) -> Result<usize>
    where
        B: CompletionBackend + 'static,
        S: Store + Clone + Send + Sync + 'static,
    {
        let mut handled = 0;
        while let Some(item) = self
            .store
            .dequeue_message(TELEGRAM_INBOUND_QUEUE)
            .context(StoreSnafu)?
        {
            handled += 1;
            self.handle_item(item).await;
        }
        Ok(handled)
    }

    async fn handle_item(&self, item: MessageQueueItem)
    where
        B: CompletionBackend + 'static,
        S: Store + Clone + Send + Sync + 'static,
    {
        let message = match decode_telegram_message(item.payload) {
            Ok(message) => message,
            Err(error) => {
                tracing::error!(
                    message_id = %item.message_id,
                    queue = TELEGRAM_INBOUND_QUEUE,
                    error = %error,
                    "discarded invalid telegram queue message"
                );
                return;
            }
        };

        let conversation_id = message.conversation_id().to_owned();
        let sender_id = message.sender_id().to_owned();
        let source_message_id = message.source_message_id().map(str::to_owned);
        tracing::info!(
            message_id = %item.message_id,
            queue = TELEGRAM_INBOUND_QUEUE,
            branch = %self.branch,
            conversation_id = %conversation_id,
            sender_id = %sender_id,
            source_message_id = ?source_message_id,
            "handling queued telegram inbound message"
        );

        if let Err(error) = wait_for_branch_to_accept_channel_prompt(
            &self.store,
            &self.engine,
            &self.branch,
            &item.message_id,
        )
        .await
        {
            tracing::error!(
                message_id = %item.message_id,
                queue = TELEGRAM_INBOUND_QUEUE,
                branch = %self.branch,
                error = %error,
                "queued telegram inbound message failed while waiting for branch"
            );
            return;
        }

        match self.service.handle_message(message).await {
            Ok(response) => {
                tracing::debug!(
                    message_id = %item.message_id,
                    queue = TELEGRAM_INBOUND_QUEUE,
                    branch = %self.branch,
                    conversation_id = %conversation_id,
                    sender_id = %sender_id,
                    source_message_id = ?source_message_id,
                    response_bytes = response.text.len(),
                    "handled queued telegram inbound message"
                );
            }
            Err(error) => {
                tracing::error!(
                    message_id = %item.message_id,
                    queue = TELEGRAM_INBOUND_QUEUE,
                    branch = %self.branch,
                    error = %error,
                    "queued telegram inbound message failed"
                );
            }
        }
    }
}

async fn wait_for_branch_to_accept_channel_prompt<B, S>(
    store: &S,
    engine: &ConversationEngine<B, S>,
    branch: &str,
    message_id: &str,
) -> Result<()>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    let mut active_job_joins = HashMap::new();
    loop {
        let active_job = engine
            .active_branch_prompt_job(branch)
            .context(CoreEngineSnafu)?;
        let Some(active_job) = active_job else {
            return Ok(());
        };

        if active_job_is_waiting_for_recovery(store, &active_job).context(StoreSnafu)? {
            tracing::debug!(
                message_id = %message_id,
                queue = TELEGRAM_INBOUND_QUEUE,
                branch = %branch,
                active_job_id = %active_job.job_id,
                active_job_status = ?active_job.status,
                wait_ms = ACTIVE_JOB_JOIN_INTERVAL.as_millis(),
                "active branch prompt job is waiting for recovery; queued request will wait"
            );
            tokio::time::sleep(ACTIVE_JOB_JOIN_INTERVAL).await;
            continue;
        }

        if let ActiveJobJoinDecision::Backoff(duration) =
            active_job_join_decision_with_backoff(&mut active_job_joins, &active_job)
        {
            tracing::debug!(
                message_id = %message_id,
                queue = TELEGRAM_INBOUND_QUEUE,
                branch = %branch,
                active_job_id = %active_job.job_id,
                active_job_status = ?active_job.status,
                wait_ms = duration.as_millis(),
                "active branch prompt job was joined recently; queued request will wait"
            );
            tokio::time::sleep(duration).await;
            continue;
        }

        tracing::info!(
            message_id = %message_id,
            queue = TELEGRAM_INBOUND_QUEUE,
            branch = %branch,
            active_job_id = %active_job.job_id,
            active_job_status = ?active_job.status,
            "waiting for active branch prompt job before handling queued request"
        );

        let snapshot = engine
            .join_job(&active_job.job_id)
            .await
            .context(CoreEngineSnafu)?;
        if !matches!(snapshot.status, JobStatus::Finished) {
            tokio::time::sleep(ACTIVE_JOB_JOIN_INTERVAL).await;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromptQueueHeadResult {
    Continue,
    Wait(Duration),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveJobJoinDecision {
    Join,
    Backoff(Duration),
}

fn active_job_join_decision_with_backoff(
    active_job_joins: &mut HashMap<String, Instant>,
    active_job: &coco_mem::Job,
) -> ActiveJobJoinDecision {
    let now = Instant::now();
    active_job_joins
        .retain(|_, last_join| now.duration_since(*last_join) < ACTIVE_JOB_JOIN_INTERVAL);

    if let Some(last_join) = active_job_joins.get(&active_job.job_id) {
        let elapsed = now.duration_since(*last_join);
        if elapsed < ACTIVE_JOB_JOIN_INTERVAL {
            return ActiveJobJoinDecision::Backoff(ACTIVE_JOB_JOIN_INTERVAL - elapsed);
        }
    }

    active_job_joins.insert(active_job.job_id.clone(), now);
    ActiveJobJoinDecision::Join
}

fn active_job_is_waiting_for_recovery(
    store: &impl Store,
    active_job: &coco_mem::Job,
) -> std::result::Result<bool, StoreError> {
    let path = match store.log(&active_job.base, &active_job.work_branch) {
        Ok(path) => path,
        Err(StoreError::RefsNotConnected { .. }) => return Ok(false),
        Err(error) => return Err(error),
    };
    let mut ordered = path.into_iter().rev();
    let Some(prompt_anchor) = ordered.next() else {
        return Ok(false);
    };
    let mut last_node = prompt_anchor;
    for node in ordered {
        last_node = node;
    }

    if matches!(last_node.kind, Kind::Failure(_)) {
        return Ok(true);
    }

    Ok(store
        .list_children(&last_node.id)?
        .into_iter()
        .any(|child| matches!(child.kind, Kind::Failure(_))))
}

fn is_missing_branch_error(error: &crate::error::Error) -> bool {
    matches!(
        error,
        crate::error::Error::CoreEngine {
            source: EngineError::SessionMissing { .. },
        }
    )
}

fn is_prompt_job_already_exists_error(error: &crate::error::Error) -> bool {
    matches!(
        error,
        crate::error::Error::CoreEngine {
            source: EngineError::EngineFailed { message },
        }
            if message.contains("already exists")
    )
}

fn encode_telegram_message(message: &InboundMessage) -> serde_json::Value {
    let InboundMessage::Telegram(message) = message else {
        unreachable!("telegram queue only accepts telegram inbound messages");
    };

    json!({
        "chat_id": message.chat_id(),
        "sender_id": message.sender_id(),
        "source_message_id": message.source_message_id(),
        "text": message.text(),
        "image_attachments": message
            .image_attachments()
            .iter()
            .map(|image| {
                json!({
                    "file_id": image.file_id(),
                    "file_unique_id": image.file_unique_id(),
                    "width": image.width(),
                    "height": image.height(),
                    "file_size": image.file_size(),
                })
            })
            .collect::<Vec<_>>(),
    })
}

fn decode_telegram_message(
    payload: serde_json::Value,
) -> std::result::Result<InboundMessage, TelegramQueuePayloadError> {
    let message = serde_json::from_value::<QueuedTelegramMessage>(payload).context(DecodeSnafu)?;
    let image_attachments = message
        .image_attachments
        .into_iter()
        .map(|image| {
            TelegramImageAttachment::from_parts(
                image.file_id,
                image.file_unique_id,
                image.width,
                image.height,
                image.file_size,
            )
        })
        .collect::<Vec<_>>();
    Ok(match message.source_message_id {
        Some(source_message_id) if !image_attachments.is_empty() => {
            InboundMessage::telegram_with_message_id_and_images(
                message.chat_id,
                message.sender_id,
                source_message_id,
                message.text,
                image_attachments,
            )
        }
        Some(source_message_id) => InboundMessage::telegram_with_message_id(
            message.chat_id,
            message.sender_id,
            source_message_id,
            message.text,
        ),
        None if !image_attachments.is_empty() => InboundMessage::telegram_with_images(
            message.chat_id,
            message.sender_id,
            message.text,
            image_attachments,
        ),
        None => InboundMessage::telegram(message.chat_id, message.sender_id, message.text),
    })
}

fn decode_prompt_job_message(
    payload: serde_json::Value,
) -> std::result::Result<QueuedPromptRequest, PromptJobQueuePayloadError> {
    serde_json::from_value(payload).context(DecodePromptJobSnafu)
}

fn decode_system_event_message(
    payload: serde_json::Value,
) -> std::result::Result<SystemEvent, SystemEventQueuePayloadError> {
    let envelope =
        serde_json::from_value::<SystemEventEnvelope>(payload).context(DecodeSystemEventSnafu)?;
    match (envelope.event_type.as_str(), envelope.version) {
        ("llm.backend_failure.recovery_requested", 1) => {
            let mut event =
                serde_json::from_value::<LlmBackendFailureRecoveryRequested>(envelope.data)
                    .context(DecodeSystemEventSnafu)?;
            event.dedupe_key = envelope.dedupe_key.unwrap_or_else(|| {
                backend_failure_recovery_dedupe_key(
                    &event.job_id,
                    &event.work_branch,
                    &event.retry_from_node_id,
                )
            });
            Ok(SystemEvent::LlmBackendFailureRecoveryRequested(event))
        }
        _ => UnsupportedEventSnafu {
            event_type: envelope.event_type,
            version: envelope.version,
        }
        .fail(),
    }
}

fn render_system_event_prompt(event: &SystemEvent) -> String {
    match event {
        SystemEvent::LlmBackendFailureRecoveryRequested(event) => format!(
            "Handle this LLM backend failure recovery event.\n\n\
             Run `coco skill run recovery --handoff ...` from the `day` branch and pass the event fields below as the handoff. \
             Treat work branch {work_branch:?} as the failed target branch for job {job_id:?}, not as the branch executing recovery. \
             Do not fork or create another recovery branch; recover the original task through `day` and produce a normal successful answer.\n\n\
             Event fields:\n\
             - job_id: {job_id}\n\
             - root_branch: {root_branch}\n\
             - work_branch: {work_branch}\n\
             - failed_branch: {failed_branch}\n\
             - base_node_id: {base_node_id}\n\
             - execution_id: {execution_id}\n\
             - error_node_id: {error_node_id}\n\
             - retry_from_node_id: {retry_from_node_id}\n\
             - message: {message}",
            job_id = event.job_id,
            root_branch = event.root_branch,
            work_branch = event.work_branch,
            failed_branch = event.failed_branch,
            base_node_id = event.base_node_id,
            execution_id = event.execution_id,
            error_node_id = event.error_node_id,
            retry_from_node_id = event.retry_from_node_id,
            message = event.message,
        ),
    }
}

#[derive(Debug, Clone, Copy)]
enum SystemEventHandler {
    Day,
}

#[derive(Debug, Clone, Copy)]
struct SystemEventRoute {
    branch: &'static str,
    handler: SystemEventHandler,
}

fn route_system_event(event: &SystemEvent) -> SystemEventRoute {
    match event {
        SystemEvent::LlmBackendFailureRecoveryRequested(_) => SystemEventRoute {
            branch: BUILTIN_DAY_BRANCH,
            handler: SystemEventHandler::Day,
        },
    }
}

fn materialize_system_event_prompt_job(
    route: SystemEventRoute,
    event: &SystemEvent,
) -> QueuedPromptRequest {
    match route.handler {
        SystemEventHandler::Day => QueuedPromptRequest {
            job_id: stable_system_event_prompt_job_id(event),
            branch: route.branch.to_owned(),
            prompt: render_system_event_prompt(event),
            merge_parents: vec![],
            session_patch: None,
        },
    }
}

fn stable_system_event_prompt_job_id(event: &SystemEvent) -> String {
    match event {
        SystemEvent::LlmBackendFailureRecoveryRequested(event) => {
            format!("job-{}", hex_encode(event.dedupe_key.as_bytes()))
        }
    }
}

fn backend_failure_recovery_dedupe_key(
    job_id: &str,
    work_branch: &str,
    retry_from_node_id: &str,
) -> String {
    format!("llm.backend_failure:{job_id}:{work_branch}:{retry_from_node_id}")
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn spawn_queued_prompt_job_driver<B, S>(engine: ConversationEngine<B, S>, job_id: String)
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    tokio::spawn(async move {
        if let Err(error) = engine.drive_job(&job_id).await {
            tracing::error!(
                job_id = %job_id,
                error = %error,
                "failed to drive queued prompt job"
            );
        }
    });
}

impl<B, S> CocoCliDaemonServerHandle<B, S> {
    pub(crate) async fn wait(self) -> Result<()> {
        let Self {
            socket_path,
            llm,
            tasks,
        } = self;

        let result = tasks.run_until_exit().await;
        llm.cleanup_runtime_processes().await;
        cleanup_socket(&socket_path);
        result
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn shutdown(self) -> Result<()> {
        let Self {
            socket_path,
            llm,
            tasks,
        } = self;

        let result = tasks.shutdown_all().await;
        llm.cleanup_runtime_processes().await;
        cleanup_socket(&socket_path);
        result
    }
}

struct DaemonTaskGroup {
    socket_task: Option<tokio::task::JoinHandle<Result<()>>>,
    console: Option<ConsoleServerHandle>,
    channel_task: Option<tokio::task::JoinHandle<Result<()>>>,
    message_queue_task: Option<tokio::task::JoinHandle<Result<()>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DaemonTaskKind {
    Socket,
    Console,
    Channel,
    MessageQueue,
}

struct DaemonTaskExit {
    kind: DaemonTaskKind,
    result: Result<()>,
}

impl DaemonTaskGroup {
    async fn run_until_exit(mut self) -> Result<()> {
        let exit = self.wait_for_exit().await;
        tracing::warn!(
            task = ?exit.kind,
            "daemon task exited; shutting down remaining tasks"
        );
        let shutdown_result = self.shutdown_all().await;
        merge_daemon_exit_results(exit.result, shutdown_result)
    }

    async fn wait_for_exit(&mut self) -> DaemonTaskExit {
        let Self {
            socket_task,
            console,
            channel_task,
            message_queue_task,
        } = self;

        tokio::select! {
            socket_result = async { socket_task.as_mut().expect("socket task should exist").await }, if socket_task.is_some() => {
                *socket_task = None;
                DaemonTaskExit::socket(socket_result)
            }
            console_result = async { console.as_mut().expect("console task should exist").wait_mut().await }, if console.is_some() => {
                *console = None;
                DaemonTaskExit::console(console_result)
            }
            channel_result = async { channel_task.as_mut().expect("channel task should exist").await }, if channel_task.is_some() => {
                *channel_task = None;
                DaemonTaskExit::channel(channel_result)
            }
            message_queue_result = async { message_queue_task.as_mut().expect("message queue task should exist").await }, if message_queue_task.is_some() => {
                *message_queue_task = None;
                DaemonTaskExit::message_queue(message_queue_result)
            }
            else => unreachable!("daemon task group should contain at least one task"),
        }
    }

    async fn shutdown_all(mut self) -> Result<()> {
        self.shutdown_remaining().await
    }

    async fn shutdown_remaining(&mut self) -> Result<()> {
        let mut first_error = None;
        if let Some(socket_task) = self.socket_task.take() {
            record_daemon_shutdown_result(
                &mut first_error,
                DaemonTaskKind::Socket,
                abort_socket_task(socket_task).await,
            );
        }
        if let Some(console) = self.console.take() {
            record_daemon_shutdown_result(
                &mut first_error,
                DaemonTaskKind::Console,
                console.shutdown().await.context(ConsoleSnafu),
            );
        }
        if let Some(channel_task) = self.channel_task.take() {
            record_daemon_shutdown_result(
                &mut first_error,
                DaemonTaskKind::Channel,
                abort_channel_task(channel_task).await,
            );
        }
        if let Some(message_queue_task) = self.message_queue_task.take() {
            record_daemon_shutdown_result(
                &mut first_error,
                DaemonTaskKind::MessageQueue,
                abort_message_queue_task(message_queue_task).await,
            );
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl DaemonTaskExit {
    fn socket(result: std::result::Result<Result<()>, tokio::task::JoinError>) -> Self {
        Self {
            kind: DaemonTaskKind::Socket,
            result: joined_socket_task_result(result),
        }
    }

    fn console(result: coco_console::Result<()>) -> Self {
        Self {
            kind: DaemonTaskKind::Console,
            result: result.context(ConsoleSnafu),
        }
    }

    fn channel(result: std::result::Result<Result<()>, tokio::task::JoinError>) -> Self {
        Self {
            kind: DaemonTaskKind::Channel,
            result: joined_channel_task_result(result),
        }
    }

    fn message_queue(result: std::result::Result<Result<()>, tokio::task::JoinError>) -> Self {
        Self {
            kind: DaemonTaskKind::MessageQueue,
            result: joined_message_queue_task_result(result),
        }
    }
}

fn joined_socket_task_result(
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    result.context(JoinDaemonServerSnafu)?
}

fn joined_channel_task_result(
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    result.context(JoinChannelTaskSnafu)?
}

fn joined_message_queue_task_result(
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    result.context(JoinMessageQueueTaskSnafu)?
}

fn merge_daemon_exit_results(exit_result: Result<()>, shutdown_result: Result<()>) -> Result<()> {
    match (exit_result, shutdown_result) {
        (Err(error), Err(shutdown_error)) => {
            tracing::error!(
                error = %shutdown_error,
                "failed to shut down remaining daemon tasks"
            );
            Err(error)
        }
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

fn record_daemon_shutdown_result(
    first_error: &mut Option<crate::Error>,
    task: DaemonTaskKind,
    result: Result<()>,
) {
    if let Err(error) = result {
        if first_error.is_none() {
            *first_error = Some(error);
        } else {
            tracing::error!(
                task = ?task,
                error = %error,
                "additional daemon task shutdown failed"
            );
        }
    }
}

async fn abort_socket_task(socket_task: tokio::task::JoinHandle<Result<()>>) -> Result<()> {
    socket_task.abort();
    match socket_task.await {
        Ok(result) => result,
        Err(source) if source.is_cancelled() => Ok(()),
        Err(source) => Err(source).context(JoinDaemonServerSnafu),
    }
}

async fn abort_channel_task(channel_task: tokio::task::JoinHandle<Result<()>>) -> Result<()> {
    channel_task.abort();
    match channel_task.await {
        Ok(result) => result,
        Err(source) if source.is_cancelled() => Ok(()),
        Err(source) => Err(source).context(JoinChannelTaskSnafu),
    }
}

async fn abort_message_queue_task(
    message_queue_task: tokio::task::JoinHandle<Result<()>>,
) -> Result<()> {
    message_queue_task.abort();
    match message_queue_task.await {
        Ok(result) => result,
        Err(source) if source.is_cancelled() => Ok(()),
        Err(source) => Err(source).context(JoinMessageQueueTaskSnafu),
    }
}

async fn handle_client<B, S>(
    stream: &mut tokio::net::UnixStream,
    shared_store: &S,
    llm: &Arc<LlmService<B, S>>,
    provider_profiles: &ProviderProfiles,
    shared_engine: &Arc<ConversationEngine<B, S>>,
) -> CocoCliRuntimeResponse
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    let mut input = Vec::new();
    match stream.read_to_end(&mut input).await {
        Ok(_) => match serde_json::from_slice::<CocoCliRuntimeRequest>(&input) {
            Ok(request) => {
                tracing::debug!(
                    arg_count = request.args.len(),
                    stdin_bytes = request.stdin.len(),
                    session_role = ?request.session_role,
                    has_branch_env = request.branch_env.is_some(),
                    "received daemon client request"
                );
                run_forwarded_with_services(
                    ForwardedRuntimeInputs {
                        args: &request.args,
                        stdin: &request.stdin,
                        branch_env: request.branch_env.as_deref(),
                        session_role: request.session_role.or(Some(SessionRole::Orchestrator)),
                        store_path_env: request.store_path_env.as_deref(),
                        parent_tool_use_id_env: request.parent_tool_use_id_env.as_deref(),
                    },
                    RuntimeServices {
                        shared_store,
                        llm,
                        provider_profiles,
                        shared_engine: Some(shared_engine),
                    },
                )
                .await
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    request_bytes = input.len(),
                    "invalid daemon client request"
                );
                runtime_error_response(2, format!("invalid coco-cli daemon request: {error}"))
            }
        },
        Err(error) => {
            tracing::warn!(error = %error, "failed to read daemon client request");
            runtime_error_response(
                2,
                format!("failed to read coco-cli daemon request: {error}"),
            )
        }
    }
}

fn runtime_error_response(exit_code: i32, stderr: impl Into<String>) -> CocoCliRuntimeResponse {
    CocoCliRuntimeResponse {
        exit_code,
        stdout: String::new(),
        stderr: stderr.into(),
    }
}

fn encode_runtime_response(response: &CocoCliRuntimeResponse) -> Vec<u8> {
    match serde_json::to_vec(response) {
        Ok(payload) => payload,
        Err(error) => serde_json::to_vec(&runtime_error_response(
            2,
            format!("failed to serialize coco-cli daemon response: {error}"),
        ))
        .unwrap_or_else(|_| {
            br#"{"exit_code":2,"stdout":"","stderr":"failed to serialize coco-cli daemon response"}"#
                .to_vec()
        }),
    }
}

fn cleanup_socket(path: &Path) {
    let _ = std::fs::remove_file(path);
}

fn canonicalize_existing_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use coco_channel::{InboundMessage, MessageHandler, TelegramImageAttachment};
    use coco_core::ConversationEngine;
    use coco_llm::{
        BackendError, BackendTurn, CompletionBackend, CompletionMessage, LlmService, Provider,
        SessionConfig, StepContext,
    };
    use coco_mem::{
        BackendMetadata, BranchStore, Job, JobStatus, JobStore, Kind, MemoryStore,
        MessageQueueStore, NewNode, NodeStore, Role, SessionAnchorPatch, SessionRole, SessionStore,
    };
    use serde_json::json;
    use tokio::sync::Notify;

    use crate::app::prompt::QueuedPromptRequest;

    use super::{
        DaemonTaskGroup, LlmBackendFailureRecoveryRequested, PROMPT_JOB_QUEUE,
        PromptJobMessageQueueWorker, SYSTEM_EVENT_QUEUE, SystemEvent,
        SystemEventMessageQueueWorker, TELEGRAM_INBOUND_QUEUE, TelegramMessageQueuePublisher,
        TelegramMessageQueueWorker, abort_channel_task, decode_telegram_message,
        encode_telegram_message, resolve_daemon_socket_path,
    };

    #[test]
    fn resolve_daemon_socket_path_uses_explicit_socket() {
        let path = resolve_daemon_socket_path(Some(Path::new("/tmp/coco.sock"))).unwrap();

        assert_eq!(path, PathBuf::from("/tmp/coco.sock"));
    }

    #[test]
    fn telegram_queue_payload_round_trips_inbound_message() {
        let message =
            InboundMessage::telegram_with_message_id("chat-1", "sender-1", "message-1", "hello");

        let decoded = decode_telegram_message(encode_telegram_message(&message)).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn telegram_queue_payload_round_trips_image_attachments() {
        let message = InboundMessage::telegram_with_message_id_and_images(
            "chat-1",
            "sender-1",
            "message-1",
            "",
            vec![TelegramImageAttachment::from_parts(
                "file-id",
                Some("unique-id".to_owned()),
                Some(1280),
                Some(960),
                Some(200_000),
            )],
        );

        let decoded = decode_telegram_message(encode_telegram_message(&message)).unwrap();

        assert_eq!(decoded, message);
    }

    #[tokio::test]
    async fn abort_channel_task_handles_cancelled_tasks() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = started_tx.send(());
            std::future::pending::<crate::Result<()>>().await
        });
        started_rx.await.unwrap();

        abort_channel_task(task).await.unwrap();
    }

    #[tokio::test]
    async fn daemon_task_group_drains_remaining_tasks_after_first_exit() {
        let tasks = DaemonTaskGroup {
            socket_task: Some(tokio::spawn(async {
                std::future::pending::<crate::Result<()>>().await
            })),
            console: None,
            channel_task: Some(tokio::spawn(async { Ok(()) })),
            message_queue_task: Some(tokio::spawn(async {
                std::future::pending::<crate::Result<()>>().await
            })),
        };

        tasks.run_until_exit().await.unwrap();
    }

    #[tokio::test]
    async fn daemon_task_group_returns_first_exit_error_after_shutdown() {
        let tasks = DaemonTaskGroup {
            socket_task: Some(tokio::spawn(async {
                std::future::pending::<crate::Result<()>>().await
            })),
            console: None,
            channel_task: Some(tokio::spawn(async { Err(crate::Error::EmptyPrompt) })),
            message_queue_task: Some(tokio::spawn(async {
                std::future::pending::<crate::Result<()>>().await
            })),
        };

        let error = tasks.run_until_exit().await.unwrap_err();

        assert!(matches!(error, crate::Error::EmptyPrompt));
    }

    #[tokio::test]
    async fn telegram_message_queue_publisher_persists_inbound_message() {
        let store = MemoryStore::new();
        let publisher = TelegramMessageQueuePublisher::new(store.clone(), Arc::new(Notify::new()));
        let message =
            InboundMessage::telegram_with_message_id("chat-1", "sender-1", "message-1", "hello");

        let response = publisher.handle(message.clone()).await.unwrap();

        assert_eq!(response.text, "queued telegram inbound message");
        let items = store.list_queue_messages(TELEGRAM_INBOUND_QUEUE).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(
            decode_telegram_message(items[0].payload.clone()).unwrap(),
            message
        );
    }

    #[tokio::test]
    async fn telegram_queue_worker_returns_after_message_processing_finishes() {
        let store = MemoryStore::new();
        let backend = BlockingOnceBackend::default();
        let calls = backend.calls.clone();
        let release_first = backend.release_first.clone();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("main")).await.unwrap();
        let message = InboundMessage::telegram("chat", "user", "first");
        store
            .enqueue_message(TELEGRAM_INBOUND_QUEUE, encode_telegram_message(&message))
            .unwrap();
        let worker = TelegramMessageQueueWorker::new(
            "main",
            store.clone(),
            ConversationEngine::new(llm.clone()),
            Arc::new(Notify::new()),
        );

        let drain_task = tokio::spawn(async move { worker.drain_once().await });
        wait_until(Duration::from_secs(1), || calls.load(Ordering::SeqCst) == 1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!drain_task.is_finished());

        release_first.notify_waiters();
        assert_eq!(drain_task.await.unwrap().unwrap(), 1);
        assert!(
            store
                .list_queue_messages(TELEGRAM_INBOUND_QUEUE)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn telegram_queue_worker_waits_for_active_target_branch_job() {
        let store = MemoryStore::new();
        let backend = BlockingOnceBackend::default();
        let calls = backend.calls.clone();
        let release_first = backend.release_first.clone();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("main")).await.unwrap();
        let engine = ConversationEngine::new(llm.clone());
        let active_job = engine
            .submit_job("main", "existing work", vec![])
            .await
            .unwrap();
        let drive_engine = engine.clone();
        let drive_task =
            tokio::spawn(async move { drive_engine.drive_job(&active_job.job_id).await });
        wait_until(Duration::from_secs(1), || calls.load(Ordering::SeqCst) == 1).await;

        let message = InboundMessage::telegram("chat", "user", "next");
        store
            .enqueue_message(TELEGRAM_INBOUND_QUEUE, encode_telegram_message(&message))
            .unwrap();
        let worker =
            TelegramMessageQueueWorker::new("main", store, engine, Arc::new(Notify::new()));
        let drain_task = tokio::spawn(async move { worker.drain_once().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!drain_task.is_finished());

        release_first.notify_waiters();
        let snapshot = drive_task.await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Finished);
        wait_until(Duration::from_secs(1), || calls.load(Ordering::SeqCst) == 2).await;
        assert_eq!(drain_task.await.unwrap().unwrap(), 1);
    }

    #[tokio::test]
    async fn prompt_job_queue_worker_submits_queued_request_after_active_branch_job() {
        let store = MemoryStore::new();
        let backend = BlockingOnceBackend::default();
        let calls = backend.calls.clone();
        let release_first = backend.release_first.clone();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("main")).await.unwrap();
        let engine = ConversationEngine::new(llm);
        let active_job = engine
            .submit_job("main", "active work", vec![])
            .await
            .unwrap();
        let active_job_id = active_job.job_id.clone();
        let drive_engine = engine.clone();
        let drive_job_id = active_job_id.clone();
        let drive_task = tokio::spawn(async move { drive_engine.drive_job(&drive_job_id).await });
        wait_until(Duration::from_secs(1), || calls.load(Ordering::SeqCst) == 1).await;

        store
            .enqueue_message(
                PROMPT_JOB_QUEUE,
                json!(QueuedPromptRequest {
                    job_id: "job-request".to_owned(),
                    branch: "main".to_owned(),
                    prompt: "queued work".to_owned(),
                    merge_parents: vec![],
                    session_patch: None,
                }),
            )
            .unwrap();
        let worker = PromptJobMessageQueueWorker::new(store.clone(), engine.clone());
        let drain_task = tokio::spawn(async move { worker.drain_once().await });
        assert!(engine.get_job("job-request").is_err());
        assert_eq!(
            store.list_queue_messages(PROMPT_JOB_QUEUE).unwrap().len(),
            1
        );
        assert!(!drain_task.is_finished());

        release_first.notify_waiters();
        let snapshot = drive_task.await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Finished);
        drain_task.await.unwrap().unwrap();
        assert_eq!(
            engine.get_job(&active_job_id).unwrap().status,
            JobStatus::Finished
        );
        wait_until(Duration::from_secs(1), || calls.load(Ordering::SeqCst) == 2).await;
        wait_until(Duration::from_secs(1), || {
            engine
                .get_job("job-request")
                .is_ok_and(|job| job.status == JobStatus::Finished)
        })
        .await;
    }

    #[tokio::test]
    async fn prompt_job_queue_worker_backs_off_when_active_job_waits_for_recovery() {
        let store = MemoryStore::new();
        let backend = FailingBackend::default();
        let calls = backend.calls.clone();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("main")).await.unwrap();
        let engine = ConversationEngine::new(llm);
        let active_job = engine
            .submit_job("main", "active work", vec![])
            .await
            .unwrap();
        let active_job_id = active_job.job_id.clone();
        let failed = engine.drive_job(&active_job_id).await.unwrap();
        assert_eq!(failed.status, JobStatus::Running);

        store
            .enqueue_message(
                PROMPT_JOB_QUEUE,
                json!(QueuedPromptRequest {
                    job_id: "job-request".to_owned(),
                    branch: "main".to_owned(),
                    prompt: "queued work".to_owned(),
                    merge_parents: vec![],
                    session_patch: None,
                }),
            )
            .unwrap();
        let worker = PromptJobMessageQueueWorker::new(store.clone(), engine.clone());

        let result = tokio::time::timeout(Duration::from_millis(100), worker.drain_once()).await;

        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            engine.get_job(&active_job_id).unwrap().status,
            JobStatus::Running
        );
        assert_eq!(
            store.list_queue_messages(PROMPT_JOB_QUEUE).unwrap().len(),
            1
        );
        let failure_count = store
            .list_children(&active_job.base)
            .unwrap()
            .into_iter()
            .filter(|node| matches!(node.kind, Kind::Failure(_)))
            .count();
        assert_eq!(failure_count, 1);
    }

    #[tokio::test]
    async fn prompt_job_queue_worker_does_not_start_idle_active_job() {
        let store = MemoryStore::new();
        let backend = FailingBackend::default();
        let calls = backend.calls.clone();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("main")).await.unwrap();
        let engine = ConversationEngine::new(llm);
        let active_job = engine
            .submit_job("main", "active work", vec![])
            .await
            .unwrap();
        let active_job_id = active_job.job_id.clone();
        let request = QueuedPromptRequest {
            job_id: "job-request".to_owned(),
            branch: "main".to_owned(),
            prompt: "queued work".to_owned(),
            merge_parents: vec![],
            session_patch: None,
        };
        store
            .enqueue_message(PROMPT_JOB_QUEUE, json!(request.clone()))
            .unwrap();
        let worker = PromptJobMessageQueueWorker::new(store.clone(), engine.clone());

        let item = store.peek_message(PROMPT_JOB_QUEUE).unwrap().unwrap();
        assert!(matches!(
            worker
                .handle_prompt_request_queue_head(&item, &request)
                .await
                .unwrap(),
            super::PromptQueueHeadResult::Wait(duration)
                if duration == super::ACTIVE_JOB_JOIN_INTERVAL
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            engine.get_job(&active_job_id).unwrap().status,
            JobStatus::Queued
        );

        let item = store.peek_message(PROMPT_JOB_QUEUE).unwrap().unwrap();
        assert!(matches!(
            worker
                .handle_prompt_request_queue_head(&item, &request)
                .await
                .unwrap(),
            super::PromptQueueHeadResult::Wait(duration)
                if duration <= super::ACTIVE_JOB_JOIN_INTERVAL
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.list_queue_messages(PROMPT_JOB_QUEUE).unwrap().len(),
            1
        );
    }

    #[test]
    fn active_job_waiting_for_recovery_detects_failure_child() {
        let store = MemoryStore::new();
        store.fork("main", &store.root_id()).unwrap();
        let base = store.get_branch_head("main").unwrap();
        let active_job = store.submit_job("main", &base).unwrap();
        store
            .append(NewNode {
                parent: base,
                role: Role::System,
                metadata: BackendMetadata::builder().build(),
                kind: Kind::Failure("backend failed".to_owned()),
            })
            .unwrap();

        assert!(super::active_job_is_waiting_for_recovery(&store, &active_job).unwrap());
    }

    #[test]
    fn active_job_waiting_for_recovery_detects_terminal_failure() {
        let store = MemoryStore::new();
        store.fork("main", &store.root_id()).unwrap();
        let base = store.get_branch_head("main").unwrap();
        let active_job = store.submit_job("main", &base).unwrap();
        let failure = store
            .append(NewNode {
                parent: base.clone(),
                role: Role::System,
                metadata: BackendMetadata::builder().build(),
                kind: Kind::Failure("backend failed".to_owned()),
            })
            .unwrap();
        store.set_branch_head("main", &base, &failure).unwrap();

        assert!(super::active_job_is_waiting_for_recovery(&store, &active_job).unwrap());
    }

    #[test]
    fn active_job_waiting_for_recovery_ignores_clean_job() {
        let store = MemoryStore::new();
        store.fork("main", &store.root_id()).unwrap();
        let base = store.get_branch_head("main").unwrap();
        let active_job = store.submit_job("main", &base).unwrap();

        assert!(!super::active_job_is_waiting_for_recovery(&store, &active_job).unwrap());
    }

    #[tokio::test]
    async fn prompt_job_queue_worker_discards_prompt_request_for_missing_branch() {
        let store = MemoryStore::new();
        let backend = BlockingOnceBackend::default();
        let calls = backend.calls.clone();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("main")).await.unwrap();
        let engine = ConversationEngine::new(llm);
        store
            .enqueue_message(
                PROMPT_JOB_QUEUE,
                json!(QueuedPromptRequest {
                    job_id: "job-missing-branch".to_owned(),
                    branch: "missing".to_owned(),
                    prompt: "queued work".to_owned(),
                    merge_parents: vec![],
                    session_patch: None,
                }),
            )
            .unwrap();
        let worker = PromptJobMessageQueueWorker::new(store.clone(), engine.clone());

        worker.drain_once().await.unwrap();

        assert!(engine.get_job("job-missing-branch").is_err());
        assert_eq!(
            store.list_queue_messages(PROMPT_JOB_QUEUE).unwrap().len(),
            0
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn prompt_job_queue_worker_discards_duplicate_prompt_request() {
        let store = MemoryStore::new();
        let backend = BlockingOnceBackend::default();
        let calls = backend.calls.clone();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("main")).await.unwrap();
        let engine = ConversationEngine::new(llm);
        let existing_job = engine
            .submit_job("main", "existing work", vec![])
            .await
            .unwrap();
        store
            .enqueue_message(
                PROMPT_JOB_QUEUE,
                json!(QueuedPromptRequest {
                    job_id: existing_job.job_id.clone(),
                    branch: "main".to_owned(),
                    prompt: "queued work".to_owned(),
                    merge_parents: vec![],
                    session_patch: None,
                }),
            )
            .unwrap();
        let worker = PromptJobMessageQueueWorker::new(store.clone(), engine);

        worker.drain_once().await.unwrap();

        assert!(
            store
                .list_queue_messages(PROMPT_JOB_QUEUE)
                .unwrap()
                .is_empty()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn system_event_queue_worker_materializes_day_prompt_request() {
        let store = MemoryStore::new();
        let backend = BlockingOnceBackend::default();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("day")).await.unwrap();
        store
            .enqueue_message(
                SYSTEM_EVENT_QUEUE,
                json!({
                    "type": "llm.backend_failure.recovery_requested",
                    "version": 1,
                    "dedupe_key": "llm.backend_failure:error-node",
                    "data": {
                        "job_id": "job-failed",
                        "root_branch": "main",
                        "work_branch": "main",
                        "failed_branch": "main",
                        "base_node_id": "base-node",
                        "execution_id": "execution-1",
                        "error_node_id": "error-node",
                        "retry_from_node_id": "retry-node",
                        "message": "backend failed"
                    }
                }),
            )
            .unwrap();
        let worker = SystemEventMessageQueueWorker::new(store.clone());

        assert_eq!(worker.drain_once().await.unwrap(), 1);

        assert!(
            store
                .list_queue_messages(SYSTEM_EVENT_QUEUE)
                .unwrap()
                .is_empty()
        );
        let prompt_items = store.list_queue_messages(PROMPT_JOB_QUEUE).unwrap();
        assert_eq!(prompt_items.len(), 1);
        let request: QueuedPromptRequest =
            serde_json::from_value(prompt_items[0].payload.clone()).unwrap();
        assert_eq!(
            request.job_id,
            format!(
                "job-{}",
                super::hex_encode(b"llm.backend_failure:error-node")
            )
        );
        assert_eq!(request.branch, "day");
        assert!(request.prompt.contains("job-failed"));
        assert!(request.prompt.contains("retry-node"));
        assert!(request.prompt.contains("coco skill run recovery"));
        assert!(request.prompt.contains("from the `day` branch"));
        assert!(request.prompt.contains("Do not fork"));
        assert!(!request.prompt.contains("active recovery branch"));
    }

    #[tokio::test]
    async fn system_event_queue_worker_derives_missing_recovery_dedupe_key() {
        let store = MemoryStore::new();
        let backend = BlockingOnceBackend::default();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("day")).await.unwrap();
        store
            .enqueue_message(
                SYSTEM_EVENT_QUEUE,
                json!({
                    "type": "llm.backend_failure.recovery_requested",
                    "version": 1,
                    "data": {
                        "job_id": "job-failed",
                        "root_branch": "main",
                        "work_branch": "main",
                        "failed_branch": "main",
                        "base_node_id": "base-node",
                        "execution_id": "execution-1",
                        "error_node_id": "error-node",
                        "retry_from_node_id": "retry-node",
                        "message": "backend failed"
                    }
                }),
            )
            .unwrap();
        let worker = SystemEventMessageQueueWorker::new(store.clone());

        assert_eq!(worker.drain_once().await.unwrap(), 1);

        let prompt_items = store.list_queue_messages(PROMPT_JOB_QUEUE).unwrap();
        assert_eq!(prompt_items.len(), 1);
        let request: QueuedPromptRequest =
            serde_json::from_value(prompt_items[0].payload.clone()).unwrap();
        let dedupe_key =
            super::backend_failure_recovery_dedupe_key("job-failed", "main", "retry-node");
        assert_eq!(
            request.job_id,
            format!("job-{}", super::hex_encode(dedupe_key.as_bytes()))
        );
    }

    #[tokio::test]
    async fn system_event_queue_worker_skips_duplicate_recovery_prompt_request() {
        let store = MemoryStore::new();
        let backend = BlockingOnceBackend::default();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("day")).await.unwrap();
        let payload = json!({
            "type": "llm.backend_failure.recovery_requested",
            "version": 1,
            "dedupe_key": "llm.backend_failure:job-failed:main:retry-node",
            "data": {
                "job_id": "job-failed",
                "root_branch": "main",
                "work_branch": "main",
                "failed_branch": "main",
                "base_node_id": "base-node",
                "execution_id": "execution-1",
                "error_node_id": "error-node",
                "retry_from_node_id": "retry-node",
                "message": "backend failed"
            }
        });
        store
            .enqueue_message(SYSTEM_EVENT_QUEUE, payload.clone())
            .unwrap();
        store.enqueue_message(SYSTEM_EVENT_QUEUE, payload).unwrap();
        let worker = SystemEventMessageQueueWorker::new(store.clone());

        assert_eq!(worker.drain_once().await.unwrap(), 2);

        assert!(
            store
                .list_queue_messages(SYSTEM_EVENT_QUEUE)
                .unwrap()
                .is_empty()
        );
        let prompt_items = store.list_queue_messages(PROMPT_JOB_QUEUE).unwrap();
        assert_eq!(prompt_items.len(), 1);
        let request: QueuedPromptRequest =
            serde_json::from_value(prompt_items[0].payload.clone()).unwrap();
        assert_eq!(
            request.job_id,
            format!(
                "job-{}",
                super::hex_encode(b"llm.backend_failure:job-failed:main:retry-node")
            )
        );
    }

    #[test]
    fn stable_system_event_prompt_job_id_preserves_dedupe_key_boundaries() {
        let left =
            SystemEvent::LlmBackendFailureRecoveryRequested(LlmBackendFailureRecoveryRequested {
                dedupe_key: "llm.backend_failure:a-b".to_owned(),
                job_id: "job".to_owned(),
                root_branch: "main".to_owned(),
                work_branch: "main".to_owned(),
                failed_branch: "main".to_owned(),
                base_node_id: "base".to_owned(),
                execution_id: "execution".to_owned(),
                error_node_id: "error".to_owned(),
                retry_from_node_id: "retry".to_owned(),
                message: "failed".to_owned(),
            });
        let right =
            SystemEvent::LlmBackendFailureRecoveryRequested(LlmBackendFailureRecoveryRequested {
                dedupe_key: "llm.backend_failure:a:b".to_owned(),
                job_id: "job".to_owned(),
                root_branch: "main".to_owned(),
                work_branch: "main".to_owned(),
                failed_branch: "main".to_owned(),
                base_node_id: "base".to_owned(),
                execution_id: "execution".to_owned(),
                error_node_id: "error".to_owned(),
                retry_from_node_id: "retry".to_owned(),
                message: "failed".to_owned(),
            });

        assert_ne!(
            super::stable_system_event_prompt_job_id(&left),
            super::stable_system_event_prompt_job_id(&right)
        );
    }

    #[tokio::test]
    async fn system_event_queue_worker_skips_recovery_prompt_when_job_exists() {
        let store = MemoryStore::new();
        let backend = BlockingOnceBackend::default();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("day")).await.unwrap();
        store
            .submit_job_with_id(
                &format!(
                    "job-{}",
                    super::hex_encode(b"llm.backend_failure:job-failed:main:retry-node")
                ),
                "day",
                &store.get_branch_head("day").unwrap(),
            )
            .unwrap();
        store
            .enqueue_message(
                SYSTEM_EVENT_QUEUE,
                json!({
                    "type": "llm.backend_failure.recovery_requested",
                    "version": 1,
                    "dedupe_key": "llm.backend_failure:job-failed:main:retry-node",
                    "data": {
                        "job_id": "job-failed",
                        "root_branch": "main",
                        "work_branch": "main",
                        "failed_branch": "main",
                        "base_node_id": "base-node",
                        "execution_id": "execution-1",
                        "error_node_id": "error-node",
                        "retry_from_node_id": "retry-node",
                        "message": "backend failed"
                    }
                }),
            )
            .unwrap();
        let worker = SystemEventMessageQueueWorker::new(store.clone());

        assert_eq!(worker.drain_once().await.unwrap(), 1);

        assert!(
            store
                .list_queue_messages(SYSTEM_EVENT_QUEUE)
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .list_queue_messages(PROMPT_JOB_QUEUE)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn derive_day_session_config_uses_latest_session_anchor() {
        let store = MemoryStore::new();
        let llm = Arc::new(LlmService::new(
            store.clone(),
            BlockingOnceBackend::default(),
        ));
        let mut config = session_config("main");
        config.model = "old-model".to_owned();
        llm.create_session(config).await.unwrap();
        store
            .handoff_session(
                "main",
                &SessionAnchorPatch {
                    model: Some("new-model".to_owned()),
                    max_tokens: Some(Some(12_345)),
                    ..SessionAnchorPatch::default()
                },
            )
            .unwrap();

        let config = super::derive_day_session_config(&store)
            .unwrap()
            .expect("day config should be derived");

        assert_eq!(config.model, "new-model");
        assert_eq!(config.max_tokens, Some(12_345));
    }

    #[test]
    fn active_job_join_backoff_prunes_expired_entries() {
        let mut active_job = Job::new("job-active", "main", "base");
        active_job.status = JobStatus::Running;
        let stale_job_id = "job-stale".to_owned();
        let mut active_job_joins = HashMap::from([(
            stale_job_id.clone(),
            Instant::now() - super::ACTIVE_JOB_JOIN_INTERVAL,
        )]);

        assert!(matches!(
            super::active_job_join_decision_with_backoff(&mut active_job_joins, &active_job),
            super::ActiveJobJoinDecision::Join
        ));

        assert!(!active_job_joins.contains_key(&stale_job_id));
        assert!(active_job_joins.contains_key(&active_job.job_id));
    }

    #[derive(Debug, Clone, Default)]
    struct BlockingOnceBackend {
        calls: Arc<AtomicUsize>,
        release_first: Arc<Notify>,
    }

    #[derive(Debug, Clone, Default)]
    struct FailingBackend {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl CompletionBackend for BlockingOnceBackend {
        async fn step(
            &self,
            _ctx: StepContext<'_>,
        ) -> std::result::Result<BackendTurn, BackendError> {
            let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
            if call_index == 0 {
                self.release_first.notified().await;
            }

            Ok(BackendTurn {
                message: CompletionMessage::assistant("done"),
                events: vec![],
                tool_calls: vec![],
                final_text: Some("done".to_owned()),
                trace_persisted: false,
            })
        }
    }

    #[async_trait]
    impl CompletionBackend for FailingBackend {
        async fn step(
            &self,
            _ctx: StepContext<'_>,
        ) -> std::result::Result<BackendTurn, BackendError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(BackendError::Failed {
                message: "recoverable backend outage".to_owned(),
            })
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
            prompt: String::new(),
            tools: vec![],
            temperature: None,
            max_tokens: None,
            additional_params: None,
            enable_coco_shim: false,
        }
    }

    async fn wait_until(timeout: Duration, condition: impl Fn() -> bool) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        panic!("condition was not met before timeout");
    }
}
