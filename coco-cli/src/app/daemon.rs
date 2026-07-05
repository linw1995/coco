use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use coco_channel::{
    ChannelRuntime, InboundMessage, MessageHandler, OutboundMessage, TelegramImageAttachment,
    TelegramVoiceAttachment,
};
use coco_channel::{Error as ChannelError, telegram::TelegramChannel};
use coco_console::{
    ConsoleConfig, ConsolePublisher, ConsoleServerHandle, GraphMode, GraphSnapshot,
    build_graph_snapshot_with_mode, start_console_server_with_graph_store_path,
};
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
use sha2::{Digest, Sha256};
use snafu::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::Notify;

use super::{
    config::{ChannelConfigs, ProviderProfiles, resolve_channel_secret},
    prompt::{
        PROMPT_JOB_BRANCH_QUEUE_PREFIX, PROMPT_JOB_ID_BODY_LEN, PROMPT_JOB_ID_PREFIX,
        PROMPT_JOB_QUEUE, QueuedPromptRequest, queue_prompt_job_request,
    },
    run_forwarded_with_services,
    runtime::{ForwardedRuntimeInputs, RuntimeServices},
    session::resolve_session_config,
};
use crate::{
    Result,
    cli::{
        CliSessionRole, CliTool, DaemonCommand, DaemonProfileCommand, DaemonProfileGraphCommand,
        DaemonProfileSubcommand, DaemonSubcommand, SessionCreateCommand,
    },
    error::{
        BindDaemonSocketSnafu, ChannelSnafu, ConsoleSnafu, CoreEngineSnafu, JoinChannelTaskSnafu,
        JoinDaemonServerSnafu, JoinMessageQueueTaskSnafu, LlmSnafu, ResolveDaemonSocketRootSnafu,
        StoreSnafu,
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
const ACTIVE_JOB_RECHECK_INTERVAL: Duration = Duration::from_secs(30);
const CHATGPT_AUTH_CHECK_INTERVAL: Duration = Duration::from_secs(15);

pub struct CocoCliDaemonServerHandle<B, S> {
    socket_path: PathBuf,
    llm: Arc<LlmService<B, S>>,
    socket_task: tokio::task::JoinHandle<()>,
    channel_task: Option<tokio::task::JoinHandle<Result<()>>>,
    message_queue_task: tokio::task::JoinHandle<Result<()>>,
    chatgpt_auth_check_task: Option<tokio::task::JoinHandle<()>>,
    console: Option<ConsoleServerHandle>,
}

pub struct DaemonServerOptions<'a> {
    pub channel_configs: &'a ChannelConfigs,
    pub console: Option<ConsoleServerHandle>,
}

#[derive(Debug, Serialize)]
struct DaemonGraphProfileResult {
    mode: &'static str,
    version: u64,
    nodes: usize,
    edges: usize,
    branches: usize,
    provider_contexts: usize,
    duration_ms: u128,
}

pub async fn run_daemon_command<B, S>(
    command: DaemonCommand,
    shared_store: &S,
    llm: &Arc<LlmService<B, S>>,
    provider_profiles: &ProviderProfiles,
    channel_configs: &ChannelConfigs,
    console_publisher: Option<ConsolePublisher>,
    console_graph_store_path: PathBuf,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    if let DaemonSubcommand::Profile(command) = &command.command {
        return run_daemon_profile_command(command, shared_store)
            .await
            .map(Some);
    }

    let socket_path = resolve_daemon_command_socket_path(&command)?;
    let shared_engine = Arc::new(ConversationEngine::new(llm.clone()));
    let console_config = daemon_console_config(&command, console_publisher.as_ref());

    ensure_initial_session(shared_store, llm, provider_profiles).await?;
    let mut server = start_daemon_server(
        &socket_path,
        shared_store,
        llm,
        provider_profiles,
        &shared_engine,
        DaemonServerOptions {
            channel_configs,
            console: None,
        },
    )?;
    if let Some(config) = console_config {
        let console = match start_console_server_with_graph_store_path(
            config,
            shared_store.clone(),
            console_publisher.expect("console publisher should exist when console is enabled"),
            console_graph_store_path,
        )
        .await
        {
            Ok(console) => console,
            Err(source) => {
                if let Err(error) = server.shutdown().await {
                    tracing::warn!(
                        error = %error,
                        "failed to shut down daemon after console startup failure"
                    );
                }
                return Err(source).context(ConsoleSnafu);
            }
        };
        tracing::info!(addr = %console.addr(), "coco console listening");
        server.console = Some(console);
    }
    spawn_resume_incomplete_jobs(shared_engine);
    server.wait().await?;
    Ok(None)
}

fn resolve_daemon_command_socket_path(command: &DaemonCommand) -> Result<PathBuf> {
    resolve_daemon_socket_path(match &command.command {
        DaemonSubcommand::Serve(command) => command.socket.as_deref(),
        DaemonSubcommand::Profile(_) => None,
    })
}

fn daemon_console_config(
    command: &DaemonCommand,
    console_publisher: Option<&ConsolePublisher>,
) -> Option<ConsoleConfig> {
    match (&command.command, console_publisher) {
        (DaemonSubcommand::Serve(command), Some(_)) if !command.no_console => Some(ConsoleConfig {
            addr: command.console_addr,
        }),
        _ => None,
    }
}

async fn run_daemon_profile_command<S>(
    command: &DaemonProfileCommand,
    shared_store: &S,
) -> Result<String>
where
    S: Store + Clone + Send + Sync + 'static,
{
    match &command.command {
        DaemonProfileSubcommand::Graph(command) => {
            run_daemon_profile_graph_command(command, shared_store).await
        }
    }
}

async fn run_daemon_profile_graph_command<S>(
    command: &DaemonProfileGraphCommand,
    shared_store: &S,
) -> Result<String>
where
    S: Store + Clone + Send + Sync + 'static,
{
    let mode = daemon_profile_graph_mode(command);
    let started_at = Instant::now();
    let snapshot = build_graph_snapshot_with_mode(shared_store, 0, mode)
        .await
        .context(ConsoleSnafu)?;
    let result = daemon_graph_profile_result(mode, &snapshot, started_at.elapsed());

    if command.json {
        Ok(render_daemon_graph_profile_json(&result))
    } else {
        Ok(render_daemon_graph_profile_text(&result))
    }
}

fn daemon_profile_graph_mode(command: &DaemonProfileGraphCommand) -> GraphMode {
    if command.all {
        GraphMode::All
    } else {
        GraphMode::Anchors
    }
}

fn daemon_graph_profile_result(
    mode: GraphMode,
    snapshot: &GraphSnapshot,
    duration: Duration,
) -> DaemonGraphProfileResult {
    DaemonGraphProfileResult {
        mode: mode.as_query_value(),
        version: snapshot.version,
        nodes: snapshot.nodes.len(),
        edges: snapshot.edges.len(),
        branches: snapshot.branches.len(),
        provider_contexts: snapshot.provider_contexts.len(),
        duration_ms: duration.as_millis(),
    }
}

fn render_daemon_graph_profile_text(result: &DaemonGraphProfileResult) -> String {
    format!(
        "built graph mode={} version={} nodes={} edges={} branches={} provider_contexts={} duration_ms={}",
        result.mode,
        result.version,
        result.nodes,
        result.edges,
        result.branches,
        result.provider_contexts,
        result.duration_ms
    )
}

fn render_daemon_graph_profile_json(result: &DaemonGraphProfileResult) -> String {
    serde_json::to_string_pretty(result).expect("daemon graph profile output should serialize")
}

pub async fn ensure_initial_session<B, S>(
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
        .await
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
    if builtin_day_session_is_valid(shared_store).await? {
        return Ok(());
    }

    match shared_store.get_branch_head(BUILTIN_DAY_BRANCH).await {
        Ok(_) => {
            tracing::warn!(
                branch = BUILTIN_DAY_BRANCH,
                "replacing invalid builtin day session"
            );
            shared_store
                .delete_branch(BUILTIN_DAY_BRANCH)
                .await
                .context(StoreSnafu)?;
        }
        Err(StoreError::BranchNotFound { .. }) => {}
        Err(source) => return Err(source).context(StoreSnafu),
    }

    let config = match resolve_session_config(day_session_create_command(), provider_profiles) {
        Ok(config) => config,
        Err(error) => {
            let Some(config) = derive_day_session_config(shared_store).await? else {
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

async fn builtin_day_session_is_valid(store: &impl Store) -> Result<bool> {
    let head = match store.get_branch_head(BUILTIN_DAY_BRANCH).await {
        Ok(head) => head,
        Err(StoreError::BranchNotFound { .. }) => return Ok(false),
        Err(source) => return Err(source).context(StoreSnafu),
    };
    match store.get_session_state(BUILTIN_DAY_BRANCH).await {
        Ok(_) => {}
        Err(StoreError::BranchNotFound { .. }) => return Ok(false),
        Err(source) => return Err(source).context(StoreSnafu),
    }

    let ancestry = store.ancestry(&head).await.context(StoreSnafu)?;
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

async fn derive_day_session_config(store: &impl Store) -> Result<Option<SessionConfig>> {
    let mut branches = store
        .list_session_states()
        .await
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
        let head = store.get_branch_head(&branch).await.context(StoreSnafu)?;
        let ancestry = store.ancestry(&head).await.context(StoreSnafu)?;
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
        enable_all_tools: false,
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
        enable_all_tools: false,
        enable_coco_shim: true,
        disable_coco_shim: false,
    }
}

pub async fn resume_incomplete_jobs<B, S>(engine: &ConversationEngine<B, S>) -> Result<()>
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

pub fn resolve_daemon_socket_path(socket_path: Option<&Path>) -> Result<PathBuf> {
    match socket_path {
        Some(path) => Ok(path.to_path_buf()),
        None => resolve_default_daemon_socket_path(),
    }
}

pub fn start_daemon_server<B, S>(
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
    let console = options.console;
    if let Some(console) = &console {
        tracing::info!(addr = %console.addr(), "coco console listening");
    }
    let channel_task = start_channel_task(options.channel_configs, &shared_store, &shared_engine)?;
    let message_queue_task = start_message_queue_task(&shared_store, &shared_engine);
    let chatgpt_auth_check_task = if llm.enables_provider_auth_checks() {
        start_chatgpt_auth_check_task(llm.clone())
    } else {
        None
    };
    let handle_llm = llm.clone();

    let socket_task = tokio::spawn(async move {
        loop {
            let accepted = listener.accept().await;
            let (mut stream, _) = match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::warn!(error = %error, "daemon socket accept failed");
                    break;
                }
            };
            let shared_store = shared_store.clone();
            let llm = llm.clone();
            let provider_profiles = provider_profiles.clone();
            let shared_engine = shared_engine.clone();
            tokio::spawn(async move {
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
            });
        }
    });

    Ok(CocoCliDaemonServerHandle {
        socket_path,
        llm: handle_llm,
        socket_task,
        channel_task,
        message_queue_task,
        chatgpt_auth_check_task,
        console,
    })
}

fn start_chatgpt_auth_check_task<B, S>(
    llm: Arc<LlmService<B, S>>,
) -> Option<tokio::task::JoinHandle<()>>
where
    B: CompletionBackend + 'static,
    S: Send + Sync + 'static,
{
    let configs = llm.chatgpt_auth_check_configs();
    if configs.is_empty() {
        return None;
    }

    tracing::info!(
        provider_profile_count = configs.len(),
        interval_secs = CHATGPT_AUTH_CHECK_INTERVAL.as_secs(),
        "starting chatgpt auth check task"
    );
    Some(tokio::spawn(async move {
        loop {
            for config in &configs {
                match llm.authorize_chatgpt_provider(config).await {
                    Ok(()) => tracing::debug!(
                        provider_profile = %config.profile,
                        "chatgpt auth check succeeded"
                    ),
                    Err(error) => tracing::warn!(
                        provider_profile = %config.profile,
                        error = %error,
                        "chatgpt auth check failed"
                    ),
                }
            }
            tokio::time::sleep(CHATGPT_AUTH_CHECK_INTERVAL).await;
        }
    }))
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
        PromptJobMessageQueueSupervisor::new(shared_store.clone(), shared_engine.as_ref().clone());
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
    #[serde(default)]
    voice_attachments: Vec<QueuedTelegramVoiceAttachment>,
}

#[derive(Debug, Serialize, Deserialize)]
struct QueuedTelegramImageAttachment {
    file_id: String,
    file_unique_id: Option<String>,
    width: Option<u32>,
    height: Option<u32>,
    file_size: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct QueuedTelegramVoiceAttachment {
    file_id: String,
    file_unique_id: Option<String>,
    duration_secs: Option<u32>,
    mime_type: Option<String>,
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
            .await
            .context(StoreSnafu)?
        {
            match decode_system_event_message(item.payload.clone()) {
                Ok(event) => {
                    if !self.queue_prompt_job_for_event(&item, event).await? {
                        return Ok(handled);
                    }
                    self.store
                        .dequeue_message(SYSTEM_EVENT_QUEUE)
                        .await
                        .context(StoreSnafu)?;
                }
                Err(error) => {
                    self.store
                        .dequeue_message(SYSTEM_EVENT_QUEUE)
                        .await
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

    async fn queue_prompt_job_for_event(
        &self,
        item: &MessageQueueItem,
        event: SystemEvent,
    ) -> Result<bool>
    where
        S: Store,
    {
        let route = route_system_event(&event);
        match self.store.get_branch_head(route.branch).await {
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
        let dedupe_job_ids = system_event_prompt_job_dedupe_ids(&event);
        if self.prompt_job_request_exists(&dedupe_job_ids).await? {
            tracing::debug!(
                message_id = %item.message_id,
                queue = SYSTEM_EVENT_QUEUE,
                branch = route.branch,
                job_id = %request.job_id,
                "skipped duplicate system event prompt job"
            );
            return Ok(true);
        }

        queue_prompt_job_request(&self.store, request).await?;
        tracing::info!(
            message_id = %item.message_id,
            queue = SYSTEM_EVENT_QUEUE,
            branch = route.branch,
            "queued system event prompt job"
        );
        Ok(true)
    }

    async fn prompt_job_request_exists(&self, job_ids: &[String]) -> Result<bool>
    where
        S: Store,
    {
        for job_id in job_ids {
            match self.store.get_job(job_id).await {
                Ok(_) => return Ok(true),
                Err(StoreError::PromptJobNotFound { .. }) => {}
                Err(source) => return Err(source).context(StoreSnafu),
            }
        }

        for queue in self
            .store
            .list_message_queues()
            .await
            .context(StoreSnafu)?
            .into_iter()
            .filter(|queue| is_prompt_job_queue(queue))
        {
            if self
                .store
                .list_queue_messages(&queue)
                .await
                .context(StoreSnafu)?
                .into_iter()
                .filter_map(|item| decode_prompt_job_message(item.payload).ok())
                .any(|request| job_ids.contains(&request.job_id))
            {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

struct PromptJobMessageQueueSupervisor<B, S> {
    store: S,
    engine: ConversationEngine<B, S>,
}

impl<B, S> PromptJobMessageQueueSupervisor<B, S> {
    fn new(store: S, engine: ConversationEngine<B, S>) -> Self {
        Self { store, engine }
    }

    async fn run(self) -> Result<()>
    where
        B: CompletionBackend + 'static,
        S: Store + Clone + Send + Sync + 'static,
    {
        let mut queues = HashSet::new();
        let mut workers = Vec::<(String, tokio::task::JoinHandle<Result<()>>)>::new();
        loop {
            for queue in self.prompt_job_queues().await? {
                if queues.insert(queue.clone()) {
                    let worker = PromptJobMessageQueueWorker::new(
                        queue.clone(),
                        self.store.clone(),
                        self.engine.clone(),
                    );
                    workers.push((queue, tokio::spawn(worker.run())));
                }
            }

            let mut index = 0;
            while index < workers.len() {
                if workers[index].1.is_finished() {
                    let (queue, worker) = workers.swap_remove(index);
                    queues.remove(&queue);
                    worker.await.context(JoinMessageQueueTaskSnafu)??;
                } else {
                    index += 1;
                }
            }

            tokio::time::sleep(PROMPT_JOB_QUEUE_IDLE_DELAY).await;
        }
    }

    async fn prompt_job_queues(&self) -> Result<Vec<String>>
    where
        S: Store,
    {
        Ok(self
            .store
            .list_message_queues()
            .await
            .context(StoreSnafu)?
            .into_iter()
            .filter(|queue| is_prompt_job_queue(queue))
            .collect())
    }
}

struct PromptJobMessageQueueWorker<B, S> {
    queue: String,
    store: S,
    engine: ConversationEngine<B, S>,
}

impl<B, S> PromptJobMessageQueueWorker<B, S> {
    fn new(queue: String, store: S, engine: ConversationEngine<B, S>) -> Self {
        Self {
            queue,
            store,
            engine,
        }
    }

    async fn run(self) -> Result<()>
    where
        B: CompletionBackend + 'static,
        S: Store + Clone + Send + Sync + 'static,
    {
        loop {
            self.drain_once().await?;
            if self.peek_prompt_queue_head().await?.is_none() {
                return Ok(());
            }
        }
    }

    async fn drain_once(&self) -> Result<()>
    where
        B: CompletionBackend + 'static,
        S: Store + Clone + Send + Sync + 'static,
    {
        let mut progressed = false;
        let mut saw_item = false;
        let mut wait_duration = None;
        while let Some(item) = self.peek_prompt_queue_head().await? {
            saw_item = true;
            match decode_prompt_job_message(item.payload.clone()) {
                Ok(request) => {
                    match self
                        .handle_prompt_request_queue_head(&item, &request)
                        .await?
                    {
                        PromptQueueHeadResult::Continue => {
                            progressed = true;
                        }
                        PromptQueueHeadResult::Wait(duration) => {
                            wait_duration = Some(shortest_wait(wait_duration, duration));
                            break;
                        }
                    }
                }
                Err(_) => {
                    let Some(item) = self
                        .store
                        .dequeue_message(&self.queue)
                        .await
                        .context(StoreSnafu)?
                    else {
                        continue;
                    };
                    self.handle_item(item).await;
                    progressed = true;
                }
            }
        }
        if !progressed && saw_item {
            tokio::time::sleep(wait_duration.unwrap_or(PROMPT_JOB_QUEUE_IDLE_DELAY)).await;
        }
        Ok(())
    }

    async fn peek_prompt_queue_head(&self) -> Result<Option<MessageQueueItem>>
    where
        S: Store,
    {
        self.store
            .peek_message(&self.queue)
            .await
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
        if matches!(
            self.ensure_prompt_request_queue_head_ready(item, request)
                .await?,
            PromptQueueHeadReadiness::Continue
        ) {
            return Ok(PromptQueueHeadResult::Continue);
        }

        if let Some(active_job) = self
            .engine
            .active_branch_prompt_job(&request.branch)
            .await
            .context(CoreEngineSnafu)?
        {
            return self
                .wait_for_active_branch_job(item, request, active_job)
                .await;
        }

        let guard = self.engine.lock_branch(&request.branch).await;
        let Some(item) = self.peek_prompt_queue_head().await? else {
            return Ok(PromptQueueHeadResult::Continue);
        };
        let request = match decode_prompt_job_message(item.payload.clone()) {
            Ok(request) => request,
            Err(_) => {
                let Some(item) = self
                    .store
                    .dequeue_message(&self.queue)
                    .await
                    .context(StoreSnafu)?
                else {
                    return Ok(PromptQueueHeadResult::Continue);
                };
                self.handle_item(item).await;
                return Ok(PromptQueueHeadResult::Continue);
            }
        };

        if matches!(
            self.ensure_prompt_request_queue_head_ready(&item, &request)
                .await?,
            PromptQueueHeadReadiness::Continue
        ) {
            return Ok(PromptQueueHeadResult::Continue);
        }

        if let Some(active_job) = self
            .engine
            .active_branch_prompt_job(&request.branch)
            .await
            .context(CoreEngineSnafu)?
        {
            drop(guard);
            return self
                .wait_for_active_branch_job(&item, &request, active_job)
                .await;
        }
        let Some(item) = self
            .store
            .dequeue_message(&self.queue)
            .await
            .context(StoreSnafu)?
        else {
            return Ok(PromptQueueHeadResult::Continue);
        };
        self.handle_item(item).await;
        Ok(PromptQueueHeadResult::Continue)
    }

    async fn ensure_prompt_request_queue_head_ready(
        &self,
        item: &MessageQueueItem,
        request: &QueuedPromptRequest,
    ) -> Result<PromptQueueHeadReadiness>
    where
        S: Store,
    {
        match self.store.get_branch_head(&request.branch).await {
            Ok(_) => {}
            Err(coco_mem::StoreError::BranchNotFound { .. }) => {
                self.discard_prompt_request_for_missing_branch(item, request)
                    .await?;
                return Ok(PromptQueueHeadReadiness::Continue);
            }
            Err(error) => return Err(error).context(StoreSnafu),
        }

        match self.store.get_job(&request.job_id).await {
            Ok(_) => {
                self.discard_duplicate_prompt_request(item, request).await?;
                return Ok(PromptQueueHeadReadiness::Continue);
            }
            Err(coco_mem::StoreError::PromptJobNotFound { .. }) => {}
            Err(error) => return Err(error).context(StoreSnafu),
        }

        Ok(PromptQueueHeadReadiness::Ready)
    }

    async fn discard_prompt_request_for_missing_branch(
        &self,
        item: &MessageQueueItem,
        request: &QueuedPromptRequest,
    ) -> Result<()>
    where
        S: Store,
    {
        if self
            .store
            .dequeue_message(&self.queue)
            .await
            .context(StoreSnafu)?
            .is_some()
        {
            tracing::warn!(
                message_id = %item.message_id,
                queue = %self.queue,
                job_id = %request.job_id,
                branch = %request.branch,
                "discarded queued prompt job request for missing branch"
            );
        }
        Ok(())
    }

    async fn discard_duplicate_prompt_request(
        &self,
        item: &MessageQueueItem,
        request: &QueuedPromptRequest,
    ) -> Result<()>
    where
        S: Store,
    {
        if self
            .store
            .dequeue_message(&self.queue)
            .await
            .context(StoreSnafu)?
            .is_some()
        {
            tracing::warn!(
                message_id = %item.message_id,
                queue = %self.queue,
                job_id = %request.job_id,
                branch = %request.branch,
                "discarded duplicate queued prompt job request"
            );
        }
        Ok(())
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
        if matches!(active_job.status, JobStatus::Queued) {
            tracing::debug!(
                message_id = %item.message_id,
                queue = %self.queue,
                branch = %request.branch,
                job_id = %request.job_id,
                active_job_id = %active_job.job_id,
                active_job_status = ?active_job.status,
                wait_ms = ACTIVE_JOB_RECHECK_INTERVAL.as_millis(),
                "active branch prompt job has not started; queued request will wait"
            );
            return Ok(PromptQueueHeadResult::Wait(ACTIVE_JOB_RECHECK_INTERVAL));
        }

        if active_job_is_waiting_for_recovery(&self.store, &active_job)
            .await
            .context(StoreSnafu)?
        {
            tracing::debug!(
                message_id = %item.message_id,
                queue = %self.queue,
                branch = %request.branch,
                job_id = %request.job_id,
                active_job_id = %active_job.job_id,
                active_job_status = ?active_job.status,
                wait_ms = ACTIVE_JOB_RECHECK_INTERVAL.as_millis(),
                "active branch prompt job is waiting for recovery; queued request will wait"
            );
            return Ok(PromptQueueHeadResult::Wait(ACTIVE_JOB_RECHECK_INTERVAL));
        }

        if !self.engine.has_inflight_job(&active_job.job_id).await {
            tracing::debug!(
                message_id = %item.message_id,
                queue = %self.queue,
                branch = %request.branch,
                job_id = %request.job_id,
                active_job_id = %active_job.job_id,
                active_job_status = ?active_job.status,
                wait_ms = ACTIVE_JOB_RECHECK_INTERVAL.as_millis(),
                "active branch prompt job has no inflight drive; queued request will wait"
            );
            return Ok(PromptQueueHeadResult::Wait(ACTIVE_JOB_RECHECK_INTERVAL));
        }

        tracing::info!(
            message_id = %item.message_id,
            queue = %self.queue,
            branch = %request.branch,
            job_id = %request.job_id,
            active_job_id = %active_job.job_id,
            active_job_status = ?active_job.status,
            "joining active branch prompt job before handling queued request"
        );

        Ok(match self.engine.join_job(&active_job.job_id).await {
            Ok(snapshot) if matches!(snapshot.status, JobStatus::Finished) => {
                PromptQueueHeadResult::Continue
            }
            Ok(snapshot) => {
                tracing::info!(
                    message_id = %item.message_id,
                    queue = %self.queue,
                    branch = %request.branch,
                    job_id = %request.job_id,
                    active_job_id = %active_job.job_id,
                    active_job_status = ?snapshot.status,
                    wait_ms = ACTIVE_JOB_RECHECK_INTERVAL.as_millis(),
                    "active branch prompt job still blocks queued request"
                );
                PromptQueueHeadResult::Wait(ACTIVE_JOB_RECHECK_INTERVAL)
            }
            Err(error) => {
                tracing::warn!(
                    message_id = %item.message_id,
                    queue = %self.queue,
                    branch = %request.branch,
                    job_id = %request.job_id,
                    active_job_id = %active_job.job_id,
                    error = %error,
                    wait_ms = ACTIVE_JOB_RECHECK_INTERVAL.as_millis(),
                    "failed to join active branch prompt job before materializing queued request"
                );
                PromptQueueHeadResult::Wait(ACTIVE_JOB_RECHECK_INTERVAL)
            }
        })
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
                    queue = %item.queue,
                    error = %error,
                    "discarded invalid prompt job queue message"
                );
                return;
            }
        };

        tracing::info!(
            message_id = %item.message_id,
            queue = %item.queue,
            job_id = %job.job_id,
            branch = %job.branch,
            "handling queued prompt job request"
        );
        self.handle_prompt_request(&item.queue, &item.message_id, job)
            .await;
    }

    async fn handle_prompt_request(
        &self,
        queue: &str,
        message_id: &str,
        request: QueuedPromptRequest,
    ) where
        B: CompletionBackend + 'static,
        S: Store + Clone + Send + Sync + 'static,
    {
        if let Err(error) = self.try_handle_prompt_request(&request).await {
            if is_missing_branch_error(&error) {
                tracing::warn!(
                    message_id = %message_id,
                    queue = %queue,
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
                    queue = %queue,
                    job_id = %request.job_id,
                    branch = %request.branch,
                    error = %error,
                    "discarded duplicate queued prompt job request"
                );
                return;
            }

            tracing::warn!(
                message_id = %message_id,
                queue = %queue,
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
            .await
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
            .await
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

        if let Err(error) =
            wait_for_branch_to_accept_channel_prompt(&self.engine, &self.branch, &item.message_id)
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
    engine: &ConversationEngine<B, S>,
    branch: &str,
    message_id: &str,
) -> Result<()>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    loop {
        let active_job = engine
            .active_branch_prompt_job(branch)
            .await
            .context(CoreEngineSnafu)?;
        let Some(active_job) = active_job else {
            return Ok(());
        };

        if matches!(active_job.status, JobStatus::Queued) {
            tracing::debug!(
                message_id = %message_id,
                queue = TELEGRAM_INBOUND_QUEUE,
                branch = %branch,
                active_job_id = %active_job.job_id,
                active_job_status = ?active_job.status,
                "active branch prompt job has not started; queued request will wait"
            );
        } else {
            tracing::info!(
                message_id = %message_id,
                queue = TELEGRAM_INBOUND_QUEUE,
                branch = %branch,
                active_job_id = %active_job.job_id,
                active_job_status = ?active_job.status,
                "waiting for active branch prompt job before handling queued request"
            );
        }

        engine
            .join_job(&active_job.job_id)
            .await
            .context(CoreEngineSnafu)?;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromptQueueHeadResult {
    Continue,
    Wait(Duration),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromptQueueHeadReadiness {
    Ready,
    Continue,
}

fn is_prompt_job_queue(queue: &str) -> bool {
    queue == PROMPT_JOB_QUEUE || queue.starts_with(PROMPT_JOB_BRANCH_QUEUE_PREFIX)
}

fn shortest_wait(current: Option<Duration>, candidate: Duration) -> Duration {
    current.map_or(candidate, |current| current.min(candidate))
}

async fn active_job_is_waiting_for_recovery(
    store: &impl Store,
    active_job: &coco_mem::Job,
) -> std::result::Result<bool, StoreError> {
    let path = match store.log(&active_job.base, &active_job.work_branch).await {
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
        .list_children(&last_node.id)
        .await?
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
        "voice_attachments": message
            .voice_attachments()
            .iter()
            .map(|voice| {
                json!({
                    "file_id": voice.file_id(),
                    "file_unique_id": voice.file_unique_id(),
                    "duration_secs": voice.duration_secs(),
                    "mime_type": voice.mime_type(),
                    "file_size": voice.file_size(),
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
    let voice_attachments = message
        .voice_attachments
        .into_iter()
        .map(|voice| {
            TelegramVoiceAttachment::from_parts(
                voice.file_id,
                voice.file_unique_id,
                voice.duration_secs,
                voice.mime_type,
                voice.file_size,
            )
        })
        .collect::<Vec<_>>();
    Ok(match message.source_message_id {
        Some(source_message_id)
            if !image_attachments.is_empty() || !voice_attachments.is_empty() =>
        {
            InboundMessage::telegram_with_message_id_and_attachments(
                message.chat_id,
                message.sender_id,
                source_message_id,
                message.text,
                image_attachments,
                voice_attachments,
            )
        }
        Some(source_message_id) => InboundMessage::telegram_with_message_id(
            message.chat_id,
            message.sender_id,
            source_message_id,
            message.text,
        ),
        None if !image_attachments.is_empty() || !voice_attachments.is_empty() => {
            InboundMessage::telegram_with_attachments(
                message.chat_id,
                message.sender_id,
                message.text,
                image_attachments,
                voice_attachments,
            )
        }
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
            stable_prompt_job_id_from_dedupe_key(&event.dedupe_key)
        }
    }
}

fn system_event_prompt_job_dedupe_ids(event: &SystemEvent) -> Vec<String> {
    match event {
        SystemEvent::LlmBackendFailureRecoveryRequested(event) => vec![
            stable_prompt_job_id_from_dedupe_key(&event.dedupe_key),
            legacy_prompt_job_id_from_dedupe_key(&event.dedupe_key),
        ],
    }
}

fn stable_prompt_job_id_from_dedupe_key(dedupe_key: &str) -> String {
    let digest = Sha256::digest(dedupe_key.as_bytes());
    let body = nanoid::format(
        |size| digest.iter().copied().cycle().take(size).collect(),
        &nanoid::alphabet::SAFE,
        PROMPT_JOB_ID_BODY_LEN,
    );
    format!("{PROMPT_JOB_ID_PREFIX}{body}")
}

fn legacy_prompt_job_id_from_dedupe_key(dedupe_key: &str) -> String {
    format!(
        "{PROMPT_JOB_ID_PREFIX}{}",
        hex_encode(dedupe_key.as_bytes())
    )
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
    pub async fn wait(self) -> Result<()> {
        let Self {
            socket_path,
            llm,
            socket_task,
            channel_task,
            message_queue_task,
            chatgpt_auth_check_task,
            console,
        } = self;

        wait_daemon_tasks(
            socket_path,
            llm,
            socket_task,
            console,
            channel_task,
            message_queue_task,
            chatgpt_auth_check_task,
        )
        .await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn shutdown(self) -> Result<()> {
        self.socket_task.abort();
        if let Some(channel_task) = &self.channel_task {
            channel_task.abort();
        }
        self.message_queue_task.abort();
        abort_background_task(self.chatgpt_auth_check_task, "chatgpt auth check").await;
        let socket_result = self.socket_task.await;
        if let Some(channel_task) = self.channel_task {
            match channel_task.await {
                Ok(result) => result?,
                Err(source) if source.is_cancelled() => {}
                Err(source) => return Err(source).context(JoinChannelTaskSnafu),
            }
        }
        match self.message_queue_task.await {
            Ok(result) => result?,
            Err(source) if source.is_cancelled() => {}
            Err(source) => return Err(source).context(JoinMessageQueueTaskSnafu),
        }
        if let Some(console) = self.console {
            console.shutdown().await.context(ConsoleSnafu)?;
        }
        self.llm.cleanup_runtime_processes().await;
        cleanup_socket(&self.socket_path);
        match socket_result {
            Ok(()) => Ok(()),
            Err(source) if source.is_cancelled() => Ok(()),
            Err(source) => Err(source).context(JoinDaemonServerSnafu),
        }
    }
}

async fn wait_daemon_tasks<B, S>(
    socket_path: PathBuf,
    llm: Arc<LlmService<B, S>>,
    socket_task: tokio::task::JoinHandle<()>,
    mut console: Option<ConsoleServerHandle>,
    mut channel_task: Option<tokio::task::JoinHandle<Result<()>>>,
    mut message_queue_task: tokio::task::JoinHandle<Result<()>>,
    chatgpt_auth_check_task: Option<tokio::task::JoinHandle<()>>,
) -> Result<()> {
    tokio::select! {
        socket_result = socket_task => {
            shutdown_console(console).await?;
            abort_channel_task(channel_task).await?;
            abort_message_queue_task(message_queue_task).await?;
            abort_background_task(chatgpt_auth_check_task, "chatgpt auth check").await;
            llm.cleanup_runtime_processes().await;
            cleanup_socket(&socket_path);
            socket_result.context(JoinDaemonServerSnafu).map(|_| ())
        }
        console_result = async { console.as_mut().expect("console task should exist").wait_mut().await }, if console.is_some() => {
            abort_channel_task(channel_task).await?;
            abort_message_queue_task(message_queue_task).await?;
            abort_background_task(chatgpt_auth_check_task, "chatgpt auth check").await;
            llm.cleanup_runtime_processes().await;
            cleanup_socket(&socket_path);
            console_result.context(ConsoleSnafu)
        }
        channel_result = async { channel_task.as_mut().expect("channel task should exist").await }, if channel_task.is_some() => {
            shutdown_console(console).await?;
            abort_message_queue_task(message_queue_task).await?;
            abort_background_task(chatgpt_auth_check_task, "chatgpt auth check").await;
            llm.cleanup_runtime_processes().await;
            cleanup_socket(&socket_path);
            channel_result.context(JoinChannelTaskSnafu)??;
            Ok(())
        }
        message_queue_result = &mut message_queue_task => {
            shutdown_console(console).await?;
            abort_channel_task(channel_task).await?;
            abort_background_task(chatgpt_auth_check_task, "chatgpt auth check").await;
            llm.cleanup_runtime_processes().await;
            cleanup_socket(&socket_path);
            message_queue_result.context(JoinMessageQueueTaskSnafu)??;
            Ok(())
        }
    }
}

async fn shutdown_console(console: Option<ConsoleServerHandle>) -> Result<()> {
    if let Some(console) = console {
        console.shutdown().await.context(ConsoleSnafu)?;
    }
    Ok(())
}

async fn abort_channel_task(
    channel_task: Option<tokio::task::JoinHandle<Result<()>>>,
) -> Result<()> {
    if let Some(channel_task) = channel_task {
        channel_task.abort();
        match channel_task.await {
            Ok(result) => result,
            Err(source) if source.is_cancelled() => Ok(()),
            Err(source) => Err(source).context(JoinChannelTaskSnafu),
        }?;
    }
    Ok(())
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

async fn abort_background_task(task: Option<tokio::task::JoinHandle<()>>, task_name: &str) {
    let Some(task) = task else {
        return;
    };

    task.abort();
    match task.await {
        Ok(()) => {}
        Err(source) if source.is_cancelled() => {}
        Err(source) => tracing::warn!(
            task = task_name,
            error = %source,
            "background task ended with join error"
        ),
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
    use std::collections::{BTreeMap, HashMap};
    use std::ffi::OsString;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use clap::Parser;
    use coco_channel::{
        InboundMessage, MessageHandler, TelegramImageAttachment, TelegramVoiceAttachment,
    };
    use coco_console::ConsolePublisher;
    use coco_core::ConversationEngine;
    use coco_llm::{
        BackendError, BackendTurn, CompletionBackend, CompletionMessage, LlmService, Provider,
        SessionConfig, StepContext,
    };
    use coco_mem::{
        BackendMetadata, BranchStore, JobStatus, JobStore, Kind, MessageQueueStore, NewNode,
        NodeStore, ProviderProfile, Role, SessionAnchorPatch, SessionRole, SessionStore,
        SqliteStore,
    };
    use serde_json::json;
    use tokio::sync::Notify;

    use crate::app::prompt::{
        PROMPT_JOB_ID_BODY_LEN, PROMPT_JOB_ID_PREFIX, QueuedPromptRequest,
        prompt_job_queue_for_branch,
    };
    use crate::cli::{Cli, Command, DaemonCommand};

    use super::{
        ChannelConfigs, CocoCliDaemonServerHandle, DEFAULT_SESSION_BRANCH,
        LlmBackendFailureRecoveryRequested, PROMPT_JOB_QUEUE, PromptJobMessageQueueWorker,
        ProviderProfiles, SYSTEM_EVENT_QUEUE, SystemEvent, SystemEventMessageQueueWorker,
        TELEGRAM_INBOUND_QUEUE, TelegramMessageQueuePublisher, TelegramMessageQueueWorker,
        abort_channel_task, daemon_console_config, decode_telegram_message,
        encode_telegram_message, resolve_daemon_command_socket_path, resolve_daemon_socket_path,
        run_daemon_command, start_chatgpt_auth_check_task,
    };

    async fn test_store() -> SqliteStore {
        SqliteStore::open_temporary()
            .await
            .expect("temporary SQLite store should open")
    }

    fn daemon_command<I, T>(args: I) -> DaemonCommand
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let cli = Cli::parse_from(args);
        let Command::Daemon(command) = cli.command else {
            panic!("expected daemon command");
        };
        command
    }

    fn provider_profiles() -> ProviderProfiles {
        ProviderProfiles::from_profiles(HashMap::from([(
            "openai".to_owned(),
            ProviderProfile {
                provider: "openai".to_owned(),
                secrets: BTreeMap::new(),
                base_url: None,
                default_model: Some("gpt-4.1-mini".to_owned()),
                spec: Default::default(),
            },
        )]))
    }

    fn assert_prompt_job_id_format(job_id: &str) {
        assert!(job_id.starts_with(PROMPT_JOB_ID_PREFIX));
        assert_eq!(
            job_id.len(),
            PROMPT_JOB_ID_PREFIX.len() + PROMPT_JOB_ID_BODY_LEN
        );
        assert!(
            job_id[PROMPT_JOB_ID_PREFIX.len()..]
                .chars()
                .all(|character| nanoid::alphabet::SAFE.contains(&character))
        );
    }

    #[test]
    fn resolve_daemon_socket_path_uses_explicit_socket() {
        let path = resolve_daemon_socket_path(Some(Path::new("/tmp/coco.sock"))).unwrap();

        assert_eq!(path, PathBuf::from("/tmp/coco.sock"));
    }

    #[test]
    fn resolve_daemon_command_socket_path_uses_serve_socket() {
        let command = daemon_command(["coco", "daemon", "serve", "--socket", "/tmp/coco.sock"]);

        let path = resolve_daemon_command_socket_path(&command).unwrap();

        assert_eq!(path, PathBuf::from("/tmp/coco.sock"));
    }

    #[tokio::test]
    async fn daemon_profile_graph_builds_snapshot_without_serving() {
        let store = test_store().await;
        let root = store.root_id();
        store.fork("main", &root).await.unwrap();
        let node = store
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("profile graph".to_owned()),
            })
            .await
            .unwrap();
        store.set_branch_head("main", &root, &node).await.unwrap();
        let llm = Arc::new(LlmService::new(
            store.clone(),
            BlockingOnceBackend::default(),
        ));
        let command = daemon_command(["coco", "daemon", "profile", "graph", "--all", "--json"]);

        let output = run_daemon_command(
            command,
            &store,
            &llm,
            &provider_profiles(),
            &ChannelConfigs::default(),
            None,
            PathBuf::from(".coco-store"),
        )
        .await
        .unwrap()
        .unwrap();
        let value: serde_json::Value = serde_json::from_str(&output).unwrap();

        assert_eq!(value["mode"], "all");
        assert_eq!(value["version"], 0);
        assert_eq!(value["branches"], 1);
        assert_eq!(value["nodes"], 1);
    }

    #[test]
    fn daemon_console_config_uses_serve_addr_when_enabled() {
        let command = daemon_command(["coco", "daemon", "serve", "--console-addr", "127.0.0.1:0"]);
        let publisher = ConsolePublisher::new();

        let config = daemon_console_config(&command, Some(&publisher)).unwrap();

        assert_eq!(config.addr, "127.0.0.1:0".parse().unwrap());
    }

    #[test]
    fn daemon_console_config_returns_none_without_publisher() {
        let command = daemon_command(["coco", "daemon", "serve"]);

        let config = daemon_console_config(&command, None);

        assert!(config.is_none());
    }

    #[test]
    fn daemon_console_config_returns_none_when_disabled() {
        let command = daemon_command(["coco", "daemon", "serve", "--no-console"]);
        let publisher = ConsolePublisher::new();

        let config = daemon_console_config(&command, Some(&publisher));

        assert!(config.is_none());
    }

    #[tokio::test]
    async fn run_daemon_command_reports_socket_parent_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let socket_parent = temp_dir.path().join("not-a-directory");
        fs::write(&socket_parent, "").unwrap();
        let socket_path = socket_parent.join("coco.sock");
        let store = test_store().await;
        let llm = Arc::new(LlmService::new(
            store.clone(),
            BlockingOnceBackend::default(),
        ));
        let command = daemon_command([
            OsString::from("coco"),
            OsString::from("daemon"),
            OsString::from("serve"),
            OsString::from("--socket"),
            socket_path.into_os_string(),
        ]);

        let error = run_daemon_command(
            command,
            &store,
            &llm,
            &provider_profiles(),
            &ChannelConfigs::default(),
            None,
            PathBuf::from(".coco-store"),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, crate::Error::BindDaemonSocket { .. }));
        assert!(
            store
                .get_session_state(DEFAULT_SESSION_BRANCH)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn daemon_server_handle_wait_returns_when_socket_task_finishes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let socket_path = temp_dir.path().join("coco.sock");
        fs::write(&socket_path, "").unwrap();
        let store = test_store().await;
        let llm = Arc::new(LlmService::new(
            store.clone(),
            BlockingOnceBackend::default(),
        ));
        let handle = CocoCliDaemonServerHandle {
            socket_path: socket_path.clone(),
            llm,
            socket_task: tokio::spawn(async {}),
            channel_task: None,
            message_queue_task: tokio::spawn(async {
                std::future::pending::<crate::Result<()>>().await
            }),
            chatgpt_auth_check_task: None,
            console: None,
        };

        handle.wait().await.unwrap();

        assert!(!socket_path.exists());
    }

    #[tokio::test]
    async fn chatgpt_auth_check_task_skips_empty_config() {
        let llm = Arc::new(LlmService::new(
            test_store().await,
            BlockingOnceBackend::default(),
        ));

        assert!(start_chatgpt_auth_check_task(llm).is_none());
    }

    #[tokio::test]
    async fn abort_channel_task_ignores_absent_task() {
        abort_channel_task(None).await.unwrap();
    }

    #[tokio::test]
    async fn abort_channel_task_accepts_completed_success() {
        let task = tokio::spawn(async { Ok(()) });
        wait_until(Duration::from_secs(1), || task.is_finished()).await;

        abort_channel_task(Some(task)).await.unwrap();
    }

    #[tokio::test]
    async fn abort_channel_task_preserves_completed_error() {
        let task = tokio::spawn(async { Err(crate::Error::EmptyPrompt) });
        wait_until(Duration::from_secs(1), || task.is_finished()).await;

        let error = abort_channel_task(Some(task)).await.unwrap_err();

        assert!(matches!(error, crate::Error::EmptyPrompt));
    }

    #[tokio::test]
    async fn abort_channel_task_accepts_cancelled_task() {
        let task = tokio::spawn(async { std::future::pending::<crate::Result<()>>().await });

        abort_channel_task(Some(task)).await.unwrap();
    }

    #[tokio::test]
    async fn abort_channel_task_reports_join_error() {
        let task = tokio::spawn(async {
            panic!("channel task panic");
            #[allow(unreachable_code)]
            Ok::<(), crate::Error>(())
        });
        wait_until(Duration::from_secs(1), || task.is_finished()).await;

        let error = abort_channel_task(Some(task)).await.unwrap_err();

        assert!(matches!(error, crate::Error::JoinChannelTask { .. }));
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

    #[test]
    fn telegram_queue_payload_round_trips_voice_attachments() {
        let message = InboundMessage::telegram_with_message_id_and_attachments(
            "chat-1",
            "sender-1",
            "message-1",
            "",
            vec![],
            vec![TelegramVoiceAttachment::from_parts(
                "voice-file-id",
                Some("voice-unique-id".to_owned()),
                Some(12),
                Some("audio/ogg".to_owned()),
                Some(50_000),
            )],
        );

        let decoded = decode_telegram_message(encode_telegram_message(&message)).unwrap();

        assert_eq!(decoded, message);
    }

    #[tokio::test]
    async fn telegram_message_queue_publisher_persists_inbound_message() {
        let store = test_store().await;
        let publisher = TelegramMessageQueuePublisher::new(store.clone(), Arc::new(Notify::new()));
        let message =
            InboundMessage::telegram_with_message_id("chat-1", "sender-1", "message-1", "hello");

        let response = publisher.handle(message.clone()).await.unwrap();

        assert_eq!(response.text, "queued telegram inbound message");
        let items = store
            .list_queue_messages(TELEGRAM_INBOUND_QUEUE)
            .await
            .unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(
            decode_telegram_message(items[0].payload.clone()).unwrap(),
            message
        );
    }

    #[tokio::test]
    async fn telegram_queue_worker_returns_after_message_processing_finishes() {
        let store = test_store().await;
        let backend = BlockingOnceBackend::default();
        let calls = backend.calls.clone();
        let release_first = backend.release_first.clone();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("main")).await.unwrap();
        let message = InboundMessage::telegram("chat", "user", "first");
        store
            .enqueue_message(TELEGRAM_INBOUND_QUEUE, encode_telegram_message(&message))
            .await
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
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn telegram_queue_worker_waits_for_active_target_branch_job() {
        let store = test_store().await;
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
            .await
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
    async fn prompt_job_queue_worker_joins_inflight_active_branch_job() {
        let store = test_store().await;
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
            .await
            .unwrap();
        let worker = PromptJobMessageQueueWorker::new(
            PROMPT_JOB_QUEUE.to_owned(),
            store.clone(),
            engine.clone(),
        );
        let item = store.peek_message(PROMPT_JOB_QUEUE).await.unwrap().unwrap();
        let request = super::decode_prompt_job_message(item.payload.clone()).unwrap();

        let join_task = tokio::spawn(async move {
            worker
                .handle_prompt_request_queue_head(&item, &request)
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!join_task.is_finished());
        assert!(engine.get_job("job-request").await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store
                .list_queue_messages(PROMPT_JOB_QUEUE)
                .await
                .unwrap()
                .len(),
            1
        );

        release_first.notify_waiters();
        let snapshot = drive_task.await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Finished);
        assert!(matches!(
            join_task.await.unwrap().unwrap(),
            super::PromptQueueHeadResult::Continue
        ));
        assert_eq!(
            engine.get_job(&active_job_id).await.unwrap().status,
            JobStatus::Finished
        );
    }

    #[tokio::test]
    async fn prompt_job_queue_worker_exits_after_draining_branch_queue() {
        let store = test_store().await;
        let backend = BlockingOnceBackend::default();
        let calls = backend.calls.clone();
        let release_first = backend.release_first.clone();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("day")).await.unwrap();
        let engine = ConversationEngine::new(llm);
        let queue = prompt_job_queue_for_branch("day");
        store
            .enqueue_message(
                &queue,
                json!(QueuedPromptRequest {
                    job_id: "job-day-queued".to_owned(),
                    branch: "day".to_owned(),
                    prompt: "queued work".to_owned(),
                    merge_parents: vec![],
                    session_patch: None,
                }),
            )
            .await
            .unwrap();
        let worker = PromptJobMessageQueueWorker::new(queue.clone(), store.clone(), engine.clone());

        let worker_task = tokio::spawn(async move { worker.run().await });
        wait_until(Duration::from_secs(1), || calls.load(Ordering::SeqCst) == 1).await;
        release_first.notify_waiters();
        tokio::time::timeout(Duration::from_secs(1), worker_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(store.list_queue_messages(&queue).await.unwrap().is_empty());
        assert_eq!(
            engine.join_job("job-day-queued").await.unwrap().status,
            JobStatus::Finished
        );
    }

    #[tokio::test]
    async fn prompt_job_queue_worker_backs_off_when_active_job_waits_for_recovery() {
        let store = test_store().await;
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
            .await
            .unwrap();
        let worker = PromptJobMessageQueueWorker::new(
            PROMPT_JOB_QUEUE.to_owned(),
            store.clone(),
            engine.clone(),
        );

        let result = tokio::time::timeout(Duration::from_millis(100), worker.drain_once()).await;

        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            engine.get_job(&active_job_id).await.unwrap().status,
            JobStatus::Running
        );
        assert_eq!(
            store
                .list_queue_messages(PROMPT_JOB_QUEUE)
                .await
                .unwrap()
                .len(),
            1
        );
        let failure_count = store
            .list_children(&active_job.base)
            .await
            .unwrap()
            .into_iter()
            .filter(|node| matches!(node.kind, Kind::Failure(_)))
            .count();
        assert_eq!(failure_count, 1);
    }

    #[tokio::test]
    async fn prompt_job_queue_worker_does_not_start_idle_active_job() {
        let store = test_store().await;
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
            .await
            .unwrap();
        let worker = PromptJobMessageQueueWorker::new(
            PROMPT_JOB_QUEUE.to_owned(),
            store.clone(),
            engine.clone(),
        );

        let item = store.peek_message(PROMPT_JOB_QUEUE).await.unwrap().unwrap();
        assert!(matches!(
            worker
                .handle_prompt_request_queue_head(&item, &request)
                .await
                .unwrap(),
            super::PromptQueueHeadResult::Wait(duration)
                if duration == super::ACTIVE_JOB_RECHECK_INTERVAL
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            engine.get_job(&active_job_id).await.unwrap().status,
            JobStatus::Queued
        );

        let item = store.peek_message(PROMPT_JOB_QUEUE).await.unwrap().unwrap();
        assert!(matches!(
            worker
                .handle_prompt_request_queue_head(&item, &request)
                .await
                .unwrap(),
            super::PromptQueueHeadResult::Wait(duration)
                if duration <= super::ACTIVE_JOB_RECHECK_INTERVAL
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            store
                .list_queue_messages(PROMPT_JOB_QUEUE)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn prompt_job_queue_worker_processes_branch_queue_when_legacy_head_waits() {
        let store = test_store().await;
        let backend = BlockingOnceBackend::default();
        let calls = backend.calls.clone();
        let release_first = backend.release_first.clone();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("main")).await.unwrap();
        llm.create_session(session_config("day")).await.unwrap();
        let engine = ConversationEngine::new(llm);
        let active_job = store
            .submit_job("main", &store.get_branch_head("main").await.unwrap())
            .await
            .unwrap();
        store
            .set_job_status(&active_job.job_id, JobStatus::Queued, JobStatus::Running)
            .await
            .unwrap();
        store
            .enqueue_message(
                PROMPT_JOB_QUEUE,
                json!(QueuedPromptRequest {
                    job_id: "job-main-queued".to_owned(),
                    branch: "main".to_owned(),
                    prompt: "queued main work".to_owned(),
                    merge_parents: vec![],
                    session_patch: None,
                }),
            )
            .await
            .unwrap();
        super::queue_prompt_job_request(
            &store,
            QueuedPromptRequest {
                job_id: "job-day-queued".to_owned(),
                branch: "day".to_owned(),
                prompt: "queued day work".to_owned(),
                merge_parents: vec![],
                session_patch: None,
            },
        )
        .await
        .unwrap();
        let legacy_worker = PromptJobMessageQueueWorker::new(
            PROMPT_JOB_QUEUE.to_owned(),
            store.clone(),
            engine.clone(),
        );
        let day_worker = PromptJobMessageQueueWorker::new(
            prompt_job_queue_for_branch("day"),
            store.clone(),
            engine.clone(),
        );

        let legacy_task = tokio::spawn(async move { legacy_worker.drain_once().await });
        let day_task = tokio::spawn(async move { day_worker.drain_once().await });
        wait_until(Duration::from_secs(1), || calls.load(Ordering::SeqCst) == 1).await;
        assert_eq!(
            store
                .list_queue_messages(PROMPT_JOB_QUEUE)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            engine.get_job("job-day-queued").await.unwrap().status,
            JobStatus::Running
        );

        release_first.notify_waiters();
        day_task.await.unwrap().unwrap();
        assert!(!legacy_task.is_finished());
        legacy_task.abort();
        wait_until_async(Duration::from_secs(1), || async {
            engine
                .get_job("job-day-queued")
                .await
                .is_ok_and(|job| job.status == JobStatus::Finished)
        })
        .await;
    }

    #[tokio::test]
    async fn prompt_job_queue_worker_processes_branch_queue_when_legacy_head_is_inflight() {
        let store = test_store().await;
        let backend = BlockingOnceBackend::default();
        let calls = backend.calls.clone();
        let release_first = backend.release_first.clone();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("main")).await.unwrap();
        llm.create_session(session_config("day")).await.unwrap();
        let engine = ConversationEngine::new(llm);
        let active_job = engine
            .submit_job("main", "active main work", vec![])
            .await
            .unwrap();
        let active_job_id = active_job.job_id.clone();
        let drive_engine = engine.clone();
        let drive_task = tokio::spawn(async move { drive_engine.drive_job(&active_job_id).await });
        wait_until(Duration::from_secs(1), || calls.load(Ordering::SeqCst) == 1).await;

        store
            .enqueue_message(
                PROMPT_JOB_QUEUE,
                json!(QueuedPromptRequest {
                    job_id: "job-main-queued".to_owned(),
                    branch: "main".to_owned(),
                    prompt: "queued main work".to_owned(),
                    merge_parents: vec![],
                    session_patch: None,
                }),
            )
            .await
            .unwrap();
        super::queue_prompt_job_request(
            &store,
            QueuedPromptRequest {
                job_id: "job-day-queued".to_owned(),
                branch: "day".to_owned(),
                prompt: "queued day work".to_owned(),
                merge_parents: vec![],
                session_patch: None,
            },
        )
        .await
        .unwrap();
        let legacy_worker = PromptJobMessageQueueWorker::new(
            PROMPT_JOB_QUEUE.to_owned(),
            store.clone(),
            engine.clone(),
        );
        let day_worker = PromptJobMessageQueueWorker::new(
            prompt_job_queue_for_branch("day"),
            store.clone(),
            engine.clone(),
        );

        let legacy_task = tokio::spawn(async move { legacy_worker.drain_once().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!legacy_task.is_finished());
        day_worker.drain_once().await.unwrap();

        assert!(engine.get_job("job-main-queued").await.is_err());
        assert_eq!(
            store
                .list_queue_messages(PROMPT_JOB_QUEUE)
                .await
                .unwrap()
                .len(),
            1
        );
        wait_until(Duration::from_secs(1), || calls.load(Ordering::SeqCst) == 2).await;
        wait_until_async(Duration::from_secs(1), || async {
            engine
                .get_job("job-day-queued")
                .await
                .is_ok_and(|job| job.status == JobStatus::Finished)
        })
        .await;

        release_first.notify_waiters();
        let snapshot = drive_task.await.unwrap().unwrap();
        assert_eq!(snapshot.status, JobStatus::Finished);
        legacy_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn active_job_waiting_for_recovery_detects_failure_child() {
        let store = test_store().await;
        store.fork("main", &store.root_id()).await.unwrap();
        let base = store.get_branch_head("main").await.unwrap();
        let active_job = store.submit_job("main", &base).await.unwrap();
        store
            .append(NewNode {
                parent: base,
                role: Role::System,
                metadata: BackendMetadata::builder().build(),
                kind: Kind::Failure("backend failed".to_owned()),
            })
            .await
            .unwrap();

        assert!(
            super::active_job_is_waiting_for_recovery(&store, &active_job)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn active_job_waiting_for_recovery_detects_terminal_failure() {
        let store = test_store().await;
        store.fork("main", &store.root_id()).await.unwrap();
        let base = store.get_branch_head("main").await.unwrap();
        let active_job = store.submit_job("main", &base).await.unwrap();
        let failure = store
            .append(NewNode {
                parent: base.clone(),
                role: Role::System,
                metadata: BackendMetadata::builder().build(),
                kind: Kind::Failure("backend failed".to_owned()),
            })
            .await
            .unwrap();
        store
            .set_branch_head("main", &base, &failure)
            .await
            .unwrap();

        assert!(
            super::active_job_is_waiting_for_recovery(&store, &active_job)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn active_job_waiting_for_recovery_ignores_clean_job() {
        let store = test_store().await;
        store.fork("main", &store.root_id()).await.unwrap();
        let base = store.get_branch_head("main").await.unwrap();
        let active_job = store.submit_job("main", &base).await.unwrap();

        assert!(
            !super::active_job_is_waiting_for_recovery(&store, &active_job)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn prompt_job_queue_worker_discards_prompt_request_for_missing_branch() {
        let store = test_store().await;
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
            .await
            .unwrap();
        let worker = PromptJobMessageQueueWorker::new(
            PROMPT_JOB_QUEUE.to_owned(),
            store.clone(),
            engine.clone(),
        );

        worker.drain_once().await.unwrap();

        assert!(engine.get_job("job-missing-branch").await.is_err());
        assert_eq!(
            store
                .list_queue_messages(PROMPT_JOB_QUEUE)
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn prompt_job_queue_worker_discards_duplicate_prompt_request() {
        let store = test_store().await;
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
            .await
            .unwrap();
        let worker =
            PromptJobMessageQueueWorker::new(PROMPT_JOB_QUEUE.to_owned(), store.clone(), engine);

        worker.drain_once().await.unwrap();

        assert!(
            store
                .list_queue_messages(PROMPT_JOB_QUEUE)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn system_event_queue_worker_materializes_day_prompt_request() {
        let store = test_store().await;
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
            .await
            .unwrap();
        let worker = SystemEventMessageQueueWorker::new(store.clone());

        assert_eq!(worker.drain_once().await.unwrap(), 1);

        assert!(
            store
                .list_queue_messages(SYSTEM_EVENT_QUEUE)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .list_queue_messages(PROMPT_JOB_QUEUE)
                .await
                .unwrap()
                .is_empty()
        );
        let prompt_items = store
            .list_queue_messages(&prompt_job_queue_for_branch("day"))
            .await
            .unwrap();
        assert_eq!(prompt_items.len(), 1);
        let request: QueuedPromptRequest =
            serde_json::from_value(prompt_items[0].payload.clone()).unwrap();
        assert_eq!(
            request.job_id,
            super::stable_prompt_job_id_from_dedupe_key("llm.backend_failure:error-node")
        );
        assert_prompt_job_id_format(&request.job_id);
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
        let store = test_store().await;
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
            .await
            .unwrap();
        let worker = SystemEventMessageQueueWorker::new(store.clone());

        assert_eq!(worker.drain_once().await.unwrap(), 1);

        let dedupe_key =
            super::backend_failure_recovery_dedupe_key("job-failed", "main", "retry-node");
        let prompt_items = store
            .list_queue_messages(&prompt_job_queue_for_branch("day"))
            .await
            .unwrap();
        assert_eq!(prompt_items.len(), 1);
        let request: QueuedPromptRequest =
            serde_json::from_value(prompt_items[0].payload.clone()).unwrap();
        assert_eq!(
            request.job_id,
            super::stable_prompt_job_id_from_dedupe_key(&dedupe_key)
        );
        assert_prompt_job_id_format(&request.job_id);
    }

    #[tokio::test]
    async fn system_event_queue_worker_skips_duplicate_recovery_prompt_request() {
        let store = test_store().await;
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
            .await
            .unwrap();
        store
            .enqueue_message(SYSTEM_EVENT_QUEUE, payload)
            .await
            .unwrap();
        let worker = SystemEventMessageQueueWorker::new(store.clone());

        assert_eq!(worker.drain_once().await.unwrap(), 2);

        assert!(
            store
                .list_queue_messages(SYSTEM_EVENT_QUEUE)
                .await
                .unwrap()
                .is_empty()
        );
        let prompt_items = store
            .list_queue_messages(&prompt_job_queue_for_branch("day"))
            .await
            .unwrap();
        assert_eq!(prompt_items.len(), 1);
        let request: QueuedPromptRequest =
            serde_json::from_value(prompt_items[0].payload.clone()).unwrap();
        assert_eq!(
            request.job_id,
            super::stable_prompt_job_id_from_dedupe_key(
                "llm.backend_failure:job-failed:main:retry-node"
            )
        );
        assert_prompt_job_id_format(&request.job_id);
    }

    #[tokio::test]
    async fn system_event_queue_worker_queues_day_recovery_behind_blocked_main_queue() {
        let store = test_store().await;
        let backend = BlockingOnceBackend::default();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("main")).await.unwrap();
        llm.create_session(session_config("day")).await.unwrap();
        store
            .enqueue_message(
                PROMPT_JOB_QUEUE,
                json!(QueuedPromptRequest {
                    job_id: "job-main-queued".to_owned(),
                    branch: "main".to_owned(),
                    prompt: "queued main work".to_owned(),
                    merge_parents: vec![],
                    session_patch: None,
                }),
            )
            .await
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
            .await
            .unwrap();
        let worker = SystemEventMessageQueueWorker::new(store.clone());

        assert_eq!(worker.drain_once().await.unwrap(), 1);

        assert_eq!(
            store
                .list_queue_messages(PROMPT_JOB_QUEUE)
                .await
                .unwrap()
                .len(),
            1
        );
        let prompt_items = store
            .list_queue_messages(&prompt_job_queue_for_branch("day"))
            .await
            .unwrap();
        assert_eq!(prompt_items.len(), 1);
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
        assert_prompt_job_id_format(&super::stable_system_event_prompt_job_id(&left));
        assert_prompt_job_id_format(&super::stable_system_event_prompt_job_id(&right));
    }

    #[tokio::test]
    async fn system_event_queue_worker_skips_recovery_prompt_when_job_exists() {
        let store = test_store().await;
        let backend = BlockingOnceBackend::default();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("day")).await.unwrap();
        store
            .submit_job_with_id(
                &super::stable_prompt_job_id_from_dedupe_key(
                    "llm.backend_failure:job-failed:main:retry-node",
                ),
                "day",
                &store.get_branch_head("day").await.unwrap(),
            )
            .await
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
            .await
            .unwrap();
        let worker = SystemEventMessageQueueWorker::new(store.clone());

        assert_eq!(worker.drain_once().await.unwrap(), 1);

        assert!(
            store
                .list_queue_messages(SYSTEM_EVENT_QUEUE)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .list_queue_messages(PROMPT_JOB_QUEUE)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn system_event_queue_worker_skips_recovery_prompt_when_legacy_job_exists() {
        let store = test_store().await;
        let backend = BlockingOnceBackend::default();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("day")).await.unwrap();
        store
            .submit_job_with_id(
                &super::legacy_prompt_job_id_from_dedupe_key(
                    "llm.backend_failure:job-failed:main:retry-node",
                ),
                "day",
                &store.get_branch_head("day").await.unwrap(),
            )
            .await
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
            .await
            .unwrap();
        let worker = SystemEventMessageQueueWorker::new(store.clone());

        assert_eq!(worker.drain_once().await.unwrap(), 1);

        assert!(
            store
                .list_queue_messages(SYSTEM_EVENT_QUEUE)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .list_queue_messages(PROMPT_JOB_QUEUE)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn system_event_queue_worker_skips_recovery_prompt_when_legacy_request_exists() {
        let store = test_store().await;
        let backend = BlockingOnceBackend::default();
        let llm = Arc::new(LlmService::new(store.clone(), backend));
        llm.create_session(session_config("day")).await.unwrap();
        super::queue_prompt_job_request(
            &store,
            QueuedPromptRequest {
                job_id: super::legacy_prompt_job_id_from_dedupe_key(
                    "llm.backend_failure:job-failed:main:retry-node",
                ),
                branch: "day".to_owned(),
                prompt: "recover".to_owned(),
                merge_parents: vec![],
                session_patch: None,
            },
        )
        .await
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
            .await
            .unwrap();
        let worker = SystemEventMessageQueueWorker::new(store.clone());

        assert_eq!(worker.drain_once().await.unwrap(), 1);

        assert!(
            store
                .list_queue_messages(SYSTEM_EVENT_QUEUE)
                .await
                .unwrap()
                .is_empty()
        );
        let prompt_items = store
            .list_queue_messages(&prompt_job_queue_for_branch("day"))
            .await
            .unwrap();
        assert_eq!(prompt_items.len(), 1);
        let request: QueuedPromptRequest =
            serde_json::from_value(prompt_items[0].payload.clone()).unwrap();
        assert_eq!(
            request.job_id,
            super::legacy_prompt_job_id_from_dedupe_key(
                "llm.backend_failure:job-failed:main:retry-node"
            )
        );
    }

    #[tokio::test]
    async fn derive_day_session_config_uses_latest_session_anchor() {
        let store = test_store().await;
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
                "day handoff",
            )
            .await
            .unwrap();

        let config = super::derive_day_session_config(&store)
            .await
            .unwrap()
            .expect("day config should be derived");

        assert_eq!(config.model, "new-model");
        assert_eq!(config.max_tokens, Some(12_345));
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

    async fn wait_until_async<F, Fut>(timeout: Duration, mut condition: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if condition().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        panic!("condition was not met before timeout");
    }
}
