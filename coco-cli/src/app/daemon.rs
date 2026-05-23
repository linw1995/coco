use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use coco_channel::{
    ChannelRuntime, InboundMessage, MessageHandler, OutboundMessage, TelegramImageAttachment,
};
use coco_channel::{Error as ChannelError, telegram::TelegramChannel};
use coco_console::{ConsoleConfig, ConsolePublisher, ConsoleServerHandle, start_console_server};
use coco_core::{ConversationEngine, CoreService, EngineError, FixedBranchResolver};
use coco_llm::{CocoCliRuntimeRequest, CocoCliRuntimeResponse, CompletionBackend, LlmService};
use coco_mem::{JobStatus, MessageQueueItem, SessionRole, Store};
use serde::{Deserialize, Serialize};
use serde_json::json;
use snafu::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::Notify;

use super::{
    config::{ChannelConfigs, ProviderProfiles, resolve_channel_secret},
    prompt::{PROMPT_JOB_QUEUE, QueuedPromptRequest},
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
        StoreSnafu,
    },
};

const DEFAULT_SESSION_BRANCH: &str = "main";
const DEFAULT_SYSTEM_PROMPT: &str = "You are CoCo. An AI copilot";
const DEFAULT_MAX_TOKENS: u64 = 32_000;
const TELEGRAM_INBOUND_QUEUE: &str = "telegram.inbound";
const PROMPT_JOB_QUEUE_IDLE_DELAY: Duration = Duration::from_secs(1);
const TELEGRAM_QUEUE_IDLE_DELAY: Duration = Duration::from_secs(1);
const CHANNEL_BRANCH_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(250);

pub(crate) struct CocoCliDaemonServerHandle<B, S> {
    socket_path: PathBuf,
    llm: Arc<LlmService<B, S>>,
    socket_task: tokio::task::JoinHandle<()>,
    channel_task: Option<tokio::task::JoinHandle<Result<()>>>,
    message_queue_task: tokio::task::JoinHandle<Result<()>>,
    console: Option<ConsoleServerHandle>,
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
    if !shared_store
        .list_session_states()
        .context(StoreSnafu)?
        .is_empty()
    {
        return Ok(());
    }

    let config = resolve_session_config(default_session_create_command(), provider_profiles)?;
    tracing::info!(
        branch = %config.branch,
        max_tokens = config.max_tokens,
        tool_count = config.tools.len(),
        "creating default session on empty store"
    );
    llm.create_session(config).await.context(LlmSnafu)?;
    Ok(())
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
        console,
    })
}

fn start_message_queue_task<B, S>(
    shared_store: &S,
    shared_engine: &Arc<ConversationEngine<B, S>>,
) -> tokio::task::JoinHandle<Result<()>>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    let worker =
        PromptJobMessageQueueWorker::new(shared_store.clone(), shared_engine.as_ref().clone());
    tokio::spawn(async move { worker.run().await })
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

struct PromptJobMessageQueueWorker<B, S> {
    store: S,
    engine: ConversationEngine<B, S>,
}

impl<B, S> PromptJobMessageQueueWorker<B, S> {
    fn new(store: S, engine: ConversationEngine<B, S>) -> Self {
        Self { store, engine }
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
                    if !self
                        .handle_prompt_request_queue_head(&item, &request)
                        .await?
                    {
                        tokio::time::sleep(PROMPT_JOB_QUEUE_IDLE_DELAY).await;
                        return Ok(());
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
    ) -> Result<bool>
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
                return Ok(true);
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
                return Ok(true);
            }
            Err(coco_mem::StoreError::PromptJobNotFound { .. }) => {}
            Err(error) => return Err(error).context(StoreSnafu),
        }

        if let Some(active_job) = self
            .engine
            .active_branch_prompt_job(&request.branch)
            .context(CoreEngineSnafu)?
        {
            self.wait_for_active_branch_job(item, request, active_job)
                .await;
            return Ok(true);
        }

        let guard = self.engine.lock_branch(&request.branch).await;
        let Some(item) = self.peek_prompt_queue_head()? else {
            return Ok(true);
        };
        let request = match decode_prompt_job_message(item.payload.clone()) {
            Ok(request) => request,
            Err(_) => {
                let Some(item) = self
                    .store
                    .dequeue_message(PROMPT_JOB_QUEUE)
                    .context(StoreSnafu)?
                else {
                    return Ok(true);
                };
                self.handle_item(item).await;
                return Ok(true);
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
                return Ok(true);
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
                return Ok(true);
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
            self.wait_for_active_branch_job(&item, &request, active_job)
                .await;
            return Ok(true);
        }
        let Some(item) = self
            .store
            .dequeue_message(PROMPT_JOB_QUEUE)
            .context(StoreSnafu)?
        else {
            return Ok(true);
        };
        self.handle_item(item).await;
        Ok(true)
    }

    async fn wait_for_active_branch_job(
        &self,
        item: &MessageQueueItem,
        request: &QueuedPromptRequest,
        active_job: coco_mem::Job,
    ) where
        B: CompletionBackend + 'static,
        S: Store + Clone + Send + Sync + 'static,
    {
        tracing::info!(
            message_id = %item.message_id,
            queue = PROMPT_JOB_QUEUE,
            branch = %request.branch,
            job_id = %request.job_id,
            active_job_id = %active_job.job_id,
            active_job_status = ?active_job.status,
            "waiting to materialize queued prompt job request behind active branch job"
        );

        if let Err(error) = self.engine.drive_job(&active_job.job_id).await {
            tracing::warn!(
                message_id = %item.message_id,
                queue = PROMPT_JOB_QUEUE,
                branch = %request.branch,
                job_id = %request.job_id,
                active_job_id = %active_job.job_id,
                error = %error,
                "failed to wait for active branch prompt job before materializing queued request"
            );
        }
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

        if let Err(error) =
            wait_for_branch_to_accept_channel_prompt(&self.engine, &self.branch).await
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
) -> std::result::Result<(), coco_core::EngineError>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    loop {
        let active_job = engine.active_branch_prompt_job(branch)?;
        let Some(active_job) = active_job else {
            return Ok(());
        };

        tracing::info!(
            branch = %branch,
            active_job_id = %active_job.job_id,
            active_job_status = ?active_job.status,
            "waiting for active branch prompt job before handling queued telegram message"
        );

        let snapshot = engine.drive_job(&active_job.job_id).await?;
        if !matches!(snapshot.status, JobStatus::Finished) {
            tokio::time::sleep(CHANNEL_BRANCH_WAIT_POLL_INTERVAL).await;
        }
    }
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
            socket_task,
            channel_task,
            message_queue_task,
            console,
        } = self;

        wait_daemon_tasks(
            socket_path,
            llm,
            socket_task,
            console,
            channel_task,
            message_queue_task,
        )
        .await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn shutdown(self) -> Result<()> {
        self.socket_task.abort();
        if let Some(channel_task) = &self.channel_task {
            channel_task.abort();
        }
        self.message_queue_task.abort();
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
) -> Result<()> {
    tokio::select! {
        socket_result = socket_task => {
            shutdown_console(console).await?;
            abort_channel_task(channel_task).await?;
            abort_message_queue_task(message_queue_task).await?;
            llm.cleanup_runtime_processes().await;
            cleanup_socket(&socket_path);
            socket_result.context(JoinDaemonServerSnafu).map(|_| ())
        }
        console_result = async { console.as_mut().expect("console task should exist").wait_mut().await }, if console.is_some() => {
            abort_channel_task(channel_task).await?;
            abort_message_queue_task(message_queue_task).await?;
            llm.cleanup_runtime_processes().await;
            cleanup_socket(&socket_path);
            console_result.context(ConsoleSnafu)
        }
        channel_result = async { channel_task.as_mut().expect("channel task should exist").await }, if channel_task.is_some() => {
            shutdown_console(console).await?;
            abort_message_queue_task(message_queue_task).await?;
            llm.cleanup_runtime_processes().await;
            cleanup_socket(&socket_path);
            channel_result.context(JoinChannelTaskSnafu)??;
            Ok(())
        }
        message_queue_result = &mut message_queue_task => {
            shutdown_console(console).await?;
            abort_channel_task(channel_task).await?;
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
    use coco_mem::{JobStatus, MemoryStore, MessageQueueStore, SessionRole};
    use serde_json::json;
    use tokio::sync::Notify;

    use crate::app::prompt::QueuedPromptRequest;

    use super::{
        PROMPT_JOB_QUEUE, PromptJobMessageQueueWorker, TELEGRAM_INBOUND_QUEUE,
        TelegramMessageQueuePublisher, TelegramMessageQueueWorker, decode_telegram_message,
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
        wait_until(Duration::from_secs(1), || calls.load(Ordering::SeqCst) == 1).await;
        assert!(engine.get_job("job-request").is_err());
        assert_eq!(
            store.list_queue_messages(PROMPT_JOB_QUEUE).unwrap().len(),
            1
        );
        assert!(!drain_task.is_finished());

        release_first.notify_waiters();
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

    #[derive(Debug, Clone, Default)]
    struct BlockingOnceBackend {
        calls: Arc<AtomicUsize>,
        release_first: Arc<Notify>,
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
