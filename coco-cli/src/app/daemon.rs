use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use coco_channel::{ChannelRuntime, InboundMessage, MessageHandler, OutboundMessage};
use coco_channel::{Error as ChannelError, telegram::TelegramChannel};
use coco_console::{ConsoleConfig, ConsolePublisher, ConsoleServerHandle, start_console_server};
use coco_core::{ConversationEngine, CoreService, FixedBranchResolver};
use coco_llm::{CocoCliRuntimeRequest, CocoCliRuntimeResponse, CompletionBackend, LlmService};
use coco_mem::{SessionRole, Store};
use snafu::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

use super::{
    config::{ChannelConfigs, ProviderProfiles, resolve_channel_secret},
    run_forwarded_with_services,
    runtime::{ForwardedRuntimeInputs, RuntimeServices},
};
use crate::{
    Result,
    cli::{DaemonCommand, DaemonSubcommand},
    error::{
        BindDaemonSocketSnafu, ChannelSnafu, ConsoleSnafu, JoinChannelTaskSnafu,
        JoinDaemonServerSnafu, ResolveDaemonSocketRootSnafu,
    },
};

#[derive(Debug)]
pub(crate) struct CocoCliDaemonServerHandle {
    socket_path: PathBuf,
    socket_task: tokio::task::JoinHandle<()>,
    channel_task: Option<tokio::task::JoinHandle<Result<()>>>,
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
        DaemonSubcommand::Serve(_) => start_daemon_server(
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
        )?,
    };
    spawn_resume_incomplete_jobs(shared_engine);
    server.wait().await?;
    Ok(None)
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
) -> Result<CocoCliDaemonServerHandle>
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
    let channel_task = start_channel_task(options.channel_configs, &shared_engine)?;

    let socket_task = tokio::spawn(async move {
        loop {
            let accepted = listener.accept().await;
            let Ok((mut stream, _)) = accepted else {
                break;
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
                let _ = stream.write_all(&payload).await;
            });
        }
    });

    Ok(CocoCliDaemonServerHandle {
        socket_path,
        socket_task,
        channel_task,
        console,
    })
}

fn start_channel_task<B, S>(
    channel_configs: &ChannelConfigs,
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
    let channel = TelegramChannel::from_config(coco_channel::telegram::TelegramChannelConfig {
        token,
        poll_timeout_secs: config.poll_timeout_secs,
        allowed_chat_ids: config.allowed_chat_ids.clone(),
    })
    .context(ChannelSnafu)?;
    let handler =
        CoreChannelMessageHandler::new(config.branch.clone(), shared_engine.as_ref().clone());

    Ok(Some(tokio::spawn(async move {
        channel.run(&handler).await.context(ChannelSnafu)
    })))
}

struct CoreChannelMessageHandler<B, S> {
    service: CoreService<FixedBranchResolver, B, S>,
}

impl<B, S> CoreChannelMessageHandler<B, S> {
    fn new(branch: impl Into<String>, engine: ConversationEngine<B, S>) -> Self {
        Self {
            service: CoreService::new(FixedBranchResolver::new(branch), engine),
        }
    }
}

#[async_trait]
impl<B, S> MessageHandler for CoreChannelMessageHandler<B, S>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    async fn handle(&self, message: InboundMessage) -> coco_channel::Result<OutboundMessage> {
        self.service
            .handle_message(message)
            .await
            .map_err(ChannelError::handler)
    }
}

impl CocoCliDaemonServerHandle {
    pub(crate) async fn wait(self) -> Result<()> {
        let Self {
            socket_path,
            socket_task,
            channel_task,
            console,
        } = self;

        wait_daemon_tasks(socket_path, socket_task, console, channel_task).await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn shutdown(self) -> Result<()> {
        self.socket_task.abort();
        if let Some(channel_task) = &self.channel_task {
            channel_task.abort();
        }
        let socket_result = self.socket_task.await;
        if let Some(channel_task) = self.channel_task {
            match channel_task.await {
                Ok(result) => result?,
                Err(source) if source.is_cancelled() => {}
                Err(source) => return Err(source).context(JoinChannelTaskSnafu),
            }
        }
        if let Some(console) = self.console {
            console.shutdown().await.context(ConsoleSnafu)?;
        }
        cleanup_socket(&self.socket_path);
        match socket_result {
            Ok(()) => Ok(()),
            Err(source) if source.is_cancelled() => Ok(()),
            Err(source) => Err(source).context(JoinDaemonServerSnafu),
        }
    }
}

async fn wait_daemon_tasks(
    socket_path: PathBuf,
    socket_task: tokio::task::JoinHandle<()>,
    mut console: Option<ConsoleServerHandle>,
    mut channel_task: Option<tokio::task::JoinHandle<Result<()>>>,
) -> Result<()> {
    tokio::select! {
        socket_result = socket_task => {
            shutdown_console(console).await?;
            abort_channel_task(channel_task).await?;
            cleanup_socket(&socket_path);
            socket_result.context(JoinDaemonServerSnafu).map(|_| ())
        }
        console_result = async { console.as_mut().expect("console task should exist").wait_mut().await }, if console.is_some() => {
            abort_channel_task(channel_task).await?;
            cleanup_socket(&socket_path);
            console_result.context(ConsoleSnafu)
        }
        channel_result = async { channel_task.as_mut().expect("channel task should exist").await }, if channel_task.is_some() => {
            shutdown_console(console).await?;
            cleanup_socket(&socket_path);
            channel_result.context(JoinChannelTaskSnafu)??;
            Ok(())
        }
        else => {
            cleanup_socket(&socket_path);
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
                runtime_error_response(2, format!("invalid coco-cli daemon request: {error}"))
            }
        },
        Err(error) => runtime_error_response(
            2,
            format!("failed to read coco-cli daemon request: {error}"),
        ),
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

    use super::resolve_daemon_socket_path;

    #[test]
    fn resolve_daemon_socket_path_uses_explicit_socket() {
        let path = resolve_daemon_socket_path(Some(Path::new("/tmp/coco.sock"))).unwrap();

        assert_eq!(path, PathBuf::from("/tmp/coco.sock"));
    }
}
