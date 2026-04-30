use std::path::{Path, PathBuf};
use std::sync::Arc;

use coco_console::{ConsoleConfig, ConsolePublisher, ConsoleServerHandle, start_console_server};
use coco_core::ConversationEngine;
use coco_llm::{CocoCliRuntimeRequest, CocoCliRuntimeResponse, CompletionBackend, LlmService};
use coco_mem::{SessionRole, Store};
use snafu::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

use super::{
    config::ProviderProfiles,
    run_forwarded_with_services,
    runtime::{ForwardedRuntimeInputs, RuntimeServices},
};
use crate::{
    Result,
    cli::{DaemonCommand, DaemonSubcommand},
    error::{
        BindDaemonSocketSnafu, ConsoleSnafu, JoinDaemonServerSnafu, ResolveDaemonSocketRootSnafu,
    },
};

#[derive(Debug)]
pub(crate) struct CocoCliDaemonServerHandle {
    socket_path: PathBuf,
    socket_task: tokio::task::JoinHandle<()>,
    console: Option<ConsoleServerHandle>,
}

pub(super) async fn run_daemon_command<B, S>(
    command: DaemonCommand,
    shared_store: &S,
    llm: &Arc<LlmService<B, S>>,
    provider_profiles: &ProviderProfiles,
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
            console_config,
            console_publisher,
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
            eprintln!("failed to resume incomplete prompt jobs on daemon startup: {error}");
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
    console_config: Option<ConsoleConfig>,
    console_publisher: Option<ConsolePublisher>,
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
    let console = match console_config {
        Some(config) => Some(
            start_console_server(
                config,
                shared_store.clone(),
                console_publisher.expect("console publisher should exist when console is enabled"),
            )
            .context(ConsoleSnafu)?,
        ),
        None => None,
    };
    if let Some(console) = &console {
        eprintln!("coco console listening on http://{}", console.addr());
    }

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
        console,
    })
}

impl CocoCliDaemonServerHandle {
    pub(crate) async fn wait(self) -> Result<()> {
        let Self {
            socket_path,
            socket_task,
            console,
        } = self;

        match console {
            Some(mut console) => {
                tokio::select! {
                    socket_result = socket_task => {
                        cleanup_socket(&socket_path);
                        console.shutdown().await.context(ConsoleSnafu)?;
                        socket_result.context(JoinDaemonServerSnafu).map(|_| ())
                    }
                    console_result = console.wait_mut() => {
                        cleanup_socket(&socket_path);
                        console_result.context(ConsoleSnafu)
                    }
                }
            }
            None => {
                let result = socket_task.await.context(JoinDaemonServerSnafu);
                cleanup_socket(&socket_path);
                result.map(|_| ())
            }
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn shutdown(self) -> Result<()> {
        self.socket_task.abort();
        let result = self.socket_task.await;
        if let Some(console) = self.console {
            console.shutdown().await.context(ConsoleSnafu)?;
        }
        cleanup_socket(&self.socket_path);
        match result {
            Ok(()) => Ok(()),
            Err(source) if source.is_cancelled() => Ok(()),
            Err(source) => Err(source).context(JoinDaemonServerSnafu),
        }
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
