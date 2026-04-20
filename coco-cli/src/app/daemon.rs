use std::path::{Path, PathBuf};
use std::sync::Arc;

use coco_llm::{CocoCliRuntimeRequest, CocoCliRuntimeResponse, CompletionBackend, LlmService};
use coco_mem::{FsStore, SessionRole};
use snafu::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

use super::run_forwarded_with_services;
use crate::{
    Result,
    cli::{DaemonCommand, DaemonSubcommand},
    error::{BindDaemonSocketSnafu, JoinDaemonServerSnafu, ResolveDaemonSocketRootSnafu},
};

#[derive(Debug)]
pub(crate) struct CocoCliDaemonServerHandle {
    socket_path: PathBuf,
    task: tokio::task::JoinHandle<()>,
}

pub(super) async fn run_daemon_command<B>(
    command: DaemonCommand,
    shared_store: &FsStore,
    llm: &Arc<LlmService<B, FsStore>>,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
{
    let socket_path = resolve_daemon_socket_path(match &command.command {
        DaemonSubcommand::Serve(command) => command.socket.as_deref(),
    })?;
    let server = match command.command {
        DaemonSubcommand::Serve(_) => start_daemon_server(&socket_path, shared_store, llm)?,
    };
    server.wait().await?;
    Ok(None)
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

pub(crate) fn start_daemon_server<B>(
    socket_path: &Path,
    shared_store: &FsStore,
    llm: &Arc<LlmService<B, FsStore>>,
) -> Result<CocoCliDaemonServerHandle>
where
    B: CompletionBackend + 'static,
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
    let task = tokio::spawn(async move {
        loop {
            let accepted = listener.accept().await;
            let Ok((mut stream, _)) = accepted else {
                break;
            };
            let shared_store = shared_store.clone();
            let llm = llm.clone();
            tokio::spawn(async move {
                let response = handle_client(&mut stream, &shared_store, &llm).await;
                let payload = encode_runtime_response(&response);
                let _ = stream.write_all(&payload).await;
            });
        }
    });

    Ok(CocoCliDaemonServerHandle { socket_path, task })
}

impl CocoCliDaemonServerHandle {
    pub(crate) async fn wait(self) -> Result<()> {
        let result = self.task.await.context(JoinDaemonServerSnafu);
        cleanup_socket(&self.socket_path);
        result.map(|_| ())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn shutdown(self) -> Result<()> {
        self.task.abort();
        let result = self.task.await;
        cleanup_socket(&self.socket_path);
        match result {
            Ok(()) => Ok(()),
            Err(source) if source.is_cancelled() => Ok(()),
            Err(source) => Err(source).context(JoinDaemonServerSnafu),
        }
    }
}

async fn handle_client<B>(
    stream: &mut tokio::net::UnixStream,
    shared_store: &FsStore,
    llm: &Arc<LlmService<B, FsStore>>,
) -> CocoCliRuntimeResponse
where
    B: CompletionBackend + 'static,
{
    let mut input = Vec::new();
    match stream.read_to_end(&mut input).await {
        Ok(_) => match serde_json::from_slice::<CocoCliRuntimeRequest>(&input) {
            Ok(request) => {
                run_forwarded_with_services(
                    &request.args,
                    &request.stdin,
                    request.branch_env.as_deref(),
                    request.session_role.or(Some(SessionRole::Orchestrator)),
                    request.store_path_env.as_deref(),
                    shared_store,
                    llm,
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
