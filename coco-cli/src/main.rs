use std::io::{ErrorKind, IsTerminal, Read};

use clap::Parser;
use coco_llm::{
    COCO_CLI_RUNTIME_SOCKET_ENV, COCO_COMMAND_SHIM_MODE_ENV, COCO_PARENT_TOOL_USE_ID_ENV,
    COCO_SESSION_BRANCH_ENV, COCO_SESSION_ROLE_ENV, COCO_STORE_PATH_ENV, CocoCliRuntimeRequest,
    CocoCliRuntimeResponse,
};
use coco_mem::SessionRole;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use coco_cli::{
    COCO_DAEMON_SOCKET_ENV, Cli, LoggingGuard, init_tracing, resolve_default_daemon_socket_path,
    run,
};

#[tokio::main]
async fn main() {
    let _logging_guard = init_cli_tracing();
    let args = std::env::args().collect::<Vec<_>>();
    if shim_mode_is_disabled() {
        eprintln!(
            "{}",
            concat!(
                "coco command is not enabled for this unified exec session; ",
                "enable the coco shim to use CoCo CLI commands."
            )
        );
        std::process::exit(1);
    }
    if let Err(error) = forward_cli_command(&args[1..]).await {
        eprintln!("{error}");
        std::process::exit(1);
    }

    let cli = Cli::parse();

    match run(cli, &mut std::io::stdin()).await {
        Ok(Some(output)) => println!("{output}"),
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(error = %error, "cli command failed");
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn init_cli_tracing() -> Option<LoggingGuard> {
    match init_tracing() {
        Ok(guard) => Some(guard),
        Err(error) => {
            eprintln!("{error}");
            None
        }
    }
}

fn shim_mode_is_disabled() -> bool {
    matches!(
        std::env::var(COCO_COMMAND_SHIM_MODE_ENV).ok().as_deref(),
        Some("disabled")
    )
}

fn resolve_runtime_socket(args: &[String]) -> Option<String> {
    let _ = args;
    std::env::var(COCO_CLI_RUNTIME_SOCKET_ENV).ok()
}

#[derive(Debug, PartialEq, Eq)]
enum ForwardingTarget {
    RuntimeSocket {
        socket_path: String,
    },
    DaemonSocket {
        socket_path: String,
        forwarded_args: Vec<String>,
        source: DaemonSocketSource,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DaemonSocketSource {
    Flag,
    Env,
    Implicit,
}

#[derive(Debug)]
enum ForwardSocketError {
    Connect {
        socket_path: String,
        source: std::io::Error,
    },
    WriteRequest(std::io::Error),
    Shutdown(std::io::Error),
    ReadResponse(std::io::Error),
    ParseResponse(serde_json::Error),
}

impl std::fmt::Display for ForwardSocketError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect {
                socket_path,
                source,
            } => write!(
                formatter,
                "failed to connect to coco-cli daemon socket {socket_path:?}: {source}"
            ),
            Self::WriteRequest(source) => {
                write!(
                    formatter,
                    "failed to send coco-cli daemon request: {source}"
                )
            }
            Self::Shutdown(source) => write!(
                formatter,
                "failed to close coco-cli daemon request stream: {source}"
            ),
            Self::ReadResponse(source) => {
                write!(
                    formatter,
                    "failed to read coco-cli daemon response: {source}"
                )
            }
            Self::ParseResponse(source) => {
                write!(
                    formatter,
                    "failed to parse coco-cli daemon response: {source}"
                )
            }
        }
    }
}

fn resolve_forwarding_target(args: &[String]) -> Result<Option<ForwardingTarget>, String> {
    if is_local_daemon_command(args) {
        return Ok(None);
    }

    if let Some(socket_path) = resolve_runtime_socket(args) {
        return Ok(Some(ForwardingTarget::RuntimeSocket { socket_path }));
    }

    // The daemon socket is OS-scoped rather than project-scoped, so client
    // forwarding must stay explicit. If project-level behavior is needed, run
    // the command locally instead of auto-discovering a host-level daemon.
    let mut daemon_socket = std::env::var(COCO_DAEMON_SOCKET_ENV)
        .ok()
        .map(|socket_path| (socket_path, DaemonSocketSource::Env));
    let mut forwarded_args = Vec::with_capacity(args.len());
    let mut saw_store_path = false;
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--daemon-socket" {
            let Some(value) = args.get(index + 1) else {
                return Err("coco command \"--daemon-socket\" requires a value".to_owned());
            };
            daemon_socket = Some((value.clone(), DaemonSocketSource::Flag));
            index += 2;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--daemon-socket=") {
            daemon_socket = Some((value.to_owned(), DaemonSocketSource::Flag));
            index += 1;
            continue;
        }
        if arg == "--store-path" {
            saw_store_path = true;
        }
        if arg.starts_with("--store-path=") {
            saw_store_path = true;
        }
        forwarded_args.push(arg.clone());
        index += 1;
    }

    let Some((socket_path, source)) = daemon_socket else {
        if saw_store_path {
            return Ok(None);
        }
        let socket_path = resolve_default_daemon_socket_path()
            .map_err(|error| error.to_string())?
            .to_string_lossy()
            .into_owned();
        if !std::path::Path::new(&socket_path).exists() {
            return Ok(None);
        }
        return Ok(Some(ForwardingTarget::DaemonSocket {
            socket_path,
            forwarded_args,
            source: DaemonSocketSource::Implicit,
        }));
    };

    if saw_store_path {
        return match source {
            DaemonSocketSource::Flag => Err(
                "coco command \"--store-path\" is not available in daemon client mode".to_owned(),
            ),
            DaemonSocketSource::Env | DaemonSocketSource::Implicit => Ok(None),
        };
    }

    Ok(Some(ForwardingTarget::DaemonSocket {
        socket_path,
        forwarded_args,
        source,
    }))
}

fn is_local_daemon_command(args: &[String]) -> bool {
    let mut command_tokens = Vec::with_capacity(2);
    let mut index = 0;
    while index < args.len() && command_tokens.len() < 2 {
        let arg = &args[index];
        if arg == "--store-path" || arg == "--daemon-socket" {
            index += 2;
            continue;
        }
        if arg.starts_with("--store-path=") || arg.starts_with("--daemon-socket=") {
            index += 1;
            continue;
        }
        if arg.starts_with('-') {
            index += 1;
            continue;
        }
        command_tokens.push(arg.as_str());
        index += 1;
    }

    matches!(command_tokens.as_slice(), ["daemon", "serve" | "profile"])
}

fn should_fallback_to_local(source: DaemonSocketSource, error: &ForwardSocketError) -> bool {
    matches!(
        (source, error),
        (
            DaemonSocketSource::Implicit,
            ForwardSocketError::Connect {
                source,
                ..
            }
        ) if matches!(
            source.kind(),
            ErrorKind::ConnectionRefused | ErrorKind::NotFound
        )
    )
}

async fn forward_cli_command(args: &[String]) -> Result<(), String> {
    match resolve_forwarding_target(args) {
        Ok(Some(ForwardingTarget::RuntimeSocket { socket_path })) => {
            forward_runtime_socket(&socket_path, args).await
        }
        Ok(Some(ForwardingTarget::DaemonSocket {
            socket_path,
            forwarded_args,
            source,
        })) => forward_daemon_socket(&socket_path, &forwarded_args, source).await,
        Ok(None) => Ok(()),
        Err(error) => {
            tracing::warn!(error = %error, "failed to resolve cli forwarding target");
            Err(error)
        }
    }
}

async fn forward_runtime_socket(socket_path: &str, args: &[String]) -> Result<(), String> {
    tracing::debug!(
        socket_path = %socket_path,
        arg_count = args.len(),
        "forwarding cli command to runtime socket"
    );
    forward_to_socket(socket_path, args, should_forward_runtime_stdin(args))
        .await
        .map_err(|error| {
            tracing::warn!(socket_path = %socket_path, error = %error, "runtime socket forwarding failed");
            error.to_string()
        })
}

async fn forward_daemon_socket(
    socket_path: &str,
    forwarded_args: &[String],
    source: DaemonSocketSource,
) -> Result<(), String> {
    tracing::debug!(
        socket_path = %socket_path,
        arg_count = forwarded_args.len(),
        source = ?source,
        "forwarding cli command to daemon socket"
    );
    let Err(error) = forward_to_socket(socket_path, forwarded_args, true).await else {
        return Ok(());
    };
    if should_fallback_to_local(source, &error) {
        tracing::info!(
            socket_path = %socket_path,
            error = %error,
            "daemon socket unavailable; falling back to local execution"
        );
        let _ = std::fs::remove_file(socket_path);
        return Ok(());
    }

    tracing::warn!(socket_path = %socket_path, error = %error, "daemon socket forwarding failed");
    Err(error.to_string())
}

async fn forward_to_socket(
    socket_path: &str,
    args: &[String],
    forward_stdin: bool,
) -> Result<(), ForwardSocketError> {
    let mut stream = connect_forward_socket(socket_path).await?;
    let request = build_forward_socket_request(args, collect_forward_socket_stdin(forward_stdin));
    let response = exchange_forward_socket_request_stream(&mut stream, request).await?;
    exit_with_forward_socket_response(response);
}

fn collect_forward_socket_stdin(forward_stdin: bool) -> Vec<u8> {
    if !forward_stdin {
        return Vec::new();
    }

    read_forwarded_stdin()
}

fn build_forward_socket_request(args: &[String], stdin: Vec<u8>) -> CocoCliRuntimeRequest {
    CocoCliRuntimeRequest {
        args: args.to_vec(),
        stdin,
        branch_env: std::env::var(COCO_SESSION_BRANCH_ENV).ok(),
        session_role: std::env::var(COCO_SESSION_ROLE_ENV)
            .ok()
            .and_then(|value| SessionRole::parse(&value)),
        store_path_env: std::env::var(COCO_STORE_PATH_ENV).ok(),
        parent_tool_use_id_env: std::env::var(COCO_PARENT_TOOL_USE_ID_ENV).ok(),
    }
}

fn exit_with_forward_socket_response(response: CocoCliRuntimeResponse) -> ! {
    print!("{}", response.stdout);
    eprint!("{}", response.stderr);
    std::process::exit(response.exit_code);
}

#[cfg(test)]
async fn exchange_forward_socket_request(
    socket_path: &str,
    request: CocoCliRuntimeRequest,
) -> Result<CocoCliRuntimeResponse, ForwardSocketError> {
    let mut stream = connect_forward_socket(socket_path).await?;
    exchange_forward_socket_request_stream(&mut stream, request).await
}

async fn connect_forward_socket(socket_path: &str) -> Result<UnixStream, ForwardSocketError> {
    UnixStream::connect(socket_path)
        .await
        .map_err(|source| ForwardSocketError::Connect {
            socket_path: socket_path.to_owned(),
            source,
        })
}

async fn exchange_forward_socket_request_stream(
    stream: &mut UnixStream,
    request: CocoCliRuntimeRequest,
) -> Result<CocoCliRuntimeResponse, ForwardSocketError> {
    let payload =
        serde_json::to_vec(&request).expect("failed to serialize coco-cli daemon request");
    stream
        .write_all(&payload)
        .await
        .map_err(ForwardSocketError::WriteRequest)?;
    stream
        .shutdown()
        .await
        .map_err(ForwardSocketError::Shutdown)?;

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(ForwardSocketError::ReadResponse)?;
    serde_json::from_slice(&response).map_err(ForwardSocketError::ParseResponse)
}

fn read_forwarded_stdin() -> Vec<u8> {
    let mut stdin = std::io::stdin();
    let is_terminal = stdin.is_terminal();
    collect_forwarded_stdin(&mut stdin, is_terminal)
}

fn collect_forwarded_stdin<R>(reader: &mut R, is_terminal: bool) -> Vec<u8>
where
    R: Read,
{
    if is_terminal {
        return Vec::new();
    }

    let mut stdin = Vec::new();
    reader
        .read_to_end(&mut stdin)
        .expect("failed to read stdin for coco-cli daemon forwarding");
    stdin
}

fn should_forward_runtime_stdin(args: &[String]) -> bool {
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--store-path" || arg == "--daemon-socket" {
            index += 2;
            continue;
        }
        if arg.starts_with("--store-path=") || arg.starts_with("--daemon-socket=") {
            index += 1;
            continue;
        }
        if arg.starts_with('-') {
            index += 1;
            continue;
        }
        break;
    }

    if !matches!(args.get(index).map(String::as_str), Some("job" | "prompt")) {
        return false;
    }
    index += 1;

    while index < args.len() {
        let arg = &args[index];
        match arg.as_str() {
            "list" | "status" | "worker" => return false,
            "--" => return index + 1 == args.len(),
            "--branch" | "--role" | "--tool" => {
                index += 2;
            }
            "--async" | "--json" | "--clear-tools" | "--enable-all-tools" => {
                index += 1;
            }
            value
                if value.starts_with("--branch=")
                    || value.starts_with("--role=")
                    || value.starts_with("--tool=") =>
            {
                index += 1;
            }
            value if value.starts_with('-') => {
                index += 1;
            }
            _ => return false,
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::OnceLock;

    use super::{
        DaemonSocketSource, ForwardSocketError, ForwardingTarget, build_forward_socket_request,
        collect_forward_socket_stdin, collect_forwarded_stdin, exchange_forward_socket_request,
        forward_cli_command, forward_daemon_socket, forward_runtime_socket,
        is_local_daemon_command, resolve_forwarding_target, should_fallback_to_local,
        should_forward_runtime_stdin,
    };
    use coco_cli::{COCO_DAEMON_SOCKET_ENV, resolve_default_daemon_socket_path};
    use coco_llm::{
        COCO_CLI_RUNTIME_SOCKET_ENV, COCO_PARENT_TOOL_USE_ID_ENV, COCO_SESSION_BRANCH_ENV,
        COCO_SESSION_ROLE_ENV, COCO_STORE_PATH_ENV, CocoCliRuntimeRequest, CocoCliRuntimeResponse,
    };
    use coco_mem::SessionRole;
    use tempfile::tempdir;
    use tokio::sync::Mutex;

    fn env_lock() -> &'static Mutex<()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn with_env_vars<T>(entries: &[(&str, Option<&str>)], run: impl FnOnce() -> T) -> T {
        let _guard = env_lock().blocking_lock();
        let previous = set_env_vars(entries);
        let output = run();
        restore_env_vars(previous);

        output
    }

    async fn with_env_vars_async<T, F, Fut>(entries: &[(&str, Option<&str>)], run: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let _guard = env_lock().lock().await;
        let previous = set_env_vars(entries);
        let output = run().await;
        restore_env_vars(previous);

        output
    }

    fn set_env_vars(entries: &[(&str, Option<&str>)]) -> Vec<(String, Option<String>)> {
        let previous: Vec<_> = entries
            .iter()
            .map(|(name, _)| ((*name).to_owned(), std::env::var(name).ok()))
            .collect();
        for (name, value) in entries {
            match value {
                Some(value) => unsafe { std::env::set_var(name, value) },
                None => unsafe { std::env::remove_var(name) },
            }
        }
        previous
    }

    fn restore_env_vars(previous: Vec<(String, Option<String>)>) {
        for (name, value) in previous {
            match value {
                Some(value) => unsafe { std::env::set_var(name, value) },
                None => unsafe { std::env::remove_var(name) },
            }
        }
    }

    #[test]
    fn daemon_client_flag_is_removed_before_forwarding() {
        let target = with_env_vars(&[(COCO_DAEMON_SOCKET_ENV, Some("/tmp/coco.sock"))], || {
            resolve_forwarding_target(&[
                "--daemon-socket".to_owned(),
                "/tmp/override.sock".to_owned(),
                "session".to_owned(),
                "list".to_owned(),
            ])
        })
        .unwrap();

        assert_eq!(
            target,
            Some(ForwardingTarget::DaemonSocket {
                socket_path: "/tmp/override.sock".to_owned(),
                forwarded_args: vec!["session".to_owned(), "list".to_owned()],
                source: DaemonSocketSource::Flag,
            })
        );
    }

    #[test]
    fn daemon_serve_command_is_not_forwarded() {
        assert!(is_local_daemon_command(&[
            "--daemon-socket".to_owned(),
            "/tmp/coco.sock".to_owned(),
            "daemon".to_owned(),
            "serve".to_owned(),
            "--socket".to_owned(),
            "/tmp/coco.sock".to_owned(),
        ]));
    }

    #[test]
    fn daemon_profile_command_is_not_forwarded() {
        let target = with_env_vars(
            &[
                (COCO_CLI_RUNTIME_SOCKET_ENV, Some("/tmp/runtime.sock")),
                (COCO_DAEMON_SOCKET_ENV, Some("/tmp/daemon.sock")),
            ],
            || {
                resolve_forwarding_target(&[
                    "--daemon-socket".to_owned(),
                    "/tmp/override.sock".to_owned(),
                    "daemon".to_owned(),
                    "profile".to_owned(),
                    "graph".to_owned(),
                ])
            },
        )
        .unwrap();

        assert_eq!(target, None);
    }

    #[test]
    fn runtime_socket_takes_priority_over_daemon_client_socket() {
        let target = with_env_vars(
            &[
                (COCO_CLI_RUNTIME_SOCKET_ENV, Some("/tmp/runtime.sock")),
                (COCO_DAEMON_SOCKET_ENV, Some("/tmp/daemon.sock")),
            ],
            || resolve_forwarding_target(&["session".to_owned(), "list".to_owned()]),
        )
        .unwrap();

        assert_eq!(
            target,
            Some(ForwardingTarget::RuntimeSocket {
                socket_path: "/tmp/runtime.sock".to_owned(),
            })
        );
    }

    #[test]
    fn default_daemon_socket_is_used_when_socket_exists() {
        let runtime_dir = tempdir().unwrap();
        let socket_dir = runtime_dir.path().join("coco");
        std::fs::create_dir_all(&socket_dir).unwrap();
        let socket_path = socket_dir.join("coco-daemon.sock");
        std::fs::write(&socket_path, "").unwrap();

        let xdg_runtime_dir = runtime_dir.path().to_string_lossy().into_owned();
        let expected_socket_path = with_env_vars(
            &[("XDG_RUNTIME_DIR", Some(xdg_runtime_dir.as_str()))],
            || resolve_default_daemon_socket_path().unwrap(),
        );
        let target = with_env_vars(
            &[
                ("XDG_RUNTIME_DIR", Some(xdg_runtime_dir.as_str())),
                (COCO_DAEMON_SOCKET_ENV, None),
            ],
            || resolve_forwarding_target(&["session".to_owned(), "list".to_owned()]),
        )
        .unwrap();

        assert_eq!(
            target,
            Some(ForwardingTarget::DaemonSocket {
                socket_path: expected_socket_path.to_string_lossy().into_owned(),
                forwarded_args: vec!["session".to_owned(), "list".to_owned()],
                source: DaemonSocketSource::Implicit,
            })
        );
    }

    #[test]
    fn store_path_keeps_local_mode_even_when_default_daemon_socket_exists() {
        let runtime_dir = tempdir().unwrap();
        let socket_dir = runtime_dir.path().join("coco");
        std::fs::create_dir_all(&socket_dir).unwrap();
        let socket_path = socket_dir.join("coco-daemon.sock");
        std::fs::write(&socket_path, "").unwrap();

        let xdg_runtime_dir = runtime_dir.path().to_string_lossy().into_owned();
        let target = with_env_vars(
            &[("XDG_RUNTIME_DIR", Some(xdg_runtime_dir.as_str()))],
            || {
                resolve_forwarding_target(&[
                    "--store-path".to_owned(),
                    "/tmp/custom-store".to_owned(),
                    "session".to_owned(),
                    "list".to_owned(),
                ])
            },
        )
        .unwrap();

        assert_eq!(target, None);
    }

    #[test]
    fn store_path_ignores_daemon_socket_from_env() {
        let target = with_env_vars(
            &[(COCO_DAEMON_SOCKET_ENV, Some("/tmp/daemon.sock"))],
            || {
                resolve_forwarding_target(&[
                    "--store-path".to_owned(),
                    "/tmp/custom-store".to_owned(),
                    "session".to_owned(),
                    "list".to_owned(),
                ])
            },
        )
        .unwrap();

        assert_eq!(target, None);
    }

    #[test]
    fn forwarded_stdin_is_empty_for_terminal_sessions() {
        let mut reader = std::io::Cursor::new(b"ignored".to_vec());
        let stdin = collect_forwarded_stdin(&mut reader, true);

        assert!(stdin.is_empty());
    }

    #[test]
    fn forwarded_stdin_reads_pipe_input() {
        let mut reader = std::io::Cursor::new(b"hello\n".to_vec());
        let stdin = collect_forwarded_stdin(&mut reader, false);

        assert_eq!(stdin, b"hello\n");
    }

    #[test]
    fn forward_socket_stdin_is_empty_when_not_forwarded() {
        assert!(collect_forward_socket_stdin(false).is_empty());
    }

    #[test]
    fn forward_socket_request_captures_environment() {
        let request = with_env_vars(
            &[
                (COCO_SESSION_BRANCH_ENV, Some("feature")),
                (COCO_SESSION_ROLE_ENV, Some("runner")),
                (COCO_STORE_PATH_ENV, Some("/tmp/store")),
                (COCO_PARENT_TOOL_USE_ID_ENV, Some("tool-call")),
            ],
            || {
                build_forward_socket_request(
                    &["session".to_owned(), "list".to_owned()],
                    b"input".to_vec(),
                )
            },
        );

        assert_eq!(request.args, ["session", "list"]);
        assert_eq!(request.stdin, b"input");
        assert_eq!(request.branch_env.as_deref(), Some("feature"));
        assert_eq!(request.session_role, Some(SessionRole::Runner));
        assert_eq!(request.store_path_env.as_deref(), Some("/tmp/store"));
        assert_eq!(request.parent_tool_use_id_env.as_deref(), Some("tool-call"));
    }

    #[test]
    fn runtime_socket_stdin_is_only_forwarded_for_prompt_without_text() {
        assert!(!should_forward_runtime_stdin(&[
            "session".to_owned(),
            "list".to_owned(),
        ]));
        assert!(!should_forward_runtime_stdin(&[
            "job".to_owned(),
            "status".to_owned(),
            "--job".to_owned(),
            "job-1".to_owned(),
        ]));
        assert!(!should_forward_runtime_stdin(&[
            "job".to_owned(),
            "--branch".to_owned(),
            "draft".to_owned(),
            "hello".to_owned(),
        ]));
        assert!(should_forward_runtime_stdin(&[
            "job".to_owned(),
            "--branch".to_owned(),
            "draft".to_owned(),
        ]));
    }

    #[test]
    fn implicit_daemon_socket_falls_back_on_connection_refused() {
        let error = ForwardSocketError::Connect {
            socket_path: "/tmp/coco.sock".to_owned(),
            source: std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused"),
        };

        assert!(should_fallback_to_local(
            DaemonSocketSource::Implicit,
            &error
        ));
        assert!(!should_fallback_to_local(DaemonSocketSource::Env, &error));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn forward_cli_command_reports_resolution_errors() {
        let error = with_env_vars_async(
            &[
                (COCO_CLI_RUNTIME_SOCKET_ENV, None),
                (COCO_DAEMON_SOCKET_ENV, None),
            ],
            || async {
                forward_cli_command(&["--daemon-socket".to_owned()])
                    .await
                    .unwrap_err()
            },
        )
        .await;

        assert_eq!(error, "coco command \"--daemon-socket\" requires a value");
    }

    #[tokio::test]
    async fn forward_runtime_socket_reports_connect_failure() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("missing-runtime.sock");

        let error = forward_runtime_socket(
            socket_path.to_str().unwrap(),
            &["session".to_owned(), "list".to_owned()],
        )
        .await
        .unwrap_err();

        assert!(error.contains("failed to connect to coco-cli daemon socket"));
        assert!(error.contains("missing-runtime.sock"));
    }

    #[tokio::test]
    async fn forward_daemon_socket_reports_explicit_connect_failure() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("missing-daemon.sock");

        let error = forward_daemon_socket(
            socket_path.to_str().unwrap(),
            &["session".to_owned(), "list".to_owned()],
            DaemonSocketSource::Flag,
        )
        .await
        .unwrap_err();

        assert!(error.contains("failed to connect to coco-cli daemon socket"));
        assert!(error.contains("missing-daemon.sock"));
    }

    #[tokio::test]
    async fn forward_daemon_socket_falls_back_for_implicit_connect_failure() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("missing-implicit.sock");

        let result = forward_daemon_socket(
            socket_path.to_str().unwrap(),
            &["session".to_owned(), "list".to_owned()],
            DaemonSocketSource::Implicit,
        )
        .await;

        assert!(result.is_ok());
    }

    #[test]
    fn forward_socket_error_display_covers_all_variants() {
        let io_error = |message| std::io::Error::other(message);
        let cases = vec![
            (
                ForwardSocketError::Connect {
                    socket_path: "/tmp/coco.sock".to_owned(),
                    source: io_error("connect failed"),
                },
                "failed to connect to coco-cli daemon socket \"/tmp/coco.sock\": connect failed",
            ),
            (
                ForwardSocketError::WriteRequest(io_error("write failed")),
                "failed to send coco-cli daemon request: write failed",
            ),
            (
                ForwardSocketError::Shutdown(io_error("shutdown failed")),
                "failed to close coco-cli daemon request stream: shutdown failed",
            ),
            (
                ForwardSocketError::ReadResponse(io_error("read failed")),
                "failed to read coco-cli daemon response: read failed",
            ),
            (
                ForwardSocketError::ParseResponse(
                    serde_json::from_slice::<serde_json::Value>(b"{").unwrap_err(),
                ),
                "failed to parse coco-cli daemon response: EOF while parsing an object at line 1 column 1",
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.to_string(), expected);
        }
    }

    #[tokio::test]
    async fn forward_to_socket_reports_connect_context() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("missing.sock");
        let request = CocoCliRuntimeRequest {
            args: Vec::new(),
            stdin: Vec::new(),
            branch_env: None,
            session_role: None,
            store_path_env: None,
            parent_tool_use_id_env: None,
        };

        let error = exchange_forward_socket_request(socket_path.to_str().unwrap(), request)
            .await
            .unwrap_err();

        let ForwardSocketError::Connect {
            socket_path: reported_path,
            source,
        } = error
        else {
            panic!("expected connect error");
        };
        assert_eq!(reported_path, socket_path.to_string_lossy());
        assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn forward_to_socket_exchanges_runtime_request() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("coco.sock");
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut request)
                .await
                .unwrap();
            let request = serde_json::from_slice::<CocoCliRuntimeRequest>(&request).unwrap();
            assert_eq!(request.args, ["session", "list"]);
            assert_eq!(request.stdin, b"input");
            assert_eq!(request.branch_env.as_deref(), Some("feature"));
            assert_eq!(request.session_role, Some(SessionRole::Runner));
            assert_eq!(request.store_path_env.as_deref(), Some("/tmp/store"));
            assert_eq!(request.parent_tool_use_id_env.as_deref(), Some("tool-call"));

            let response = CocoCliRuntimeResponse {
                exit_code: 7,
                stdout: "output".to_owned(),
                stderr: "warning".to_owned(),
            };
            let payload = serde_json::to_vec(&response).unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut stream, &payload)
                .await
                .unwrap();
        });
        let request = CocoCliRuntimeRequest {
            args: vec!["session".to_owned(), "list".to_owned()],
            stdin: b"input".to_vec(),
            branch_env: Some("feature".to_owned()),
            session_role: Some(SessionRole::Runner),
            store_path_env: Some("/tmp/store".to_owned()),
            parent_tool_use_id_env: Some("tool-call".to_owned()),
        };

        let response = exchange_forward_socket_request(socket_path.to_str().unwrap(), request)
            .await
            .unwrap();
        server.await.unwrap();

        assert_eq!(response.exit_code, 7);
        assert_eq!(response.stdout, "output");
        assert_eq!(response.stderr, "warning");
    }

    #[tokio::test]
    async fn forward_to_socket_reports_parse_context() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("coco.sock");
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut request)
                .await
                .unwrap();
            assert!(!request.is_empty());
            tokio::io::AsyncWriteExt::write_all(&mut stream, b"not json")
                .await
                .unwrap();
        });
        let request = CocoCliRuntimeRequest {
            args: vec!["session".to_owned(), "list".to_owned()],
            stdin: Vec::new(),
            branch_env: None,
            session_role: None,
            store_path_env: None,
            parent_tool_use_id_env: None,
        };

        let error = exchange_forward_socket_request(socket_path.to_str().unwrap(), request)
            .await
            .unwrap_err();
        server.await.unwrap();

        assert!(matches!(error, ForwardSocketError::ParseResponse(_)));
        assert!(
            error
                .to_string()
                .starts_with("failed to parse coco-cli daemon response:")
        );
    }
}
