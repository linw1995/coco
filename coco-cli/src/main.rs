use std::io::{ErrorKind, IsTerminal, Read};

use clap::Parser;
use coco_llm::{
    COCO_CLI_RUNTIME_SOCKET_ENV, COCO_COMMAND_SHIM_MODE_ENV, COCO_PARENT_TOOL_USE_ID_ENV,
    COCO_SESSION_BRANCH_ENV, COCO_SESSION_ROLE_ENV, COCO_STORE_PATH_ENV, CocoCliRuntimeRequest,
    CocoCliRuntimeResponse,
};
use coco_mem::SessionRole;
use snafu::ResultExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use coco_cli::{
    COCO_DAEMON_SOCKET_ENV, Cli, init_tracing, resolve_default_daemon_socket_path, run,
};

#[tokio::main]
async fn main() {
    let _logging_guard = match init_tracing() {
        Ok(guard) => Some(guard),
        Err(error) => {
            eprintln!("{error}");
            None
        }
    };
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
    match resolve_forwarding_target(&args[1..]) {
        Ok(Some(ForwardingTarget::RuntimeSocket { socket_path })) => {
            tracing::debug!(
                socket_path = %socket_path,
                arg_count = args.len().saturating_sub(1),
                "forwarding cli command to runtime socket"
            );
            if let Err(error) = forward_to_socket(
                &socket_path,
                &args[1..],
                should_forward_runtime_stdin(&args[1..]),
            )
            .await
            {
                tracing::warn!(socket_path = %socket_path, error = %error, "runtime socket forwarding failed");
                eprintln!("{error}");
                std::process::exit(1);
            }
        }
        Ok(Some(ForwardingTarget::DaemonSocket {
            socket_path,
            forwarded_args,
            source,
        })) => {
            tracing::debug!(
                socket_path = %socket_path,
                arg_count = forwarded_args.len(),
                source = ?source,
                "forwarding cli command to daemon socket"
            );
            if let Err(error) = forward_to_socket(&socket_path, &forwarded_args, true).await {
                if should_fallback_to_local(source, &error) {
                    tracing::info!(
                        socket_path = %socket_path,
                        error = %error,
                        "daemon socket unavailable; falling back to local execution"
                    );
                    let _ = std::fs::remove_file(&socket_path);
                } else {
                    tracing::warn!(socket_path = %socket_path, error = %error, "daemon socket forwarding failed");
                    eprintln!("{error}");
                    std::process::exit(1);
                }
            }
        }
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(error = %error, "failed to resolve cli forwarding target");
            eprintln!("{error}");
            std::process::exit(1);
        }
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

#[derive(Debug, snafu::Snafu)]
enum ForwardSocketError {
    #[snafu(display("failed to connect to coco-cli daemon socket {socket_path:?}: {source}"))]
    Connect {
        socket_path: String,
        source: std::io::Error,
    },
    #[snafu(display("failed to send coco-cli daemon request: {source}"))]
    WriteRequest { source: std::io::Error },
    #[snafu(display("failed to close coco-cli daemon request stream: {source}"))]
    Shutdown { source: std::io::Error },
    #[snafu(display("failed to read coco-cli daemon response: {source}"))]
    ReadResponse { source: std::io::Error },
    #[snafu(display("failed to parse coco-cli daemon response: {source}"))]
    ParseResponse { source: serde_json::Error },
}

fn resolve_forwarding_target(args: &[String]) -> Result<Option<ForwardingTarget>, String> {
    if let Some(socket_path) = resolve_runtime_socket(args) {
        return Ok(Some(ForwardingTarget::RuntimeSocket { socket_path }));
    }

    if is_daemon_serve_command(args) {
        return Ok(None);
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

fn is_daemon_serve_command(args: &[String]) -> bool {
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

    matches!(command_tokens.as_slice(), ["daemon", "serve"])
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

async fn forward_to_socket(
    socket_path: &str,
    args: &[String],
    forward_stdin: bool,
) -> Result<(), ForwardSocketError> {
    let stdin = if forward_stdin {
        read_forwarded_stdin()
    } else {
        Vec::new()
    };

    let request = CocoCliRuntimeRequest {
        args: args.to_vec(),
        stdin,
        branch_env: std::env::var(COCO_SESSION_BRANCH_ENV).ok(),
        session_role: std::env::var(COCO_SESSION_ROLE_ENV)
            .ok()
            .and_then(|value| SessionRole::parse(&value)),
        store_path_env: std::env::var(COCO_STORE_PATH_ENV).ok(),
        parent_tool_use_id_env: std::env::var(COCO_PARENT_TOOL_USE_ID_ENV).ok(),
    };
    let payload =
        serde_json::to_vec(&request).expect("failed to serialize coco-cli daemon request");

    let mut stream = UnixStream::connect(socket_path)
        .await
        .context(ConnectSnafu {
            socket_path: socket_path.to_owned(),
        })?;
    stream
        .write_all(&payload)
        .await
        .context(WriteRequestSnafu)?;
    stream.shutdown().await.context(ShutdownSnafu)?;

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .context(ReadResponseSnafu)?;
    let response: CocoCliRuntimeResponse =
        serde_json::from_slice(&response).context(ParseResponseSnafu)?;
    print!("{}", response.stdout);
    eprint!("{}", response.stderr);
    std::process::exit(response.exit_code);
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

    if args.get(index).map(String::as_str) != Some("prompt") {
        return false;
    }
    index += 1;

    while index < args.len() {
        let arg = &args[index];
        match arg.as_str() {
            "status" | "branch-status" | "worker" => return false,
            "--" => return index + 1 == args.len(),
            "--branch" | "--role" | "--tool" => {
                index += 2;
            }
            "--async" | "--json" | "--clear-tools" => {
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
    use std::sync::{Mutex, OnceLock};

    use super::{
        DaemonSocketSource, ForwardSocketError, ForwardingTarget, collect_forwarded_stdin,
        forward_to_socket, is_daemon_serve_command, resolve_forwarding_target,
        should_fallback_to_local, should_forward_runtime_stdin,
    };
    use coco_cli::{COCO_DAEMON_SOCKET_ENV, resolve_default_daemon_socket_path};
    use coco_llm::COCO_CLI_RUNTIME_SOCKET_ENV;
    use tempfile::tempdir;

    fn with_env_vars<T>(entries: &[(&str, Option<&str>)], run: impl FnOnce() -> T) -> T {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
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

        let output = run();

        for (name, value) in previous {
            match value {
                Some(value) => unsafe { std::env::set_var(name, value) },
                None => unsafe { std::env::remove_var(name) },
            }
        }

        output
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
        assert!(is_daemon_serve_command(&[
            "--daemon-socket".to_owned(),
            "/tmp/coco.sock".to_owned(),
            "daemon".to_owned(),
            "serve".to_owned(),
            "--socket".to_owned(),
            "/tmp/coco.sock".to_owned(),
        ]));
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
    fn runtime_socket_stdin_is_only_forwarded_for_prompt_without_text() {
        assert!(!should_forward_runtime_stdin(&[
            "session".to_owned(),
            "list".to_owned(),
        ]));
        assert!(!should_forward_runtime_stdin(&[
            "prompt".to_owned(),
            "status".to_owned(),
            "--job".to_owned(),
            "job-1".to_owned(),
        ]));
        assert!(!should_forward_runtime_stdin(&[
            "prompt".to_owned(),
            "--branch".to_owned(),
            "draft".to_owned(),
            "hello".to_owned(),
        ]));
        assert!(should_forward_runtime_stdin(&[
            "prompt".to_owned(),
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

    #[test]
    fn forward_socket_errors_keep_context_messages() {
        let connect = ForwardSocketError::Connect {
            socket_path: "/tmp/coco.sock".to_owned(),
            source: std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused"),
        };
        assert_eq!(
            connect.to_string(),
            "failed to connect to coco-cli daemon socket \"/tmp/coco.sock\": refused"
        );

        let write = ForwardSocketError::WriteRequest {
            source: std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken"),
        };
        assert_eq!(
            write.to_string(),
            "failed to send coco-cli daemon request: broken"
        );

        let shutdown = ForwardSocketError::Shutdown {
            source: std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken"),
        };
        assert_eq!(
            shutdown.to_string(),
            "failed to close coco-cli daemon request stream: broken"
        );

        let read = ForwardSocketError::ReadResponse {
            source: std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof"),
        };
        assert_eq!(
            read.to_string(),
            "failed to read coco-cli daemon response: eof"
        );

        let parse = ForwardSocketError::ParseResponse {
            source: serde_json::from_str::<serde_json::Value>("not json").unwrap_err(),
        };
        assert!(
            parse
                .to_string()
                .starts_with("failed to parse coco-cli daemon response:")
        );
    }

    #[tokio::test]
    async fn forward_to_socket_reports_connect_context() {
        let socket_path = tempdir().unwrap().path().join("missing.sock");
        let error = forward_to_socket(socket_path.to_str().unwrap(), &[], false)
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

        let error = forward_to_socket(
            socket_path.to_str().unwrap(),
            &["session".to_owned(), "list".to_owned()],
            false,
        )
        .await
        .unwrap_err();
        server.await.unwrap();

        assert!(matches!(error, ForwardSocketError::ParseResponse { .. }));
        assert!(
            error
                .to_string()
                .starts_with("failed to parse coco-cli daemon response:")
        );
    }
}
