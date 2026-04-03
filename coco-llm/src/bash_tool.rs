use std::ffi::{OsStr, OsString};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use coco_mem::Tool;
use serde_json::Value;
use snafu::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::process::Command;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

use crate::{
    BashToolContext, COCO_CLI_RUNTIME_SOCKET_ENV, COCO_SESSION_BRANCH_ENV, COCO_STORE_PATH_ENV,
    CocoCliRuntimeRequest, CocoCliRuntimeResponse,
};

#[derive(Debug, Clone)]
pub struct BashToolRuntime {
    definition: Tool,
    workspace_root: PathBuf,
    context: BashToolContext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BashSandboxMode {
    Auto,
    Nono,
    Off,
}

#[derive(Debug, Clone)]
struct BashCommandRequest {
    command: String,
    workdir: PathBuf,
    workspace_root: PathBuf,
    sandbox_mode: BashSandboxMode,
    timeout_ms: Option<u64>,
    context: BashToolContext,
}

#[derive(Debug)]
struct CocoCliRuntimeServer {
    socket_dir: PathBuf,
    socket_path: PathBuf,
    task: tokio::task::JoinHandle<()>,
}

const MAX_RUNTIME_SOCKET_PATH_LEN: usize = 107;

#[cfg(unix)]
const NONO_DEFAULT_ALLOW_FILES: &[&str] = &["/dev/null"];

#[cfg(not(unix))]
const NONO_DEFAULT_ALLOW_FILES: &[&str] = &[];

#[derive(Debug, Snafu)]
pub enum BashToolError {
    #[snafu(display("failed to resolve workspace root: {source}"))]
    ResolveWorkspaceRoot { source: io::Error },

    #[snafu(display("failed to canonicalize workspace root: {source}"))]
    CanonicalizeWorkspaceRoot { source: io::Error },

    #[snafu(display("failed to resolve runtime root: {source}"))]
    ResolveRuntimeRoot { source: io::Error },

    #[snafu(display(
        "bash tool sandbox mode must be one of \"auto\", \"nono\", or \"off\", got {value:?}"
    ))]
    InvalidSandboxMode { value: String },

    #[snafu(display("bash tool workspace override could not resolve: {source}"))]
    ResolveWorkspaceOverride { source: io::Error },

    #[snafu(display("bash tool workspace override must point to an existing directory"))]
    InvalidWorkspaceOverride,

    #[snafu(display("bash tool runtime root must stay isolated from the configured workspace"))]
    RuntimeRootOverlapsWorkspace,

    #[snafu(display("bash tool expects a JSON object input"))]
    InvalidInputType,

    #[snafu(display("bash tool requires a string field `command`"))]
    MissingCommand,

    #[snafu(display("bash tool could not resolve workdir: {source}"))]
    ResolveWorkdir { source: io::Error },

    #[snafu(display("bash tool workdir must point to an existing directory"))]
    InvalidWorkdir,

    #[snafu(display("bash tool workdir must stay within the configured workspace"))]
    WorkdirOutsideWorkspace,

    #[snafu(display("bash tool arguments must be valid JSON: {source}"))]
    ParseArgs { source: serde_json::Error },

    #[snafu(display("bash tool could not locate `nono` in PATH"))]
    NonoNotFound,

    #[snafu(display("bash tool could not spawn process: {source}"))]
    SpawnProcess { source: io::Error },

    #[snafu(display("bash tool could not wait for process: {source}"))]
    WaitProcess { source: io::Error },

    #[snafu(display("bash tool could not join stdout task: {source}"))]
    JoinStdout { source: tokio::task::JoinError },

    #[snafu(display("bash tool could not join stderr task: {source}"))]
    JoinStderr { source: tokio::task::JoinError },

    #[snafu(display("bash tool could not read stdout: {source}"))]
    ReadStdout { source: io::Error },

    #[snafu(display("bash tool could not read stderr: {source}"))]
    ReadStderr { source: io::Error },

    #[snafu(display("bash tool could not bind coco-cli runtime socket: {source}"))]
    BindRuntimeSocket { source: io::Error },

    #[snafu(display(
        "bash tool runtime socket path is too long for a Unix socket: {path:?} ({length} bytes, max {max})"
    ))]
    RuntimeSocketPathTooLong {
        path: PathBuf,
        length: usize,
        max: usize,
    },

    #[snafu(display("bash tool runtime socket server task failed: {source}"))]
    JoinRuntimeSocketTask { source: tokio::task::JoinError },
}

pub fn resolve_workspace_root() -> std::result::Result<PathBuf, BashToolError> {
    let current_dir = std::env::current_dir().context(ResolveWorkspaceRootSnafu)?;
    let workspace_raw = std::env::var_os("COCO_BASH_WORKSPACE");
    let path = match workspace_raw {
        Some(workspace_raw) => {
            let configured = PathBuf::from(workspace_raw);
            if configured.is_absolute() {
                configured
            } else {
                current_dir.join(configured)
            }
        }
        None => {
            let path = current_dir
                .canonicalize()
                .context(CanonicalizeWorkspaceRootSnafu)?;
            if !path.is_dir() {
                return InvalidWorkspaceOverrideSnafu.fail();
            }
            return Ok(path);
        }
    };
    let path = path.canonicalize().context(ResolveWorkspaceOverrideSnafu)?;
    if !path.is_dir() {
        return InvalidWorkspaceOverrideSnafu.fail();
    }
    Ok(path)
}

pub fn runtime_tool(
    definition: Tool,
    workspace_root: PathBuf,
    context: BashToolContext,
) -> Box<dyn rig::tool::ToolDyn> {
    let workspace_root = canonicalize_existing_path(&workspace_root);
    Box::new(BashToolRuntime {
        definition,
        workspace_root,
        context,
    })
}

fn resolve_sandbox_mode() -> std::result::Result<BashSandboxMode, BashToolError> {
    let Some(value) = std::env::var_os("COCO_BASH_SANDBOX") else {
        return Ok(BashSandboxMode::Auto);
    };
    match value.to_string_lossy().as_ref() {
        "auto" => Ok(BashSandboxMode::Auto),
        "nono" => Ok(BashSandboxMode::Nono),
        "off" => Ok(BashSandboxMode::Off),
        other => InvalidSandboxModeSnafu {
            value: other.to_owned(),
        }
        .fail(),
    }
}

fn path_is_within_workspace(path: &Path, workspace_root: &Path) -> bool {
    path == workspace_root || path.starts_with(workspace_root)
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
}

fn canonicalize_existing_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn resolve_runtime_root(workspace_root: &Path) -> std::result::Result<PathBuf, BashToolError> {
    let current_dir = std::env::current_dir().context(ResolveRuntimeRootSnafu)?;
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
    }
    .join("coco");
    let runtime_root = canonicalize_existing_path(&runtime_root);
    let workspace_root = canonicalize_existing_path(workspace_root);

    if paths_overlap(&runtime_root, &workspace_root) {
        return RuntimeRootOverlapsWorkspaceSnafu.fail();
    }

    Ok(runtime_root)
}

fn resolve_bash_request(
    args: &Value,
    workspace_root: &Path,
    context: BashToolContext,
) -> std::result::Result<BashCommandRequest, BashToolError> {
    let workspace_root = canonicalize_existing_path(workspace_root);
    let object = args.as_object().context(InvalidInputTypeSnafu)?;
    let command = object
        .get("command")
        .and_then(Value::as_str)
        .context(MissingCommandSnafu)?;

    let timeout_ms = object.get("timeout_ms").and_then(Value::as_u64);

    let workdir_raw = object.get("workdir").and_then(Value::as_str).unwrap_or(".");
    let workdir_candidate = Path::new(workdir_raw);
    let workdir = if workdir_candidate.is_absolute() {
        workdir_candidate.to_path_buf()
    } else {
        workspace_root.join(workdir_candidate)
    };
    let workdir = workdir.canonicalize().context(ResolveWorkdirSnafu)?;
    if !workdir.is_dir() {
        return InvalidWorkdirSnafu.fail();
    }
    if !path_is_within_workspace(&workdir, &workspace_root) {
        return WorkdirOutsideWorkspaceSnafu.fail();
    }

    Ok(BashCommandRequest {
        command: command.to_owned(),
        workdir,
        workspace_root,
        sandbox_mode: resolve_sandbox_mode()?,
        timeout_ms,
        context,
    })
}

async fn read_child_pipe<R>(reader: Option<R>) -> io::Result<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let Some(mut reader) = reader else {
        return Ok(Vec::new());
    };
    let mut output = Vec::new();
    reader.read_to_end(&mut output).await?;
    Ok(output)
}

fn format_bash_result(
    exit_status: Option<i32>,
    stdout: String,
    stderr: String,
    timed_out: bool,
) -> String {
    let status = match (timed_out, exit_status) {
        (true, _) => "timed_out".to_owned(),
        (false, Some(code)) => code.to_string(),
        (false, None) => "terminated_by_signal".to_owned(),
    };
    format!(
        "exit_status: {status}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        stdout = stdout,
        stderr = stderr,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExecutionSpec {
    program: PathBuf,
    args: Vec<OsString>,
}

fn find_program_in_path(name: &str, path_env: Option<&OsStr>) -> Option<PathBuf> {
    let owned_path_env;
    let path_env = match path_env {
        Some(path_env) => path_env,
        None => {
            owned_path_env = std::env::var_os("PATH")?;
            owned_path_env.as_os_str()
        }
    };
    for directory in std::env::split_paths(path_env) {
        let candidate = directory.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn bash_execution_spec(command: &str) -> ExecutionSpec {
    ExecutionSpec {
        program: PathBuf::from("bash"),
        args: vec![OsString::from("-lc"), OsString::from(command.to_owned())],
    }
}

fn nono_execution_spec(
    nono: PathBuf,
    request: &BashCommandRequest,
    extra_allow_path: Option<&Path>,
) -> ExecutionSpec {
    let mut args = vec![
        OsString::from("run"),
        OsString::from("--silent"),
        OsString::from("--allow"),
        request.workspace_root.as_os_str().to_owned(),
    ];
    for default_allow_file in NONO_DEFAULT_ALLOW_FILES {
        args.push(OsString::from("--allow-file"));
        args.push(OsString::from(default_allow_file));
    }
    if let Some(extra_allow_path) = extra_allow_path {
        args.push(OsString::from("--allow"));
        args.push(extra_allow_path.as_os_str().to_owned());
    }
    args.extend([
        OsString::from("--"),
        OsString::from("bash"),
        OsString::from("-lc"),
        OsString::from(request.command.to_owned()),
    ]);
    ExecutionSpec {
        program: nono,
        args,
    }
}

fn resolve_execution_spec(
    request: &BashCommandRequest,
    path_env: Option<&OsStr>,
    extra_allow_path: Option<&Path>,
) -> std::result::Result<ExecutionSpec, BashToolError> {
    match request.sandbox_mode {
        BashSandboxMode::Off => Ok(bash_execution_spec(&request.command)),
        BashSandboxMode::Auto => match find_program_in_path("nono", path_env) {
            Some(nono) => Ok(nono_execution_spec(nono, request, extra_allow_path)),
            None => Ok(bash_execution_spec(&request.command)),
        },
        BashSandboxMode::Nono => {
            let nono = find_program_in_path("nono", path_env).context(NonoNotFoundSnafu)?;
            Ok(nono_execution_spec(nono, request, extra_allow_path))
        }
    }
}

fn next_runtime_socket_dir(runtime_root: &Path) -> PathBuf {
    runtime_root.join(nanoid::nanoid!(6))
}

fn validate_runtime_socket_path(path: &Path) -> std::result::Result<(), BashToolError> {
    #[cfg(unix)]
    {
        let length = path.as_os_str().as_bytes().len();
        ensure!(
            length <= MAX_RUNTIME_SOCKET_PATH_LEN,
            RuntimeSocketPathTooLongSnafu {
                path: path.to_path_buf(),
                length,
                max: MAX_RUNTIME_SOCKET_PATH_LEN,
            }
        );
    }

    Ok(())
}

async fn start_coco_cli_runtime_server(
    workspace_root: &Path,
    context: &BashToolContext,
) -> std::result::Result<Option<CocoCliRuntimeServer>, BashToolError> {
    let Some(cli_bridge) = context.cli_bridge.clone() else {
        return Ok(None);
    };

    let runtime_root = resolve_runtime_root(workspace_root)?;
    std::fs::create_dir_all(&runtime_root).context(BindRuntimeSocketSnafu)?;
    let socket_dir = next_runtime_socket_dir(&runtime_root);
    std::fs::create_dir_all(&socket_dir).context(BindRuntimeSocketSnafu)?;
    let socket_path = socket_dir.join("coco-cli.sock");
    validate_runtime_socket_path(&socket_path)?;
    let listener = UnixListener::bind(&socket_path).context(BindRuntimeSocketSnafu)?;
    let task = tokio::spawn(async move {
        loop {
            let accepted = listener.accept().await;
            let Ok((mut stream, _)) = accepted else {
                break;
            };
            let cli_bridge = cli_bridge.clone();
            tokio::spawn(async move {
                let mut input = Vec::new();
                let response = match stream.read_to_end(&mut input).await {
                    Ok(_) => match serde_json::from_slice::<CocoCliRuntimeRequest>(&input) {
                        Ok(request) => match cli_bridge.execute_coco_cli(request).await {
                            Ok(response) => response,
                            Err(error) => CocoCliRuntimeResponse {
                                exit_code: 1,
                                stdout: String::new(),
                                stderr: error.to_string(),
                            },
                        },
                        Err(error) => CocoCliRuntimeResponse {
                            exit_code: 2,
                            stdout: String::new(),
                            stderr: format!("invalid coco-cli runtime request: {error}"),
                        },
                    },
                    Err(error) => CocoCliRuntimeResponse {
                        exit_code: 2,
                        stdout: String::new(),
                        stderr: format!("failed to read coco-cli runtime request: {error}"),
                    },
                };
                if let Ok(payload) = serde_json::to_vec(&response) {
                    let _ = stream.write_all(&payload).await;
                }
            });
        }
    });

    Ok(Some(CocoCliRuntimeServer {
        socket_dir,
        socket_path,
        task,
    }))
}

impl CocoCliRuntimeServer {
    async fn shutdown(self) -> std::result::Result<(), BashToolError> {
        self.task.abort();
        match self.task.await {
            Ok(()) => {}
            Err(source) if source.is_cancelled() => {}
            Err(source) => return Err(BashToolError::JoinRuntimeSocketTask { source }),
        }
        let _ = std::fs::remove_file(&self.socket_path);
        let _ = std::fs::remove_dir(&self.socket_dir);
        Ok(())
    }
}

async fn execute_bash_command(
    request: BashCommandRequest,
) -> std::result::Result<String, BashToolError> {
    let runtime_server =
        start_coco_cli_runtime_server(&request.workspace_root, &request.context).await?;
    let execution = resolve_execution_spec(
        &request,
        None,
        runtime_server
            .as_ref()
            .map(|runtime_server| runtime_server.socket_dir.as_path()),
    )?;
    let mut command = Command::new(&execution.program);
    command
        .args(&execution.args)
        .current_dir(&request.workdir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(COCO_SESSION_BRANCH_ENV)
        .env_remove(COCO_STORE_PATH_ENV)
        .env_remove(COCO_CLI_RUNTIME_SOCKET_ENV)
        .env(COCO_SESSION_BRANCH_ENV, &request.context.session_branch);
    if let Some(store_path) = &request.context.store_path {
        command.env(COCO_STORE_PATH_ENV, store_path);
    }
    if let Some(runtime_server) = &runtime_server {
        command.env(COCO_CLI_RUNTIME_SOCKET_ENV, &runtime_server.socket_path);
    }
    let mut child = command.spawn().context(SpawnProcessSnafu)?;

    let stdout_handle = tokio::spawn(read_child_pipe(child.stdout.take()));
    let stderr_handle = tokio::spawn(read_child_pipe(child.stderr.take()));

    let status = match request.timeout_ms {
        Some(timeout_ms) => {
            match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), child.wait())
                .await
            {
                Ok(result) => Some(result.context(WaitProcessSnafu)?),
                Err(_) => {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    None
                }
            }
        }
        None => Some(child.wait().await.context(WaitProcessSnafu)?),
    };

    let stdout = stdout_handle.await.context(JoinStdoutSnafu)?;
    let stdout = stdout.context(ReadStdoutSnafu)?;
    let stderr = stderr_handle.await.context(JoinStderrSnafu)?;
    let stderr = stderr.context(ReadStderrSnafu)?;
    if let Some(runtime_server) = runtime_server {
        runtime_server.shutdown().await?;
    }

    Ok(format_bash_result(
        status.as_ref().and_then(std::process::ExitStatus::code),
        String::from_utf8_lossy(&stdout).into_owned(),
        String::from_utf8_lossy(&stderr).into_owned(),
        status.is_none(),
    ))
}

impl rig::tool::ToolDyn for BashToolRuntime {
    fn name(&self) -> String {
        self.definition.name.clone()
    }

    fn definition<'a>(
        &'a self,
        _prompt: String,
    ) -> rig::wasm_compat::WasmBoxedFuture<'a, rig::completion::ToolDefinition> {
        let definition = rig::completion::ToolDefinition {
            name: self.definition.name.clone(),
            description: self.definition.description.clone(),
            parameters: self.definition.input_schema.clone(),
        };
        Box::pin(async move { definition })
    }

    fn call<'a>(
        &'a self,
        args: String,
    ) -> rig::wasm_compat::WasmBoxedFuture<'a, std::result::Result<String, rig::tool::ToolError>>
    {
        use rig::tool::ToolError;

        let workspace_root = self.workspace_root.clone();
        let context = self.context.clone();
        Box::pin(async move {
            let result = async {
                let args: Value = serde_json::from_str(&args).context(ParseArgsSnafu)?;
                let request = resolve_bash_request(&args, &workspace_root, context)?;
                execute_bash_command(request).await
            }
            .await;
            match result {
                Ok(output) => Ok(output),
                Err(source) => Err(ToolError::ToolCallError(Box::new(source))),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Arc, OnceLock};

    use async_trait::async_trait;
    use tokio::net::UnixStream;
    use tokio::sync::Mutex;

    use super::*;

    fn test_context() -> BashToolContext {
        BashToolContext {
            session_branch: "main".to_owned(),
            store_path: None,
            cli_bridge: None,
        }
    }

    #[derive(Debug)]
    struct FakeCliBridge {
        requests: Arc<Mutex<Vec<CocoCliRuntimeRequest>>>,
    }

    #[async_trait]
    impl crate::BashToolCliBridge for FakeCliBridge {
        async fn execute_coco_cli(
            &self,
            request: CocoCliRuntimeRequest,
        ) -> std::result::Result<CocoCliRuntimeResponse, crate::BashToolCliBridgeError> {
            self.requests.lock().await.push(request);
            Ok(CocoCliRuntimeResponse {
                exit_code: 0,
                stdout: "delegated".to_owned(),
                stderr: String::new(),
            })
        }
    }


    fn temp_tool() -> Tool {
        Tool {
            name: "bash".to_owned(),
            description: "Run a bash command".to_owned(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "command": {"type": "string"},
                    "workdir": {"type": "string"},
                    "timeout_ms": {"type": "integer"}
                },
                "required": ["command"]
            }),
        }
    }

    fn resolve_bash_path() -> PathBuf {
        let output = std::process::Command::new("bash")
            .arg("-lc")
            .arg("command -v bash")
            .output()
            .expect("failed to resolve bash path");
        assert!(output.status.success(), "bash path lookup should succeed");
        PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
    }

    async fn resolve_bash_path_locked() -> PathBuf {
        crate::with_process_env_async(&[], || async { resolve_bash_path() }).await
    }

    fn write_executable_script(path: &Path, body: &str) {
        std::fs::write(path, body).unwrap();
        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).unwrap();
    }

    #[tokio::test]
    async fn resolve_workspace_root_uses_relative_env_override() {
        let temp_root = tempfile::tempdir().unwrap();
        let nested = temp_root.path().join("workspace");
        std::fs::create_dir(&nested).unwrap();
        let previous_dir = std::env::current_dir().unwrap();
        let relative = OsString::from("workspace");

        let resolved = crate::with_process_env_async(
            &[("COCO_BASH_WORKSPACE", Some(relative.as_os_str()))],
            || async {
                std::env::set_current_dir(temp_root.path()).unwrap();
                let result = resolve_workspace_root();
                std::env::set_current_dir(&previous_dir).unwrap();
                result
            },
        )
        .await
        .unwrap();

        assert_eq!(resolved, nested.canonicalize().unwrap());
    }

    #[tokio::test]
    async fn resolve_runtime_root_prefers_xdg_runtime_dir() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime = tempfile::tempdir().unwrap();

        let resolved = crate::with_process_env_async(
            &[("XDG_RUNTIME_DIR", Some(runtime.path().as_os_str()))],
            || async { resolve_runtime_root(workspace.path()) },
        )
        .await
        .unwrap();

        assert_eq!(resolved, runtime.path().join("coco"));
    }

    #[tokio::test]
    async fn resolve_runtime_root_falls_back_to_temp_dir() {
        let workspace = tempfile::tempdir().unwrap();

        let resolved = crate::with_process_env_async(&[("XDG_RUNTIME_DIR", None)], || async {
            resolve_runtime_root(workspace.path())
        })
        .await
        .unwrap();

        assert_eq!(resolved, std::env::temp_dir().join("coco"));
    }

    #[tokio::test]
    async fn resolve_runtime_root_rejects_overlap_with_workspace() {
        let temp_root = tempfile::tempdir().unwrap();
        let workspace = temp_root.path().join("coco");
        std::fs::create_dir(&workspace).unwrap();

        let error = crate::with_process_env_async(
            &[("XDG_RUNTIME_DIR", Some(temp_root.path().as_os_str()))],
            || async { resolve_runtime_root(&workspace) },
        )
        .await
        .unwrap_err();

        assert!(matches!(error, BashToolError::RuntimeRootOverlapsWorkspace));
    }

    #[test]
    fn validate_runtime_socket_path_rejects_long_paths() {
        let long_dir = "a".repeat(MAX_RUNTIME_SOCKET_PATH_LEN);
        let path = PathBuf::from("/tmp").join(long_dir).join("coco-cli.sock");

        let error = validate_runtime_socket_path(&path).unwrap_err();

        assert!(matches!(
            error,
            BashToolError::RuntimeSocketPathTooLong { .. }
        ));
    }

    #[test]
    fn resolve_execution_spec_uses_nono_when_available_in_nono_mode() {
        let temp_root = tempfile::tempdir().unwrap();
        let request = BashCommandRequest {
            command: "pwd".to_owned(),
            workdir: temp_root.path().to_path_buf(),
            workspace_root: temp_root.path().to_path_buf(),
            sandbox_mode: BashSandboxMode::Nono,
            timeout_ms: None,
            context: test_context(),
        };
        let path_env = OsString::from("/tmp/nono-bin:/tmp/bash-bin");
        let spec = resolve_execution_spec(&request, Some(path_env.as_os_str()), None);
        assert!(matches!(spec, Err(BashToolError::NonoNotFound)));
    }

    #[test]
    fn resolve_execution_spec_adds_runtime_socket_allow_path() {
        let temp_root = tempfile::tempdir().unwrap();
        let request = BashCommandRequest {
            command: "pwd".to_owned(),
            workdir: temp_root.path().to_path_buf(),
            workspace_root: temp_root.path().to_path_buf(),
            sandbox_mode: BashSandboxMode::Off,
            timeout_ms: None,
            context: test_context(),
        };
        let nono = PathBuf::from("/bin/nono");
        let runtime_dir = temp_root.path().join(".coco-runtime").join("socket-dir");
        let spec = nono_execution_spec(nono.clone(), &request, Some(&runtime_dir));

        assert_eq!(spec.program, nono);
        let mut expected_args = vec![
            OsString::from("run"),
            OsString::from("--silent"),
            OsString::from("--allow"),
            temp_root.path().as_os_str().to_owned(),
        ];
        for default_allow_file in NONO_DEFAULT_ALLOW_FILES {
            expected_args.push(OsString::from("--allow-file"));
            expected_args.push(OsString::from(default_allow_file));
        }
        expected_args.extend([
            OsString::from("--allow"),
            runtime_dir.as_os_str().to_owned(),
            OsString::from("--"),
            OsString::from("bash"),
            OsString::from("-lc"),
            OsString::from("pwd"),
        ]);

        assert_eq!(spec.args, expected_args);
    }

    #[cfg(unix)]
    #[test]
    fn resolve_execution_spec_allows_dev_null_by_default() {
        let temp_root = tempfile::tempdir().unwrap();
        let request = BashCommandRequest {
            command: "pwd".to_owned(),
            workdir: temp_root.path().to_path_buf(),
            workspace_root: temp_root.path().to_path_buf(),
            sandbox_mode: BashSandboxMode::Off,
            timeout_ms: None,
            context: test_context(),
        };
        let spec = nono_execution_spec(PathBuf::from("/bin/nono"), &request, None);

        assert!(
            spec.args
                .windows(2)
                .any(|window| window
                    == [OsString::from("--allow-file"), OsString::from("/dev/null")])
        );
    }

    #[test]
    fn resolve_bash_request_rejects_workdir_outside_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let args = serde_json::json!({
            "command": "pwd",
            "workdir": outside.path(),
        });

        let error = resolve_bash_request(&args, workspace.path(), test_context()).unwrap_err();
        assert!(matches!(error, BashToolError::WorkdirOutsideWorkspace));
    }

    #[tokio::test]
    async fn bash_runtime_off_mode_allows_writes_inside_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime = runtime_tool(temp_tool(), workspace.path().to_path_buf(), test_context());

        let output = crate::with_process_env_async(
            &[("COCO_BASH_SANDBOX", Some(OsStr::new("off")))],
            || async {
                runtime
                    .call(format!(
                        r#"{{"command":"printf 'hello' > trace.txt; cat trace.txt","workdir":"{}"}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(output.contains("exit_status: 0"));
        assert!(output.contains("stdout:\nhello"));
        assert_eq!(
            std::fs::read_to_string(workspace.path().join("trace.txt")).unwrap(),
            "hello"
        );
    }

    #[tokio::test]
    async fn bash_runtime_auto_mode_falls_back_without_nono() {
        let workspace = tempfile::tempdir().unwrap();
        let fake_bin = tempfile::tempdir().unwrap();
        let bash_path = resolve_bash_path_locked().await;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&bash_path, fake_bin.path().join("bash")).unwrap();
        let runtime = runtime_tool(temp_tool(), workspace.path().to_path_buf(), test_context());
        let path_env = OsString::from(fake_bin.path().as_os_str());

        let output = crate::with_process_env_async(
            &[
                ("COCO_BASH_SANDBOX", Some(OsStr::new("auto"))),
                ("PATH", Some(path_env.as_os_str())),
            ],
            || async {
                runtime
                    .call(format!(
                        r#"{{"command":"printf 'fallback'","workdir":"{}"}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(output.contains("exit_status: 0"));
        assert!(output.contains("stdout:\nfallback"));
    }

    #[tokio::test]
    async fn bash_runtime_nono_mode_errors_when_binary_missing() {
        let workspace = tempfile::tempdir().unwrap();
        let fake_bin = tempfile::tempdir().unwrap();
        let bash_path = resolve_bash_path_locked().await;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&bash_path, fake_bin.path().join("bash")).unwrap();
        let runtime = runtime_tool(temp_tool(), workspace.path().to_path_buf(), test_context());
        let path_env = OsString::from(fake_bin.path().as_os_str());

        let error = crate::with_process_env_async(
            &[
                ("COCO_BASH_SANDBOX", Some(OsStr::new("nono"))),
                ("PATH", Some(path_env.as_os_str())),
            ],
            || async {
                runtime
                    .call(format!(
                        r#"{{"command":"printf 'blocked'","workdir":"{}"}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("could not locate `nono`"));
    }

    #[tokio::test]
    async fn bash_runtime_nono_mode_wraps_command_with_allow_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        let fake_bin = tempfile::tempdir().unwrap();
        let observed_args = workspace.path().join("nono-args.txt");
        let bash_path = resolve_bash_path_locked().await;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&bash_path, fake_bin.path().join("bash")).unwrap();
        write_executable_script(
            &fake_bin.path().join("nono"),
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"{}\"\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = \"--\" ]; then\n    shift\n    break\n  fi\n  shift\ndone\nexec \"$@\"\n",
                observed_args.display()
            ),
        );
        let runtime = runtime_tool(temp_tool(), workspace.path().to_path_buf(), test_context());
        let path_env = OsString::from(fake_bin.path().as_os_str());

        let output = crate::with_process_env_async(
            &[
                ("COCO_BASH_SANDBOX", Some(OsStr::new("nono"))),
                ("PATH", Some(path_env.as_os_str())),
            ],
            || async {
                runtime
                    .call(format!(
                        r#"{{"command":"printf 'sandboxed'","workdir":"{}"}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(output.contains("exit_status: 0"));
        let args = std::fs::read_to_string(&observed_args).unwrap();
        let expected_workspace = workspace.path().display().to_string();
        assert!(args.contains("run"));
        assert!(args.contains("--silent"));
        assert!(args.contains("--allow"));
        assert!(args.contains(&expected_workspace));
        #[cfg(unix)]
        assert!(args.contains("--allow-file"));
        #[cfg(unix)]
        assert!(args.contains("/dev/null"));
        assert!(args.contains("--"));
        assert!(args.contains("bash"));
        assert!(args.contains("-lc"));
    }

    #[tokio::test]
    async fn bash_runtime_clears_stale_runtime_env_before_injecting_context() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime = runtime_tool(
            temp_tool(),
            workspace.path().to_path_buf(),
            BashToolContext {
                session_branch: "draft".to_owned(),
                store_path: None,
                cli_bridge: None,
            },
        );

        let output = crate::with_process_env_async(
            &[
                (COCO_SESSION_BRANCH_ENV, Some(OsStr::new("stale-branch"))),
                (COCO_STORE_PATH_ENV, Some(OsStr::new("/tmp/stale-store"))),
                (
                    COCO_CLI_RUNTIME_SOCKET_ENV,
                    Some(OsStr::new("/tmp/stale-runtime.sock")),
                ),
                ("COCO_BASH_SANDBOX", Some(OsStr::new("off"))),
            ],
            || async {
                runtime
                    .call(format!(
                        r#"{{"command":"printf '%s|%s|%s' \"$COCO_BRANCH\" \"${{COCO_STORE_PATH:-}}\" \"${{COCO_CLI_RUNTIME_SOCKET:-}}\"","workdir":"{}"}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(output.contains("stdout:\ndraft||"));
    }

    #[tokio::test]
    async fn coco_cli_runtime_socket_forwards_requests_to_bridge() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime = tempfile::tempdir().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let bridge = crate::BashToolCliBridgeHandle::new(Arc::new(FakeCliBridge {
            requests: requests.clone(),
        }));
        let runtime_store = workspace.path().join("runtime-store");
        let context = BashToolContext {
            session_branch: "draft".to_owned(),
            store_path: Some(runtime_store.clone()),
            cli_bridge: Some(bridge),
        };
        let server = crate::with_process_env_async(
            &[("XDG_RUNTIME_DIR", Some(runtime.path().as_os_str()))],
            || async { start_coco_cli_runtime_server(workspace.path(), &context).await },
        )
        .await
        .unwrap()
        .expect("runtime server should be started");

        let request = CocoCliRuntimeRequest {
            args: vec!["prompt".to_owned(), "hello".to_owned()],
            stdin: b"stdin".to_vec(),
            branch_env: Some("draft".to_owned()),
            store_path_env: Some(runtime_store.display().to_string()),
        };
        let payload = serde_json::to_vec(&request).unwrap();

        let mut stream = UnixStream::connect(&server.socket_path).await.unwrap();
        stream.write_all(&payload).await.unwrap();
        stream.shutdown().await.unwrap();
        let mut output = Vec::new();
        stream.read_to_end(&mut output).await.unwrap();
        let response = serde_json::from_slice::<CocoCliRuntimeResponse>(&output).unwrap();

        assert_eq!(response.exit_code, 0);
        assert_eq!(response.stdout, "delegated");
        assert!(response.stderr.is_empty());

        let forwarded = requests.lock().await;
        assert_eq!(forwarded.as_slice(), &[request]);
        assert!(server.socket_path.starts_with(runtime.path().join("coco")));
        assert!(!server.socket_path.starts_with(workspace.path()));

        server.shutdown().await.unwrap();
    }
}
