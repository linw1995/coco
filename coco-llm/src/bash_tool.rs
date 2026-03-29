use std::ffi::{OsStr, OsString};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use coco_mem::Tool;
use serde_json::Value;
use snafu::prelude::*;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

#[derive(Debug, Clone)]
pub struct BashToolRuntime {
    definition: Tool,
    workspace_root: PathBuf,
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
}

#[derive(Debug, Snafu)]
pub enum BashToolError {
    #[snafu(display("failed to resolve workspace root: {source}"))]
    ResolveWorkspaceRoot { source: io::Error },

    #[snafu(display("failed to canonicalize workspace root: {source}"))]
    CanonicalizeWorkspaceRoot { source: io::Error },

    #[snafu(display(
        "bash tool sandbox mode must be one of \"auto\", \"nono\", or \"off\", got {value:?}"
    ))]
    InvalidSandboxMode { value: String },

    #[snafu(display("bash tool workspace override could not resolve: {source}"))]
    ResolveWorkspaceOverride { source: io::Error },

    #[snafu(display("bash tool workspace override must point to an existing directory"))]
    InvalidWorkspaceOverride,

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

pub fn runtime_tool(definition: Tool, workspace_root: PathBuf) -> Box<dyn rig::tool::ToolDyn> {
    Box::new(BashToolRuntime {
        definition,
        workspace_root,
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

fn resolve_bash_request(
    args: &Value,
    workspace_root: &Path,
) -> std::result::Result<BashCommandRequest, BashToolError> {
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
    if !path_is_within_workspace(&workdir, workspace_root) {
        return WorkdirOutsideWorkspaceSnafu.fail();
    }

    Ok(BashCommandRequest {
        command: command.to_owned(),
        workdir,
        workspace_root: workspace_root.to_path_buf(),
        sandbox_mode: resolve_sandbox_mode()?,
        timeout_ms,
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

fn nono_execution_spec(nono: PathBuf, request: &BashCommandRequest) -> ExecutionSpec {
    ExecutionSpec {
        program: nono,
        args: vec![
            OsString::from("run"),
            OsString::from("--allow"),
            request.workspace_root.as_os_str().to_owned(),
            OsString::from("--"),
            OsString::from("bash"),
            OsString::from("-lc"),
            OsString::from(request.command.to_owned()),
        ],
    }
}

fn resolve_execution_spec(
    request: &BashCommandRequest,
    path_env: Option<&OsStr>,
) -> std::result::Result<ExecutionSpec, BashToolError> {
    match request.sandbox_mode {
        BashSandboxMode::Off => Ok(bash_execution_spec(&request.command)),
        BashSandboxMode::Auto => match find_program_in_path("nono", path_env) {
            Some(nono) => Ok(nono_execution_spec(nono, request)),
            None => Ok(bash_execution_spec(&request.command)),
        },
        BashSandboxMode::Nono => {
            let nono = find_program_in_path("nono", path_env).context(NonoNotFoundSnafu)?;
            Ok(nono_execution_spec(nono, request))
        }
    }
}

async fn execute_bash_command(
    request: BashCommandRequest,
) -> std::result::Result<String, BashToolError> {
    let execution = resolve_execution_spec(&request, None)?;
    let mut child = Command::new(&execution.program)
        .args(&execution.args)
        .current_dir(&request.workdir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context(SpawnProcessSnafu)?;

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
        Box::pin(async move {
            let result = async {
                let args: Value = serde_json::from_str(&args).context(ParseArgsSnafu)?;
                let request = resolve_bash_request(&args, &workspace_root)?;
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
    use std::sync::OnceLock;

    use tokio::sync::Mutex;

    use super::*;

    async fn with_env_async<T, F, Fut>(entries: &[(&str, Option<&OsStr>)], run: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
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

        let resolved = with_env_async(
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

    #[test]
    fn resolve_execution_spec_uses_nono_when_available_in_nono_mode() {
        let temp_root = tempfile::tempdir().unwrap();
        let request = BashCommandRequest {
            command: "pwd".to_owned(),
            workdir: temp_root.path().to_path_buf(),
            workspace_root: temp_root.path().to_path_buf(),
            sandbox_mode: BashSandboxMode::Nono,
            timeout_ms: None,
        };
        let path_env = OsString::from("/tmp/nono-bin:/tmp/bash-bin");
        let spec = resolve_execution_spec(&request, Some(path_env.as_os_str()));
        assert!(matches!(spec, Err(BashToolError::NonoNotFound)));
    }

    #[test]
    fn resolve_bash_request_rejects_workdir_outside_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let args = serde_json::json!({
            "command": "pwd",
            "workdir": outside.path(),
        });

        let error = resolve_bash_request(&args, workspace.path()).unwrap_err();
        assert!(matches!(error, BashToolError::WorkdirOutsideWorkspace));
    }

    #[tokio::test]
    async fn bash_runtime_off_mode_allows_writes_inside_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime = runtime_tool(temp_tool(), workspace.path().to_path_buf());

        let output = with_env_async(
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
        let bash_path = resolve_bash_path();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&bash_path, fake_bin.path().join("bash")).unwrap();
        let runtime = runtime_tool(temp_tool(), workspace.path().to_path_buf());
        let path_env = OsString::from(fake_bin.path().as_os_str());

        let output = with_env_async(
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
        let bash_path = resolve_bash_path();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&bash_path, fake_bin.path().join("bash")).unwrap();
        let runtime = runtime_tool(temp_tool(), workspace.path().to_path_buf());
        let path_env = OsString::from(fake_bin.path().as_os_str());

        let error = with_env_async(
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
        let bash_path = resolve_bash_path();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&bash_path, fake_bin.path().join("bash")).unwrap();
        write_executable_script(
            &fake_bin.path().join("nono"),
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"{}\"\nshift 4\nexec \"$@\"\n",
                observed_args.display()
            ),
        );
        let runtime = runtime_tool(temp_tool(), workspace.path().to_path_buf());
        let path_env = OsString::from(fake_bin.path().as_os_str());

        let output = with_env_async(
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
        assert!(args.contains("--allow"));
        assert!(args.contains(&expected_workspace));
        assert!(args.contains("--"));
        assert!(args.contains("bash"));
        assert!(args.contains("-lc"));
    }
}
