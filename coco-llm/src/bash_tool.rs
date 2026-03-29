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

#[derive(Debug, Clone)]
struct BashCommandRequest {
    command: String,
    workdir: PathBuf,
    timeout_ms: Option<u64>,
}

#[derive(Debug, Snafu)]
pub enum BashToolError {
    #[snafu(display("failed to resolve workspace root: {source}"))]
    ResolveWorkspaceRoot { source: io::Error },

    #[snafu(display("failed to canonicalize workspace root: {source}"))]
    CanonicalizeWorkspaceRoot { source: io::Error },

    #[snafu(display("bash tool expects a JSON object input"))]
    InvalidInputType,

    #[snafu(display("bash tool requires a string field `command`"))]
    MissingCommand,

    #[snafu(display("bash tool could not resolve workdir: {source}"))]
    ResolveWorkdir { source: io::Error },

    #[snafu(display("bash tool workdir must point to an existing directory"))]
    InvalidWorkdir,

    #[snafu(display("bash tool arguments must be valid JSON: {source}"))]
    ParseArgs { source: serde_json::Error },

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
    let path = std::env::current_dir().context(ResolveWorkspaceRootSnafu)?;
    path.canonicalize().context(CanonicalizeWorkspaceRootSnafu)
}

pub fn runtime_tool(definition: Tool, workspace_root: PathBuf) -> Box<dyn rig::tool::ToolDyn> {
    Box::new(BashToolRuntime {
        definition,
        workspace_root,
    })
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

    Ok(BashCommandRequest {
        command: command.to_owned(),
        workdir,
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

async fn execute_bash_command(
    request: BashCommandRequest,
) -> std::result::Result<String, BashToolError> {
    let mut child = Command::new("bash")
        .arg("-lc")
        .arg(&request.command)
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
