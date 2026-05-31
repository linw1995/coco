use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use coco_mem::Tool;
use portable_pty::{ChildKiller, CommandBuilder, NativePtySystem, PtySize, PtySystem};
use serde_json::Value;
use snafu::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::sync::{Mutex, Notify};

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

use crate::{
    COCO_CLI_RUNTIME_SOCKET_ENV, COCO_COMMAND_SHIM_MODE_ENV, COCO_PARENT_TOOL_USE_ID_ENV,
    COCO_SESSION_BRANCH_ENV, COCO_SESSION_ROLE_ENV, COCO_SKILL_DIR_ENV, COCO_SKILL_NAME_ENV,
    COCO_SKILL_PERSIST_DIR_ENV, COCO_STORE_PATH_ENV, CocoCliRuntimeRequest, CocoCliRuntimeResponse,
    ToolInvocationContext, ToolRuntimeEnv,
};

#[derive(Debug, Clone)]
pub struct UnifiedExecToolRuntime {
    definition: Tool,
    workspace_root: PathBuf,
    context: ToolRuntimeEnv,
    kind: UnifiedExecToolKind,
    sessions: UnifiedExecSessionStoreHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnifiedExecToolKind {
    ExecCommand,
    WriteStdin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecSandboxMode {
    Auto,
    Nono,
    Off,
}

#[derive(Debug, Clone)]
struct ExecCommandRequest {
    cmd: String,
    workdir: PathBuf,
    workspace_root: PathBuf,
    sandbox_mode: ExecSandboxMode,
    shell: OsString,
    tty: bool,
    yield_time_ms: u64,
    max_output_tokens: Option<u64>,
    context: ToolRuntimeEnv,
    parent_tool_use_id: Option<String>,
}

#[derive(Debug, Clone)]
struct WriteStdinRequest {
    session_id: String,
    chars: String,
    yield_time_ms: u64,
    max_output_tokens: Option<u64>,
}

#[derive(Debug)]
struct CocoCliRuntimeServer {
    socket_dir: PathBuf,
    socket_path: PathBuf,
    task: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug, Default)]
pub struct UnifiedExecSessionStoreHandle {
    inner: Arc<Mutex<UnifiedExecSessionStore>>,
}

impl Clone for UnifiedExecSessionStoreHandle {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Debug, Default)]
struct UnifiedExecSessionStore {
    sessions: HashMap<String, UnifiedExecSession>,
    by_branch: HashMap<String, HashSet<String>>,
}

impl UnifiedExecSessionStore {
    fn insert_session(&mut self, session_id: String, session: UnifiedExecSession) {
        self.by_branch
            .entry(session.branch.clone())
            .or_default()
            .insert(session_id.clone());
        self.sessions.insert(session_id, session);
    }

    fn remove_session(&mut self, session_id: &str) -> Option<UnifiedExecSession> {
        let session = self.sessions.remove(session_id)?;
        if let Some(session_ids) = self.by_branch.get_mut(&session.branch) {
            session_ids.remove(session_id);
            if session_ids.is_empty() {
                self.by_branch.remove(&session.branch);
            }
        }
        Some(session)
    }

    fn remove_branch(&mut self, branch: &str) -> Vec<UnifiedExecSession> {
        let Some(session_ids) = self.by_branch.remove(branch) else {
            return vec![];
        };
        session_ids
            .into_iter()
            .filter_map(|session_id| self.sessions.remove(&session_id))
            .collect()
    }

    fn remove_all(&mut self) -> Vec<UnifiedExecSession> {
        self.by_branch.clear();
        self.sessions.drain().map(|(_, session)| session).collect()
    }
}

impl UnifiedExecSessionStoreHandle {
    pub async fn remove_branch(&self, branch: &str) -> usize {
        let sessions = {
            let mut store = self.inner.lock().await;
            store.remove_branch(branch)
        };
        let count = sessions.len();
        drop(sessions);
        count
    }

    pub async fn remove_all(&self) -> usize {
        let sessions = {
            let mut store = self.inner.lock().await;
            store.remove_all()
        };
        let count = sessions.len();
        drop(sessions);
        count
    }
}

#[derive(Debug, Default)]
struct UnifiedExecOutputBuffer {
    start_offset: usize,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct UnifiedExecSession {
    branch: String,
    stdin: Option<UnifiedExecSessionStdin>,
    stdout: Arc<Mutex<UnifiedExecOutputBuffer>>,
    stderr: Arc<Mutex<UnifiedExecOutputBuffer>>,
    stdout_cursor: usize,
    stderr_cursor: usize,
    notify: Arc<Notify>,
    wait_task: Option<tokio::task::JoinHandle<io::Result<Option<i32>>>>,
    stdout_task: Option<tokio::task::JoinHandle<io::Result<()>>>,
    stderr_task: Option<tokio::task::JoinHandle<io::Result<()>>>,
    pty_child_killer: Option<Box<dyn ChildKiller + Send + Sync>>,
    runtime_server: Option<CocoCliRuntimeServer>,
    _coco_command: Option<CocoCommandPathInjection>,
}

enum UnifiedExecSessionStdin {
    Blocking(Box<dyn Write + Send>),
}

impl std::fmt::Debug for UnifiedExecSessionStdin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Blocking(_) => formatter.write_str("Blocking(..)"),
        }
    }
}

impl Drop for UnifiedExecSession {
    fn drop(&mut self) {
        if let Some(mut killer) = self.pty_child_killer.take() {
            let _ = killer.kill();
        }
        if let Some(wait_task) = &self.wait_task {
            wait_task.abort();
        }
        if let Some(stdout_task) = &self.stdout_task {
            stdout_task.abort();
        }
        if let Some(stderr_task) = &self.stderr_task {
            stderr_task.abort();
        }
    }
}

const MAX_RUNTIME_SOCKET_PATH_LEN: usize = 107;
const DEFAULT_YIELD_TIME_MS: u64 = 1_000;
const SESSION_POLL_INTERVAL_MS: u64 = 25;
const COCO_DAEMON_SOCKET_ENV: &str = "COCO_DAEMON_SOCKET";
const COCO_EXEC_SANDBOX_ENV: &str = "COCO_EXEC_SANDBOX";
const COCO_EXEC_WORKSPACE_ENV: &str = "COCO_EXEC_WORKSPACE";
const COCO_LOG_DIR_ENV: &str = "COCO_LOG_DIR";
const UV_CACHE_DIR_ENV: &str = "UV_CACHE_DIR";
const UV_PYTHON_INSTALL_DIR_ENV: &str = "UV_PYTHON_INSTALL_DIR";
const TMPDIR_ENV: &str = "TMPDIR";
const XDG_CACHE_HOME_ENV: &str = "XDG_CACHE_HOME";
const XDG_CONFIG_HOME_ENV: &str = "XDG_CONFIG_HOME";
const XDG_DATA_HOME_ENV: &str = "XDG_DATA_HOME";
const XDG_BIN_HOME_ENV: &str = "XDG_BIN_HOME";
const XDG_STATE_HOME_ENV: &str = "XDG_STATE_HOME";
const HOME_ENV: &str = "HOME";
const XDG_CONFIG_ALLOW_DIRS: &[&str] = &["uv"];
const XDG_DATA_ALLOW_DIRS: &[&str] = &["uv"];

#[cfg(unix)]
const NONO_DEFAULT_ALLOW_FILES: &[&str] = &["/dev/null"];

#[cfg(not(unix))]
const NONO_DEFAULT_ALLOW_FILES: &[&str] = &[];

#[cfg(unix)]
const NONO_DEFAULT_READ_PATHS: &[&str] = &["/lib", "/lib64", "/nix/store"];

#[cfg(not(unix))]
const NONO_DEFAULT_READ_PATHS: &[&str] = &[];

#[derive(Debug, Snafu)]
pub enum UnifiedExecToolError {
    #[snafu(display("failed to resolve workspace root: {source}"))]
    ResolveWorkspaceRoot { source: io::Error },

    #[snafu(display("failed to canonicalize workspace root: {source}"))]
    CanonicalizeWorkspaceRoot { source: io::Error },

    #[snafu(display("failed to resolve runtime root: {source}"))]
    ResolveRuntimeRoot { source: io::Error },

    #[snafu(display("failed to resolve current executable for coco command injection: {source}"))]
    ResolveCurrentExe { source: io::Error },

    #[snafu(display(
        "unified exec tool sandbox mode must be one of \"auto\", \"nono\", or \"off\", got {value:?}"
    ))]
    InvalidSandboxMode { value: String },

    #[snafu(display("unified exec tool workspace override could not resolve: {source}"))]
    ResolveWorkspaceOverride { source: io::Error },

    #[snafu(display("unified exec tool workspace override must point to an existing directory"))]
    InvalidWorkspaceOverride,

    #[snafu(display(
        "unified exec tool runtime root must stay isolated from the configured workspace"
    ))]
    RuntimeRootOverlapsWorkspace,

    #[snafu(display("unified exec tool expects a JSON object input"))]
    InvalidInputType,

    #[snafu(display("exec_command requires a string field `cmd`"))]
    MissingCommand,

    #[snafu(display("write_stdin requires a string field `session_id`"))]
    MissingSessionId,

    #[snafu(display("unified exec tool input includes unsupported field `{field}`"))]
    UnsupportedInputField { field: String },

    #[snafu(display("unified exec tool session {session_id:?} was not found"))]
    SessionNotFound { session_id: String },

    #[snafu(display("unified exec tool could not resolve workdir: {source}"))]
    ResolveWorkdir { source: io::Error },

    #[snafu(display("unified exec tool workdir must point to an existing directory"))]
    InvalidWorkdir,

    #[snafu(display("unified exec tool workdir must stay within the configured workspace"))]
    WorkdirOutsideWorkspace,

    #[snafu(display("unified exec tool arguments must be valid JSON: {source}"))]
    ParseArgs { source: serde_json::Error },

    #[snafu(display("unified exec tool could not locate `nono` in PATH"))]
    NonoNotFound,

    #[snafu(display("unified exec tool could not spawn process: {source}"))]
    SpawnProcess { source: io::Error },

    #[snafu(display("unified exec tool could not wait for process: {source}"))]
    WaitProcess { source: io::Error },

    #[snafu(display("unified exec tool process wait task failed: {source}"))]
    JoinProcess { source: tokio::task::JoinError },

    #[snafu(display("unified exec tool stdin write task failed: {source}"))]
    JoinStdin { source: tokio::task::JoinError },

    #[snafu(display("unified exec tool could not join stdout task: {source}"))]
    JoinStdout { source: tokio::task::JoinError },

    #[snafu(display("unified exec tool could not join stderr task: {source}"))]
    JoinStderr { source: tokio::task::JoinError },

    #[snafu(display("unified exec tool could not read stdout: {source}"))]
    ReadStdout { source: io::Error },

    #[snafu(display("unified exec tool could not read stderr: {source}"))]
    ReadStderr { source: io::Error },

    #[snafu(display("unified exec tool session stdin is closed"))]
    StdinClosed,

    #[snafu(display(
        "unified exec tool session does not support stdin writes; start exec_command with tty=true"
    ))]
    StdinUnavailable,

    #[snafu(display("unified exec tool could not write to session stdin: {source}"))]
    WriteStdin { source: io::Error },

    #[snafu(display("unified exec tool could not open PTY: {message}"))]
    OpenPty { message: String },

    #[snafu(display("unified exec tool could not clone PTY reader: {message}"))]
    ClonePtyReader { message: String },

    #[snafu(display("unified exec tool could not take PTY writer: {message}"))]
    TakePtyWriter { message: String },

    #[snafu(display("unified exec tool could not spawn PTY process: {message}"))]
    SpawnPtyProcess { message: String },

    #[snafu(display("unified exec tool could not bind coco-cli runtime socket: {source}"))]
    BindRuntimeSocket { source: io::Error },

    #[snafu(display("unified exec tool could not create coco command shim directory: {source}"))]
    CreateCocoCommandShimDir { source: io::Error },

    #[snafu(display("unified exec tool could not create coco command shim at {path:?}: {source}"))]
    CreateCocoCommandShim { path: PathBuf, source: io::Error },

    #[snafu(display(
        "unified exec tool runtime socket path is too long for a Unix socket: {path:?} ({length} bytes, max {max})"
    ))]
    RuntimeSocketPathTooLong {
        path: PathBuf,
        length: usize,
        max: usize,
    },

    #[snafu(display("unified exec tool runtime socket server task failed: {source}"))]
    JoinRuntimeSocketTask { source: tokio::task::JoinError },
}

pub fn resolve_workspace_root() -> std::result::Result<PathBuf, UnifiedExecToolError> {
    let current_dir = std::env::current_dir().context(ResolveWorkspaceRootSnafu)?;
    let workspace_raw = std::env::var_os(COCO_EXEC_WORKSPACE_ENV);
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

#[cfg(test)]
fn runtime(
    definition: Tool,
    workspace_root: PathBuf,
    context: ToolRuntimeEnv,
) -> UnifiedExecToolRuntime {
    runtime_with_sessions(
        definition,
        workspace_root,
        context,
        UnifiedExecToolKind::ExecCommand,
        UnifiedExecSessionStoreHandle::default(),
    )
}

pub fn runtime_with_sessions(
    definition: Tool,
    workspace_root: PathBuf,
    context: ToolRuntimeEnv,
    kind: UnifiedExecToolKind,
    sessions: UnifiedExecSessionStoreHandle,
) -> UnifiedExecToolRuntime {
    let workspace_root = canonicalize_existing_path(&workspace_root);
    UnifiedExecToolRuntime {
        definition,
        workspace_root,
        context,
        kind,
        sessions,
    }
}

pub fn session_store() -> UnifiedExecSessionStoreHandle {
    UnifiedExecSessionStoreHandle::default()
}

fn next_session_id(store: &UnifiedExecSessionStore) -> String {
    loop {
        let candidate = format!("exec-{}", nanoid::nanoid!());
        if !store.sessions.contains_key(&candidate) {
            return candidate;
        }
    }
}

#[cfg(test)]
pub fn runtime_tool(
    definition: Tool,
    workspace_root: PathBuf,
    context: ToolRuntimeEnv,
) -> Box<dyn rig::tool::ToolDyn> {
    Box::new(runtime(definition, workspace_root, context))
}

fn resolve_sandbox_mode() -> std::result::Result<ExecSandboxMode, UnifiedExecToolError> {
    let Some(value) = std::env::var_os(COCO_EXEC_SANDBOX_ENV) else {
        return Ok(ExecSandboxMode::Nono);
    };
    match value.to_string_lossy().as_ref() {
        "auto" => Ok(ExecSandboxMode::Auto),
        "nono" => Ok(ExecSandboxMode::Nono),
        "off" => Ok(ExecSandboxMode::Off),
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

fn default_shell() -> OsString {
    std::env::var_os("SHELL")
        .filter(|shell| !shell.is_empty())
        .unwrap_or_else(|| OsString::from("bash"))
}

fn resolve_runtime_root(
    workspace_root: &Path,
) -> std::result::Result<PathBuf, UnifiedExecToolError> {
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

fn resolve_exec_command_request(
    args: &Value,
    workspace_root: &Path,
    context: ToolRuntimeEnv,
) -> std::result::Result<ExecCommandRequest, UnifiedExecToolError> {
    let workspace_root = canonicalize_existing_path(workspace_root);
    validate_tool_input_fields(
        args,
        &[
            "cmd",
            "workdir",
            "shell",
            "tty",
            "yield_time_ms",
            "max_output_tokens",
        ],
    )?;
    let object = args.as_object().context(InvalidInputTypeSnafu)?;
    let cmd = object
        .get("cmd")
        .and_then(Value::as_str)
        .context(MissingCommandSnafu)?;
    let shell = object
        .get("shell")
        .and_then(Value::as_str)
        .map(OsString::from)
        .unwrap_or_else(default_shell);
    let tty = object.get("tty").and_then(Value::as_bool).unwrap_or(false);

    let yield_time_ms = object
        .get("yield_time_ms")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_YIELD_TIME_MS);
    let max_output_tokens = object.get("max_output_tokens").and_then(Value::as_u64);

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

    Ok(ExecCommandRequest {
        cmd: cmd.to_owned(),
        workdir,
        workspace_root,
        sandbox_mode: resolve_sandbox_mode()?,
        shell,
        tty,
        yield_time_ms,
        max_output_tokens,
        context,
        parent_tool_use_id: None,
    })
}

fn validate_tool_input_fields(
    args: &Value,
    allowed_fields: &[&str],
) -> std::result::Result<(), UnifiedExecToolError> {
    let object = args.as_object().context(InvalidInputTypeSnafu)?;
    if let Some(field) = object
        .keys()
        .find(|field| !allowed_fields.contains(&field.as_str()))
    {
        return UnsupportedInputFieldSnafu {
            field: field.clone(),
        }
        .fail();
    }
    Ok(())
}

fn resolve_write_stdin_request(
    args: &Value,
) -> std::result::Result<WriteStdinRequest, UnifiedExecToolError> {
    validate_tool_input_fields(
        args,
        &["session_id", "chars", "yield_time_ms", "max_output_tokens"],
    )?;
    let object = args.as_object().context(InvalidInputTypeSnafu)?;
    let session_id = object
        .get("session_id")
        .and_then(Value::as_str)
        .context(MissingSessionIdSnafu)?
        .to_owned();
    let chars = object
        .get("chars")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let yield_time_ms = object
        .get("yield_time_ms")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_YIELD_TIME_MS);
    let max_output_tokens = object.get("max_output_tokens").and_then(Value::as_u64);

    Ok(WriteStdinRequest {
        session_id,
        chars: chars.to_owned(),
        yield_time_ms,
        max_output_tokens,
    })
}

fn format_exec_result(
    session_id: Option<&str>,
    exit_status: Option<i32>,
    stdout: String,
    stderr: String,
    running: bool,
) -> String {
    let status = match (running, exit_status) {
        (true, _) => "running".to_owned(),
        (false, Some(code)) => code.to_string(),
        (false, None) => "terminated_by_signal".to_owned(),
    };
    let mut output = String::new();
    match (running, session_id) {
        (true, Some(session_id)) => {
            output.push_str(&format!("Process running with session ID {session_id}\n"));
            output.push_str(&format!("session_id: {session_id}\n"));
        }
        (false, _) => match exit_status {
            Some(code) => output.push_str(&format!("Process exited with code {code}\n")),
            None => output.push_str("Process terminated by signal\n"),
        },
        (true, None) => output.push_str("Process running\n"),
    }
    output.push_str(&format!(
        "exit_status: {status}\nstdout:\n{stdout}\nstderr:\n{stderr}",
    ));
    output
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExecutionSpec {
    program: PathBuf,
    args: Vec<OsString>,
    nono: Option<PathBuf>,
}

#[derive(Debug)]
struct CocoCommandPathInjection {
    root_dir: PathBuf,
    path_entries: Vec<PathBuf>,
    extra_allow_paths: Vec<PathBuf>,
    log_dir: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CocoCommandShimMode {
    Enabled,
    Disabled,
}

impl Drop for CocoCommandPathInjection {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root_dir);
    }
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

fn shell_execution_args(command: &str) -> Vec<OsString> {
    vec![OsString::from("-c"), OsString::from(command.to_owned())]
}

fn command_fingerprint(command: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in command.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn shell_execution_spec(request: &ExecCommandRequest) -> ExecutionSpec {
    ExecutionSpec {
        program: PathBuf::from(&request.shell),
        args: shell_execution_args(&request.cmd),
        nono: None,
    }
}

fn create_command_alias(target: &Path, alias_path: &Path) -> std::result::Result<(), io::Error> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target, alias_path)
    }

    #[cfg(not(unix))]
    {
        std::fs::copy(target, alias_path).map(|_| ())
    }
}

fn push_unique_path(paths: &mut Vec<PathBuf>, path: PathBuf) {
    if !paths.iter().any(|existing| existing == &path) {
        paths.push(path);
    }
}

fn current_active_skill_persistent_directory(request: &ExecCommandRequest) -> Option<&Path> {
    request
        .context
        .active_skill
        .as_ref()
        .map(|active_skill| active_skill.persistent_directory.as_path())
}

fn uv_python_install_dir_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Some(path) = std::env::var_os(UV_PYTHON_INSTALL_DIR_ENV).filter(|path| !path.is_empty())
    {
        push_unique_path(&mut candidates, PathBuf::from(path));
    }
    if let Some(path) = std::env::var_os(XDG_DATA_HOME_ENV).filter(|path| !path.is_empty()) {
        push_unique_path(
            &mut candidates,
            PathBuf::from(path).join("uv").join("python"),
        );
    }
    if let Some(path) = std::env::var_os(HOME_ENV).filter(|path| !path.is_empty()) {
        push_unique_path(
            &mut candidates,
            PathBuf::from(path)
                .join(".local")
                .join("share")
                .join("uv")
                .join("python"),
        );
    }

    candidates
}

fn uv_python_install_allow_paths() -> Vec<PathBuf> {
    let mut allow_paths = Vec::new();
    for candidate in uv_python_install_dir_candidates() {
        if candidate.exists() {
            push_unique_path(&mut allow_paths, canonicalize_existing_path(&candidate));
        }
    }
    allow_paths
}

fn path_entries_with_source(
    entries: &[PathBuf],
    source_path_env: Option<OsString>,
) -> Vec<PathBuf> {
    entries
        .iter()
        .cloned()
        .chain(
            source_path_env
                .as_ref()
                .into_iter()
                .flat_map(std::env::split_paths),
        )
        .collect()
}

fn join_path_entries(entries: impl IntoIterator<Item = PathBuf>) -> OsString {
    let entries = entries.into_iter().collect::<Vec<_>>();
    std::env::join_paths(&entries).unwrap_or_else(|_| {
        let separator = {
            #[cfg(unix)]
            {
                ":"
            }
            #[cfg(windows)]
            {
                ";"
            }
        };

        let mut entries = entries.into_iter();
        let Some(first) = entries.next() else {
            return OsString::new();
        };
        entries.fold(first.into_os_string(), |mut path_env, entry| {
            path_env.push(separator);
            path_env.push(entry);
            path_env
        })
    })
}

fn coco_cli_binary_name() -> &'static str {
    #[cfg(windows)]
    {
        "coco.exe"
    }

    #[cfg(not(windows))]
    {
        "coco"
    }
}

fn resolve_coco_cli_executable() -> std::result::Result<PathBuf, UnifiedExecToolError> {
    let current_exe = std::env::current_exe().context(ResolveCurrentExeSnafu)?;
    let current_exe = canonicalize_existing_path(&current_exe);
    let binary_name = coco_cli_binary_name();

    if current_exe
        .file_name()
        .is_some_and(|name| name == OsStr::new(binary_name))
    {
        return Ok(current_exe);
    }

    let mut candidates = Vec::new();
    if let Some(parent) = current_exe.parent() {
        candidates.push(parent.join(binary_name));
        if let Some(grandparent) = parent.parent() {
            candidates.push(grandparent.join(binary_name));
        }
    }

    for candidate in candidates {
        if candidate.is_file() {
            return Ok(canonicalize_existing_path(&candidate));
        }
    }

    Ok(current_exe)
}

fn prepare_coco_command_path_injection(
    workspace_root: &Path,
    mode: CocoCommandShimMode,
    include_uv_path: bool,
) -> std::result::Result<CocoCommandPathInjection, UnifiedExecToolError> {
    let runtime_root = resolve_runtime_root(workspace_root)?;
    std::fs::create_dir_all(&runtime_root).context(CreateCocoCommandShimDirSnafu)?;

    let root_dir = next_runtime_socket_dir(&runtime_root);
    let bin_dir = root_dir.join("bin");
    std::fs::create_dir_all(&bin_dir).context(CreateCocoCommandShimDirSnafu)?;
    let log_dir = root_dir.join("logs");
    std::fs::create_dir_all(&log_dir).context(CreateCocoCommandShimDirSnafu)?;

    let current_exe = resolve_coco_cli_executable()?;
    let alias_path = bin_dir.join("coco");
    create_command_alias(&current_exe, &alias_path).context(CreateCocoCommandShimSnafu {
        path: alias_path.clone(),
    })?;

    let mut extra_allow_paths = vec![bin_dir.clone(), log_dir.clone()];
    if matches!(mode, CocoCommandShimMode::Enabled)
        && let Some(parent) = current_exe.parent()
        && !path_is_within_workspace(parent, workspace_root)
    {
        extra_allow_paths.push(parent.to_path_buf());
    }
    let source_path_env = std::env::var_os("PATH");
    let mut extra_path_entries = vec![bin_dir.clone()];
    if include_uv_path && let Some(uv_path) = find_program_in_path("uv", source_path_env.as_deref())
    {
        let uv_path = canonicalize_existing_path(&uv_path);
        if let Some(parent) = uv_path.parent() {
            extra_path_entries.push(parent.to_path_buf());
            push_unique_path(&mut extra_allow_paths, parent.to_path_buf());
        }
        for allow_path in uv_python_install_allow_paths() {
            push_unique_path(&mut extra_allow_paths, allow_path);
        }
    }

    Ok(CocoCommandPathInjection {
        root_dir,
        path_entries: path_entries_with_source(&extra_path_entries, source_path_env),
        extra_allow_paths,
        log_dir,
    })
}

#[derive(Clone, Debug)]
struct SkillUvRuntimeDirs {
    env_vars: Vec<(&'static str, PathBuf)>,
    allow_paths: Vec<PathBuf>,
    xdg_bin_dir: PathBuf,
}

impl SkillUvRuntimeDirs {
    fn created_dirs(&self) -> impl Iterator<Item = PathBuf> + '_ {
        self.env_vars
            .iter()
            .map(|(_, path)| path.clone())
            .chain(self.allow_paths.iter().cloned())
    }

    fn allow_paths(&self) -> impl Iterator<Item = PathBuf> + '_ {
        self.allow_paths.iter().cloned()
    }

    fn env_vars(&self) -> impl Iterator<Item = (&'static str, PathBuf)> + '_ {
        self.env_vars
            .iter()
            .map(|(name, path)| (*name, path.clone()))
    }

    fn xdg_bin_dir(&self) -> &Path {
        &self.xdg_bin_dir
    }
}

fn prepare_skill_uv_runtime_dirs(
    workspace_root: &Path,
) -> std::result::Result<SkillUvRuntimeDirs, UnifiedExecToolError> {
    let runtime_root = resolve_runtime_root(workspace_root)?.join("skill-runtime");
    let cache_home = env_path_or(XDG_CACHE_HOME_ENV, runtime_root.join(".cache"));
    let config_home = env_path_or(XDG_CONFIG_HOME_ENV, runtime_root.join(".config"));
    let data_home = env_path_or(XDG_DATA_HOME_ENV, runtime_root.join(".local").join("share"));
    let state_home = env_path_or(
        XDG_STATE_HOME_ENV,
        runtime_root.join(".local").join("state"),
    );
    let tmp_dir = env_path_or(TMPDIR_ENV, cache_home.join("coco").join("tmp"));
    let cache_dir = env_path_or(UV_CACHE_DIR_ENV, cache_home.join("uv"));
    let python_install_dir = env_path_or(
        UV_PYTHON_INSTALL_DIR_ENV,
        data_home.join("uv").join("python"),
    );
    let default_xdg_bin_dir = data_home
        .parent()
        .map(|parent| parent.join("bin"))
        .unwrap_or_else(|| data_home.join("bin"));
    let xdg_bin_dir = env_path_or(XDG_BIN_HOME_ENV, default_xdg_bin_dir);

    let env_vars = vec![
        (TMPDIR_ENV, tmp_dir.clone()),
        (UV_CACHE_DIR_ENV, cache_dir.clone()),
        (UV_PYTHON_INSTALL_DIR_ENV, python_install_dir.clone()),
        (XDG_CACHE_HOME_ENV, cache_home.clone()),
        (XDG_CONFIG_HOME_ENV, config_home.clone()),
        (XDG_DATA_HOME_ENV, data_home.clone()),
        (XDG_BIN_HOME_ENV, xdg_bin_dir.clone()),
        (XDG_STATE_HOME_ENV, state_home.clone()),
    ];
    let created_dirs = env_vars
        .iter()
        .map(|(_, path)| path.clone())
        .chain(std::iter::once(xdg_bin_dir.clone()));
    let mut allow_paths = Vec::new();
    for path in created_dirs
        .filter(|path| path != &config_home && path != &data_home)
        .chain(
            XDG_CONFIG_ALLOW_DIRS
                .iter()
                .map(|name| config_home.join(name)),
        )
        .chain(XDG_DATA_ALLOW_DIRS.iter().map(|name| data_home.join(name)))
    {
        push_unique_path(&mut allow_paths, path);
    }

    let dirs = SkillUvRuntimeDirs {
        env_vars,
        allow_paths,
        xdg_bin_dir,
    };
    for dir in dirs.created_dirs() {
        std::fs::create_dir_all(dir).context(CreateCocoCommandShimDirSnafu)?;
    }
    Ok(dirs)
}

fn configured_env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .filter(|path| path != Path::new("/"))
}

fn env_path_or(name: &str, fallback: PathBuf) -> PathBuf {
    configured_env_path(name).unwrap_or(fallback)
}

fn skill_uv_path_entries(
    coco_command: Option<&CocoCommandPathInjection>,
    dirs: &SkillUvRuntimeDirs,
) -> Vec<PathBuf> {
    if let Some(command) = coco_command {
        let mut path_entries = command.path_entries.clone();
        let insert_index = if path_entries.is_empty() { 0 } else { 1 };
        path_entries.insert(insert_index, dirs.xdg_bin_dir().to_path_buf());
        path_entries
    } else {
        path_entries_with_source(
            &[dirs.xdg_bin_dir().to_path_buf()],
            std::env::var_os("PATH"),
        )
    }
}

fn skill_uv_path_env(
    coco_command: Option<&CocoCommandPathInjection>,
    dirs: &SkillUvRuntimeDirs,
) -> OsString {
    join_path_entries(skill_uv_path_entries(coco_command, dirs))
}

fn nono_execution_spec(
    nono: PathBuf,
    request: &ExecCommandRequest,
    extra_allow_paths: &[PathBuf],
) -> ExecutionSpec {
    let mut args = vec![
        OsString::from("run"),
        OsString::from("--silent"),
        OsString::from("--allow"),
        request.workspace_root.as_os_str().to_owned(),
    ];
    for default_read_path in NONO_DEFAULT_READ_PATHS {
        let default_read_path = Path::new(default_read_path);
        if default_read_path.exists() {
            args.push(OsString::from("--read"));
            args.push(default_read_path.as_os_str().to_owned());
        }
    }
    for default_allow_file in NONO_DEFAULT_ALLOW_FILES {
        args.push(OsString::from("--allow-file"));
        args.push(OsString::from(default_allow_file));
    }
    for extra_allow_path in extra_allow_paths {
        args.push(OsString::from("--allow"));
        args.push(extra_allow_path.as_os_str().to_owned());
    }
    args.extend([OsString::from("--"), request.shell.clone()]);
    args.extend(shell_execution_args(&request.cmd));
    ExecutionSpec {
        program: nono.clone(),
        args,
        nono: Some(nono),
    }
}

fn resolve_execution_spec(
    request: &ExecCommandRequest,
    path_env: Option<&OsStr>,
    extra_allow_paths: &[PathBuf],
) -> std::result::Result<ExecutionSpec, UnifiedExecToolError> {
    match request.sandbox_mode {
        ExecSandboxMode::Off => Ok(shell_execution_spec(request)),
        ExecSandboxMode::Auto => match find_program_in_path("nono", path_env) {
            Some(nono) => Ok(nono_execution_spec(nono, request, extra_allow_paths)),
            None => Ok(shell_execution_spec(request)),
        },
        ExecSandboxMode::Nono => {
            let nono = find_program_in_path("nono", path_env).context(NonoNotFoundSnafu)?;
            Ok(nono_execution_spec(nono, request, extra_allow_paths))
        }
    }
}

fn next_runtime_socket_dir(runtime_root: &Path) -> PathBuf {
    runtime_root.join(nanoid::nanoid!(6))
}

fn validate_runtime_socket_path(path: &Path) -> std::result::Result<(), UnifiedExecToolError> {
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
            format!("failed to serialize coco-cli runtime response: {error}"),
        ))
        .unwrap_or_else(|_| {
            br#"{"exit_code":2,"stdout":"","stderr":"failed to serialize coco-cli runtime response"}"#
                .to_vec()
        }),
    }
}

async fn start_coco_cli_runtime_server(
    workspace_root: &Path,
    context: &ToolRuntimeEnv,
) -> std::result::Result<Option<CocoCliRuntimeServer>, UnifiedExecToolError> {
    if !context.cli_bridge.is_available() {
        return Ok(None);
    }
    let cli_bridge = context.cli_bridge.clone();

    let runtime_root = resolve_runtime_root(workspace_root)?;
    std::fs::create_dir_all(&runtime_root).context(BindRuntimeSocketSnafu)?;
    let socket_dir = next_runtime_socket_dir(&runtime_root);
    std::fs::create_dir_all(&socket_dir).context(BindRuntimeSocketSnafu)?;
    let socket_path = socket_dir.join("coco-cli.sock");
    validate_runtime_socket_path(&socket_path)?;
    let listener = UnixListener::bind(&socket_path).context(BindRuntimeSocketSnafu)?;
    tracing::debug!(
        socket_path = %socket_path.display(),
        branch = %context.session_branch,
        role = ?context.session_role,
        "started coco cli runtime socket"
    );
    let task = tokio::spawn(async move {
        loop {
            let accepted = listener.accept().await;
            let (mut stream, _) = match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::warn!(error = %error, "coco cli runtime socket accept failed");
                    break;
                }
            };
            let cli_bridge = cli_bridge.clone();
            tokio::spawn(async move {
                let mut input = Vec::new();
                let response = match stream.read_to_end(&mut input).await {
                    Ok(_) => match serde_json::from_slice::<CocoCliRuntimeRequest>(&input) {
                        Ok(request) => {
                            tracing::debug!(
                                arg_count = request.args.len(),
                                stdin_bytes = request.stdin.len(),
                                session_role = ?request.session_role,
                                "received coco cli runtime request"
                            );
                            match cli_bridge.execute_coco_cli(request).await {
                                Ok(response) => response,
                                Err(error) => {
                                    tracing::warn!(
                                        error = %error,
                                        "coco cli runtime bridge failed"
                                    );
                                    runtime_error_response(1, error.to_string())
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                error = %error,
                                request_bytes = input.len(),
                                "invalid coco cli runtime request"
                            );
                            runtime_error_response(
                                2,
                                format!("invalid coco-cli runtime request: {error}"),
                            )
                        }
                    },
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            "failed to read coco cli runtime request"
                        );
                        runtime_error_response(
                            2,
                            format!("failed to read coco-cli runtime request: {error}"),
                        )
                    }
                };
                let payload = encode_runtime_response(&response);
                if let Err(error) = stream.write_all(&payload).await {
                    tracing::error!(
                        error = %error,
                        "failed to write coco-cli runtime response"
                    );
                } else {
                    tracing::debug!(
                        exit_code = response.exit_code,
                        stdout_bytes = response.stdout.len(),
                        stderr_bytes = response.stderr.len(),
                        "handled coco cli runtime request"
                    );
                }
            });
        }
    });

    Ok(Some(CocoCliRuntimeServer {
        socket_dir,
        socket_path,
        task: Some(task),
    }))
}

impl CocoCliRuntimeServer {
    async fn shutdown(mut self) -> std::result::Result<(), UnifiedExecToolError> {
        if let Some(task) = self.task.take() {
            task.abort();
            match task.await {
                Ok(()) => {}
                Err(source) if source.is_cancelled() => {}
                Err(source) => return Err(UnifiedExecToolError::JoinRuntimeSocketTask { source }),
            }
        }
        let _ = std::fs::remove_file(&self.socket_path);
        let _ = std::fs::remove_dir(&self.socket_dir);
        Ok(())
    }
}

impl Drop for CocoCliRuntimeServer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
        let _ = std::fs::remove_file(&self.socket_path);
        let _ = std::fs::remove_dir(&self.socket_dir);
    }
}

async fn stream_child_pipe<R>(
    reader: Option<R>,
    output: Arc<Mutex<UnifiedExecOutputBuffer>>,
    output_limit_bytes: Option<usize>,
    notify: Arc<Notify>,
) -> io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let Some(mut reader) = reader else {
        return Ok(());
    };
    let mut buffer = [0_u8; 8192];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            notify.notify_waiters();
            return Ok(());
        }
        output
            .lock()
            .await
            .append(&buffer[..read], output_limit_bytes);
        notify.notify_waiters();
    }
}

fn stream_blocking_reader(
    mut reader: Box<dyn Read + Send>,
    output: Arc<Mutex<UnifiedExecOutputBuffer>>,
    output_limit_bytes: Option<usize>,
    notify: Arc<Notify>,
) -> io::Result<()> {
    let mut buffer = [0_u8; 8192];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            notify.notify_waiters();
            return Ok(());
        }
        output
            .blocking_lock()
            .append(&buffer[..read], output_limit_bytes);
        notify.notify_waiters();
    }
}

async fn write_session_stdin(
    mut stdin: UnifiedExecSessionStdin,
    chars: String,
) -> std::result::Result<UnifiedExecSessionStdin, UnifiedExecToolError> {
    match &mut stdin {
        UnifiedExecSessionStdin::Blocking(_) => tokio::task::spawn_blocking(move || {
            let UnifiedExecSessionStdin::Blocking(mut writer) = stdin;
            writer.write_all(chars.as_bytes())?;
            writer.flush()?;
            Ok(UnifiedExecSessionStdin::Blocking(writer))
        })
        .await
        .context(JoinStdinSnafu)?
        .context(WriteStdinSnafu),
    }
}

impl UnifiedExecOutputBuffer {
    fn append(&mut self, bytes: &[u8], limit_bytes: Option<usize>) {
        self.bytes.extend_from_slice(bytes);

        if let Some(limit_bytes) = limit_bytes
            && self.bytes.len() > limit_bytes
        {
            let dropped = self.bytes.len() - limit_bytes;
            self.bytes.drain(..dropped);
            self.start_offset = self.start_offset.saturating_add(dropped);
        }
    }

    fn text_since(&self, cursor: &mut usize) -> String {
        let buffer_end = self.start_offset + self.bytes.len();
        let absolute_start = (*cursor).max(self.start_offset).min(buffer_end);
        let relative_start = absolute_start - self.start_offset;
        *cursor = buffer_end;
        String::from_utf8_lossy(&self.bytes[relative_start..]).into_owned()
    }
}

fn output_limit_bytes(max_output_tokens: Option<u64>) -> Option<usize> {
    max_output_tokens.map(|tokens| tokens.saturating_mul(4).min(usize::MAX as u64) as usize)
}

fn limit_output_text(output: String, max_output_tokens: Option<u64>) -> String {
    let Some(max_output_tokens) = max_output_tokens else {
        return output;
    };
    let max_bytes = max_output_tokens.saturating_mul(4).min(usize::MAX as u64) as usize;
    if max_bytes == 0 || output.len() <= max_bytes {
        return output;
    }

    let mut start = output.len() - max_bytes;
    while !output.is_char_boundary(start) {
        start += 1;
    }
    format!(
        "[output truncated: showing last {} bytes]\n{}",
        output.len() - start,
        &output[start..]
    )
}

async fn read_session_output(
    session: &mut UnifiedExecSession,
    max_output_tokens: Option<u64>,
) -> (String, String) {
    let stdout_guard = session.stdout.lock().await;
    let stdout = stdout_guard.text_since(&mut session.stdout_cursor);
    drop(stdout_guard);

    let stderr_guard = session.stderr.lock().await;
    let stderr = stderr_guard.text_since(&mut session.stderr_cursor);
    drop(stderr_guard);

    (
        limit_output_text(stdout, max_output_tokens),
        limit_output_text(stderr, max_output_tokens),
    )
}

async fn finish_bash_session(
    session_id: String,
    mut session: UnifiedExecSession,
    max_output_tokens: Option<u64>,
) -> std::result::Result<String, UnifiedExecToolError> {
    let wait_task = session
        .wait_task
        .take()
        .expect("unified exec session should have a process wait task");
    let exit_code = wait_task
        .await
        .context(JoinProcessSnafu)?
        .context(WaitProcessSnafu)?;
    session.pty_child_killer.take();

    let stdout_task = session
        .stdout_task
        .take()
        .expect("unified exec session should have a stdout task");
    stdout_task
        .await
        .context(JoinStdoutSnafu)?
        .context(ReadStdoutSnafu)?;

    let stderr_task = session
        .stderr_task
        .take()
        .expect("unified exec session should have a stderr task");
    stderr_task
        .await
        .context(JoinStderrSnafu)?
        .context(ReadStderrSnafu)?;

    let (stdout, stderr) = read_session_output(&mut session, max_output_tokens).await;
    if let Some(runtime_server) = session.runtime_server.take() {
        runtime_server.shutdown().await?;
    }

    tracing::info!(
        session_id,
        exit_code = ?exit_code,
        stdout_bytes = stdout.len(),
        stderr_bytes = stderr.len(),
        "unified exec tool session completed"
    );

    Ok(format_exec_result(
        Some(&session_id),
        exit_code,
        stdout,
        stderr,
        false,
    ))
}

async fn collect_session_until_deadline(
    sessions: UnifiedExecSessionStoreHandle,
    session_id: String,
    yield_time_ms: u64,
    max_output_tokens: Option<u64>,
) -> std::result::Result<String, UnifiedExecToolError> {
    let deadline = Instant::now() + Duration::from_millis(yield_time_ms);
    loop {
        let notify = {
            let mut store = sessions.inner.lock().await;
            let session = store
                .sessions
                .get_mut(&session_id)
                .context(SessionNotFoundSnafu {
                    session_id: session_id.clone(),
                })?;
            if session
                .wait_task
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished)
            {
                let session = store
                    .remove_session(&session_id)
                    .expect("unified exec session should still exist");
                drop(store);
                return finish_bash_session(session_id, session, max_output_tokens).await;
            }
            if Instant::now() >= deadline {
                let (stdout, stderr) = read_session_output(session, max_output_tokens).await;
                return Ok(format_exec_result(
                    Some(&session_id),
                    None,
                    stdout,
                    stderr,
                    true,
                ));
            }
            session.notify.clone()
        };

        let remaining = deadline.saturating_duration_since(Instant::now());
        let poll = Duration::from_millis(SESSION_POLL_INTERVAL_MS).min(remaining);
        let _ = tokio::time::timeout(poll, notify.notified()).await;
    }
}

async fn execute_command(
    request: ExecCommandRequest,
    sessions: UnifiedExecSessionStoreHandle,
) -> std::result::Result<String, UnifiedExecToolError> {
    let coco_command = Some(prepare_coco_command_path_injection(
        &request.workspace_root,
        if request.context.enable_coco_shim {
            CocoCommandShimMode::Enabled
        } else {
            CocoCommandShimMode::Disabled
        },
        request.context.active_skill.is_some(),
    )?);
    let runtime_server = if request.context.enable_coco_shim {
        start_coco_cli_runtime_server(&request.workspace_root, &request.context).await?
    } else {
        None
    };
    let mut extra_allow_paths = coco_command
        .as_ref()
        .map(|command| command.extra_allow_paths.clone())
        .unwrap_or_default();
    if let Some(runtime_server) = &runtime_server {
        extra_allow_paths.push(runtime_server.socket_dir.clone());
    }
    let active_skill_persistent_directory = current_active_skill_persistent_directory(&request);
    let skill_uv_runtime_dirs =
        if let Some(persistent_directory) = &active_skill_persistent_directory {
            std::fs::create_dir_all(persistent_directory).context(CreateCocoCommandShimDirSnafu)?;
            extra_allow_paths.push(persistent_directory.to_path_buf());
            let runtime_dirs = prepare_skill_uv_runtime_dirs(&request.workspace_root)?;
            extra_allow_paths.extend(runtime_dirs.allow_paths());
            Some(runtime_dirs)
        } else {
            None
        };
    if let Some(active_skill) = &request.context.active_skill {
        extra_allow_paths.push(active_skill.directory.clone());
    }
    let path_env = coco_command
        .as_ref()
        .map(|command| join_path_entries(command.path_entries.clone()));
    let execution = resolve_execution_spec(&request, path_env.as_deref(), &extra_allow_paths)?;
    let nono = execution
        .nono
        .as_ref()
        .map(|nono| nono.display().to_string())
        .unwrap_or_default();
    tracing::info!(
        cmd_hash = %command_fingerprint(&request.cmd),
        cmd_len = request.cmd.len(),
        shell = %request.shell.to_string_lossy(),
        nono = %nono,
        arg_count = execution.args.len(),
        workdir = %request.workdir.display(),
        yield_time_ms = request.yield_time_ms,
        tty = request.tty,
        shim_enabled = request.context.enable_coco_shim,
        "starting unified exec tool command"
    );
    if request.tty {
        return execute_pty_command(
            request,
            execution,
            runtime_server,
            coco_command,
            skill_uv_runtime_dirs,
            sessions,
        )
        .await;
    }

    let mut command = Command::new(&execution.program);
    command.kill_on_drop(true);
    command
        .args(&execution.args)
        .current_dir(&request.workdir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(COCO_DAEMON_SOCKET_ENV)
        .env_remove(COCO_SESSION_BRANCH_ENV)
        .env_remove(COCO_SESSION_ROLE_ENV)
        .env_remove(COCO_STORE_PATH_ENV)
        .env_remove(COCO_CLI_RUNTIME_SOCKET_ENV)
        .env_remove(COCO_PARENT_TOOL_USE_ID_ENV)
        .env_remove(COCO_LOG_DIR_ENV)
        .env_remove(COCO_SKILL_NAME_ENV)
        .env_remove(COCO_SKILL_DIR_ENV)
        .env_remove(COCO_SKILL_PERSIST_DIR_ENV)
        .env_remove(UV_CACHE_DIR_ENV)
        .env_remove(UV_PYTHON_INSTALL_DIR_ENV)
        .env_remove(COCO_COMMAND_SHIM_MODE_ENV);
    command.env(COCO_EXEC_WORKSPACE_ENV, &request.workspace_root);
    if let Some(coco_command) = &coco_command {
        command.env("PATH", join_path_entries(coco_command.path_entries.clone()));
        command.env(COCO_LOG_DIR_ENV, &coco_command.log_dir);
        if request.context.enable_coco_shim {
            command
                .env(COCO_SESSION_BRANCH_ENV, &request.context.session_branch)
                .env(COCO_SESSION_ROLE_ENV, request.context.session_role.as_str());
            if let Some(store_path) = &request.context.store_path {
                command.env(COCO_STORE_PATH_ENV, store_path);
            }
            if let Some(runtime_server) = &runtime_server {
                command.env(COCO_CLI_RUNTIME_SOCKET_ENV, &runtime_server.socket_path);
            }
            if let Some(parent_tool_use_id) = &request.parent_tool_use_id {
                command.env(COCO_PARENT_TOOL_USE_ID_ENV, parent_tool_use_id);
            }
        } else {
            command.env(COCO_COMMAND_SHIM_MODE_ENV, "disabled");
        }
    }
    if let Some(active_skill) = &request.context.active_skill {
        command
            .env(COCO_SKILL_NAME_ENV, &active_skill.name)
            .env(COCO_SKILL_DIR_ENV, &active_skill.directory);
        if let Some(persistent_directory) = &active_skill_persistent_directory {
            command.env(COCO_SKILL_PERSIST_DIR_ENV, persistent_directory);
        }
    }
    if let Some(skill_uv_runtime_dirs) = &skill_uv_runtime_dirs {
        command.env(
            "PATH",
            skill_uv_path_env(coco_command.as_ref(), skill_uv_runtime_dirs),
        );
        for (name, value) in skill_uv_runtime_dirs.env_vars() {
            command.env(name, value);
        }
    }
    let mut child = command.spawn().context(SpawnProcessSnafu)?;

    let output_limit_bytes = output_limit_bytes(request.max_output_tokens);
    let stdout = Arc::new(Mutex::new(UnifiedExecOutputBuffer::default()));
    let stderr = Arc::new(Mutex::new(UnifiedExecOutputBuffer::default()));
    let notify = Arc::new(Notify::new());
    let stdout_task = tokio::spawn(stream_child_pipe(
        child.stdout.take(),
        stdout.clone(),
        output_limit_bytes,
        notify.clone(),
    ));
    let stderr_task = tokio::spawn(stream_child_pipe(
        child.stderr.take(),
        stderr.clone(),
        output_limit_bytes,
        notify.clone(),
    ));
    let wait_notify = notify.clone();
    let wait_task = tokio::spawn(async move {
        let status = child.wait().await.map(|status| status.code());
        wait_notify.notify_waiters();
        status
    });

    let session_id = {
        let mut store = sessions.inner.lock().await;
        let session_id = next_session_id(&store);
        store.insert_session(
            session_id.clone(),
            UnifiedExecSession {
                branch: request.context.session_branch.clone(),
                stdin: None,
                stdout,
                stderr,
                stdout_cursor: 0,
                stderr_cursor: 0,
                notify,
                wait_task: Some(wait_task),
                stdout_task: Some(stdout_task),
                stderr_task: Some(stderr_task),
                pty_child_killer: None,
                runtime_server,
                _coco_command: coco_command,
            },
        );
        session_id
    };

    collect_session_until_deadline(
        sessions,
        session_id,
        request.yield_time_ms,
        request.max_output_tokens,
    )
    .await
}

async fn execute_pty_command(
    request: ExecCommandRequest,
    execution: ExecutionSpec,
    runtime_server: Option<CocoCliRuntimeServer>,
    coco_command: Option<CocoCommandPathInjection>,
    skill_uv_runtime_dirs: Option<SkillUvRuntimeDirs>,
    sessions: UnifiedExecSessionStoreHandle,
) -> std::result::Result<String, UnifiedExecToolError> {
    let pty_system = NativePtySystem::default();
    let pair = pty_system
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .map_err(|source| UnifiedExecToolError::OpenPty {
            message: source.to_string(),
        })?;
    let reader =
        pair.master
            .try_clone_reader()
            .map_err(|source| UnifiedExecToolError::ClonePtyReader {
                message: source.to_string(),
            })?;
    let writer =
        pair.master
            .take_writer()
            .map_err(|source| UnifiedExecToolError::TakePtyWriter {
                message: source.to_string(),
            })?;

    let mut command = CommandBuilder::new(execution.program.as_os_str());
    command.args(execution.args.iter().map(OsString::as_os_str));
    command.cwd(&request.workdir);
    command.env(COCO_EXEC_WORKSPACE_ENV, &request.workspace_root);
    command.env_remove(COCO_DAEMON_SOCKET_ENV);
    command.env_remove(COCO_SESSION_BRANCH_ENV);
    command.env_remove(COCO_SESSION_ROLE_ENV);
    command.env_remove(COCO_STORE_PATH_ENV);
    command.env_remove(COCO_CLI_RUNTIME_SOCKET_ENV);
    command.env_remove(COCO_PARENT_TOOL_USE_ID_ENV);
    command.env_remove(COCO_LOG_DIR_ENV);
    command.env_remove(COCO_SKILL_NAME_ENV);
    command.env_remove(COCO_SKILL_DIR_ENV);
    command.env_remove(COCO_SKILL_PERSIST_DIR_ENV);
    command.env_remove(UV_CACHE_DIR_ENV);
    command.env_remove(UV_PYTHON_INSTALL_DIR_ENV);
    command.env_remove(COCO_COMMAND_SHIM_MODE_ENV);
    if let Some(coco_command) = &coco_command {
        command.env("PATH", join_path_entries(coco_command.path_entries.clone()));
        command.env(COCO_LOG_DIR_ENV, &coco_command.log_dir);
        if request.context.enable_coco_shim {
            command.env(COCO_SESSION_BRANCH_ENV, &request.context.session_branch);
            command.env(COCO_SESSION_ROLE_ENV, request.context.session_role.as_str());
            if let Some(store_path) = &request.context.store_path {
                command.env(COCO_STORE_PATH_ENV, store_path);
            }
            if let Some(runtime_server) = &runtime_server {
                command.env(COCO_CLI_RUNTIME_SOCKET_ENV, &runtime_server.socket_path);
            }
            if let Some(parent_tool_use_id) = &request.parent_tool_use_id {
                command.env(COCO_PARENT_TOOL_USE_ID_ENV, parent_tool_use_id);
            }
        } else {
            command.env(COCO_COMMAND_SHIM_MODE_ENV, "disabled");
        }
    }
    let active_skill_persistent_directory = current_active_skill_persistent_directory(&request);
    if let Some(active_skill) = &request.context.active_skill {
        command.env(COCO_SKILL_NAME_ENV, &active_skill.name);
        command.env(COCO_SKILL_DIR_ENV, &active_skill.directory);
        if let Some(persistent_directory) = &active_skill_persistent_directory {
            command.env(COCO_SKILL_PERSIST_DIR_ENV, persistent_directory);
        }
    }
    if let Some(skill_uv_runtime_dirs) = &skill_uv_runtime_dirs {
        command.env(
            "PATH",
            skill_uv_path_env(coco_command.as_ref(), skill_uv_runtime_dirs),
        );
        for (name, value) in skill_uv_runtime_dirs.env_vars() {
            command.env(name, value);
        }
    }

    let mut child = pair.slave.spawn_command(command).map_err(|source| {
        UnifiedExecToolError::SpawnPtyProcess {
            message: source.to_string(),
        }
    })?;
    let pty_child_killer = Some(child.clone_killer());
    drop(pair.slave);

    let output_limit_bytes = output_limit_bytes(request.max_output_tokens);
    let stdout = Arc::new(Mutex::new(UnifiedExecOutputBuffer::default()));
    let stderr = Arc::new(Mutex::new(UnifiedExecOutputBuffer::default()));
    let notify = Arc::new(Notify::new());
    let stdout_task = tokio::task::spawn_blocking({
        let stdout = stdout.clone();
        let notify = notify.clone();
        move || stream_blocking_reader(reader, stdout, output_limit_bytes, notify)
    });
    let stderr_task = tokio::spawn(async { Ok(()) });
    let wait_notify = notify.clone();
    let wait_task = tokio::task::spawn_blocking(move || {
        let status = child.wait()?;
        wait_notify.notify_waiters();
        Ok(Some(status.exit_code() as i32))
    });

    let session_id = {
        let mut store = sessions.inner.lock().await;
        let session_id = next_session_id(&store);
        store.insert_session(
            session_id.clone(),
            UnifiedExecSession {
                branch: request.context.session_branch.clone(),
                stdin: Some(UnifiedExecSessionStdin::Blocking(writer)),
                stdout,
                stderr,
                stdout_cursor: 0,
                stderr_cursor: 0,
                notify,
                wait_task: Some(wait_task),
                stdout_task: Some(stdout_task),
                stderr_task: Some(stderr_task),
                pty_child_killer,
                runtime_server,
                _coco_command: coco_command,
            },
        );
        session_id
    };

    collect_session_until_deadline(
        sessions,
        session_id,
        request.yield_time_ms,
        request.max_output_tokens,
    )
    .await
}

async fn write_stdin(
    request: WriteStdinRequest,
    sessions: UnifiedExecSessionStoreHandle,
) -> std::result::Result<String, UnifiedExecToolError> {
    if !request.chars.is_empty() {
        let stdin = {
            let mut store = sessions.inner.lock().await;
            let session =
                store
                    .sessions
                    .get_mut(&request.session_id)
                    .context(SessionNotFoundSnafu {
                        session_id: request.session_id.clone(),
                    })?;
            match session.stdin.take() {
                Some(stdin) => stdin,
                None => return StdinUnavailableSnafu.fail(),
            }
        };

        let stdin = write_session_stdin(stdin, request.chars.clone()).await?;

        let mut store = sessions.inner.lock().await;
        if let Some(session) = store.sessions.get_mut(&request.session_id) {
            session.stdin = Some(stdin);
        }
    }

    collect_session_until_deadline(
        sessions,
        request.session_id,
        request.yield_time_ms,
        request.max_output_tokens,
    )
    .await
}

impl UnifiedExecToolRuntime {
    pub fn tool_definition(&self) -> rig::completion::ToolDefinition {
        rig::completion::ToolDefinition {
            name: self.definition.name.clone(),
            description: self.definition.description.clone(),
            parameters: self.definition.input_schema.clone(),
        }
    }

    pub async fn execute(
        &self,
        args: String,
        invocation: ToolInvocationContext,
    ) -> std::result::Result<String, rig::tool::ToolError> {
        use rig::tool::ToolError;

        let workspace_root = self.workspace_root.clone();
        let context = self.context.clone();
        let sessions = self.sessions.clone();
        let result = async {
            let args: Value = serde_json::from_str(&args).context(ParseArgsSnafu)?;
            match self.kind {
                UnifiedExecToolKind::ExecCommand => {
                    let mut request =
                        resolve_exec_command_request(&args, &workspace_root, context)?;
                    request.parent_tool_use_id = invocation.persisted_tool_use_node_id;
                    execute_command(request, sessions).await
                }
                UnifiedExecToolKind::WriteStdin => {
                    let request = resolve_write_stdin_request(&args)?;
                    write_stdin(request, sessions).await
                }
            }
        }
        .await;
        match result {
            Ok(output) => Ok(output),
            Err(source) => Err(ToolError::ToolCallError(Box::new(source))),
        }
    }
}

impl rig::tool::ToolDyn for UnifiedExecToolRuntime {
    fn name(&self) -> String {
        self.definition.name.clone()
    }

    fn definition<'a>(
        &'a self,
        _prompt: String,
    ) -> rig::wasm_compat::WasmBoxedFuture<'a, rig::completion::ToolDefinition> {
        let definition = self.tool_definition();
        Box::pin(async move { definition })
    }

    fn call<'a>(
        &'a self,
        args: String,
    ) -> rig::wasm_compat::WasmBoxedFuture<'a, std::result::Result<String, rig::tool::ToolError>>
    {
        Box::pin(async move { self.execute(args, ToolInvocationContext::default()).await })
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::net::UnixStream;
    use tokio::sync::Mutex;

    use super::*;

    fn test_context() -> ToolRuntimeEnv {
        test_context_with_shim(false)
    }

    fn test_context_with_shim(enable_coco_shim: bool) -> ToolRuntimeEnv {
        ToolRuntimeEnv {
            session_branch: "main".to_owned(),
            session_role: coco_mem::SessionRole::Orchestrator,
            current_skill_name: None,
            active_skill: None,
            store_path: None,
            enable_coco_shim,
            cli_bridge: crate::UnifiedExecCliBridgeHandle::default(),
            skill_executor: crate::SkillSearchExecutorHandle::default(),
        }
    }

    #[derive(Debug)]
    struct FakeCliBridge {
        requests: Arc<Mutex<Vec<CocoCliRuntimeRequest>>>,
    }

    #[async_trait]
    impl crate::UnifiedExecCliBridge for FakeCliBridge {
        async fn execute_coco_cli(
            &self,
            request: CocoCliRuntimeRequest,
        ) -> std::result::Result<CocoCliRuntimeResponse, crate::UnifiedExecCliBridgeError> {
            self.requests.lock().await.push(request);
            Ok(CocoCliRuntimeResponse {
                exit_code: 0,
                stdout: "delegated".to_owned(),
                stderr: String::new(),
            })
        }
    }

    #[derive(Debug)]
    struct FailingCliBridge;

    #[async_trait]
    impl crate::UnifiedExecCliBridge for FailingCliBridge {
        async fn execute_coco_cli(
            &self,
            _request: CocoCliRuntimeRequest,
        ) -> std::result::Result<CocoCliRuntimeResponse, crate::UnifiedExecCliBridgeError> {
            Err(crate::UnifiedExecCliBridgeError::Unavailable)
        }
    }

    fn temp_exec_command_tool() -> Tool {
        Tool {
            name: "exec_command".to_owned(),
            description: "Run a command".to_owned(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "cmd": {"type": "string"},
                    "workdir": {"type": "string"},
                    "shell": {"type": "string"},
                    "yield_time_ms": {"type": "integer"},
                    "max_output_tokens": {"type": "integer"}
                },
                "required": ["cmd"],
                "additionalProperties": false
            }),
        }
    }

    fn temp_write_stdin_tool() -> Tool {
        Tool {
            name: "write_stdin".to_owned(),
            description: "Write stdin".to_owned(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "session_id": {"type": "string"},
                    "chars": {"type": "string"},
                    "yield_time_ms": {"type": "integer"},
                    "max_output_tokens": {"type": "integer"}
                },
                "required": ["session_id"],
                "additionalProperties": false
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

    fn parse_session_id(output: &str) -> String {
        output
            .lines()
            .find_map(|line| line.strip_prefix("session_id: "))
            .expect("output should include session_id")
            .to_owned()
    }

    #[cfg(unix)]
    fn parse_pid(output: &str) -> Option<String> {
        output
            .lines()
            .find_map(|line| line.trim_end_matches('\r').strip_prefix("pid:"))
            .map(str::trim)
            .map(str::to_owned)
    }

    #[cfg(unix)]
    fn process_exists(pid: &str) -> bool {
        std::process::Command::new("kill")
            .arg("-0")
            .arg(pid)
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    #[cfg(unix)]
    async fn wait_for_process(pid: &str) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if process_exists(pid) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        false
    }

    #[cfg(unix)]
    async fn wait_for_session_pid(
        write_stdin: &dyn rig::tool::ToolDyn,
        session_id: &str,
    ) -> String {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let output = write_stdin
                .call(format!(
                    r#"{{"session_id":"{}","chars":"","yield_time_ms":100}}"#,
                    session_id
                ))
                .await
                .unwrap();
            if let Some(pid) = parse_pid(&output) {
                return pid;
            }
            if std::time::Instant::now() >= deadline {
                panic!("expected child pid in output");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    fn runtime_pair(
        workspace_root: PathBuf,
        context: ToolRuntimeEnv,
    ) -> (Box<dyn rig::tool::ToolDyn>, Box<dyn rig::tool::ToolDyn>) {
        let sessions = session_store();
        runtime_pair_with_sessions(workspace_root, context, sessions)
    }

    fn runtime_pair_with_sessions(
        workspace_root: PathBuf,
        context: ToolRuntimeEnv,
        sessions: UnifiedExecSessionStoreHandle,
    ) -> (Box<dyn rig::tool::ToolDyn>, Box<dyn rig::tool::ToolDyn>) {
        (
            Box::new(runtime_with_sessions(
                temp_exec_command_tool(),
                workspace_root.clone(),
                context.clone(),
                UnifiedExecToolKind::ExecCommand,
                sessions.clone(),
            )),
            Box::new(runtime_with_sessions(
                temp_write_stdin_tool(),
                workspace_root,
                context,
                UnifiedExecToolKind::WriteStdin,
                sessions,
            )),
        )
    }

    #[tokio::test]
    async fn resolve_workspace_root_uses_relative_env_override() {
        let temp_root = tempfile::tempdir().unwrap();
        let nested = temp_root.path().join("workspace");
        std::fs::create_dir(&nested).unwrap();
        let previous_dir = std::env::current_dir().unwrap();
        let relative = OsString::from("workspace");

        let resolved = crate::with_process_env_async(
            &[(COCO_EXEC_WORKSPACE_ENV, Some(relative.as_os_str()))],
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
    async fn resolve_sandbox_mode_defaults_to_nono() {
        let resolved = crate::with_process_env_async(&[(COCO_EXEC_SANDBOX_ENV, None)], || async {
            resolve_sandbox_mode()
        })
        .await
        .unwrap();

        assert_eq!(resolved, ExecSandboxMode::Nono);
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

        assert_eq!(
            resolved,
            canonicalize_existing_path(&std::env::temp_dir()).join("coco")
        );
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

        assert!(matches!(
            error,
            UnifiedExecToolError::RuntimeRootOverlapsWorkspace
        ));
    }

    #[test]
    fn validate_runtime_socket_path_rejects_long_paths() {
        let long_dir = "a".repeat(MAX_RUNTIME_SOCKET_PATH_LEN);
        let path = PathBuf::from("/tmp").join(long_dir).join("coco-cli.sock");

        let error = validate_runtime_socket_path(&path).unwrap_err();

        assert!(matches!(
            error,
            UnifiedExecToolError::RuntimeSocketPathTooLong { .. }
        ));
    }

    #[test]
    fn command_fingerprint_does_not_include_raw_command_text() {
        let command = "curl -H 'Authorization: Bearer secret' https://example.com";
        let fingerprint = command_fingerprint(command);

        assert_eq!(fingerprint.len(), 16);
        assert!(!fingerprint.contains("secret"));
        assert_ne!(fingerprint, command);
    }

    #[test]
    fn resolve_execution_spec_uses_nono_when_available_in_nono_mode() {
        let temp_root = tempfile::tempdir().unwrap();
        let request = ExecCommandRequest {
            cmd: "pwd".to_owned(),
            workdir: temp_root.path().to_path_buf(),
            workspace_root: temp_root.path().to_path_buf(),
            sandbox_mode: ExecSandboxMode::Nono,
            shell: OsString::from("bash"),
            tty: false,
            yield_time_ms: DEFAULT_YIELD_TIME_MS,
            max_output_tokens: None,
            context: test_context(),
            parent_tool_use_id: None,
        };
        let path_env = OsString::from("/tmp/nono-bin:/tmp/bash-bin");
        let spec = resolve_execution_spec(&request, Some(path_env.as_os_str()), &[]);
        assert!(matches!(spec, Err(UnifiedExecToolError::NonoNotFound)));
    }

    #[test]
    fn resolve_execution_spec_adds_runtime_socket_allow_path() {
        let temp_root = tempfile::tempdir().unwrap();
        let request = ExecCommandRequest {
            cmd: "pwd".to_owned(),
            workdir: temp_root.path().to_path_buf(),
            workspace_root: temp_root.path().to_path_buf(),
            sandbox_mode: ExecSandboxMode::Off,
            shell: OsString::from("bash"),
            tty: false,
            yield_time_ms: DEFAULT_YIELD_TIME_MS,
            max_output_tokens: None,
            context: test_context(),
            parent_tool_use_id: None,
        };
        let nono = PathBuf::from("/bin/nono");
        let runtime_dir = temp_root.path().join(".coco-runtime").join("socket-dir");
        let spec = nono_execution_spec(nono.clone(), &request, std::slice::from_ref(&runtime_dir));

        assert_eq!(spec.program, nono);
        assert_eq!(spec.nono, Some(PathBuf::from("/bin/nono")));
        let mut expected_args = vec![
            OsString::from("run"),
            OsString::from("--silent"),
            OsString::from("--allow"),
            temp_root.path().as_os_str().to_owned(),
        ];
        for default_read_path in NONO_DEFAULT_READ_PATHS {
            let default_read_path = Path::new(default_read_path);
            if default_read_path.exists() {
                expected_args.push(OsString::from("--read"));
                expected_args.push(default_read_path.as_os_str().to_owned());
            }
        }
        for default_allow_file in NONO_DEFAULT_ALLOW_FILES {
            expected_args.push(OsString::from("--allow-file"));
            expected_args.push(OsString::from(default_allow_file));
        }
        expected_args.extend([
            OsString::from("--allow"),
            runtime_dir.as_os_str().to_owned(),
            OsString::from("--"),
            OsString::from("bash"),
            OsString::from("-c"),
            OsString::from("pwd"),
        ]);

        assert_eq!(spec.args, expected_args);
    }

    #[cfg(unix)]
    #[test]
    fn resolve_execution_spec_allows_dev_null_by_default() {
        let temp_root = tempfile::tempdir().unwrap();
        let request = ExecCommandRequest {
            cmd: "pwd".to_owned(),
            workdir: temp_root.path().to_path_buf(),
            workspace_root: temp_root.path().to_path_buf(),
            sandbox_mode: ExecSandboxMode::Off,
            shell: OsString::from("bash"),
            tty: false,
            yield_time_ms: DEFAULT_YIELD_TIME_MS,
            max_output_tokens: None,
            context: test_context(),
            parent_tool_use_id: None,
        };
        let spec = nono_execution_spec(PathBuf::from("/bin/nono"), &request, &[]);

        assert!(
            spec.args
                .windows(2)
                .any(|window| window
                    == [OsString::from("--allow-file"), OsString::from("/dev/null")])
        );
    }

    #[test]
    fn resolve_exec_command_request_rejects_workdir_outside_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let args = serde_json::json!({
            "cmd": "pwd",
            "workdir": outside.path(),
        });

        let error =
            resolve_exec_command_request(&args, workspace.path(), test_context()).unwrap_err();
        assert!(matches!(
            error,
            UnifiedExecToolError::WorkdirOutsideWorkspace
        ));
    }

    #[test]
    fn resolve_exec_command_request_rejects_removed_timeout_field() {
        let workspace = tempfile::tempdir().unwrap();
        let args = serde_json::json!({
            "cmd": "pwd",
            "timeout_ms": 10,
        });

        let error =
            resolve_exec_command_request(&args, workspace.path(), test_context()).unwrap_err();

        assert!(matches!(
            error,
            UnifiedExecToolError::UnsupportedInputField { field } if field == "timeout_ms"
        ));
    }

    #[test]
    fn resolve_write_stdin_request_rejects_removed_timeout_field() {
        let args = serde_json::json!({
            "session_id": "exec-test",
            "timeout_ms": 10,
        });

        let error = resolve_write_stdin_request(&args).unwrap_err();

        assert!(matches!(
            error,
            UnifiedExecToolError::UnsupportedInputField { field } if field == "timeout_ms"
        ));
    }

    #[tokio::test]
    async fn exec_command_runtime_off_mode_allows_writes_inside_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime = runtime_tool(
            temp_exec_command_tool(),
            workspace.path().to_path_buf(),
            test_context(),
        );

        let output = crate::with_process_env_async(
            &[("COCO_EXEC_SANDBOX", Some(OsStr::new("off")))],
            || async {
                runtime
                    .call(format!(
                        r#"{{"cmd":"printf 'hello' > trace.txt; cat trace.txt","workdir":"{}","shell":"bash"}}"#,
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
    async fn exec_command_runtime_auto_mode_falls_back_without_nono() {
        let workspace = tempfile::tempdir().unwrap();
        let fake_bin = tempfile::tempdir().unwrap();
        let bash_path = resolve_bash_path_locked().await;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&bash_path, fake_bin.path().join("bash")).unwrap();
        let runtime = runtime_tool(
            temp_exec_command_tool(),
            workspace.path().to_path_buf(),
            test_context(),
        );
        let path_env = OsString::from(fake_bin.path().as_os_str());

        let output = crate::with_process_env_async(
            &[
                ("COCO_EXEC_SANDBOX", Some(OsStr::new("auto"))),
                ("PATH", Some(path_env.as_os_str())),
            ],
            || async {
                runtime
                    .call(format!(
                        r#"{{"cmd":"printf 'fallback'","workdir":"{}","shell":"bash"}}"#,
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
    async fn exec_command_runtime_masks_host_coco_with_injected_alias_by_default() {
        let workspace = tempfile::tempdir().unwrap();
        let fake_bin = tempfile::tempdir().unwrap();
        let bash_path = resolve_bash_path_locked().await;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&bash_path, fake_bin.path().join("bash")).unwrap();
        let runtime = runtime_tool(
            temp_exec_command_tool(),
            workspace.path().to_path_buf(),
            test_context(),
        );
        let path_env = OsString::from(fake_bin.path().as_os_str());

        let output = crate::with_process_env_async(
            &[
                ("COCO_EXEC_SANDBOX", Some(OsStr::new("off"))),
                ("PATH", Some(path_env.as_os_str())),
            ],
            || async {
                runtime
                    .call(format!(
                        r#"{{"cmd":"command -v coco","workdir":"{}","shell":"bash"}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(output.contains("exit_status: 0"));
        assert!(output.contains("/bin/coco"));
    }

    #[test]
    fn disabled_coco_shim_uses_binary_alias_instead_of_script() {
        let workspace = tempfile::tempdir().unwrap();
        let injection = prepare_coco_command_path_injection(
            workspace.path(),
            CocoCommandShimMode::Disabled,
            false,
        )
        .unwrap();
        let alias_path = injection.root_dir.join("bin").join("coco");
        let expected = resolve_coco_cli_executable().unwrap();

        #[cfg(unix)]
        {
            assert_eq!(std::fs::read_link(&alias_path).unwrap(), expected);
        }

        #[cfg(not(unix))]
        {
            assert_eq!(
                std::fs::read(&alias_path).unwrap(),
                std::fs::read(expected).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn active_skill_coco_shim_adds_uv_directory_from_path() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime = tempfile::tempdir().unwrap();
        let fake_bin = tempfile::tempdir().unwrap();
        let fake_uv = fake_bin.path().join("uv");
        std::fs::write(&fake_uv, "#!/bin/sh\n").unwrap();

        let injection = crate::with_process_env_async(
            &[
                ("XDG_RUNTIME_DIR", Some(runtime.path().as_os_str())),
                ("PATH", Some(fake_bin.path().as_os_str())),
            ],
            || async {
                prepare_coco_command_path_injection(
                    workspace.path(),
                    CocoCommandShimMode::Disabled,
                    true,
                )
            },
        )
        .await
        .unwrap();

        let expected_uv_parent = fake_uv
            .canonicalize()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        assert!(injection.extra_allow_paths.contains(&expected_uv_parent));
        assert_eq!(
            injection.path_entries.first(),
            Some(&injection.root_dir.join("bin"))
        );
        assert_eq!(injection.path_entries.get(1), Some(&expected_uv_parent));
    }

    #[tokio::test]
    async fn uv_python_install_allow_paths_include_existing_install_dirs() {
        let explicit_dir = tempfile::tempdir().unwrap();
        let xdg_dir = tempfile::tempdir().unwrap();
        let home_dir = tempfile::tempdir().unwrap();
        let xdg_uv_python_dir = xdg_dir.path().join("uv").join("python");
        let home_uv_python_dir = home_dir
            .path()
            .join(".local")
            .join("share")
            .join("uv")
            .join("python");
        std::fs::create_dir_all(&xdg_uv_python_dir).unwrap();
        std::fs::create_dir_all(&home_uv_python_dir).unwrap();

        let allow_paths = crate::with_process_env_async(
            &[
                (
                    UV_PYTHON_INSTALL_DIR_ENV,
                    Some(explicit_dir.path().as_os_str()),
                ),
                (XDG_DATA_HOME_ENV, Some(xdg_dir.path().as_os_str())),
                (HOME_ENV, Some(home_dir.path().as_os_str())),
            ],
            || async { uv_python_install_allow_paths() },
        )
        .await;

        assert!(allow_paths.contains(&explicit_dir.path().canonicalize().unwrap()));
        assert!(allow_paths.contains(&xdg_uv_python_dir.canonicalize().unwrap()));
        assert!(allow_paths.contains(&home_uv_python_dir.canonicalize().unwrap()));
    }

    #[tokio::test]
    async fn active_skill_coco_shim_allows_uv_managed_python_dir() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime = tempfile::tempdir().unwrap();
        let fake_bin = tempfile::tempdir().unwrap();
        let home_dir = tempfile::tempdir().unwrap();
        let fake_uv = fake_bin.path().join("uv");
        let uv_python_dir = home_dir
            .path()
            .join(".local")
            .join("share")
            .join("uv")
            .join("python");
        std::fs::write(&fake_uv, "#!/bin/sh\n").unwrap();
        std::fs::create_dir_all(&uv_python_dir).unwrap();

        let injection = crate::with_process_env_async(
            &[
                ("XDG_RUNTIME_DIR", Some(runtime.path().as_os_str())),
                ("PATH", Some(fake_bin.path().as_os_str())),
                (HOME_ENV, Some(home_dir.path().as_os_str())),
                (XDG_DATA_HOME_ENV, None),
                (UV_PYTHON_INSTALL_DIR_ENV, None),
            ],
            || async {
                prepare_coco_command_path_injection(
                    workspace.path(),
                    CocoCommandShimMode::Disabled,
                    true,
                )
            },
        )
        .await
        .unwrap();

        assert!(
            injection
                .extra_allow_paths
                .contains(&uv_python_dir.canonicalize().unwrap())
        );
    }

    #[tokio::test]
    async fn skill_uv_runtime_dirs_prefer_configured_parent_env_paths() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime_root = tempfile::tempdir().unwrap();
        let skill_persist_dir = tempfile::tempdir().unwrap();
        let cache_home = tempfile::tempdir().unwrap();
        let config_home = tempfile::tempdir().unwrap();
        let data_root = tempfile::tempdir().unwrap();
        let data_home = data_root.path().join(".local").join("share");
        let bin_home = tempfile::tempdir().unwrap();
        let state_home = tempfile::tempdir().unwrap();

        let dirs = crate::with_process_env_async(
            &[
                ("XDG_RUNTIME_DIR", Some(runtime_root.path().as_os_str())),
                (XDG_CACHE_HOME_ENV, Some(cache_home.path().as_os_str())),
                (XDG_CONFIG_HOME_ENV, Some(config_home.path().as_os_str())),
                (XDG_DATA_HOME_ENV, Some(data_home.as_os_str())),
                (XDG_BIN_HOME_ENV, Some(bin_home.path().as_os_str())),
                (XDG_STATE_HOME_ENV, Some(state_home.path().as_os_str())),
                (TMPDIR_ENV, None),
                (UV_CACHE_DIR_ENV, None),
                (UV_PYTHON_INSTALL_DIR_ENV, None),
            ],
            || async { prepare_skill_uv_runtime_dirs(workspace.path()) },
        )
        .await
        .unwrap();

        let env_vars = dirs.env_vars().collect::<HashMap<_, _>>();
        assert_eq!(env_vars.get(HOME_ENV), None);
        assert_eq!(
            env_vars.get(TMPDIR_ENV),
            Some(&cache_home.path().join("coco").join("tmp"))
        );
        assert_eq!(
            env_vars.get(UV_CACHE_DIR_ENV),
            Some(&cache_home.path().join("uv"))
        );
        assert_eq!(
            env_vars.get(UV_PYTHON_INSTALL_DIR_ENV),
            Some(&data_home.join("uv").join("python"))
        );
        assert_eq!(
            env_vars.get(XDG_BIN_HOME_ENV),
            Some(&bin_home.path().to_path_buf())
        );

        let allow_paths = dirs.allow_paths().collect::<Vec<_>>();
        assert!(allow_paths.contains(&cache_home.path().to_path_buf()));
        assert!(!allow_paths.contains(&config_home.path().to_path_buf()));
        assert!(allow_paths.contains(&config_home.path().join("uv")));
        assert!(!allow_paths.contains(&data_home));
        assert!(allow_paths.contains(&data_home.join("uv")));
        assert!(allow_paths.contains(&data_home.join("uv").join("python")));
        assert!(allow_paths.contains(&bin_home.path().to_path_buf()));
        assert!(allow_paths.contains(&state_home.path().to_path_buf()));
        assert!(
            !allow_paths
                .iter()
                .any(|path| path.starts_with(skill_persist_dir.path()))
        );

        let path_entries = skill_uv_path_entries(None, &dirs);
        assert_eq!(path_entries.first(), Some(&bin_home.path().to_path_buf()));
    }

    #[tokio::test]
    async fn skill_uv_runtime_dirs_ignore_relative_parent_env_paths() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime_root = tempfile::tempdir().unwrap();

        let dirs = crate::with_process_env_async(
            &[
                ("XDG_RUNTIME_DIR", Some(runtime_root.path().as_os_str())),
                (TMPDIR_ENV, Some(OsStr::new("tmp"))),
                (UV_CACHE_DIR_ENV, Some(OsStr::new("uv-cache"))),
                (UV_PYTHON_INSTALL_DIR_ENV, Some(OsStr::new("uv-python"))),
                (XDG_CACHE_HOME_ENV, Some(OsStr::new(".cache"))),
                (XDG_CONFIG_HOME_ENV, Some(OsStr::new(".config"))),
                (XDG_DATA_HOME_ENV, Some(OsStr::new("data"))),
                (XDG_BIN_HOME_ENV, Some(OsStr::new("bin"))),
                (XDG_STATE_HOME_ENV, Some(OsStr::new("state"))),
            ],
            || async { prepare_skill_uv_runtime_dirs(workspace.path()) },
        )
        .await
        .unwrap();

        let env_vars = dirs.env_vars().collect::<HashMap<_, _>>();
        let expected_runtime_root = runtime_root.path().join("coco").join("skill-runtime");
        assert_eq!(
            env_vars.get(TMPDIR_ENV),
            Some(
                &expected_runtime_root
                    .join(".cache")
                    .join("coco")
                    .join("tmp")
            )
        );
        assert_eq!(
            env_vars.get(UV_CACHE_DIR_ENV),
            Some(&expected_runtime_root.join(".cache").join("uv"))
        );
        assert_eq!(
            env_vars.get(UV_PYTHON_INSTALL_DIR_ENV),
            Some(
                &expected_runtime_root
                    .join(".local")
                    .join("share")
                    .join("uv")
                    .join("python")
            )
        );
        assert_eq!(
            env_vars.get(XDG_BIN_HOME_ENV),
            Some(&expected_runtime_root.join(".local").join("bin"))
        );
    }

    #[test]
    fn skill_uv_path_env_keeps_coco_shim_ahead_of_xdg_bin() {
        let shim_root = tempfile::tempdir().unwrap();
        let shim_bin = shim_root.path().join("bin");
        let xdg_bin = tempfile::tempdir().unwrap();
        let rest = tempfile::tempdir().unwrap();
        let command = CocoCommandPathInjection {
            root_dir: shim_root.path().to_path_buf(),
            path_entries: vec![shim_bin.clone(), rest.path().to_path_buf()],
            extra_allow_paths: Vec::new(),
            log_dir: shim_root.path().join("logs"),
        };
        let dirs = SkillUvRuntimeDirs {
            env_vars: Vec::new(),
            allow_paths: Vec::new(),
            xdg_bin_dir: xdg_bin.path().to_path_buf(),
        };

        let path_entries = skill_uv_path_entries(Some(&command), &dirs);
        assert_eq!(path_entries.first(), Some(&shim_bin));
        assert_eq!(path_entries.get(1), Some(&xdg_bin.path().to_path_buf()));
        assert_eq!(path_entries.get(2), Some(&rest.path().to_path_buf()));
    }

    #[tokio::test]
    async fn exec_command_runtime_injects_coco_command_when_enabled() {
        let workspace = tempfile::tempdir().unwrap();
        let fake_bin = tempfile::tempdir().unwrap();
        let bash_path = resolve_bash_path_locked().await;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&bash_path, fake_bin.path().join("bash")).unwrap();
        let runtime = runtime_tool(
            temp_exec_command_tool(),
            workspace.path().to_path_buf(),
            test_context_with_shim(true),
        );
        let path_env = OsString::from(fake_bin.path().as_os_str());

        let output = crate::with_process_env_async(
            &[
                ("COCO_EXEC_SANDBOX", Some(OsStr::new("off"))),
                ("PATH", Some(path_env.as_os_str())),
            ],
            || async {
                runtime
                    .call(format!(
                        r#"{{"cmd":"command -v coco","workdir":"{}","shell":"bash"}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(output.contains("exit_status: 0"));
        assert!(output.contains("/bin/coco"));
    }

    #[tokio::test]
    async fn exec_command_runtime_sets_coco_log_dir_for_injected_alias() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime_root = tempfile::tempdir().unwrap();
        let runtime = runtime_tool(
            temp_exec_command_tool(),
            workspace.path().to_path_buf(),
            test_context_with_shim(false),
        );

        let output = crate::with_process_env_async(
            &[
                ("COCO_EXEC_SANDBOX", Some(OsStr::new("off"))),
                ("XDG_RUNTIME_DIR", Some(runtime_root.path().as_os_str())),
                (COCO_LOG_DIR_ENV, Some(OsStr::new("/stale/coco/logs"))),
            ],
            || async {
                runtime
                    .call(format!(
                        r#"{{"cmd":"printf '%s' \"$COCO_LOG_DIR\"","workdir":"{}","shell":"bash"}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        let expected_root = runtime_root.path().join("coco").display().to_string();
        assert!(output.contains("exit_status: 0"));
        assert!(output.contains(&expected_root));
        assert!(output.contains("/logs"));
        assert!(!output.contains("/stale/coco/logs"));
    }

    #[tokio::test]
    async fn coco_command_path_injection_allows_runtime_log_dir() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime_root = tempfile::tempdir().unwrap();

        let injection = crate::with_process_env_async(
            &[("XDG_RUNTIME_DIR", Some(runtime_root.path().as_os_str()))],
            || async {
                prepare_coco_command_path_injection(
                    workspace.path(),
                    CocoCommandShimMode::Disabled,
                    false,
                )
            },
        )
        .await
        .unwrap();

        assert!(injection.log_dir.is_dir());
        assert!(injection.extra_allow_paths.contains(&injection.log_dir));
    }

    #[tokio::test]
    async fn exec_command_runtime_nono_mode_errors_when_binary_missing() {
        let workspace = tempfile::tempdir().unwrap();
        let fake_bin = tempfile::tempdir().unwrap();
        let bash_path = resolve_bash_path_locked().await;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&bash_path, fake_bin.path().join("bash")).unwrap();
        let runtime = runtime_tool(
            temp_exec_command_tool(),
            workspace.path().to_path_buf(),
            test_context(),
        );
        let path_env = OsString::from(fake_bin.path().as_os_str());

        let error = crate::with_process_env_async(
            &[
                ("COCO_EXEC_SANDBOX", Some(OsStr::new("nono"))),
                ("PATH", Some(path_env.as_os_str())),
            ],
            || async {
                runtime
                    .call(format!(
                        r#"{{"cmd":"printf 'blocked'","workdir":"{}","shell":"bash"}}"#,
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
    async fn exec_command_runtime_nono_mode_wraps_command_with_allow_workspace() {
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
        let runtime = runtime_tool(
            temp_exec_command_tool(),
            workspace.path().to_path_buf(),
            test_context(),
        );
        let path_env = OsString::from(fake_bin.path().as_os_str());

        let output = crate::with_process_env_async(
            &[
                ("COCO_EXEC_SANDBOX", Some(OsStr::new("nono"))),
                ("PATH", Some(path_env.as_os_str())),
            ],
            || async {
                runtime
                    .call(format!(
                        r#"{{"cmd":"printf 'sandboxed'","workdir":"{}","shell":"bash","yield_time_ms":5000}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(output.contains("exit_status: 0"), "{output}");
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
        assert!(args.contains("-c"));
    }

    #[tokio::test]
    async fn exec_command_runtime_nono_mode_allows_active_skill_uv_paths() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime_root = tempfile::tempdir().unwrap();
        let fake_bin = tempfile::tempdir().unwrap();
        let skill_dir = tempfile::tempdir().unwrap();
        let skill_persist_dir = workspace
            .path()
            .join(".coco-workspace")
            .join("skills")
            .join("scripted")
            .join("data");
        let observed_args = workspace.path().join("nono-args.txt");
        let bash_path = resolve_bash_path_locked().await;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&bash_path, fake_bin.path().join("bash")).unwrap();
        write_executable_script(&fake_bin.path().join("uv"), "#!/bin/sh\nexit 0\n");
        write_executable_script(
            &fake_bin.path().join("nono"),
            &format!(
                "#!/bin/sh\ncase \"${{HOME:-}}\" in \"{}\"|\"{}\"/*) echo 'nono state root overlaps active skill persistent data directory' >&2; exit 64;; esac\nprintf '%s\\n' \"$@\" > \"{}\"\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = \"--\" ]; then\n    shift\n    break\n  fi\n  shift\ndone\nexec \"$@\"\n",
                skill_persist_dir.display(),
                skill_persist_dir.display(),
                observed_args.display()
            ),
        );
        let runtime = runtime_tool(
            temp_exec_command_tool(),
            workspace.path().to_path_buf(),
            ToolRuntimeEnv {
                session_branch: "draft".to_owned(),
                session_role: coco_mem::SessionRole::Runner,
                current_skill_name: Some("scripted".to_owned()),
                active_skill: Some(crate::ActiveSkillRuntimeContext {
                    name: "scripted".to_owned(),
                    directory: skill_dir.path().to_path_buf(),
                    persistent_directory: skill_persist_dir.clone(),
                }),
                store_path: None,
                enable_coco_shim: false,
                cli_bridge: crate::UnifiedExecCliBridgeHandle::default(),
                skill_executor: crate::SkillSearchExecutorHandle::default(),
            },
        );
        let path_env = OsString::from(fake_bin.path().as_os_str());

        let output = crate::with_process_env_async(
            &[
                ("COCO_EXEC_SANDBOX", Some(OsStr::new("nono"))),
                ("PATH", Some(path_env.as_os_str())),
                ("XDG_RUNTIME_DIR", Some(runtime_root.path().as_os_str())),
                (HOME_ENV, None),
                (TMPDIR_ENV, None),
                (XDG_CACHE_HOME_ENV, None),
                (XDG_CONFIG_HOME_ENV, None),
                (XDG_DATA_HOME_ENV, None),
                (XDG_BIN_HOME_ENV, None),
                (XDG_STATE_HOME_ENV, None),
            ],
            || async {
                runtime
                    .call(format!(
                        r#"{{"cmd":"printf '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s' \"$(command -v uv)\" \"$HOME\" \"$TMPDIR\" \"$UV_CACHE_DIR\" \"$UV_PYTHON_INSTALL_DIR\" \"$XDG_CACHE_HOME\" \"$XDG_CONFIG_HOME\" \"$XDG_DATA_HOME\" \"$XDG_BIN_HOME\" \"$XDG_STATE_HOME\" \"${{PATH%%:*}}\"","workdir":"{}","shell":"bash","yield_time_ms":5000}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(output.contains("exit_status: 0"), "{output}");
        assert!(output.contains("uv|"));
        assert!(output.contains("/.cache/coco/tmp"));
        assert!(output.contains("/.cache/uv"));
        assert!(output.contains("/.cache"));
        assert!(output.contains("/.config"));
        assert!(output.contains("/.local/share/uv/python"));
        assert!(output.contains("/.local/share"));
        assert!(output.contains("/.local/state"));
        assert!(output.contains("/.local/bin"));
        let args = std::fs::read_to_string(&observed_args).unwrap();
        assert!(args.contains(&fake_bin.path().display().to_string()));
        assert!(args.contains(&skill_dir.path().display().to_string()));
        assert!(args.contains(&skill_persist_dir.display().to_string()));
        assert!(args.contains("/.cache"));
        assert!(args.contains("/.config/uv"));
        assert!(args.contains("/.local/share/uv"));
        assert!(args.contains("/.local/bin"));
        assert!(args.contains("/.local/state"));
        let config_home = runtime_root.path().join("coco").join(".config");
        assert!(
            !args
                .lines()
                .any(|arg| arg == config_home.display().to_string())
        );
        let data_home = runtime_root
            .path()
            .join("coco")
            .join(".local")
            .join("share");
        assert!(
            !args
                .lines()
                .any(|arg| arg == data_home.display().to_string())
        );
    }

    #[tokio::test]
    async fn exec_command_runtime_clears_stale_runtime_env_before_injecting_context() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime = runtime_tool(
            temp_exec_command_tool(),
            workspace.path().to_path_buf(),
            ToolRuntimeEnv {
                session_branch: "draft".to_owned(),
                session_role: coco_mem::SessionRole::Runner,
                current_skill_name: None,
                active_skill: None,
                store_path: None,
                enable_coco_shim: true,
                cli_bridge: crate::UnifiedExecCliBridgeHandle::default(),
                skill_executor: crate::SkillSearchExecutorHandle::default(),
            },
        );

        let output = crate::with_process_env_async(
            &[
                (COCO_SESSION_BRANCH_ENV, Some(OsStr::new("stale-branch"))),
                (COCO_SESSION_ROLE_ENV, Some(OsStr::new("orchestrator"))),
                (COCO_STORE_PATH_ENV, Some(OsStr::new("/tmp/stale-store"))),
                (
                    COCO_CLI_RUNTIME_SOCKET_ENV,
                    Some(OsStr::new("/tmp/stale-runtime.sock")),
                ),
                (
                    COCO_PARENT_TOOL_USE_ID_ENV,
                    Some(OsStr::new("stale-tool-use")),
                ),
                (
                    COCO_SKILL_PERSIST_DIR_ENV,
                    Some(OsStr::new("/tmp/stale-skill-persist")),
                ),
                (
                    COCO_EXEC_WORKSPACE_ENV,
                    Some(OsStr::new("/tmp/stale-workspace")),
                ),
                ("COCO_EXEC_SANDBOX", Some(OsStr::new("off"))),
            ],
            || async {
                runtime
                    .call(format!(
                        r#"{{"cmd":"printf '%s|%s|%s|%s|%s|%s|%s' \"$COCO_BRANCH\" \"$COCO_SESSION_ROLE\" \"${{COCO_STORE_PATH:-}}\" \"${{COCO_CLI_RUNTIME_SOCKET:-}}\" \"${{COCO_PARENT_TOOL_USE_ID:-}}\" \"${{COCO_SKILL_PERSIST_DIR:-}}\" \"$COCO_EXEC_WORKSPACE\"","workdir":"{}","shell":"bash"}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        let workspace_root = workspace.path().canonicalize().unwrap();
        let expected = format!("stdout:\ndraft|runner|||||{}", workspace_root.display());
        assert!(output.contains(&expected), "{output}");
    }

    #[tokio::test]
    async fn exec_command_runtime_injects_active_skill_env() {
        let workspace = tempfile::tempdir().unwrap();
        let skill_dir = tempfile::tempdir().unwrap();
        let skill_persist_dir = workspace
            .path()
            .join(".coco-workspace")
            .join("skills")
            .join("scripted")
            .join("data");
        let runtime = runtime_tool(
            temp_exec_command_tool(),
            workspace.path().to_path_buf(),
            ToolRuntimeEnv {
                session_branch: "draft".to_owned(),
                session_role: coco_mem::SessionRole::Runner,
                current_skill_name: Some("scripted".to_owned()),
                active_skill: Some(crate::ActiveSkillRuntimeContext {
                    name: "scripted".to_owned(),
                    directory: skill_dir.path().to_path_buf(),
                    persistent_directory: skill_persist_dir.clone(),
                }),
                store_path: None,
                enable_coco_shim: false,
                cli_bridge: crate::UnifiedExecCliBridgeHandle::default(),
                skill_executor: crate::SkillSearchExecutorHandle::default(),
            },
        );

        let output = crate::with_process_env_async(
            &[
                ("COCO_EXEC_SANDBOX", Some(OsStr::new("off"))),
                (HOME_ENV, None),
                (TMPDIR_ENV, None),
                (XDG_CACHE_HOME_ENV, None),
                (XDG_CONFIG_HOME_ENV, None),
                (XDG_DATA_HOME_ENV, None),
                (XDG_BIN_HOME_ENV, None),
                (XDG_STATE_HOME_ENV, None),
            ],
            || async {
                runtime
                    .call(format!(
                        r#"{{"cmd":"printf '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s' \"$COCO_SKILL_NAME\" \"$COCO_SKILL_DIR\" \"$COCO_SKILL_PERSIST_DIR\" \"$TMPDIR\" \"$UV_CACHE_DIR\" \"$UV_PYTHON_INSTALL_DIR\" \"$XDG_CACHE_HOME\" \"$XDG_CONFIG_HOME\" \"$XDG_DATA_HOME\" \"$XDG_STATE_HOME\"","workdir":"{}","shell":"bash"}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(output.contains(&format!(
            "stdout:\nscripted|{}|{}|",
            skill_dir.path().display(),
            skill_persist_dir.display()
        )));
        assert!(output.contains("/.cache/coco/tmp"));
        assert!(output.contains("/.cache/uv"));
        assert!(output.contains("/.cache"));
        assert!(output.contains("/.config"));
        assert!(output.contains("/.local/share/uv/python"));
        assert!(output.contains("/.local/share"));
        assert!(output.contains("/.local/state"));
    }

    #[tokio::test]
    async fn exec_command_runtime_injects_parent_tool_use_env_for_invocation() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime = runtime(
            temp_exec_command_tool(),
            workspace.path().to_path_buf(),
            ToolRuntimeEnv {
                session_branch: "draft".to_owned(),
                session_role: coco_mem::SessionRole::Orchestrator,
                current_skill_name: None,
                active_skill: None,
                store_path: None,
                enable_coco_shim: true,
                cli_bridge: crate::UnifiedExecCliBridgeHandle::default(),
                skill_executor: crate::SkillSearchExecutorHandle::default(),
            },
        );

        let output = crate::with_process_env_async(
            &[("COCO_EXEC_SANDBOX", Some(OsStr::new("off")))],
            || async {
                runtime
                    .execute(
                        format!(
                            r#"{{"cmd":"printf '%s' \"${{COCO_PARENT_TOOL_USE_ID:-}}\"","workdir":"{}","shell":"bash"}}"#,
                            workspace.path().display()
                        ),
                        ToolInvocationContext {
                            persisted_tool_use_node_id: Some("tool-use-node".to_owned()),
                        },
                    )
                    .await
            },
        )
        .await
        .unwrap();

        assert!(output.contains("stdout:\ntool-use-node"));
    }

    #[tokio::test]
    async fn exec_command_runtime_returns_running_session_and_later_exit() {
        let workspace = tempfile::tempdir().unwrap();
        let (exec_command, write_stdin) =
            runtime_pair(workspace.path().to_path_buf(), test_context());

        let first = crate::with_process_env_async(
            &[("COCO_EXEC_SANDBOX", Some(OsStr::new("off")))],
            || async {
                exec_command
                    .call(format!(
                        r#"{{"cmd":"printf first; sleep 0.2; printf second","workdir":"{}","shell":"bash","yield_time_ms":10}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(first.contains("Process running with session ID"));
        assert!(first.contains("exit_status: running"));
        assert!(first.contains("stdout:\nfirst"));

        let session_id = parse_session_id(&first);
        let second = crate::with_process_env_async(
            &[("COCO_EXEC_SANDBOX", Some(OsStr::new("off")))],
            || async {
                write_stdin
                    .call(format!(
                        r#"{{"session_id":"{}","chars":"","yield_time_ms":1000}}"#,
                        session_id
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(second.contains("Process exited with code 0"));
        assert!(second.contains("exit_status: 0"));
        assert!(second.contains("stdout:\nsecond"));
    }

    #[tokio::test]
    async fn exec_command_runtime_retains_limited_session_output() {
        let workspace = tempfile::tempdir().unwrap();
        let script = workspace.path().join("slow-output.sh");
        write_executable_script(
            &script,
            "#!/usr/bin/env bash\nprintf '0123456789abcdef'; sleep 0.2\n",
        );
        let sessions = session_store();
        let (exec_command, _write_stdin) = runtime_pair_with_sessions(
            workspace.path().to_path_buf(),
            test_context(),
            sessions.clone(),
        );

        let first = crate::with_process_env_async(
            &[("COCO_EXEC_SANDBOX", Some(OsStr::new("off")))],
            || async {
                exec_command
                    .call(format!(
                        r#"{{"cmd":"{}","workdir":"{}","yield_time_ms":10,"max_output_tokens":2}}"#,
                        script.display(),
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        let session_id = parse_session_id(&first);
        let mut retained = String::new();
        for _ in 0..500 {
            let store = sessions.inner.lock().await;
            let session = store.sessions.get(&session_id).unwrap();
            let stdout = session.stdout.lock().await;
            retained = String::from_utf8_lossy(&stdout.bytes).into_owned();
            assert!(stdout.bytes.len() <= 8);
            if retained == "89abcdef" {
                break;
            }
            drop(stdout);
            drop(store);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(retained, "89abcdef");
    }

    #[tokio::test]
    async fn exec_command_runtime_uses_null_stdin_without_tty() {
        let workspace = tempfile::tempdir().unwrap();
        let (exec_command, _) = runtime_pair(workspace.path().to_path_buf(), test_context());

        let output = crate::with_process_env_async(
            &[("COCO_EXEC_SANDBOX", Some(OsStr::new("off")))],
            || async {
                exec_command
                    .call(format!(
                        r#"{{"cmd":"cat; printf done","workdir":"{}","shell":"bash","yield_time_ms":1000}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(output.contains("Process exited with code 0"));
        assert!(output.contains("stdout:\ndone"));
    }

    #[tokio::test]
    async fn write_stdin_rejects_non_tty_session() {
        let workspace = tempfile::tempdir().unwrap();
        let (exec_command, write_stdin) =
            runtime_pair(workspace.path().to_path_buf(), test_context());

        let first = crate::with_process_env_async(
            &[("COCO_EXEC_SANDBOX", Some(OsStr::new("off")))],
            || async {
                exec_command
                    .call(format!(
                        r#"{{"cmd":"sleep 0.2","workdir":"{}","shell":"bash","yield_time_ms":10}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(first.contains("exit_status: running"));
        let session_id = parse_session_id(&first);
        let error = crate::with_process_env_async(
            &[("COCO_EXEC_SANDBOX", Some(OsStr::new("off")))],
            || async {
                write_stdin
                    .call(format!(
                        r#"{{"session_id":"{}","chars":"hello\n","yield_time_ms":1000}}"#,
                        session_id
                    ))
                    .await
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("tty=true"));
    }

    #[tokio::test]
    async fn exec_command_runtime_writes_stdin_to_tty_session() {
        let workspace = tempfile::tempdir().unwrap();
        let (exec_command, write_stdin) =
            runtime_pair(workspace.path().to_path_buf(), test_context());

        let first = crate::with_process_env_async(
            &[("COCO_EXEC_SANDBOX", Some(OsStr::new("off")))],
            || async {
                exec_command
                    .call(format!(
                        r#"{{"cmd":"read line; printf 'got:%s' \"$line\"","workdir":"{}","shell":"bash","tty":true,"yield_time_ms":10}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(first.contains("exit_status: running"));

        let session_id = parse_session_id(&first);
        let second = crate::with_process_env_async(
            &[("COCO_EXEC_SANDBOX", Some(OsStr::new("off")))],
            || async {
                write_stdin
                    .call(format!(
                        r#"{{"session_id":"{}","chars":"hello\n","yield_time_ms":1000}}"#,
                        session_id
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(second.contains("Process exited with code 0"));
        assert!(second.contains("got:hello"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dropping_tty_session_kills_child_process() {
        let workspace = tempfile::tempdir().unwrap();
        let (exec_command, write_stdin) =
            runtime_pair(workspace.path().to_path_buf(), test_context());

        let first = crate::with_process_env_async(
            &[("COCO_EXEC_SANDBOX", Some(OsStr::new("off")))],
            || async {
                exec_command
                    .call(format!(
                        r#"{{"cmd":"printf 'pid:%s\n' $$; read _","workdir":"{}","shell":"bash","tty":true,"yield_time_ms":100}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(first.contains("exit_status: running"));
        let pid = match parse_pid(&first) {
            Some(pid) => pid,
            None => {
                let session_id = parse_session_id(&first);
                wait_for_session_pid(write_stdin.as_ref(), &session_id).await
            }
        };
        assert!(
            wait_for_process(&pid).await,
            "child process should be running"
        );

        drop(exec_command);
        drop(write_stdin);

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while process_exists(&pid) && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        assert!(
            !process_exists(&pid),
            "dropping the tty session should kill child process {pid}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn removing_branch_kills_tty_sessions_for_branch() {
        let workspace = tempfile::tempdir().unwrap();
        let sessions = session_store();
        let (exec_command, write_stdin) = runtime_pair_with_sessions(
            workspace.path().to_path_buf(),
            ToolRuntimeEnv {
                session_branch: "draft".to_owned(),
                ..test_context()
            },
            sessions.clone(),
        );

        let first = crate::with_process_env_async(
            &[("COCO_EXEC_SANDBOX", Some(OsStr::new("off")))],
            || async {
                exec_command
                    .call(format!(
                        r#"{{"cmd":"printf 'pid:%s\n' $$; read _","workdir":"{}","shell":"bash","tty":true,"yield_time_ms":100}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(first.contains("exit_status: running"));
        let pid = match parse_pid(&first) {
            Some(pid) => pid,
            None => {
                let session_id = parse_session_id(&first);
                wait_for_session_pid(write_stdin.as_ref(), &session_id).await
            }
        };
        assert!(
            wait_for_process(&pid).await,
            "child process should be running"
        );

        assert_eq!(sessions.remove_branch("other").await, 0);
        assert!(
            process_exists(&pid),
            "unrelated branch cleanup should not kill child"
        );
        assert_eq!(sessions.remove_branch("draft").await, 1);

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while process_exists(&pid) && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        assert!(
            !process_exists(&pid),
            "removing the branch should kill child process {pid}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn removing_all_sessions_kills_tty_sessions() {
        let workspace = tempfile::tempdir().unwrap();
        let sessions = session_store();
        let (exec_command, write_stdin) = runtime_pair_with_sessions(
            workspace.path().to_path_buf(),
            test_context(),
            sessions.clone(),
        );

        let first = crate::with_process_env_async(
            &[("COCO_EXEC_SANDBOX", Some(OsStr::new("off")))],
            || async {
                exec_command
                    .call(format!(
                        r#"{{"cmd":"printf 'pid:%s\n' $$; read _","workdir":"{}","shell":"bash","tty":true,"yield_time_ms":100}}"#,
                        workspace.path().display()
                    ))
                    .await
            },
        )
        .await
        .unwrap();

        assert!(first.contains("exit_status: running"));
        let pid = match parse_pid(&first) {
            Some(pid) => pid,
            None => {
                let session_id = parse_session_id(&first);
                wait_for_session_pid(write_stdin.as_ref(), &session_id).await
            }
        };
        assert!(
            wait_for_process(&pid).await,
            "child process should be running"
        );

        assert_eq!(sessions.remove_all().await, 1);

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while process_exists(&pid) && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        assert!(
            !process_exists(&pid),
            "removing all sessions should kill child process {pid}"
        );
    }

    #[tokio::test]
    async fn coco_cli_runtime_server_skips_unavailable_bridge() {
        let workspace = tempfile::tempdir().unwrap();
        let context = test_context();

        let server = start_coco_cli_runtime_server(workspace.path(), &context)
            .await
            .unwrap();

        assert!(server.is_none());
    }

    #[tokio::test]
    async fn coco_cli_runtime_socket_forwards_requests_to_bridge() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime = tempfile::tempdir().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let bridge = crate::UnifiedExecCliBridgeHandle::new(Arc::new(FakeCliBridge {
            requests: requests.clone(),
        }));
        let runtime_store = workspace.path().join("runtime-store");
        let context = ToolRuntimeEnv {
            session_branch: "draft".to_owned(),
            session_role: coco_mem::SessionRole::Runner,
            current_skill_name: None,
            active_skill: None,
            store_path: Some(runtime_store.clone()),
            enable_coco_shim: true,
            cli_bridge: bridge,
            skill_executor: crate::SkillSearchExecutorHandle::default(),
        };
        let server = crate::with_process_env_async(
            &[("XDG_RUNTIME_DIR", Some(runtime.path().as_os_str()))],
            || async { start_coco_cli_runtime_server(workspace.path(), &context).await },
        )
        .await;
        let server = match server {
            Ok(Some(server)) => server,
            Ok(None) => panic!("runtime server should be started"),
            Err(UnifiedExecToolError::BindRuntimeSocket { source })
                if source.kind() == std::io::ErrorKind::PermissionDenied =>
            {
                return;
            }
            Err(error) => panic!("runtime server setup failed: {error}"),
        };

        let request = CocoCliRuntimeRequest {
            args: vec!["job".to_owned(), "hello".to_owned()],
            stdin: b"stdin".to_vec(),
            branch_env: Some("draft".to_owned()),
            session_role: Some(coco_mem::SessionRole::Runner),
            store_path_env: Some(runtime_store.display().to_string()),
            parent_tool_use_id_env: None,
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

    #[tokio::test]
    async fn coco_cli_runtime_socket_rejects_invalid_requests() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime = tempfile::tempdir().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let bridge = crate::UnifiedExecCliBridgeHandle::new(Arc::new(FakeCliBridge {
            requests: requests.clone(),
        }));
        let context = ToolRuntimeEnv {
            cli_bridge: bridge,
            ..test_context()
        };
        let server = crate::with_process_env_async(
            &[("XDG_RUNTIME_DIR", Some(runtime.path().as_os_str()))],
            || async { start_coco_cli_runtime_server(workspace.path(), &context).await },
        )
        .await
        .unwrap()
        .unwrap();

        let mut stream = UnixStream::connect(&server.socket_path).await.unwrap();
        stream.write_all(b"not json").await.unwrap();
        stream.shutdown().await.unwrap();
        let mut output = Vec::new();
        stream.read_to_end(&mut output).await.unwrap();
        let response = serde_json::from_slice::<CocoCliRuntimeResponse>(&output).unwrap();

        assert_eq!(response.exit_code, 2);
        assert!(response.stdout.is_empty());
        assert!(
            response
                .stderr
                .starts_with("invalid coco-cli runtime request:")
        );
        assert!(requests.lock().await.is_empty());

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn coco_cli_runtime_socket_reports_bridge_errors() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime = tempfile::tempdir().unwrap();
        let bridge = crate::UnifiedExecCliBridgeHandle::new(Arc::new(FailingCliBridge));
        let context = ToolRuntimeEnv {
            cli_bridge: bridge,
            ..test_context()
        };
        let server = crate::with_process_env_async(
            &[("XDG_RUNTIME_DIR", Some(runtime.path().as_os_str()))],
            || async { start_coco_cli_runtime_server(workspace.path(), &context).await },
        )
        .await
        .unwrap()
        .unwrap();
        let request = CocoCliRuntimeRequest {
            args: vec!["job".to_owned()],
            stdin: Vec::new(),
            branch_env: None,
            session_role: None,
            store_path_env: None,
            parent_tool_use_id_env: None,
        };
        let payload = serde_json::to_vec(&request).unwrap();

        let mut stream = UnixStream::connect(&server.socket_path).await.unwrap();
        stream.write_all(&payload).await.unwrap();
        stream.shutdown().await.unwrap();
        let mut output = Vec::new();
        stream.read_to_end(&mut output).await.unwrap();
        let response = serde_json::from_slice::<CocoCliRuntimeResponse>(&output).unwrap();

        assert_eq!(response.exit_code, 1);
        assert!(response.stdout.is_empty());
        assert_eq!(response.stderr, "coco-cli runtime bridge is unavailable");

        server.shutdown().await.unwrap();
    }
}
