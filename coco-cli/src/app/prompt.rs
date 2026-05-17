use std::io::Read;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;

use coco_core::{
    ConversationEngine, CoreService, EngineError, FixedBranchResolver, InboundMessage, JobStatus,
    JobStatusSnapshot,
};
use coco_llm::{
    COCO_CLI_RUNTIME_SOCKET_ENV, COCO_SESSION_BRANCH_ENV, CompletionBackend, LlmService,
    SessionConfigPatch,
};
use coco_mem::{AnchorPayload, JobStore, Kind, MergeParent, MessageQueueItem, NodeStore, Store};
use serde::{Deserialize, Serialize};
use serde_json::json;
use snafu::prelude::*;
use tokio::process::Command;

use crate::{
    COCO_DAEMON_SOCKET_ENV, Result,
    cli::{
        CliTool, PromptBranchStatusCommand, PromptCommand, PromptRunCommand, PromptStatusCommand,
        PromptSubcommand, PromptWorkerCommand,
    },
    error::{
        CoreEngineSnafu, EmptyPromptSnafu, ReadStdinSnafu, ResolveCurrentExeSnafu,
        SpawnPromptWorkerSnafu, StoreSnafu,
    },
};

pub(crate) const PROMPT_JOB_QUEUE: &str = "prompt.job";

#[derive(Debug, Serialize)]
struct JobQueuedView {
    job_id: String,
    status: JobStatus,
    created_at: String,
    branch: String,
}

#[derive(Debug, Serialize)]
struct PromptJobStatusView {
    job: JobStatusSnapshot,
    base_node: PromptBaseNodeView,
}

#[derive(Debug, Serialize)]
struct PromptBaseNodeView {
    node_id: String,
    kind: &'static str,
    prompt: String,
    merge_parents: Vec<MergeParent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueuedPromptRequest {
    pub job_id: String,
    pub branch: String,
    pub prompt: String,
    pub merge_parents: Vec<MergeParent>,
    pub session_patch: Option<SessionConfigPatch>,
}

#[derive(Debug, Serialize)]
struct QueuedPromptRequestStatusView {
    job: QueuedPromptRequestJobView,
    request: QueuedPromptRequestDetailsView,
}

#[derive(Debug, Serialize)]
struct QueuedPromptRequestJobView {
    job_id: String,
    created_at: String,
    finished_at: Option<String>,
    branch: String,
    base: String,
    status: JobStatus,
    head: String,
}

#[derive(Debug, Serialize)]
struct QueuedPromptRequestDetailsView {
    prompt: String,
    merge_parents: Vec<MergeParent>,
}

#[derive(Debug)]
struct PromptAnchorDetails {
    node_id: String,
    prompt: String,
    merge_parents: Vec<MergeParent>,
}

pub(super) async fn run_prompt_command<B, R, S>(
    command: PromptCommand,
    reader: &mut R,
    shared_store: &S,
    llm: &Arc<LlmService<B, S>>,
    shared_engine: Option<&ConversationEngine<B, S>>,
    forwarded_runtime: bool,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    R: Read,
    S: Store + Clone + Send + Sync + 'static,
{
    if let Some(engine) = shared_engine {
        return run_prompt_command_with_engine(
            command,
            reader,
            shared_store,
            engine,
            forwarded_runtime,
        )
        .await;
    }

    let engine = ConversationEngine::new(llm.clone());
    run_prompt_command_with_engine(command, reader, shared_store, &engine, forwarded_runtime).await
}

async fn run_prompt_command_with_engine<B, R, S>(
    command: PromptCommand,
    reader: &mut R,
    shared_store: &S,
    engine: &ConversationEngine<B, S>,
    forwarded_runtime: bool,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    R: Read,
    S: Store + Clone + Send + Sync + 'static,
{
    match command.command {
        None => run_prompt_run(command.run, reader, shared_store, engine, forwarded_runtime).await,
        Some(PromptSubcommand::Status(command)) => {
            run_prompt_status(command, shared_store, engine).await
        }
        Some(PromptSubcommand::BranchStatus(command)) => {
            run_prompt_branch_status(command, engine).await
        }
        Some(PromptSubcommand::Worker(command)) => run_prompt_worker(command, engine).await,
    }
}

pub fn resolve_prompt_input<R>(command: &PromptRunCommand, reader: &mut R) -> Result<String>
where
    R: Read,
{
    let text = if command.text.is_empty() {
        let mut buffer = String::new();
        reader.read_to_string(&mut buffer).context(ReadStdinSnafu)?;
        buffer.trim_end_matches(['\r', '\n']).to_owned()
    } else {
        command.text.join(" ")
    };

    ensure!(!text.trim().is_empty(), EmptyPromptSnafu);
    Ok(text)
}

async fn run_prompt_run<B, R, S>(
    command: PromptRunCommand,
    reader: &mut R,
    shared_store: &S,
    engine: &ConversationEngine<B, S>,
    forwarded_runtime: bool,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    R: Read,
    S: Store + Clone + Send + Sync + 'static,
{
    let input = resolve_prompt_input(&command, reader)?;
    let session_patch = resolve_prompt_session_patch(&command);
    if command.asynchronous {
        if forwarded_runtime {
            let job_id = next_prompt_job_id();
            let item = queue_prompt_job_request(
                shared_store,
                QueuedPromptRequest {
                    job_id: job_id.clone(),
                    branch: command.branch.clone(),
                    prompt: input,
                    merge_parents: command.merge_parents,
                    session_patch,
                },
            )?;
            let view = JobQueuedView {
                job_id,
                status: JobStatus::Queued,
                created_at: item.created_at.to_string(),
                branch: command.branch,
            };
            return Ok(Some(if command.json {
                render_json(view)
            } else {
                render_job_queued_text(&view)
            }));
        }

        let job = engine
            .submit_job_with_session_patch(
                &command.branch,
                &input,
                command.merge_parents,
                session_patch,
            )
            .await
            .context(CoreEngineSnafu)?;
        let store_path = engine
            .runtime_store_path()
            .context(crate::error::StoreRuntimePathUnavailableSnafu)?;
        ensure_job_driver(Some(store_path), &job.job_id).await?;
        let view = JobQueuedView {
            job_id: job.job_id.clone(),
            status: JobStatus::Queued,
            created_at: job.created_at.to_string(),
            branch: job.branch.clone(),
        };
        return Ok(Some(if command.json {
            render_json(view)
        } else {
            render_job_queued_text(&view)
        }));
    }

    if !command.merge_parents.is_empty() || session_patch.is_some() {
        return engine
            .reply_with_session_patch(
                &command.branch,
                &input,
                command.merge_parents,
                session_patch,
            )
            .await
            .map(Some)
            .context(CoreEngineSnafu);
    }

    let service = CoreService::new(FixedBranchResolver::new(command.branch), (*engine).clone());
    let response = service
        .handle_message(InboundMessage::cli("cli", "cli", input))
        .await
        .context(crate::error::CoreSnafu)?;
    Ok(Some(response.text))
}

fn resolve_prompt_session_patch(command: &PromptRunCommand) -> Option<SessionConfigPatch> {
    let mut patch = SessionConfigPatch::default();
    let mut has_patch = false;

    if let Some(role) = command.role {
        patch.role = Some(role.into());
        has_patch = true;
    }
    if command.clear_tools {
        patch.tools = Some(vec![]);
        has_patch = true;
    } else if !command.tools.is_empty() {
        patch.tools = Some(resolve_cli_tools(&command.tools));
        has_patch = true;
    }
    if command.enable_coco_shim {
        patch.enable_coco_shim = Some(true);
        has_patch = true;
    } else if command.disable_coco_shim {
        patch.enable_coco_shim = Some(false);
        has_patch = true;
    }

    has_patch.then_some(patch)
}

fn resolve_cli_tools(tools: &[CliTool]) -> Vec<coco_mem::Tool> {
    tools
        .iter()
        .copied()
        .map(CliTool::as_str)
        .map(|name| {
            coco_llm::builtin_tool_definition(name)
                .expect("CliTool names should always map to built-in tool definitions")
        })
        .collect()
}

async fn run_prompt_status<B, S>(
    command: PromptStatusCommand,
    shared_store: &S,
    engine: &ConversationEngine<B, S>,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    let snapshot = match engine.get_job(&command.job) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            if let Some(view) = load_queued_prompt_request_status(shared_store, &command.job)? {
                return Ok(Some(if command.json {
                    render_json(view)
                } else {
                    render_queued_prompt_request_status_text(&view)
                }));
            }
            return Err(error).context(CoreEngineSnafu);
        }
    };
    let prompt_details =
        load_prompt_anchor_details(shared_store, &command.job).context(CoreEngineSnafu)?;
    let view = build_job_status_view(snapshot, prompt_details);
    Ok(Some(if command.json {
        render_json(view)
    } else {
        render_prompt_status_text(&view)
    }))
}

async fn run_prompt_branch_status<B, S>(
    command: PromptBranchStatusCommand,
    engine: &ConversationEngine<B, S>,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    let snapshot = engine.get_job(&command.job).context(CoreEngineSnafu)?;
    if command
        .branch
        .as_ref()
        .is_none_or(|branch| branch == &snapshot.branch)
    {
        return Ok(Some(if command.json {
            render_json(snapshot)
        } else {
            render_job_status_snapshot_text(&snapshot)
        }));
    }
    Ok(Some(if command.json {
        render_json(Vec::<JobStatusSnapshot>::new())
    } else {
        "No matching prompt job.".to_owned()
    }))
}

async fn run_prompt_worker<B, S>(
    command: PromptWorkerCommand,
    engine: &ConversationEngine<B, S>,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    engine
        .drive_job_with_merge_parents(&command.job, command.merge_parents)
        .await
        .context(CoreEngineSnafu)?;
    Ok(None)
}

async fn ensure_job_driver(store_path: Option<&Path>, job_id: &str) -> Result<()> {
    let store_path = store_path.context(crate::error::StoreRuntimePathUnavailableSnafu)?;
    spawn_prompt_worker(store_path, job_id).await
}

fn queue_prompt_job_request(
    store: &impl Store,
    request: QueuedPromptRequest,
) -> Result<MessageQueueItem> {
    store
        .enqueue_message(PROMPT_JOB_QUEUE, json!(request))
        .context(StoreSnafu)
}

async fn spawn_prompt_worker(store_path: &Path, job_id: &str) -> Result<()> {
    let current_exe = std::env::current_exe().context(ResolveCurrentExeSnafu)?;
    Command::new(current_exe)
        .arg("--store-path")
        .arg(store_path)
        .arg("prompt")
        .arg("worker")
        .arg("--job")
        .arg(job_id)
        .env_remove(COCO_DAEMON_SOCKET_ENV)
        .env_remove(COCO_CLI_RUNTIME_SOCKET_ENV)
        .env_remove(COCO_SESSION_BRANCH_ENV)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context(SpawnPromptWorkerSnafu)?;
    Ok(())
}

fn build_job_status_view(
    snapshot: JobStatusSnapshot,
    prompt_details: PromptAnchorDetails,
) -> PromptJobStatusView {
    PromptJobStatusView {
        job: snapshot,
        base_node: PromptBaseNodeView {
            node_id: prompt_details.node_id,
            kind: "prompt",
            prompt: prompt_details.prompt,
            merge_parents: prompt_details.merge_parents,
        },
    }
}

fn load_prompt_anchor_details(
    store: &(impl JobStore + NodeStore),
    job_id: &str,
) -> std::result::Result<PromptAnchorDetails, EngineError> {
    let job = store.get_job(job_id)?;
    let node = store.get_node(&job.base)?;
    match node.kind {
        Kind::Anchor(anchor) => match &anchor.payload {
            AnchorPayload::Prompt(prompt_anchor) => Ok(PromptAnchorDetails {
                node_id: node.id,
                prompt: prompt_anchor.prompt.clone(),
                merge_parents: anchor.merge_parents().to_vec(),
            }),
            _ => Err(EngineError::EngineFailed {
                message: format!(
                    "job {:?} prompt anchor {:?} does not contain a prompt payload",
                    job.job_id, job.base
                ),
            }),
        },
        _ => Err(EngineError::EngineFailed {
            message: format!(
                "job {:?} prompt anchor {:?} is not an anchor node",
                job.job_id, job.base
            ),
        }),
    }
}

fn render_json<T>(value: T) -> String
where
    T: Serialize,
{
    serde_json::to_string_pretty(&value).expect("prompt output should serialize")
}

fn render_job_queued_text(view: &JobQueuedView) -> String {
    format!(
        "job_id: {}\nstatus: {:?}\ncreated_at: {}\nbranch: {}",
        view.job_id, view.status, view.created_at, view.branch
    )
}

fn render_prompt_status_text(view: &PromptJobStatusView) -> String {
    format!(
        "{}\nbase_node: {}\nbase_kind: {}\nprompt: {}\nmerge_parents: {}",
        render_job_status_snapshot_text(&view.job),
        view.base_node.node_id,
        view.base_node.kind,
        view.base_node.prompt,
        render_merge_parents(&view.base_node.merge_parents)
    )
}

fn render_queued_prompt_request_status_text(view: &QueuedPromptRequestStatusView) -> String {
    format!(
        "{}\nprompt: {}\nmerge_parents: {}",
        render_queued_prompt_request_job_text(&view.job),
        view.request.prompt,
        render_merge_parents(&view.request.merge_parents)
    )
}

fn render_queued_prompt_request_job_text(snapshot: &QueuedPromptRequestJobView) -> String {
    format!(
        "job_id: {}\nstatus: {:?}\nbranch: {}\nbase: {}\nhead: {}\ncreated_at: {}\nfinished_at: {}",
        snapshot.job_id,
        snapshot.status,
        snapshot.branch,
        snapshot.base,
        snapshot.head,
        snapshot.created_at,
        snapshot
            .finished_at
            .clone()
            .unwrap_or_else(|| "null".to_owned())
    )
}

fn render_job_status_snapshot_text(snapshot: &JobStatusSnapshot) -> String {
    format!(
        "job_id: {}\nstatus: {:?}\nbranch: {}\nbase: {}\nhead: {}\ncreated_at: {}\nfinished_at: {}",
        snapshot.job_id,
        snapshot.status,
        snapshot.branch,
        snapshot.base,
        snapshot.head,
        snapshot.created_at,
        snapshot
            .finished_at
            .map(|finished_at| finished_at.to_string())
            .unwrap_or_else(|| "null".to_owned())
    )
}

fn load_queued_prompt_request_status(
    store: &impl Store,
    job_id: &str,
) -> Result<Option<QueuedPromptRequestStatusView>> {
    let items = store
        .list_queue_messages(PROMPT_JOB_QUEUE)
        .context(StoreSnafu)?;
    let Some((item, request)) = items.into_iter().find_map(|item| {
        serde_json::from_value::<QueuedPromptRequest>(item.payload.clone())
            .ok()
            .filter(|request| request.job_id == job_id)
            .map(|request| (item, request))
    }) else {
        return Ok(None);
    };
    let head = store
        .get_branch_head(&request.branch)
        .context(StoreSnafu)?
        .to_owned();
    Ok(Some(QueuedPromptRequestStatusView {
        job: QueuedPromptRequestJobView {
            job_id: request.job_id,
            created_at: item.created_at.to_string(),
            finished_at: None,
            branch: request.branch,
            base: head.clone(),
            status: JobStatus::Queued,
            head,
        },
        request: QueuedPromptRequestDetailsView {
            prompt: request.prompt,
            merge_parents: request.merge_parents,
        },
    }))
}

fn next_prompt_job_id() -> String {
    format!("job-{}", nanoid::nanoid!())
}

fn render_merge_parents(parents: &[MergeParent]) -> String {
    if parents.is_empty() {
        return "[]".to_owned();
    }

    let rendered = parents
        .iter()
        .map(|parent| {
            let kind = if parent.is_shadow() {
                "shadow"
            } else {
                "merge"
            };
            format!("{kind}:{}", parent.node_id())
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{rendered}]")
}
