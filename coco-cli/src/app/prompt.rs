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
};
use coco_mem::{AnchorPayload, FsStore, JobStore, Kind, NodeStore};
use serde::Serialize;
use snafu::prelude::*;
use tokio::process::Command;

use crate::{
    COCO_DAEMON_SOCKET_ENV, Result,
    cli::{
        PromptBranchStatusCommand, PromptCommand, PromptRunCommand, PromptStatusCommand,
        PromptSubcommand, PromptWorkerCommand,
    },
    error::{
        CoreEngineSnafu, EmptyPromptSnafu, ReadStdinSnafu, ResolveCurrentExeSnafu,
        SpawnPromptWorkerSnafu,
    },
};

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
    merge_parents: Vec<String>,
}

#[derive(Debug)]
struct PromptAnchorDetails {
    node_id: String,
    prompt: String,
    merge_parents: Vec<String>,
}

pub(super) async fn run_prompt_command<B, R>(
    command: PromptCommand,
    reader: &mut R,
    shared_store: &FsStore,
    llm: &Arc<LlmService<B, FsStore>>,
    shared_engine: Option<&ConversationEngine<B, FsStore>>,
    forwarded_runtime: bool,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    R: Read,
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

async fn run_prompt_command_with_engine<B, R>(
    command: PromptCommand,
    reader: &mut R,
    shared_store: &FsStore,
    engine: &ConversationEngine<B, FsStore>,
    forwarded_runtime: bool,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    R: Read,
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

async fn run_prompt_run<B, R>(
    command: PromptRunCommand,
    reader: &mut R,
    shared_store: &FsStore,
    engine: &ConversationEngine<B, FsStore>,
    forwarded_runtime: bool,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    R: Read,
{
    let input = resolve_prompt_input(&command, reader)?;
    if command.asynchronous {
        let job = engine
            .submit_job(&command.branch, &input, vec![])
            .await
            .context(CoreEngineSnafu)?;
        ensure_job_driver(
            shared_store.path(),
            &job.job_id,
            (*engine).clone(),
            forwarded_runtime,
        )
        .await?;
        return Ok(Some(render_json(JobQueuedView {
            job_id: job.job_id.clone(),
            status: JobStatus::Queued,
            created_at: job.created_at.to_string(),
            branch: job.branch.clone(),
        })));
    }

    let service = CoreService::new(FixedBranchResolver::new(command.branch), (*engine).clone());
    let response = service
        .handle_message(InboundMessage::cli("cli", "cli", input))
        .await
        .context(crate::error::CoreSnafu)?;
    Ok(Some(response.text))
}

async fn run_prompt_status<B>(
    command: PromptStatusCommand,
    shared_store: &FsStore,
    engine: &ConversationEngine<B, FsStore>,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
{
    let snapshot = engine.get_job(&command.job).context(CoreEngineSnafu)?;
    let prompt_details =
        load_prompt_anchor_details(shared_store, &command.job).context(CoreEngineSnafu)?;
    Ok(Some(render_json(build_job_status_view(
        snapshot,
        prompt_details,
    ))))
}

async fn run_prompt_branch_status<B>(
    command: PromptBranchStatusCommand,
    engine: &ConversationEngine<B, FsStore>,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
{
    let snapshot = engine.get_job(&command.job).context(CoreEngineSnafu)?;
    if command
        .branch
        .as_ref()
        .is_none_or(|branch| branch == &snapshot.branch)
    {
        return Ok(Some(render_json(snapshot)));
    }
    Ok(Some(render_json(Vec::<JobStatusSnapshot>::new())))
}

async fn run_prompt_worker<B>(
    command: PromptWorkerCommand,
    engine: &ConversationEngine<B, FsStore>,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
{
    engine
        .drive_job(&command.job)
        .await
        .context(CoreEngineSnafu)?;
    Ok(None)
}

async fn ensure_job_driver<B>(
    store_path: &Path,
    job_id: &str,
    engine: ConversationEngine<B, FsStore>,
    forwarded_runtime: bool,
) -> Result<()>
where
    B: CompletionBackend + 'static,
{
    if forwarded_runtime {
        let job_id = job_id.to_owned();
        tokio::spawn(async move {
            let _ = engine.drive_job(&job_id).await;
        });
        return Ok(());
    }

    spawn_prompt_worker(store_path, job_id).await
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
    store: &FsStore,
    job_id: &str,
) -> std::result::Result<PromptAnchorDetails, EngineError> {
    let job = store.get_job(job_id)?;
    let node = store.get_node(&job.base)?;
    match node.kind {
        Kind::Anchor(anchor) => match anchor.payload {
            AnchorPayload::Prompt(prompt_anchor) => Ok(PromptAnchorDetails {
                node_id: node.id,
                prompt: prompt_anchor.prompt,
                merge_parents: anchor.merge_parents,
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
