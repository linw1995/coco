use std::io::Read;
use std::sync::Arc;

use coco_core::{ConversationEngine, CoreService, FixedBranchResolver, InboundMessage};
use coco_llm::{
    CompletionBackend, LlmService, RigBackend, SessionConfig, SessionConfigPatch, SessionFeedback,
    SessionMerge,
};
use coco_mem::{AnchorPayload, FsStore, PauseReason, SessionAnchor, SessionState, Store};
use serde::Serialize;
use snafu::prelude::*;

use crate::{
    Result,
    cli::{Cli, Command, PromptCommand, SessionCommand, SessionCreateCommand, SessionSubcommand},
    env::{read_env, resolve_env_provider},
    error::{
        CoreSnafu, EmptyPromptSnafu, LlmSnafu, MissingConfigurationSnafu, ReadStdinSnafu,
        StoreSnafu,
    },
    store::open_store,
};

pub async fn run<R>(cli: Cli, reader: &mut R) -> Result<Option<String>>
where
    R: Read,
{
    run_with_backend(cli, reader, RigBackend).await
}

pub async fn run_with_backend<B, R>(cli: Cli, reader: &mut R, backend: B) -> Result<Option<String>>
where
    B: CompletionBackend,
    R: Read,
{
    let shared_store = open_store(&cli.store_path)?;
    let llm = Arc::new(LlmService::new(shared_store.clone(), backend));

    match cli.command {
        Command::Prompt(command) => {
            let input = resolve_prompt_input(&command, reader)?;
            let service = CoreService::new(
                FixedBranchResolver::new(command.branch),
                ConversationEngine::new(llm),
            );
            let response = service
                .handle_message(InboundMessage::cli("cli", "cli", input))
                .await
                .context(CoreSnafu)?;
            Ok(Some(response.text))
        }
        Command::Session(command) => run_session_command(command, &shared_store, &llm).await,
    }
}

pub fn resolve_session_config(command: SessionCreateCommand) -> Result<SessionConfig> {
    let provider = resolve_env_provider()?;
    let model = read_env("COCO_MODEL").context(MissingConfigurationSnafu { name: "COCO_MODEL" })?;

    Ok(SessionConfig {
        branch: command.branch,
        merge_parents: vec![],
        provider: provider.into(),
        model,
        system_prompt: command.system_prompt,
        prompt: command.prompt,
        tools: vec![],
        temperature: command.temperature,
        max_tokens: command.max_tokens,
        additional_params: None,
    })
}

pub fn resolve_prompt_input<R>(command: &PromptCommand, reader: &mut R) -> Result<String>
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

#[derive(Debug, Serialize, PartialEq)]
struct SessionSummary {
    branch: String,
    head_id: String,
    state: SessionState,
}

#[derive(Debug, Serialize, PartialEq)]
struct SessionDetails {
    branch: String,
    head_id: String,
    anchor_id: String,
    state: SessionState,
    anchor: SessionAnchor,
}

#[derive(Debug, Serialize, PartialEq)]
struct SessionMutationResult {
    branch: String,
    state: SessionState,
}

#[derive(Debug, Serialize, PartialEq)]
struct SessionRebaseResult {
    branch: String,
    head_id: String,
}

#[derive(Debug, Serialize, PartialEq)]
struct SessionForkResult {
    branch: String,
    head_id: String,
    state: SessionState,
}

#[derive(Debug, Serialize, PartialEq)]
struct PullRequestResult {
    branch: String,
    target_branch: String,
    base_head_id: String,
    state: SessionState,
}

#[derive(Debug, Serialize, PartialEq)]
struct SessionMergeResult {
    branch: String,
    target_branch: String,
    source_head_id: String,
    merged_anchor_id: String,
    state: SessionState,
}

#[derive(Debug, Serialize, PartialEq)]
struct SessionFeedbackResult {
    branch: String,
    target_branch: String,
    base_head_id: String,
    source_anchor_id: String,
    feedback_anchor_id: String,
    state: SessionState,
}

async fn run_session_command<B>(
    command: SessionCommand,
    store: &FsStore,
    llm: &Arc<LlmService<B, FsStore>>,
) -> Result<Option<String>>
where
    B: CompletionBackend,
{
    match command.command {
        SessionSubcommand::Create(command) => {
            let config = resolve_session_config(command)?;
            llm.create_session(config).await.context(LlmSnafu)?;
            Ok(None)
        }
        SessionSubcommand::Fork(command) => {
            let branch = command.branch.clone();
            let head_id = llm
                .fork(branch.clone(), &command.from_ref)
                .context(LlmSnafu)?;
            Ok(Some(render_json(SessionForkResult {
                state: store.get_session_state(&branch).context(StoreSnafu)?,
                branch,
                head_id,
            })))
        }
        SessionSubcommand::List => Ok(Some(render_json(list_sessions(store)?))),
        SessionSubcommand::Get(command) => Ok(Some(render_json(read_session_details(
            store,
            &command.branch,
        )?))),
        SessionSubcommand::Rebase(command) => {
            let branch = command.branch.clone();
            let head_id = llm
                .rebase_session(&branch, resolve_session_patch(command))
                .await
                .context(LlmSnafu)?;
            Ok(Some(render_json(SessionRebaseResult { branch, head_id })))
        }
        SessionSubcommand::Reopen(command) => Ok(Some(render_json(SessionMutationResult {
            branch: command.branch.clone(),
            state: store
                .set_session_state(&command.branch, None, SessionState::Active)
                .context(StoreSnafu)?,
        }))),
        SessionSubcommand::Pr(command) => {
            let pr = llm
                .open_pull_request(&command.branch, &command.target_branch)
                .await
                .context(LlmSnafu)?;
            Ok(Some(render_json(build_pull_request_result(store, pr)?)))
        }
        SessionSubcommand::Close(command) => Ok(Some(render_json(SessionMutationResult {
            branch: command.branch.clone(),
            state: store
                .set_session_state(
                    &command.branch,
                    None,
                    SessionState::Paused {
                        target_branch: command.target_branch,
                        reason: PauseReason::Closed,
                    },
                )
                .context(StoreSnafu)?,
        }))),
        SessionSubcommand::Merge(command) => {
            let merged = llm
                .merge_session(
                    &command.branch,
                    command.target_branch.as_deref(),
                    &command.prompt,
                )
                .await
                .context(LlmSnafu)?;
            Ok(Some(render_json(build_session_merge_result(
                store, merged,
            )?)))
        }
        SessionSubcommand::Feedback(command) => {
            let feedback = llm
                .apply_feedback(
                    &command.branch,
                    &command.prompt,
                    command.from_ref.as_deref(),
                )
                .await
                .context(LlmSnafu)?;
            Ok(Some(render_json(build_session_feedback_result(
                store, feedback,
            )?)))
        }
    }
}

fn list_sessions(store: &FsStore) -> Result<Vec<SessionSummary>> {
    let states = store.list_session_states().context(StoreSnafu)?;
    let mut branches = states.into_iter().collect::<Vec<_>>();
    branches.sort_by(|(left, _), (right, _)| left.cmp(right));

    branches
        .into_iter()
        .map(|(branch, state)| {
            Ok(SessionSummary {
                head_id: store.get_branch_head(&branch).context(StoreSnafu)?,
                branch,
                state,
            })
        })
        .collect()
}

fn read_session_details(store: &FsStore, branch: &str) -> Result<SessionDetails> {
    let head_id = store.get_branch_head(branch).context(StoreSnafu)?;
    let state = store.get_session_state(branch).context(StoreSnafu)?;
    let (anchor_id, anchor) = resolve_visible_session_anchor(store, branch)?;

    Ok(SessionDetails {
        branch: branch.to_owned(),
        head_id,
        anchor_id,
        state,
        anchor,
    })
}

fn build_pull_request_result(
    store: &FsStore,
    pr: coco_llm::PullRequest,
) -> Result<PullRequestResult> {
    Ok(PullRequestResult {
        branch: pr.branch.clone(),
        target_branch: pr.target_branch,
        base_head_id: pr.base_head_id,
        state: store.get_session_state(&pr.branch).context(StoreSnafu)?,
    })
}

fn build_session_merge_result(store: &FsStore, merged: SessionMerge) -> Result<SessionMergeResult> {
    Ok(SessionMergeResult {
        branch: merged.branch.clone(),
        target_branch: merged.target_branch,
        source_head_id: merged.source_head_id,
        merged_anchor_id: merged.merged_anchor_id,
        state: store
            .get_session_state(&merged.branch)
            .context(StoreSnafu)?,
    })
}

fn build_session_feedback_result(
    store: &FsStore,
    feedback: SessionFeedback,
) -> Result<SessionFeedbackResult> {
    Ok(SessionFeedbackResult {
        branch: feedback.branch.clone(),
        target_branch: feedback.target_branch,
        base_head_id: feedback.base_head_id,
        source_anchor_id: feedback.source_anchor_id,
        feedback_anchor_id: feedback.feedback_anchor_id,
        state: store
            .get_session_state(&feedback.branch)
            .context(StoreSnafu)?,
    })
}

fn resolve_visible_session_anchor(
    store: &FsStore,
    branch: &str,
) -> Result<(String, SessionAnchor)> {
    let ancestry = store.ancestry(branch).context(StoreSnafu)?;
    for node in ancestry {
        let coco_mem::Kind::Anchor(anchor) = node.kind else {
            continue;
        };

        let AnchorPayload::Session(session_anchor) = anchor.payload else {
            continue;
        };

        return Ok((node.id, session_anchor));
    }

    Err(crate::Error::Llm {
        source: coco_llm::Error::MissingAnchor {
            branch: branch.to_owned(),
        },
    })
}

fn resolve_session_patch(command: crate::cli::SessionRebaseCommand) -> SessionConfigPatch {
    SessionConfigPatch {
        provider: command
            .provider
            .map(|provider| coco_llm::Provider::from(provider).as_str().to_owned()),
        model: command.model,
        tools: None,
        system_prompt: command.system_prompt,
        prompt: command.prompt,
        temperature: if command.clear_temperature {
            Some(None)
        } else {
            command.temperature.map(Some)
        },
        max_tokens: if command.clear_max_tokens {
            Some(None)
        } else {
            command.max_tokens.map(Some)
        },
        additional_params: None,
    }
}

fn render_json<T>(value: T) -> String
where
    T: Serialize,
{
    serde_json::to_string_pretty(&value).expect("session output should serialize")
}
