use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, Weak};

use clap::Parser;
use coco_core::{ConversationEngine, CoreService, FixedBranchResolver, InboundMessage};
use coco_llm::{
    BashToolCliBridge, BashToolCliBridgeError, BashToolCliBridgeHandle, CocoCliRuntimeRequest,
    CocoCliRuntimeResponse, CompletionBackend, LlmService, RigBackend, SessionConfig,
    SessionConfigPatch, SessionFeedback, SessionMerge,
};
use coco_mem::{
    AnchorPayload, FsStore, Kind, Node, PauseReason, SessionAnchor, SessionState, Store,
    StoreError, Tool,
};
use serde::Serialize;
use snafu::prelude::*;

use crate::{
    Result,
    cli::{Cli, Command, PromptCommand, SessionCommand, SessionCreateCommand, SessionSubcommand},
    env::{read_env, resolve_env_provider, resolve_env_tools},
    error::{
        AmbiguousNodePrefixSnafu, CoreSnafu, EmptyPromptSnafu, LlmSnafu, MissingConfigurationSnafu,
        ReadStdinSnafu, StoreSnafu, UnknownShowReferenceSnafu,
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
    B: CompletionBackend + 'static,
    R: Read,
{
    let shared_store = open_store(&cli.store_path)?;
    let llm = Arc::new_cyclic(|weak_llm| {
        let bridge = BashToolCliBridgeHandle::new(Arc::new(CocoCliRuntimeBridge {
            store: shared_store.clone(),
            llm: weak_llm.clone(),
        }));
        LlmService::new(shared_store.clone(), backend).with_bash_tool_cli_bridge(bridge)
    });

    run_with_services(cli, reader, &shared_store, &llm).await
}

pub async fn run_with_services<B, R>(
    cli: Cli,
    reader: &mut R,
    shared_store: &FsStore,
    llm: &Arc<LlmService<B, FsStore>>,
) -> Result<Option<String>>
where
    B: CompletionBackend,
    R: Read,
{
    match cli.command {
        Command::Prompt(command) => {
            let input = resolve_prompt_input(&command, reader)?;
            let service = CoreService::new(
                FixedBranchResolver::new(command.branch),
                ConversationEngine::new(llm.clone()),
            );
            let response = service
                .handle_message(InboundMessage::cli("cli", "cli", input))
                .await
                .context(CoreSnafu)?;
            Ok(Some(response.text))
        }
        Command::Session(command) => run_session_command(command, shared_store, llm).await,
    }
}

pub async fn run_forwarded_with_services<B>(
    args: &[String],
    stdin: &[u8],
    branch_env: Option<&str>,
    store_path_env: Option<&str>,
    shared_store: &FsStore,
    llm: &Arc<LlmService<B, FsStore>>,
) -> CocoCliRuntimeResponse
where
    B: CompletionBackend,
{
    let argv = std::iter::once("coco-cli".to_owned())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>();
    let mut cli = match Cli::try_parse_from(argv) {
        Ok(cli) => cli,
        Err(error) => {
            let output = error.to_string();
            return if error.use_stderr() {
                CocoCliRuntimeResponse {
                    exit_code: error.exit_code(),
                    stdout: String::new(),
                    stderr: output,
                }
            } else {
                CocoCliRuntimeResponse {
                    exit_code: error.exit_code(),
                    stdout: output,
                    stderr: String::new(),
                }
            };
        }
    };

    apply_forwarded_defaults(&mut cli, args, branch_env, store_path_env);

    match run_with_services(cli, &mut std::io::Cursor::new(stdin), shared_store, llm).await {
        Ok(Some(output)) => CocoCliRuntimeResponse {
            exit_code: 0,
            stdout: format!("{output}\n"),
            stderr: String::new(),
        },
        Ok(None) => CocoCliRuntimeResponse {
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
        },
        Err(error) => CocoCliRuntimeResponse {
            exit_code: 1,
            stdout: String::new(),
            stderr: format!("{error}\n"),
        },
    }
}

#[derive(Debug)]
struct CocoCliRuntimeBridge<B>
where
    B: CompletionBackend,
{
    store: FsStore,
    llm: Weak<LlmService<B, FsStore>>,
}

#[async_trait::async_trait]
impl<B> BashToolCliBridge for CocoCliRuntimeBridge<B>
where
    B: CompletionBackend,
{
    async fn execute_coco_cli(
        &self,
        request: CocoCliRuntimeRequest,
    ) -> std::result::Result<CocoCliRuntimeResponse, BashToolCliBridgeError> {
        let llm = self
            .llm
            .upgrade()
            .ok_or(BashToolCliBridgeError::Unavailable)?;
        Ok(run_forwarded_with_services(
            &request.args,
            &request.stdin,
            request.branch_env.as_deref(),
            request.store_path_env.as_deref(),
            &self.store,
            &llm,
        )
        .await)
    }
}

fn has_explicit_flag(args: &[String], name: &str) -> bool {
    let long = format!("--{name}");
    let long_eq = format!("--{name}=");
    args.iter()
        .any(|arg| arg == &long || arg.starts_with(&long_eq))
}

fn apply_forwarded_defaults(
    cli: &mut Cli,
    args: &[String],
    branch_env: Option<&str>,
    store_path_env: Option<&str>,
) {
    if !has_explicit_flag(args, "store-path")
        && let Some(store_path_env) = store_path_env
    {
        cli.store_path = PathBuf::from(store_path_env);
    }

    if has_explicit_flag(args, "branch") {
        return;
    }
    let Some(branch_env) = branch_env else {
        return;
    };
    let branch = branch_env.to_owned();

    match &mut cli.command {
        Command::Prompt(command) => command.branch = branch,
        Command::Session(command) => match &mut command.command {
            SessionSubcommand::Create(command) => command.branch = branch,
            SessionSubcommand::Fork(_) => {}
            SessionSubcommand::List => {}
            SessionSubcommand::Get(command) => command.branch = branch,
            SessionSubcommand::Graph(_) => {}
            SessionSubcommand::Show(_) => {}
            SessionSubcommand::Rebase(command) => command.branch = branch,
            SessionSubcommand::Reopen(command) => command.branch = branch,
            SessionSubcommand::Pr(command) => command.branch = branch,
            SessionSubcommand::Close(command) => command.branch = branch,
            SessionSubcommand::Merge(command) => command.branch = branch,
            SessionSubcommand::Feedback(command) => command.branch = branch,
        },
    }
}

pub fn resolve_session_config(command: SessionCreateCommand) -> Result<SessionConfig> {
    let provider = resolve_env_provider()?;
    let model = read_env("COCO_MODEL").context(MissingConfigurationSnafu { name: "COCO_MODEL" })?;
    let tools = if command.tools.is_empty() {
        resolve_env_tools()?
            .into_iter()
            .map(crate::cli::CliTool::to_tool)
            .collect()
    } else {
        resolve_cli_tools(&command.tools)
    };

    Ok(SessionConfig {
        branch: command.branch,
        merge_parents: vec![],
        provider: provider.into(),
        model,
        system_prompt: command.system_prompt,
        prompt: command.prompt,
        tools,
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

#[derive(Debug, Clone)]
struct GraphBranchLabel {
    branch: String,
    state: SessionState,
}

#[derive(Debug, Clone)]
struct GraphNodeEntry {
    node: Node,
    primary_parent: Option<String>,
    merge_parents: Vec<String>,
    labels: Vec<GraphBranchLabel>,
}

#[derive(Debug)]
struct GraphTransition {
    next_columns: Vec<String>,
    connector_row: Option<String>,
}

#[derive(Debug, Serialize, PartialEq)]
struct NodeShowResult {
    #[serde(rename = "ref")]
    reference: String,
    resolved_id: String,
    node: Node,
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
        SessionSubcommand::Graph(_) => Ok(Some(render_session_graph(store)?)),
        SessionSubcommand::Show(command) => Ok(Some(render_session_show(
            store,
            &command.reference,
            command.json,
        )?)),
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

fn render_session_graph(store: &FsStore) -> Result<String> {
    let states = store.list_session_states().context(StoreSnafu)?;
    if states.is_empty() {
        return Ok("No sessions found.".to_owned());
    }

    let mut branches = states.into_iter().collect::<Vec<_>>();
    branches.sort_by(|(left, _), (right, _)| left.cmp(right));

    let mut visible_node_ids = BTreeSet::new();
    let mut visible_nodes = HashMap::new();
    let mut labels_by_node = HashMap::<String, Vec<GraphBranchLabel>>::new();

    for (branch, state) in branches {
        let head_id = store.get_branch_head(&branch).context(StoreSnafu)?;
        let ancestry = store.ancestry(&head_id).context(StoreSnafu)?;

        for node in ancestry {
            if node.is_root() {
                continue;
            }
            visible_node_ids.insert(node.id.clone());
            visible_nodes.insert(node.id.clone(), node.clone());
        }

        labels_by_node
            .entry(head_id)
            .or_default()
            .push(GraphBranchLabel { branch, state });
    }

    let mut entries = visible_nodes
        .into_values()
        .map(|node| {
            let primary_parent = resolve_visible_parent(&visible_node_ids, node.parent.as_str());
            let merge_parents = match &node.kind {
                Kind::Anchor(anchor) => {
                    let mut parents = Vec::new();
                    for merge_parent in anchor.merge_parents() {
                        let Some(parent_id) =
                            resolve_visible_parent(&visible_node_ids, merge_parent)
                        else {
                            continue;
                        };
                        if primary_parent.as_ref() == Some(&parent_id)
                            || parents.iter().any(|existing| existing == &parent_id)
                        {
                            continue;
                        }
                        parents.push(parent_id);
                    }
                    parents
                }
                _ => vec![],
            };

            Ok(GraphNodeEntry {
                labels: labels_by_node.remove(&node.id).unwrap_or_default(),
                node,
                primary_parent,
                merge_parents,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    entries.sort_by(|left, right| {
        let left_ts = left.node.created_at.to_string();
        let right_ts = right.node.created_at.to_string();
        right_ts
            .cmp(&left_ts)
            .then_with(|| right.node.id.cmp(&left.node.id))
    });

    Ok(render_graph_entries(&entries))
}

fn render_session_show(store: &FsStore, reference: &str, json_output: bool) -> Result<String> {
    let resolved_id = resolve_show_reference(store, reference)?;
    let node = store.get_node(&resolved_id).context(StoreSnafu)?;
    let result = NodeShowResult {
        reference: reference.to_owned(),
        resolved_id,
        node,
    };

    if json_output {
        Ok(render_json(result))
    } else {
        Ok(render_node_show_text(&result))
    }
}

fn resolve_show_reference(store: &FsStore, reference: &str) -> Result<String> {
    match store.get_branch_head(reference) {
        Ok(head_id) => Ok(head_id),
        Err(StoreError::BranchNotFound { .. }) => match store.get_node(reference) {
            Ok(_) => Ok(reference.to_owned()),
            Err(StoreError::NotFound { .. }) => {
                let matches = collect_known_node_ids(store)?
                    .into_iter()
                    .filter(|node_id| node_id.starts_with(reference))
                    .collect::<Vec<_>>();
                match matches.as_slice() {
                    [matched] => Ok(matched.clone()),
                    [] => UnknownShowReferenceSnafu {
                        reference: reference.to_owned(),
                    }
                    .fail(),
                    _ => AmbiguousNodePrefixSnafu {
                        prefix: reference.to_owned(),
                        matches,
                    }
                    .fail(),
                }
            }
            Err(source) => Err(crate::Error::Store { source }),
        },
        Err(source) => Err(crate::Error::Store { source }),
    }
}

fn collect_known_node_ids(store: &FsStore) -> Result<BTreeSet<String>> {
    let mut node_ids = BTreeSet::new();
    for branch in store
        .list_session_states()
        .context(StoreSnafu)?
        .into_keys()
        .collect::<Vec<_>>()
    {
        for node in store.ancestry(&branch).context(StoreSnafu)? {
            node_ids.insert(node.id);
        }
    }
    Ok(node_ids)
}

fn resolve_visible_parent(visible_node_ids: &BTreeSet<String>, start_id: &str) -> Option<String> {
    if start_id.is_empty() {
        return None;
    }

    if visible_node_ids.contains(start_id) {
        Some(start_id.to_owned())
    } else {
        None
    }
}

fn render_graph_entries(entries: &[GraphNodeEntry]) -> String {
    let mut output = String::new();
    let mut active_columns = Vec::<String>::new();

    for entry in entries {
        let current_col = active_columns
            .iter()
            .position(|node_id| node_id == &entry.node.id)
            .unwrap_or_else(|| {
                active_columns.push(entry.node.id.clone());
                active_columns.len() - 1
            });

        let prefix = render_graph_prefix(&active_columns, current_col);
        let summary = render_graph_summary(entry);
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str(&prefix);
        output.push(' ');
        output.push_str(&summary);

        let transition = build_graph_transition(&active_columns, current_col, entry);
        if let Some(connector_row) = transition.connector_row {
            output.push('\n');
            output.push_str(&connector_row);
        }
        active_columns = transition.next_columns;
    }

    output
}

fn render_graph_prefix(active_columns: &[String], current_col: usize) -> String {
    let mut parts = Vec::with_capacity(active_columns.len());
    for index in 0..active_columns.len() {
        if index == current_col {
            parts.push("*".to_owned());
        } else {
            parts.push("|".to_owned());
        }
    }
    parts.join(" ")
}

fn build_graph_transition(
    active_columns: &[String],
    current_col: usize,
    entry: &GraphNodeEntry,
) -> GraphTransition {
    let mut next = active_columns.to_vec();

    match &entry.primary_parent {
        Some(primary_parent) => {
            if let Some(existing_idx) = next.iter().position(|node_id| node_id == primary_parent) {
                if existing_idx != current_col {
                    next.remove(current_col);
                }
            } else {
                next[current_col] = primary_parent.clone();
            }
        }
        None => {
            next.remove(current_col);
        }
    }

    let mut insert_at = current_col.min(next.len());
    for merge_parent in &entry.merge_parents {
        if next.iter().any(|node_id| node_id == merge_parent) {
            continue;
        }
        next.insert(insert_at, merge_parent.clone());
        insert_at += 1;
    }

    let next_columns = dedupe_columns(next);
    let connector_row =
        render_graph_connector_row(active_columns, &next_columns, current_col, entry);

    GraphTransition {
        next_columns,
        connector_row,
    }
}

fn dedupe_columns(columns: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    columns
        .into_iter()
        .filter(|node_id| seen.insert(node_id.clone()))
        .collect()
}

fn render_graph_connector_row(
    active_columns: &[String],
    next_columns: &[String],
    current_col: usize,
    entry: &GraphNodeEntry,
) -> Option<String> {
    let primary_parent_col = entry.primary_parent.as_ref().and_then(|node_id| {
        next_columns
            .iter()
            .position(|candidate| candidate == node_id)
    });
    let merge_parent_cols = entry
        .merge_parents
        .iter()
        .filter_map(|node_id| {
            next_columns
                .iter()
                .position(|candidate| candidate == node_id)
        })
        .collect::<Vec<_>>();

    let should_render = !merge_parent_cols.is_empty()
        || primary_parent_col != Some(current_col)
        || active_columns.len() != next_columns.len();
    if !should_render {
        return None;
    }

    let width = active_columns.len().max(next_columns.len());
    if width == 0 {
        return None;
    }

    let mut chars = vec![' '; width * 2 - 1];
    for col in 0..width {
        if col == current_col {
            continue;
        }
        if active_columns.get(col).is_some() && next_columns.get(col).is_some() {
            chars[col * 2] = '|';
        }
    }

    let mut target_cols = Vec::new();
    if let Some(primary_parent_col) = primary_parent_col {
        target_cols.push(primary_parent_col);
    }
    target_cols.extend(merge_parent_cols);

    if target_cols.is_empty() {
        return None;
    }

    let current_pos = current_col * 2;
    chars[current_pos] = '|';

    for target_col in target_cols {
        let target_pos = target_col * 2;
        if target_pos == current_pos {
            chars[current_pos] = '|';
            continue;
        }

        let range = if target_pos < current_pos {
            (target_pos + 1)..current_pos
        } else {
            (current_pos + 1)..target_pos
        };
        for idx in range {
            if chars[idx] == ' ' {
                chars[idx] = '-';
            }
        }

        chars[target_pos] = if target_pos < current_pos { '/' } else { '\\' };
    }

    let connector_row = chars.into_iter().collect::<String>();
    Some(connector_row.trim_end().to_owned())
}

fn render_graph_summary(entry: &GraphNodeEntry) -> String {
    let kind = graph_kind_name(&entry.node);
    let short_id = shorten_id(&entry.node.id);
    let created_at = entry.node.created_at.to_string();
    let labels = render_graph_labels(&entry.labels);
    let summary = summarize_node(&entry.node);
    let merge_suffix = if entry.merge_parents.is_empty() {
        String::new()
    } else {
        format!(
            " merge=[{}]",
            entry
                .merge_parents
                .iter()
                .map(|node_id| shorten_id(node_id))
                .collect::<Vec<_>>()
                .join(",")
        )
    };

    if labels.is_empty() {
        format!("{short_id} {kind} {created_at} {summary}{merge_suffix}")
    } else {
        format!("{short_id} {kind} {created_at} [{labels}] {summary}{merge_suffix}")
    }
}

fn render_graph_labels(labels: &[GraphBranchLabel]) -> String {
    let mut by_branch = BTreeMap::new();
    for label in labels {
        by_branch.insert(label.branch.clone(), label);
    }

    by_branch
        .into_values()
        .map(|label| format!("{}{}", label.branch, format_state_suffix(&label.state)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_state_suffix(state: &SessionState) -> String {
    match state {
        SessionState::Active => String::new(),
        SessionState::Attached { target_branch, .. } => {
            format!("@Attached({target_branch})")
        }
        SessionState::Paused {
            target_branch,
            reason,
        } => match reason {
            PauseReason::Merged { .. } => {
                format!("@Paused({target_branch},merged)")
            }
            PauseReason::Closed => format!("@Paused({target_branch},closed)"),
        },
    }
}

fn graph_kind_name(node: &Node) -> &'static str {
    match &node.kind {
        Kind::Anchor(anchor) => match &anchor.payload {
            AnchorPayload::Session(_) => "session",
            AnchorPayload::Prompt(_) => "prompt",
        },
        Kind::ToolUse(_) => "tool_use",
        Kind::ToolResult(_) => "tool_result",
        Kind::Text(_) => "text",
        Kind::Failure(_) => "failure",
    }
}

fn summarize_node(node: &Node) -> String {
    let raw = match &node.kind {
        Kind::Anchor(anchor) => match &anchor.payload {
            AnchorPayload::Session(session) => {
                if session.prompt.trim().is_empty() {
                    session.system_prompt.clone()
                } else {
                    session.prompt.clone()
                }
            }
            AnchorPayload::Prompt(prompt) => prompt.prompt.clone(),
        },
        Kind::ToolUse(tool_use) => tool_use.input.to_string(),
        Kind::ToolResult(tool_result) => tool_result.output.clone(),
        Kind::Text(text) => text.clone(),
        Kind::Failure(message) => message.clone(),
    };

    truncate_summary(&raw)
}

fn render_node_show_text(result: &NodeShowResult) -> String {
    let mut lines = vec![
        format!("ref: {}", result.reference),
        format!("resolved_id: {}", result.resolved_id),
        format!("id: {}", result.node.id),
        format!("parent: {}", result.node.parent),
        format!("created_at: {}", result.node.created_at),
        format!("role: {:?}", result.node.role),
        format!("kind: {}", graph_kind_name(&result.node)),
    ];

    if let Some(execution_id) = result
        .node
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.execution_id.as_deref())
    {
        lines.push(format!("execution_id: {execution_id}"));
    }

    match &result.node.kind {
        Kind::Anchor(anchor) => {
            lines.push(format!(
                "merge_parents: {}",
                if anchor.merge_parents.is_empty() {
                    "[]".to_owned()
                } else {
                    format!("[{}]", anchor.merge_parents.join(", "))
                }
            ));
            match &anchor.payload {
                AnchorPayload::Session(session) => {
                    lines.extend([
                        format!("provider: {}", session.provider),
                        format!("model: {}", session.model),
                        format!(
                            "temperature: {}",
                            session
                                .temperature
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "null".to_owned())
                        ),
                        format!(
                            "max_tokens: {}",
                            session
                                .max_tokens
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "null".to_owned())
                        ),
                        format!(
                            "tools: {}",
                            if session.tools.is_empty() {
                                "[]".to_owned()
                            } else {
                                format!(
                                    "[{}]",
                                    session
                                        .tools
                                        .iter()
                                        .map(|tool| tool.name.as_str())
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                )
                            }
                        ),
                        "system_prompt:".to_owned(),
                        session.system_prompt.clone(),
                        "prompt:".to_owned(),
                        session.prompt.clone(),
                    ]);

                    if let Some(additional_params) = &session.additional_params {
                        lines.push("additional_params:".to_owned());
                        lines.push(
                            serde_json::to_string_pretty(additional_params)
                                .expect("additional params should serialize"),
                        );
                    }
                }
                AnchorPayload::Prompt(prompt) => {
                    lines.push("prompt:".to_owned());
                    lines.push(prompt.prompt.clone());
                }
            }
        }
        Kind::ToolUse(tool_use) => {
            lines.push(format!("tool_id: {}", tool_use.id));
            lines.push(format!("tool_name: {}", tool_use.name));
            lines.push("input:".to_owned());
            lines.push(
                serde_json::to_string_pretty(&tool_use.input).expect("tool input should serialize"),
            );
        }
        Kind::ToolResult(tool_result) => {
            lines.push(format!("tool_id: {}", tool_result.id));
            lines.push("output:".to_owned());
            lines.push(tool_result.output.clone());
        }
        Kind::Text(text) => {
            lines.push("text:".to_owned());
            lines.push(text.clone());
        }
        Kind::Failure(message) => {
            lines.push("message:".to_owned());
            lines.push(message.clone());
        }
    }

    lines.join("\n")
}

fn truncate_summary(text: &str) -> String {
    const MAX_CHARS: usize = 48;

    let trimmed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = trimmed.chars();
    let shortened = chars.by_ref().take(MAX_CHARS).collect::<String>();
    if chars.next().is_some() {
        format!("{shortened}...")
    } else {
        shortened
    }
}

fn shorten_id(node_id: &str) -> &str {
    &node_id[..node_id.len().min(8)]
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
        tools: if command.clear_tools {
            Some(vec![])
        } else if command.tools.is_empty() {
            None
        } else {
            Some(resolve_cli_tools(&command.tools))
        },
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

fn resolve_cli_tools(tools: &[crate::cli::CliTool]) -> Vec<Tool> {
    tools
        .iter()
        .copied()
        .map(crate::cli::CliTool::to_tool)
        .collect()
}

fn render_json<T>(value: T) -> String
where
    T: Serialize,
{
    serde_json::to_string_pretty(&value).expect("session output should serialize")
}
