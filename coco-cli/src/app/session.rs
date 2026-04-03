use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap};
use std::sync::Arc;

use coco_llm::{
    CompletionBackend, LlmService, SessionConfig, SessionConfigPatch, SessionFeedback, SessionMerge,
};
use coco_mem::{
    AnchorPayload, FsStore, Kind, Node, PauseReason, SessionAnchor, SessionState, Store,
    StoreError, Tool,
};
use serde::Serialize;
use serde_json::Value;
use snafu::prelude::*;

use crate::{
    Result,
    cli::{CliTool, SessionCommand, SessionCreateCommand, SessionRebaseCommand, SessionSubcommand},
    env::{read_env, resolve_env_provider, resolve_env_tools},
    error::{
        AmbiguousNodePrefixSnafu, LlmSnafu, MissingConfigurationSnafu,
        ParseSessionAdditionalParamsSnafu, StoreSnafu, UnknownShowReferenceSnafu,
    },
};

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
struct SessionDeleteResult {
    branch: String,
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

#[derive(Debug, Clone, Eq, PartialEq)]
struct ReadyGraphEntry {
    created_at: String,
    node_id: String,
}

impl Ord for ReadyGraphEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.created_at
            .cmp(&other.created_at)
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

impl PartialOrd for ReadyGraphEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Serialize, PartialEq)]
struct NodeShowResult {
    #[serde(rename = "ref")]
    reference: String,
    resolved_id: String,
    children: Vec<String>,
    node: Node,
}

pub(super) async fn run_session_command<B>(
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
        SessionSubcommand::Delete(command) => {
            store.delete_branch(&command.branch).context(StoreSnafu)?;
            Ok(Some(render_json(SessionDeleteResult {
                branch: command.branch,
            })))
        }
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

pub fn resolve_session_config(command: SessionCreateCommand) -> Result<SessionConfig> {
    let provider = resolve_env_provider()?;
    let model = read_env("COCO_MODEL").context(MissingConfigurationSnafu { name: "COCO_MODEL" })?;
    let tools = if command.tools.is_empty() {
        resolve_cli_tools(&resolve_env_tools()?)
    } else {
        resolve_cli_tools(&command.tools)
    };
    let additional_params = parse_session_additional_params(command.additional_params)?;

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
        additional_params,
    })
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
        collect_visible_graph_nodes(store, &head_id, &mut visible_node_ids, &mut visible_nodes)?;

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

    entries = topologically_sort_graph_entries(entries);

    Ok(render_graph_entries(&entries))
}

fn topologically_sort_graph_entries(entries: Vec<GraphNodeEntry>) -> Vec<GraphNodeEntry> {
    fn ready_graph_entry(entry: &GraphNodeEntry) -> ReadyGraphEntry {
        ReadyGraphEntry {
            created_at: entry.node.created_at.to_string(),
            node_id: entry.node.id.clone(),
        }
    }

    let mut pending_children = HashMap::<String, usize>::new();
    let mut entries_by_id = HashMap::<String, GraphNodeEntry>::new();

    for entry in entries {
        pending_children.insert(entry.node.id.clone(), 0);
        entries_by_id.insert(entry.node.id.clone(), entry);
    }

    for entry in entries_by_id.values() {
        if let Some(primary_parent) = &entry.primary_parent
            && let Some(count) = pending_children.get_mut(primary_parent)
        {
            *count += 1;
        }
        for merge_parent in &entry.merge_parents {
            if let Some(count) = pending_children.get_mut(merge_parent) {
                *count += 1;
            }
        }
    }

    let mut ready = pending_children
        .iter()
        .filter(|(_, count)| **count == 0)
        .map(|(node_id, _)| {
            ready_graph_entry(
                entries_by_id
                    .get(node_id)
                    .expect("ready node should exist in graph entries"),
            )
        })
        .collect::<BinaryHeap<_>>();
    let mut ordered = Vec::with_capacity(entries_by_id.len());

    while let Some(ready_entry) = ready.pop() {
        let node_id = ready_entry.node_id;
        let entry = entries_by_id
            .remove(&node_id)
            .expect("ready node should still exist in graph entries");

        let mut parents = Vec::with_capacity(1 + entry.merge_parents.len());
        if let Some(primary_parent) = &entry.primary_parent {
            parents.push(primary_parent.clone());
        }
        parents.extend(entry.merge_parents.iter().cloned());

        for parent_id in parents {
            let Some(count) = pending_children.get_mut(&parent_id) else {
                continue;
            };
            *count -= 1;
            if *count == 0 {
                ready.push(ready_graph_entry(
                    entries_by_id
                        .get(&parent_id)
                        .expect("newly ready node should exist in graph entries"),
                ));
            }
        }

        ordered.push(entry);
    }

    if !entries_by_id.is_empty() {
        let mut remaining = entries_by_id.into_values().collect::<Vec<_>>();
        remaining.sort_by(compare_graph_entries_desc);
        ordered.extend(remaining);
    }

    ordered
}

fn compare_graph_entries_desc(left: &GraphNodeEntry, right: &GraphNodeEntry) -> Ordering {
    let left_ts = left.node.created_at.to_string();
    let right_ts = right.node.created_at.to_string();
    right_ts
        .cmp(&left_ts)
        .then_with(|| right.node.id.cmp(&left.node.id))
}

fn collect_visible_graph_nodes(
    store: &FsStore,
    head_id: &str,
    visible_node_ids: &mut BTreeSet<String>,
    visible_nodes: &mut HashMap<String, Node>,
) -> Result<()> {
    let mut pending = vec![head_id.to_owned()];
    let mut visited = BTreeSet::new();

    while let Some(node_id) = pending.pop() {
        if node_id.is_empty() || !visited.insert(node_id.clone()) {
            continue;
        }

        let node = store.get_node(&node_id).context(StoreSnafu)?;
        if node.is_root() {
            continue;
        }

        pending.push(node.parent.clone());
        if let Kind::Anchor(anchor) = &node.kind {
            pending.extend(anchor.merge_parents().iter().cloned());
        }

        visible_node_ids.insert(node.id.clone());
        visible_nodes.insert(node.id.clone(), node);
    }

    Ok(())
}

fn render_session_show(store: &FsStore, reference: &str, json_output: bool) -> Result<String> {
    let node = resolve_show_reference(store, reference)?;
    let children = store
        .list_children(&node.id)
        .context(StoreSnafu)?
        .into_iter()
        .map(|node| node.id)
        .collect();
    let result = NodeShowResult {
        reference: reference.to_owned(),
        resolved_id: node.id.clone(),
        children,
        node,
    };

    if json_output {
        Ok(render_json(result))
    } else {
        Ok(render_node_show_text(&result))
    }
}

fn resolve_show_reference(store: &FsStore, reference: &str) -> Result<Node> {
    match store.get_node(reference) {
        Ok(node) => Ok(node),
        Err(StoreError::NotFound { .. }) => UnknownShowReferenceSnafu {
            reference: reference.to_owned(),
        }
        .fail(),
        Err(StoreError::AmbiguousNodePrefix { prefix, matches }) => {
            AmbiguousNodePrefixSnafu { prefix, matches }.fail()
        }
        Err(source) => Err(crate::Error::Store { source }),
    }
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
    if target_cols.contains(&current_col) {
        chars[current_pos] = '|';
    }

    for target_col in target_cols {
        let target_pos = target_col * 2;
        if target_pos == current_pos {
            chars[current_pos] = '|';
            continue;
        }

        let connector_pos = if target_pos < current_pos {
            current_pos - 1
        } else {
            current_pos + 1
        };
        chars[connector_pos] = if target_pos < current_pos { '/' } else { '\\' };

        let range = if target_pos < current_pos {
            (target_pos + 1)..connector_pos
        } else {
            (connector_pos + 1)..target_pos
        };
        for idx in range {
            if chars[idx] == ' ' {
                chars[idx] = '-';
            }
        }
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
        format!(
            "children: {}",
            if result.children.is_empty() {
                "[]".to_owned()
            } else {
                format!("[{}]", result.children.join(", "))
            }
        ),
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

fn resolve_session_patch(command: SessionRebaseCommand) -> SessionConfigPatch {
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

fn resolve_cli_tools(tools: &[CliTool]) -> Vec<Tool> {
    tools.iter().copied().map(cli_tool_definition).collect()
}

fn cli_tool_definition(tool: CliTool) -> Tool {
    match tool {
        CliTool::Bash => Tool {
            name: "bash".to_owned(),
            description: "Run a bash command.".to_owned(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The bash command to execute."
                    },
                    "workdir": {
                        "type": "string",
                        "description": "Optional working directory."
                    },
                    "timeout_ms": {
                        "type": "integer",
                        "description": "Optional timeout in milliseconds."
                    }
                },
                "required": ["command"],
                "additionalProperties": false
            }),
        },
    }
}

fn parse_session_additional_params(additional_params: Option<String>) -> Result<Option<Value>> {
    let Some(additional_params) = additional_params else {
        return Ok(None);
    };

    let value: Value =
        serde_json::from_str(&additional_params).context(ParseSessionAdditionalParamsSnafu)?;
    ensure!(
        value.is_object(),
        crate::error::InvalidSessionAdditionalParamsTypeSnafu { value }
    );
    Ok(Some(value))
}

fn render_json<T>(value: T) -> String
where
    T: Serialize,
{
    serde_json::to_string_pretty(&value).expect("session output should serialize")
}

#[cfg(test)]
mod tests {
    use super::{GraphNodeEntry, render_graph_connector_row};
    use coco_mem::{Kind, Role};
    use serde_json::json;

    fn graph_entry(node_id: &str, primary_parent: Option<&str>) -> GraphNodeEntry {
        GraphNodeEntry {
            node: serde_json::from_value(json!({
                "id": node_id,
                "parent": primary_parent.unwrap_or_default(),
                "created_at": "1970-01-01T00:00:00Z",
                "role": Role::User,
                "metadata": null,
                "kind": Kind::Text("graph".to_owned()),
            }))
            .expect("graph test node should deserialize"),
            primary_parent: primary_parent.map(str::to_owned),
            merge_parents: Vec::new(),
            labels: Vec::new(),
        }
    }

    #[test]
    fn graph_connector_row_places_left_diagonal_between_columns() {
        let active_columns = vec!["left".to_owned(), "current".to_owned()];
        let next_columns = vec!["left".to_owned()];
        let entry = graph_entry("current", Some("left"));

        let connector_row = render_graph_connector_row(&active_columns, &next_columns, 1, &entry);

        assert_eq!(connector_row.as_deref(), Some("|/"));
    }
}
