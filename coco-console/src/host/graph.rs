use std::collections::{BTreeSet, HashMap};

use coco_mem::{
    Anchor, AnchorPayload, BranchStore, Kind, ManyOrOne, MergeParent, Node, NodeStore, PauseReason,
    SessionAnchor, SessionState, SessionStore, ToolResult, ToolUse,
};
use serde::Serialize;
use snafu::prelude::*;

use crate::Result;
use crate::error::StoreSnafu;

const SUMMARY_LIMIT: usize = 140;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphMode {
    Anchors,
    All,
}

impl GraphMode {
    pub fn as_query_value(self) -> &'static str {
        match self {
            Self::Anchors => "anchors",
            Self::All => "all",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Anchors => "Anchors",
            Self::All => "All",
        }
    }
}

#[derive(Debug, Serialize, PartialEq)]
pub struct GraphSnapshot {
    pub version: u64,
    pub mode: GraphMode,
    pub root_id: String,
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
    pub branches: Vec<GraphBranch>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct GraphNode {
    pub id: String,
    pub short_id: String,
    pub kind: String,
    pub role: String,
    pub created_at: String,
    pub created_at_ns: i128,
    pub content: String,
    pub summary: String,
    pub labels: Vec<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct GraphEdge {
    pub source: String,
    pub target: String,
    pub kind: GraphEdgeKind,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphEdgeKind {
    Primary,
    Merge,
    Shadow,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct GraphBranch {
    pub name: String,
    pub head_id: String,
    pub visible_head_id: Option<String>,
    pub state: SessionState,
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
    merge_parents: Vec<MergeParent>,
    labels: Vec<GraphBranchLabel>,
}

#[cfg(test)]
pub fn build_graph_snapshot(
    store: &(impl BranchStore + NodeStore + SessionStore),
    version: u64,
) -> Result<GraphSnapshot> {
    build_graph_snapshot_with_mode(store, version, GraphMode::All)
}

pub fn build_graph_snapshot_with_mode(
    store: &(impl BranchStore + NodeStore + SessionStore),
    version: u64,
    mode: GraphMode,
) -> Result<GraphSnapshot> {
    let states = store.list_session_states().context(StoreSnafu)?;
    let mut branches = states.into_iter().collect::<Vec<_>>();
    branches.sort_by(|(left, _), (right, _)| left.cmp(right));

    let mut visible_node_ids = BTreeSet::new();
    let mut visible_nodes = HashMap::new();
    let mut visible_node_scopes = HashMap::<String, BTreeSet<String>>::new();
    let mut labels_by_node = HashMap::<String, Vec<GraphBranchLabel>>::new();
    let mut graph_branches = Vec::with_capacity(branches.len());

    for (branch, state) in branches {
        let head_id = store.get_branch_head(&branch).context(StoreSnafu)?;
        let mut scope_node_ids = initial_graph_scope(store, &head_id, mode)?;
        let mut branch_visible_node_ids = BTreeSet::new();
        collect_visible_graph_nodes(
            store,
            &head_id,
            &mut scope_node_ids,
            mode,
            &mut visible_node_ids,
            &mut visible_nodes,
            &mut branch_visible_node_ids,
        )?;
        for node_id in branch_visible_node_ids {
            visible_node_scopes
                .entry(node_id)
                .or_default()
                .extend(scope_node_ids.iter().cloned());
        }

        let visible_head_id =
            resolve_visible_parent(store, &visible_node_ids, &scope_node_ids, &head_id)?;
        if let Some(label_node_id) = &visible_head_id {
            labels_by_node
                .entry(label_node_id.clone())
                .or_default()
                .push(GraphBranchLabel {
                    branch: branch.clone(),
                    state: state.clone(),
                });
        }
        graph_branches.push(GraphBranch {
            name: branch,
            head_id,
            visible_head_id,
            state,
        });
    }

    let mut entries = visible_nodes
        .into_values()
        .map(|node| {
            let scope_node_ids = visible_node_scopes.remove(&node.id).unwrap_or_default();
            let primary_parent = resolve_visible_parent(
                store,
                &visible_node_ids,
                &scope_node_ids,
                node.parent.as_str(),
            )?;
            let merge_parents = match &node.kind {
                Kind::Anchor(anchor) => {
                    let mut parents = Vec::new();
                    for merge_parent in anchor.merge_parents() {
                        let Some(parent_id) = resolve_visible_parent(
                            store,
                            &visible_node_ids,
                            &scope_node_ids,
                            merge_parent.node_id(),
                        )?
                        else {
                            continue;
                        };
                        if primary_parent.as_ref() == Some(&parent_id)
                            || parents
                                .iter()
                                .any(|existing: &MergeParent| existing.node_id() == parent_id)
                        {
                            continue;
                        }
                        parents.push(visible_merge_parent(merge_parent, parent_id));
                    }
                    parents
                }
                _ => Vec::new(),
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
        left.node
            .created_at
            .to_string()
            .cmp(&right.node.created_at.to_string())
            .then_with(|| left.node.id.cmp(&right.node.id))
    });

    let mut edges = Vec::new();
    let mut nodes = Vec::with_capacity(entries.len());
    for entry in entries {
        if let Some(parent) = &entry.primary_parent {
            edges.push(GraphEdge {
                source: parent.clone(),
                target: entry.node.id.clone(),
                kind: GraphEdgeKind::Primary,
            });
        }
        for parent in &entry.merge_parents {
            edges.push(GraphEdge {
                source: parent.node_id().to_owned(),
                target: entry.node.id.clone(),
                kind: if parent.is_shadow() {
                    GraphEdgeKind::Shadow
                } else {
                    GraphEdgeKind::Merge
                },
            });
        }
        nodes.push(GraphNode {
            id: entry.node.id.clone(),
            short_id: shorten_id(&entry.node.id),
            kind: graph_kind_name(&entry.node).to_owned(),
            role: format!("{:?}", entry.node.role),
            created_at: entry.node.created_at.to_string(),
            created_at_ns: entry.node.created_at.as_nanosecond(),
            content: render_node_content(&entry.node),
            summary: summarize_node(&entry.node),
            labels: render_graph_labels(&entry.labels),
        });
    }

    Ok(GraphSnapshot {
        version,
        mode,
        root_id: store.root_id(),
        nodes,
        edges,
        branches: graph_branches,
    })
}

fn collect_visible_graph_nodes(
    store: &impl NodeStore,
    head_id: &str,
    scope_node_ids: &mut BTreeSet<String>,
    mode: GraphMode,
    visible_node_ids: &mut BTreeSet<String>,
    visible_nodes: &mut HashMap<String, Node>,
    branch_visible_node_ids: &mut BTreeSet<String>,
) -> Result<()> {
    let mut pending = vec![head_id.to_owned()];
    let mut visited = BTreeSet::new();

    while let Some(node_id) = pending.pop() {
        if node_id.is_empty() || !visited.insert(node_id.clone()) {
            continue;
        }
        if !scope_node_ids.contains(&node_id) {
            continue;
        }

        let node = store.get_node(&node_id).context(StoreSnafu)?;
        if node.is_root() {
            continue;
        }

        if mode == GraphMode::All || !is_provider_context_start(&node) {
            push_scoped_graph_node(scope_node_ids, &mut pending, node.parent.clone());
        }
        if let Kind::Anchor(anchor) = &node.kind {
            for merge_parent in anchor.merge_parents() {
                push_scoped_graph_node(
                    scope_node_ids,
                    &mut pending,
                    merge_parent.node_id().to_owned(),
                );
            }
        }
        if node.kind.as_tool_uses().is_some() {
            collect_visible_skill_invocation_subtrees(
                store,
                &node.id,
                scope_node_ids,
                &mut pending,
            )?;
        }

        if is_visible_graph_node(&node, mode) {
            visible_node_ids.insert(node.id.clone());
            branch_visible_node_ids.insert(node.id.clone());
            visible_nodes.insert(node.id.clone(), node);
        }
    }

    Ok(())
}

fn initial_graph_scope(
    store: &impl NodeStore,
    head_id: &str,
    mode: GraphMode,
) -> Result<BTreeSet<String>> {
    match mode {
        GraphMode::Anchors => collect_provider_context_node_ids(store, head_id),
        GraphMode::All => Ok(BTreeSet::from([head_id.to_owned()])),
    }
}

fn push_scoped_graph_node(
    scope_node_ids: &mut BTreeSet<String>,
    pending: &mut Vec<String>,
    node_id: String,
) {
    if node_id.is_empty() {
        return;
    }

    scope_node_ids.insert(node_id.clone());
    pending.push(node_id);
}

fn collect_provider_context_node_ids(
    store: &impl NodeStore,
    head_id: &str,
) -> Result<BTreeSet<String>> {
    let mut node_ids = BTreeSet::new();

    for node in store.ancestry(head_id).context(StoreSnafu)? {
        if node.is_root() {
            break;
        }

        let is_context_start = is_provider_context_start(&node);
        node_ids.insert(node.id);
        if is_context_start {
            break;
        }
    }

    Ok(node_ids)
}

fn is_provider_context_start(node: &Node) -> bool {
    matches!(
        &node.kind,
        Kind::Anchor(anchor)
            if anchor.as_session().is_some_and(is_context_start_session)
    )
}

fn is_context_start_session(session: &SessionAnchor) -> bool {
    session
        .active_skill
        .as_ref()
        .is_none_or(|active_skill| active_skill.handoff.is_some())
}

fn is_visible_graph_node(node: &Node, mode: GraphMode) -> bool {
    match mode {
        GraphMode::Anchors => matches!(&node.kind, Kind::Anchor(_)),
        GraphMode::All => true,
    }
}

fn collect_visible_skill_invocation_subtrees(
    store: &impl NodeStore,
    parent_id: &str,
    scope_node_ids: &mut BTreeSet<String>,
    pending: &mut Vec<String>,
) -> Result<()> {
    let mut descendants = skill_invocation_children(store, parent_id)?;
    let mut visited = BTreeSet::new();

    while let Some(node_id) = next_unvisited_descendant(&mut descendants, &mut visited) {
        push_scoped_graph_node(scope_node_ids, pending, node_id.clone());
        descendants.extend(child_ids(store, &node_id)?);
    }

    Ok(())
}

fn skill_invocation_children(store: &impl NodeStore, parent_id: &str) -> Result<Vec<String>> {
    Ok(store
        .list_children(parent_id)
        .context(StoreSnafu)?
        .into_iter()
        .filter_map(skill_invocation_child_id)
        .collect())
}

fn skill_invocation_child_id(child: Node) -> Option<String> {
    match child.kind {
        Kind::Anchor(anchor) if anchor.as_skill_invocation().is_some() => Some(child.id),
        _ => None,
    }
}

fn next_unvisited_descendant(
    descendants: &mut Vec<String>,
    visited: &mut BTreeSet<String>,
) -> Option<String> {
    while let Some(node_id) = descendants.pop() {
        if !node_id.is_empty() && visited.insert(node_id.clone()) {
            return Some(node_id);
        }
    }
    None
}

fn child_ids(store: &impl NodeStore, node_id: &str) -> Result<Vec<String>> {
    Ok(store
        .list_children(node_id)
        .context(StoreSnafu)?
        .into_iter()
        .map(|child| child.id)
        .collect())
}

fn resolve_visible_parent(
    store: &impl NodeStore,
    visible_node_ids: &BTreeSet<String>,
    scope_node_ids: &BTreeSet<String>,
    start_id: &str,
) -> Result<Option<String>> {
    if start_id.is_empty() {
        return Ok(None);
    }

    let mut current_id = start_id.to_owned();
    loop {
        if !scope_node_ids.contains(&current_id) {
            return Ok(None);
        }
        if visible_node_ids.contains(&current_id) {
            return Ok(Some(current_id));
        }

        let node = store.get_node(&current_id).context(StoreSnafu)?;
        if node.is_root() {
            return Ok(None);
        }
        current_id = node.parent;
    }
}

fn visible_merge_parent(parent: &MergeParent, node_id: String) -> MergeParent {
    if parent.is_shadow() {
        MergeParent::shadow(node_id)
    } else {
        MergeParent::merge(node_id)
    }
}

fn graph_kind_name(node: &Node) -> &'static str {
    match &node.kind {
        Kind::Anchor(anchor) => match &anchor.payload {
            AnchorPayload::Session(_) => "session",
            AnchorPayload::SessionPatch(_) => "session_patch",
            AnchorPayload::Prompt(_) => "prompt",
            AnchorPayload::SkillInvocation(_) => "skill_invocation",
            AnchorPayload::SkillResult(_) => "skill_result",
        },
        Kind::ToolUse(_) => "tool_use",
        Kind::ToolResult(_) => "tool_result",
        Kind::Text(_) => "text",
        Kind::Failure(_) => "failure",
    }
}

fn summarize_node(node: &Node) -> String {
    truncate_summary(&render_node_content(node))
}

fn render_node_content(node: &Node) -> String {
    match &node.kind {
        Kind::Anchor(anchor) => render_anchor_content(anchor),
        Kind::ToolUse(tool_uses) => render_tool_use_content(tool_uses),
        Kind::ToolResult(tool_results) => render_tool_result_content(tool_results),
        Kind::Text(text) => text.clone(),
        Kind::Failure(message) => message.clone(),
    }
}

fn render_anchor_content(anchor: &Anchor) -> String {
    match &anchor.payload {
        AnchorPayload::Session(session) => render_session_content(session),
        AnchorPayload::SessionPatch(patch) => {
            serde_json::to_string(patch).expect("session patch should serialize")
        }
        AnchorPayload::Prompt(prompt) => prompt.prompt.clone(),
        AnchorPayload::SkillInvocation(invocation) => invocation.skill_name.clone(),
        AnchorPayload::SkillResult(skill_result) => skill_result.output.clone(),
    }
}

fn render_session_content(session: &SessionAnchor) -> String {
    if session.prompt.trim().is_empty() {
        session.system_prompt.clone()
    } else {
        session.prompt.clone()
    }
}

fn render_tool_use_content(tool_uses: &ManyOrOne<ToolUse>) -> String {
    tool_uses
        .first()
        .map(|tool_use| tool_use.input.to_string())
        .unwrap_or_default()
}

fn render_tool_result_content(tool_results: &ManyOrOne<ToolResult>) -> String {
    tool_results
        .first()
        .map(|tool_result| tool_result.output.clone())
        .unwrap_or_default()
}

fn truncate_summary(raw: &str) -> String {
    let collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = collapsed.chars();
    let truncated = chars.by_ref().take(SUMMARY_LIMIT).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

pub fn shorten_id(id: &str) -> String {
    id.chars().take(8).collect()
}

pub fn css_token(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

pub fn node_target_id(node_id: &str) -> String {
    format!("detail-{}", css_token(node_id))
}

fn render_graph_labels(labels: &[GraphBranchLabel]) -> Vec<String> {
    let mut labels = labels
        .iter()
        .map(|label| format!("{}{}", label.branch, format_state_suffix(&label.state)))
        .collect::<Vec<_>>();
    labels.sort();
    labels
}

fn format_state_suffix(state: &SessionState) -> String {
    match state {
        SessionState::Active => String::new(),
        SessionState::Attached { target_branch, .. } => format!("@Attached({target_branch})"),
        SessionState::Paused {
            target_branch,
            reason,
        } => match reason {
            PauseReason::Merged { .. } => format!("@Paused({target_branch},merged)"),
            PauseReason::Closed => format!("@Paused({target_branch},closed)"),
        },
    }
}
