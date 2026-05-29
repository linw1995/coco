use std::collections::{BTreeSet, HashMap};

use coco_mem::{
    AnchorPayload, BranchStore, Kind, MergeParent, Node, NodeStore, PauseReason, SessionState,
    SessionStore,
};
use serde::Serialize;
use snafu::prelude::*;

use crate::Result;
use crate::error::StoreSnafu;

const SUMMARY_LIMIT: usize = 140;
#[derive(Debug, Serialize, PartialEq)]
pub struct GraphSnapshot {
    pub version: u64,
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
    PrimaryParent,
    MergeParent,
    ShadowParent,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct GraphBranch {
    pub name: String,
    pub head_id: String,
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

pub fn build_graph_snapshot(
    store: &(impl BranchStore + NodeStore + SessionStore),
    version: u64,
) -> Result<GraphSnapshot> {
    let states = store.list_session_states().context(StoreSnafu)?;
    let mut branches = states.into_iter().collect::<Vec<_>>();
    branches.sort_by(|(left, _), (right, _)| left.cmp(right));

    let mut visible_node_ids = BTreeSet::new();
    let mut visible_nodes = HashMap::new();
    let mut labels_by_node = HashMap::<String, Vec<GraphBranchLabel>>::new();
    let mut graph_branches = Vec::with_capacity(branches.len());

    for (branch, state) in branches {
        let head_id = store.get_branch_head(&branch).context(StoreSnafu)?;
        collect_visible_graph_nodes(store, &head_id, &mut visible_node_ids, &mut visible_nodes)?;
        labels_by_node
            .entry(head_id.clone())
            .or_default()
            .push(GraphBranchLabel {
                branch: branch.clone(),
                state: state.clone(),
            });
        graph_branches.push(GraphBranch {
            name: branch,
            head_id,
            state,
        });
    }

    let mut entries = visible_nodes
        .into_values()
        .map(|node| {
            let primary_parent = resolve_visible_parent(&visible_node_ids, node.parent.as_str());
            let merge_parents = match &node.kind {
                Kind::Anchor(anchor) => anchor
                    .merge_parents()
                    .iter()
                    .filter_map(|merge_parent| {
                        resolve_visible_parent(&visible_node_ids, merge_parent.node_id())
                            .map(|parent_id| visible_merge_parent(merge_parent, parent_id))
                    })
                    .filter(|parent| primary_parent.as_deref() != Some(parent.node_id()))
                    .fold(Vec::<MergeParent>::new(), |mut parents, parent| {
                        if !parents
                            .iter()
                            .any(|existing| existing.node_id() == parent.node_id())
                        {
                            parents.push(parent);
                        }
                        parents
                    }),
                _ => Vec::new(),
            };

            GraphNodeEntry {
                labels: labels_by_node.remove(&node.id).unwrap_or_default(),
                node,
                primary_parent,
                merge_parents,
            }
        })
        .collect::<Vec<_>>();
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
                kind: GraphEdgeKind::PrimaryParent,
            });
        }
        for parent in &entry.merge_parents {
            edges.push(GraphEdge {
                source: parent.node_id().to_owned(),
                target: entry.node.id.clone(),
                kind: if parent.is_shadow() {
                    GraphEdgeKind::ShadowParent
                } else {
                    GraphEdgeKind::MergeParent
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
        root_id: store.root_id(),
        nodes,
        edges,
        branches: graph_branches,
    })
}

fn collect_visible_graph_nodes(
    store: &impl NodeStore,
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
            pending.extend(
                anchor
                    .merge_parents()
                    .iter()
                    .map(|parent| parent.node_id().to_owned()),
            );
        }
        if node.kind.as_tool_uses().is_some() {
            collect_visible_skill_invocation_subtrees(store, &node.id, &mut pending)?;
        }

        visible_node_ids.insert(node.id.clone());
        visible_nodes.insert(node.id.clone(), node);
    }

    Ok(())
}

fn collect_visible_skill_invocation_subtrees(
    store: &impl NodeStore,
    parent_id: &str,
    pending: &mut Vec<String>,
) -> Result<()> {
    let mut descendants = skill_invocation_children(store, parent_id)?;
    let mut visited = BTreeSet::new();

    while let Some(node_id) = next_unvisited_descendant(&mut descendants, &mut visited) {
        pending.push(node_id.clone());
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

fn resolve_visible_parent(visible_node_ids: &BTreeSet<String>, start_id: &str) -> Option<String> {
    if start_id.is_empty() {
        return None;
    }

    visible_node_ids
        .contains(start_id)
        .then(|| start_id.to_owned())
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
        Kind::Anchor(anchor) => match &anchor.payload {
            AnchorPayload::Session(session) => {
                if session.prompt.trim().is_empty() {
                    session.system_prompt.clone()
                } else {
                    session.prompt.clone()
                }
            }
            AnchorPayload::SessionPatch(patch) => {
                serde_json::to_string(patch).expect("session patch should serialize")
            }
            AnchorPayload::Prompt(prompt) => prompt.prompt.clone(),
            AnchorPayload::SkillInvocation(invocation) => invocation.skill_name.clone(),
            AnchorPayload::SkillResult(skill_result) => skill_result.output.clone(),
        },
        Kind::ToolUse(tool_uses) => tool_uses
            .first()
            .map(|tool_use| tool_use.input.to_string())
            .unwrap_or_default(),
        Kind::ToolResult(tool_results) => tool_results
            .first()
            .map(|tool_result| tool_result.output.clone())
            .unwrap_or_default(),
        Kind::Text(text) => text.clone(),
        Kind::Failure(message) => message.clone(),
    }
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
