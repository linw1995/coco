use std::collections::{BTreeSet, HashMap};

use coco_mem::{
    AnchorPayload, BranchStore, Job, JobStatus, JobStore, Kind, MergeParent, MessageQueueItem,
    MessageQueueStore, Node, NodeStore, PauseReason, PresetRecord, PresetStore, SessionRole,
    SessionState, SessionStore, SkillRecord, SkillStore,
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
    pub sessions: Vec<GraphSession>,
    pub presets: Vec<GraphPreset>,
    pub skills: Vec<GraphSkill>,
    pub jobs: Vec<GraphJob>,
    pub queues: Vec<GraphQueue>,
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

#[derive(Debug, Serialize, PartialEq)]
pub struct GraphSession {
    pub branch: String,
    pub head_id: String,
    pub state: String,
    pub target_branch: Option<String>,
    pub base_head_id: Option<String>,
    pub pause_reason: Option<String>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct GraphPreset {
    pub name: String,
    pub current_version: u64,
    pub version_count: usize,
    pub role: String,
    pub provider_profile: String,
    pub model: String,
    pub tool_count: usize,
    pub prompt: String,
    pub system_prompt: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct GraphSkill {
    pub role: String,
    pub name: String,
    pub current_version: u64,
    pub version_count: usize,
    pub revision_id: String,
    pub description: String,
    pub script_count: usize,
    pub enable_coco_shim: bool,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct GraphJob {
    pub job_id: String,
    pub created_at: String,
    pub finished_at: Option<String>,
    pub branch: String,
    pub base: String,
    pub status: String,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct GraphQueue {
    pub name: String,
    pub message_count: usize,
    pub messages: Vec<GraphQueueMessage>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct GraphQueueMessage {
    pub message_id: String,
    pub created_at: String,
    pub payload: String,
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
    store: &(
         impl BranchStore
         + JobStore
         + MessageQueueStore
         + NodeStore
         + PresetStore
         + SessionStore
         + SkillStore
     ),
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

    let sessions = build_sessions(&graph_branches);
    let presets = build_presets(store)?;
    let skills = build_skills(store)?;
    let jobs = build_jobs(store)?;
    let queues = build_queues(store)?;

    Ok(GraphSnapshot {
        version,
        root_id: store.root_id(),
        nodes,
        edges,
        branches: graph_branches,
        sessions,
        presets,
        skills,
        jobs,
        queues,
    })
}

fn build_sessions(branches: &[GraphBranch]) -> Vec<GraphSession> {
    branches
        .iter()
        .map(|branch| {
            let (state, target_branch, base_head_id, pause_reason) = match &branch.state {
                SessionState::Active => ("Active".to_owned(), None, None, None),
                SessionState::Attached {
                    target_branch,
                    base_head_id,
                } => (
                    "Attached".to_owned(),
                    Some(target_branch.clone()),
                    Some(base_head_id.clone()),
                    None,
                ),
                SessionState::Paused {
                    target_branch,
                    reason,
                } => (
                    "Paused".to_owned(),
                    Some(target_branch.clone()),
                    None,
                    Some(format_pause_reason(reason)),
                ),
            };

            GraphSession {
                branch: branch.name.clone(),
                head_id: branch.head_id.clone(),
                state,
                target_branch,
                base_head_id,
                pause_reason,
            }
        })
        .collect()
}

fn build_presets(store: &impl PresetStore) -> Result<Vec<GraphPreset>> {
    let mut records = store
        .list_preset_records()
        .context(StoreSnafu)?
        .into_values()
        .collect::<Vec<_>>();
    records.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(records.iter().map(render_preset).collect())
}

fn render_preset(record: &PresetRecord) -> GraphPreset {
    let current = record.versions.get(&record.current_version);

    GraphPreset {
        name: record.name.clone(),
        current_version: record.current_version,
        version_count: record.versions.len(),
        role: current
            .map(|version| format_session_role(version.config.role))
            .unwrap_or_default(),
        provider_profile: current
            .map(|version| version.config.provider_profile.clone())
            .unwrap_or_default(),
        model: current
            .map(|version| version.config.model.clone())
            .unwrap_or_default(),
        tool_count: current
            .map(|version| version.config.tools.len())
            .unwrap_or_default(),
        prompt: current
            .map(|version| truncate_summary(&version.config.prompt))
            .unwrap_or_default(),
        system_prompt: current
            .map(|version| truncate_summary(&version.config.system_prompt))
            .unwrap_or_default(),
    }
}

fn build_skills(store: &impl SkillStore) -> Result<Vec<GraphSkill>> {
    let mut skills = Vec::new();
    for role in [SessionRole::Orchestrator, SessionRole::Runner] {
        skills.extend(
            store
                .list_skills(role)
                .context(StoreSnafu)?
                .iter()
                .map(|record| render_skill(role, record)),
        );
    }
    skills.sort_by(|left, right| {
        left.role
            .cmp(&right.role)
            .then_with(|| left.name.cmp(&right.name))
    });
    Ok(skills)
}

fn render_skill(role: SessionRole, record: &SkillRecord) -> GraphSkill {
    let current = record.current();

    GraphSkill {
        role: format_session_role(role),
        name: record.name.clone(),
        current_version: record.current_version,
        version_count: record.versions.len(),
        revision_id: current
            .map(|version| version.id.clone())
            .unwrap_or_default(),
        description: current
            .map(|version| truncate_summary(&version.description))
            .unwrap_or_default(),
        script_count: current
            .map(|version| version.scripts.len())
            .unwrap_or_default(),
        enable_coco_shim: current
            .map(|version| version.enable_coco_shim)
            .unwrap_or_default(),
    }
}

fn build_jobs(store: &impl JobStore) -> Result<Vec<GraphJob>> {
    let mut jobs = store
        .list_jobs()
        .context(StoreSnafu)?
        .into_values()
        .map(render_job)
        .collect::<Vec<_>>();
    jobs.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| left.job_id.cmp(&right.job_id))
    });
    Ok(jobs)
}

fn render_job(job: Job) -> GraphJob {
    GraphJob {
        job_id: job.job_id,
        created_at: job.created_at.to_string(),
        finished_at: job.finished_at.map(|finished_at| finished_at.to_string()),
        branch: job.branch,
        base: job.base,
        status: format_job_status(job.status),
    }
}

fn build_queues(store: &impl MessageQueueStore) -> Result<Vec<GraphQueue>> {
    let mut queues = store
        .list_message_queues()
        .context(StoreSnafu)?
        .into_iter()
        .map(|(name, messages)| render_queue(name, messages))
        .collect::<Vec<_>>();
    queues.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(queues)
}

fn render_queue(name: String, messages: Vec<MessageQueueItem>) -> GraphQueue {
    GraphQueue {
        name,
        message_count: messages.len(),
        messages: messages.into_iter().map(render_queue_message).collect(),
    }
}

fn render_queue_message(message: MessageQueueItem) -> GraphQueueMessage {
    GraphQueueMessage {
        message_id: message.message_id,
        created_at: message.created_at.to_string(),
        payload: serde_json::to_string_pretty(&message.payload)
            .unwrap_or_else(|_| message.payload.to_string()),
    }
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
    let mut descendants = store
        .list_children(parent_id)
        .context(StoreSnafu)?
        .into_iter()
        .filter_map(|child| match child.kind {
            Kind::Anchor(anchor) if anchor.as_skill_invocation().is_some() => Some(child.id),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut visited = BTreeSet::new();

    while let Some(node_id) = descendants.pop() {
        if node_id.is_empty() || !visited.insert(node_id.clone()) {
            continue;
        }

        pending.push(node_id.clone());
        for child in store.list_children(&node_id).context(StoreSnafu)? {
            descendants.push(child.id);
        }
    }

    Ok(())
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

fn format_pause_reason(reason: &PauseReason) -> String {
    match reason {
        PauseReason::Merged { merged_anchor_id } => {
            format!("Merged at {}", shorten_id(merged_anchor_id))
        }
        PauseReason::Closed => "Closed".to_owned(),
    }
}

fn format_session_role(role: SessionRole) -> String {
    match role {
        SessionRole::Orchestrator => "Orchestrator".to_owned(),
        SessionRole::Runner => "Runner".to_owned(),
    }
}

fn format_job_status(status: JobStatus) -> String {
    match status {
        JobStatus::Queued => "Queued".to_owned(),
        JobStatus::Running => "Running".to_owned(),
        JobStatus::Finished => "Finished".to_owned(),
    }
}
