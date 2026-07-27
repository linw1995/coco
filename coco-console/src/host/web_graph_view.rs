use std::collections::{BTreeMap, BTreeSet, HashMap};

use coco_mem::{
    Anchor, AnchorPayload, BranchStore, Kind, Node, NodeStore, SessionAnchor, SessionStore,
    ToolResult, ToolUse,
};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use crate::api::{
    GraphViewportDiffResponse, GraphViewportEdge, GraphViewportEdgeKind, GraphViewportItemKind,
    GraphViewportItems, GraphViewportNode, GraphViewportRemovedItem, GraphViewportResponse,
    ToolUseInputLink,
};
use crate::host::api::GraphViewportKnownItems;
use crate::web_graph::{BezierRoute, Point};

use super::error::StoreSnafu;

pub const GRAPH_PADDING: i32 = 56;
pub const GRAPH_RANK_STEP: i32 = 112;
pub const GRAPH_ROW_STEP: i32 = 72;
pub const GRAPH_NODE_RADIUS: i32 = 18;

const SUMMARY_LIMIT: usize = 140;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ViewMode {
    Anchors,
    All,
}

impl ViewMode {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeView {
    pub id: String,
    pub short_id: String,
    pub kind: String,
    pub role: String,
    pub created_at: String,
    pub content: String,
    pub summary: String,
}

impl From<&Node> for NodeView {
    fn from(node: &Node) -> Self {
        Self {
            id: node.id.clone(),
            short_id: shorten_id(&node.id),
            kind: graph_kind_name(node).to_owned(),
            role: format!("{:?}", node.role),
            created_at: node.created_at.to_string(),
            content: render_node_content(node),
            summary: summarize_node(node),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderContext {
    pub id: String,
    pub nodes: Vec<ProviderContextNode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderContextNode {
    pub node: NodeView,
    pub created_at_ns: i128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderContextSelection {
    pub context: ProviderContext,
    pub selected_id: String,
}

pub async fn provider_context_for_node(
    store: &(impl BranchStore + NodeStore + SessionStore),
    target_node_id: &str,
    context_id: Option<&str>,
) -> crate::Result<Option<ProviderContextSelection>> {
    let mut branches = store
        .list_session_states()
        .await
        .context(StoreSnafu)?
        .into_keys()
        .collect::<Vec<_>>();
    branches.sort();

    let mut contexts = HashMap::<String, ProviderContext>::new();
    for branch in branches {
        let head_id = store.get_branch_head(&branch).await.context(StoreSnafu)?;
        let ancestry = store.ancestry(&head_id).await.context(StoreSnafu)?;
        for nodes in provider_contexts_from_head(ancestry) {
            let Some(id) = provider_context_id(&branch, &nodes) else {
                continue;
            };
            if context_id.is_some_and(|selected| selected != id)
                || !nodes.iter().any(|node| node.id == target_node_id)
            {
                continue;
            }
            contexts
                .entry(id.clone())
                .or_insert_with(|| ProviderContext {
                    id,
                    nodes: nodes
                        .iter()
                        .map(|node| ProviderContextNode {
                            node: NodeView::from(node),
                            created_at_ns: node.created_at.as_nanosecond(),
                        })
                        .collect(),
                });
        }
    }

    let mut contexts = contexts.into_values().collect::<Vec<_>>();
    contexts.sort_by(|left, right| {
        context_head_time_ns(left)
            .cmp(&context_head_time_ns(right))
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(contexts.into_iter().find_map(|context| {
        let selected_id = context
            .nodes
            .iter()
            .find(|node| node.node.id == target_node_id)?
            .node
            .id
            .clone();
        Some(ProviderContextSelection {
            context,
            selected_id,
        })
    }))
}

fn provider_contexts_from_head(ancestry: Vec<Node>) -> Vec<Vec<Node>> {
    let mut contexts = Vec::new();
    let mut current = Vec::new();
    let mut previous_is_skill_invocation = false;

    for node in ancestry.into_iter().take_while(|node| !node.is_root()) {
        if node.kind.as_tool_uses().is_some() && previous_is_skill_invocation {
            continue;
        }
        let is_start = is_provider_context_start(&node);
        previous_is_skill_invocation = matches!(
            &node.kind,
            Kind::Anchor(anchor) if anchor.as_skill_invocation().is_some()
        );
        current.push(node);
        if is_start {
            contexts.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        contexts.push(current);
    }
    contexts
}

fn is_provider_context_start(node: &Node) -> bool {
    matches!(
        &node.kind,
        Kind::Anchor(anchor)
            if anchor.as_session().is_some_and(|session| {
                session
                    .active_skill
                    .as_ref()
                    .is_none_or(|active_skill| active_skill.handoff.is_some())
            })
    )
}

fn provider_context_id(branch: &str, context: &[Node]) -> Option<String> {
    Some(format!(
        "{}-context-{}",
        node_target_id(&context.last()?.id),
        stable_token(branch)
    ))
}

fn context_head_time_ns(context: &ProviderContext) -> i128 {
    context
        .nodes
        .first()
        .map(|node| node.created_at_ns)
        .unwrap_or_default()
}

fn stable_token(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    value
        .bytes()
        .flat_map(|byte| {
            [
                HEX[(byte >> 4) as usize] as char,
                HEX[(byte & 0x0f) as usize] as char,
            ]
        })
        .collect()
}

pub fn graph_kind_name(node: &Node) -> &'static str {
    node.kind
        .anchor_payload_kind()
        .map(|kind| kind.as_str())
        .unwrap_or_else(|| node.kind.tag().as_str())
}

pub fn summarize_node(node: &Node) -> String {
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
        AnchorPayload::SkillResult(result) => result.output.clone(),
    }
}

fn render_session_content(session: &SessionAnchor) -> String {
    if session.prompt.trim().is_empty() {
        session.system_prompt.clone()
    } else {
        session.prompt.clone()
    }
}

fn render_tool_use_content(tool_uses: &[ToolUse]) -> String {
    tool_uses
        .first()
        .map(|tool_use| tool_use.input.to_string())
        .unwrap_or_default()
}

fn render_tool_result_content(tool_results: &[ToolResult]) -> String {
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

pub fn node_target_id(node_id: &str) -> String {
    format!("detail-{node_id}")
}

pub fn node_id_from_target(target: &str) -> Option<&str> {
    target
        .strip_prefix("detail-")
        .filter(|node_id| !node_id.is_empty())
}

pub fn tool_use_input_links(node: &Node, ancestry: &[Node]) -> Vec<ToolUseInputLink> {
    let Some(tool_uses) = node.kind.as_tool_uses() else {
        return Vec::new();
    };

    tool_uses
        .iter()
        .enumerate()
        .filter(|(_, tool_use)| tool_use.name == "write_stdin")
        .filter_map(|(tool_use_index, tool_use)| {
            let session_id = tool_use_session_id(tool_use)?;
            let exec_command = find_exec_command_tool_use_for_session(ancestry, session_id)?;
            Some(ToolUseInputLink {
                tool_use_index,
                tool_use_id: tool_use.id.clone(),
                input_pointer: "/session_id".to_owned(),
                value: session_id.to_owned(),
                target: node_target_id(&exec_command.id),
            })
        })
        .collect()
}

fn tool_use_session_id(tool_use: &ToolUse) -> Option<&str> {
    tool_use
        .input
        .get("session_id")
        .and_then(serde_json::Value::as_str)
}

fn output_session_id(output: &str) -> Option<&str> {
    for line in output.lines() {
        let line = line.trim_end_matches('\r');
        if matches!(line, "stdout:" | "stderr:") {
            return None;
        }
        if let Some(session_id) = line.strip_prefix("session_id: ") {
            return Some(session_id);
        }
    }
    None
}

fn find_tool_use_for_result<'a>(
    ancestry: &'a [Node],
    tool_result_id: &str,
) -> Option<(&'a Node, &'a ToolUse)> {
    ancestry.iter().find_map(|node| {
        let tool_uses = node.kind.as_tool_uses()?;
        tool_uses
            .iter()
            .find(|tool_use| tool_use.id == tool_result_id)
            .map(|tool_use| (node, tool_use))
    })
}

fn find_exec_command_tool_use_for_session<'a>(
    ancestry: &'a [Node],
    session_id: &str,
) -> Option<&'a Node> {
    for node in ancestry {
        let Some(tool_results) = node.kind.as_tool_results() else {
            continue;
        };
        for tool_result in tool_results {
            if output_session_id(&tool_result.output) != Some(session_id) {
                continue;
            }
            let Some((tool_use_node, tool_use)) =
                find_tool_use_for_result(ancestry, &tool_result.id)
            else {
                continue;
            };
            if tool_use.name == "exec_command" {
                return Some(tool_use_node);
            }
        }
    }

    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndpointPortSlots {
    pub source: usize,
    pub source_count: usize,
    pub target: usize,
    pub target_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndpointPortOffsets {
    pub source: i32,
    pub target: i32,
}

pub fn edge_port_offset(slot: usize, count: usize) -> i32 {
    const PORT_RANGE: i32 = 12;

    if count <= 1 {
        return 0;
    }
    let slot = i64::try_from(slot.min(count - 1)).unwrap_or(i64::MAX);
    let intervals = i64::try_from(count - 1).unwrap_or(i64::MAX);
    let numerator = slot.saturating_mul(i64::from(PORT_RANGE)).saturating_mul(2);
    i32::try_from(-i64::from(PORT_RANGE) + numerator / intervals).unwrap_or(i32::MAX)
}

pub fn route_edge(
    source_center: Point,
    target_center: Point,
    slots: EndpointPortSlots,
) -> BezierRoute {
    route_edge_with_offsets(
        source_center,
        target_center,
        EndpointPortOffsets {
            source: edge_port_offset(slots.source, slots.source_count),
            target: edge_port_offset(slots.target, slots.target_count),
        },
    )
}

pub fn route_edge_with_offsets(
    source_center: Point,
    target_center: Point,
    offsets: EndpointPortOffsets,
) -> BezierRoute {
    const SOURCE_PADDING: i32 = 2;
    const TARGET_PADDING: i32 = 6;
    const CONTROL_RATIO_PERCENT: i32 = 45;
    const MIN_CONTROL_DISTANCE: i32 = 24;

    let source = Point {
        x: source_center
            .x
            .saturating_add(GRAPH_NODE_RADIUS)
            .saturating_add(SOURCE_PADDING),
        y: source_center.y.saturating_add(offsets.source),
    };
    let target = Point {
        x: target_center
            .x
            .saturating_sub(GRAPH_NODE_RADIUS)
            .saturating_sub(TARGET_PADDING),
        y: target_center.y.saturating_add(offsets.target),
    };
    let horizontal_distance = target.x.saturating_sub(source.x).max(MIN_CONTROL_DISTANCE);
    let control_distance = i32::try_from(
        i64::from(horizontal_distance).saturating_mul(CONTROL_RATIO_PERCENT.into()) / 100,
    )
    .unwrap_or(i32::MAX)
    .max(MIN_CONTROL_DISTANCE);
    BezierRoute {
        source,
        control_1: Point {
            x: source
                .x
                .saturating_add(control_distance)
                .min(target.x.saturating_sub(8)),
            y: source.y,
        },
        control_2: Point {
            x: target
                .x
                .saturating_sub(control_distance)
                .max(source.x.saturating_add(8)),
            y: target.y,
        },
        target,
    }
}

pub fn node_key(node_id: &str) -> String {
    format!("node:{node_id}")
}

pub fn edge_key(kind: GraphViewportEdgeKind, source_id: &str, target_id: &str) -> String {
    format!("edge:{}:{source_id}:{target_id}", kind.key_part())
}

pub fn diff_graph_viewport_responses(
    previous: GraphViewportResponse,
    current: GraphViewportResponse,
    known: Option<&GraphViewportKnownItems>,
) -> GraphViewportDiffResponse {
    let (known_node_keys, known_node_fingerprints) = known.map_or_else(
        || {
            item_state(
                &previous.nodes,
                |node| &node.key,
                GraphViewportNode::fingerprint,
            )
        },
        |known| {
            (
                known.nodes.iter().cloned().collect(),
                known.node_fingerprints.clone(),
            )
        },
    );
    let (known_edge_keys, known_edge_fingerprints) = known.map_or_else(
        || {
            item_state(
                &previous.edges,
                |edge| &edge.key,
                GraphViewportEdge::fingerprint,
            )
        },
        |known| {
            (
                known.edges.iter().cloned().collect(),
                known.edge_fingerprints.clone(),
            )
        },
    );

    let (added_nodes, updated_nodes, removed_nodes) = diff_items(
        current.nodes,
        known_node_keys,
        known_node_fingerprints,
        |node| &node.key,
        GraphViewportNode::fingerprint,
    );
    let (added_edges, updated_edges, removed_edges) = diff_items(
        current.edges,
        known_edge_keys,
        known_edge_fingerprints,
        |edge| &edge.key,
        GraphViewportEdge::fingerprint,
    );
    let mut removed = removed_nodes
        .into_iter()
        .map(|key| GraphViewportRemovedItem {
            kind: GraphViewportItemKind::Node,
            key,
        })
        .chain(
            removed_edges
                .into_iter()
                .map(|key| GraphViewportRemovedItem {
                    kind: GraphViewportItemKind::Edge,
                    key,
                }),
        )
        .collect::<Vec<_>>();
    removed.sort_by(|left, right| left.key.cmp(&right.key));

    GraphViewportDiffResponse {
        version: current.version,
        canvas: current.canvas,
        previous_viewport: previous.viewport,
        viewport: current.viewport,
        added: GraphViewportItems {
            nodes: added_nodes,
            edges: added_edges,
        },
        updated: GraphViewportItems {
            nodes: updated_nodes,
            edges: updated_edges,
        },
        removed,
    }
}

fn item_state<T>(
    items: &[T],
    key: impl Fn(&T) -> &String,
    fingerprint: impl Fn(&T) -> String,
) -> (BTreeSet<String>, BTreeMap<String, String>) {
    (
        items.iter().map(|item| key(item).clone()).collect(),
        items
            .iter()
            .map(|item| (key(item).clone(), fingerprint(item)))
            .collect(),
    )
}

fn diff_items<T: Clone>(
    current: Vec<T>,
    known_keys: BTreeSet<String>,
    known_fingerprints: BTreeMap<String, String>,
    key: impl Fn(&T) -> &String,
    fingerprint: impl Fn(&T) -> String,
) -> (Vec<T>, Vec<T>, Vec<String>) {
    let current_keys = current
        .iter()
        .map(|item| key(item).clone())
        .collect::<BTreeSet<_>>();
    let mut added = Vec::new();
    let mut updated = Vec::new();
    for item in current {
        let item_key = key(&item);
        if !known_keys.contains(item_key) {
            added.push(item);
        } else if known_fingerprints
            .get(item_key)
            .is_none_or(|known| known != &fingerprint(&item))
        {
            updated.push(item);
        }
    }
    let removed = known_keys.difference(&current_keys).cloned().collect();
    (added, updated, removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{GraphCanvas, GraphViewport};

    fn test_node(id: &str, kind: Kind) -> Node {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "parent": "parent-node",
            "created_at": "2026-07-25T00:00:00Z",
            "role": "LLM",
            "metadata": null,
            "kind": kind,
        }))
        .expect("test node should deserialize")
    }

    fn tool_use(id: &str, name: &str, input: serde_json::Value) -> ToolUse {
        ToolUse {
            id: id.to_owned(),
            name: name.to_owned(),
            input,
        }
    }

    fn tool_result(id: &str, output: &str) -> ToolResult {
        ToolResult {
            id: id.to_owned(),
            output: output.to_owned(),
        }
    }

    fn response(version: u64, nodes: Vec<GraphViewportNode>) -> GraphViewportResponse {
        GraphViewportResponse {
            version,
            canvas: GraphCanvas {
                width: 100,
                height: 100,
            },
            viewport: GraphViewport {
                x: 0,
                y: 0,
                width: 100,
                height: 100,
                overscan: 0,
            },
            nodes,
            edges: Vec::new(),
        }
    }

    #[test]
    fn diff_reports_added_updated_and_removed_nodes() {
        let node = |id: &str, summary: &str| GraphViewportNode {
            key: node_key(id),
            id: id.to_owned(),
            node_target: node_target_id(id),
            short_id: id.to_owned(),
            kind: "text".to_owned(),
            summary: summary.to_owned(),
            labels: Vec::new(),
            x: 0,
            y: 0,
        };
        let diff = diff_graph_viewport_responses(
            response(1, vec![node("a", "old"), node("b", "removed")]),
            response(1, vec![node("a", "new"), node("c", "added")]),
            None,
        );

        assert_eq!(diff.added.nodes[0].id, "c");
        assert_eq!(diff.updated.nodes[0].id, "a");
        assert_eq!(diff.removed[0].key, "node:b");
    }

    #[test]
    fn route_uses_distinct_endpoint_slots() {
        let source = Point { x: 0, y: 0 };
        let target = Point { x: 100, y: 0 };

        let first = route_edge(
            source,
            target,
            EndpointPortSlots {
                source: 0,
                source_count: 2,
                target: 0,
                target_count: 2,
            },
        );
        let second = route_edge(
            source,
            target,
            EndpointPortSlots {
                source: 1,
                source_count: 2,
                target: 1,
                target_count: 2,
            },
        );

        assert_ne!(first.source.y, second.source.y);
        assert_ne!(first.target.y, second.target.y);
    }

    #[test]
    fn a_single_edge_uses_the_vertical_center() {
        assert_eq!(edge_port_offset(0, 1), 0);
        assert_eq!(edge_port_offset(0, 2), -12);
        assert_eq!(edge_port_offset(1, 2), 12);
    }

    #[test]
    fn tool_input_links_identify_the_exact_item_pointer_and_exec_target() {
        let current = test_node(
            "write-node",
            Kind::tool_uses(vec![
                tool_use(
                    "other",
                    "search_skill",
                    serde_json::json!({"session_id": "exec-1"}),
                ),
                tool_use(
                    "write-1",
                    "write_stdin",
                    serde_json::json!({"session_id": "exec-1", "chars": ""}),
                ),
            ]),
        );
        let result = test_node(
            "exec-result",
            Kind::tool_result(tool_result(
                "exec-call",
                "Process running\nsession_id: exec-1\nexit_status: running\n",
            )),
        );
        let exec = test_node(
            "exec-node",
            Kind::tool_use(tool_use(
                "exec-call",
                "exec_command",
                serde_json::json!({"cmd": "sleep 10"}),
            )),
        );

        assert_eq!(
            tool_use_input_links(&current, &[current.clone(), result, exec]),
            vec![ToolUseInputLink {
                tool_use_index: 1,
                tool_use_id: "write-1".to_owned(),
                input_pointer: "/session_id".to_owned(),
                value: "exec-1".to_owned(),
                target: "detail-exec-node".to_owned(),
            }]
        );
    }

    #[test]
    fn duplicate_session_ids_link_to_the_nearest_ancestor_exec_command() {
        let current = test_node(
            "write-node",
            Kind::tool_use(tool_use(
                "write-1",
                "write_stdin",
                serde_json::json!({"session_id": "reused-session"}),
            )),
        );
        let recent_result = test_node(
            "recent-result",
            Kind::tool_result(tool_result(
                "recent-exec-call",
                "session_id: reused-session\nexit_status: running\n",
            )),
        );
        let recent_exec = test_node(
            "recent-exec-node",
            Kind::tool_use(tool_use(
                "recent-exec-call",
                "exec_command",
                serde_json::json!({"cmd": "recent"}),
            )),
        );
        let old_result = test_node(
            "old-result",
            Kind::tool_result(tool_result(
                "old-exec-call",
                "session_id: reused-session\nexit_status: running\n",
            )),
        );
        let old_exec = test_node(
            "old-exec-node",
            Kind::tool_use(tool_use(
                "old-exec-call",
                "exec_command",
                serde_json::json!({"cmd": "old"}),
            )),
        );

        let links = tool_use_input_links(
            &current,
            &[
                current.clone(),
                recent_result,
                recent_exec,
                old_result,
                old_exec,
            ],
        );

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target, "detail-recent-exec-node");
    }

    #[test]
    fn stdout_session_id_does_not_shadow_an_older_structured_exec_header() {
        let current = test_node(
            "write-node",
            Kind::tool_use(tool_use(
                "write-1",
                "write_stdin",
                serde_json::json!({"session_id": "active-session"}),
            )),
        );
        let unrelated_result = test_node(
            "unrelated-result",
            Kind::tool_result(tool_result(
                "unrelated-exec-call",
                "Process exited with code 0\nexit_status: 0\nstdout:\nsession_id: active-session\nstderr:\n",
            )),
        );
        let unrelated_exec = test_node(
            "unrelated-exec-node",
            Kind::tool_use(tool_use(
                "unrelated-exec-call",
                "exec_command",
                serde_json::json!({"cmd": "printf 'session_id: active-session'"}),
            )),
        );
        let original_result = test_node(
            "original-result",
            Kind::tool_result(tool_result(
                "original-exec-call",
                "Process running\r\nsession_id: active-session\r\nexit_status: running\r\nstdout:\r\n\r\nstderr:\r\n",
            )),
        );
        let original_exec = test_node(
            "original-exec-node",
            Kind::tool_use(tool_use(
                "original-exec-call",
                "exec_command",
                serde_json::json!({"cmd": "sleep 10"}),
            )),
        );

        let links = tool_use_input_links(
            &current,
            &[
                current.clone(),
                unrelated_result,
                unrelated_exec,
                original_result,
                original_exec,
            ],
        );

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target, "detail-original-exec-node");
    }

    #[test]
    fn tool_input_links_skip_missing_non_string_and_unresolved_session_ids() {
        let current = test_node(
            "write-node",
            Kind::tool_uses(vec![
                tool_use("missing", "write_stdin", serde_json::json!({"chars": ""})),
                tool_use(
                    "non-string",
                    "write_stdin",
                    serde_json::json!({"session_id": 42}),
                ),
                tool_use(
                    "unresolved",
                    "write_stdin",
                    serde_json::json!({"session_id": "unknown"}),
                ),
            ]),
        );

        assert!(tool_use_input_links(&current, std::slice::from_ref(&current)).is_empty());
    }

    #[test]
    fn multiple_write_stdin_calls_link_independently_by_item() {
        let current = test_node(
            "write-node",
            Kind::tool_uses(vec![
                tool_use(
                    "write-1",
                    "write_stdin",
                    serde_json::json!({"session_id": "exec-1"}),
                ),
                tool_use(
                    "write-2",
                    "write_stdin",
                    serde_json::json!({"session_id": "exec-2"}),
                ),
            ]),
        );
        let first_result = test_node(
            "result-1",
            Kind::tool_result(tool_result("exec-call-1", "session_id: exec-1\n")),
        );
        let first_exec = test_node(
            "exec-node-1",
            Kind::tool_use(tool_use(
                "exec-call-1",
                "exec_command",
                serde_json::json!({"cmd": "first"}),
            )),
        );
        let second_result = test_node(
            "result-2",
            Kind::tool_result(tool_result("exec-call-2", "session_id: exec-2\n")),
        );
        let second_exec = test_node(
            "exec-node-2",
            Kind::tool_use(tool_use(
                "exec-call-2",
                "exec_command",
                serde_json::json!({"cmd": "second"}),
            )),
        );

        let links = tool_use_input_links(
            &current,
            &[
                current.clone(),
                second_result,
                second_exec,
                first_result,
                first_exec,
            ],
        );

        assert_eq!(
            links
                .iter()
                .map(|link| (link.tool_use_index, link.target.as_str()))
                .collect::<Vec<_>>(),
            [(0, "detail-exec-node-1"), (1, "detail-exec-node-2")]
        );
    }
}
