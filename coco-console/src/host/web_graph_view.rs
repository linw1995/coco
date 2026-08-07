use std::collections::{BTreeMap, BTreeSet, HashMap};

use coco_mem::{Anchor, AnchorPayload, Kind, Node, SessionAnchor, ToolResult, ToolUse};
use serde::{Deserialize, Serialize};

use crate::api::{
    GRAPH_SOURCE_PORT_OFFSET_X, GRAPH_TARGET_PORT_OFFSET_X, GraphViewportDiffResponse,
    GraphViewportEdge, GraphViewportEdgeKind, GraphViewportItemKind, GraphViewportItems,
    GraphViewportNode, GraphViewportRemovedItem, GraphViewportResponse, ToolUseInputLink,
};
use crate::host::api::GraphViewportKnownItems;
use crate::web_graph::{BezierRoute, Point};

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
    pub previous_id: Option<String>,
    pub node_ids: Vec<String>,
    pub branches: Vec<ProviderContextBranch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderContextBranch {
    pub name: String,
    pub head_node_id: String,
    pub context_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderContextSelection {
    pub context: ProviderContext,
    pub selected_id: String,
}

pub fn is_provider_context_start(node: &Node) -> bool {
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

pub fn is_skill_invocation_anchor(node: &Node) -> bool {
    matches!(
        &node.kind,
        Kind::Anchor(anchor) if anchor.as_skill_invocation().is_some()
    )
}

pub fn provider_context_id(head_node_id: &str) -> String {
    format!("{}-context", node_target_id(head_node_id))
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

pub fn write_stdin_session_ids(node: &Node) -> impl Iterator<Item = &str> {
    node.kind
        .as_tool_uses()
        .into_iter()
        .flatten()
        .filter(|tool_use| tool_use.name == "write_stdin")
        .filter_map(tool_use_session_id)
}

pub fn tool_use_input_links(
    node: &Node,
    exec_node_ids_by_session: &HashMap<String, String>,
) -> Vec<ToolUseInputLink> {
    let Some(tool_uses) = node.kind.as_tool_uses() else {
        return Vec::new();
    };

    tool_uses
        .iter()
        .enumerate()
        .filter(|(_, tool_use)| tool_use.name == "write_stdin")
        .filter_map(|(tool_use_index, tool_use)| {
            let session_id = tool_use_session_id(tool_use)?;
            let exec_node_id = exec_node_ids_by_session.get(session_id)?;
            Some(ToolUseInputLink {
                tool_use_index,
                tool_use_id: tool_use.id.clone(),
                input_pointer: "/session_id".to_owned(),
                value: session_id.to_owned(),
                target: node_target_id(exec_node_id),
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
    const CONTROL_RATIO_PERCENT: i32 = 45;
    const MIN_CONTROL_DISTANCE: i32 = 24;

    let source = Point {
        x: source_center.x.saturating_add(GRAPH_SOURCE_PORT_OFFSET_X),
        y: source_center.y.saturating_add(offsets.source),
    };
    let target = Point {
        x: target_center.x.saturating_sub(GRAPH_TARGET_PORT_OFFSET_X),
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
    let error_nodes = current.error_nodes.clone();
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
        error_nodes,
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
            error_nodes: Vec::new(),
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
        let targets = HashMap::from([("exec-1".to_owned(), "exec-node".to_owned())]);

        assert_eq!(
            tool_use_input_links(&current, &targets),
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

        assert!(tool_use_input_links(&current, &HashMap::new()).is_empty());
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
        let links = tool_use_input_links(
            &current,
            &HashMap::from([
                ("exec-1".to_owned(), "exec-node-1".to_owned()),
                ("exec-2".to_owned(), "exec-node-2".to_owned()),
            ]),
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
