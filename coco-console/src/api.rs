use std::collections::BTreeMap;

use coco_types::Node;
use serde::{Deserialize, Serialize};

pub const GRAPH_SOURCE_PORT_OFFSET_X: i32 = 20;
pub const GRAPH_TARGET_PORT_OFFSET_X: i32 = 24;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct Point {
    pub x: i32,
    pub y: i32,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum NodeDetailResponse {
    Default,
    Missing {
        target: String,
    },
    Found {
        node: Box<Node>,
        #[serde(default)]
        parent_graph_links: BTreeMap<String, GraphPointLink>,
        #[serde(default)]
        markdown_documents: Vec<MarkdownDocument>,
        tool_use_input_links: Vec<ToolUseInputLink>,
        #[serde(default)]
        tool_input_shell_highlights: Vec<ToolInputShellHighlight>,
        #[serde(default)]
        tool_input_json_highlights: Vec<ToolInputJsonHighlight>,
    },
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphPointLink {
    pub point: Point,
    pub local: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct MarkdownDocument {
    pub source: String,
    pub blocks: Vec<MarkdownNode>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MarkdownNode {
    Text {
        text: String,
    },
    Paragraph {
        children: Vec<MarkdownNode>,
    },
    Heading {
        level: u8,
        children: Vec<MarkdownNode>,
    },
    Emphasis {
        children: Vec<MarkdownNode>,
    },
    Strong {
        children: Vec<MarkdownNode>,
    },
    Strikethrough {
        children: Vec<MarkdownNode>,
    },
    InlineCode {
        code: String,
    },
    Link {
        destination: String,
        children: Vec<MarkdownNode>,
    },
    UnorderedList {
        items: Vec<Vec<MarkdownNode>>,
    },
    OrderedList {
        start: u64,
        items: Vec<Vec<MarkdownNode>>,
    },
    BlockQuote {
        children: Vec<MarkdownNode>,
    },
    CodeBlock {
        language: Option<String>,
        code: String,
        #[serde(default)]
        highlights: Vec<CodeHighlightRange>,
    },
    ThematicBreak,
    LineBreak,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct CodeHighlightRange {
    pub kind: CodeHighlightKind,
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CodeHighlightKind {
    Plain,
    Comment,
    Constant,
    Function,
    Keyword,
    Number,
    Operator,
    Property,
    String,
    Type,
    Variable,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ToolUseInputLink {
    pub tool_use_index: usize,
    pub tool_use_id: String,
    pub input_pointer: String,
    pub value: String,
    pub target: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ToolInputShellHighlight {
    pub tool_use_index: usize,
    pub tool_use_id: String,
    pub input_pointer: String,
    pub tokens: Vec<ShellHighlightToken>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ToolInputJsonHighlight {
    pub tool_use_index: usize,
    pub tool_use_id: String,
    pub ranges: Vec<JsonHighlightRange>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ShellHighlightToken {
    pub kind: ShellHighlightKind,
    pub text: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShellHighlightKind {
    Plain,
    Command,
    Option,
    String,
    Variable,
    Operator,
    Comment,
    Keyword,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct JsonHighlightRange {
    pub kind: JsonHighlightKind,
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JsonHighlightKind {
    Plain,
    Key,
    String,
    Number,
    Boolean,
    Null,
    Escape,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AnchorRangeNode {
    pub id: String,
    pub short_id: String,
    pub kind: String,
    pub role: String,
    pub summary: String,
    pub incoming_edge: Option<GraphViewportEdgeKind>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AnchorRangePath {
    pub nodes: Vec<AnchorRangeNode>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AnchorRangeResponse {
    Missing,
    Found { paths: Vec<AnchorRangePath> },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProviderContextItem {
    pub node: ProviderContextNode,
    pub point: Option<Point>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProviderContextNode {
    pub id: String,
    pub short_id: String,
    pub kind: String,
    pub role: String,
    pub created_at: String,
    pub summary: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProviderContextBranch {
    pub name: String,
    pub head_node_id: String,
    pub context_target: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ProviderContextResponse {
    Default,
    Missing {
        target: String,
    },
    Found {
        context_target: String,
        previous_context_target: Option<String>,
        selected_id: String,
        node_ids: Vec<String>,
        branches: Vec<ProviderContextBranch>,
    },
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphCanvas {
    pub width: i32,
    pub height: i32,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphViewport {
    pub x: i32,
    pub y: i32,
    pub width: i32,
    pub height: i32,
    pub overscan: i32,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct GraphViewportResponse {
    pub version: u64,
    pub canvas: GraphCanvas,
    pub viewport: GraphViewport,
    pub error_nodes: Vec<GraphErrorNode>,
    pub active_job_count: usize,
    pub jobs: Vec<GraphJob>,
    pub nodes: Vec<GraphViewportNode>,
    pub edges: Vec<GraphViewportEdge>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct GraphViewportDiffResponse {
    pub version: u64,
    pub canvas: GraphCanvas,
    pub previous_viewport: GraphViewport,
    pub viewport: GraphViewport,
    pub error_nodes: Vec<GraphErrorNode>,
    pub active_job_count: usize,
    pub jobs: Vec<GraphJob>,
    pub added: GraphViewportItems,
    pub updated: GraphViewportItems,
    pub removed: Vec<GraphViewportRemovedItem>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct GraphJobsResponse {
    pub active_job_count: usize,
    pub jobs: Vec<GraphJob>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphErrorNode {
    pub id: String,
    pub node_target: String,
    pub short_id: String,
    pub created_at: String,
    pub summary: String,
    pub point: Point,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphJob {
    pub id: String,
    pub short_id: String,
    pub created_at: String,
    pub status: String,
    pub branch: String,
    pub work_branch: String,
    pub head_id: String,
    pub head_target: String,
    pub head_short_id: String,
    pub point: Point,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct GraphViewportItems {
    pub nodes: Vec<GraphViewportNode>,
    pub edges: Vec<GraphViewportEdge>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphViewportItemKind {
    Node,
    Edge,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphViewportRemovedItem {
    pub kind: GraphViewportItemKind,
    pub key: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphViewportNode {
    pub key: String,
    pub id: String,
    pub node_target: String,
    pub short_id: String,
    pub kind: String,
    pub summary: String,
    pub labels: Vec<String>,
    pub x: i32,
    pub y: i32,
}

impl GraphViewportNode {
    pub fn fingerprint(&self) -> String {
        graph_viewport_item_fingerprint(self)
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum GraphViewportEdgeKind {
    #[serde(rename = "primary_parent")]
    Primary,
    #[serde(rename = "merge_parent")]
    Merge,
    #[serde(rename = "shadow_parent")]
    Shadow,
}

impl GraphViewportEdgeKind {
    pub fn key_part(self) -> &'static str {
        match self {
            Self::Primary => "primary_parent",
            Self::Merge => "merge_parent",
            Self::Shadow => "shadow_parent",
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn from_key_part(value: &str) -> Option<Self> {
        match value {
            "primary_parent" => Some(Self::Primary),
            "merge_parent" => Some(Self::Merge),
            "shadow_parent" => Some(Self::Shadow),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphBezierRoute {
    pub source: Point,
    pub control_1: Point,
    pub control_2: Point,
    pub target: Point,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct GraphViewportEdge {
    pub key: String,
    pub kind: GraphViewportEdgeKind,
    pub source_id: String,
    pub target_id: String,
    pub route: GraphBezierRoute,
}

impl GraphViewportEdge {
    pub fn fingerprint(&self) -> String {
        graph_viewport_item_fingerprint(self)
    }
}

fn graph_viewport_item_fingerprint<T>(item: &T) -> String
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(item).expect("graph viewport items should serialize");
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::{
        GraphBezierRoute, GraphViewportEdge, GraphViewportEdgeKind, GraphViewportNode, Point,
    };

    #[test]
    fn fingerprints_change_when_item_payload_changes() {
        let mut node = GraphViewportNode {
            key: "node:1".to_owned(),
            id: "1".to_owned(),
            node_target: "node:1".to_owned(),
            short_id: "1".to_owned(),
            kind: "text".to_owned(),
            summary: "first".to_owned(),
            labels: vec!["main".to_owned()],
            x: 0,
            y: 0,
        };
        let mut edge = GraphViewportEdge {
            key: "edge:primary:1:2".to_owned(),
            kind: GraphViewportEdgeKind::Primary,
            source_id: "1".to_owned(),
            target_id: "2".to_owned(),
            route: GraphBezierRoute {
                source: Point { x: 0, y: 0 },
                control_1: Point { x: 30, y: 0 },
                control_2: Point { x: 70, y: 100 },
                target: Point { x: 100, y: 100 },
            },
        };

        let node_fingerprint = node.fingerprint();
        let edge_fingerprint = edge.fingerprint();
        node.labels.push("draft".to_owned());
        edge.route.control_1.y = 10;

        assert_ne!(node_fingerprint, node.fingerprint());
        assert_ne!(edge_fingerprint, edge.fingerprint());
    }
}
