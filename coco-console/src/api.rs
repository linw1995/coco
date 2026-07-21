use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct Point {
    pub x: i32,
    pub y: i32,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PanelNode {
    pub id: String,
    pub short_id: String,
    pub kind: String,
    pub role: String,
    pub created_at: String,
    pub content: String,
    pub summary: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum NodeDetailResponse {
    Default,
    Missing { target: String },
    Found { node: PanelNode },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProviderContextItem {
    pub context_target: String,
    pub node: PanelNode,
    pub selected: bool,
    pub point: Option<Point>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ProviderContextResponse {
    Default,
    Missing { target: String },
    Found { items: Vec<ProviderContextItem> },
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
    pub nodes: Vec<GraphViewportNode>,
    pub edges: Vec<GraphViewportEdge>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct GraphViewportDiffResponse {
    pub version: u64,
    pub canvas: GraphCanvas,
    pub previous_viewport: GraphViewport,
    pub viewport: GraphViewport,
    pub added: GraphViewportItems,
    pub updated: GraphViewportItems,
    pub removed: Vec<GraphViewportRemovedItem>,
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
