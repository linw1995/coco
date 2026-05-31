use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct Point {
    pub x: i32,
    pub y: i32,
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
    pub lanes: Vec<GraphViewportLane>,
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
    pub lanes: Vec<GraphViewportLane>,
    pub nodes: Vec<GraphViewportNode>,
    pub edges: Vec<GraphViewportEdge>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphViewportItemKind {
    Lane,
    Node,
    Edge,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphViewportRemovedItem {
    pub kind: GraphViewportItemKind,
    pub key: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphViewportLane {
    pub key: String,
    pub label: String,
    pub y: i32,
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

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphViewportEdgeKind {
    PrimaryParent,
    Fork,
    MergeParent,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct GraphViewportEdge {
    pub key: String,
    pub kind: GraphViewportEdgeKind,
    pub source_id: String,
    pub target_id: String,
    pub source: Point,
    pub target: Point,
    pub route_slot: i32,
    pub target_port_offset: f64,
}
