use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

use serde::{Deserialize, Serialize};

pub const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(Error::EmptyNodeId { context: "node id" });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct Revision(u64);

impl Revision {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Revision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct SourceVersion(u64);

impl SourceVersion {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for SourceVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum EdgeKind {
    #[serde(rename = "primary_parent")]
    Primary,
    #[serde(rename = "merge_parent")]
    Merge,
    #[serde(rename = "shadow_parent")]
    Shadow,
}

impl EdgeKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary_parent",
            Self::Merge => "merge_parent",
            Self::Shadow => "shadow_parent",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeId {
    pub kind: EdgeKind,
    pub source: NodeId,
    pub target: NodeId,
}

impl EdgeId {
    pub fn new(kind: EdgeKind, source: NodeId, target: NodeId) -> Self {
        Self {
            kind,
            source,
            target,
        }
    }
}

impl fmt::Display for EdgeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}:{}->{}",
            self.kind.as_str(),
            self.source,
            self.target
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Point {
    pub x: i32,
    pub y: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Canvas {
    pub width: i32,
    pub height: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BezierRoute {
    pub source: Point,
    pub control_1: Point,
    pub control_2: Point,
    pub target: Point,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LayoutKind {
    Anchors,
    All,
}

impl LayoutKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Anchors => "anchors",
            Self::All => "all",
        }
    }
}

impl fmt::Display for LayoutKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Graph {
    revision: Revision,
    source_version: SourceVersion,
    topology: Topology,
    layouts: Layouts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topology {
    nodes: BTreeSet<NodeId>,
    edges: BTreeSet<EdgeId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layouts {
    anchors: Layout,
    all: Layout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    canvas: Canvas,
    nodes: BTreeMap<NodeId, Point>,
    edges: BTreeMap<EdgeId, BezierRoute>,
}

impl Graph {
    pub fn from_snapshot(snapshot: Snapshot) -> Result<Self> {
        validate_format_version(snapshot.format_version)?;
        let graph = Self {
            revision: snapshot.revision,
            source_version: snapshot.source_version,
            topology: Topology::from_snapshot(snapshot.topology)?,
            layouts: Layouts::from_snapshot(snapshot.layouts)?,
        };
        graph.validate()?;
        Ok(graph)
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            format_version: FORMAT_VERSION,
            revision: self.revision,
            source_version: self.source_version,
            topology: self.topology.snapshot(),
            layouts: self.layouts.snapshot(),
        }
    }

    pub fn apply_patch(&mut self, patch: Patch) -> Result<()> {
        patch.validate_against(self.revision, self.source_version)?;
        let mut candidate = self.clone();
        candidate.topology.apply_patch(patch.topology)?;
        candidate.layouts.apply_patch(patch.layouts)?;
        candidate.revision = patch.revision;
        candidate.source_version = patch.source_version;
        candidate.validate()?;
        *self = candidate;
        Ok(())
    }

    pub fn revision(&self) -> Revision {
        self.revision
    }

    pub fn source_version(&self) -> SourceVersion {
        self.source_version
    }

    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    pub fn layout(&self, kind: LayoutKind) -> &Layout {
        self.layouts.get(kind)
    }

    fn validate(&self) -> Result<()> {
        self.topology.validate()?;
        self.layouts.validate(&self.topology)
    }
}

impl Topology {
    pub fn nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.nodes.iter()
    }

    pub fn edges(&self) -> impl Iterator<Item = &EdgeId> {
        self.edges.iter()
    }

    fn from_snapshot(snapshot: TopologySnapshot) -> Result<Self> {
        let nodes = collect_unique_nodes(snapshot.nodes, "snapshot.topology.nodes")?;
        let edges = collect_unique_edges(snapshot.edges, "snapshot.topology.edges")?;
        Ok(Self { nodes, edges })
    }

    fn snapshot(&self) -> TopologySnapshot {
        TopologySnapshot {
            nodes: self.nodes.iter().cloned().collect(),
            edges: self.edges.iter().cloned().collect(),
        }
    }

    fn apply_patch(&mut self, patch: TopologyPatch) -> Result<()> {
        for edge in patch.remove_edges {
            if !self.edges.remove(&edge) {
                return Err(missing_patch_item("topology.remove_edges", &edge));
            }
        }
        for node in patch.remove_nodes {
            if !self.nodes.remove(&node) {
                return Err(missing_patch_item("topology.remove_nodes", &node));
            }
        }
        for node in patch.add_nodes {
            if !self.nodes.insert(node.clone()) {
                return Err(existing_patch_item("topology.add_nodes", &node));
            }
        }
        for edge in patch.add_edges {
            if !self.edges.insert(edge.clone()) {
                return Err(existing_patch_item("topology.add_edges", &edge));
            }
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        for node in &self.nodes {
            validate_node_id(node, "topology.nodes")?;
        }
        for edge in &self.edges {
            validate_edge_id(edge, "topology.edges")?;
            for endpoint in [&edge.source, &edge.target] {
                if !self.nodes.contains(endpoint) {
                    return Err(Error::MissingEdgeEndpoint {
                        edge: edge.clone(),
                        node: endpoint.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

impl Layouts {
    fn from_snapshot(snapshot: LayoutSnapshots) -> Result<Self> {
        Ok(Self {
            anchors: Layout::from_snapshot(LayoutKind::Anchors, snapshot.anchors)?,
            all: Layout::from_snapshot(LayoutKind::All, snapshot.all)?,
        })
    }

    fn snapshot(&self) -> LayoutSnapshots {
        LayoutSnapshots {
            anchors: self.anchors.snapshot(),
            all: self.all.snapshot(),
        }
    }

    fn apply_patch(&mut self, patch: LayoutPatches) -> Result<()> {
        self.anchors
            .apply_patch(LayoutKind::Anchors, patch.anchors)?;
        self.all.apply_patch(LayoutKind::All, patch.all)
    }

    fn get(&self, kind: LayoutKind) -> &Layout {
        match kind {
            LayoutKind::Anchors => &self.anchors,
            LayoutKind::All => &self.all,
        }
    }

    fn validate(&self, topology: &Topology) -> Result<()> {
        self.anchors.validate(LayoutKind::Anchors, topology)?;
        self.all.validate(LayoutKind::All, topology)?;

        for node in self.anchors.nodes.keys() {
            if !self.all.nodes.contains_key(node) {
                return Err(Error::AnchorNodeMissingFromAll { node: node.clone() });
            }
        }
        for node in &topology.nodes {
            if !self.all.nodes.contains_key(node) {
                return Err(Error::TopologyNodeMissingFromAll { node: node.clone() });
            }
        }
        for edge in &topology.edges {
            if !self.anchors.edges.contains_key(edge) && !self.all.edges.contains_key(edge) {
                return Err(Error::UnusedTopologyEdge { edge: edge.clone() });
            }
        }
        Ok(())
    }
}

impl Layout {
    pub fn canvas(&self) -> Canvas {
        self.canvas
    }

    pub fn nodes(&self) -> impl Iterator<Item = (&NodeId, &Point)> {
        self.nodes.iter()
    }

    pub fn edges(&self) -> impl Iterator<Item = (&EdgeId, &BezierRoute)> {
        self.edges.iter()
    }

    pub fn node(&self, node: &NodeId) -> Option<Point> {
        self.nodes.get(node).copied()
    }

    pub fn edge(&self, edge: &EdgeId) -> Option<BezierRoute> {
        self.edges.get(edge).copied()
    }

    fn from_snapshot(kind: LayoutKind, snapshot: LayoutSnapshot) -> Result<Self> {
        let nodes = collect_unique_placements(kind, snapshot.nodes)?;
        let edges = collect_unique_routes(kind, snapshot.edges)?;
        Ok(Self {
            canvas: snapshot.canvas,
            nodes,
            edges,
        })
    }

    fn snapshot(&self) -> LayoutSnapshot {
        LayoutSnapshot {
            canvas: self.canvas,
            nodes: self
                .nodes
                .iter()
                .map(|(node, point)| NodePlacement {
                    node: node.clone(),
                    point: *point,
                })
                .collect(),
            edges: self
                .edges
                .iter()
                .map(|(edge, route)| RoutedEdge {
                    edge: edge.clone(),
                    route: *route,
                })
                .collect(),
        }
    }

    fn apply_patch(&mut self, kind: LayoutKind, patch: LayoutPatch) -> Result<()> {
        if let Some(canvas) = patch.canvas {
            self.canvas = canvas;
        }
        for edge in patch.remove_edges {
            if self.edges.remove(&edge).is_none() {
                return Err(missing_patch_item(
                    layout_collection(kind, "remove_edges"),
                    &edge,
                ));
            }
        }
        for node in patch.remove_nodes {
            if self.nodes.remove(&node).is_none() {
                return Err(missing_patch_item(
                    layout_collection(kind, "remove_nodes"),
                    &node,
                ));
            }
        }
        for placement in patch.upsert_nodes {
            self.nodes.insert(placement.node, placement.point);
        }
        for routed in patch.upsert_edges {
            self.edges.insert(routed.edge, routed.route);
        }
        Ok(())
    }

    fn validate(&self, kind: LayoutKind, topology: &Topology) -> Result<()> {
        if self.canvas.width <= 0 || self.canvas.height <= 0 {
            return Err(Error::InvalidCanvas {
                layout: kind,
                width: self.canvas.width,
                height: self.canvas.height,
            });
        }

        for node in self.nodes.keys() {
            validate_node_id(node, "layout.nodes")?;
            if !topology.nodes.contains(node) {
                return Err(Error::LayoutNodeMissingFromTopology {
                    layout: kind,
                    node: node.clone(),
                });
            }
        }
        for edge in self.edges.keys() {
            validate_edge_id(edge, "layout.edges")?;
            if !topology.edges.contains(edge) {
                return Err(Error::LayoutEdgeMissingFromTopology {
                    layout: kind,
                    edge: edge.clone(),
                });
            }
            for endpoint in [&edge.source, &edge.target] {
                if !self.nodes.contains_key(endpoint) {
                    return Err(Error::LayoutEdgeEndpointMissing {
                        layout: kind,
                        edge: edge.clone(),
                        node: endpoint.clone(),
                    });
                }
            }
        }
        validate_acyclic(kind, self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub format_version: u32,
    pub revision: Revision,
    pub source_version: SourceVersion,
    pub topology: TopologySnapshot,
    pub layouts: LayoutSnapshots,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TopologySnapshot {
    pub nodes: Vec<NodeId>,
    pub edges: Vec<EdgeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LayoutSnapshots {
    pub anchors: LayoutSnapshot,
    pub all: LayoutSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LayoutSnapshot {
    pub canvas: Canvas,
    pub nodes: Vec<NodePlacement>,
    pub edges: Vec<RoutedEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodePlacement {
    pub node: NodeId,
    pub point: Point,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RoutedEdge {
    pub edge: EdgeId,
    pub route: BezierRoute,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Patch {
    pub format_version: u32,
    pub base_revision: Revision,
    pub revision: Revision,
    pub source_version: SourceVersion,
    pub topology: TopologyPatch,
    pub layouts: LayoutPatches,
}

impl Patch {
    pub fn validate_against(
        &self,
        current_revision: Revision,
        current_source_version: SourceVersion,
    ) -> Result<()> {
        validate_format_version(self.format_version)?;
        if self.base_revision != current_revision {
            return Err(Error::RevisionMismatch {
                current: current_revision,
                base: self.base_revision,
            });
        }
        if self.revision <= current_revision {
            return Err(Error::RevisionNotAdvanced {
                current: current_revision,
                next: self.revision,
            });
        }
        if self.source_version < current_source_version {
            return Err(Error::SourceVersionRegressed {
                current: current_source_version,
                next: self.source_version,
            });
        }
        self.validate()
    }

    fn validate(&self) -> Result<()> {
        self.topology.validate()?;
        self.layouts.validate()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyPatch {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub add_nodes: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remove_nodes: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub add_edges: Vec<EdgeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remove_edges: Vec<EdgeId>,
}

impl TopologyPatch {
    fn validate(&self) -> Result<()> {
        validate_node_list(&self.add_nodes, "topology.add_nodes")?;
        validate_node_list(&self.remove_nodes, "topology.remove_nodes")?;
        validate_edge_list(&self.add_edges, "topology.add_edges")?;
        validate_edge_list(&self.remove_edges, "topology.remove_edges")?;
        ensure_disjoint(&self.add_nodes, &self.remove_nodes, "topology.nodes")?;
        ensure_disjoint(&self.add_edges, &self.remove_edges, "topology.edges")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LayoutPatches {
    #[serde(default)]
    pub anchors: LayoutPatch,
    #[serde(default)]
    pub all: LayoutPatch,
}

impl LayoutPatches {
    fn validate(&self) -> Result<()> {
        self.anchors.validate(LayoutKind::Anchors)?;
        self.all.validate(LayoutKind::All)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LayoutPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canvas: Option<Canvas>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub upsert_nodes: Vec<NodePlacement>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remove_nodes: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub upsert_edges: Vec<RoutedEdge>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remove_edges: Vec<EdgeId>,
}

impl LayoutPatch {
    fn validate(&self, kind: LayoutKind) -> Result<()> {
        let prefix = kind.as_str();
        if let Some(canvas) = self.canvas
            && (canvas.width <= 0 || canvas.height <= 0)
        {
            return Err(Error::InvalidCanvas {
                layout: kind,
                width: canvas.width,
                height: canvas.height,
            });
        }
        validate_placements(&self.upsert_nodes, layout_collection(kind, "upsert_nodes"))?;
        validate_node_list(&self.remove_nodes, layout_collection(kind, "remove_nodes"))?;
        validate_routes(&self.upsert_edges, layout_collection(kind, "upsert_edges"))?;
        validate_edge_list(&self.remove_edges, layout_collection(kind, "remove_edges"))?;
        ensure_disjoint_by_key(
            self.upsert_nodes.iter().map(|placement| &placement.node),
            self.remove_nodes.iter(),
            if prefix == "anchors" {
                "anchors.nodes"
            } else {
                "all.nodes"
            },
        )?;
        ensure_disjoint_by_key(
            self.upsert_edges.iter().map(|routed| &routed.edge),
            self.remove_edges.iter(),
            if prefix == "anchors" {
                "anchors.edges"
            } else {
                "all.edges"
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    UnsupportedFormat {
        actual: u32,
    },
    EmptyNodeId {
        context: &'static str,
    },
    SelfEdge {
        context: &'static str,
        edge: EdgeId,
    },
    DuplicateItem {
        collection: &'static str,
        key: String,
    },
    ConflictingPatchItem {
        collection: &'static str,
        key: String,
    },
    MissingPatchItem {
        collection: &'static str,
        key: String,
    },
    ExistingPatchItem {
        collection: &'static str,
        key: String,
    },
    RevisionMismatch {
        current: Revision,
        base: Revision,
    },
    RevisionNotAdvanced {
        current: Revision,
        next: Revision,
    },
    SourceVersionRegressed {
        current: SourceVersion,
        next: SourceVersion,
    },
    InvalidCanvas {
        layout: LayoutKind,
        width: i32,
        height: i32,
    },
    MissingEdgeEndpoint {
        edge: EdgeId,
        node: NodeId,
    },
    LayoutNodeMissingFromTopology {
        layout: LayoutKind,
        node: NodeId,
    },
    LayoutEdgeMissingFromTopology {
        layout: LayoutKind,
        edge: EdgeId,
    },
    LayoutEdgeEndpointMissing {
        layout: LayoutKind,
        edge: EdgeId,
        node: NodeId,
    },
    AnchorNodeMissingFromAll {
        node: NodeId,
    },
    TopologyNodeMissingFromAll {
        node: NodeId,
    },
    UnusedTopologyEdge {
        edge: EdgeId,
    },
    Cycle {
        layout: LayoutKind,
        node: NodeId,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedFormat { actual } => write!(
                formatter,
                "unsupported web graph format version {actual}; expected {FORMAT_VERSION}"
            ),
            Self::EmptyNodeId { context } => write!(formatter, "{context} contains an empty id"),
            Self::SelfEdge { context, edge } => {
                write!(formatter, "{context} contains self edge {edge}")
            }
            Self::DuplicateItem { collection, key } => {
                write!(formatter, "{collection} contains duplicate item {key}")
            }
            Self::ConflictingPatchItem { collection, key } => {
                write!(formatter, "{collection} adds and removes item {key}")
            }
            Self::MissingPatchItem { collection, key } => {
                write!(formatter, "{collection} removes missing item {key}")
            }
            Self::ExistingPatchItem { collection, key } => {
                write!(formatter, "{collection} adds existing item {key}")
            }
            Self::RevisionMismatch { current, base } => write!(
                formatter,
                "patch base revision {base} does not match graph revision {current}"
            ),
            Self::RevisionNotAdvanced { current, next } => write!(
                formatter,
                "patch revision {next} does not advance graph revision {current}"
            ),
            Self::SourceVersionRegressed { current, next } => write!(
                formatter,
                "patch source version {next} regresses graph source version {current}"
            ),
            Self::InvalidCanvas {
                layout,
                width,
                height,
            } => write!(
                formatter,
                "{layout} layout has invalid canvas {width}x{height}"
            ),
            Self::MissingEdgeEndpoint { edge, node } => {
                write!(
                    formatter,
                    "topology edge {edge} references missing node {node}"
                )
            }
            Self::LayoutNodeMissingFromTopology { layout, node } => write!(
                formatter,
                "{layout} layout node {node} is missing from topology"
            ),
            Self::LayoutEdgeMissingFromTopology { layout, edge } => write!(
                formatter,
                "{layout} layout edge {edge} is missing from topology"
            ),
            Self::LayoutEdgeEndpointMissing { layout, edge, node } => write!(
                formatter,
                "{layout} layout edge {edge} references hidden node {node}"
            ),
            Self::AnchorNodeMissingFromAll { node } => {
                write!(
                    formatter,
                    "anchors layout node {node} is missing from all layout"
                )
            }
            Self::TopologyNodeMissingFromAll { node } => {
                write!(formatter, "topology node {node} is missing from all layout")
            }
            Self::UnusedTopologyEdge { edge } => {
                write!(formatter, "topology edge {edge} is unused by both layouts")
            }
            Self::Cycle { layout, node } => {
                write!(
                    formatter,
                    "{layout} layout contains a cycle involving node {node}"
                )
            }
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;

fn validate_format_version(actual: u32) -> Result<()> {
    if actual == FORMAT_VERSION {
        Ok(())
    } else {
        Err(Error::UnsupportedFormat { actual })
    }
}

fn validate_node_id(node: &NodeId, context: &'static str) -> Result<()> {
    if node.0.is_empty() {
        Err(Error::EmptyNodeId { context })
    } else {
        Ok(())
    }
}

fn validate_edge_id(edge: &EdgeId, context: &'static str) -> Result<()> {
    validate_node_id(&edge.source, context)?;
    validate_node_id(&edge.target, context)?;
    if edge.source == edge.target {
        return Err(Error::SelfEdge {
            context,
            edge: edge.clone(),
        });
    }
    Ok(())
}

fn collect_unique_nodes(nodes: Vec<NodeId>, collection: &'static str) -> Result<BTreeSet<NodeId>> {
    let mut unique = BTreeSet::new();
    for node in nodes {
        validate_node_id(&node, collection)?;
        if !unique.insert(node.clone()) {
            return Err(duplicate_item(collection, &node));
        }
    }
    Ok(unique)
}

fn collect_unique_edges(edges: Vec<EdgeId>, collection: &'static str) -> Result<BTreeSet<EdgeId>> {
    let mut unique = BTreeSet::new();
    for edge in edges {
        validate_edge_id(&edge, collection)?;
        if !unique.insert(edge.clone()) {
            return Err(duplicate_item(collection, &edge));
        }
    }
    Ok(unique)
}

fn collect_unique_placements(
    kind: LayoutKind,
    placements: Vec<NodePlacement>,
) -> Result<BTreeMap<NodeId, Point>> {
    let collection = layout_collection(kind, "nodes");
    let mut unique = BTreeMap::new();
    for placement in placements {
        validate_node_id(&placement.node, collection)?;
        if unique
            .insert(placement.node.clone(), placement.point)
            .is_some()
        {
            return Err(duplicate_item(collection, &placement.node));
        }
    }
    Ok(unique)
}

fn collect_unique_routes(
    kind: LayoutKind,
    routes: Vec<RoutedEdge>,
) -> Result<BTreeMap<EdgeId, BezierRoute>> {
    let collection = layout_collection(kind, "edges");
    let mut unique = BTreeMap::new();
    for routed in routes {
        validate_edge_id(&routed.edge, collection)?;
        if unique.insert(routed.edge.clone(), routed.route).is_some() {
            return Err(duplicate_item(collection, &routed.edge));
        }
    }
    Ok(unique)
}

fn validate_node_list(nodes: &[NodeId], collection: &'static str) -> Result<()> {
    let mut unique = BTreeSet::new();
    for node in nodes {
        validate_node_id(node, collection)?;
        if !unique.insert(node) {
            return Err(duplicate_item(collection, node));
        }
    }
    Ok(())
}

fn validate_edge_list(edges: &[EdgeId], collection: &'static str) -> Result<()> {
    let mut unique = BTreeSet::new();
    for edge in edges {
        validate_edge_id(edge, collection)?;
        if !unique.insert(edge) {
            return Err(duplicate_item(collection, edge));
        }
    }
    Ok(())
}

fn validate_placements(placements: &[NodePlacement], collection: &'static str) -> Result<()> {
    let mut unique = BTreeSet::new();
    for placement in placements {
        validate_node_id(&placement.node, collection)?;
        if !unique.insert(&placement.node) {
            return Err(duplicate_item(collection, &placement.node));
        }
    }
    Ok(())
}

fn validate_routes(routes: &[RoutedEdge], collection: &'static str) -> Result<()> {
    let mut unique = BTreeSet::new();
    for routed in routes {
        validate_edge_id(&routed.edge, collection)?;
        if !unique.insert(&routed.edge) {
            return Err(duplicate_item(collection, &routed.edge));
        }
    }
    Ok(())
}

fn ensure_disjoint<T>(added: &[T], removed: &[T], collection: &'static str) -> Result<()>
where
    T: Ord + fmt::Display,
{
    ensure_disjoint_by_key(added.iter(), removed.iter(), collection)
}

fn ensure_disjoint_by_key<'a, T, I, J>(added: I, removed: J, collection: &'static str) -> Result<()>
where
    T: 'a + Ord + fmt::Display,
    I: IntoIterator<Item = &'a T>,
    J: IntoIterator<Item = &'a T>,
{
    let added = added.into_iter().collect::<BTreeSet<_>>();
    for item in removed {
        if added.contains(item) {
            return Err(Error::ConflictingPatchItem {
                collection,
                key: item.to_string(),
            });
        }
    }
    Ok(())
}

fn validate_acyclic(kind: LayoutKind, layout: &Layout) -> Result<()> {
    let mut incoming = layout
        .nodes
        .keys()
        .cloned()
        .map(|node| (node, 0_usize))
        .collect::<BTreeMap<_, _>>();
    let mut outgoing = BTreeMap::<NodeId, Vec<NodeId>>::new();
    for edge in layout.edges.keys() {
        *incoming
            .get_mut(&edge.target)
            .expect("layout endpoints were validated before cycle detection") += 1;
        outgoing
            .entry(edge.source.clone())
            .or_default()
            .push(edge.target.clone());
    }

    let mut ready = incoming
        .iter()
        .filter_map(|(node, count)| (*count == 0).then_some(node.clone()))
        .collect::<VecDeque<_>>();
    let mut visited = 0;
    while let Some(node) = ready.pop_front() {
        visited += 1;
        for target in outgoing.get(&node).into_iter().flatten() {
            let count = incoming
                .get_mut(target)
                .expect("layout endpoints were validated before cycle detection");
            *count -= 1;
            if *count == 0 {
                ready.push_back(target.clone());
            }
        }
    }
    if visited == layout.nodes.len() {
        return Ok(());
    }
    let node = incoming
        .into_iter()
        .find_map(|(node, count)| (count > 0).then_some(node))
        .expect("an unvisited layout must contain a cycle");
    Err(Error::Cycle { layout: kind, node })
}

fn layout_collection(kind: LayoutKind, suffix: &'static str) -> &'static str {
    match (kind, suffix) {
        (LayoutKind::Anchors, "nodes") => "snapshot.layouts.anchors.nodes",
        (LayoutKind::Anchors, "edges") => "snapshot.layouts.anchors.edges",
        (LayoutKind::Anchors, "upsert_nodes") => "layouts.anchors.upsert_nodes",
        (LayoutKind::Anchors, "remove_nodes") => "layouts.anchors.remove_nodes",
        (LayoutKind::Anchors, "upsert_edges") => "layouts.anchors.upsert_edges",
        (LayoutKind::Anchors, "remove_edges") => "layouts.anchors.remove_edges",
        (LayoutKind::All, "nodes") => "snapshot.layouts.all.nodes",
        (LayoutKind::All, "edges") => "snapshot.layouts.all.edges",
        (LayoutKind::All, "upsert_nodes") => "layouts.all.upsert_nodes",
        (LayoutKind::All, "remove_nodes") => "layouts.all.remove_nodes",
        (LayoutKind::All, "upsert_edges") => "layouts.all.upsert_edges",
        (LayoutKind::All, "remove_edges") => "layouts.all.remove_edges",
        _ => "layouts",
    }
}

fn duplicate_item(collection: &'static str, key: &impl fmt::Display) -> Error {
    Error::DuplicateItem {
        collection,
        key: key.to_string(),
    }
}

fn missing_patch_item(collection: &'static str, key: &impl fmt::Display) -> Error {
    Error::MissingPatchItem {
        collection,
        key: key.to_string(),
    }
}

fn existing_patch_item(collection: &'static str, key: &impl fmt::Display) -> Error {
    Error::ExistingPatchItem {
        collection,
        key: key.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(value: &str) -> NodeId {
        NodeId::new(value).unwrap()
    }

    fn edge(kind: EdgeKind, source: &str, target: &str) -> EdgeId {
        EdgeId::new(kind, node(source), node(target))
    }

    fn point(x: i32, y: i32) -> Point {
        Point { x, y }
    }

    fn route(offset: i32) -> BezierRoute {
        BezierRoute {
            source: point(offset, offset + 1),
            control_1: point(offset + 2, offset + 3),
            control_2: point(offset + 4, offset + 5),
            target: point(offset + 6, offset + 7),
        }
    }

    fn placement(node_id: &str, x: i32, y: i32) -> NodePlacement {
        NodePlacement {
            node: node(node_id),
            point: point(x, y),
        }
    }

    fn routed(edge: EdgeId, offset: i32) -> RoutedEdge {
        RoutedEdge {
            edge,
            route: route(offset),
        }
    }

    fn fixture_snapshot() -> Snapshot {
        let primary_ab = edge(EdgeKind::Primary, "a", "b");
        let primary_bc = edge(EdgeKind::Primary, "b", "c");
        let merge_ac = edge(EdgeKind::Merge, "a", "c");
        let shadow_ac = edge(EdgeKind::Shadow, "a", "c");
        Snapshot {
            format_version: FORMAT_VERSION,
            revision: Revision::new(1),
            source_version: SourceVersion::new(10),
            topology: TopologySnapshot {
                nodes: vec![node("c"), node("a"), node("b")],
                edges: vec![
                    shadow_ac.clone(),
                    primary_bc.clone(),
                    merge_ac.clone(),
                    primary_ab.clone(),
                ],
            },
            layouts: LayoutSnapshots {
                anchors: LayoutSnapshot {
                    canvas: Canvas {
                        width: 480,
                        height: 240,
                    },
                    nodes: vec![placement("c", 320, 80), placement("a", 80, 80)],
                    edges: vec![routed(shadow_ac, 400), routed(merge_ac.clone(), 300)],
                },
                all: LayoutSnapshot {
                    canvas: Canvas {
                        width: 640,
                        height: 360,
                    },
                    nodes: vec![
                        placement("c", 360, 120),
                        placement("a", 80, 120),
                        placement("b", 220, 120),
                    ],
                    edges: vec![
                        routed(primary_bc, 200),
                        routed(merge_ac, 100),
                        routed(primary_ab, 0),
                    ],
                },
            },
        }
    }

    fn empty_snapshot() -> Snapshot {
        Snapshot {
            format_version: FORMAT_VERSION,
            revision: Revision::new(0),
            source_version: SourceVersion::new(0),
            topology: TopologySnapshot {
                nodes: Vec::new(),
                edges: Vec::new(),
            },
            layouts: LayoutSnapshots {
                anchors: empty_layout_snapshot(),
                all: empty_layout_snapshot(),
            },
        }
    }

    fn empty_layout_snapshot() -> LayoutSnapshot {
        LayoutSnapshot {
            canvas: Canvas {
                width: 1,
                height: 1,
            },
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    fn patch(base: u64, revision: u64, source_version: u64) -> Patch {
        Patch {
            format_version: FORMAT_VERSION,
            base_revision: Revision::new(base),
            revision: Revision::new(revision),
            source_version: SourceVersion::new(source_version),
            topology: TopologyPatch::default(),
            layouts: LayoutPatches::default(),
        }
    }

    #[test]
    fn snapshot_round_trip_is_canonical_and_stable() {
        let graph = Graph::from_snapshot(fixture_snapshot()).unwrap();
        let snapshot = graph.snapshot();

        assert_eq!(
            snapshot.topology.nodes,
            vec![node("a"), node("b"), node("c")]
        );
        assert_eq!(
            snapshot.topology.edges,
            vec![
                edge(EdgeKind::Primary, "a", "b"),
                edge(EdgeKind::Primary, "b", "c"),
                edge(EdgeKind::Merge, "a", "c"),
                edge(EdgeKind::Shadow, "a", "c"),
            ]
        );
        assert_eq!(
            snapshot.layouts.all.nodes,
            vec![
                placement("a", 80, 120),
                placement("b", 220, 120),
                placement("c", 360, 120),
            ]
        );

        let json = serde_json::to_string(&snapshot).unwrap();
        let decoded = serde_json::from_str::<Snapshot>(&json).unwrap();
        let restored = Graph::from_snapshot(decoded).unwrap();

        assert_eq!(graph, restored);
        assert_eq!(json, serde_json::to_string(&restored.snapshot()).unwrap());
        assert!(json.contains(r#""format_version":1"#));
        assert!(json.contains(r#""primary_parent""#));
    }

    #[test]
    fn empty_graph_is_valid() {
        let graph = Graph::from_snapshot(empty_snapshot()).unwrap();

        assert_eq!(graph.revision(), Revision::new(0));
        assert_eq!(graph.source_version(), SourceVersion::new(0));
        assert_eq!(graph.topology().nodes().count(), 0);
        assert_eq!(graph.topology().edges().count(), 0);
        assert_eq!(graph.layout(LayoutKind::Anchors).nodes().count(), 0);
        assert_eq!(graph.layout(LayoutKind::All).edges().count(), 0);
    }

    #[test]
    fn layouts_share_topology_with_independent_routes() {
        let graph = Graph::from_snapshot(fixture_snapshot()).unwrap();
        let merge = edge(EdgeKind::Merge, "a", "c");
        let shadow = edge(EdgeKind::Shadow, "a", "c");

        assert_eq!(
            graph.layout(LayoutKind::Anchors).edge(&merge),
            Some(route(300))
        );
        assert_eq!(graph.layout(LayoutKind::All).edge(&merge), Some(route(100)));
        assert_eq!(
            graph.layout(LayoutKind::Anchors).edge(&shadow),
            Some(route(400))
        );
        assert_eq!(graph.layout(LayoutKind::All).edge(&shadow), None);
    }

    #[test]
    fn patch_atomically_adds_updates_and_removes_graph_items() {
        let mut graph = Graph::from_snapshot(fixture_snapshot()).unwrap();
        let primary_ab = edge(EdgeKind::Primary, "a", "b");
        let primary_bc = edge(EdgeKind::Primary, "b", "c");
        let primary_cd = edge(EdgeKind::Primary, "c", "d");
        let merge_ac = edge(EdgeKind::Merge, "a", "c");
        let mut update = patch(1, 2, 11);
        update.topology = TopologyPatch {
            add_nodes: vec![node("d")],
            remove_nodes: vec![node("b")],
            add_edges: vec![primary_cd.clone()],
            remove_edges: vec![primary_ab.clone(), primary_bc.clone()],
        };
        update.layouts.all = LayoutPatch {
            canvas: Some(Canvas {
                width: 720,
                height: 400,
            }),
            upsert_nodes: vec![placement("c", 340, 140), placement("d", 500, 140)],
            remove_nodes: vec![node("b")],
            upsert_edges: vec![
                routed(merge_ac.clone(), 510),
                routed(primary_cd.clone(), 520),
            ],
            remove_edges: vec![primary_ab, primary_bc],
        };
        update.layouts.anchors = LayoutPatch {
            canvas: None,
            upsert_nodes: vec![placement("c", 300, 90)],
            remove_nodes: Vec::new(),
            upsert_edges: vec![routed(merge_ac.clone(), 530)],
            remove_edges: Vec::new(),
        };

        graph.apply_patch(update).unwrap();

        assert_eq!(graph.revision(), Revision::new(2));
        assert_eq!(graph.source_version(), SourceVersion::new(11));
        assert_eq!(
            graph.topology().nodes().cloned().collect::<Vec<_>>(),
            vec![node("a"), node("c"), node("d")]
        );
        assert_eq!(
            graph.layout(LayoutKind::All).node(&node("c")),
            Some(point(340, 140))
        );
        assert_eq!(
            graph.layout(LayoutKind::All).node(&node("d")),
            Some(point(500, 140))
        );
        assert_eq!(graph.layout(LayoutKind::All).node(&node("b")), None);
        assert_eq!(
            graph.layout(LayoutKind::All).edge(&primary_cd),
            Some(route(520))
        );
        assert_eq!(
            graph.layout(LayoutKind::Anchors).edge(&merge_ac),
            Some(route(530))
        );
        assert_eq!(
            graph.layout(LayoutKind::All).canvas(),
            Canvas {
                width: 720,
                height: 400,
            }
        );
    }

    #[test]
    fn relayout_can_keep_the_same_source_version() {
        let mut graph = Graph::from_snapshot(fixture_snapshot()).unwrap();
        let merge = edge(EdgeKind::Merge, "a", "c");
        let mut relayout = patch(1, 2, 10);
        relayout.layouts.all.upsert_nodes = vec![placement("c", 400, 180)];
        relayout.layouts.all.upsert_edges = vec![routed(merge.clone(), 600)];

        graph.apply_patch(relayout).unwrap();

        assert_eq!(graph.source_version(), SourceVersion::new(10));
        assert_eq!(graph.revision(), Revision::new(2));
        assert_eq!(
            graph.layout(LayoutKind::All).node(&node("c")),
            Some(point(400, 180))
        );
        assert_eq!(graph.layout(LayoutKind::All).edge(&merge), Some(route(600)));
    }

    #[test]
    fn failed_patch_does_not_partially_mutate_graph() {
        let mut graph = Graph::from_snapshot(fixture_snapshot()).unwrap();
        let original = graph.clone();
        let mut invalid = patch(1, 2, 11);
        invalid.topology.remove_nodes = vec![node("b")];

        let error = graph.apply_patch(invalid).unwrap_err();

        assert!(matches!(error, Error::MissingEdgeEndpoint { .. }));
        assert_eq!(graph, original);
    }

    #[test]
    fn patch_rejects_invalid_format_and_versions_without_mutation() {
        let graph = Graph::from_snapshot(fixture_snapshot()).unwrap();

        let mut cases = Vec::new();
        let mut unsupported = patch(1, 2, 10);
        unsupported.format_version = FORMAT_VERSION + 1;
        cases.push(unsupported);
        cases.push(patch(0, 2, 10));
        cases.push(patch(1, 1, 10));
        cases.push(patch(1, 2, 9));

        for invalid in cases {
            let mut candidate = graph.clone();
            assert!(candidate.apply_patch(invalid).is_err());
            assert_eq!(candidate, graph);
        }
    }

    #[test]
    fn snapshot_rejects_empty_duplicate_and_dangling_ids() {
        let mut empty = fixture_snapshot();
        empty.topology.nodes[0] = NodeId(String::new());
        assert!(matches!(
            Graph::from_snapshot(empty),
            Err(Error::EmptyNodeId { .. })
        ));

        let mut duplicate = fixture_snapshot();
        duplicate.topology.nodes.push(node("a"));
        assert!(matches!(
            Graph::from_snapshot(duplicate),
            Err(Error::DuplicateItem { .. })
        ));

        let mut dangling = fixture_snapshot();
        dangling
            .topology
            .edges
            .push(edge(EdgeKind::Primary, "c", "missing"));
        assert!(matches!(
            Graph::from_snapshot(dangling),
            Err(Error::MissingEdgeEndpoint { .. })
        ));
    }

    #[test]
    fn snapshot_rejects_invalid_layout_membership_and_canvas() {
        let mut invalid_canvas = fixture_snapshot();
        invalid_canvas.layouts.anchors.canvas.width = 0;
        assert!(matches!(
            Graph::from_snapshot(invalid_canvas),
            Err(Error::InvalidCanvas {
                layout: LayoutKind::Anchors,
                ..
            })
        ));

        let mut anchors_outside_all = fixture_snapshot();
        anchors_outside_all
            .layouts
            .all
            .nodes
            .retain(|item| item.node != node("a"));
        anchors_outside_all
            .layouts
            .all
            .edges
            .retain(|item| item.edge.source != node("a"));
        assert!(matches!(
            Graph::from_snapshot(anchors_outside_all),
            Err(Error::AnchorNodeMissingFromAll { node: missing }) if missing == node("a")
        ));

        let mut hidden_endpoint = fixture_snapshot();
        hidden_endpoint
            .layouts
            .anchors
            .nodes
            .retain(|item| item.node != node("c"));
        assert!(matches!(
            Graph::from_snapshot(hidden_endpoint),
            Err(Error::LayoutEdgeEndpointMissing {
                layout: LayoutKind::Anchors,
                ..
            })
        ));
    }

    #[test]
    fn snapshot_rejects_cycles_and_unused_topology_edges() {
        let mut cyclic = empty_snapshot();
        let forward = edge(EdgeKind::Primary, "a", "b");
        let backward = edge(EdgeKind::Merge, "b", "a");
        cyclic.topology.nodes = vec![node("a"), node("b")];
        cyclic.topology.edges = vec![forward.clone(), backward.clone()];
        cyclic.layouts.all.nodes = vec![placement("a", 10, 10), placement("b", 20, 20)];
        cyclic.layouts.all.edges = vec![routed(forward, 0), routed(backward, 10)];
        assert!(matches!(
            Graph::from_snapshot(cyclic),
            Err(Error::Cycle {
                layout: LayoutKind::All,
                ..
            })
        ));

        let mut unused = fixture_snapshot();
        unused.topology.edges.push(edge(EdgeKind::Shadow, "b", "c"));
        assert!(matches!(
            Graph::from_snapshot(unused),
            Err(Error::UnusedTopologyEdge { .. })
        ));
    }

    #[test]
    fn patch_rejects_duplicate_conflicting_and_missing_operations() {
        let graph = Graph::from_snapshot(fixture_snapshot()).unwrap();

        let mut duplicate = patch(1, 2, 10);
        duplicate.topology.add_nodes = vec![node("d"), node("d")];
        assert_failed_without_mutation(&graph, duplicate, |error| {
            matches!(error, Error::DuplicateItem { .. })
        });

        let mut conflicting = patch(1, 2, 10);
        conflicting.topology.add_nodes = vec![node("d")];
        conflicting.topology.remove_nodes = vec![node("d")];
        assert_failed_without_mutation(&graph, conflicting, |error| {
            matches!(error, Error::ConflictingPatchItem { .. })
        });

        let mut missing = patch(1, 2, 10);
        missing.layouts.all.remove_nodes = vec![node("missing")];
        assert_failed_without_mutation(&graph, missing, |error| {
            matches!(error, Error::MissingPatchItem { .. })
        });

        let mut existing = patch(1, 2, 10);
        existing.topology.add_nodes = vec![node("a")];
        assert_failed_without_mutation(&graph, existing, |error| {
            matches!(error, Error::ExistingPatchItem { .. })
        });

        let mut invalid_canvas = patch(1, 2, 10);
        invalid_canvas.layouts.anchors.canvas = Some(Canvas {
            width: 0,
            height: 100,
        });
        assert_failed_without_mutation(&graph, invalid_canvas, |error| {
            matches!(
                error,
                Error::InvalidCanvas {
                    layout: LayoutKind::Anchors,
                    ..
                }
            )
        });
    }

    #[test]
    fn unknown_snapshot_format_is_rejected() {
        let mut snapshot = empty_snapshot();
        snapshot.format_version = FORMAT_VERSION + 1;

        assert_eq!(
            Graph::from_snapshot(snapshot),
            Err(Error::UnsupportedFormat {
                actual: FORMAT_VERSION + 1,
            })
        );
    }

    fn assert_failed_without_mutation(
        graph: &Graph,
        patch: Patch,
        matches_error: impl FnOnce(&Error) -> bool,
    ) {
        let mut candidate = graph.clone();
        let error = candidate.apply_patch(patch).unwrap_err();
        assert!(matches_error(&error), "unexpected error: {error}");
        assert_eq!(&candidate, graph);
    }
}
