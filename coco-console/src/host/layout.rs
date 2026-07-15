use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use snafu::prelude::*;

use crate::api::{
    GraphBezierRoute, GraphCanvas, GraphViewport, GraphViewportDiffResponse, GraphViewportEdge,
    GraphViewportEdgeKind, GraphViewportItemKind, GraphViewportItems, GraphViewportNode,
    GraphViewportRemovedItem, GraphViewportResponse, Point,
};
use crate::graph::{GraphEdgeKind, GraphNode, GraphSnapshot, node_target_id};
use crate::host::api::{GraphViewportDiffRequest, GraphViewportKnownItems, GraphViewportRequest};

pub const NODE_RADIUS: i32 = 18;
pub const GRAPH_PADDING: i32 = 56;
pub const GRAPH_RANK_STEP: i32 = 112;
pub const GRAPH_ROW_STEP: i32 = 72;

const NODE_BOUNDS_HALF_WIDTH: i32 = 64;
const NODE_BOUNDS_TOP: i32 = 24;
const NODE_BOUNDS_BOTTOM: i32 = 52;
const EDGE_SOURCE_PADDING: i32 = 2;
const EDGE_TARGET_PADDING: i32 = 6;
const EDGE_PORT_RANGE: i32 = 12;
const EDGE_CONTROL_RATIO_PERCENT: i32 = 45;
const EDGE_MIN_CONTROL_DISTANCE: i32 = 24;

#[derive(Debug, Snafu)]
pub enum GraphLayoutError {
    #[snafu(display("Graph contains duplicate node {node_id}"))]
    DuplicateNode { node_id: String },

    #[snafu(display(
        "Graph edge {source_id} -> {target_id} references missing node {missing_node_id}"
    ))]
    MissingEdgeNode {
        source_id: String,
        target_id: String,
        missing_node_id: String,
    },

    #[snafu(display("Graph contains a cycle involving node {node_id}"))]
    Cycle { node_id: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayoutHint {
    pub rank: usize,
    pub order: usize,
    pub point: Point,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLayout {
    pub nodes: Vec<GraphLayoutNode>,
    pub edges: Vec<GraphLayoutEdge>,
    pub width: i32,
    pub height: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLayoutNode {
    pub node_id: String,
    pub node_target: String,
    pub short_id: String,
    pub kind: String,
    pub summary: String,
    pub labels: Vec<String>,
    pub created_at: String,
    pub created_at_ns: i128,
    pub rank: usize,
    pub order: usize,
    pub point: Point,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLayoutEdge {
    pub source_node_id: String,
    pub target_node_id: String,
    pub kind: GraphLayoutEdgeKind,
    pub route: GraphBezierRoute,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GraphLayoutEdgeKind {
    Primary,
    Merge,
    Shadow,
}

impl GraphLayoutEdgeKind {
    pub fn key_part(self) -> &'static str {
        GraphViewportEdgeKind::from(self).key_part()
    }
}

impl From<GraphEdgeKind> for GraphLayoutEdgeKind {
    fn from(value: GraphEdgeKind) -> Self {
        match value {
            GraphEdgeKind::Primary => Self::Primary,
            GraphEdgeKind::Merge => Self::Merge,
            GraphEdgeKind::Shadow => Self::Shadow,
        }
    }
}

impl From<GraphLayoutEdgeKind> for GraphViewportEdgeKind {
    fn from(value: GraphLayoutEdgeKind) -> Self {
        match value {
            GraphLayoutEdgeKind::Primary => Self::Primary,
            GraphLayoutEdgeKind::Merge => Self::Merge,
            GraphLayoutEdgeKind::Shadow => Self::Shadow,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeIdentity {
    kind: GraphLayoutEdgeKind,
    source: String,
    target: String,
}

#[derive(Debug)]
struct PreparedGraph<'a> {
    nodes: BTreeMap<&'a str, &'a GraphNode>,
    edges: Vec<EdgeIdentity>,
    incoming: BTreeMap<String, Vec<String>>,
    outgoing: BTreeMap<String, Vec<String>>,
    ranks: BTreeMap<String, usize>,
}

#[derive(Debug)]
struct Component {
    nodes: Vec<String>,
    priority: ComponentPriority,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ComponentPriority {
    class: u8,
    branch: String,
    created_at_ns: i128,
    node_id: String,
}

#[derive(Debug, Clone, Copy)]
struct ViewportBounds {
    left: i32,
    top: i32,
    right: i32,
    bottom: i32,
}

impl ViewportBounds {
    fn from_request(request: GraphViewportRequest) -> Self {
        Self {
            left: request.x.saturating_sub(request.overscan),
            top: request.y.saturating_sub(request.overscan),
            right: request
                .x
                .saturating_add(request.width)
                .saturating_add(request.overscan),
            bottom: request
                .y
                .saturating_add(request.height)
                .saturating_add(request.overscan),
        }
    }

    fn intersects(self, left: i32, top: i32, right: i32, bottom: i32) -> bool {
        left <= self.right && right >= self.left && top <= self.bottom && bottom >= self.top
    }
}

pub fn layout_graph(snapshot: &GraphSnapshot) -> GraphLayout {
    try_layout_graph(snapshot).expect("graph snapshots must contain a DAG with valid edges")
}

pub fn try_layout_graph(snapshot: &GraphSnapshot) -> Result<GraphLayout, GraphLayoutError> {
    try_layout_graph_with_hints(snapshot, &BTreeMap::new(), None)
}

pub fn try_layout_graph_with_hints(
    snapshot: &GraphSnapshot,
    hints: &BTreeMap<String, LayoutHint>,
    reflow_ranks: Option<&BTreeSet<usize>>,
) -> Result<GraphLayout, GraphLayoutError> {
    let prepared = prepare_graph(snapshot)?;
    if prepared.nodes.is_empty() {
        return Ok(GraphLayout {
            nodes: Vec::new(),
            edges: Vec::new(),
            width: GRAPH_PADDING * 2,
            height: GRAPH_PADDING * 2,
        });
    }

    let components = ordered_components(snapshot, &prepared);
    let mut packed_nodes_by_rank = BTreeMap::<usize, Vec<String>>::new();
    for component in &components {
        let mut component_nodes = component_nodes_by_rank(component, &prepared.ranks, hints);
        reduce_crossings(&mut component_nodes, &prepared, hints, reflow_ranks);
        for (rank, node_ids) in component_nodes {
            packed_nodes_by_rank
                .entry(rank)
                .or_default()
                .extend(node_ids);
        }
    }

    let mut point_by_node = BTreeMap::<String, Point>::new();
    for (rank, node_ids) in &packed_nodes_by_rank {
        for (index, node_id) in node_ids.iter().enumerate() {
            let packed = Point {
                x: GRAPH_PADDING + *rank as i32 * GRAPH_RANK_STEP,
                y: GRAPH_PADDING + index as i32 * GRAPH_ROW_STEP,
            };
            let point = hints
                .get(node_id)
                .filter(|hint| {
                    hint.rank == *rank && reflow_ranks.is_some_and(|ranks| !ranks.contains(rank))
                })
                .map_or(packed, |hint| hint.point);
            point_by_node.insert(node_id.clone(), point);
        }
    }

    let order_by_node = packed_order_by_node(&prepared.ranks, &point_by_node);
    let mut nodes = prepared
        .nodes
        .values()
        .map(|node| GraphLayoutNode {
            node_id: node.id.clone(),
            node_target: node_target_id(&node.id),
            short_id: node.short_id.clone(),
            kind: node.kind.clone(),
            summary: node.summary.clone(),
            labels: node.labels.clone(),
            created_at: node.created_at.clone(),
            created_at_ns: node.created_at_ns,
            rank: prepared.ranks[&node.id],
            order: order_by_node[&node.id],
            point: point_by_node[&node.id],
        })
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| {
        left.rank
            .cmp(&right.rank)
            .then_with(|| left.order.cmp(&right.order))
            .then_with(|| left.node_id.cmp(&right.node_id))
    });

    let edges = route_edges(&prepared.edges, &point_by_node, &prepared.ranks);
    let max_x = nodes
        .iter()
        .map(|node| node.point.x)
        .max()
        .unwrap_or(GRAPH_PADDING);
    let max_y = nodes
        .iter()
        .map(|node| node.point.y)
        .max()
        .unwrap_or(GRAPH_PADDING);
    Ok(GraphLayout {
        nodes,
        edges,
        width: max_x.saturating_add(GRAPH_PADDING),
        height: max_y.saturating_add(GRAPH_PADDING),
    })
}

pub fn try_graph_ranks(
    snapshot: &GraphSnapshot,
) -> Result<BTreeMap<String, usize>, GraphLayoutError> {
    Ok(prepare_graph(snapshot)?.ranks)
}

fn prepare_graph(snapshot: &GraphSnapshot) -> Result<PreparedGraph<'_>, GraphLayoutError> {
    let mut nodes = BTreeMap::new();
    for node in &snapshot.nodes {
        if nodes.insert(node.id.as_str(), node).is_some() {
            return DuplicateNodeSnafu {
                node_id: node.id.clone(),
            }
            .fail();
        }
    }

    let mut identities = BTreeSet::new();
    for edge in &snapshot.edges {
        for node_id in [&edge.source, &edge.target] {
            if !nodes.contains_key(node_id.as_str()) {
                return MissingEdgeNodeSnafu {
                    source_id: edge.source.clone(),
                    target_id: edge.target.clone(),
                    missing_node_id: node_id.clone(),
                }
                .fail();
            }
        }
        identities.insert(EdgeIdentity {
            kind: edge.kind.into(),
            source: edge.source.clone(),
            target: edge.target.clone(),
        });
    }
    let edges = identities.into_iter().collect::<Vec<_>>();

    let mut incoming = nodes
        .keys()
        .map(|node_id| ((*node_id).to_owned(), Vec::new()))
        .collect::<BTreeMap<_, _>>();
    let mut outgoing = incoming.clone();
    let mut indegree = nodes
        .keys()
        .map(|node_id| ((*node_id).to_owned(), 0_usize))
        .collect::<BTreeMap<_, _>>();
    for edge in &edges {
        incoming
            .get_mut(&edge.target)
            .expect("validated target should exist")
            .push(edge.source.clone());
        outgoing
            .get_mut(&edge.source)
            .expect("validated source should exist")
            .push(edge.target.clone());
        *indegree
            .get_mut(&edge.target)
            .expect("validated target should exist") += 1;
    }
    for neighbors in incoming.values_mut().chain(outgoing.values_mut()) {
        neighbors.sort();
        neighbors.dedup();
    }

    let mut ready = BTreeSet::<(i128, String)>::new();
    for (node_id, degree) in &indegree {
        if *degree == 0 {
            let node = nodes[node_id.as_str()];
            ready.insert((node.created_at_ns, node_id.clone()));
        }
    }
    let mut ranks = nodes
        .keys()
        .map(|node_id| ((*node_id).to_owned(), 0_usize))
        .collect::<BTreeMap<_, _>>();
    let mut visited = 0_usize;
    while let Some((_, node_id)) = ready.pop_first() {
        visited += 1;
        let source_rank = ranks[&node_id];
        for target_id in &outgoing[&node_id] {
            let target_rank = ranks
                .get_mut(target_id)
                .expect("validated target should have a rank");
            *target_rank = (*target_rank).max(source_rank + 1);
            let target_indegree = indegree
                .get_mut(target_id)
                .expect("validated target should have indegree");
            *target_indegree -= 1;
            if *target_indegree == 0 {
                let node = nodes[target_id.as_str()];
                ready.insert((node.created_at_ns, target_id.clone()));
            }
        }
    }
    if visited != nodes.len() {
        let node_id = indegree
            .into_iter()
            .find_map(|(node_id, degree)| (degree > 0).then_some(node_id))
            .expect("a cycle should leave an incoming edge");
        return CycleSnafu { node_id }.fail();
    }

    Ok(PreparedGraph {
        nodes,
        edges,
        incoming,
        outgoing,
        ranks,
    })
}

fn ordered_components(snapshot: &GraphSnapshot, graph: &PreparedGraph<'_>) -> Vec<Component> {
    let mut undirected = graph
        .nodes
        .keys()
        .map(|node_id| ((*node_id).to_owned(), Vec::<String>::new()))
        .collect::<BTreeMap<_, _>>();
    for edge in &graph.edges {
        undirected
            .get_mut(&edge.source)
            .expect("validated source should exist")
            .push(edge.target.clone());
        undirected
            .get_mut(&edge.target)
            .expect("validated target should exist")
            .push(edge.source.clone());
    }

    let mut components = Vec::new();
    let mut visited = BTreeSet::new();
    for start in graph.nodes.keys() {
        if !visited.insert((*start).to_owned()) {
            continue;
        }
        let mut pending = VecDeque::from([(*start).to_owned()]);
        let mut component_nodes = Vec::new();
        while let Some(node_id) = pending.pop_front() {
            component_nodes.push(node_id.clone());
            for neighbor in &undirected[&node_id] {
                if visited.insert(neighbor.clone()) {
                    pending.push_back(neighbor.clone());
                }
            }
        }
        component_nodes.sort();
        let priority = component_priority(snapshot, graph, &component_nodes);
        components.push(Component {
            nodes: component_nodes,
            priority,
        });
    }
    components.sort_by(|left, right| left.priority.cmp(&right.priority));
    components
}

fn component_priority(
    snapshot: &GraphSnapshot,
    graph: &PreparedGraph<'_>,
    component: &[String],
) -> ComponentPriority {
    let node_ids = component
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut branch_names = snapshot
        .branches
        .iter()
        .filter(|branch| {
            branch
                .visible_head_id
                .as_deref()
                .is_some_and(|head_id| node_ids.contains(head_id))
        })
        .map(|branch| branch.name.as_str())
        .collect::<Vec<_>>();
    branch_names.sort();
    let earliest = component
        .iter()
        .map(|node_id| graph.nodes[node_id.as_str()])
        .min_by(|left, right| {
            left.created_at_ns
                .cmp(&right.created_at_ns)
                .then_with(|| left.id.cmp(&right.id))
        })
        .expect("components should not be empty");
    if branch_names.contains(&"main") {
        ComponentPriority {
            class: 0,
            branch: String::new(),
            created_at_ns: earliest.created_at_ns,
            node_id: earliest.id.clone(),
        }
    } else if let Some(branch) = branch_names.first() {
        ComponentPriority {
            class: 1,
            branch: (*branch).to_owned(),
            created_at_ns: earliest.created_at_ns,
            node_id: earliest.id.clone(),
        }
    } else {
        ComponentPriority {
            class: 2,
            branch: String::new(),
            created_at_ns: earliest.created_at_ns,
            node_id: earliest.id.clone(),
        }
    }
}

fn component_nodes_by_rank(
    component: &Component,
    ranks: &BTreeMap<String, usize>,
    hints: &BTreeMap<String, LayoutHint>,
) -> BTreeMap<usize, Vec<String>> {
    let mut nodes_by_rank = BTreeMap::<usize, Vec<String>>::new();
    for node_id in &component.nodes {
        nodes_by_rank
            .entry(ranks[node_id])
            .or_default()
            .push(node_id.clone());
    }
    for (rank, node_ids) in &mut nodes_by_rank {
        node_ids.sort_by(|left, right| {
            hint_order(*rank, left, hints)
                .cmp(&hint_order(*rank, right, hints))
                .then_with(|| left.cmp(right))
        });
    }
    nodes_by_rank
}

fn hint_order(
    rank: usize,
    node_id: &str,
    hints: &BTreeMap<String, LayoutHint>,
) -> (u8, usize, i32) {
    hints
        .get(node_id)
        .map_or((1, usize::MAX, i32::MAX), |hint| {
            if hint.rank == rank {
                (0, hint.order, hint.point.y)
            } else {
                (1, usize::MAX, i32::MAX)
            }
        })
}

fn reduce_crossings(
    nodes_by_rank: &mut BTreeMap<usize, Vec<String>>,
    graph: &PreparedGraph<'_>,
    hints: &BTreeMap<String, LayoutHint>,
    reflow_ranks: Option<&BTreeSet<usize>>,
) {
    for _ in 0..2 {
        let ranks = nodes_by_rank.keys().copied().collect::<Vec<_>>();
        for rank in ranks
            .iter()
            .copied()
            .filter(|rank| reflow_ranks.is_none_or(|reflow| reflow.contains(rank)))
        {
            sweep_rank(nodes_by_rank, rank, &graph.incoming, graph, hints);
        }
        for rank in ranks
            .into_iter()
            .rev()
            .filter(|rank| reflow_ranks.is_none_or(|reflow| reflow.contains(rank)))
        {
            sweep_rank(nodes_by_rank, rank, &graph.outgoing, graph, hints);
        }
    }
}

fn sweep_rank(
    nodes_by_rank: &mut BTreeMap<usize, Vec<String>>,
    rank: usize,
    neighbors: &BTreeMap<String, Vec<String>>,
    graph: &PreparedGraph<'_>,
    hints: &BTreeMap<String, LayoutHint>,
) {
    let positions = rank_positions(nodes_by_rank);
    let Some(node_ids) = nodes_by_rank.get_mut(&rank) else {
        return;
    };
    let current_positions = node_ids
        .iter()
        .enumerate()
        .map(|(index, node_id)| (node_id.clone(), index))
        .collect::<BTreeMap<_, _>>();
    node_ids.sort_by(|left, right| {
        barycenter(left, neighbors, &positions)
            .partial_cmp(&barycenter(right, neighbors, &positions))
            .unwrap_or(Ordering::Equal)
            .then_with(|| hint_order(rank, left, hints).cmp(&hint_order(rank, right, hints)))
            .then_with(|| current_positions[left].cmp(&current_positions[right]))
            .then_with(|| {
                let left_node = graph.nodes[left.as_str()];
                let right_node = graph.nodes[right.as_str()];
                left_node
                    .created_at_ns
                    .cmp(&right_node.created_at_ns)
                    .then_with(|| left.cmp(right))
            })
    });
}

fn rank_positions(nodes_by_rank: &BTreeMap<usize, Vec<String>>) -> BTreeMap<String, f64> {
    nodes_by_rank
        .values()
        .flat_map(|node_ids| {
            node_ids
                .iter()
                .enumerate()
                .map(|(index, node_id)| (node_id.clone(), index as f64))
        })
        .collect()
}

fn barycenter(
    node_id: &str,
    neighbors: &BTreeMap<String, Vec<String>>,
    positions: &BTreeMap<String, f64>,
) -> f64 {
    let values = neighbors[node_id]
        .iter()
        .filter_map(|neighbor| positions.get(neighbor))
        .copied()
        .collect::<Vec<_>>();
    if values.is_empty() {
        positions.get(node_id).copied().unwrap_or_default()
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn packed_order_by_node(
    ranks: &BTreeMap<String, usize>,
    points: &BTreeMap<String, Point>,
) -> BTreeMap<String, usize> {
    let mut nodes_by_rank = BTreeMap::<usize, Vec<String>>::new();
    for (node_id, rank) in ranks {
        nodes_by_rank
            .entry(*rank)
            .or_default()
            .push(node_id.clone());
    }
    let mut order = BTreeMap::new();
    for node_ids in nodes_by_rank.values_mut() {
        node_ids.sort_by(|left, right| {
            points[left]
                .y
                .cmp(&points[right].y)
                .then_with(|| left.cmp(right))
        });
        for (index, node_id) in node_ids.iter().enumerate() {
            order.insert(node_id.clone(), index);
        }
    }
    order
}

fn route_edges(
    edges: &[EdgeIdentity],
    points: &BTreeMap<String, Point>,
    ranks: &BTreeMap<String, usize>,
) -> Vec<GraphLayoutEdge> {
    let outgoing = grouped_edge_ports(
        edges,
        |edge| &edge.source,
        |edge| {
            (
                ranks[&edge.target],
                points[&edge.target].y,
                edge.target.clone(),
                edge.kind,
            )
        },
    );
    let incoming = grouped_edge_ports(
        edges,
        |edge| &edge.target,
        |edge| {
            (
                ranks[&edge.source],
                points[&edge.source].y,
                edge.source.clone(),
                edge.kind,
            )
        },
    );
    edges
        .iter()
        .map(|edge| {
            let (source_index, source_count) = outgoing[edge];
            let (target_index, target_count) = incoming[edge];
            let source_center = points[&edge.source];
            let target_center = points[&edge.target];
            let source = Point {
                x: source_center.x + NODE_RADIUS + EDGE_SOURCE_PADDING,
                y: source_center.y + port_offset(source_index, source_count),
            };
            let target = Point {
                x: target_center.x - NODE_RADIUS - EDGE_TARGET_PADDING,
                y: target_center.y + port_offset(target_index, target_count),
            };
            let horizontal_distance = (target.x - source.x).max(EDGE_MIN_CONTROL_DISTANCE);
            let control_distance = (horizontal_distance * EDGE_CONTROL_RATIO_PERCENT / 100)
                .max(EDGE_MIN_CONTROL_DISTANCE);
            let control_1 = Point {
                x: (source.x + control_distance).min(target.x - 8),
                y: source.y,
            };
            let control_2 = Point {
                x: (target.x - control_distance).max(source.x + 8),
                y: target.y,
            };
            GraphLayoutEdge {
                source_node_id: edge.source.clone(),
                target_node_id: edge.target.clone(),
                kind: edge.kind,
                route: GraphBezierRoute {
                    source,
                    control_1,
                    control_2,
                    target,
                },
            }
        })
        .collect()
}

fn grouped_edge_ports<K, F>(
    edges: &[EdgeIdentity],
    group: impl Fn(&EdgeIdentity) -> &String,
    sort_key: F,
) -> BTreeMap<EdgeIdentity, (usize, usize)>
where
    K: Ord,
    F: Fn(&EdgeIdentity) -> K,
{
    let mut groups = BTreeMap::<String, Vec<&EdgeIdentity>>::new();
    for edge in edges {
        groups.entry(group(edge).clone()).or_default().push(edge);
    }
    let mut ports = BTreeMap::new();
    for group_edges in groups.values_mut() {
        group_edges.sort_by_key(|edge| sort_key(edge));
        let count = group_edges.len();
        for (index, edge) in group_edges.iter().enumerate() {
            ports.insert((*edge).clone(), (index, count));
        }
    }
    ports
}

fn port_offset(index: usize, count: usize) -> i32 {
    if count <= 1 {
        return 0;
    }
    let numerator = index as i32 * EDGE_PORT_RANGE * 2;
    -EDGE_PORT_RANGE + numerator / (count as i32 - 1)
}

pub fn layout_graph_viewport(
    snapshot: &GraphSnapshot,
    request: GraphViewportRequest,
) -> GraphViewportResponse {
    viewport_from_layout(snapshot.version, &layout_graph(snapshot), request)
}

pub fn viewport_from_layout(
    version: u64,
    layout: &GraphLayout,
    request: GraphViewportRequest,
) -> GraphViewportResponse {
    let request = request.normalized();
    let bounds = ViewportBounds::from_request(request);
    let nodes = layout
        .nodes
        .iter()
        .filter(|node| node_intersects(bounds, node))
        .map(viewport_node)
        .collect();
    let edges = layout
        .edges
        .iter()
        .filter(|edge| edge_intersects(bounds, edge))
        .map(viewport_edge)
        .collect();
    GraphViewportResponse {
        version,
        canvas: GraphCanvas {
            width: layout.width,
            height: layout.height,
        },
        viewport: GraphViewport {
            x: request.x,
            y: request.y,
            width: request.width,
            height: request.height,
            overscan: request.overscan,
        },
        nodes,
        edges,
    }
}

pub fn viewport_node(node: &GraphLayoutNode) -> GraphViewportNode {
    GraphViewportNode {
        key: node_key(&node.node_id),
        id: node.node_id.clone(),
        node_target: node.node_target.clone(),
        short_id: node.short_id.clone(),
        kind: node.kind.clone(),
        summary: node.summary.clone(),
        labels: node.labels.clone(),
        x: node.point.x,
        y: node.point.y,
    }
}

pub fn viewport_edge(edge: &GraphLayoutEdge) -> GraphViewportEdge {
    GraphViewportEdge {
        key: edge_key(edge.kind, &edge.source_node_id, &edge.target_node_id),
        kind: edge.kind.into(),
        source_id: edge.source_node_id.clone(),
        target_id: edge.target_node_id.clone(),
        route: edge.route,
    }
}

pub fn node_key(node_id: &str) -> String {
    format!("node:{node_id}")
}

pub fn edge_key(kind: GraphLayoutEdgeKind, source_id: &str, target_id: &str) -> String {
    format!("edge:{}:{source_id}:{target_id}", kind.key_part())
}

pub fn node_bounds(node: &GraphLayoutNode) -> (i32, i32, i32, i32) {
    (
        node.point.x - NODE_BOUNDS_HALF_WIDTH,
        node.point.y - NODE_BOUNDS_TOP,
        node.point.x + NODE_BOUNDS_HALF_WIDTH,
        node.point.y + NODE_BOUNDS_BOTTOM,
    )
}

pub fn edge_bounds(edge: &GraphLayoutEdge) -> (i32, i32, i32, i32) {
    route_bounds(edge.route)
}

pub fn route_bounds(route: GraphBezierRoute) -> (i32, i32, i32, i32) {
    let points = [route.source, route.control_1, route.control_2, route.target];
    (
        points.iter().map(|point| point.x).min().unwrap_or_default(),
        points.iter().map(|point| point.y).min().unwrap_or_default(),
        points.iter().map(|point| point.x).max().unwrap_or_default(),
        points.iter().map(|point| point.y).max().unwrap_or_default(),
    )
}

fn node_intersects(bounds: ViewportBounds, node: &GraphLayoutNode) -> bool {
    let (left, top, right, bottom) = node_bounds(node);
    bounds.intersects(left, top, right, bottom)
}

fn edge_intersects(bounds: ViewportBounds, edge: &GraphLayoutEdge) -> bool {
    let (left, top, right, bottom) = edge_bounds(edge);
    bounds.intersects(left, top, right, bottom)
}

pub fn layout_graph_viewport_diff(
    snapshot: &GraphSnapshot,
    request: GraphViewportDiffRequest,
) -> GraphViewportDiffResponse {
    let layout = layout_graph(snapshot);
    let previous = viewport_from_layout(snapshot.version, &layout, request.previous);
    let current = viewport_from_layout(snapshot.version, &layout, request.current);
    diff_graph_viewport_responses(previous, current, request.known.as_ref())
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
    use crate::graph::{GraphBranch, GraphEdge, GraphMode};
    use coco_mem::SessionState;

    fn node(id: &str, created_at_ns: i128) -> GraphNode {
        GraphNode {
            id: id.to_owned(),
            short_id: id.to_owned(),
            kind: "text".to_owned(),
            role: "User".to_owned(),
            created_at: created_at_ns.to_string(),
            created_at_ns,
            content: String::new(),
            summary: id.to_owned(),
            labels: Vec::new(),
            provider_context_ids: Vec::new(),
        }
    }

    fn snapshot(nodes: Vec<GraphNode>, edges: Vec<GraphEdge>) -> GraphSnapshot {
        GraphSnapshot {
            version: 1,
            mode: GraphMode::All,
            root_id: "root".to_owned(),
            nodes,
            edges,
            branches: vec![GraphBranch {
                name: "main".to_owned(),
                head_id: "c".to_owned(),
                visible_head_id: Some("c".to_owned()),
                state: SessionState::Active,
            }],
            provider_contexts: Vec::new(),
        }
    }

    #[test]
    fn layered_layout_is_unique_and_compact() {
        let graph = snapshot(
            vec![node("a", 1), node("b", 2), node("c", 3), node("d", 4)],
            vec![
                GraphEdge {
                    source: "a".to_owned(),
                    target: "b".to_owned(),
                    kind: GraphEdgeKind::Primary,
                },
                GraphEdge {
                    source: "a".to_owned(),
                    target: "c".to_owned(),
                    kind: GraphEdgeKind::Primary,
                },
                GraphEdge {
                    source: "b".to_owned(),
                    target: "d".to_owned(),
                    kind: GraphEdgeKind::Merge,
                },
                GraphEdge {
                    source: "c".to_owned(),
                    target: "d".to_owned(),
                    kind: GraphEdgeKind::Shadow,
                },
            ],
        );
        let layout = try_layout_graph(&graph).unwrap();
        let repeated = try_layout_graph(&graph).unwrap();
        let ids = layout
            .nodes
            .iter()
            .map(|node| node.node_id.as_str())
            .collect::<BTreeSet<_>>();
        let node_keys = layout
            .nodes
            .iter()
            .map(|node| node_key(&node.node_id))
            .collect::<BTreeSet<_>>();
        let edge_keys = layout
            .edges
            .iter()
            .map(|edge| edge_key(edge.kind, &edge.source_node_id, &edge.target_node_id))
            .collect::<BTreeSet<_>>();

        assert_eq!(layout, repeated);
        assert_eq!(ids.len(), graph.nodes.len());
        assert_eq!(node_keys.len(), graph.nodes.len());
        assert_eq!(edge_keys.len(), graph.edges.len());
        assert_eq!(layout.nodes[0].point.x, GRAPH_PADDING);
        assert!(layout.edges.iter().all(|edge| {
            let source = layout
                .nodes
                .iter()
                .find(|node| node.node_id == edge.source_node_id)
                .unwrap();
            let target = layout
                .nodes
                .iter()
                .find(|node| node.node_id == edge.target_node_id)
                .unwrap();
            source.rank < target.rank
        }));
        assert!(
            layout
                .edges
                .iter()
                .any(|edge| edge.kind == GraphLayoutEdgeKind::Shadow)
        );
        let ranks = layout
            .nodes
            .iter()
            .map(|node| node.rank)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            ranks,
            (0..=*ranks.last().expect("layout should have a rank")).collect()
        );
        for rank in ranks {
            let mut rank_nodes = layout
                .nodes
                .iter()
                .filter(|node| node.rank == rank)
                .collect::<Vec<_>>();
            rank_nodes.sort_by_key(|node| node.point.y);
            assert!(
                rank_nodes
                    .iter()
                    .all(|node| { node.point.x == GRAPH_PADDING + rank as i32 * GRAPH_RANK_STEP })
            );
            assert!(
                rank_nodes
                    .windows(2)
                    .all(|nodes| { nodes[1].point.y - nodes[0].point.y == GRAPH_ROW_STEP })
            );
        }

        let fork_routes = layout
            .edges
            .iter()
            .filter(|edge| edge.source_node_id == "a")
            .map(|edge| edge.route.source.y)
            .collect::<BTreeSet<_>>();
        let merge_routes = layout
            .edges
            .iter()
            .filter(|edge| edge.target_node_id == "d")
            .map(|edge| edge.route.target.y)
            .collect::<BTreeSet<_>>();
        assert_eq!(fork_routes.len(), 2);
        assert_eq!(merge_routes.len(), 2);
    }

    #[test]
    fn disconnected_components_are_prioritized_and_each_rank_is_tightly_packed() {
        let nodes = [
            "main-a",
            "main-b",
            "main-head",
            "alpha-root",
            "alpha-head",
            "zeta-root",
            "zeta-head",
            "orphan-root",
            "orphan-head",
        ]
        .into_iter()
        .enumerate()
        .map(|(index, id)| node(id, index as i128))
        .collect();
        let edges = vec![
            GraphEdge {
                source: "main-a".to_owned(),
                target: "main-head".to_owned(),
                kind: GraphEdgeKind::Primary,
            },
            GraphEdge {
                source: "main-b".to_owned(),
                target: "main-head".to_owned(),
                kind: GraphEdgeKind::Merge,
            },
            GraphEdge {
                source: "alpha-root".to_owned(),
                target: "alpha-head".to_owned(),
                kind: GraphEdgeKind::Primary,
            },
            GraphEdge {
                source: "zeta-root".to_owned(),
                target: "zeta-head".to_owned(),
                kind: GraphEdgeKind::Primary,
            },
            GraphEdge {
                source: "orphan-root".to_owned(),
                target: "orphan-head".to_owned(),
                kind: GraphEdgeKind::Primary,
            },
        ];
        let graph = GraphSnapshot {
            version: 1,
            mode: GraphMode::All,
            root_id: "root".to_owned(),
            nodes,
            edges,
            branches: vec![
                GraphBranch {
                    name: "zeta".to_owned(),
                    head_id: "zeta-head".to_owned(),
                    visible_head_id: Some("zeta-head".to_owned()),
                    state: SessionState::Active,
                },
                GraphBranch {
                    name: "main".to_owned(),
                    head_id: "main-head".to_owned(),
                    visible_head_id: Some("main-head".to_owned()),
                    state: SessionState::Active,
                },
                GraphBranch {
                    name: "alpha".to_owned(),
                    head_id: "alpha-head".to_owned(),
                    visible_head_id: Some("alpha-head".to_owned()),
                    state: SessionState::Active,
                },
            ],
            provider_contexts: Vec::new(),
        };

        let layout = try_layout_graph(&graph).unwrap();
        let rank_one = layout
            .nodes
            .iter()
            .filter(|node| node.rank == 1)
            .map(|node| (node.point.y, node.node_id.as_str()))
            .collect::<Vec<_>>();

        assert_eq!(
            rank_one,
            vec![
                (GRAPH_PADDING, "main-head"),
                (GRAPH_PADDING + GRAPH_ROW_STEP, "alpha-head"),
                (GRAPH_PADDING + GRAPH_ROW_STEP * 2, "zeta-head"),
                (GRAPH_PADDING + GRAPH_ROW_STEP * 3, "orphan-head"),
            ]
        );
    }

    #[test]
    fn stable_keys_turn_coordinate_changes_into_updates() {
        let graph = snapshot(vec![node("a", 1)], Vec::new());
        let previous = layout_graph_viewport(&graph, GraphViewportRequest::default());
        let mut current = layout_graph_viewport(&graph, GraphViewportRequest::default());
        current.nodes[0].x += GRAPH_RANK_STEP;
        let diff = diff_graph_viewport_responses(previous, current, None);

        assert!(diff.added.nodes.is_empty());
        assert!(diff.removed.is_empty());
        assert_eq!(diff.updated.nodes[0].key, "node:a");
    }

    #[test]
    fn bezier_bounds_include_both_control_points() {
        assert_eq!(
            route_bounds(GraphBezierRoute {
                source: Point { x: 0, y: 0 },
                control_1: Point { x: 25, y: -40 },
                control_2: Point { x: 75, y: 140 },
                target: Point { x: 100, y: 100 },
            }),
            (0, -40, 100, 140)
        );
    }

    #[test]
    fn cycles_return_a_typed_error() {
        let graph = snapshot(
            vec![node("a", 1), node("b", 2)],
            vec![
                GraphEdge {
                    source: "a".to_owned(),
                    target: "b".to_owned(),
                    kind: GraphEdgeKind::Primary,
                },
                GraphEdge {
                    source: "b".to_owned(),
                    target: "a".to_owned(),
                    kind: GraphEdgeKind::Merge,
                },
            ],
        );

        assert!(matches!(
            try_layout_graph(&graph),
            Err(GraphLayoutError::Cycle { .. })
        ));
    }

    #[test]
    fn duplicate_nodes_return_a_typed_error() {
        let graph = snapshot(vec![node("a", 1), node("a", 2)], Vec::new());

        assert!(matches!(
            try_layout_graph(&graph),
            Err(GraphLayoutError::DuplicateNode { node_id }) if node_id == "a"
        ));
    }
}
