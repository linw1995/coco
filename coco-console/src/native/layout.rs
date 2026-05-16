use std::collections::{BTreeSet, HashMap};

use coco_mem::SessionState;

use crate::graph::{GraphBranch, GraphEdgeKind, GraphSnapshot, node_target_id, shorten_id};

const NODE_RADIUS: f64 = 26.0;
pub const GRAPH_LEFT_X: i32 = 120;
pub const GRAPH_TOP_Y: i32 = 90;
pub const GRAPH_COLUMN_WIDTH: i32 = 220;
pub const GRAPH_LANE_HEIGHT: i32 = 140;
const MAX_EDGE_COLUMN_GAP: i32 = 5;
const EDGE_NODE_EXIT: f64 = 42.0;
const EDGE_TARGET_APPROACH: f64 = 48.0;
const EDGE_ROUTE_STEP: f64 = 12.0;
pub const EDGE_TARGET_PORT_STEP: f64 = 14.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Point {
    pub x: i32,
    pub y: i32,
}

#[derive(Debug)]
pub struct GraphLayout {
    pub lanes: Vec<GraphLane>,
    pub occurrences: Vec<GraphNodeOccurrence>,
    pub primary_edges: Vec<GraphLayoutEdge>,
    pub fork_edges: Vec<GraphLayoutEdge>,
    pub merge_edges: Vec<GraphLayoutEdge>,
    pub width: i32,
    pub height: i32,
}

#[derive(Debug)]
pub struct GraphLane {
    pub label: String,
    pub y: i32,
}

#[derive(Debug)]
pub struct GraphNodeOccurrence {
    pub node_id: String,
    pub node_target: String,
    pub point: Point,
}

#[derive(Debug)]
pub struct GraphLayoutEdge {
    pub source: Point,
    pub target: Point,
    pub kind: GraphLayoutEdgeKind,
    pub route_slot: i32,
    pub target_port_offset: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphLayoutEdgeKind {
    PrimaryParent,
    Fork,
    MergeParent,
}

fn is_merge_like_edge(kind: GraphEdgeKind) -> bool {
    matches!(
        kind,
        GraphEdgeKind::MergeParent | GraphEdgeKind::ShadowParent
    )
}

#[derive(Debug)]
struct LanePlan {
    label: String,
    nodes: Vec<String>,
    fork_source: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct NodeLocation {
    lane_index: usize,
    node_index: usize,
}
pub fn line_points(
    source: Point,
    target: Point,
    target_port_offset: f64,
) -> (String, String, String, String) {
    let (start_x, start_y, end_x, end_y) = line_point_values(source, target, target_port_offset);
    (
        format!("{start_x:.1}"),
        format!("{start_y:.1}"),
        format!("{end_x:.1}"),
        format!("{end_y:.1}"),
    )
}

fn line_point_values(
    source: Point,
    target: Point,
    target_port_offset: f64,
) -> (f64, f64, f64, f64) {
    let dx = f64::from(target.x - source.x);
    let target_y = f64::from(target.y) + target_port_offset;
    let dy = target_y - f64::from(source.y);
    let distance = (dx * dx + dy * dy).sqrt();
    if distance <= NODE_RADIUS * 2.0 {
        return (
            f64::from(source.x),
            f64::from(source.y),
            f64::from(target.x),
            target_y,
        );
    }

    let ux = dx / distance;
    let uy = dy / distance;
    let start_x = f64::from(source.x) + ux * (NODE_RADIUS + 2.0);
    let start_y = f64::from(source.y) + uy * (NODE_RADIUS + 2.0);
    let end_x = f64::from(target.x) - ux * (NODE_RADIUS + 8.0);
    let end_y = target_y - uy * (NODE_RADIUS + 8.0);

    (start_x, start_y, end_x, end_y)
}

pub fn routed_elbow_points(
    source: Point,
    target: Point,
    route_slot: i32,
    target_port_offset: f64,
) -> String {
    let start_x = f64::from(source.x) + NODE_RADIUS + 2.0;
    let start_y = f64::from(source.y);
    let end_x = if target.x > source.x {
        f64::from(target.x) - NODE_RADIUS - 8.0
    } else {
        f64::from(target.x) + NODE_RADIUS + 8.0
    };
    let end_y = f64::from(target.y) + target_port_offset;
    let exit_x = (start_x + EDGE_NODE_EXIT).min(end_x - EDGE_TARGET_APPROACH);
    let approach_x = (end_x - EDGE_TARGET_APPROACH).max(exit_x + EDGE_TARGET_APPROACH);
    let corridor_y = edge_corridor_y(source.y, target.y, route_slot);

    format!(
        "{start_x:.1},{start_y:.1} {exit_x:.1},{start_y:.1} {exit_x:.1},{corridor_y:.1} {approach_x:.1},{corridor_y:.1} {approach_x:.1},{end_y:.1} {end_x:.1},{end_y:.1}"
    )
}

fn edge_corridor_y(source_y: i32, target_y: i32, route_slot: i32) -> f64 {
    let base_y = match target_y.cmp(&source_y) {
        std::cmp::Ordering::Less => source_y - GRAPH_LANE_HEIGHT / 2,
        std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => source_y + GRAPH_LANE_HEIGHT / 2,
    };
    let offset = route_slot_offset(route_slot);
    (f64::from(base_y) + offset).max(16.0)
}

fn route_slot_offset(route_slot: i32) -> f64 {
    let magnitude = (route_slot + 1) / 2;
    let direction = if route_slot % 2 == 0 { 1.0 } else { -1.0 };

    f64::from(magnitude.min(4)) * EDGE_ROUTE_STEP * direction
}

pub fn layout_graph(snapshot: &GraphSnapshot) -> GraphLayout {
    let mut parent_by_child = HashMap::<&str, &str>::new();
    for edge in snapshot
        .edges
        .iter()
        .filter(|edge| edge.kind == GraphEdgeKind::PrimaryParent)
    {
        parent_by_child.insert(edge.target.as_str(), edge.source.as_str());
    }

    let node_ids = snapshot
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut lanes = Vec::<LanePlan>::new();
    let mut covered = BTreeSet::<String>::new();

    let mut branches = snapshot.branches.iter().collect::<Vec<_>>();
    branches.sort_by(|left, right| branch_lane_priority(left).cmp(&branch_lane_priority(right)));
    for branch in branches {
        let chain = collect_visible_chain(branch.head_id.as_str(), &parent_by_child, &node_ids);
        if chain.is_empty() {
            continue;
        }
        let first_new = chain
            .iter()
            .position(|node_id| !covered.contains(node_id))
            .unwrap_or_else(|| chain.len().saturating_sub(1));
        let nodes = chain[first_new..].to_vec();
        let fork_source = first_new
            .checked_sub(1)
            .and_then(|index| chain.get(index))
            .cloned();
        covered.extend(chain.iter().cloned());
        lanes.push(LanePlan {
            label: branch.name.clone(),
            nodes,
            fork_source,
        });
    }

    let children_by_parent = primary_children_by_parent(snapshot);
    let mut orphan_heads = node_ids
        .iter()
        .filter(|node_id| !covered.contains(**node_id))
        .filter(|node_id| {
            !children_by_parent
                .get(**node_id)
                .into_iter()
                .flatten()
                .any(|child| !covered.contains(*child))
        })
        .copied()
        .collect::<Vec<_>>();
    orphan_heads.sort();
    for head_id in orphan_heads {
        let chain = collect_orphan_chain(head_id, &parent_by_child, &node_ids, &covered);
        if chain.is_empty() {
            continue;
        }
        let fork_source = chain
            .first()
            .and_then(|node_id| parent_by_child.get(node_id.as_str()))
            .filter(|parent_id| covered.contains(**parent_id))
            .map(|parent_id| (*parent_id).to_owned());
        covered.extend(chain.iter().cloned());
        lanes.push(LanePlan {
            label: format!("orphan {}", shorten_id(head_id)),
            nodes: chain,
            fork_source,
        });
    }

    for node_id in node_ids {
        if !covered.contains(node_id) {
            lanes.push(LanePlan {
                label: format!("orphan {}", shorten_id(node_id)),
                nodes: vec![node_id.to_owned()],
                fork_source: None,
            });
        }
    }

    let event_order_by_node = event_order_by_node(snapshot);
    let mut layout_lanes = Vec::new();
    let mut lane_columns = Vec::<Vec<i32>>::new();
    let mut first_location_by_node = HashMap::<String, NodeLocation>::new();

    for (lane_index, lane) in lanes.iter().enumerate() {
        let y = GRAPH_TOP_Y + lane_index as i32 * GRAPH_LANE_HEIGHT;
        layout_lanes.push(GraphLane {
            label: lane.label.clone(),
            y,
        });

        let mut columns = Vec::with_capacity(lane.nodes.len());
        for (node_index, node_id) in lane.nodes.iter().enumerate() {
            let column = if let Some(previous_id) = node_index
                .checked_sub(1)
                .and_then(|previous_index| lane.nodes.get(previous_index))
            {
                columns[node_index - 1]
                    + required_column_gap(previous_id, node_id, &event_order_by_node)
            } else {
                lane.fork_source
                    .as_ref()
                    .and_then(|fork_source| {
                        let source = first_location_by_node.get(fork_source)?;
                        let source_column = lane_columns[source.lane_index][source.node_index];
                        Some(
                            source_column
                                + required_column_gap(fork_source, node_id, &event_order_by_node),
                        )
                    })
                    .unwrap_or(0)
            };
            columns.push(column);
            first_location_by_node
                .entry(node_id.clone())
                .or_insert(NodeLocation {
                    lane_index,
                    node_index,
                });
        }
        lane_columns.push(columns);
    }

    relax_lane_columns(
        snapshot,
        &lanes,
        &mut lane_columns,
        &first_location_by_node,
        &event_order_by_node,
    );

    let mut occurrences = Vec::new();
    let mut primary_edges = Vec::new();
    let mut fork_edges = Vec::new();
    let mut first_occurrence_by_node = HashMap::<String, Point>::new();
    let mut route_slots_by_corridor = HashMap::<(i32, i32), i32>::new();

    for (lane_index, lane) in lanes.iter().enumerate() {
        let y = GRAPH_TOP_Y + lane_index as i32 * GRAPH_LANE_HEIGHT;
        let mut previous = None::<Point>;
        let mut first_point = None;
        for (node_index, node_id) in lane.nodes.iter().enumerate() {
            let point = Point {
                x: column_to_x(lane_columns[lane_index][node_index]),
                y,
            };
            first_point.get_or_insert(point);
            occurrences.push(GraphNodeOccurrence {
                node_id: node_id.clone(),
                node_target: node_target_id(node_id),
                point,
            });
            if let Some(source) = previous {
                primary_edges.push(GraphLayoutEdge {
                    source,
                    target: point,
                    kind: GraphLayoutEdgeKind::PrimaryParent,
                    route_slot: 0,
                    target_port_offset: 0.0,
                });
            }
            first_occurrence_by_node
                .entry(node_id.clone())
                .or_insert(point);
            previous = Some(point);
        }

        if let (Some(fork_source), Some(target)) = (&lane.fork_source, first_point)
            && let Some(source) = first_occurrence_by_node.get(fork_source)
        {
            let route_slot = reserve_route_slot(&mut route_slots_by_corridor, *source, target);
            fork_edges.push(GraphLayoutEdge {
                source: *source,
                target,
                kind: GraphLayoutEdgeKind::Fork,
                route_slot,
                target_port_offset: 0.0,
            });
        }
    }

    let mut merge_edges = snapshot
        .edges
        .iter()
        .filter(|edge| is_merge_like_edge(edge.kind))
        .filter_map(|edge| {
            let source = *first_occurrence_by_node.get(&edge.source)?;
            let target = *first_occurrence_by_node.get(&edge.target)?;
            let route_slot = reserve_route_slot(&mut route_slots_by_corridor, source, target);
            Some(GraphLayoutEdge {
                source,
                target,
                kind: GraphLayoutEdgeKind::MergeParent,
                route_slot,
                target_port_offset: 0.0,
            })
        })
        .collect::<Vec<_>>();
    distribute_incoming_ports(&mut primary_edges, &mut fork_edges, &mut merge_edges);

    let width = occurrences
        .iter()
        .map(|occurrence| occurrence.point.x)
        .chain(
            fork_edges
                .iter()
                .chain(merge_edges.iter())
                .map(|edge| edge.source.x.max(edge.target.x) + 180),
        )
        .max()
        .unwrap_or(720)
        + 120;
    let height = layout_lanes.last().map(|lane| lane.y).unwrap_or(420) + 120;

    GraphLayout {
        lanes: layout_lanes,
        occurrences,
        primary_edges,
        fork_edges,
        merge_edges,
        width,
        height,
    }
}

fn event_order_by_node(snapshot: &GraphSnapshot) -> HashMap<&str, usize> {
    let mut nodes = snapshot.nodes.iter().collect::<Vec<_>>();
    nodes.sort_by(|left, right| {
        left.created_at_ns
            .cmp(&right.created_at_ns)
            .then_with(|| left.id.cmp(&right.id))
    });
    nodes
        .into_iter()
        .enumerate()
        .map(|(index, node)| (node.id.as_str(), index))
        .collect()
}

fn required_column_gap(
    source_id: &str,
    target_id: &str,
    event_order_by_node: &HashMap<&str, usize>,
) -> i32 {
    event_order_by_node
        .get(target_id)
        .zip(event_order_by_node.get(source_id))
        .and_then(|(target_order, source_order)| target_order.checked_sub(*source_order))
        .map(|gap| gap.clamp(1, MAX_EDGE_COLUMN_GAP as usize) as i32)
        .unwrap_or(1)
}

fn relax_lane_columns(
    snapshot: &GraphSnapshot,
    lanes: &[LanePlan],
    lane_columns: &mut [Vec<i32>],
    first_location_by_node: &HashMap<String, NodeLocation>,
    event_order_by_node: &HashMap<&str, usize>,
) {
    let constraint_count =
        snapshot.edges.len() + lanes.iter().map(|lane| lane.nodes.len()).sum::<usize>();
    let max_passes = constraint_count.saturating_mul(lanes.len().max(1)).max(1);

    for _ in 0..max_passes {
        let mut changed = false;

        for (lane_index, lane) in lanes.iter().enumerate() {
            if let Some((source_id, target_id)) = lane.fork_source.as_ref().zip(lane.nodes.first())
                && let Some(source) = first_location_by_node.get(source_id)
            {
                let min_column = lane_columns[source.lane_index][source.node_index]
                    + required_column_gap(source_id, target_id, event_order_by_node);
                changed |= ensure_lane_column(lane_columns, lane_index, 0, min_column);
            }

            for node_index in 1..lane.nodes.len() {
                let source_id = &lane.nodes[node_index - 1];
                let target_id = &lane.nodes[node_index];
                let min_column = lane_columns[lane_index][node_index - 1]
                    + required_column_gap(source_id, target_id, event_order_by_node);
                changed |= ensure_lane_column(lane_columns, lane_index, node_index, min_column);
            }
        }

        for edge in snapshot
            .edges
            .iter()
            .filter(|edge| is_merge_like_edge(edge.kind))
        {
            let Some(source) = first_location_by_node.get(&edge.source) else {
                continue;
            };
            let Some(target) = first_location_by_node.get(&edge.target) else {
                continue;
            };
            let min_column = lane_columns[source.lane_index][source.node_index]
                + required_column_gap(&edge.source, &edge.target, event_order_by_node);
            changed |= ensure_lane_column(
                lane_columns,
                target.lane_index,
                target.node_index,
                min_column,
            );
        }

        if !changed {
            return;
        }
    }
}

fn ensure_lane_column(
    lane_columns: &mut [Vec<i32>],
    lane_index: usize,
    node_index: usize,
    min_column: i32,
) -> bool {
    let column = lane_columns[lane_index][node_index];
    if column >= min_column {
        return false;
    }

    let delta = min_column - column;
    for column in &mut lane_columns[lane_index][node_index..] {
        *column += delta;
    }
    true
}

fn distribute_incoming_ports(
    primary_edges: &mut [GraphLayoutEdge],
    fork_edges: &mut [GraphLayoutEdge],
    merge_edges: &mut [GraphLayoutEdge],
) {
    let mut primary_counts = HashMap::<Point, usize>::new();
    for edge in primary_edges.iter() {
        *primary_counts.entry(edge.target).or_default() += 1;
    }
    let mut secondary_counts = HashMap::<Point, usize>::new();
    for edge in fork_edges.iter().chain(merge_edges.iter()) {
        *secondary_counts.entry(edge.target).or_default() += 1;
    }

    let mut primary_indexes = HashMap::<Point, usize>::new();
    for edge in primary_edges.iter_mut() {
        let count = primary_counts.get(&edge.target).copied().unwrap_or(1);
        let index = primary_indexes.entry(edge.target).or_default();
        edge.target_port_offset = primary_incoming_port_offset(count, *index);
        *index += 1;
    }

    let mut secondary_indexes = HashMap::<Point, usize>::new();
    for edge in fork_edges.iter_mut().chain(merge_edges.iter_mut()) {
        let index = secondary_indexes.entry(edge.target).or_default();
        edge.target_port_offset = if primary_counts.contains_key(&edge.target) {
            secondary_incoming_port_offset(*index)
        } else {
            let count = secondary_counts.get(&edge.target).copied().unwrap_or(1);
            secondary_only_incoming_port_offset(count, *index)
        };
        *index += 1;
    }
}

fn primary_incoming_port_offset(count: usize, index: usize) -> f64 {
    (index as f64 - (count as f64 - 1.0) / 2.0) * EDGE_TARGET_PORT_STEP
}

fn secondary_incoming_port_offset(index: usize) -> f64 {
    let distance = index / 2 + 1;
    let direction = if index.is_multiple_of(2) { 1.0 } else { -1.0 };
    distance as f64 * EDGE_TARGET_PORT_STEP * direction
}

fn secondary_only_incoming_port_offset(count: usize, index: usize) -> f64 {
    primary_incoming_port_offset(count, index)
}

fn column_to_x(column: i32) -> i32 {
    GRAPH_LEFT_X + column * GRAPH_COLUMN_WIDTH
}

fn reserve_route_slot(
    route_slots_by_corridor: &mut HashMap<(i32, i32), i32>,
    source: Point,
    target: Point,
) -> i32 {
    let key = edge_corridor_key(source, target);
    let next_slot = route_slots_by_corridor.entry(key).or_default();
    let route_slot = *next_slot;
    *next_slot += 1;
    route_slot
}

fn edge_corridor_key(source: Point, target: Point) -> (i32, i32) {
    (source.y, (target.y - source.y).signum())
}

fn branch_lane_priority(branch: &GraphBranch) -> (u8, &str) {
    if branch.name == "main" {
        (0, branch.name.as_str())
    } else {
        let state_priority = match &branch.state {
            SessionState::Active => 1,
            SessionState::Attached { .. } => 2,
            SessionState::Paused { .. } => 3,
        };
        (state_priority, branch.name.as_str())
    }
}

fn collect_visible_chain(
    head_id: &str,
    parent_by_child: &HashMap<&str, &str>,
    node_ids: &BTreeSet<&str>,
) -> Vec<String> {
    let mut chain = Vec::new();
    let mut node_id = head_id;
    let mut visited = BTreeSet::new();

    while node_ids.contains(node_id) && visited.insert(node_id) {
        chain.push(node_id.to_owned());
        let Some(parent) = parent_by_child.get(node_id) else {
            break;
        };
        node_id = parent;
    }

    chain.reverse();
    chain
}

fn collect_orphan_chain(
    head_id: &str,
    parent_by_child: &HashMap<&str, &str>,
    node_ids: &BTreeSet<&str>,
    covered: &BTreeSet<String>,
) -> Vec<String> {
    let mut chain = Vec::new();
    let mut node_id = head_id;
    let mut visited = BTreeSet::new();

    while node_ids.contains(node_id) && !covered.contains(node_id) && visited.insert(node_id) {
        chain.push(node_id.to_owned());
        let Some(parent) = parent_by_child.get(node_id) else {
            break;
        };
        node_id = parent;
    }

    chain.reverse();
    chain
}

fn primary_children_by_parent(snapshot: &GraphSnapshot) -> HashMap<&str, Vec<&str>> {
    let mut children_by_parent = HashMap::<&str, Vec<&str>>::new();
    for edge in snapshot
        .edges
        .iter()
        .filter(|edge| edge.kind == GraphEdgeKind::PrimaryParent)
    {
        children_by_parent
            .entry(edge.source.as_str())
            .or_default()
            .push(edge.target.as_str());
    }
    children_by_parent
}
