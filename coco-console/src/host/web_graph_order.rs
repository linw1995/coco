use std::collections::{BTreeMap, BTreeSet};

use snafu::prelude::*;

use super::web_graph_view::{GRAPH_PADDING, GRAPH_ROW_STEP};
use crate::web_graph::{EdgeKind, NodeId, NodePlacement, Point};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Web graph order {order} exceeds the layout coordinate range"))]
    CoordinateOverflow { order: usize },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IncomingEdge {
    pub kind: EdgeKind,
    pub source_y: i32,
}

pub fn stable_column_order(
    placements: &[NodePlacement],
    new_node_order: &BTreeMap<NodeId, usize>,
    incoming: &BTreeMap<NodeId, Vec<IncomingEdge>>,
) -> Result<Vec<NodePlacement>> {
    let mut placements = placements.to_vec();
    placements.sort_by(|left, right| {
        left.point
            .y
            .cmp(&right.point.y)
            .then_with(|| left.node.cmp(&right.node))
    });
    let new_node_ids = placements
        .iter()
        .filter(|placement| new_node_order.contains_key(&placement.node))
        .map(|placement| placement.node.clone())
        .collect::<BTreeSet<_>>();
    let mut fixed = placements
        .iter()
        .filter(|placement| !new_node_ids.contains(&placement.node))
        .map(|placement| placement.node.clone())
        .collect::<Vec<_>>();
    let mut pending = new_node_ids.into_iter().collect::<Vec<_>>();
    pending.sort_by(|left, right| {
        preferred_position(left, incoming)
            .cmp(&preferred_position(right, incoming))
            .then_with(|| {
                new_nodes_order(left, new_node_order, placements.as_slice()).cmp(&new_nodes_order(
                    right,
                    new_node_order,
                    placements.as_slice(),
                ))
            })
            .then_with(|| left.cmp(right))
    });

    for node in pending {
        let index = best_insertion_index(&fixed, &node, incoming);
        fixed.insert(index, node);
    }

    let x_by_node = placements
        .into_iter()
        .map(|placement| (placement.node, placement.point.x))
        .collect::<BTreeMap<_, _>>();
    fixed
        .into_iter()
        .enumerate()
        .map(|(order, node)| {
            Ok(NodePlacement {
                point: Point {
                    x: x_by_node[&node],
                    y: order_y(order)?,
                },
                node,
            })
        })
        .collect()
}

fn new_nodes_order(
    node: &NodeId,
    new_nodes: &BTreeMap<NodeId, usize>,
    placements: &[NodePlacement],
) -> (usize, i32) {
    (
        new_nodes.get(node).copied().unwrap_or(usize::MAX),
        placements
            .iter()
            .find(|placement| &placement.node == node)
            .map_or(i32::MAX, |placement| placement.point.y),
    )
}

fn preferred_position(
    node: &NodeId,
    incoming: &BTreeMap<NodeId, Vec<IncomingEdge>>,
) -> (u8, i32, i32) {
    let edges = incoming.get(node).map(Vec::as_slice).unwrap_or_default();
    let primary = weighted_median(
        edges
            .iter()
            .filter(|edge| edge.kind == EdgeKind::Primary)
            .copied(),
    );
    let all = weighted_median(edges.iter().copied());
    (
        u8::from(primary.is_none()),
        primary.or(all).unwrap_or(i32::MAX),
        all.unwrap_or(i32::MAX),
    )
}

fn weighted_median(edges: impl Iterator<Item = IncomingEdge>) -> Option<i32> {
    let mut values = edges.collect::<Vec<_>>();
    values.sort_by_key(|edge| edge.source_y);
    let total = values
        .iter()
        .map(|edge| edge_weight(edge.kind))
        .sum::<u64>();
    if total == 0 {
        return None;
    }
    let midpoint = total.div_ceil(2);
    let mut cumulative = 0_u64;
    values.into_iter().find_map(|edge| {
        cumulative = cumulative.saturating_add(edge_weight(edge.kind));
        (cumulative >= midpoint).then_some(edge.source_y)
    })
}

fn best_insertion_index(
    fixed: &[NodeId],
    node: &NodeId,
    incoming: &BTreeMap<NodeId, Vec<IncomingEdge>>,
) -> usize {
    let new_edges = incoming.get(node).map(Vec::as_slice).unwrap_or_default();
    let mut deltas = vec![CrossingDelta::default(); fixed.len() + 2];
    for (target_order, target) in fixed.iter().enumerate() {
        let old_edges = incoming.get(target).map(Vec::as_slice).unwrap_or_default();
        for new_edge in new_edges {
            for old_edge in old_edges {
                match new_edge.source_y.cmp(&old_edge.source_y) {
                    std::cmp::Ordering::Less => add_crossing_range(
                        &mut deltas,
                        target_order + 1,
                        fixed.len(),
                        crossing_class(new_edge.kind, old_edge.kind),
                    ),
                    std::cmp::Ordering::Greater => add_crossing_range(
                        &mut deltas,
                        0,
                        target_order,
                        crossing_class(new_edge.kind, old_edge.kind),
                    ),
                    std::cmp::Ordering::Equal => {}
                }
            }
        }
    }

    let mut crossings = CrossingDelta::default();
    let mut best = None;
    for (index, delta) in deltas.into_iter().take(fixed.len() + 1).enumerate() {
        crossings += delta;
        let target_y = i64::from(GRAPH_PADDING).saturating_add(
            i64::try_from(index)
                .unwrap_or(i64::MAX)
                .saturating_mul(i64::from(GRAPH_ROW_STEP)),
        );
        let primary_span = new_edges
            .iter()
            .filter(|edge| edge.kind == EdgeKind::Primary)
            .map(|edge| target_y.abs_diff(i64::from(edge.source_y)))
            .sum();
        let total_span = new_edges
            .iter()
            .map(|edge| {
                target_y
                    .abs_diff(i64::from(edge.source_y))
                    .saturating_mul(edge_weight(edge.kind))
            })
            .sum();
        let cost = InsertionCost {
            primary_primary_crossings: nonnegative(crossings.primary_primary),
            primary_other_crossings: nonnegative(crossings.primary_other),
            merge_crossings: nonnegative(crossings.merge),
            shadow_crossings: nonnegative(crossings.shadow),
            moved_nodes: u64::try_from(fixed.len() - index).unwrap_or(u64::MAX),
            primary_span,
            total_span,
            index,
        };
        if best.as_ref().is_none_or(|(best_cost, _)| &cost < best_cost) {
            best = Some((cost, index));
        }
    }
    best.expect("a column always has an insertion gap").1
}

fn add_crossing_range(
    deltas: &mut [CrossingDelta],
    start: usize,
    end: usize,
    class: CrossingClass,
) {
    if start > end {
        return;
    }
    deltas[start].change(class, 1);
    deltas[end + 1].change(class, -1);
}

fn crossing_class(left: EdgeKind, right: EdgeKind) -> CrossingClass {
    match (left, right) {
        (EdgeKind::Primary, EdgeKind::Primary) => CrossingClass::PrimaryPrimary,
        (EdgeKind::Primary, _) | (_, EdgeKind::Primary) => CrossingClass::PrimaryOther,
        (EdgeKind::Merge, _) | (_, EdgeKind::Merge) => CrossingClass::Merge,
        (EdgeKind::Shadow, EdgeKind::Shadow) => CrossingClass::Shadow,
    }
}

fn edge_weight(kind: EdgeKind) -> u64 {
    match kind {
        EdgeKind::Primary => 4,
        EdgeKind::Merge => 2,
        EdgeKind::Shadow => 1,
    }
}

fn order_y(order: usize) -> Result<i32> {
    let Ok(sqlite_order) = i32::try_from(order) else {
        return CoordinateOverflowSnafu { order }.fail();
    };
    sqlite_order
        .checked_mul(GRAPH_ROW_STEP)
        .and_then(|offset| GRAPH_PADDING.checked_add(offset))
        .context(CoordinateOverflowSnafu { order })
}

fn nonnegative(value: i64) -> u64 {
    debug_assert!(value >= 0);
    u64::try_from(value).unwrap_or_default()
}

#[derive(Debug, Clone, Copy)]
enum CrossingClass {
    PrimaryPrimary,
    PrimaryOther,
    Merge,
    Shadow,
}

#[derive(Debug, Clone, Copy, Default)]
struct CrossingDelta {
    primary_primary: i64,
    primary_other: i64,
    merge: i64,
    shadow: i64,
}

impl CrossingDelta {
    fn change(&mut self, class: CrossingClass, amount: i64) {
        match class {
            CrossingClass::PrimaryPrimary => self.primary_primary += amount,
            CrossingClass::PrimaryOther => self.primary_other += amount,
            CrossingClass::Merge => self.merge += amount,
            CrossingClass::Shadow => self.shadow += amount,
        }
    }
}

impl std::ops::AddAssign for CrossingDelta {
    fn add_assign(&mut self, right: Self) {
        self.primary_primary += right.primary_primary;
        self.primary_other += right.primary_other;
        self.merge += right.merge;
        self.shadow += right.shadow;
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct InsertionCost {
    primary_primary_crossings: u64,
    primary_other_crossings: u64,
    merge_crossings: u64,
    shadow_crossings: u64,
    moved_nodes: u64,
    primary_span: u64,
    total_span: u64,
    index: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(value: &str) -> NodeId {
        NodeId::new(value).unwrap()
    }

    fn placement(value: &str, order: i32) -> NodePlacement {
        NodePlacement {
            node: node(value),
            point: Point {
                x: 168,
                y: GRAPH_PADDING + order * GRAPH_ROW_STEP,
            },
        }
    }

    fn incoming(kind: EdgeKind, source_order: i32) -> IncomingEdge {
        IncomingEdge {
            kind,
            source_y: GRAPH_PADDING + source_order * GRAPH_ROW_STEP,
        }
    }

    #[test]
    fn inserts_a_new_node_where_its_primary_edge_does_not_cross() {
        let placements = vec![
            placement("left", 0),
            placement("right", 1),
            placement("new", 2),
        ];
        let incoming = BTreeMap::from([
            (node("left"), vec![incoming(EdgeKind::Primary, 0)]),
            (node("right"), vec![incoming(EdgeKind::Primary, 2)]),
            (node("new"), vec![incoming(EdgeKind::Primary, 1)]),
        ]);

        let ordered =
            stable_column_order(&placements, &BTreeMap::from([(node("new"), 0)]), &incoming)
                .unwrap();

        assert_eq!(
            ordered
                .into_iter()
                .map(|placement| placement.node)
                .collect::<Vec<_>>(),
            vec![node("left"), node("new"), node("right")]
        );
    }

    #[test]
    fn equal_parent_positions_append_without_moving_existing_nodes() {
        let placements = vec![
            placement("first", 0),
            placement("second", 1),
            placement("new", 2),
        ];
        let incoming = BTreeMap::from([
            (node("first"), vec![incoming(EdgeKind::Primary, 0)]),
            (node("second"), vec![incoming(EdgeKind::Primary, 0)]),
            (node("new"), vec![incoming(EdgeKind::Primary, 0)]),
        ]);

        let ordered =
            stable_column_order(&placements, &BTreeMap::from([(node("new"), 0)]), &incoming)
                .unwrap();

        assert_eq!(ordered, placements);
    }

    #[test]
    fn existing_nodes_keep_their_relative_order() {
        let placements = vec![placement("a", 0), placement("b", 1), placement("new", 2)];
        let incoming = BTreeMap::from([
            (node("a"), vec![incoming(EdgeKind::Primary, 2)]),
            (node("b"), vec![incoming(EdgeKind::Primary, 0)]),
            (node("new"), vec![incoming(EdgeKind::Primary, 1)]),
        ]);

        let ordered =
            stable_column_order(&placements, &BTreeMap::from([(node("new"), 0)]), &incoming)
                .unwrap();
        let nodes = ordered
            .into_iter()
            .map(|placement| placement.node)
            .collect::<Vec<_>>();

        let a = node("a");
        let b = node("b");
        assert!(
            nodes.iter().position(|candidate| candidate == &a).unwrap()
                < nodes.iter().position(|candidate| candidate == &b).unwrap()
        );
    }
}
