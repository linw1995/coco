use crate::api::{
    AnchorRangeNode, AnchorRangePath, GRAPH_SOURCE_PORT_OFFSET_X, GRAPH_TARGET_PORT_OFFSET_X,
    GraphBezierRoute, GraphViewportEdgeKind, Point,
};

pub const DETAIL_RANK_STEP: i32 = 112;
const LANE_ROW_STEP: i32 = 72;
const GRAPH_PADDING: i32 = 56;
const NODE_RADIUS: i32 = 18;
const RANGE_PADDING: i32 = 52;
const BEZIER_PARAMETER_SCALE: i64 = 1 << 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchorRangeLayout {
    pub paths: Vec<AnchorRangeLayoutPath>,
    pub bounds: AnchorRangeBounds,
    pub detail_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchorRangeLayoutPath {
    pub nodes: Vec<AnchorRangeLayoutNode>,
    pub edges: Vec<AnchorRangeLayoutEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchorRangeLayoutNode {
    pub node: AnchorRangeNode,
    pub point: Point,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnchorRangeLayoutEdge {
    pub kind: GraphViewportEdgeKind,
    pub source: Point,
    pub target: Point,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnchorRangeBounds {
    pub left: i32,
    pub top: i32,
    pub right: i32,
    pub bottom: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnchorRangeTransform {
    pub source_x: i32,
    pub target_x: i32,
    pub extra_width: i32,
}

impl AnchorRangeTransform {
    pub fn transform_x(self, x: i32) -> i32 {
        if x < self.target_x || self.extra_width == 0 {
            return x;
        }
        x.saturating_add(self.extra_width)
    }

    pub fn inverse_x(self, x: f64) -> f64 {
        let target_x = f64::from(self.target_x);
        let expanded_target_x = target_x + f64::from(self.extra_width);
        if x < target_x || self.extra_width == 0 {
            return x;
        }
        if x >= expanded_target_x {
            return x - f64::from(self.extra_width);
        }
        target_x
    }
}

pub fn transform_graph_route(
    route: GraphBezierRoute,
    source_node_x: Option<i32>,
    target_node_x: Option<i32>,
    transform: AnchorRangeTransform,
) -> GraphBezierRoute {
    let source_node_x =
        source_node_x.unwrap_or_else(|| route.source.x.saturating_sub(GRAPH_SOURCE_PORT_OFFSET_X));
    let target_node_x =
        target_node_x.unwrap_or_else(|| route.target.x.saturating_add(GRAPH_TARGET_PORT_OFFSET_X));
    let endpoint_delta = |node_x: i32| transform.transform_x(node_x).saturating_sub(node_x);
    let source_delta = endpoint_delta(source_node_x);
    let target_delta = endpoint_delta(target_node_x);
    let translate = |point: Point, delta: i32| Point {
        x: point.x.saturating_add(delta),
        y: point.y,
    };

    GraphBezierRoute {
        source: translate(route.source, source_delta),
        control_1: translate(route.control_1, source_delta),
        control_2: translate(route.control_2, target_delta),
        target: translate(route.target, target_delta),
    }
}

pub fn route_anchor_range_edge(edge: AnchorRangeLayoutEdge, node_radius: f64) -> GraphBezierRoute {
    let delta_x = f64::from(edge.target.x) - f64::from(edge.source.x);
    let delta_y = f64::from(edge.target.y) - f64::from(edge.source.y);
    let distance = delta_x.hypot(delta_y);
    let (source, target) = if distance > 0.0 {
        let offset_x = delta_x * node_radius / distance;
        let offset_y = delta_y * node_radius / distance;
        (
            Point {
                x: rounded_i32(f64::from(edge.source.x) + offset_x),
                y: rounded_i32(f64::from(edge.source.y) + offset_y),
            },
            Point {
                x: rounded_i32(f64::from(edge.target.x) - offset_x),
                y: rounded_i32(f64::from(edge.target.y) - offset_y),
            },
        )
    } else {
        (edge.source, edge.target)
    };
    let horizontal = target.x.saturating_sub(source.x);
    let control = horizontal / 2;

    GraphBezierRoute {
        source,
        control_1: Point {
            x: source.x.saturating_add(control),
            y: source.y,
        },
        control_2: Point {
            x: target.x.saturating_sub(control),
            y: target.y,
        },
        target,
    }
}

pub fn anchor_range_extra_width(paths: &[AnchorRangePath]) -> i32 {
    let detail_columns = paths
        .iter()
        .map(|path| path.nodes.len().saturating_sub(2))
        .max()
        .unwrap_or_default();
    i32::try_from(detail_columns)
        .unwrap_or(i32::MAX)
        .saturating_mul(DETAIL_RANK_STEP)
}

pub fn layout_anchor_range(
    source: Point,
    target: Point,
    paths: Vec<AnchorRangePath>,
    occupied_routes: &[GraphBezierRoute],
) -> AnchorRangeLayout {
    let inserted_left = target.x.saturating_sub(anchor_range_extra_width(&paths));
    let mut bounds = None;
    let mut detail_ids = std::collections::BTreeSet::new();
    let mut paths = paths
        .into_iter()
        .map(|path| {
            let detail_count = path.nodes.len().saturating_sub(2);
            let target_kind = path
                .nodes
                .last()
                .and_then(|node| node.incoming_edge)
                .unwrap_or(GraphViewportEdgeKind::Primary);
            let denominator = i64::try_from(detail_count + 1).unwrap_or(i64::MAX);
            let nodes = path
                .nodes
                .into_iter()
                .skip(1)
                .take(detail_count)
                .enumerate()
                .map(|(index, node)| {
                    let numerator = i64::try_from(index + 1).unwrap_or(i64::MAX);
                    let point = Point {
                        x: inserted_left.saturating_add(
                            i32::try_from(index)
                                .unwrap_or(i32::MAX)
                                .saturating_mul(DETAIL_RANK_STEP),
                        ),
                        y: interpolate(source.y, target.y, numerator, denominator),
                    };
                    detail_ids.insert(node.id.clone());
                    AnchorRangeLayoutNode { node, point }
                })
                .collect::<Vec<_>>();
            AnchorRangeLayoutPath {
                nodes,
                edges: vec![AnchorRangeLayoutEdge {
                    kind: target_kind,
                    source,
                    target,
                }],
            }
        })
        .collect::<Vec<_>>();

    reorder_detail_rows(&mut paths, occupied_routes);
    for path in &mut paths {
        path.edges = layout_path_edges(source, target, path);
        for node in &path.nodes {
            bounds
                .get_or_insert_with(|| AnchorRangeBounds::from_points(node.point, node.point))
                .include(node.point);
        }
    }

    AnchorRangeLayout {
        paths,
        bounds: bounds
            .unwrap_or_else(|| AnchorRangeBounds::from_points(source, target))
            .padded(RANGE_PADDING),
        detail_count: detail_ids.len(),
    }
}

fn reorder_detail_rows(paths: &mut [AnchorRangeLayoutPath], occupied_routes: &[GraphBezierRoute]) {
    let column_count = paths
        .iter()
        .map(|path| path.nodes.len())
        .max()
        .unwrap_or_default();
    for column in 0..column_count {
        let mut active_paths = paths
            .iter()
            .enumerate()
            .filter_map(|(path_index, path)| {
                path.nodes.get(column).map(|node| (path_index, node.point))
            })
            .collect::<Vec<_>>();
        active_paths.sort_by_key(|(path_index, point)| (point.y, *path_index));
        let Some(x) = active_paths.first().map(|(_, point)| point.x) else {
            continue;
        };
        let desired_rows = active_paths
            .iter()
            .map(|(_, point)| nearest_row_for_y(point.y))
            .collect::<Vec<_>>();
        let clearances = occupied_routes
            .iter()
            .filter_map(|route| route_clearance_at_x(*route, x))
            .collect::<Vec<_>>();
        let mut reserved_rows = clearances
            .iter()
            .filter_map(|clearance| canonical_rows_in_clearance(clearance.clone()))
            .flatten()
            .collect::<std::collections::BTreeSet<_>>();
        for ((_, point), desired_row) in active_paths.iter().zip(&desired_rows) {
            if clearances
                .iter()
                .any(|clearance| clearance.contains(&point.y))
            {
                reserved_rows.insert(*desired_row);
            }
        }
        for ((path_index, point), (desired_row, row)) in active_paths.into_iter().zip(
            desired_rows
                .iter()
                .copied()
                .zip(available_rows_nearest(&desired_rows, &reserved_rows)),
        ) {
            paths[path_index].nodes[column].point.y = if row == desired_row {
                point.y
            } else {
                row_y(row)
            };
        }
    }
}

fn available_rows_nearest(
    desired_rows: &[usize],
    reserved_rows: &std::collections::BTreeSet<usize>,
) -> Vec<usize> {
    let Some((first_desired, last_desired)) = desired_rows
        .first()
        .copied()
        .zip(desired_rows.last().copied())
    else {
        return Vec::new();
    };
    let margin = desired_rows
        .len()
        .saturating_add(reserved_rows.len())
        .saturating_add(1);
    let candidates = (first_desired.saturating_sub(margin)..=last_desired.saturating_add(margin))
        .filter(|row| !reserved_rows.contains(row))
        .collect::<Vec<_>>();

    let mut states = candidates
        .iter()
        .copied()
        .map(|row| (row.abs_diff(desired_rows[0]) as u128, vec![row]))
        .collect::<Vec<_>>();
    for desired in desired_rows.iter().copied().skip(1) {
        let mut next = Vec::with_capacity(candidates.len());
        for (candidate_index, candidate) in candidates.iter().copied().enumerate() {
            let best = states
                .iter()
                .take(candidate_index)
                .map(|(cost, rows)| {
                    let mut next_rows = rows.clone();
                    next_rows.push(candidate);
                    (
                        cost.saturating_add(candidate.abs_diff(desired) as u128),
                        next_rows,
                    )
                })
                .min();
            next.push(best.unwrap_or((u128::MAX, Vec::new())));
        }
        states = next;
    }
    states
        .into_iter()
        .min()
        .map(|(_, rows)| rows)
        .unwrap_or_default()
}

fn layout_path_edges(
    source: Point,
    target: Point,
    path: &AnchorRangeLayoutPath,
) -> Vec<AnchorRangeLayoutEdge> {
    let target_kind = path
        .edges
        .first()
        .map(|edge| edge.kind)
        .unwrap_or(GraphViewportEdgeKind::Primary);
    let mut points = Vec::with_capacity(path.nodes.len() + 2);
    points.push((source, GraphViewportEdgeKind::Primary));
    points.extend(path.nodes.iter().map(|node| {
        (
            node.point,
            node.node
                .incoming_edge
                .unwrap_or(GraphViewportEdgeKind::Primary),
        )
    }));
    points.push((target, target_kind));
    points
        .windows(2)
        .map(|points| AnchorRangeLayoutEdge {
            kind: points[1].1,
            source: points[0].0,
            target: points[1].0,
        })
        .collect()
}

fn route_clearance_at_x(route: GraphBezierRoute, x: i32) -> Option<std::ops::RangeInclusive<i32>> {
    let left = route.source.x.min(route.target.x);
    let right = route.source.x.max(route.target.x);
    let clearance_left = x.saturating_sub(NODE_RADIUS);
    let clearance_right = x.saturating_add(NODE_RADIUS);
    if clearance_right < left || clearance_left > right {
        return None;
    }
    let left_y = route_y_at_x(route, clearance_left.clamp(left, right));
    let right_y = route_y_at_x(route, clearance_right.clamp(left, right));
    Some(
        left_y.min(right_y).saturating_sub(NODE_RADIUS)
            ..=left_y.max(right_y).saturating_add(NODE_RADIUS),
    )
}

fn canonical_rows_in_clearance(
    clearance: std::ops::RangeInclusive<i32>,
) -> Option<std::ops::RangeInclusive<usize>> {
    let start = i64::from(*clearance.start());
    let end = i64::from(*clearance.end());
    let padding = i64::from(GRAPH_PADDING);
    let step = i64::from(LANE_ROW_STEP);
    if end < padding {
        return None;
    }
    let first = if start <= padding {
        0
    } else {
        usize::try_from((start - padding + step - 1) / step).unwrap_or(usize::MAX)
    };
    let last = usize::try_from((end - padding) / step).unwrap_or(usize::MAX);
    (first <= last).then_some(first..=last)
}

fn route_y_at_x(route: GraphBezierRoute, x: i32) -> i32 {
    if x <= route.source.x.min(route.target.x) {
        return if route.source.x <= route.target.x {
            route.source.y
        } else {
            route.target.y
        };
    }
    if x >= route.source.x.max(route.target.x) {
        return if route.source.x <= route.target.x {
            route.target.y
        } else {
            route.source.y
        };
    }
    let t = route_parameter_at_x(route, x);
    cubic_coordinate(
        route.source.y,
        route.control_1.y,
        route.control_2.y,
        route.target.y,
        t,
    )
}

fn route_parameter_at_x(route: GraphBezierRoute, x: i32) -> i64 {
    let mut lower = 0_i64;
    let mut upper = BEZIER_PARAMETER_SCALE;
    while upper - lower > 1 {
        let middle = lower + (upper - lower) / 2;
        let middle_x = cubic_coordinate(
            route.source.x,
            route.control_1.x,
            route.control_2.x,
            route.target.x,
            middle,
        );
        if middle_x < x {
            lower = middle;
        } else {
            upper = middle;
        }
    }
    lower + (upper - lower) / 2
}

fn cubic_coordinate(start: i32, control_1: i32, control_2: i32, end: i32, t: i64) -> i32 {
    let t = i128::from(t);
    let scale = i128::from(BEZIER_PARAMETER_SCALE);
    let inverse = scale - t;
    let denominator = scale.saturating_pow(3);
    let numerator = i128::from(start)
        .saturating_mul(inverse.saturating_pow(3))
        .saturating_add(
            i128::from(control_1)
                .saturating_mul(3)
                .saturating_mul(inverse.saturating_pow(2))
                .saturating_mul(t),
        )
        .saturating_add(
            i128::from(control_2)
                .saturating_mul(3)
                .saturating_mul(inverse)
                .saturating_mul(t.saturating_pow(2)),
        )
        .saturating_add(i128::from(end).saturating_mul(t.saturating_pow(3)));
    let value = numerator / denominator;
    i32::try_from(value).unwrap_or(if value < 0 { i32::MIN } else { i32::MAX })
}

fn nearest_row_for_y(y: i32) -> usize {
    if y <= GRAPH_PADDING {
        return 0;
    }
    let offset = i64::from(y) - i64::from(GRAPH_PADDING);
    let rounded = offset.saturating_add(i64::from(LANE_ROW_STEP) / 2) / i64::from(LANE_ROW_STEP);
    usize::try_from(rounded).unwrap_or(usize::MAX)
}

fn row_y(row: usize) -> i32 {
    i32::try_from(row)
        .unwrap_or(i32::MAX)
        .saturating_mul(LANE_ROW_STEP)
        .saturating_add(GRAPH_PADDING)
}

impl AnchorRangeBounds {
    fn from_points(left: Point, right: Point) -> Self {
        Self {
            left: left.x.min(right.x),
            top: left.y.min(right.y),
            right: left.x.max(right.x),
            bottom: left.y.max(right.y),
        }
    }

    fn include(&mut self, point: Point) {
        self.left = self.left.min(point.x);
        self.top = self.top.min(point.y);
        self.right = self.right.max(point.x);
        self.bottom = self.bottom.max(point.y);
    }

    fn padded(self, padding: i32) -> Self {
        Self {
            left: self.left.saturating_sub(padding),
            top: self.top.saturating_sub(padding),
            right: self.right.saturating_add(padding),
            bottom: self.bottom.saturating_add(padding),
        }
    }
}

fn interpolate(start: i32, end: i32, numerator: i64, denominator: i64) -> i32 {
    let delta = i64::from(end).saturating_sub(i64::from(start));
    let value = i64::from(start).saturating_add(
        delta
            .saturating_mul(numerator)
            .checked_div(denominator.max(1))
            .unwrap_or_default(),
    );
    i32::try_from(value).unwrap_or(if value < 0 { i32::MIN } else { i32::MAX })
}

fn rounded_i32(value: f64) -> i32 {
    value
        .round()
        .clamp(f64::from(i32::MIN), f64::from(i32::MAX)) as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, incoming_edge: Option<GraphViewportEdgeKind>) -> AnchorRangeNode {
        AnchorRangeNode {
            id: id.to_owned(),
            short_id: id.to_owned(),
            kind: "text".to_owned(),
            role: "User".to_owned(),
            summary: id.to_owned(),
            incoming_edge,
        }
    }

    #[test]
    fn lays_detail_nodes_in_inserted_graph_ranks() {
        let paths = vec![AnchorRangePath {
            nodes: vec![
                node("source", None),
                node("first", Some(GraphViewportEdgeKind::Primary)),
                node("second", Some(GraphViewportEdgeKind::Primary)),
                node("target", Some(GraphViewportEdgeKind::Merge)),
            ],
        }];
        assert_eq!(anchor_range_extra_width(&paths), DETAIL_RANK_STEP * 2);

        let layout = layout_anchor_range(
            Point { x: 100, y: 100 },
            Point { x: 436, y: 100 },
            paths,
            &[],
        );

        assert_eq!(layout.detail_count, 2);
        assert_eq!(layout.paths[0].nodes[0].point, Point { x: 212, y: 100 });
        assert_eq!(layout.paths[0].nodes[1].point, Point { x: 324, y: 100 });
        assert_eq!(
            layout.paths[0].edges.last().map(|edge| edge.kind),
            Some(GraphViewportEdgeKind::Merge)
        );
    }

    #[test]
    fn separates_collapsed_relationship_paths_and_deduplicates_details() {
        let path = |kind| AnchorRangePath {
            nodes: vec![
                node("source", None),
                node("shared", Some(GraphViewportEdgeKind::Primary)),
                node("target", Some(kind)),
            ],
        };
        let layout = layout_anchor_range(
            Point { x: 100, y: 200 },
            Point { x: 324, y: 200 },
            vec![
                path(GraphViewportEdgeKind::Merge),
                path(GraphViewportEdgeKind::Shadow),
            ],
            &[],
        );

        assert_eq!(layout.detail_count, 1);
        assert_eq!(
            layout.paths[1].nodes[0].point.y - layout.paths[0].nodes[0].point.y,
            LANE_ROW_STEP
        );
    }

    #[test]
    fn expanded_and_base_coordinates_round_trip() {
        let transform = AnchorRangeTransform {
            source_x: 100,
            target_x: 212,
            extra_width: 224,
        };

        for x in [40, 100, 128, 184, 212, 340, 500] {
            let expanded = transform.transform_x(x);
            assert!(
                (transform.inverse_x(f64::from(expanded)) - f64::from(x)).abs() < 1.0,
                "x={x} expanded={expanded}"
            );
        }
        assert_eq!(transform.transform_x(212), 436);
        assert_eq!(transform.transform_x(184), 184);
        assert_eq!(transform.transform_x(500), 724);
    }

    #[test]
    fn inserted_detail_rank_does_not_displace_intermediate_anchor() {
        let paths = vec![AnchorRangePath {
            nodes: vec![
                node("source", None),
                node("detail", Some(GraphViewportEdgeKind::Merge)),
                node("target", Some(GraphViewportEdgeKind::Merge)),
            ],
        }];
        let transform = AnchorRangeTransform {
            source_x: 100,
            target_x: 324,
            extra_width: DETAIL_RANK_STEP,
        };
        let expanded_target = Point {
            x: transform.transform_x(324),
            y: 100,
        };
        let layout = layout_anchor_range(Point { x: 100, y: 100 }, expanded_target, paths, &[]);

        assert_eq!(transform.transform_x(212), 212);
        assert_eq!(expanded_target.x, 436);
        assert_eq!(layout.paths[0].nodes[0].point.x, 324);
    }

    #[test]
    fn graph_route_preserves_endpoint_and_tangent_offsets() {
        let transform = AnchorRangeTransform {
            source_x: 100,
            target_x: 212,
            extra_width: 112,
        };
        let route = GraphBezierRoute {
            source: Point { x: 120, y: 80 },
            control_1: Point { x: 150, y: 80 },
            control_2: Point { x: 168, y: 80 },
            target: Point { x: 188, y: 80 },
        };

        assert_eq!(
            transform_graph_route(route, Some(100), Some(212), transform),
            GraphBezierRoute {
                source: Point { x: 120, y: 80 },
                control_1: Point { x: 150, y: 80 },
                control_2: Point { x: 280, y: 80 },
                target: Point { x: 300, y: 80 },
            }
        );
    }

    #[test]
    fn graph_route_derives_missing_node_centers_from_endpoint_ports() {
        let transform = AnchorRangeTransform {
            source_x: 100,
            target_x: 212,
            extra_width: 112,
        };
        let route = GraphBezierRoute {
            source: Point { x: 120, y: 80 },
            control_1: Point { x: 150, y: 80 },
            control_2: Point { x: 168, y: 80 },
            target: Point { x: 188, y: 80 },
        };

        assert_eq!(
            transform_graph_route(route, None, None, transform),
            GraphBezierRoute {
                source: Point { x: 120, y: 80 },
                control_1: Point { x: 150, y: 80 },
                control_2: Point { x: 280, y: 80 },
                target: Point { x: 300, y: 80 },
            }
        );
    }

    #[test]
    fn anchor_range_route_meets_horizontal_node_boundaries() {
        let route = route_anchor_range_edge(
            AnchorRangeLayoutEdge {
                kind: GraphViewportEdgeKind::Primary,
                source: Point { x: 100, y: 80 },
                target: Point { x: 212, y: 80 },
            },
            18.0,
        );

        assert_eq!(
            route,
            GraphBezierRoute {
                source: Point { x: 118, y: 80 },
                control_1: Point { x: 156, y: 80 },
                control_2: Point { x: 156, y: 80 },
                target: Point { x: 194, y: 80 },
            }
        );
    }

    #[test]
    fn anchor_range_route_meets_diagonal_node_boundaries() {
        let route = route_anchor_range_edge(
            AnchorRangeLayoutEdge {
                kind: GraphViewportEdgeKind::Merge,
                source: Point { x: 100, y: 100 },
                target: Point { x: 212, y: 172 },
            },
            18.0,
        );

        assert_eq!(route.source, Point { x: 115, y: 110 });
        assert_eq!(route.target, Point { x: 197, y: 162 });
    }

    #[test]
    fn expanded_details_skip_rows_occupied_by_other_edges() {
        let paths = vec![AnchorRangePath {
            nodes: vec![
                node("source", None),
                node("detail", Some(GraphViewportEdgeKind::Primary)),
                node("target", Some(GraphViewportEdgeKind::Primary)),
            ],
        }];
        let occupied = GraphBezierRoute {
            source: Point {
                x: 100,
                y: GRAPH_PADDING,
            },
            control_1: Point {
                x: 180,
                y: GRAPH_PADDING,
            },
            control_2: Point {
                x: 356,
                y: GRAPH_PADDING,
            },
            target: Point {
                x: 436,
                y: GRAPH_PADDING,
            },
        };

        let layout = layout_anchor_range(
            Point {
                x: 100,
                y: GRAPH_PADDING,
            },
            Point {
                x: 436,
                y: GRAPH_PADDING,
            },
            paths,
            &[occupied],
        );

        assert_eq!(
            layout.paths[0].nodes[0].point,
            Point {
                x: 324,
                y: GRAPH_PADDING + LANE_ROW_STEP,
            }
        );
    }

    #[test]
    fn temporary_path_order_is_stable_around_occupied_rows() {
        let path = |id| AnchorRangePath {
            nodes: vec![
                node("source", None),
                node(id, Some(GraphViewportEdgeKind::Primary)),
                node("target", Some(GraphViewportEdgeKind::Primary)),
            ],
        };
        let occupied = GraphBezierRoute {
            source: Point {
                x: 100,
                y: GRAPH_PADDING + LANE_ROW_STEP,
            },
            control_1: Point {
                x: 180,
                y: GRAPH_PADDING + LANE_ROW_STEP,
            },
            control_2: Point {
                x: 356,
                y: GRAPH_PADDING + LANE_ROW_STEP,
            },
            target: Point {
                x: 436,
                y: GRAPH_PADDING + LANE_ROW_STEP,
            },
        };

        let layout = layout_anchor_range(
            Point {
                x: 100,
                y: GRAPH_PADDING,
            },
            Point {
                x: 436,
                y: GRAPH_PADDING,
            },
            vec![path("first"), path("second")],
            &[occupied],
        );

        assert_eq!(layout.paths[0].nodes[0].point.y, GRAPH_PADDING);
        assert_eq!(
            layout.paths[1].nodes[0].point.y,
            GRAPH_PADDING + LANE_ROW_STEP * 2
        );
    }

    #[test]
    fn detail_keeps_its_position_when_the_occupied_route_has_clearance() {
        let paths = vec![AnchorRangePath {
            nodes: vec![
                node("source", None),
                node("detail", Some(GraphViewportEdgeKind::Primary)),
                node("target", Some(GraphViewportEdgeKind::Primary)),
            ],
        }];
        let occupied = GraphBezierRoute {
            source: Point { x: 140, y: 100 },
            control_1: Point { x: 170, y: 100 },
            control_2: Point { x: 328, y: 220 },
            target: Point { x: 348, y: 220 },
        };

        let layout = layout_anchor_range(
            Point { x: 120, y: 100 },
            Point { x: 372, y: 140 },
            paths,
            &[occupied],
        );

        assert_eq!(layout.paths[0].nodes[0].point, Point { x: 260, y: 120 });
    }

    #[test]
    fn occupied_edge_reserves_adjacent_row_for_unsnapped_detail() {
        let paths = vec![AnchorRangePath {
            nodes: vec![
                node("source", None),
                node("detail", Some(GraphViewportEdgeKind::Primary)),
                node("target", Some(GraphViewportEdgeKind::Primary)),
            ],
        }];
        let occupied = GraphBezierRoute {
            source: Point { x: 100, y: 90 },
            control_1: Point { x: 180, y: 90 },
            control_2: Point { x: 356, y: 90 },
            target: Point { x: 436, y: 90 },
        };

        let layout = layout_anchor_range(
            Point { x: 100, y: 104 },
            Point { x: 436, y: 104 },
            paths,
            &[occupied],
        );

        assert_eq!(layout.paths[0].nodes[0].point, Point { x: 324, y: 56 });
    }
}
