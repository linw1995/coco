use crate::api::{
    AnchorRangeNode, AnchorRangePath, GraphBezierRoute, GraphViewportEdgeKind, Point,
};

pub const DETAIL_RANK_STEP: i32 = 112;
const LANE_ROW_STEP: i32 = 72;
const RANGE_PADDING: i32 = 52;

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
    let endpoint_delta = |node_x: Option<i32>, endpoint_x: i32| {
        let anchor_x = node_x.unwrap_or(endpoint_x);
        transform.transform_x(anchor_x).saturating_sub(anchor_x)
    };
    let source_delta = endpoint_delta(source_node_x, route.source.x);
    let target_delta = endpoint_delta(target_node_x, route.target.x);
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
) -> AnchorRangeLayout {
    let inserted_left = target.x.saturating_sub(anchor_range_extra_width(&paths));
    let mut bounds = None;
    let mut detail_ids = std::collections::BTreeSet::new();
    let paths = paths
        .into_iter()
        .enumerate()
        .map(|(path_index, path)| {
            let detail_count = path.nodes.len().saturating_sub(2);
            let target_kind = path
                .nodes
                .last()
                .and_then(|node| node.incoming_edge)
                .unwrap_or(GraphViewportEdgeKind::Primary);
            let denominator = i64::try_from(detail_count + 1).unwrap_or(i64::MAX);
            let lane_offset = i32::try_from(path_index)
                .unwrap_or(i32::MAX)
                .saturating_mul(LANE_ROW_STEP);
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
                        y: interpolate(source.y, target.y, numerator, denominator)
                            .saturating_add(lane_offset),
                    };
                    detail_ids.insert(node.id.clone());
                    bounds
                        .get_or_insert_with(|| AnchorRangeBounds::from_points(point, point))
                        .include(point);
                    AnchorRangeLayoutNode { node, point }
                })
                .collect::<Vec<_>>();

            let mut points = Vec::with_capacity(nodes.len() + 2);
            points.push((source, GraphViewportEdgeKind::Primary));
            points.extend(nodes.iter().map(|node| {
                (
                    node.point,
                    node.node
                        .incoming_edge
                        .unwrap_or(GraphViewportEdgeKind::Primary),
                )
            }));
            points.push((target, target_kind));
            let edges = points
                .windows(2)
                .map(|points| AnchorRangeLayoutEdge {
                    kind: points[1].1,
                    source: points[0].0,
                    target: points[1].0,
                })
                .collect();
            AnchorRangeLayoutPath { nodes, edges }
        })
        .collect();

    AnchorRangeLayout {
        paths,
        bounds: bounds
            .unwrap_or_else(|| AnchorRangeBounds::from_points(source, target))
            .padded(RANGE_PADDING),
        detail_count: detail_ids.len(),
    }
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

        let layout = layout_anchor_range(Point { x: 100, y: 100 }, Point { x: 436, y: 100 }, paths);

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
        let layout = layout_anchor_range(Point { x: 100, y: 100 }, expanded_target, paths);

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
}
