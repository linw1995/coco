use crate::api::{AnchorRangeNode, AnchorRangePath, GraphViewportEdgeKind, Point};

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
        if x <= self.source_x || self.extra_width == 0 {
            return x;
        }
        if x >= self.target_x {
            return x.saturating_add(self.extra_width);
        }
        let source_span = i64::from(self.target_x.saturating_sub(self.source_x)).max(1);
        let expanded_span = source_span.saturating_add(i64::from(self.extra_width));
        let offset =
            i64::from(x.saturating_sub(self.source_x)).saturating_mul(expanded_span) / source_span;
        self.source_x
            .saturating_add(i32::try_from(offset).unwrap_or(i32::MAX))
    }

    pub fn inverse_x(self, x: f64) -> f64 {
        let source_x = f64::from(self.source_x);
        let target_x = f64::from(self.target_x);
        let expanded_target_x = target_x + f64::from(self.extra_width);
        if x <= source_x || self.extra_width == 0 {
            return x;
        }
        if x >= expanded_target_x {
            return x - f64::from(self.extra_width);
        }
        source_x + (x - source_x) * (target_x - source_x) / (expanded_target_x - source_x)
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
                        x: interpolate(source.x, target.x, numerator, denominator),
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
        assert_eq!(transform.transform_x(500), 724);
    }
}
