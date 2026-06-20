use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Double, Integer, Text};
use diesel::sqlite::SqliteConnection;
use snafu::prelude::*;

use crate::api::{
    GraphCanvas, GraphViewport, GraphViewportDiffResponse, GraphViewportEdge,
    GraphViewportEdgeKind, GraphViewportLane, GraphViewportNode, GraphViewportResponse, Point,
};
use crate::error::{
    ConnectGraphSnapshotStoreSnafu, ParseGraphSnapshotStoreValueSnafu, QueryGraphSnapshotStoreSnafu,
};
use crate::graph::{
    GraphMode, GraphSnapshot, graph_kind_name, node_target_id, shorten_id, summarize_node,
};
use crate::host::api::{GraphViewportDiffRequest, GraphViewportRequest};
use crate::layout::{
    EDGE_TARGET_PORT_STEP, GRAPH_COLUMN_WIDTH, GRAPH_LEFT_X, diff_graph_viewport_responses,
    lane_key, materialize_graph_viewport,
};
use coco_mem::{
    BranchStore, Kind, MergeParent, Node, NodeStore, PauseReason, SessionState, SessionStore,
};

const SQLITE_DATABASE_FILE_NAME: &str = "store.sqlite3";
const COORDINATE_SPACE: &str = "graph_layout_v1";
const NODE_RADIUS: i32 = 26;
const EDGE_TARGET_APPROACH: i32 = 48;
const GRAPH_LANE_HEIGHT: i32 = 140;
const EDGE_ROUTE_STEP: i32 = 12;
const MAX_EDGE_COLUMN_GAP: usize = 5;

#[derive(Clone, Debug)]
pub struct ConsoleGraphSnapshotStore {
    path: Arc<PathBuf>,
}

#[derive(Clone, QueryableByName)]
struct MaterializationRow {
    #[diesel(sql_type = BigInt)]
    source_version: i64,
    #[diesel(sql_type = Integer)]
    world_min_x: i32,
    #[diesel(sql_type = Integer)]
    world_min_y: i32,
    #[diesel(sql_type = Integer)]
    world_max_x: i32,
    #[diesel(sql_type = Integer)]
    world_max_y: i32,
}

#[derive(Clone, QueryableByName)]
struct LaneRow {
    #[diesel(sql_type = Text)]
    lane_key: String,
    #[diesel(sql_type = Text)]
    lane_label: String,
    #[diesel(sql_type = Integer)]
    lane_y: i32,
}

#[derive(QueryableByName)]
struct NodeLocationRow {
    #[diesel(sql_type = Text)]
    node_key: String,
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Text)]
    node_target: String,
    #[diesel(sql_type = Text)]
    short_id: String,
    #[diesel(sql_type = Text)]
    node_kind: String,
    #[diesel(sql_type = Text)]
    summary: String,
    #[diesel(sql_type = Text)]
    labels_json: String,
    #[diesel(sql_type = Integer)]
    x: i32,
    #[diesel(sql_type = Integer)]
    y: i32,
}

#[derive(QueryableByName)]
struct EdgeRouteRow {
    #[diesel(sql_type = Text)]
    edge_key: String,
    #[diesel(sql_type = Text)]
    edge_kind: String,
    #[diesel(sql_type = Text)]
    source_id: String,
    #[diesel(sql_type = Text)]
    target_id: String,
    #[diesel(sql_type = Integer)]
    source_x: i32,
    #[diesel(sql_type = Integer)]
    source_y: i32,
    #[diesel(sql_type = Integer)]
    target_x: i32,
    #[diesel(sql_type = Integer)]
    target_y: i32,
    #[diesel(sql_type = Integer)]
    route_slot: i32,
    #[diesel(sql_type = Double)]
    target_port_offset: f64,
}

#[derive(QueryableByName)]
struct MaterializedKeyRow {
    #[diesel(sql_type = Text)]
    item_key: String,
}

#[derive(QueryableByName)]
struct MaterializedTailNodeRow {
    #[diesel(sql_type = Text)]
    node_key: String,
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Text)]
    lane_key: String,
    #[diesel(sql_type = Text)]
    lane_label: String,
    #[diesel(sql_type = Integer)]
    lane_y: i32,
    #[diesel(sql_type = Integer)]
    x: i32,
    #[diesel(sql_type = Integer)]
    y: i32,
}

#[derive(QueryableByName)]
struct MaterializedNodePointRow {
    #[diesel(sql_type = Integer)]
    x: i32,
    #[diesel(sql_type = Integer)]
    y: i32,
}

#[derive(Clone, Debug)]
pub(crate) struct MaterializedGraphShellFacts {
    pub version: u64,
    pub lanes: Vec<GraphViewportLane>,
    pub nodes: Vec<MaterializedGraphShellNode>,
    pub edge_count: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct MaterializedGraphShellNode {
    pub node_id: String,
    pub point: Point,
}

struct NodeLocationInsert<'a> {
    mode: GraphMode,
    node: &'a GraphViewportNode,
    lane: &'a GraphViewportLane,
    bounds: ItemBounds,
}

pub(crate) struct MaterializedNodeReference {
    pub node_id: String,
    pub labels: Vec<String>,
}

struct EdgeRouteInsert<'a> {
    mode: GraphMode,
    edge: &'a GraphViewportEdge,
    bounds: ItemBounds,
}

struct AppendLinearBranchInput<'a> {
    mode: GraphMode,
    branch: &'a str,
    state: &'a SessionState,
    head_id: &'a str,
}

struct MaterializationMetaInput {
    source_version: u64,
    mode: GraphMode,
    world_min_x: i32,
    world_min_y: i32,
    world_max_x: i32,
    world_max_y: i32,
}

#[derive(QueryableByName)]
struct SqliteInteger {
    #[diesel(sql_type = Integer)]
    value: i32,
}

impl ConsoleGraphSnapshotStore {
    pub fn open(dir: impl AsRef<Path>) -> crate::Result<Self> {
        let path = dir.as_ref().join(SQLITE_DATABASE_FILE_NAME);
        let store = Self {
            path: Arc::new(path),
        };
        store.ensure_schema()?;
        Ok(store)
    }

    pub fn latest_viewport(
        &self,
        mode: GraphMode,
        request: GraphViewportRequest,
    ) -> crate::Result<Option<GraphViewportResponse>> {
        let Some(meta) = self.latest_materialization_row(mode)? else {
            return Ok(None);
        };
        self.viewport_from_row(mode, meta, request)
    }

    pub fn latest_viewport_diff(
        &self,
        mode: GraphMode,
        request: GraphViewportDiffRequest,
    ) -> crate::Result<Option<GraphViewportDiffResponse>> {
        let Some(meta) = self.latest_materialization_row(mode)? else {
            return Ok(None);
        };
        self.viewport_diff_from_row(mode, meta, request)
    }

    pub(crate) fn materialized_node_reference(
        &self,
        mode: GraphMode,
        target: &str,
    ) -> crate::Result<Option<MaterializedNodeReference>> {
        let mut connection = self.connect()?;
        let row = sql_query(
            r#"
SELECT node_key, node_id, node_target, short_id, node_kind, summary, labels_json, x, y
FROM console_graph_node_locations
WHERE mode = ? AND node_target = ?
ORDER BY y, x, node_key
LIMIT 1
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Text, _>(target)
        .get_result::<NodeLocationRow>(&mut connection)
        .optional()
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        row.map(|row| {
            let labels = serde_json::from_str::<Vec<String>>(&row.labels_json).context(
                ParseGraphSnapshotStoreValueSnafu {
                    column: "console_graph_node_locations.labels_json",
                },
            )?;
            Ok(MaterializedNodeReference {
                node_id: row.node_id,
                labels,
            })
        })
        .transpose()
    }

    pub(crate) fn materialized_node_points(
        &self,
        mode: GraphMode,
        node_ids: &BTreeSet<String>,
    ) -> crate::Result<BTreeMap<String, Point>> {
        let mut connection = self.connect()?;
        let mut points = BTreeMap::new();
        for node_id in node_ids {
            if let Some(point) =
                self.materialized_node_point_in_connection(&mut connection, mode, node_id)?
            {
                points.insert(node_id.clone(), point);
            }
        }
        Ok(points)
    }

    pub(crate) fn materialized_shell_facts(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Option<MaterializedGraphShellFacts>> {
        let mut connection = self.connect()?;
        let Some(meta) = self.latest_materialization_row_in_connection(&mut connection, mode)?
        else {
            return Ok(None);
        };
        let lanes = self
            .materialized_lanes_in_connection(&mut connection, mode)?
            .into_iter()
            .map(|row| GraphViewportLane {
                key: row.lane_key,
                label: row.lane_label,
                y: row.lane_y,
            })
            .collect();
        let mut nodes_by_id = BTreeMap::new();
        for row in self.materialized_node_rows_in_connection(&mut connection, mode)? {
            nodes_by_id
                .entry(row.node_id)
                .or_insert(Point { x: row.x, y: row.y });
        }
        let nodes = nodes_by_id
            .into_iter()
            .map(|(node_id, point)| MaterializedGraphShellNode { node_id, point })
            .collect();
        Ok(Some(MaterializedGraphShellFacts {
            version: meta.source_version.max(0) as u64,
            lanes,
            nodes,
            edge_count: self.materialized_edge_count_in_connection(&mut connection, mode)?,
        }))
    }

    pub fn put(&self, source_version: u64, snapshot: &GraphSnapshot) -> crate::Result<()> {
        let materialized = materialize_graph_viewport(snapshot);
        let mut connection = self.connect()?;
        self.begin_write_transaction(&mut connection)?;
        let result = self.put_materialized_graph(
            &mut connection,
            source_version,
            snapshot.mode,
            &materialized,
        );
        match result {
            Ok(()) => self.commit_transaction(&mut connection),
            Err(error) => {
                let _ = self.rollback_transaction(&mut connection);
                Err(error)
            }
        }
    }

    pub fn try_append_linear_branch(
        &self,
        source_version: u64,
        mode: GraphMode,
        store: &(impl BranchStore + NodeStore + SessionStore),
    ) -> crate::Result<bool> {
        let mut session_states = store
            .list_session_states()
            .context(crate::error::StoreSnafu)?
            .into_iter()
            .collect::<Vec<_>>();
        if session_states.is_empty() {
            return Ok(false);
        }
        session_states.sort_by(|(left, _), (right, _)| {
            branch_lane_priority(left).cmp(&branch_lane_priority(right))
        });

        let mut connection = self.connect()?;
        self.begin_write_transaction(&mut connection)?;
        let result = match mode {
            GraphMode::Anchors => self.try_update_anchor_materialization_in_transaction(
                &mut connection,
                store,
                source_version,
                &session_states,
            ),
            GraphMode::All => self.try_append_linear_branches_in_transaction(
                &mut connection,
                store,
                source_version,
                mode,
                &session_states,
            ),
        };
        match result {
            Ok(true) => {
                self.commit_transaction(&mut connection)?;
                Ok(true)
            }
            Ok(false) => {
                let _ = self.rollback_transaction(&mut connection);
                Ok(false)
            }
            Err(error) => {
                let _ = self.rollback_transaction(&mut connection);
                Err(error)
            }
        }
    }

    fn try_append_linear_branch_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        input: AppendLinearBranchInput<'_>,
    ) -> crate::Result<bool> {
        let Some(tail) =
            self.latest_lane_tail_in_connection(connection, input.mode, input.branch)?
        else {
            return Ok(false);
        };
        let branch_label = branch_label(input.branch, input.state);
        if input.head_id == tail.node_id {
            self.update_node_labels(connection, input.mode, &tail.node_key, vec![branch_label])?;
            return Ok(true);
        }
        if let Some(head) = self.materialized_lane_node_in_connection(
            connection,
            input.mode,
            input.branch,
            input.head_id,
        )? && head.x < tail.x
        {
            self.delete_materialized_lane_suffix(
                connection,
                input.mode,
                input.branch,
                head.x,
                head.y,
            )?;
            self.update_node_labels(connection, input.mode, &head.node_key, vec![branch_label])?;
            return Ok(true);
        }
        let Ok(mut chain) = store.log(&tail.node_id, input.head_id) else {
            return Ok(false);
        };
        chain.reverse();
        if chain.first().is_none_or(|node| node.id != tail.node_id) {
            return Ok(false);
        }
        if !is_linear_primary_chain(&chain) {
            return Ok(false);
        }

        self.update_node_labels(connection, input.mode, &tail.node_key, Vec::new())?;
        let lane = GraphViewportLane {
            key: tail.lane_key,
            label: tail.lane_label,
            y: tail.lane_y,
        };
        let appended_nodes = chain.into_iter().skip(1).collect::<Vec<_>>();
        let event_order = self.event_order_by_materialized_and_new_nodes(
            connection,
            store,
            input.mode,
            &appended_nodes,
        )?;
        let mut previous_id = tail.node_id;
        let mut previous_point = Point {
            x: tail.x,
            y: tail.y,
        };
        let appended_len = appended_nodes.len();
        for (index, node) in appended_nodes.into_iter().enumerate() {
            let point = Point {
                x: previous_point.x
                    + required_column_gap(&previous_id, &node.id, &event_order)
                        * GRAPH_COLUMN_WIDTH,
                y: previous_point.y,
            };
            let labels = if index + 1 == appended_len {
                vec![branch_label.clone()]
            } else {
                Vec::new()
            };
            let viewport_node = graph_viewport_node_from_node(&node, point, labels);
            self.insert_node_location(
                connection,
                NodeLocationInsert {
                    mode: input.mode,
                    node: &viewport_node,
                    lane: &lane,
                    bounds: node_bounds(&viewport_node),
                },
            )?;
            let edge = primary_parent_edge(&previous_id, previous_point, &node.id, point);
            self.insert_edge_route(
                connection,
                EdgeRouteInsert {
                    mode: input.mode,
                    edge: &edge,
                    bounds: edge_bounds(&edge),
                },
            )?;
            if !self.insert_node_merge_edges(
                connection,
                store,
                input.mode,
                &node,
                &previous_id,
                point,
            )? {
                return Ok(false);
            }
            previous_id = node.id;
            previous_point = point;
        }
        Ok(true)
    }

    fn try_update_anchor_materialization_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &(impl BranchStore + NodeStore),
        source_version: u64,
        session_states: &[(String, SessionState)],
    ) -> crate::Result<bool> {
        let mode = GraphMode::Anchors;
        let Some(meta) = self.latest_materialization_row_in_connection(connection, mode)? else {
            return Ok(false);
        };
        if meta.source_version >= 0 && source_version <= meta.source_version as u64 {
            return Ok(true);
        }
        let mut materialized_lanes = self.materialized_lanes_in_connection(connection, mode)?;
        let branch_names = session_states
            .iter()
            .map(|(branch, _)| branch.clone())
            .collect::<BTreeSet<_>>();
        let removed_lanes = removed_lanes_in_order(&materialized_lanes, &branch_names);
        if !removed_lanes.is_empty() {
            self.delete_materialized_lanes(connection, mode, &removed_lanes)?;
            self.shift_lanes_after_deletion(connection, mode, &removed_lanes)?;
            materialized_lanes = self.materialized_lanes_in_connection(connection, mode)?;
        }
        let materialized_lane_labels = materialized_lanes
            .iter()
            .map(|lane| lane.lane_label.clone())
            .collect::<BTreeSet<_>>();
        if !existing_branch_lanes_preserve_order(
            session_states,
            &materialized_lanes,
            &materialized_lane_labels,
        ) {
            return Ok(false);
        }

        let mut materialized_lane_labels = materialized_lane_labels;
        let mut next_lane_y = crate::layout::GRAPH_TOP_Y;
        for (branch, state) in session_states {
            let head_id = store
                .get_branch_head(branch)
                .context(crate::error::StoreSnafu)?;
            let appended = if materialized_lane_labels.contains(branch) {
                let ancestry = store.ancestry(&head_id).context(crate::error::StoreSnafu)?;
                let Some(visible_head) = self
                    .first_materialized_lane_ancestry_node(connection, mode, branch, &ancestry)?
                else {
                    return Ok(false);
                };
                let Some(tail) = self.latest_lane_tail_in_connection(connection, mode, branch)?
                else {
                    return Ok(false);
                };
                if visible_head.x < tail.x {
                    self.delete_materialized_lane_suffix(
                        connection,
                        mode,
                        branch,
                        visible_head.x,
                        visible_head.y,
                    )?;
                }
                self.try_append_anchor_branch_after_row(
                    connection,
                    store,
                    AppendLinearBranchInput {
                        mode,
                        branch,
                        state,
                        head_id: &head_id,
                    },
                    visible_head,
                )?
            } else {
                self.shift_lanes_for_insertion(connection, mode, next_lane_y)?;
                let appended = self.try_append_new_anchor_branch_lane_in_transaction(
                    connection,
                    store,
                    AppendLinearBranchInput {
                        mode,
                        branch,
                        state,
                        head_id: &head_id,
                    },
                    next_lane_y,
                )?;
                if appended {
                    materialized_lane_labels.insert(branch.clone());
                }
                appended
            };
            if !appended {
                return Ok(false);
            }
            next_lane_y += GRAPH_LANE_HEIGHT;
        }

        let mut labels_by_node_id = BTreeMap::<String, Vec<String>>::new();
        for (branch, state) in session_states {
            let head_id = store
                .get_branch_head(branch)
                .context(crate::error::StoreSnafu)?;
            let ancestry = store.ancestry(&head_id).context(crate::error::StoreSnafu)?;
            let Some(row) =
                self.first_materialized_lane_ancestry_node(connection, mode, branch, &ancestry)?
            else {
                return Ok(false);
            };
            labels_by_node_id
                .entry(row.node_id)
                .or_default()
                .push(branch_label(branch, state));
        }
        for labels in labels_by_node_id.values_mut() {
            labels.sort();
        }
        let materialized_nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        for row in &materialized_nodes {
            let labels = labels_by_node_id
                .get(&row.node_id)
                .cloned()
                .unwrap_or_default();
            self.update_node_labels(connection, mode, &row.node_key, labels)?;
        }
        let world_max_x = materialized_nodes
            .iter()
            .map(|row| row.x)
            .max()
            .unwrap_or(meta.world_max_x - 120)
            + 120;
        let world_max_y = materialized_nodes
            .iter()
            .map(|row| row.y)
            .max()
            .unwrap_or(meta.world_max_y - 120)
            + 120;
        self.put_materialization_meta(
            connection,
            MaterializationMetaInput {
                source_version,
                mode,
                world_min_x: meta.world_min_x,
                world_min_y: meta.world_min_y,
                world_max_x,
                world_max_y,
            },
        )?;
        Ok(true)
    }

    fn try_append_new_anchor_branch_lane_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        input: AppendLinearBranchInput<'_>,
        lane_y: i32,
    ) -> crate::Result<bool> {
        let ancestry = store
            .ancestry(input.head_id)
            .context(crate::error::StoreSnafu)?;
        let visible_chain = ancestry
            .iter()
            .rev()
            .filter(|node| is_anchor_node(node))
            .cloned()
            .collect::<Vec<_>>();
        if visible_chain.is_empty() {
            return Ok(false);
        }

        let covered_before_lane = self
            .materialized_node_rows_in_connection(connection, input.mode)?
            .into_iter()
            .filter(|row| row.y < lane_y)
            .map(|row| row.node_id)
            .collect::<BTreeSet<_>>();
        let first_new = visible_chain
            .iter()
            .position(|node| !covered_before_lane.contains(&node.id))
            .unwrap_or_else(|| visible_chain.len().saturating_sub(1));
        let nodes = visible_chain[first_new..].to_vec();
        if nodes.is_empty() {
            return Ok(false);
        }

        let fork_source = first_new
            .checked_sub(1)
            .and_then(|index| visible_chain.get(index));
        let mut previous = match fork_source {
            Some(source) => {
                let Some(source_point) =
                    self.materialized_node_point_in_connection(connection, input.mode, &source.id)?
                else {
                    return Ok(false);
                };
                Some((source.id.clone(), source_point))
            }
            None => None,
        };

        let lane = GraphViewportLane {
            key: lane_key(input.branch),
            label: input.branch.to_owned(),
            y: lane_y,
        };
        let branch_label = branch_label(input.branch, input.state);
        let appended_len = nodes.len();
        let event_order =
            self.event_order_by_materialized_and_new_nodes(connection, store, input.mode, &nodes)?;
        for (index, node) in nodes.into_iter().enumerate() {
            let point = match previous.as_ref() {
                Some((previous_id, previous_point)) => Point {
                    x: previous_point.x
                        + required_column_gap(previous_id, &node.id, &event_order)
                            * GRAPH_COLUMN_WIDTH,
                    y: lane_y,
                },
                None => Point {
                    x: GRAPH_LEFT_X,
                    y: lane_y,
                },
            };
            let labels = if index + 1 == appended_len {
                vec![branch_label.clone()]
            } else {
                Vec::new()
            };
            let viewport_node = graph_viewport_node_from_node(&node, point, labels);
            self.insert_node_location(
                connection,
                NodeLocationInsert {
                    mode: input.mode,
                    node: &viewport_node,
                    lane: &lane,
                    bounds: node_bounds(&viewport_node),
                },
            )?;
            if let Some((previous_id, previous_point)) = previous.as_ref() {
                let edge = if index == 0 && fork_source.is_some() {
                    routed_edge(
                        GraphViewportEdgeKind::Fork,
                        previous_id,
                        *previous_point,
                        &node.id,
                        point,
                        self.next_routed_edge_slot_in_connection(
                            connection,
                            input.mode,
                            *previous_point,
                            point,
                        )?,
                    )
                } else {
                    primary_parent_edge(previous_id, *previous_point, &node.id, point)
                };
                self.insert_edge_route(
                    connection,
                    EdgeRouteInsert {
                        mode: input.mode,
                        edge: &edge,
                        bounds: edge_bounds(&edge),
                    },
                )?;
                if !self.insert_node_merge_edges(
                    connection,
                    store,
                    input.mode,
                    &node,
                    previous_id,
                    point,
                )? {
                    return Ok(false);
                }
            } else if !self
                .insert_node_merge_edges(connection, store, input.mode, &node, "", point)?
            {
                return Ok(false);
            }
            previous = Some((node.id, point));
        }
        Ok(true)
    }

    fn try_append_anchor_branch_after_row(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        input: AppendLinearBranchInput<'_>,
        tail: MaterializedTailNodeRow,
    ) -> crate::Result<bool> {
        if input.head_id == tail.node_id {
            return Ok(true);
        }
        let Ok(mut chain) = store.log(&tail.node_id, input.head_id) else {
            return Ok(false);
        };
        chain.reverse();
        if chain.first().is_none_or(|node| node.id != tail.node_id) {
            return Ok(false);
        }
        if !is_linear_primary_chain(&chain) {
            return Ok(false);
        }

        let lane = GraphViewportLane {
            key: tail.lane_key,
            label: tail.lane_label,
            y: tail.lane_y,
        };
        let appended_nodes = chain
            .into_iter()
            .skip(1)
            .filter(is_anchor_node)
            .collect::<Vec<_>>();
        let event_order = self.event_order_by_materialized_and_new_nodes(
            connection,
            store,
            input.mode,
            &appended_nodes,
        )?;
        let mut previous_id = tail.node_id;
        let mut previous_point = Point {
            x: tail.x,
            y: tail.y,
        };
        for node in appended_nodes {
            let point = Point {
                x: previous_point.x
                    + required_column_gap(&previous_id, &node.id, &event_order)
                        * GRAPH_COLUMN_WIDTH,
                y: previous_point.y,
            };
            let viewport_node = graph_viewport_node_from_node(&node, point, Vec::new());
            self.insert_node_location(
                connection,
                NodeLocationInsert {
                    mode: input.mode,
                    node: &viewport_node,
                    lane: &lane,
                    bounds: node_bounds(&viewport_node),
                },
            )?;
            let edge = primary_parent_edge(&previous_id, previous_point, &node.id, point);
            self.insert_edge_route(
                connection,
                EdgeRouteInsert {
                    mode: input.mode,
                    edge: &edge,
                    bounds: edge_bounds(&edge),
                },
            )?;
            if !self.insert_node_merge_edges(
                connection,
                store,
                input.mode,
                &node,
                &previous_id,
                point,
            )? {
                return Ok(false);
            }
            previous_id = node.id;
            previous_point = point;
        }
        Ok(true)
    }

    fn insert_node_merge_edges(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        mode: GraphMode,
        node: &Node,
        primary_parent_id: &str,
        target: Point,
    ) -> crate::Result<bool> {
        let mut parent_ids = BTreeSet::from([primary_parent_id.to_owned()]);
        for merge_parent in node_anchor_merge_parents(node) {
            let Some((source_id, source)) =
                self.visible_merge_parent_point(connection, store, mode, merge_parent)?
            else {
                return Ok(false);
            };
            if !parent_ids.insert(source_id.clone()) {
                continue;
            }
            let edge = routed_edge(
                GraphViewportEdgeKind::MergeParent,
                &source_id,
                source,
                &node.id,
                target,
                self.next_routed_edge_slot_in_connection(connection, mode, source, target)?,
            );
            self.insert_edge_route(
                connection,
                EdgeRouteInsert {
                    mode,
                    edge: &edge,
                    bounds: edge_bounds(&edge),
                },
            )?;
            self.rebalance_target_port_offsets(connection, mode, target)?;
        }
        Ok(true)
    }

    fn visible_merge_parent_point(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        mode: GraphMode,
        merge_parent: &MergeParent,
    ) -> crate::Result<Option<(String, Point)>> {
        let ancestry = store
            .ancestry(merge_parent.node_id())
            .context(crate::error::StoreSnafu)?;
        let Some((source_index, source_point)) =
            self.first_materialized_ancestry_point(connection, mode, &ancestry)?
        else {
            return Ok(None);
        };
        Ok(ancestry
            .get(source_index)
            .map(|source| (source.id.clone(), source_point)))
    }

    fn rebalance_target_port_offsets(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        target: Point,
    ) -> crate::Result<()> {
        let rows = sql_query(
            r#"
SELECT edge_key, edge_kind, source_id, target_id, source_x, source_y, target_x, target_y, route_slot, target_port_offset
FROM console_graph_edge_routes
WHERE mode = ?
  AND target_x = ?
  AND target_y = ?
ORDER BY
  CASE edge_kind
    WHEN 'primary_parent' THEN 0
    WHEN 'fork' THEN 1
    ELSE 2
  END,
  edge_key
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Integer, _>(target.x)
        .bind::<Integer, _>(target.y)
        .load::<EdgeRouteRow>(&mut *connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        let mut primary_edges = Vec::new();
        let mut secondary_edges = Vec::new();
        for row in rows {
            let kind = parse_edge_kind(&row.edge_kind)?;
            let edge = GraphViewportEdge {
                key: row.edge_key,
                kind,
                source_id: row.source_id,
                target_id: row.target_id,
                source: Point {
                    x: row.source_x,
                    y: row.source_y,
                },
                target: Point {
                    x: row.target_x,
                    y: row.target_y,
                },
                route_slot: row.route_slot,
                target_port_offset: row.target_port_offset,
            };
            if kind == GraphViewportEdgeKind::PrimaryParent {
                primary_edges.push(edge);
            } else {
                secondary_edges.push(edge);
            }
        }
        let primary_count = primary_edges.len();
        for (index, edge) in primary_edges.iter_mut().enumerate() {
            edge.target_port_offset = primary_incoming_port_offset(primary_count, index);
            self.insert_edge_route(
                connection,
                EdgeRouteInsert {
                    mode,
                    edge,
                    bounds: edge_bounds(edge),
                },
            )?;
        }
        let secondary_count = secondary_edges.len();
        for (index, edge) in secondary_edges.iter_mut().enumerate() {
            edge.target_port_offset = if primary_count > 0 {
                secondary_incoming_port_offset(index)
            } else {
                primary_incoming_port_offset(secondary_count, index)
            };
            self.insert_edge_route(
                connection,
                EdgeRouteInsert {
                    mode,
                    edge,
                    bounds: edge_bounds(edge),
                },
            )?;
        }
        Ok(())
    }

    fn try_append_new_branch_lane_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        input: AppendLinearBranchInput<'_>,
        lane_y: i32,
    ) -> crate::Result<bool> {
        let ancestry = store
            .ancestry(input.head_id)
            .context(crate::error::StoreSnafu)?;
        let Some((source_index, source_point)) =
            self.first_materialized_ancestry_point(connection, input.mode, &ancestry)?
        else {
            return Ok(false);
        };
        let (source, nodes, source_point) = if source_index == 0 {
            let Some(source) = ancestry.get(1) else {
                return Ok(false);
            };
            let Some(source_point) =
                self.materialized_node_point_in_connection(connection, input.mode, &source.id)?
            else {
                return Ok(false);
            };
            (source, vec![ancestry[0].clone()], source_point)
        } else {
            let source = &ancestry[source_index];
            let mut nodes = ancestry[..source_index].to_vec();
            nodes.reverse();
            (source, nodes, source_point)
        };
        if nodes.is_empty() || !is_linear_new_nodes(&source.id, &nodes) {
            return Ok(false);
        }

        let lane = GraphViewportLane {
            key: lane_key(input.branch),
            label: input.branch.to_owned(),
            y: lane_y,
        };
        let branch_label = branch_label(input.branch, input.state);
        let mut previous_id = source.id.clone();
        let mut previous_point = source_point;
        let appended_len = nodes.len();
        let event_order =
            self.event_order_by_materialized_and_new_nodes(connection, store, input.mode, &nodes)?;
        for (index, node) in nodes.into_iter().enumerate() {
            let point = Point {
                x: previous_point.x
                    + required_column_gap(&previous_id, &node.id, &event_order)
                        * GRAPH_COLUMN_WIDTH,
                y: lane_y,
            };
            let labels = if index + 1 == appended_len {
                vec![branch_label.clone()]
            } else {
                Vec::new()
            };
            let viewport_node = graph_viewport_node_from_node(&node, point, labels);
            self.insert_node_location(
                connection,
                NodeLocationInsert {
                    mode: input.mode,
                    node: &viewport_node,
                    lane: &lane,
                    bounds: node_bounds(&viewport_node),
                },
            )?;
            let edge = if index == 0 {
                routed_edge(
                    GraphViewportEdgeKind::Fork,
                    &previous_id,
                    previous_point,
                    &node.id,
                    point,
                    self.next_routed_edge_slot_in_connection(
                        connection,
                        input.mode,
                        previous_point,
                        point,
                    )?,
                )
            } else {
                primary_parent_edge(&previous_id, previous_point, &node.id, point)
            };
            self.insert_edge_route(
                connection,
                EdgeRouteInsert {
                    mode: input.mode,
                    edge: &edge,
                    bounds: edge_bounds(&edge),
                },
            )?;
            if !self.insert_node_merge_edges(
                connection,
                store,
                input.mode,
                &node,
                &previous_id,
                point,
            )? {
                return Ok(false);
            }
            previous_id = node.id;
            previous_point = point;
        }
        Ok(true)
    }

    fn try_append_linear_branches_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &(impl BranchStore + NodeStore),
        source_version: u64,
        mode: GraphMode,
        session_states: &[(String, SessionState)],
    ) -> crate::Result<bool> {
        let Some(meta) = self.latest_materialization_row_in_connection(connection, mode)? else {
            return Ok(false);
        };
        if meta.source_version >= 0 && source_version <= meta.source_version as u64 {
            return Ok(true);
        }

        let mut materialized_lanes = self.materialized_lanes_in_connection(connection, mode)?;
        let branch_names = session_states
            .iter()
            .map(|(branch, _)| branch.clone())
            .collect::<BTreeSet<_>>();
        let removed_lanes = removed_lanes_in_order(&materialized_lanes, &branch_names);
        if !removed_lanes.is_empty() {
            self.delete_materialized_lanes(connection, mode, &removed_lanes)?;
            self.shift_lanes_after_deletion(connection, mode, &removed_lanes)?;
            materialized_lanes = self.materialized_lanes_in_connection(connection, mode)?;
        }
        let materialized_lane_labels = materialized_lanes
            .iter()
            .map(|lane| lane.lane_label.clone())
            .collect::<BTreeSet<_>>();
        if !existing_branch_lanes_preserve_order(
            session_states,
            &materialized_lanes,
            &materialized_lane_labels,
        ) {
            return Ok(false);
        }

        let mut world_max_x = meta.world_max_x;
        let mut materialized_lane_labels = materialized_lane_labels;
        let mut next_lane_y = crate::layout::GRAPH_TOP_Y;
        for (branch, state) in session_states {
            let head_id = store
                .get_branch_head(branch)
                .context(crate::error::StoreSnafu)?;
            let appended = if materialized_lane_labels.contains(branch) {
                self.try_append_linear_branch_in_transaction(
                    connection,
                    store,
                    AppendLinearBranchInput {
                        mode,
                        branch,
                        state,
                        head_id: &head_id,
                    },
                )?
            } else {
                self.shift_lanes_for_insertion(connection, mode, next_lane_y)?;
                let appended = self.try_append_new_branch_lane_in_transaction(
                    connection,
                    store,
                    AppendLinearBranchInput {
                        mode,
                        branch,
                        state,
                        head_id: &head_id,
                    },
                    next_lane_y,
                )?;
                if appended {
                    materialized_lane_labels.insert(branch.clone());
                }
                appended
            };
            if !appended {
                return Ok(false);
            }
            let Some(tail) = self.latest_lane_tail_in_connection(connection, mode, branch)? else {
                return Ok(false);
            };
            world_max_x = world_max_x.max(tail.x + 120);
            next_lane_y += GRAPH_LANE_HEIGHT;
        }
        let world_max_y = self
            .materialized_lanes_in_connection(connection, mode)?
            .iter()
            .map(|lane| lane.lane_y)
            .max()
            .unwrap_or(crate::layout::GRAPH_TOP_Y - GRAPH_LANE_HEIGHT)
            + 120;

        self.put_materialization_meta(
            connection,
            MaterializationMetaInput {
                source_version,
                mode,
                world_min_x: meta.world_min_x,
                world_min_y: meta.world_min_y,
                world_max_x,
                world_max_y,
            },
        )?;
        Ok(true)
    }

    fn put_materialized_graph(
        &self,
        connection: &mut SqliteConnection,
        source_version: u64,
        mode: GraphMode,
        materialized: &GraphViewportResponse,
    ) -> crate::Result<()> {
        self.put_materialized_viewport(connection, source_version, mode, materialized)
    }

    fn begin_write_transaction(&self, connection: &mut SqliteConnection) -> crate::Result<()> {
        sql_query("BEGIN IMMEDIATE TRANSACTION")
            .execute(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        Ok(())
    }

    fn commit_transaction(&self, connection: &mut SqliteConnection) -> crate::Result<()> {
        sql_query("COMMIT")
            .execute(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        Ok(())
    }

    fn rollback_transaction(&self, connection: &mut SqliteConnection) -> crate::Result<()> {
        sql_query("ROLLBACK")
            .execute(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        Ok(())
    }

    fn ensure_schema(&self) -> crate::Result<()> {
        let mut connection = self.connect()?;
        self.drop_legacy_materialization_tables(&mut connection)?;
        sql_query(
            r#"
CREATE TABLE IF NOT EXISTS console_graph_materializations (
    mode TEXT PRIMARY KEY NOT NULL,
    source_version INTEGER NOT NULL,
    coordinate_space TEXT NOT NULL,
    world_min_x INTEGER NOT NULL,
    world_min_y INTEGER NOT NULL,
    world_max_x INTEGER NOT NULL,
    world_max_y INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
)
"#,
        )
        .execute(&mut connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        sql_query(
            r#"
CREATE TABLE IF NOT EXISTS console_graph_node_locations (
    mode TEXT NOT NULL,
    node_key TEXT NOT NULL,
    node_id TEXT NOT NULL,
    node_target TEXT NOT NULL,
    short_id TEXT NOT NULL,
    node_kind TEXT NOT NULL,
    summary TEXT NOT NULL,
    labels_json TEXT NOT NULL,
    lane_key TEXT NOT NULL,
    lane_label TEXT NOT NULL,
    lane_y INTEGER NOT NULL,
    x INTEGER NOT NULL,
    y INTEGER NOT NULL,
    min_x INTEGER NOT NULL,
    min_y INTEGER NOT NULL,
    max_x INTEGER NOT NULL,
    max_y INTEGER NOT NULL,
    PRIMARY KEY (mode, node_key)
)
"#,
        )
        .execute(&mut connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        sql_query(
            "CREATE INDEX IF NOT EXISTS console_graph_node_locations_viewport_idx ON console_graph_node_locations(mode, min_x, min_y, max_x, max_y)",
        )
        .execute(&mut connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        sql_query(
            "CREATE INDEX IF NOT EXISTS console_graph_node_locations_lane_idx ON console_graph_node_locations(mode, lane_y, lane_key)",
        )
        .execute(&mut connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        sql_query(
            r#"
CREATE TABLE IF NOT EXISTS console_graph_edge_routes (
    mode TEXT NOT NULL,
    edge_key TEXT NOT NULL,
    edge_kind TEXT NOT NULL,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    source_x INTEGER NOT NULL,
    source_y INTEGER NOT NULL,
    target_x INTEGER NOT NULL,
    target_y INTEGER NOT NULL,
    route_slot INTEGER NOT NULL,
    target_port_offset REAL NOT NULL,
    min_x INTEGER NOT NULL,
    min_y INTEGER NOT NULL,
    max_x INTEGER NOT NULL,
    max_y INTEGER NOT NULL,
    PRIMARY KEY (mode, edge_key)
)
"#,
        )
        .execute(&mut connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        sql_query(
            "CREATE INDEX IF NOT EXISTS console_graph_edge_routes_viewport_idx ON console_graph_edge_routes(mode, min_x, min_y, max_x, max_y)",
        )
        .execute(&mut connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    fn drop_legacy_materialization_tables(
        &self,
        connection: &mut SqliteConnection,
    ) -> crate::Result<()> {
        for table in [
            "console_graph_snapshots",
            "console_graph_viewports",
            "console_graph_viewport_lanes",
            "console_graph_viewport_nodes",
            "console_graph_viewport_edges",
        ] {
            sql_query(format!("DROP TABLE IF EXISTS {table}"))
                .execute(&mut *connection)
                .context(QueryGraphSnapshotStoreSnafu {
                    path: self.path.as_ref().clone(),
                })?;
        }
        Ok(())
    }

    fn put_materialized_viewport(
        &self,
        connection: &mut SqliteConnection,
        source_version: u64,
        mode: GraphMode,
        materialized: &GraphViewportResponse,
    ) -> crate::Result<()> {
        self.put_materialization_meta(
            connection,
            MaterializationMetaInput {
                source_version,
                mode,
                world_min_x: 0,
                world_min_y: 0,
                world_max_x: materialized.canvas.width,
                world_max_y: materialized.canvas.height,
            },
        )?;
        self.replace_materialized_facts(connection, mode, materialized)
    }

    fn put_materialization_meta(
        &self,
        connection: &mut SqliteConnection,
        input: MaterializationMetaInput,
    ) -> crate::Result<()> {
        sql_query(
            r#"
INSERT INTO console_graph_materializations (
    mode, source_version, coordinate_space, world_min_x, world_min_y, world_max_x, world_max_y
)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(mode) DO UPDATE SET
    source_version = excluded.source_version,
    coordinate_space = excluded.coordinate_space,
    world_min_x = excluded.world_min_x,
    world_min_y = excluded.world_min_y,
    world_max_x = excluded.world_max_x,
    world_max_y = excluded.world_max_y,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
        )
        .bind::<Text, _>(input.mode.as_query_value())
        .bind::<BigInt, _>(input.source_version as i64)
        .bind::<Text, _>(COORDINATE_SPACE)
        .bind::<Integer, _>(input.world_min_x)
        .bind::<Integer, _>(input.world_min_y)
        .bind::<Integer, _>(input.world_max_x)
        .bind::<Integer, _>(input.world_max_y)
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    fn replace_materialized_facts(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        materialized: &GraphViewportResponse,
    ) -> crate::Result<()> {
        self.delete_stale_materialized_keys(
            connection,
            "console_graph_node_locations",
            "node_key",
            mode,
            materialized
                .nodes
                .iter()
                .map(|node| node.key.as_str())
                .collect(),
        )?;
        self.delete_stale_materialized_keys(
            connection,
            "console_graph_edge_routes",
            "edge_key",
            mode,
            materialized
                .edges
                .iter()
                .map(|edge| edge.key.as_str())
                .collect(),
        )?;
        let lane_by_y = materialized
            .lanes
            .iter()
            .map(|lane| (lane.y, lane))
            .collect::<std::collections::HashMap<_, _>>();
        for node in &materialized.nodes {
            let lane = lane_by_y
                .get(&node.y)
                .expect("materialized node should reference a lane y");
            self.insert_node_location(
                connection,
                NodeLocationInsert {
                    mode,
                    node,
                    lane,
                    bounds: node_bounds(node),
                },
            )?;
        }
        for edge in &materialized.edges {
            self.insert_edge_route(
                connection,
                EdgeRouteInsert {
                    mode,
                    edge,
                    bounds: edge_bounds(edge),
                },
            )?;
        }
        Ok(())
    }

    fn delete_stale_materialized_keys(
        &self,
        connection: &mut SqliteConnection,
        table: &'static str,
        key_column: &'static str,
        mode: GraphMode,
        current_keys: HashSet<&str>,
    ) -> crate::Result<()> {
        let existing_keys = sql_query(format!(
            "SELECT {key_column} AS item_key FROM {table} WHERE mode = ?"
        ))
        .bind::<Text, _>(mode.as_query_value())
        .load::<MaterializedKeyRow>(&mut *connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        for row in existing_keys {
            if current_keys.contains(row.item_key.as_str()) {
                continue;
            }
            sql_query(format!(
                "DELETE FROM {table} WHERE mode = ? AND {key_column} = ?"
            ))
            .bind::<Text, _>(mode.as_query_value())
            .bind::<Text, _>(row.item_key)
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        }
        Ok(())
    }

    fn insert_node_location(
        &self,
        connection: &mut SqliteConnection,
        insert: NodeLocationInsert<'_>,
    ) -> crate::Result<()> {
        let labels_json = serde_json::to_string(&insert.node.labels).context(
            ParseGraphSnapshotStoreValueSnafu {
                column: "console_graph_node_locations.labels_json",
            },
        )?;
        sql_query(
            r#"
INSERT INTO console_graph_node_locations (
    mode, node_key, node_id, node_target, short_id, node_kind, summary,
    labels_json, lane_key, lane_label, lane_y, x, y, min_x, min_y, max_x, max_y
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(mode, node_key) DO UPDATE SET
    node_id = excluded.node_id,
    node_target = excluded.node_target,
    short_id = excluded.short_id,
    node_kind = excluded.node_kind,
    summary = excluded.summary,
    labels_json = excluded.labels_json,
    lane_key = excluded.lane_key,
    lane_label = excluded.lane_label,
    lane_y = excluded.lane_y,
    x = excluded.x,
    y = excluded.y,
    min_x = excluded.min_x,
    min_y = excluded.min_y,
    max_x = excluded.max_x,
    max_y = excluded.max_y
WHERE console_graph_node_locations.node_id IS NOT excluded.node_id
   OR console_graph_node_locations.node_target IS NOT excluded.node_target
   OR console_graph_node_locations.short_id IS NOT excluded.short_id
   OR console_graph_node_locations.node_kind IS NOT excluded.node_kind
   OR console_graph_node_locations.summary IS NOT excluded.summary
   OR console_graph_node_locations.labels_json IS NOT excluded.labels_json
   OR console_graph_node_locations.lane_key IS NOT excluded.lane_key
   OR console_graph_node_locations.lane_label IS NOT excluded.lane_label
   OR console_graph_node_locations.lane_y IS NOT excluded.lane_y
   OR console_graph_node_locations.x IS NOT excluded.x
   OR console_graph_node_locations.y IS NOT excluded.y
   OR console_graph_node_locations.min_x IS NOT excluded.min_x
   OR console_graph_node_locations.min_y IS NOT excluded.min_y
   OR console_graph_node_locations.max_x IS NOT excluded.max_x
   OR console_graph_node_locations.max_y IS NOT excluded.max_y
"#,
        )
        .bind::<Text, _>(insert.mode.as_query_value())
        .bind::<Text, _>(&insert.node.key)
        .bind::<Text, _>(&insert.node.id)
        .bind::<Text, _>(&insert.node.node_target)
        .bind::<Text, _>(&insert.node.short_id)
        .bind::<Text, _>(&insert.node.kind)
        .bind::<Text, _>(&insert.node.summary)
        .bind::<Text, _>(labels_json)
        .bind::<Text, _>(&insert.lane.key)
        .bind::<Text, _>(&insert.lane.label)
        .bind::<Integer, _>(insert.lane.y)
        .bind::<Integer, _>(insert.node.x)
        .bind::<Integer, _>(insert.node.y)
        .bind::<Integer, _>(insert.bounds.left)
        .bind::<Integer, _>(insert.bounds.top)
        .bind::<Integer, _>(insert.bounds.right)
        .bind::<Integer, _>(insert.bounds.bottom)
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    fn insert_edge_route(
        &self,
        connection: &mut SqliteConnection,
        insert: EdgeRouteInsert<'_>,
    ) -> crate::Result<()> {
        sql_query(
            r#"
INSERT INTO console_graph_edge_routes (
    mode, edge_key, edge_kind, source_id, target_id,
    source_x, source_y, target_x, target_y, route_slot, target_port_offset,
    min_x, min_y, max_x, max_y
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(mode, edge_key) DO UPDATE SET
    edge_kind = excluded.edge_kind,
    source_id = excluded.source_id,
    target_id = excluded.target_id,
    source_x = excluded.source_x,
    source_y = excluded.source_y,
    target_x = excluded.target_x,
    target_y = excluded.target_y,
    route_slot = excluded.route_slot,
    target_port_offset = excluded.target_port_offset,
    min_x = excluded.min_x,
    min_y = excluded.min_y,
    max_x = excluded.max_x,
    max_y = excluded.max_y
WHERE console_graph_edge_routes.edge_kind IS NOT excluded.edge_kind
   OR console_graph_edge_routes.source_id IS NOT excluded.source_id
   OR console_graph_edge_routes.target_id IS NOT excluded.target_id
   OR console_graph_edge_routes.source_x IS NOT excluded.source_x
   OR console_graph_edge_routes.source_y IS NOT excluded.source_y
   OR console_graph_edge_routes.target_x IS NOT excluded.target_x
   OR console_graph_edge_routes.target_y IS NOT excluded.target_y
   OR console_graph_edge_routes.route_slot IS NOT excluded.route_slot
   OR console_graph_edge_routes.target_port_offset IS NOT excluded.target_port_offset
   OR console_graph_edge_routes.min_x IS NOT excluded.min_x
   OR console_graph_edge_routes.min_y IS NOT excluded.min_y
   OR console_graph_edge_routes.max_x IS NOT excluded.max_x
   OR console_graph_edge_routes.max_y IS NOT excluded.max_y
"#,
        )
        .bind::<Text, _>(insert.mode.as_query_value())
        .bind::<Text, _>(&insert.edge.key)
        .bind::<Text, _>(edge_kind_query_value(insert.edge.kind))
        .bind::<Text, _>(&insert.edge.source_id)
        .bind::<Text, _>(&insert.edge.target_id)
        .bind::<Integer, _>(insert.edge.source.x)
        .bind::<Integer, _>(insert.edge.source.y)
        .bind::<Integer, _>(insert.edge.target.x)
        .bind::<Integer, _>(insert.edge.target.y)
        .bind::<Integer, _>(insert.edge.route_slot)
        .bind::<Double, _>(insert.edge.target_port_offset)
        .bind::<Integer, _>(insert.bounds.left)
        .bind::<Integer, _>(insert.bounds.top)
        .bind::<Integer, _>(insert.bounds.right)
        .bind::<Integer, _>(insert.bounds.bottom)
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    fn update_node_labels(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_key: &str,
        labels: Vec<String>,
    ) -> crate::Result<()> {
        let labels_json =
            serde_json::to_string(&labels).context(ParseGraphSnapshotStoreValueSnafu {
                column: "console_graph_node_locations.labels_json",
            })?;
        sql_query(
            r#"
UPDATE console_graph_node_locations
SET labels_json = ?
WHERE mode = ? AND node_key = ? AND labels_json IS NOT ?
"#,
        )
        .bind::<Text, _>(&labels_json)
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Text, _>(node_key)
        .bind::<Text, _>(&labels_json)
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    fn delete_materialized_lanes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lanes: &[LaneRow],
    ) -> crate::Result<()> {
        for lane in lanes {
            sql_query(
                r#"
DELETE FROM console_graph_edge_routes
WHERE mode = ? AND (source_y = ? OR target_y = ?)
"#,
            )
            .bind::<Text, _>(mode.as_query_value())
            .bind::<Integer, _>(lane.lane_y)
            .bind::<Integer, _>(lane.lane_y)
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
            sql_query(
                r#"
DELETE FROM console_graph_node_locations
WHERE mode = ? AND lane_label = ?
"#,
            )
            .bind::<Text, _>(mode.as_query_value())
            .bind::<Text, _>(&lane.lane_label)
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        }
        Ok(())
    }

    fn delete_materialized_lane_suffix(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
        head_x: i32,
        lane_y: i32,
    ) -> crate::Result<()> {
        sql_query(
            r#"
DELETE FROM console_graph_edge_routes
WHERE mode = ?
  AND (source_y = ? OR target_y = ?)
  AND (source_x > ? OR target_x > ?)
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Integer, _>(lane_y)
        .bind::<Integer, _>(lane_y)
        .bind::<Integer, _>(head_x)
        .bind::<Integer, _>(head_x)
        .execute(&mut *connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        sql_query(
            r#"
DELETE FROM console_graph_node_locations
WHERE mode = ? AND lane_label = ? AND x > ?
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Text, _>(branch)
        .bind::<Integer, _>(head_x)
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    fn shift_lanes_for_insertion(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        insert_y: i32,
    ) -> crate::Result<()> {
        let mut lanes = self
            .materialized_lanes_in_connection(connection, mode)?
            .into_iter()
            .filter(|lane| lane.lane_y >= insert_y)
            .collect::<Vec<_>>();
        lanes.sort_by(|left, right| {
            right
                .lane_y
                .cmp(&left.lane_y)
                .then_with(|| right.lane_key.cmp(&left.lane_key))
        });
        for lane in lanes {
            self.shift_lane_nodes(connection, mode, &lane, -GRAPH_LANE_HEIGHT)?;
            self.shift_lane_edges(connection, mode, lane.lane_y, -GRAPH_LANE_HEIGHT)?;
        }
        Ok(())
    }

    fn shift_lanes_after_deletion(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        removed_lanes: &[LaneRow],
    ) -> crate::Result<()> {
        for lane in self.lane_shifts_after_deletion(connection, mode, removed_lanes)? {
            let delta = GRAPH_LANE_HEIGHT * removed_lane_count_before(removed_lanes, lane.lane_y);
            if delta == 0 {
                continue;
            }
            self.shift_lane_nodes(connection, mode, &lane, delta)?;
            self.shift_lane_edges(connection, mode, lane.lane_y, delta)?;
        }
        Ok(())
    }

    fn lane_shifts_after_deletion(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        removed_lanes: &[LaneRow],
    ) -> crate::Result<Vec<LaneRow>> {
        let first_removed_y = removed_lanes
            .iter()
            .map(|lane| lane.lane_y)
            .min()
            .unwrap_or(i32::MAX);
        Ok(self
            .materialized_lanes_in_connection(connection, mode)?
            .into_iter()
            .filter(|lane| lane.lane_y > first_removed_y)
            .collect())
    }

    fn shift_lane_nodes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane: &LaneRow,
        delta: i32,
    ) -> crate::Result<()> {
        sql_query(
            r#"
UPDATE console_graph_node_locations
SET node_key = 'node:' || node_id || ':' || x || ':' || (y - ?),
    lane_y = lane_y - ?,
    y = y - ?,
    min_y = min_y - ?,
    max_y = max_y - ?
WHERE mode = ? AND lane_label = ?
"#,
        )
        .bind::<Integer, _>(delta)
        .bind::<Integer, _>(delta)
        .bind::<Integer, _>(delta)
        .bind::<Integer, _>(delta)
        .bind::<Integer, _>(delta)
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Text, _>(&lane.lane_label)
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    fn shift_lane_edges(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_y: i32,
        delta: i32,
    ) -> crate::Result<()> {
        let rows = sql_query(
            r#"
SELECT edge_key, edge_kind, source_id, target_id, source_x, source_y, target_x, target_y, route_slot, target_port_offset
FROM console_graph_edge_routes
WHERE mode = ? AND (source_y = ? OR target_y = ?)
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Integer, _>(lane_y)
        .bind::<Integer, _>(lane_y)
        .load::<EdgeRouteRow>(&mut *connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        for row in rows {
            sql_query("DELETE FROM console_graph_edge_routes WHERE mode = ? AND edge_key = ?")
                .bind::<Text, _>(mode.as_query_value())
                .bind::<Text, _>(&row.edge_key)
                .execute(&mut *connection)
                .context(QueryGraphSnapshotStoreSnafu {
                    path: self.path.as_ref().clone(),
                })?;
            let kind = parse_edge_kind(&row.edge_kind)?;
            let source = Point {
                x: row.source_x,
                y: if row.source_y == lane_y {
                    row.source_y - delta
                } else {
                    row.source_y
                },
            };
            let target = Point {
                x: row.target_x,
                y: if row.target_y == lane_y {
                    row.target_y - delta
                } else {
                    row.target_y
                },
            };
            let edge = GraphViewportEdge {
                key: edge_key(kind, &row.source_id, source, &row.target_id, target),
                kind,
                source_id: row.source_id,
                target_id: row.target_id,
                source,
                target,
                route_slot: row.route_slot,
                target_port_offset: row.target_port_offset,
            };
            self.insert_edge_route(
                connection,
                EdgeRouteInsert {
                    mode,
                    edge: &edge,
                    bounds: edge_bounds(&edge),
                },
            )?;
        }
        Ok(())
    }

    fn latest_materialization_row(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Option<MaterializationRow>> {
        let mut connection = self.connect()?;
        self.latest_materialization_row_in_connection(&mut connection, mode)
    }

    fn latest_materialization_row_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<Option<MaterializationRow>> {
        sql_query(
            r#"
SELECT source_version, world_min_x, world_min_y, world_max_x, world_max_y
FROM console_graph_materializations
WHERE mode = ? AND coordinate_space = ?
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Text, _>(COORDINATE_SPACE)
        .get_result::<MaterializationRow>(connection)
        .optional()
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })
    }

    fn latest_lane_tail_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
    ) -> crate::Result<Option<MaterializedTailNodeRow>> {
        sql_query(
            r#"
SELECT node_key, node_id, lane_key, lane_label, lane_y, x, y
FROM console_graph_node_locations
WHERE mode = ? AND lane_label = ?
ORDER BY x DESC, node_key DESC
LIMIT 1
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Text, _>(branch)
        .get_result::<MaterializedTailNodeRow>(connection)
        .optional()
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })
    }

    fn materialized_lane_node_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
        node_id: &str,
    ) -> crate::Result<Option<MaterializedTailNodeRow>> {
        sql_query(
            r#"
SELECT node_key, node_id, lane_key, lane_label, lane_y, x, y
FROM console_graph_node_locations
WHERE mode = ? AND lane_label = ? AND node_id = ?
ORDER BY x DESC, node_key DESC
LIMIT 1
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Text, _>(branch)
        .bind::<Text, _>(node_id)
        .get_result::<MaterializedTailNodeRow>(connection)
        .optional()
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })
    }

    fn materialized_lanes_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<Vec<LaneRow>> {
        sql_query(
            r#"
SELECT DISTINCT lane_key, lane_label, lane_y
FROM console_graph_node_locations
WHERE mode = ?
ORDER BY lane_y, lane_key
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .load::<LaneRow>(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })
    }

    fn first_materialized_ancestry_point(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        ancestry: &[Node],
    ) -> crate::Result<Option<(usize, Point)>> {
        for (index, node) in ancestry.iter().enumerate() {
            let Some(point) =
                self.materialized_node_point_in_connection(connection, mode, &node.id)?
            else {
                continue;
            };
            return Ok(Some((index, point)));
        }
        Ok(None)
    }

    fn first_materialized_lane_ancestry_node(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
        ancestry: &[Node],
    ) -> crate::Result<Option<MaterializedTailNodeRow>> {
        for node in ancestry {
            let Some(row) =
                self.materialized_lane_node_in_connection(connection, mode, branch, &node.id)?
            else {
                continue;
            };
            return Ok(Some(row));
        }
        Ok(None)
    }

    fn materialized_node_rows_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<Vec<MaterializedTailNodeRow>> {
        sql_query(
            r#"
SELECT node_key, node_id, lane_key, lane_label, lane_y, x, y
FROM console_graph_node_locations
WHERE mode = ?
ORDER BY y, x, node_key
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .load::<MaterializedTailNodeRow>(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })
    }

    fn materialized_node_point_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
    ) -> crate::Result<Option<Point>> {
        sql_query(
            r#"
SELECT x, y
FROM console_graph_node_locations
WHERE mode = ? AND node_id = ?
ORDER BY y, x, node_key
LIMIT 1
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Text, _>(node_id)
        .get_result::<MaterializedNodePointRow>(connection)
        .optional()
        .map(|row| row.map(|row| Point { x: row.x, y: row.y }))
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })
    }

    fn materialized_edge_count_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<usize> {
        #[derive(QueryableByName)]
        struct CountRow {
            #[diesel(sql_type = BigInt)]
            value: i64,
        }

        sql_query(
            r#"
SELECT COUNT(DISTINCT edge_key) AS value
FROM console_graph_edge_routes
WHERE mode = ?
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .get_result::<CountRow>(connection)
        .map(|row| row.value.max(0) as usize)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })
    }

    fn event_order_by_materialized_and_new_nodes(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        mode: GraphMode,
        new_nodes: &[Node],
    ) -> crate::Result<BTreeMap<String, usize>> {
        let mut nodes_by_id = new_nodes
            .iter()
            .map(|node| (node.id.clone(), node.clone()))
            .collect::<BTreeMap<_, _>>();
        for row in self.materialized_node_rows_in_connection(connection, mode)? {
            if nodes_by_id.contains_key(&row.node_id) {
                continue;
            }
            let node = store
                .get_node(&row.node_id)
                .context(crate::error::StoreSnafu)?;
            nodes_by_id.insert(row.node_id, node);
        }

        let mut nodes = nodes_by_id.into_values().collect::<Vec<_>>();
        nodes.sort_by(|left, right| {
            left.created_at
                .as_nanosecond()
                .cmp(&right.created_at.as_nanosecond())
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(nodes
            .into_iter()
            .enumerate()
            .map(|(index, node)| (node.id, index))
            .collect())
    }

    fn next_routed_edge_slot_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        source: Point,
        target: Point,
    ) -> crate::Result<i32> {
        let direction = (target.y - source.y).signum();
        sql_query(
            r#"
SELECT COALESCE(MAX(route_slot) + 1, 0) AS value
FROM console_graph_edge_routes
WHERE mode = ?
  AND edge_kind != 'primary_parent'
  AND source_y = ?
  AND CASE
      WHEN target_y > source_y THEN 1
      WHEN target_y < source_y THEN -1
      ELSE 0
  END = ?
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Integer, _>(source.y)
        .bind::<Integer, _>(direction)
        .get_result::<SqliteInteger>(connection)
        .map(|row| row.value)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })
    }

    fn viewport_from_row(
        &self,
        mode: GraphMode,
        meta: MaterializationRow,
        request: GraphViewportRequest,
    ) -> crate::Result<Option<GraphViewportResponse>> {
        let request = request.normalized();
        let bounds = ViewportItemBounds::from_request(request);
        Ok(Some(GraphViewportResponse {
            version: meta.source_version as u64,
            canvas: GraphCanvas {
                width: meta.world_max_x.saturating_sub(meta.world_min_x),
                height: meta.world_max_y.saturating_sub(meta.world_min_y),
            },
            viewport: GraphViewport {
                x: request.x,
                y: request.y,
                width: request.width,
                height: request.height,
                overscan: request.overscan,
            },
            lanes: self.viewport_lanes(mode, bounds)?,
            nodes: self.viewport_nodes(mode, bounds)?,
            edges: self.viewport_edges(mode, bounds)?,
        }))
    }

    fn viewport_diff_from_row(
        &self,
        mode: GraphMode,
        meta: MaterializationRow,
        request: GraphViewportDiffRequest,
    ) -> crate::Result<Option<GraphViewportDiffResponse>> {
        let previous = self
            .viewport_from_row(mode, meta.clone(), request.previous)?
            .expect("viewport metadata should produce a response");
        let current = self
            .viewport_from_row(mode, meta, request.current)?
            .expect("viewport metadata should produce a response");
        Ok(Some(diff_graph_viewport_responses(
            previous,
            current,
            request.known.as_ref(),
        )))
    }

    fn viewport_lanes(
        &self,
        mode: GraphMode,
        bounds: ViewportItemBounds,
    ) -> crate::Result<Vec<GraphViewportLane>> {
        let mut connection = self.connect()?;
        let rows = sql_query(
            r#"
SELECT DISTINCT lane_key, lane_label, lane_y
FROM console_graph_node_locations
WHERE mode = ?
  AND ? <= ?
  AND ? >= ?
  AND lane_y - 24 <= ?
  AND lane_y + 24 >= ?
ORDER BY lane_y, lane_key
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Integer, _>(0)
        .bind::<Integer, _>(bounds.right)
        .bind::<Integer, _>(crate::layout::GRAPH_LEFT_X)
        .bind::<Integer, _>(bounds.left)
        .bind::<Integer, _>(bounds.bottom)
        .bind::<Integer, _>(bounds.top)
        .load::<LaneRow>(&mut connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(rows
            .into_iter()
            .map(|row| GraphViewportLane {
                key: row.lane_key,
                label: row.lane_label,
                y: row.lane_y,
            })
            .collect())
    }

    fn viewport_nodes(
        &self,
        mode: GraphMode,
        bounds: ViewportItemBounds,
    ) -> crate::Result<Vec<GraphViewportNode>> {
        let mut connection = self.connect()?;
        let rows = sql_query(
            r#"
SELECT node_key, node_id, node_target, short_id, node_kind, summary, labels_json, x, y
FROM console_graph_node_locations
WHERE mode = ?
  AND min_x <= ?
  AND max_x >= ?
  AND min_y <= ?
  AND max_y >= ?
ORDER BY y, x, node_key
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Integer, _>(bounds.right)
        .bind::<Integer, _>(bounds.left)
        .bind::<Integer, _>(bounds.bottom)
        .bind::<Integer, _>(bounds.top)
        .load::<NodeLocationRow>(&mut connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        rows.into_iter()
            .map(|row| {
                let labels = serde_json::from_str(&row.labels_json).context(
                    ParseGraphSnapshotStoreValueSnafu {
                        column: "console_graph_node_locations.labels_json",
                    },
                )?;
                Ok(GraphViewportNode {
                    key: row.node_key,
                    id: row.node_id,
                    node_target: row.node_target,
                    short_id: row.short_id,
                    kind: row.node_kind,
                    summary: row.summary,
                    labels,
                    x: row.x,
                    y: row.y,
                })
            })
            .collect()
    }

    fn viewport_edges(
        &self,
        mode: GraphMode,
        bounds: ViewportItemBounds,
    ) -> crate::Result<Vec<GraphViewportEdge>> {
        let mut connection = self.connect()?;
        let rows = sql_query(
            r#"
SELECT edge_key, edge_kind, source_id, target_id, source_x, source_y, target_x, target_y, route_slot, target_port_offset
FROM console_graph_edge_routes
WHERE mode = ?
  AND min_x <= ?
  AND max_x >= ?
  AND min_y <= ?
  AND max_y >= ?
ORDER BY min_y, min_x, edge_key
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Integer, _>(bounds.right)
        .bind::<Integer, _>(bounds.left)
        .bind::<Integer, _>(bounds.bottom)
        .bind::<Integer, _>(bounds.top)
        .load::<EdgeRouteRow>(&mut connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        rows.into_iter()
            .map(|row| {
                Ok(GraphViewportEdge {
                    key: row.edge_key,
                    kind: parse_edge_kind(&row.edge_kind)?,
                    source_id: row.source_id,
                    target_id: row.target_id,
                    source: Point {
                        x: row.source_x,
                        y: row.source_y,
                    },
                    target: Point {
                        x: row.target_x,
                        y: row.target_y,
                    },
                    route_slot: row.route_slot,
                    target_port_offset: row.target_port_offset,
                })
            })
            .collect()
    }

    fn connect(&self) -> crate::Result<SqliteConnection> {
        let database_url = self.path.to_string_lossy().into_owned();
        let mut connection =
            SqliteConnection::establish(&database_url).context(ConnectGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        connection
            .batch_execute(
                r#"
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
"#,
            )
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        Ok(connection)
    }
}

#[derive(Clone, Copy)]
struct ItemBounds {
    left: i32,
    top: i32,
    right: i32,
    bottom: i32,
}

#[derive(Clone, Copy)]
struct ViewportItemBounds {
    left: i32,
    top: i32,
    right: i32,
    bottom: i32,
}

impl ViewportItemBounds {
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
}

fn node_bounds(node: &GraphViewportNode) -> ItemBounds {
    ItemBounds {
        left: node.x - NODE_RADIUS,
        top: node.y - NODE_RADIUS,
        right: node.x + NODE_RADIUS,
        bottom: node.y + NODE_RADIUS,
    }
}

fn edge_bounds(edge: &GraphViewportEdge) -> ItemBounds {
    let padding = NODE_RADIUS + EDGE_TARGET_APPROACH;
    let mut left = edge.source.x.min(edge.target.x) - padding;
    let mut top = edge.source.y.min(edge.target.y) - padding;
    let mut right = edge.source.x.max(edge.target.x) + padding;
    let mut bottom = edge.source.y.max(edge.target.y) + padding;
    if edge.kind != GraphViewportEdgeKind::PrimaryParent {
        let corridor_y = edge_corridor_y(edge.source.y, edge.target.y, edge.route_slot);
        top = top.min(corridor_y - padding);
        bottom = bottom.max(corridor_y + padding);
        right = right.max(edge.source.x.max(edge.target.x) + EDGE_TARGET_APPROACH);
        left = left.min(edge.source.x.min(edge.target.x) - EDGE_TARGET_APPROACH);
    }

    ItemBounds {
        left,
        top,
        right,
        bottom,
    }
}

fn graph_viewport_node_from_node(
    node: &Node,
    point: Point,
    labels: Vec<String>,
) -> GraphViewportNode {
    GraphViewportNode {
        key: node_key(&node.id, point),
        id: node.id.clone(),
        node_target: node_target_id(&node.id),
        short_id: shorten_id(&node.id),
        kind: graph_kind_name(node).to_owned(),
        summary: summarize_node(node),
        labels,
        x: point.x,
        y: point.y,
    }
}

fn primary_parent_edge(
    source_id: &str,
    source: Point,
    target_id: &str,
    target: Point,
) -> GraphViewportEdge {
    GraphViewportEdge {
        key: primary_parent_edge_key(source_id, source, target_id, target),
        kind: GraphViewportEdgeKind::PrimaryParent,
        source_id: source_id.to_owned(),
        target_id: target_id.to_owned(),
        source,
        target,
        route_slot: 0,
        target_port_offset: 0.0,
    }
}

fn routed_edge(
    kind: GraphViewportEdgeKind,
    source_id: &str,
    source: Point,
    target_id: &str,
    target: Point,
    route_slot: i32,
) -> GraphViewportEdge {
    GraphViewportEdge {
        key: edge_key(kind, source_id, source, target_id, target),
        kind,
        source_id: source_id.to_owned(),
        target_id: target_id.to_owned(),
        source,
        target,
        route_slot,
        target_port_offset: 0.0,
    }
}

fn node_key(node_id: &str, point: Point) -> String {
    format!("node:{node_id}:{}:{}", point.x, point.y)
}

fn primary_parent_edge_key(
    source_id: &str,
    source: Point,
    target_id: &str,
    target: Point,
) -> String {
    edge_key(
        GraphViewportEdgeKind::PrimaryParent,
        source_id,
        source,
        target_id,
        target,
    )
}

fn edge_key(
    kind: GraphViewportEdgeKind,
    source_id: &str,
    source: Point,
    target_id: &str,
    target: Point,
) -> String {
    format!(
        "edge:{}:{source_id}:{}:{}:{target_id}:{}:{}",
        edge_kind_query_value(kind),
        source.x,
        source.y,
        target.x,
        target.y
    )
}

fn is_linear_primary_chain(chain: &[Node]) -> bool {
    chain.windows(2).all(|nodes| nodes[1].parent == nodes[0].id)
}

fn node_anchor_merge_parents(node: &Node) -> &[MergeParent] {
    match &node.kind {
        Kind::Anchor(anchor) => anchor.merge_parents(),
        _ => &[],
    }
}

fn is_anchor_node(node: &Node) -> bool {
    matches!(&node.kind, Kind::Anchor(_))
}

fn is_linear_new_nodes(source_id: &str, nodes: &[Node]) -> bool {
    nodes.first().is_some_and(|node| node.parent == source_id)
        && nodes.windows(2).all(|nodes| nodes[1].parent == nodes[0].id)
}

fn required_column_gap(
    source_id: &str,
    target_id: &str,
    event_order_by_node: &BTreeMap<String, usize>,
) -> i32 {
    event_order_by_node
        .get(target_id)
        .zip(event_order_by_node.get(source_id))
        .and_then(|(target_order, source_order)| target_order.checked_sub(*source_order))
        .map(|gap| gap.clamp(1, MAX_EDGE_COLUMN_GAP) as i32)
        .unwrap_or(1)
}

fn branch_label(branch: &str, state: &SessionState) -> String {
    format!("{branch}{}", session_state_suffix(state))
}

fn session_state_suffix(state: &SessionState) -> String {
    match state {
        SessionState::Active => String::new(),
        SessionState::Attached { target_branch, .. } => format!("@Attached({target_branch})"),
        SessionState::Paused {
            target_branch,
            reason,
        } => match reason {
            PauseReason::Merged { .. } => format!("@Paused({target_branch},merged)"),
            PauseReason::Closed => format!("@Paused({target_branch},closed)"),
        },
    }
}

fn branch_lane_priority(branch: &str) -> (u8, &str) {
    if branch == "main" {
        (0, branch)
    } else {
        (1, branch)
    }
}

fn existing_branch_lanes_preserve_order(
    session_states: &[(String, SessionState)],
    materialized_lanes: &[LaneRow],
    materialized_lane_labels: &BTreeSet<String>,
) -> bool {
    let expected_existing_lanes = session_states
        .iter()
        .filter(|(branch, _)| materialized_lane_labels.contains(branch))
        .map(|(branch, _)| branch.as_str())
        .collect::<Vec<_>>();
    let current_existing_lanes = materialized_lanes
        .iter()
        .map(|lane| lane.lane_label.as_str())
        .collect::<Vec<_>>();
    expected_existing_lanes == current_existing_lanes
}

fn removed_lanes_in_order(
    materialized_lanes: &[LaneRow],
    branch_names: &BTreeSet<String>,
) -> Vec<LaneRow> {
    materialized_lanes
        .iter()
        .filter(|lane| !branch_names.contains(&lane.lane_label))
        .cloned()
        .collect()
}

fn removed_lane_count_before(removed_lanes: &[LaneRow], lane_y: i32) -> i32 {
    removed_lanes
        .iter()
        .filter(|removed| removed.lane_y < lane_y)
        .count() as i32
}

fn edge_corridor_y(source_y: i32, target_y: i32, route_slot: i32) -> i32 {
    let base_y = match target_y.cmp(&source_y) {
        std::cmp::Ordering::Less => source_y - GRAPH_LANE_HEIGHT / 2,
        std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => source_y + GRAPH_LANE_HEIGHT / 2,
    };
    let offset = route_slot_offset(route_slot);
    (base_y + offset).max(16)
}

fn route_slot_offset(route_slot: i32) -> i32 {
    let magnitude = (route_slot + 1) / 2;
    let direction = if route_slot % 2 == 0 { 1 } else { -1 };

    magnitude.min(4) * EDGE_ROUTE_STEP * direction
}

fn primary_incoming_port_offset(count: usize, index: usize) -> f64 {
    (index as f64 - (count as f64 - 1.0) / 2.0) * EDGE_TARGET_PORT_STEP
}

fn secondary_incoming_port_offset(index: usize) -> f64 {
    let distance = index / 2 + 1;
    let direction = if index.is_multiple_of(2) { 1.0 } else { -1.0 };
    distance as f64 * EDGE_TARGET_PORT_STEP * direction
}

fn edge_kind_query_value(kind: GraphViewportEdgeKind) -> &'static str {
    match kind {
        GraphViewportEdgeKind::PrimaryParent => "primary_parent",
        GraphViewportEdgeKind::Fork => "fork",
        GraphViewportEdgeKind::MergeParent => "merge_parent",
    }
}

fn parse_edge_kind(value: &str) -> crate::Result<GraphViewportEdgeKind> {
    let json = format!("\"{value}\"");
    serde_json::from_str(&json).context(ParseGraphSnapshotStoreValueSnafu {
        column: "console_graph_edge_routes.edge_kind",
    })
}
