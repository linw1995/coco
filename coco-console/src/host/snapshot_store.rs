use std::collections::HashSet;
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
use crate::graph::{GraphMode, GraphSnapshot};
use crate::host::api::{GraphViewportDiffRequest, GraphViewportRequest};
use crate::layout::{diff_graph_viewport_responses, materialize_graph_viewport};

const SQLITE_DATABASE_FILE_NAME: &str = "store.sqlite3";
const COORDINATE_SPACE: &str = "graph_layout_v1";
const NODE_RADIUS: i32 = 26;
const EDGE_TARGET_APPROACH: i32 = 48;
const GRAPH_LANE_HEIGHT: i32 = 140;
const EDGE_ROUTE_STEP: i32 = 12;

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

#[derive(QueryableByName)]
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

struct NodeLocationInsert<'a> {
    mode: GraphMode,
    node: &'a GraphViewportNode,
    lane: &'a GraphViewportLane,
    bounds: ItemBounds,
}

struct EdgeRouteInsert<'a> {
    mode: GraphMode,
    edge: &'a GraphViewportEdge,
    bounds: ItemBounds,
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
        .bind::<Text, _>(mode.as_query_value())
        .bind::<BigInt, _>(source_version as i64)
        .bind::<Text, _>(COORDINATE_SPACE)
        .bind::<Integer, _>(0)
        .bind::<Integer, _>(0)
        .bind::<Integer, _>(materialized.canvas.width)
        .bind::<Integer, _>(materialized.canvas.height)
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        self.replace_materialized_facts(connection, mode, materialized)
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

    fn latest_materialization_row(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Option<MaterializationRow>> {
        let mut connection = self.connect()?;
        sql_query(
            r#"
SELECT source_version, world_min_x, world_min_y, world_max_x, world_max_y
FROM console_graph_materializations
WHERE mode = ? AND coordinate_space = ?
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .bind::<Text, _>(COORDINATE_SPACE)
        .get_result::<MaterializationRow>(&mut connection)
        .optional()
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
