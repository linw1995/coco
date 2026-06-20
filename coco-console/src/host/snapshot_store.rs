use std::path::{Path, PathBuf};
use std::sync::Arc;

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Integer, Text};
use diesel::sqlite::SqliteConnection;
use snafu::prelude::*;

use crate::api::{
    GraphCanvas, GraphViewport, GraphViewportDiffResponse, GraphViewportEdge,
    GraphViewportEdgeKind, GraphViewportLane, GraphViewportNode, GraphViewportResponse,
};
use crate::error::{
    ConnectGraphSnapshotStoreSnafu, ParseGraphSnapshotStoreValueSnafu, QueryGraphSnapshotStoreSnafu,
};
use crate::graph::{GraphMode, GraphSnapshot};
use crate::host::api::{GraphViewportDiffRequest, GraphViewportRequest};
use crate::layout::{diff_graph_viewport_responses, materialize_graph_viewport};

const SQLITE_DATABASE_FILE_NAME: &str = "store.sqlite3";
const NODE_RADIUS: i32 = 26;
const EDGE_TARGET_APPROACH: i32 = 48;
const GRAPH_LANE_HEIGHT: i32 = 140;
const EDGE_ROUTE_STEP: i32 = 12;

#[derive(Clone, Debug)]
pub struct ConsoleGraphSnapshotStore {
    path: Arc<PathBuf>,
}

#[derive(QueryableByName)]
struct SnapshotRow {
    #[diesel(sql_type = BigInt)]
    source_version: i64,
    #[diesel(sql_type = Text)]
    snapshot_json: String,
}

#[derive(Clone, QueryableByName)]
struct ViewportRow {
    #[diesel(sql_type = BigInt)]
    source_version: i64,
    #[diesel(sql_type = Integer)]
    canvas_width: i32,
    #[diesel(sql_type = Integer)]
    canvas_height: i32,
}

#[derive(QueryableByName)]
struct ViewportItemRow {
    #[diesel(sql_type = Text)]
    payload_json: String,
}

struct ViewportItemInsert<'a, T> {
    table: &'static str,
    mode: GraphMode,
    source_version: u64,
    key: &'a str,
    bounds: ItemBounds,
    item: &'a T,
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

    pub fn get(
        &self,
        mode: GraphMode,
        source_version: u64,
    ) -> crate::Result<Option<GraphSnapshot>> {
        let Some(row) = self.snapshot_row(mode, Some(source_version))? else {
            return Ok(None);
        };
        self.parse_snapshot(row)
    }

    pub fn latest(&self, mode: GraphMode) -> crate::Result<Option<GraphSnapshot>> {
        let Some(row) = self.snapshot_row(mode, None)? else {
            return Ok(None);
        };
        self.parse_snapshot(row)
    }

    pub fn latest_viewport(
        &self,
        mode: GraphMode,
        request: GraphViewportRequest,
    ) -> crate::Result<Option<GraphViewportResponse>> {
        let Some(meta) = self.latest_viewport_row(mode)? else {
            return Ok(None);
        };
        self.viewport_from_row(mode, meta, request)
    }

    pub fn latest_viewport_diff(
        &self,
        mode: GraphMode,
        request: GraphViewportDiffRequest,
    ) -> crate::Result<Option<GraphViewportDiffResponse>> {
        let Some(meta) = self.latest_viewport_row(mode)? else {
            return Ok(None);
        };
        self.viewport_diff_from_row(mode, meta, request)
    }

    pub fn put(&self, source_version: u64, snapshot: &GraphSnapshot) -> crate::Result<()> {
        let snapshot_json =
            serde_json::to_string(snapshot).context(ParseGraphSnapshotStoreValueSnafu {
                column: "console_graph_snapshots.snapshot_json",
            })?;
        let materialized = materialize_graph_viewport(snapshot);
        let mut connection = self.connect()?;
        sql_query(
            r#"
INSERT INTO console_graph_snapshots (mode, source_version, snapshot_json)
VALUES (?, ?, ?)
ON CONFLICT(mode) DO UPDATE SET
    source_version = excluded.source_version,
    snapshot_json = excluded.snapshot_json,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
        )
        .bind::<Text, _>(snapshot.mode.as_query_value())
        .bind::<BigInt, _>(source_version as i64)
        .bind::<Text, _>(snapshot_json)
        .execute(&mut connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        self.put_materialized_viewport(
            &mut connection,
            source_version,
            snapshot.mode,
            &materialized,
        )?;
        Ok(())
    }

    fn ensure_schema(&self) -> crate::Result<()> {
        let mut connection = self.connect()?;
        sql_query(
            r#"
CREATE TABLE IF NOT EXISTS console_graph_snapshots (
    mode TEXT PRIMARY KEY NOT NULL,
    source_version INTEGER NOT NULL,
    snapshot_json TEXT NOT NULL,
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
CREATE TABLE IF NOT EXISTS console_graph_viewports (
    mode TEXT PRIMARY KEY NOT NULL,
    source_version INTEGER NOT NULL,
    canvas_width INTEGER NOT NULL,
    canvas_height INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
)
"#,
        )
        .execute(&mut connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        for table in [
            "console_graph_viewport_lanes",
            "console_graph_viewport_nodes",
            "console_graph_viewport_edges",
        ] {
            sql_query(format!(
                r#"
CREATE TABLE IF NOT EXISTS {table} (
    mode TEXT NOT NULL,
    source_version INTEGER NOT NULL,
    item_key TEXT NOT NULL,
    left_bound INTEGER NOT NULL,
    top_bound INTEGER NOT NULL,
    right_bound INTEGER NOT NULL,
    bottom_bound INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (mode, item_key)
)
"#
            ))
            .execute(&mut connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
            sql_query(format!(
                "CREATE INDEX IF NOT EXISTS {table}_viewport_idx ON {table}(mode, source_version, left_bound, top_bound, right_bound, bottom_bound)"
            ))
            .execute(&mut connection)
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
INSERT INTO console_graph_viewports (mode, source_version, canvas_width, canvas_height)
VALUES (?, ?, ?, ?)
ON CONFLICT(mode) DO UPDATE SET
    source_version = excluded.source_version,
    canvas_width = excluded.canvas_width,
    canvas_height = excluded.canvas_height,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
"#,
        )
        .bind::<Text, _>(mode.as_query_value())
        .bind::<BigInt, _>(source_version as i64)
        .bind::<Integer, _>(materialized.canvas.width)
        .bind::<Integer, _>(materialized.canvas.height)
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        self.replace_viewport_items(connection, mode, source_version, materialized)
    }

    fn replace_viewport_items(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        source_version: u64,
        materialized: &GraphViewportResponse,
    ) -> crate::Result<()> {
        for table in [
            "console_graph_viewport_lanes",
            "console_graph_viewport_nodes",
            "console_graph_viewport_edges",
        ] {
            sql_query(format!("DELETE FROM {table} WHERE mode = ?"))
                .bind::<Text, _>(mode.as_query_value())
                .execute(&mut *connection)
                .context(QueryGraphSnapshotStoreSnafu {
                    path: self.path.as_ref().clone(),
                })?;
        }
        for lane in &materialized.lanes {
            self.insert_viewport_item(
                connection,
                ViewportItemInsert {
                    table: "console_graph_viewport_lanes",
                    mode,
                    source_version,
                    key: &lane.key,
                    bounds: lane_bounds(lane),
                    item: lane,
                },
            )?;
        }
        for node in &materialized.nodes {
            self.insert_viewport_item(
                connection,
                ViewportItemInsert {
                    table: "console_graph_viewport_nodes",
                    mode,
                    source_version,
                    key: &node.key,
                    bounds: node_bounds(node),
                    item: node,
                },
            )?;
        }
        for edge in &materialized.edges {
            self.insert_viewport_item(
                connection,
                ViewportItemInsert {
                    table: "console_graph_viewport_edges",
                    mode,
                    source_version,
                    key: &edge.key,
                    bounds: edge_bounds(edge),
                    item: edge,
                },
            )?;
        }
        Ok(())
    }

    fn insert_viewport_item<T>(
        &self,
        connection: &mut SqliteConnection,
        insert: ViewportItemInsert<'_, T>,
    ) -> crate::Result<()>
    where
        T: serde::Serialize,
    {
        let payload_json =
            serde_json::to_string(insert.item).context(ParseGraphSnapshotStoreValueSnafu {
                column: "console_graph_viewport_items.payload_json",
            })?;
        sql_query(format!(
            r#"
INSERT INTO {} (
    mode, source_version, item_key, left_bound, top_bound, right_bound, bottom_bound, payload_json
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"#,
            insert.table
        ))
        .bind::<Text, _>(insert.mode.as_query_value())
        .bind::<BigInt, _>(insert.source_version as i64)
        .bind::<Text, _>(insert.key)
        .bind::<Integer, _>(insert.bounds.left)
        .bind::<Integer, _>(insert.bounds.top)
        .bind::<Integer, _>(insert.bounds.right)
        .bind::<Integer, _>(insert.bounds.bottom)
        .bind::<Text, _>(payload_json)
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    fn snapshot_row(
        &self,
        mode: GraphMode,
        source_version: Option<u64>,
    ) -> crate::Result<Option<SnapshotRow>> {
        let mut connection = self.connect()?;
        if let Some(source_version) = source_version {
            return sql_query(
                "SELECT source_version, snapshot_json FROM console_graph_snapshots WHERE mode = ? AND source_version = ?",
            )
            .bind::<Text, _>(mode.as_query_value())
            .bind::<BigInt, _>(source_version as i64)
            .get_result::<SnapshotRow>(&mut connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            });
        }
        sql_query(
            "SELECT source_version, snapshot_json FROM console_graph_snapshots WHERE mode = ?",
        )
        .bind::<Text, _>(mode.as_query_value())
        .get_result::<SnapshotRow>(&mut connection)
        .optional()
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })
    }

    fn parse_snapshot(&self, row: SnapshotRow) -> crate::Result<Option<GraphSnapshot>> {
        let mut snapshot = serde_json::from_str::<GraphSnapshot>(&row.snapshot_json).context(
            ParseGraphSnapshotStoreValueSnafu {
                column: "console_graph_snapshots.snapshot_json",
            },
        )?;
        snapshot.version = row.source_version as u64;
        Ok(Some(snapshot))
    }

    fn latest_viewport_row(&self, mode: GraphMode) -> crate::Result<Option<ViewportRow>> {
        let mut connection = self.connect()?;
        sql_query(
            "SELECT source_version, canvas_width, canvas_height FROM console_graph_viewports WHERE mode = ?",
        )
        .bind::<Text, _>(mode.as_query_value())
        .get_result::<ViewportRow>(&mut connection)
        .optional()
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })
    }

    fn viewport_from_row(
        &self,
        mode: GraphMode,
        meta: ViewportRow,
        request: GraphViewportRequest,
    ) -> crate::Result<Option<GraphViewportResponse>> {
        let request = request.normalized();
        let bounds = ViewportItemBounds::from_request(request);
        Ok(Some(GraphViewportResponse {
            version: meta.source_version as u64,
            canvas: GraphCanvas {
                width: meta.canvas_width,
                height: meta.canvas_height,
            },
            viewport: GraphViewport {
                x: request.x,
                y: request.y,
                width: request.width,
                height: request.height,
                overscan: request.overscan,
            },
            lanes: self.viewport_items(
                "console_graph_viewport_lanes",
                mode,
                meta.source_version,
                bounds,
            )?,
            nodes: self.viewport_items(
                "console_graph_viewport_nodes",
                mode,
                meta.source_version,
                bounds,
            )?,
            edges: self.viewport_items(
                "console_graph_viewport_edges",
                mode,
                meta.source_version,
                bounds,
            )?,
        }))
    }

    fn viewport_diff_from_row(
        &self,
        mode: GraphMode,
        meta: ViewportRow,
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

    fn viewport_items<T>(
        &self,
        table: &'static str,
        mode: GraphMode,
        source_version: i64,
        bounds: ViewportItemBounds,
    ) -> crate::Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let mut connection = self.connect()?;
        let rows = sql_query(format!(
            r#"
SELECT payload_json
FROM {table}
WHERE mode = ?
  AND source_version = ?
  AND left_bound <= ?
  AND right_bound >= ?
  AND top_bound <= ?
  AND bottom_bound >= ?
ORDER BY top_bound, left_bound, item_key
"#
        ))
        .bind::<Text, _>(mode.as_query_value())
        .bind::<BigInt, _>(source_version)
        .bind::<Integer, _>(bounds.right)
        .bind::<Integer, _>(bounds.left)
        .bind::<Integer, _>(bounds.bottom)
        .bind::<Integer, _>(bounds.top)
        .load::<ViewportItemRow>(&mut connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        rows.into_iter()
            .map(|row| {
                serde_json::from_str(&row.payload_json).context(ParseGraphSnapshotStoreValueSnafu {
                    column: "console_graph_viewport_items.payload_json",
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

fn lane_bounds(lane: &GraphViewportLane) -> ItemBounds {
    ItemBounds {
        left: 0,
        top: lane.y - 24,
        right: 120,
        bottom: lane.y + 24,
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
