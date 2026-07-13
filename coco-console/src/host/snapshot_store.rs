use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Double, Integer, Text};
use diesel::sqlite::SqliteConnection;
use diesel_async::pooled_connection::bb8::Pool as AsyncSqlitePool;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, SimpleAsyncConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use snafu::prelude::*;

use crate::api::{
    GraphCanvas, GraphViewport, GraphViewportDiffResponse, GraphViewportEdge,
    GraphViewportEdgeKind, GraphViewportLane, GraphViewportNode, GraphViewportResponse, Point,
};
use crate::error::{
    AcquireGraphSnapshotConnectionSnafu, ConfigureGraphSnapshotStoreSnafu,
    CreateGraphSnapshotPoolSnafu, MigrateGraphSnapshotStoreSnafu,
    ParseGraphSnapshotStoreValueSnafu, QueryGraphSnapshotStoreSnafu,
};
use crate::graph::{
    GraphMode, graph_kind_name, initial_visible_graph_lane_nodes, node_target_id,
    provider_context_ancestry_nodes, shorten_id, summarize_node,
    visible_skill_invocation_subtree_nodes_with_lookup,
};
use crate::host::api::{GraphViewportDiffRequest, GraphViewportRequest};
use crate::layout::{
    EDGE_TARGET_PORT_STEP, GRAPH_COLUMN_WIDTH, GRAPH_LEFT_X, diff_graph_viewport_responses,
    lane_key,
};
use crate::schema::{
    console_graph_edge_routes, console_graph_materializations, console_graph_node_locations,
};
use coco_mem::{
    BranchStore, Kind, MergeParent, NewNode, Node, NodeStore, PauseReason, SessionAnchorPatch,
    SessionState, SessionStore,
};

const SQLITE_DATABASE_FILE_NAME: &str = "console-graph.sqlite3";
const SQLITE_POOL_MAX_SIZE: u32 = 4;
const COORDINATE_SPACE: &str = "graph_layout_v1";
const NODE_RADIUS: i32 = 26;
const EDGE_TARGET_APPROACH: i32 = 48;
const GRAPH_LANE_HEIGHT: i32 = 140;
const EDGE_ROUTE_STEP: i32 = 12;
const MAX_EDGE_COLUMN_GAP: usize = 5;
const DERIVED_ORPHAN_LANE_KEY_PREFIX: &str = "derived:orphan:";
const DERIVED_SKILL_LANE_KEY_PREFIX: &str = "derived:skill:";
const CONSOLE_GRAPH_MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

type AsyncSnapshotConnection = SyncConnectionWrapper<SqliteConnection>;
type AsyncSnapshotPool = AsyncSqlitePool<AsyncSnapshotConnection>;

mod anchor;
mod database;
mod full;
mod materialization;
mod mutation;
mod query;
mod row;
mod source;
mod transaction;
mod viewport;

pub(crate) use database::SnapshotDatabase;
#[cfg(test)]
pub(crate) use database::database_path;
pub use row::*;
pub use source::MaterializationSourceSnapshot;
pub use transaction::SnapshotTransactionError;
pub use viewport::*;

#[derive(Clone, Debug)]
pub struct ConsoleGraphSnapshotStore {
    path: Arc<PathBuf>,
    database: SnapshotDatabase,
}

#[cfg(test)]
mod tests;
