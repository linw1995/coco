#[cfg(test)]
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(test)]
use std::time::Instant;

use diesel::prelude::*;
use diesel::sql_types::{BigInt, Integer, Nullable, Text};
use diesel::sqlite::SqliteConnection;
use diesel_async::pooled_connection::bb8::Pool as AsyncSqlitePool;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, SimpleAsyncConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use snafu::prelude::*;

use crate::api::{
    GraphBezierRoute, GraphCanvas, GraphViewport, GraphViewportDiffResponse, GraphViewportEdge,
    GraphViewportEdgeKind, GraphViewportNode, GraphViewportResponse, Point,
};
use crate::error::{
    InvalidGraphSnapshotStoreValueSnafu, ParseGraphSnapshotStoreValueSnafu,
    QueryGraphSnapshotStoreSnafu, SerializeGraphSnapshotStoreValueSnafu,
};
use crate::graph::{GraphBuildPhase, GraphMode};
#[cfg(test)]
use crate::graph::{GraphEdgeKind, GraphSnapshot};
use crate::host::api::{GraphViewportDiffRequest, GraphViewportRequest};
use crate::layout::{
    GRAPH_PADDING, GraphLayoutEdge, GraphLayoutNode, diff_graph_viewport_responses, edge_bounds,
    edge_key, node_bounds, node_key,
};
#[cfg(test)]
use crate::layout::{GraphLayout, LayoutHint, try_graph_ranks, try_layout_graph_with_hints};
use crate::schema::{
    console_graph_edge_routes, console_graph_generation_state,
    console_graph_materialization_shells, console_graph_materialization_time_ticks,
    console_graph_materializations, console_graph_node_locations,
};

const SQLITE_DATABASE_FILE_NAME: &str = "console-graph.sqlite3";
const SQLITE_POOL_MAX_SIZE: u32 = 4;
const MATERIALIZATION_WRITE_BATCH_SIZE: usize = 128;
const PUBLICATION_RECEIPT_RETENTION: i64 = 1_024;
const MATERIALIZED_SHELL_TIME_TICK_LIMIT: usize = 256;
const COORDINATE_SPACE: &str = "graph_layout_v2";
const INCREMENTAL_BUILD_LEASE_TTL: Duration = Duration::from_secs(120);
pub(crate) const INCREMENTAL_BUILD_LEASE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const CONSOLE_GRAPH_MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

static INCREMENTAL_BUILD_OWNER_NONCE: AtomicU64 = AtomicU64::new(0);

type AsyncSnapshotConnection = SyncConnectionWrapper<SqliteConnection>;
type AsyncSnapshotPool = AsyncSqlitePool<AsyncSnapshotConnection>;

mod database;

pub(crate) use database::SnapshotDatabase;

#[derive(Clone, Debug)]
pub struct ConsoleGraphSnapshotStore {
    path: Arc<PathBuf>,
    database: SnapshotDatabase,
    policy: GraphLayoutPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IncrementalBuildLease {
    generation: i64,
    owner_id: String,
    lease_epoch: i64,
    target_source_version: u64,
    frozen_source_revision: i64,
    phase: IncrementalBuildLeasePhase,
    resumed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IncrementalBuildLeasePhase {
    Building,
    Compacting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IncrementalBuildProgress {
    pub stage: &'static str,
    pub phase: GraphBuildPhase,
    pub unit: &'static str,
    pub completed_units: usize,
    pub total_units: usize,
    pub message: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IncrementalBuildDiagnosticContext {
    pub build_run_id: i64,
    pub build_lease_epoch: i64,
    pub target_process_local_invalidation_version: u64,
    pub frozen_source_revision: i64,
    pub persisted_build_state: String,
    pub persisted_build_stage: String,
}

impl IncrementalBuildLease {
    pub(crate) fn generation(&self) -> i64 {
        self.generation
    }

    pub(crate) fn owner_id(&self) -> &str {
        &self.owner_id
    }

    pub(crate) fn lease_epoch(&self) -> i64 {
        self.lease_epoch
    }

    pub(crate) fn target_source_version(&self) -> u64 {
        self.target_source_version
    }

    pub(crate) fn frozen_source_revision(&self) -> i64 {
        self.frozen_source_revision
    }

    pub(crate) fn phase(&self) -> IncrementalBuildLeasePhase {
        self.phase
    }

    pub(crate) fn is_resumed(&self) -> bool {
        self.resumed
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GraphLayoutPolicy {
    pub full_layout_node_limit: usize,
    pub full_layout_edge_limit: usize,
    pub local_layout_node_limit: usize,
    pub local_layout_percent: usize,
}

impl Default for GraphLayoutPolicy {
    fn default() -> Self {
        Self {
            full_layout_node_limit: 10_000,
            full_layout_edge_limit: 20_000,
            local_layout_node_limit: 2_000,
            local_layout_percent: 20,
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaterializationStrategy {
    Full,
    Local,
    PayloadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GraphPublicationReceipt {
    pub build_run_id: i64,
    pub published_graph_generation: i64,
    pub publication_epoch: i64,
    pub published_source_revision: i64,
    pub replayed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActiveGraphPublicationState {
    pub graph_generation: i64,
    pub anchors_source_version: Option<u64>,
    pub all_source_version: Option<u64>,
    pub source_revision: Option<i64>,
    pub publication_epoch: Option<i64>,
    pub matches_current_source: bool,
    pub overlay_compaction_pending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IncrementalPublishOutcome {
    Published(GraphPublicationReceipt),
    Incomplete,
    LeaseLost,
    BaselineChanged {
        expected_generation: i64,
        current_generation: i64,
        expected_epoch: i64,
        current_epoch: i64,
        current_delta_compatible: bool,
    },
    Superseded,
}

#[cfg(test)]
impl MaterializationStrategy {
    fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Local => "local",
            Self::PayloadOnly => "payload_only",
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
struct PlannedMaterialization {
    layout: GraphLayout,
    strategy: MaterializationStrategy,
    affected_nodes: usize,
    affected_ranks: usize,
    fallback_reason: Option<&'static str>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaterializationWriteOutcome {
    Committed {
        strategy: MaterializationStrategy,
        fallback_reason: Option<&'static str>,
    },
    SkippedStale {
        current_version: u64,
    },
}

#[derive(Debug, Clone, Queryable, QueryableByName, Selectable)]
#[diesel(table_name = console_graph_materializations)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct MaterializationRow {
    pub source_version: i64,
    pub world_max_x: i32,
    pub world_max_y: i32,
}

#[derive(Debug, Clone, Queryable, QueryableByName, Selectable, PartialEq, Eq)]
#[diesel(table_name = console_graph_node_locations)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct StoredNodeRow {
    node_id: String,
    node_key: String,
    node_target: String,
    short_id: String,
    node_kind: String,
    summary: String,
    labels_json: String,
    rank: i32,
    sort_order: i32,
    x: i32,
    y: i32,
    created_at: String,
    created_at_ns: i64,
    min_x: i32,
    min_y: i32,
    max_x: i32,
    max_y: i32,
}

#[derive(Debug, Clone, Queryable, QueryableByName, Selectable, PartialEq, Eq)]
#[diesel(table_name = console_graph_edge_routes)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct StoredEdgeRow {
    edge_key: String,
    edge_kind: String,
    source_id: String,
    target_id: String,
    source_x: i32,
    source_y: i32,
    control_1_x: i32,
    control_1_y: i32,
    control_2_x: i32,
    control_2_y: i32,
    target_x: i32,
    target_y: i32,
    min_x: i32,
    min_y: i32,
    max_x: i32,
    max_y: i32,
}

#[derive(Debug, QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(Debug, QueryableByName)]
struct ScalarTextRow {
    #[diesel(sql_type = Text)]
    value: String,
}

#[derive(Debug, QueryableByName)]
struct SourceRevisionRow {
    #[diesel(sql_type = BigInt)]
    source_revision: i64,
}

#[derive(Debug, QueryableByName)]
struct PublicationReceiptRow {
    #[diesel(sql_type = BigInt)]
    published_graph_generation: i64,
    #[diesel(sql_type = BigInt)]
    publication_epoch: i64,
    #[diesel(sql_type = BigInt)]
    source_version: i64,
    #[diesel(sql_type = BigInt)]
    source_revision: i64,
}

#[derive(Debug, QueryableByName)]
struct PublishBuildRow {
    #[diesel(sql_type = BigInt)]
    source_revision: i64,
    #[diesel(sql_type = Text)]
    build_kind: String,
    #[diesel(sql_type = Nullable<BigInt>)]
    baseline_generation: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    baseline_source_revision: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    baseline_publication_epoch: Option<i64>,
}

#[derive(Debug, QueryableByName)]
struct ActivePublicationRow {
    #[diesel(sql_type = BigInt)]
    source_revision: i64,
}

#[derive(Debug, QueryableByName)]
struct ActivePublicationStateRow {
    #[diesel(sql_type = BigInt)]
    graph_generation: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    anchors_source_version: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    all_source_version: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    source_revision: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    publication_epoch: Option<i64>,
    #[diesel(sql_type = BigInt)]
    current_source_revision: i64,
    #[diesel(sql_type = Integer)]
    overlay_compaction_pending: i32,
}

#[derive(Debug, Clone, Copy, QueryableByName)]
struct ActiveSnapshotDescriptor {
    #[diesel(sql_type = BigInt)]
    base_generation: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    overlay_run_id: Option<i64>,
}

#[derive(Debug, QueryableByName)]
struct OverlayRunStateRow {
    #[diesel(sql_type = BigInt)]
    base_generation: i64,
    #[diesel(sql_type = BigInt)]
    baseline_source_revision: i64,
    #[diesel(sql_type = BigInt)]
    baseline_publication_epoch: i64,
    #[diesel(sql_type = BigInt)]
    target_source_version: i64,
    #[diesel(sql_type = BigInt)]
    target_source_revision: i64,
    #[diesel(sql_type = BigInt)]
    target_publication_epoch: i64,
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Text)]
    phase: String,
    #[diesel(sql_type = Text)]
    cursor_node_id: String,
    #[diesel(sql_type = Text)]
    cursor_endpoint: String,
    #[diesel(sql_type = Text)]
    cursor_mode: String,
    #[diesel(sql_type = Text)]
    cursor_work_kind: String,
    #[diesel(sql_type = Text)]
    cursor_key: String,
}

#[derive(Debug, QueryableByName)]
struct OverlayAffectedNodeRow {
    #[diesel(sql_type = Text)]
    mode: String,
    #[diesel(sql_type = Text)]
    node_id: String,
}

#[derive(Debug, QueryableByName)]
struct OverlayModeKeyRow {
    #[diesel(sql_type = Text)]
    mode: String,
    #[diesel(sql_type = Text)]
    item_key: String,
}

#[derive(Debug, QueryableByName)]
struct OverlayScopeRow {
    #[diesel(sql_type = Text)]
    branch_name: String,
    #[diesel(sql_type = Text)]
    node_id: String,
}

#[derive(Debug, QueryableByName)]
struct OverlayBranchModeRow {
    #[diesel(sql_type = Text)]
    branch_name: String,
    #[diesel(sql_type = Text)]
    mode: String,
}

#[derive(Debug, QueryableByName)]
struct OverlayModeIndexRow {
    #[diesel(sql_type = Text)]
    mode: String,
    #[diesel(sql_type = Integer)]
    sample_index: i32,
}

#[derive(Debug, QueryableByName)]
struct EffectiveNodeLabelRow {
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Text)]
    label: String,
    #[diesel(sql_type = Text)]
    branch_name: String,
}

#[derive(Debug, QueryableByName)]
struct EffectiveNodePointRow {
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Integer)]
    x: i32,
    #[diesel(sql_type = Integer)]
    y: i32,
}

impl ActiveSnapshotDescriptor {
    fn effective_generation(self) -> i64 {
        self.overlay_run_id.unwrap_or(self.base_generation)
    }
}

#[derive(Debug, QueryableByName)]
struct OptionalBigIntRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<BigInt>)]
    value: Option<i64>,
}

#[derive(Debug, QueryableByName)]
struct ResumableBuildRunRow {
    #[diesel(sql_type = BigInt)]
    run_id: i64,
    #[diesel(sql_type = BigInt)]
    lease_epoch: i64,
    #[diesel(sql_type = BigInt)]
    source_version: i64,
    #[diesel(sql_type = BigInt)]
    frozen_source_revision: i64,
}

#[derive(Debug, QueryableByName)]
struct CompactingBuildRunRow {
    #[diesel(sql_type = BigInt)]
    run_id: i64,
    #[diesel(sql_type = BigInt)]
    lease_epoch: i64,
    #[diesel(sql_type = BigInt)]
    lease_expires_at_ms: i64,
    #[diesel(sql_type = BigInt)]
    source_version: i64,
    #[diesel(sql_type = BigInt)]
    frozen_source_revision: i64,
}

#[derive(Debug, QueryableByName)]
struct IncrementalBuildProgressRow {
    #[diesel(sql_type = Text)]
    build_status: String,
    #[diesel(sql_type = Nullable<Text>)]
    overlay_phase: Option<String>,
    #[diesel(sql_type = Integer)]
    dag_initialized: i32,
    #[diesel(sql_type = Text)]
    dag_init_phase: String,
    #[diesel(sql_type = BigInt)]
    dag_init_counter: i64,
    #[diesel(sql_type = Text)]
    dag_finalize_phase: String,
    #[diesel(sql_type = BigInt)]
    dag_processed_node_count: i64,
    #[diesel(sql_type = BigInt)]
    dag_discovered_node_count: i64,
    #[diesel(sql_type = Nullable<Text>)]
    shell_phase: Option<String>,
}

#[derive(Debug, QueryableByName)]
struct IncrementalBuildDiagnosticContextRow {
    #[diesel(sql_type = BigInt)]
    build_run_id: i64,
    #[diesel(sql_type = BigInt)]
    build_lease_epoch: i64,
    #[diesel(sql_type = BigInt)]
    target_process_local_invalidation_version: i64,
    #[diesel(sql_type = BigInt)]
    frozen_source_revision: i64,
    #[diesel(sql_type = Text)]
    persisted_build_state: String,
    #[diesel(sql_type = Text)]
    persisted_build_stage: String,
}

#[cfg(test)]
#[derive(Debug)]
struct StoredGraph {
    materialization: Option<MaterializationRow>,
    nodes: BTreeMap<String, StoredNodeRow>,
    edges: BTreeMap<String, StoredEdgeRow>,
}

#[derive(Debug)]
struct StoredViewport {
    materialization: MaterializationRow,
    request: GraphViewportRequest,
    nodes: Vec<StoredNodeRow>,
    edges: Vec<StoredEdgeRow>,
}

#[derive(Debug)]
struct StoredShellFacts {
    materialization: MaterializationRow,
    node_count: i64,
    nodes: Vec<StoredShellTickRow>,
    edge_count: i64,
    branches: Vec<StoredShellBranchRow>,
}

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = console_graph_materialization_time_ticks)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct StoredShellTickRow {
    node_target: String,
    x: i32,
    y: i32,
    created_at: String,
    created_at_ns: i64,
}

#[cfg(test)]
#[derive(Debug, QueryableByName)]
struct MaterializedShellSummaryRow {
    #[diesel(sql_type = Nullable<Integer>)]
    node_max_x: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    node_max_y: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    edge_max_x: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    edge_max_y: Option<i32>,
    #[diesel(sql_type = BigInt)]
    node_count: i64,
    #[diesel(sql_type = BigInt)]
    edge_count: i64,
}

#[cfg(test)]
#[derive(Debug, QueryableByName, PartialEq, Eq)]
struct PreparedShellTickRow {
    #[diesel(sql_type = Integer)]
    sample_index: i32,
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Text)]
    node_target: String,
    #[diesel(sql_type = Integer)]
    x: i32,
    #[diesel(sql_type = Integer)]
    y: i32,
    #[diesel(sql_type = Text)]
    created_at: String,
    #[diesel(sql_type = BigInt)]
    created_at_ns: i64,
}

#[cfg(test)]
#[derive(Debug)]
struct PreparedShellProjection {
    width: i32,
    height: i32,
    node_count: i64,
    edge_count: i64,
    time_ticks: Vec<PreparedShellTickRow>,
}

#[derive(Debug, QueryableByName)]
struct IncrementalShellProjectionRow {
    #[diesel(sql_type = Text)]
    phase: String,
    #[diesel(sql_type = BigInt)]
    row_cursor: i64,
    #[diesel(sql_type = BigInt)]
    node_count: i64,
    #[diesel(sql_type = BigInt)]
    edge_count: i64,
    #[diesel(sql_type = Nullable<Integer>)]
    node_max_x: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    node_max_y: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    edge_max_x: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    edge_max_y: Option<i32>,
    #[diesel(sql_type = Nullable<BigInt>)]
    tick_cursor_created_at_ns: Option<i64>,
    #[diesel(sql_type = Nullable<Text>)]
    tick_cursor_node_id: Option<String>,
    #[diesel(sql_type = BigInt)]
    tick_ordinal: i64,
    #[diesel(sql_type = Text)]
    build_kind: String,
    #[diesel(sql_type = Nullable<BigInt>)]
    baseline_generation: Option<i64>,
    #[diesel(sql_type = Integer)]
    baseline_initialized: i32,
}

#[derive(Debug, QueryableByName)]
struct IncrementalShellBaselineRow {
    #[diesel(sql_type = Text)]
    build_kind: String,
    #[diesel(sql_type = Nullable<BigInt>)]
    baseline_generation: Option<i64>,
    #[diesel(sql_type = Integer)]
    baseline_initialized: i32,
    #[diesel(sql_type = Text)]
    baseline_phase: String,
    #[diesel(sql_type = Text)]
    baseline_work_kind: String,
    #[diesel(sql_type = Text)]
    baseline_cursor_node_id: String,
    #[diesel(sql_type = Text)]
    baseline_endpoint: String,
    #[diesel(sql_type = Text)]
    baseline_cursor_key: String,
    #[diesel(sql_type = BigInt)]
    row_cursor: i64,
    #[diesel(sql_type = BigInt)]
    node_count: i64,
    #[diesel(sql_type = BigInt)]
    edge_count: i64,
    #[diesel(sql_type = Nullable<Integer>)]
    node_max_x: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    node_max_y: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    edge_max_x: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    edge_max_y: Option<i32>,
}

#[derive(Debug, QueryableByName)]
struct IncrementalShellExtentRow {
    #[diesel(sql_type = BigInt)]
    row_id: i64,
    #[diesel(sql_type = Integer)]
    max_x: i32,
    #[diesel(sql_type = Integer)]
    max_y: i32,
}

#[derive(Debug, QueryableByName)]
struct IncrementalShellBaselineExtentRow {
    #[diesel(sql_type = BigInt)]
    row_id: i64,
    #[diesel(sql_type = Integer)]
    max_x: i32,
    #[diesel(sql_type = Integer)]
    max_y: i32,
    #[diesel(sql_type = Integer)]
    excluded: i32,
}

#[derive(Debug, QueryableByName)]
struct IncrementalShellTickSourceRow {
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Text)]
    node_target: String,
    #[diesel(sql_type = Integer)]
    x: i32,
    #[diesel(sql_type = Integer)]
    y: i32,
    #[diesel(sql_type = Text)]
    created_at: String,
    #[diesel(sql_type = BigInt)]
    created_at_ns: i64,
}

#[derive(Debug, QueryableByName)]
struct IncrementalShellBaselineTickSourceRow {
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Text)]
    node_target: String,
    #[diesel(sql_type = Integer)]
    x: i32,
    #[diesel(sql_type = Integer)]
    y: i32,
    #[diesel(sql_type = Text)]
    created_at: String,
    #[diesel(sql_type = BigInt)]
    created_at_ns: i64,
    #[diesel(sql_type = Integer)]
    excluded: i32,
}

#[derive(Debug, Clone, QueryableByName, PartialEq, Eq)]
struct StoredShellBranchRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    head_id: String,
    #[diesel(sql_type = Text)]
    state_json: String,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct PersistedMaterializationBranch {
    name: String,
    head_id: String,
    state_json: String,
}

#[derive(Debug, Clone, Insertable, AsChangeset, PartialEq, Eq)]
#[diesel(table_name = console_graph_node_locations)]
struct PersistedNode {
    node_id: String,
    node_key: String,
    node_target: String,
    short_id: String,
    node_kind: String,
    summary: String,
    labels_json: String,
    rank: i32,
    sort_order: i32,
    x: i32,
    y: i32,
    created_at: String,
    created_at_ns: i64,
    min_x: i32,
    min_y: i32,
    max_x: i32,
    max_y: i32,
}

#[derive(Debug, Clone, Insertable, AsChangeset, PartialEq, Eq)]
#[diesel(table_name = console_graph_edge_routes)]
struct PersistedEdge {
    edge_key: String,
    edge_kind: String,
    source_id: String,
    target_id: String,
    source_x: i32,
    source_y: i32,
    control_1_x: i32,
    control_1_y: i32,
    control_2_x: i32,
    control_2_y: i32,
    target_x: i32,
    target_y: i32,
    min_x: i32,
    min_y: i32,
    max_x: i32,
    max_y: i32,
}

#[derive(Debug, Insertable, AsChangeset)]
#[diesel(table_name = console_graph_materializations)]
struct PersistedMaterialization<'a> {
    generation: i64,
    mode: &'a str,
    source_version: i64,
    coordinate_space: &'a str,
    world_min_x: i32,
    world_min_y: i32,
    world_max_x: i32,
    world_max_y: i32,
    updated_at: String,
}

#[derive(Clone, Debug)]
pub struct MaterializedGraphShellFacts {
    pub version: u64,
    pub node_count: usize,
    pub nodes: Vec<MaterializedGraphShellNode>,
    pub edge_count: usize,
    pub branches: Vec<MaterializedGraphShellBranchFact>,
}

#[derive(Clone, Debug)]
pub struct MaterializedGraphShellNode {
    pub node_target: String,
    pub point: Point,
    pub created_at: String,
    pub created_at_ns: i128,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializedGraphShellBranchFact {
    pub name: String,
    pub head_id: String,
    pub state: coco_mem::SessionState,
}

pub struct MaterializedNodeReference {
    pub node_id: String,
    pub labels: Vec<String>,
}

impl ConsoleGraphSnapshotStore {
    pub async fn open(dir: impl AsRef<Path>) -> crate::Result<Self> {
        Self::open_with_policy(dir, GraphLayoutPolicy::default()).await
    }

    async fn open_with_policy(
        dir: impl AsRef<Path>,
        policy: GraphLayoutPolicy,
    ) -> crate::Result<Self> {
        let dir = dir.as_ref();
        let path = database_path(dir);
        let database = SnapshotDatabase::open(&path).await?;
        let store = Self {
            path: Arc::new(path),
            database,
            policy,
        };
        store.ensure_schema().await?;
        Ok(store)
    }

    pub async fn with_connection<T, F>(&self, operation: F) -> crate::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut SqliteConnection) -> crate::Result<T> + Send + 'static,
    {
        self.database.with_connection(operation).await
    }

    async fn with_write_connection<T, F>(
        &self,
        operation_name: impl Into<String>,
        operation: F,
    ) -> crate::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut SqliteConnection) -> crate::Result<T> + Send + 'static,
    {
        self.database
            .with_write_connection(operation_name, operation)
            .await
    }

    async fn ensure_schema(&self) -> crate::Result<()> {
        let path = self.path.as_ref().clone();
        self.with_write_connection("migrate snapshot schema", move |connection| {
            connection
                .run_pending_migrations(CONSOLE_GRAPH_MIGRATIONS)
                .map(|_| ())
                .context(crate::error::MigrateGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    pub(crate) fn database(&self) -> SnapshotDatabase {
        self.database.clone()
    }

    pub(crate) async fn active_generation(&self) -> crate::Result<i64> {
        let path = self.path.as_ref().clone();
        self.with_connection(move |connection| {
            query_active_generation(connection).context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    pub(crate) async fn active_generation_matches_source(&self) -> crate::Result<bool> {
        let path = self.path.as_ref().clone();
        self.with_connection(move |connection| {
            diesel::sql_query(
                "SELECT CASE WHEN \
                     EXISTS ( \
                         SELECT 1 \
                         FROM console_graph_generation_state AS state \
                         INNER JOIN console_graph_generation_source_revisions AS generation_source \
                             ON generation_source.generation = state.active_generation \
                         LEFT JOIN console_graph_overlay_runs AS overlay \
                             ON overlay.run_id = state.active_overlay_run_id \
                         INNER JOIN console_graph_source_identity AS source \
                             ON source.id = 1 \
                            AND source.revision = COALESCE( \
                                overlay.target_source_revision, \
                                generation_source.source_revision \
                            ) \
                         WHERE state.id = 1 \
                     ) \
                 THEN 1 ELSE 0 END AS count",
            )
            .get_result::<CountRow>(connection)
            .map(|row| row.count == 1)
            .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    pub(crate) async fn latest_resumable_build_version(&self) -> crate::Result<Option<u64>> {
        let path = self.path.as_ref().clone();
        self.with_connection(move |connection| {
            diesel::sql_query(
                "SELECT MAX(source_version) AS value \
                 FROM console_graph_build_runs \
                 WHERE status IN ('building', 'paused', 'compacting')",
            )
            .get_result::<OptionalBigIntRow>(connection)
            .map(|row| row.value.map(|value| value.max(0) as u64))
            .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    pub(crate) async fn allocate_staging_generation(&self) -> crate::Result<i64> {
        let path = self.path.as_ref().clone();
        self.with_write_connection("allocate staging generation", move |connection| {
            connection
                .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                    let generation = console_graph_generation_state::table
                        .filter(console_graph_generation_state::id.eq(1))
                        .select(console_graph_generation_state::next_generation)
                        .first::<i64>(connection)?;
                    diesel::update(
                        console_graph_generation_state::table
                            .filter(console_graph_generation_state::id.eq(1)),
                    )
                    .set(
                        console_graph_generation_state::next_generation
                            .eq(generation.saturating_add(1)),
                    )
                    .execute(connection)?;
                    Ok(generation)
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    pub(crate) async fn acquire_incremental_build_lease(
        &self,
        source_version: u64,
    ) -> crate::Result<Option<IncrementalBuildLease>> {
        let path = self.path.as_ref().clone();
        let owner_id = new_incremental_build_owner_id();
        let now_ms = unix_time_millis();
        let expires_at_ms = incremental_build_lease_deadline(now_ms);
        let expected_version = source_version.min(i64::MAX as u64) as i64;
        self.with_write_connection("acquire incremental build lease", move |connection| {
            connection
                .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                    let source_revision = diesel::sql_query(
                        "SELECT revision AS source_revision \
                         FROM console_graph_source_identity WHERE id = 1",
                    )
                    .get_result::<SourceRevisionRow>(connection)?
                    .source_revision;
                    let compacting = diesel::sql_query(
                        "SELECT overlay.run_id, overlay.lease_epoch, \
                                overlay.lease_expires_at_ms, overlay.target_source_version \
                                    AS source_version, \
                                overlay.target_source_revision AS frozen_source_revision \
                         FROM console_graph_generation_state AS state \
                         INNER JOIN console_graph_overlay_runs AS overlay \
                             ON overlay.run_id = state.active_overlay_run_id \
                         INNER JOIN console_graph_build_runs AS build \
                             ON build.run_id = overlay.run_id \
                         WHERE state.id = 1 AND overlay.status = 'compacting' \
                           AND build.status = 'compacting' \
                           AND build.owner_id = overlay.owner_id \
                           AND build.lease_epoch = overlay.lease_epoch",
                    )
                    .get_result::<CompactingBuildRunRow>(connection)
                    .optional()?;
                    if let Some(compacting) = compacting {
                        if compacting.lease_expires_at_ms > now_ms {
                            return Ok(None);
                        }
                        let lease_epoch = compacting.lease_epoch.saturating_add(1);
                        let overlay_updated = diesel::sql_query(
                            "UPDATE console_graph_overlay_runs \
                             SET owner_id = ?, lease_epoch = ?, lease_expires_at_ms = ?, \
                                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE run_id = ? AND status = 'compacting' \
                               AND lease_epoch = ? AND lease_expires_at_ms <= ?",
                        )
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<BigInt, _>(expires_at_ms)
                        .bind::<BigInt, _>(compacting.run_id)
                        .bind::<BigInt, _>(compacting.lease_epoch)
                        .bind::<BigInt, _>(now_ms)
                        .execute(connection)?;
                        let build_updated = diesel::sql_query(
                            "UPDATE console_graph_build_runs \
                             SET owner_id = ?, lease_epoch = ?, lease_expires_at_ms = ? \
                             WHERE run_id = ? AND status = 'compacting' \
                               AND lease_epoch = ? AND lease_expires_at_ms <= ?",
                        )
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<BigInt, _>(expires_at_ms)
                        .bind::<BigInt, _>(compacting.run_id)
                        .bind::<BigInt, _>(compacting.lease_epoch)
                        .bind::<BigInt, _>(now_ms)
                        .execute(connection)?;
                        if overlay_updated != 1 || build_updated != 1 {
                            return Ok(None);
                        }
                        return Ok(Some(IncrementalBuildLease {
                            generation: compacting.run_id,
                            owner_id,
                            lease_epoch,
                            target_source_version: compacting.source_version.max(0) as u64,
                            frozen_source_revision: compacting.frozen_source_revision,
                            phase: IncrementalBuildLeasePhase::Compacting,
                            resumed: true,
                        }));
                    }
                    diesel::sql_query(
                        "UPDATE console_graph_build_runs \
                         SET status = 'paused', lease_expires_at_ms = 0 \
                         WHERE status = 'building' AND lease_expires_at_ms <= ?",
                    )
                    .bind::<BigInt, _>(now_ms)
                    .execute(connection)?;
                    let active_build_exists = diesel::sql_query(
                        "SELECT CASE WHEN EXISTS ( \
                             SELECT 1 FROM console_graph_build_runs \
                             WHERE status IN ('building', 'compacting') LIMIT 1 \
                         ) THEN 1 ELSE 0 END AS count",
                    )
                    .get_result::<CountRow>(connection)?
                    .count
                        != 0;
                    if active_build_exists {
                        return Ok(None);
                    }

                    let paused = diesel::sql_query(
                        "SELECT run_id, lease_epoch, source_version, \
                                dag_source_revision AS frozen_source_revision \
                         FROM console_graph_build_runs \
                         WHERE status = 'paused' \
                         ORDER BY run_id LIMIT 1",
                    )
                    .get_result::<ResumableBuildRunRow>(connection)
                    .optional()?;
                    if let Some(paused) = paused {
                        let lease_epoch = paused.lease_epoch.saturating_add(1);
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_build_runs \
                             SET status = 'building', owner_id = ?, lease_epoch = ?, \
                                 lease_expires_at_ms = ? \
                             WHERE run_id = ? AND source_version = ? AND status = 'paused'",
                        )
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<BigInt, _>(expires_at_ms)
                        .bind::<BigInt, _>(paused.run_id)
                        .bind::<BigInt, _>(paused.source_version)
                        .execute(connection)?;
                        if updated != 1 {
                            return Ok(None);
                        }
                        return Ok(Some(IncrementalBuildLease {
                            generation: paused.run_id,
                            owner_id,
                            lease_epoch,
                            target_source_version: paused.source_version.max(0) as u64,
                            frozen_source_revision: paused.frozen_source_revision,
                            phase: IncrementalBuildLeasePhase::Building,
                            resumed: true,
                        }));
                    }

                    let generation = console_graph_generation_state::table
                        .filter(console_graph_generation_state::id.eq(1))
                        .select(console_graph_generation_state::next_generation)
                        .first::<i64>(connection)?;
                    diesel::update(
                        console_graph_generation_state::table
                            .filter(console_graph_generation_state::id.eq(1)),
                    )
                    .set(
                        console_graph_generation_state::next_generation
                            .eq(generation.saturating_add(1)),
                    )
                    .execute(connection)?;
                    let inserted = diesel::sql_query(
                        "INSERT INTO console_graph_build_runs \
                         (run_id, source_version, status, owner_id, lease_expires_at_ms, \
                          lease_epoch, dag_source_revision) \
                         SELECT ?, ?, 'building', ?, ?, 1, identity.revision \
                         FROM console_graph_source_identity AS identity \
                         WHERE identity.id = 1 AND identity.revision = ?",
                    )
                    .bind::<BigInt, _>(generation)
                    .bind::<BigInt, _>(expected_version)
                    .bind::<Text, _>(&owner_id)
                    .bind::<BigInt, _>(expires_at_ms)
                    .bind::<BigInt, _>(source_revision)
                    .execute(connection)?;
                    if inserted != 1 {
                        return Err(diesel::result::Error::NotFound);
                    }
                    Ok(Some(IncrementalBuildLease {
                        generation,
                        owner_id,
                        lease_epoch: 1,
                        target_source_version: expected_version.max(0) as u64,
                        frozen_source_revision: source_revision,
                        phase: IncrementalBuildLeasePhase::Building,
                        resumed: false,
                    }))
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    pub(crate) async fn renew_incremental_build_lease(
        &self,
        lease: &IncrementalBuildLease,
    ) -> crate::Result<bool> {
        let path = self.path.as_ref().clone();
        let generation = lease.generation;
        let owner_id = lease.owner_id.clone();
        let lease_epoch = lease.lease_epoch;
        let expires_at_ms = incremental_build_lease_deadline(unix_time_millis());
        self.with_write_connection("renew incremental build lease", move |connection| {
            connection
                .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                    let status = diesel::sql_query(
                        "SELECT status AS value FROM console_graph_build_runs \
                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                           AND status IN ('building', 'compacting')",
                    )
                    .bind::<BigInt, _>(generation)
                    .bind::<Text, _>(&owner_id)
                    .bind::<BigInt, _>(lease_epoch)
                    .get_result::<ScalarTextRow>(connection)
                    .optional()?;
                    let Some(status) = status else {
                        return Ok(false);
                    };
                    let build_updated = diesel::sql_query(
                        "UPDATE console_graph_build_runs SET lease_expires_at_ms = ? \
                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                           AND status = ?",
                    )
                    .bind::<BigInt, _>(expires_at_ms)
                    .bind::<BigInt, _>(generation)
                    .bind::<Text, _>(&owner_id)
                    .bind::<BigInt, _>(lease_epoch)
                    .bind::<Text, _>(&status.value)
                    .execute(connection)?;
                    if build_updated != 1 {
                        return Ok(false);
                    }
                    if status.value == "compacting" {
                        let overlay_updated = diesel::sql_query(
                            "UPDATE console_graph_overlay_runs \
                             SET lease_expires_at_ms = ?, \
                                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                               AND status = 'compacting' \
                               AND EXISTS ( \
                                   SELECT 1 FROM console_graph_generation_state \
                                   WHERE id = 1 AND active_overlay_run_id = ? \
                               )",
                        )
                        .bind::<BigInt, _>(expires_at_ms)
                        .bind::<BigInt, _>(generation)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<BigInt, _>(generation)
                        .execute(connection)?;
                        if overlay_updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                    }
                    Ok(true)
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    pub(crate) async fn latest_incremental_build_diagnostic_context(
        &self,
    ) -> crate::Result<Option<IncrementalBuildDiagnosticContext>> {
        let path = self.path.as_ref().clone();
        let row = self
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT runs.run_id AS build_run_id, \
                            runs.lease_epoch AS build_lease_epoch, \
                            runs.source_version AS target_process_local_invalidation_version, \
                            runs.dag_source_revision AS frozen_source_revision, \
                            runs.status AS persisted_build_state, \
                            COALESCE(( \
                                SELECT overlay.phase \
                                FROM console_graph_overlay_runs AS overlay \
                                WHERE overlay.run_id = runs.run_id \
                            ), CASE WHEN runs.dag_initialized = 0 \
                                THEN runs.dag_init_phase \
                                ELSE runs.dag_finalize_phase END) AS persisted_build_stage \
                     FROM console_graph_build_runs AS runs \
                     WHERE runs.status IN ('building', 'compacting', 'paused') \
                     ORDER BY CASE runs.status \
                         WHEN 'building' THEN 0 \
                         WHEN 'compacting' THEN 0 \
                         ELSE 1 END, runs.run_id \
                     LIMIT 1",
                )
                .get_result::<IncrementalBuildDiagnosticContextRow>(connection)
                .optional()
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        Ok(row.map(|row| IncrementalBuildDiagnosticContext {
            build_run_id: row.build_run_id,
            build_lease_epoch: row.build_lease_epoch,
            target_process_local_invalidation_version: row
                .target_process_local_invalidation_version
                .max(0) as u64,
            frozen_source_revision: row.frozen_source_revision,
            persisted_build_state: row.persisted_build_state,
            persisted_build_stage: row.persisted_build_stage,
        }))
    }

    pub(crate) async fn incremental_build_progress(
        &self,
        lease: &IncrementalBuildLease,
    ) -> crate::Result<IncrementalBuildProgress> {
        let path = self.path.as_ref().clone();
        let generation = lease.generation;
        let owner_id = lease.owner_id.clone();
        let lease_epoch = lease.lease_epoch;
        let row = self
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT runs.status AS build_status, \
                            (SELECT overlay.phase \
                             FROM console_graph_overlay_runs AS overlay \
                             WHERE overlay.run_id = runs.run_id) AS overlay_phase, \
                            runs.dag_initialized, runs.dag_init_phase, \
                            runs.dag_init_counter, runs.dag_finalize_phase, \
                            runs.dag_processed_node_count, \
                            runs.dag_discovered_node_count, \
                            (SELECT projection.phase \
                             FROM console_graph_build_shell_projections AS projection \
                             WHERE projection.run_id = runs.run_id \
                               AND projection.mode = runs.dag_finalize_phase) AS shell_phase \
                     FROM console_graph_build_runs AS runs \
                     WHERE runs.run_id = ? AND runs.owner_id = ? \
                       AND runs.lease_epoch = ? \
                       AND runs.status IN ('building', 'compacting')",
                )
                .bind::<BigInt, _>(generation)
                .bind::<Text, _>(owner_id)
                .bind::<BigInt, _>(lease_epoch)
                .get_result::<IncrementalBuildProgressRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        Ok(incremental_build_progress_from_row(&row))
    }

    pub(crate) async fn pause_incremental_build(
        &self,
        lease: &IncrementalBuildLease,
    ) -> crate::Result<bool> {
        let path = self.path.as_ref().clone();
        let generation = lease.generation;
        let owner_id = lease.owner_id.clone();
        let lease_epoch = lease.lease_epoch;
        self.with_write_connection("pause incremental build", move |connection| {
            connection
                .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                    let status = diesel::sql_query(
                        "SELECT status AS value FROM console_graph_build_runs \
                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                           AND status IN ('building', 'compacting')",
                    )
                    .bind::<BigInt, _>(generation)
                    .bind::<Text, _>(&owner_id)
                    .bind::<BigInt, _>(lease_epoch)
                    .get_result::<ScalarTextRow>(connection)
                    .optional()?;
                    let Some(status) = status else {
                        return Ok(false);
                    };
                    if status.value == "building" {
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_build_runs \
                             SET status = 'paused', lease_expires_at_ms = 0 \
                             WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                               AND status = 'building'",
                        )
                        .bind::<BigInt, _>(generation)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .execute(connection)?;
                        return Ok(updated == 1);
                    }
                    let overlay_updated = diesel::sql_query(
                        "UPDATE console_graph_overlay_runs \
                         SET lease_expires_at_ms = 0, \
                             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                           AND status = 'compacting'",
                    )
                    .bind::<BigInt, _>(generation)
                    .bind::<Text, _>(&owner_id)
                    .bind::<BigInt, _>(lease_epoch)
                    .execute(connection)?;
                    let build_updated = diesel::sql_query(
                        "UPDATE console_graph_build_runs SET lease_expires_at_ms = 0 \
                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                           AND status = 'compacting'",
                    )
                    .bind::<BigInt, _>(generation)
                    .bind::<Text, _>(&owner_id)
                    .bind::<BigInt, _>(lease_epoch)
                    .execute(connection)?;
                    Ok(overlay_updated == 1 && build_updated == 1)
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    pub(crate) async fn abandon_incremental_build(
        &self,
        lease: &IncrementalBuildLease,
    ) -> crate::Result<bool> {
        let path = self.path.as_ref().clone();
        let generation = lease.generation;
        let owner_id = lease.owner_id.clone();
        let lease_epoch = lease.lease_epoch;
        self.with_write_connection("abandon incremental build", move |connection| {
            connection
                .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                    let updated = diesel::sql_query(
                        "UPDATE console_graph_build_runs SET status = 'abandoned' \
                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                           AND status IN ('building', 'paused')",
                    )
                    .bind::<BigInt, _>(generation)
                    .bind::<diesel::sql_types::Text, _>(owner_id)
                    .bind::<BigInt, _>(lease_epoch)
                    .execute(connection)?;
                    Ok(updated == 1)
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    pub(crate) async fn cleanup_abandoned_generations(&self) -> crate::Result<()> {
        let path = self.path.as_ref().clone();
        let now_ms = unix_time_millis();
        self.with_write_connection("pause expired incremental builds", move |connection| {
            connection
                .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                    diesel::sql_query(
                        "UPDATE console_graph_build_runs \
                         SET status = 'paused', lease_expires_at_ms = 0 \
                         WHERE status = 'building' AND lease_expires_at_ms <= ?",
                    )
                    .bind::<BigInt, _>(now_ms)
                    .execute(connection)?;
                    Ok(())
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await?;

        for table in [
            "console_graph_node_locations",
            "console_graph_edge_routes",
            "console_graph_edge_ports",
            "console_graph_anchor_scopes",
            "console_graph_anchor_scope_manifests",
            "console_graph_materialization_time_ticks",
            "console_graph_materialization_shells",
            "console_graph_materializations",
            "console_graph_materialization_branches",
            "console_graph_generation_source_revisions",
            "console_graph_generation_delta_capabilities",
            "console_graph_branch_label_assignments",
        ] {
            loop {
                let path = self.path.as_ref().clone();
                let query = format!(
                    "DELETE FROM {table} WHERE rowid IN (\
                         SELECT rows.rowid FROM {table} AS rows \
                         INNER JOIN console_graph_build_runs AS runs \
                             ON runs.run_id = rows.generation \
                         INNER JOIN console_graph_generation_state AS state ON state.id = 1 \
                         WHERE runs.status = 'abandoned' \
                           AND rows.generation <> state.active_generation \
                           AND rows.generation <> COALESCE( \
                               state.active_overlay_run_id, state.active_generation \
                           ) \
                         LIMIT ?\
                     )"
                );
                let operation_name = format!("clean abandoned rows from {table}");
                let deleted = self
                    .with_write_connection(operation_name, move |connection| {
                        diesel::sql_query(query)
                            .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
                            .execute(connection)
                            .context(QueryGraphSnapshotStoreSnafu { path })
                    })
                    .await?;
                if deleted == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }

        for table in [
            "console_graph_build_branch_label_visited",
            "console_graph_build_branch_label_resolutions",
            "console_graph_build_anchor_ancestor_visits",
            "console_graph_build_anchor_raw_edges",
            "console_graph_build_anchor_projection_state",
            "console_graph_build_projection_edges",
            "console_graph_build_parent_satisfactions",
            "console_graph_build_parent_expansions",
            "console_graph_build_scope_queue",
            "console_graph_build_source_manifest",
            "console_graph_build_nodes",
            "console_graph_build_anchor_edges",
            "console_graph_build_frontier",
            "console_graph_build_rank_slots",
            "console_graph_build_edge_ports",
            "console_graph_build_source_refresh_manifest",
            "console_graph_build_branch_deltas",
            "console_graph_build_edge_route_tombstones",
            "console_graph_build_edge_port_tombstones",
            "console_graph_build_anchor_node_tombstones",
            "console_graph_build_scope_tombstones",
            "console_graph_build_changed_branches",
            "console_graph_build_delta_nodes",
            "console_graph_build_node_tombstones",
            "console_graph_build_label_affected_nodes",
            "console_graph_build_shell_tick_candidates",
            "console_graph_build_shell_projections",
        ] {
            loop {
                let path = self.path.as_ref().clone();
                let query = format!(
                    "DELETE FROM {table} WHERE rowid IN (\
                         SELECT rows.rowid FROM {table} AS rows \
                         INNER JOIN console_graph_build_runs AS runs \
                             ON runs.run_id = rows.run_id \
                         WHERE runs.status = 'abandoned' \
                         LIMIT ?\
                     )"
                );
                let operation_name = format!("clean abandoned rows from {table}");
                let deleted = self
                    .with_write_connection(operation_name, move |connection| {
                        diesel::sql_query(query)
                            .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
                            .execute(connection)
                            .context(QueryGraphSnapshotStoreSnafu { path })
                    })
                    .await?;
                if deleted == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }

        self.delete_finished_build_runs("abandoned", true).await
    }

    pub(crate) async fn cleanup_completed_build_work(&self) -> crate::Result<()> {
        for table in [
            "console_graph_build_branch_label_visited",
            "console_graph_build_branch_label_resolutions",
            "console_graph_build_anchor_ancestor_visits",
            "console_graph_build_anchor_raw_edges",
            "console_graph_build_anchor_projection_state",
            "console_graph_build_projection_edges",
            "console_graph_build_parent_satisfactions",
            "console_graph_build_parent_expansions",
            "console_graph_build_scope_queue",
            "console_graph_build_source_manifest",
            "console_graph_build_nodes",
            "console_graph_build_anchor_edges",
            "console_graph_build_frontier",
            "console_graph_build_rank_slots",
            "console_graph_build_edge_ports",
            "console_graph_build_source_refresh_manifest",
            "console_graph_build_branch_deltas",
            "console_graph_build_edge_route_tombstones",
            "console_graph_build_edge_port_tombstones",
            "console_graph_build_anchor_node_tombstones",
            "console_graph_build_scope_tombstones",
            "console_graph_build_changed_branches",
            "console_graph_build_delta_nodes",
            "console_graph_build_node_tombstones",
            "console_graph_build_label_affected_nodes",
            "console_graph_build_shell_tick_candidates",
            "console_graph_build_shell_projections",
        ] {
            loop {
                let path = self.path.as_ref().clone();
                let query = format!(
                    "DELETE FROM {table} WHERE rowid IN (\
                         SELECT rows.rowid FROM {table} AS rows \
                         INNER JOIN console_graph_build_runs AS runs \
                             ON runs.run_id = rows.run_id \
                         WHERE runs.status = 'completed' \
                         LIMIT ?\
                     )"
                );
                let operation_name = format!("clean completed rows from {table}");
                let deleted = self
                    .with_write_connection(operation_name, move |connection| {
                        diesel::sql_query(query)
                            .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
                            .execute(connection)
                            .context(QueryGraphSnapshotStoreSnafu { path })
                    })
                    .await?;
                if deleted == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        self.delete_finished_build_runs("completed", false).await?;
        self.cleanup_publication_receipts().await
    }

    async fn delete_finished_build_runs(
        &self,
        status: &'static str,
        retain_active_generations: bool,
    ) -> crate::Result<()> {
        loop {
            let deleted = self
                .delete_finished_build_run_batch(status, retain_active_generations)
                .await?;
            if deleted == 0 {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    async fn delete_finished_build_run_batch(
        &self,
        status: &'static str,
        retain_active_generations: bool,
    ) -> crate::Result<usize> {
        let path = self.path.as_ref().clone();
        let operation_name = format!("delete {status} build run batch");
        self.with_write_connection(operation_name, move |connection| {
            diesel::sql_query(
                "DELETE FROM console_graph_build_runs \
                 WHERE rowid IN ( \
                     SELECT runs.rowid \
                     FROM console_graph_build_runs AS runs \
                     CROSS JOIN console_graph_generation_state AS state \
                     WHERE state.id = 1 AND runs.status = ? \
                       AND (? = 0 OR ( \
                           runs.run_id <> state.active_generation \
                           AND runs.run_id <> COALESCE(state.active_overlay_run_id, -1) \
                       )) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_branch_label_visited \
                           WHERE run_id = runs.run_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_branch_label_resolutions \
                           WHERE run_id = runs.run_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_anchor_ancestor_visits \
                           WHERE run_id = runs.run_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_anchor_raw_edges \
                           WHERE run_id = runs.run_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_anchor_projection_state \
                           WHERE run_id = runs.run_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_projection_edges \
                           WHERE run_id = runs.run_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_parent_satisfactions \
                           WHERE run_id = runs.run_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_parent_expansions \
                           WHERE run_id = runs.run_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_scope_queue \
                           WHERE run_id = runs.run_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_source_manifest \
                           WHERE run_id = runs.run_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_nodes \
                           WHERE run_id = runs.run_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_anchor_edges \
                           WHERE run_id = runs.run_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_frontier \
                           WHERE run_id = runs.run_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_rank_slots \
                           WHERE run_id = runs.run_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_edge_ports \
                           WHERE run_id = runs.run_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_build_shell_projections \
                           WHERE run_id = runs.run_id \
                       ) \
                     ORDER BY runs.run_id \
                     LIMIT ? \
                 )",
            )
            .bind::<Text, _>(status)
            .bind::<Integer, _>(i32::from(retain_active_generations))
            .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
            .execute(connection)
            .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    async fn cleanup_publication_receipts(&self) -> crate::Result<()> {
        loop {
            let path = self.path.as_ref().clone();
            let deleted = self
                .with_write_connection("prune graph publication receipts", move |connection| {
                    diesel::sql_query(
                        "DELETE FROM console_graph_build_publications WHERE rowid IN ( \
                             SELECT publication.rowid \
                             FROM console_graph_build_publications AS publication \
                             WHERE NOT EXISTS ( \
                                 SELECT 1 FROM console_graph_build_runs AS run \
                                 WHERE run.run_id = publication.build_run_id \
                             ) \
                               AND publication.build_run_id NOT IN ( \
                                   SELECT retained.build_run_id \
                                   FROM console_graph_build_publications AS retained \
                                   ORDER BY retained.committed_at DESC, \
                                            retained.build_run_id DESC \
                                   LIMIT ? \
                               ) \
                             ORDER BY publication.committed_at, publication.build_run_id \
                             LIMIT ? \
                         )",
                    )
                    .bind::<BigInt, _>(PUBLICATION_RECEIPT_RETENTION)
                    .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
                    .execute(connection)
                    .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
            if deleted == 0 {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    pub(crate) async fn cleanup_obsolete_generations(&self) -> crate::Result<()> {
        for table in [
            "console_graph_node_locations",
            "console_graph_edge_routes",
            "console_graph_edge_ports",
            "console_graph_anchor_scopes",
            "console_graph_anchor_scope_manifests",
            "console_graph_materialization_time_ticks",
            "console_graph_materialization_shells",
            "console_graph_materializations",
            "console_graph_materialization_branches",
            "console_graph_generation_source_revisions",
            "console_graph_generation_delta_capabilities",
            "console_graph_branch_label_assignments",
        ] {
            loop {
                let path = self.path.as_ref().clone();
                let query = format!(
                    "DELETE FROM {table} WHERE rowid IN (\
                         SELECT rows.rowid FROM {table} AS rows \
                         INNER JOIN console_graph_generation_state AS state ON state.id = 1 \
                         WHERE rows.generation <> state.active_generation \
                           AND rows.generation <> COALESCE( \
                               state.active_overlay_run_id, state.active_generation \
                           ) \
                           AND NOT EXISTS ( \
                               SELECT 1 FROM console_graph_build_runs AS runs \
                               WHERE runs.run_id = rows.generation \
                                 AND runs.status IN ('building', 'paused', 'compacting') \
                           ) \
                         LIMIT ?\
                     )"
                );
                let operation_name = format!("clean obsolete rows from {table}");
                let deleted = self
                    .with_write_connection(operation_name, move |connection| {
                        connection
                            .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                                diesel::sql_query(query)
                                    .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
                                    .execute(connection)
                            })
                            .context(QueryGraphSnapshotStoreSnafu { path })
                    })
                    .await?;
                if deleted == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    }

    pub(crate) async fn write_incremental_batch(
        &self,
        lease: &IncrementalBuildLease,
        mode: GraphMode,
        nodes: Vec<GraphLayoutNode>,
        edges: Vec<GraphLayoutEdge>,
    ) -> crate::Result<()> {
        let generation = lease.generation;
        let nodes = nodes
            .iter()
            .map(PersistedNode::try_from)
            .collect::<crate::Result<Vec<_>>>()?;
        let edges = edges.iter().map(PersistedEdge::from).collect::<Vec<_>>();

        for batch in nodes.chunks(MATERIALIZATION_WRITE_BATCH_SIZE) {
            let batch = batch.to_vec();
            let path = self.path.as_ref().clone();
            let owner_id = lease.owner_id.clone();
            let lease_epoch = lease.lease_epoch;
            self.with_write_connection("write incremental node batch", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_incremental_build_fence(
                            connection,
                            generation,
                            &owner_id,
                            lease_epoch,
                        )?;
                        for node in &batch {
                            upsert_node(connection, generation, mode, node)?;
                        }
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
            tokio::task::yield_now().await;
        }

        for batch in edges.chunks(MATERIALIZATION_WRITE_BATCH_SIZE) {
            let batch = batch.to_vec();
            let path = self.path.as_ref().clone();
            let owner_id = lease.owner_id.clone();
            let lease_epoch = lease.lease_epoch;
            self.with_write_connection("write incremental edge batch", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_incremental_build_fence(
                            connection,
                            generation,
                            &owner_id,
                            lease_epoch,
                        )?;
                        for edge in &batch {
                            upsert_edge(connection, generation, mode, edge)?;
                        }
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    pub(crate) async fn finish_incremental_mode(
        &self,
        lease: &IncrementalBuildLease,
        source_version: u64,
        mode: GraphMode,
    ) -> crate::Result<()> {
        loop {
            let generation = lease.generation;
            let owner_id = lease.owner_id.clone();
            let lease_epoch = lease.lease_epoch;
            let path = self.path.as_ref().clone();
            let complete = self
                .with_write_connection(
                    "advance incremental graph mode finalization",
                    move |connection| {
                        connection
                            .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                                require_incremental_build_fence(
                                    connection,
                                    generation,
                                    &owner_id,
                                    lease_epoch,
                                )?;
                                advance_incremental_shell_projection(
                                    connection,
                                    generation,
                                    mode,
                                    source_version,
                                )
                            })
                            .context(QueryGraphSnapshotStoreSnafu { path })
                    },
                )
                .await?;
            if complete {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn publish_incremental_ports(
        &self,
        lease: &IncrementalBuildLease,
    ) -> crate::Result<()> {
        let generation = lease.generation;
        let run_id = lease.generation;
        let owner_id = lease.owner_id.clone();
        let lease_epoch = lease.lease_epoch;
        let path = self.path.as_ref().clone();
        self.with_write_connection("publish incremental edge ports", move |connection| {
            connection
                .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                    require_incremental_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                    diesel::sql_query("DELETE FROM console_graph_edge_ports WHERE generation = ?")
                        .bind::<BigInt, _>(generation)
                        .execute(connection)?;
                    diesel::sql_query(
                        "INSERT INTO console_graph_edge_ports (\
                             generation, mode, edge_key, source_id, target_id, \
                             source_slot, target_slot\
                         ) \
                         SELECT ?, mode, edge_key, source_id, target_id, source_slot, target_slot \
                         FROM console_graph_build_edge_ports \
                         WHERE run_id = ? AND active = 1",
                    )
                    .bind::<BigInt, _>(generation)
                    .bind::<BigInt, _>(run_id)
                    .execute(connection)?;
                    Ok(())
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    async fn incremental_publication_uses_overlay(&self, run_id: i64) -> crate::Result<bool> {
        let path = self.path.as_ref().clone();
        self.with_connection(move |connection| {
            diesel::sql_query(
                "SELECT CASE WHEN EXISTS ( \
                     SELECT 1 FROM console_graph_overlay_runs WHERE run_id = ? \
                 ) OR EXISTS ( \
                     SELECT 1 FROM console_graph_build_runs \
                     WHERE run_id = ? AND dag_build_kind = 'append' \
                 ) THEN 1 ELSE 0 END AS count",
            )
            .bind::<BigInt, _>(run_id)
            .bind::<BigInt, _>(run_id)
            .get_result::<CountRow>(connection)
            .map(|row| row.count == 1)
            .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    async fn publish_incremental_overlay(
        &self,
        lease: &IncrementalBuildLease,
        source_version: u64,
    ) -> crate::Result<GraphPublicationReceipt> {
        let run_id = lease.generation();
        let expected_version = source_version.min(i64::MAX as u64) as i64;
        self.ensure_overlay_publication_run(lease, expected_version)
            .await?;

        let mut last_reported_publication_stage = None;
        loop {
            let state = self.overlay_run_state(run_id).await?;
            ensure!(
                state.target_source_revision == lease.frozen_source_revision(),
                InvalidGraphSnapshotStoreValueSnafu {
                    column: "overlay_target_source_revision",
                    value: format!(
                        "build run {run_id} lease source revision {} differs from overlay source \
                         revision {}",
                        lease.frozen_source_revision(),
                        state.target_source_revision,
                    ),
                }
            );
            let publication_stage = (state.status.clone(), state.phase.clone());
            if last_reported_publication_stage.as_ref() != Some(&publication_stage) {
                tracing::info!(
                    rebuild_output_scope = "anchors_and_all",
                    build_run_id = run_id,
                    build_lease_epoch = lease.lease_epoch(),
                    build_target_process_local_invalidation_version = source_version,
                    build_frozen_source_revision = lease.frozen_source_revision(),
                    graph_build_kind = "append",
                    graph_publication_target_generation = state.base_generation,
                    graph_publication_target_source_revision = state.target_source_revision,
                    graph_publication_target_epoch = state.target_publication_epoch,
                    graph_publication_state = %state.status,
                    graph_publication_stage = %state.phase,
                    resumed_build_run = lease.is_resumed(),
                    "console graph publication stage changed",
                );
                last_reported_publication_stage = Some(publication_stage);
            }
            match state.status.as_str() {
                "completed" => {
                    return self
                        .completed_overlay_receipt(run_id, expected_version, true)
                        .await;
                }
                "preparing" if state.phase == "ready" => {
                    self.activate_incremental_overlay(lease, expected_version)
                        .await?;
                }
                "preparing" => {
                    self.advance_overlay_preparation_page(lease).await?;
                }
                "compacting" => {
                    self.advance_overlay_compaction_page(lease).await?;
                }
                value => {
                    return InvalidGraphSnapshotStoreValueSnafu {
                        column: "overlay_status",
                        value: value.to_owned(),
                    }
                    .fail();
                }
            }
            tokio::task::yield_now().await;
        }
    }

    async fn overlay_run_state(&self, run_id: i64) -> crate::Result<OverlayRunStateRow> {
        let path = self.path.as_ref().clone();
        self.with_connection(move |connection| {
            query_overlay_run_state(connection, run_id)
                .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    async fn ensure_overlay_publication_run(
        &self,
        lease: &IncrementalBuildLease,
        expected_version: i64,
    ) -> crate::Result<()> {
        let path = self.path.as_ref().clone();
        let run_id = lease.generation();
        let owner_id = lease.owner_id().to_owned();
        let lease_epoch = lease.lease_epoch();
        let now_ms = unix_time_millis();
        let expires_at_ms = incremental_build_lease_deadline(now_ms);
        self.with_write_connection("initialize incremental graph overlay", move |connection| {
            connection
                .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                    if diesel::sql_query(
                        "SELECT COUNT(*) AS count FROM console_graph_overlay_runs \
                         WHERE run_id = ?",
                    )
                    .bind::<BigInt, _>(run_id)
                    .get_result::<CountRow>(connection)?
                    .count
                        == 1
                    {
                        return Ok(());
                    }
                    require_incremental_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                    let inserted = diesel::sql_query(
                        "INSERT INTO console_graph_overlay_runs ( \
                             run_id, base_generation, baseline_source_revision, \
                             baseline_publication_epoch, target_source_version, \
                             target_source_revision, target_publication_epoch, status, phase, \
                             owner_id, lease_epoch, lease_expires_at_ms \
                         ) \
                         SELECT build.run_id, build.dag_baseline_generation, \
                                build.dag_baseline_source_revision, \
                                build.dag_baseline_publication_epoch, ?, \
                                build.dag_source_revision, \
                                build.dag_baseline_publication_epoch + 1, \
                                'preparing', 'route_tombstones', ?, ?, ? \
                         FROM console_graph_build_runs AS build \
                         INNER JOIN console_graph_generation_state AS state ON state.id = 1 \
                         INNER JOIN console_graph_generation_source_revisions AS revision \
                             ON revision.generation = state.active_generation \
                         INNER JOIN console_graph_generation_delta_capabilities AS capability \
                             ON capability.generation = state.active_generation \
                         WHERE build.run_id = ? AND build.owner_id = ? \
                           AND build.lease_epoch = ? AND build.status = 'building' \
                           AND build.lease_expires_at_ms > ? \
                           AND build.dag_build_kind = 'append' \
                           AND build.dag_initialized = 1 \
                           AND build.dag_finalize_phase = 'ready' \
                           AND build.source_version = ? \
                           AND state.active_overlay_run_id IS NULL \
                           AND state.active_generation = build.dag_baseline_generation \
                           AND revision.source_revision = \
                               build.dag_baseline_source_revision \
                           AND capability.publication_epoch = \
                               build.dag_baseline_publication_epoch \
                           AND capability.delta_compatible = 1",
                    )
                    .bind::<BigInt, _>(expected_version)
                    .bind::<Text, _>(&owner_id)
                    .bind::<BigInt, _>(lease_epoch)
                    .bind::<BigInt, _>(expires_at_ms)
                    .bind::<BigInt, _>(run_id)
                    .bind::<Text, _>(&owner_id)
                    .bind::<BigInt, _>(lease_epoch)
                    .bind::<BigInt, _>(now_ms)
                    .bind::<BigInt, _>(expected_version)
                    .execute(connection)?;
                    if inserted != 1 {
                        return Err(diesel::result::Error::NotFound);
                    }
                    Ok(())
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    async fn advance_overlay_preparation_page(
        &self,
        lease: &IncrementalBuildLease,
    ) -> crate::Result<()> {
        let path = self.path.as_ref().clone();
        let run_id = lease.generation();
        let owner_id = lease.owner_id().to_owned();
        let lease_epoch = lease.lease_epoch();
        let expires_at_ms = incremental_build_lease_deadline(unix_time_millis());
        self.with_write_connection(
            "prepare incremental graph overlay page",
            move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_incremental_build_fence(
                            connection,
                            run_id,
                            &owner_id,
                            lease_epoch,
                        )?;
                        let state = query_overlay_run_state(connection, run_id)?;
                        if state.status != "preparing" {
                            return Ok(());
                        }
                        advance_overlay_preparation(
                            connection,
                            run_id,
                            &owner_id,
                            lease_epoch,
                            &state,
                        )?;
                        let overlay_updated = diesel::sql_query(
                            "UPDATE console_graph_overlay_runs \
                         SET lease_expires_at_ms = ?, \
                             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                           AND status = 'preparing'",
                        )
                        .bind::<BigInt, _>(expires_at_ms)
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .execute(connection)?;
                        let build_updated = diesel::sql_query(
                            "UPDATE console_graph_build_runs SET lease_expires_at_ms = ? \
                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                           AND status = 'building'",
                        )
                        .bind::<BigInt, _>(expires_at_ms)
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .execute(connection)?;
                        if overlay_updated != 1 || build_updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            },
        )
        .await
    }

    async fn activate_incremental_overlay(
        &self,
        lease: &IncrementalBuildLease,
        expected_version: i64,
    ) -> crate::Result<()> {
        let path = self.path.as_ref().clone();
        let run_id = lease.generation();
        let owner_id = lease.owner_id().to_owned();
        let lease_epoch = lease.lease_epoch();
        self.with_write_connection("activate incremental graph overlay", move |connection| {
            connection
                .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                    require_incremental_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                    let state = query_overlay_run_state(connection, run_id)?;
                    if state.status == "compacting" || state.status == "completed" {
                        return Ok(());
                    }
                    if state.status != "preparing"
                        || state.phase != "ready"
                        || state.target_source_version != expected_version
                    {
                        return Err(diesel::result::Error::NotFound);
                    }
                    let ready_modes = diesel::sql_query(
                        "SELECT COUNT(*) AS count \
                         FROM console_graph_materializations AS materialization \
                         INNER JOIN console_graph_materialization_shells AS shell \
                             ON shell.generation = materialization.generation \
                            AND shell.mode = materialization.mode \
                         WHERE materialization.generation = ? \
                           AND materialization.source_version = ? \
                           AND materialization.coordinate_space = ? \
                           AND materialization.mode IN ('anchors', 'all')",
                    )
                    .bind::<BigInt, _>(run_id)
                    .bind::<BigInt, _>(expected_version)
                    .bind::<Text, _>(COORDINATE_SPACE)
                    .get_result::<CountRow>(connection)?
                    .count;
                    let ready_manifest = diesel::sql_query(
                        "SELECT COUNT(*) AS count \
                         FROM console_graph_anchor_scope_manifests WHERE generation = ?",
                    )
                    .bind::<BigInt, _>(run_id)
                    .get_result::<CountRow>(connection)?
                    .count;
                    if ready_modes != 2 || ready_manifest != 1 {
                        return Err(diesel::result::Error::NotFound);
                    }
                    let activated = diesel::sql_query(
                        "UPDATE console_graph_generation_state \
                         SET active_overlay_run_id = ? \
                         WHERE id = 1 AND active_overlay_run_id IS NULL \
                           AND active_generation = ? \
                           AND EXISTS ( \
                               SELECT 1 \
                               FROM console_graph_generation_source_revisions AS revision \
                               INNER JOIN console_graph_generation_delta_capabilities \
                                   AS capability \
                                   ON capability.generation = revision.generation \
                               WHERE revision.generation = ? \
                                 AND revision.source_revision = ? \
                                 AND capability.publication_epoch = ? \
                                 AND capability.delta_compatible = 1 \
                           )",
                    )
                    .bind::<BigInt, _>(run_id)
                    .bind::<BigInt, _>(state.base_generation)
                    .bind::<BigInt, _>(state.base_generation)
                    .bind::<BigInt, _>(state.baseline_source_revision)
                    .bind::<BigInt, _>(state.baseline_publication_epoch)
                    .execute(connection)?;
                    if activated != 1 {
                        return Err(diesel::result::Error::NotFound);
                    }
                    diesel::sql_query(
                        "INSERT OR IGNORE INTO console_graph_build_publications ( \
                             build_run_id, published_graph_generation, publication_epoch, \
                             build_kind, source_version, source_revision \
                         ) VALUES (?, ?, ?, 'append', ?, ?)",
                    )
                    .bind::<BigInt, _>(run_id)
                    .bind::<BigInt, _>(state.base_generation)
                    .bind::<BigInt, _>(state.target_publication_epoch)
                    .bind::<BigInt, _>(state.target_source_version)
                    .bind::<BigInt, _>(state.target_source_revision)
                    .execute(connection)?;
                    let overlay_updated = diesel::sql_query(
                        "UPDATE console_graph_overlay_runs \
                         SET status = 'compacting', phase = 'branch_delete', \
                             cursor_node_id = '', cursor_endpoint = '', \
                             cursor_mode = '', cursor_work_kind = '', cursor_key = '', \
                             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                           AND status = 'preparing' AND phase = 'ready'",
                    )
                    .bind::<BigInt, _>(run_id)
                    .bind::<Text, _>(&owner_id)
                    .bind::<BigInt, _>(lease_epoch)
                    .execute(connection)?;
                    let build_updated = diesel::sql_query(
                        "UPDATE console_graph_build_runs SET status = 'compacting' \
                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                           AND status = 'building'",
                    )
                    .bind::<BigInt, _>(run_id)
                    .bind::<Text, _>(&owner_id)
                    .bind::<BigInt, _>(lease_epoch)
                    .execute(connection)?;
                    if overlay_updated != 1 || build_updated != 1 {
                        return Err(diesel::result::Error::NotFound);
                    }
                    Ok(())
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    async fn advance_overlay_compaction_page(
        &self,
        lease: &IncrementalBuildLease,
    ) -> crate::Result<()> {
        let path = self.path.as_ref().clone();
        let run_id = lease.generation();
        let owner_id = lease.owner_id().to_owned();
        let lease_epoch = lease.lease_epoch();
        let expires_at_ms = incremental_build_lease_deadline(unix_time_millis());
        self.with_write_connection(
            "compact incremental graph overlay page",
            move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let state = require_overlay_compaction_fence(
                            connection,
                            run_id,
                            &owner_id,
                            lease_epoch,
                        )?;
                        if state.status == "completed" {
                            return Ok(());
                        }
                        advance_overlay_compaction(connection, run_id, &state)?;
                        diesel::sql_query(
                            "UPDATE console_graph_overlay_runs \
                         SET lease_expires_at_ms = ?, \
                             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                           AND status = 'compacting'",
                        )
                        .bind::<BigInt, _>(expires_at_ms)
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .execute(connection)?;
                        diesel::sql_query(
                            "UPDATE console_graph_build_runs SET lease_expires_at_ms = ? \
                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                           AND status = 'compacting'",
                        )
                        .bind::<BigInt, _>(expires_at_ms)
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            },
        )
        .await
    }

    async fn completed_overlay_receipt(
        &self,
        run_id: i64,
        expected_version: i64,
        replayed: bool,
    ) -> crate::Result<GraphPublicationReceipt> {
        let path = self.path.as_ref().clone();
        self.with_connection(move |connection| {
            diesel::sql_query(
                "SELECT publication.published_graph_generation, \
                        publication.publication_epoch, publication.source_version, \
                        publication.source_revision \
                 FROM console_graph_build_publications AS publication \
                 INNER JOIN console_graph_overlay_runs AS overlay \
                     ON overlay.run_id = publication.build_run_id \
                    AND overlay.status = 'completed' \
                 WHERE publication.build_run_id = ? \
                   AND publication.source_version = ?",
            )
            .bind::<BigInt, _>(run_id)
            .bind::<BigInt, _>(expected_version)
            .get_result::<PublicationReceiptRow>(connection)
            .map(|row| GraphPublicationReceipt {
                build_run_id: run_id,
                published_graph_generation: row.published_graph_generation,
                publication_epoch: row.publication_epoch,
                published_source_revision: row.source_revision,
                replayed,
            })
            .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    pub(crate) async fn publish_incremental_generation(
        &self,
        lease: &IncrementalBuildLease,
        source_version: u64,
    ) -> crate::Result<GraphPublicationReceipt> {
        if self
            .incremental_publication_uses_overlay(lease.generation())
            .await?
        {
            return self
                .publish_incremental_overlay(lease, source_version)
                .await;
        }
        let path = self.path.as_ref().clone();
        let build_run_id = lease.generation;
        let owner_id = lease.owner_id.clone();
        let build_lease_epoch = lease.lease_epoch;
        let expected_version = source_version.min(i64::MAX as u64) as i64;
        let now_ms = unix_time_millis();
        let outcome = self
            .with_write_connection("commit graph publication", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        if let Some(receipt) = diesel::sql_query(
                            "SELECT published_graph_generation, publication_epoch, source_version, \
                                    source_revision \
                             FROM console_graph_build_publications WHERE build_run_id = ?",
                        )
                        .bind::<BigInt, _>(build_run_id)
                        .get_result::<PublicationReceiptRow>(connection)
                        .optional()?
                        {
                            if receipt.source_version != expected_version {
                                return Ok(IncrementalPublishOutcome::Incomplete);
                            }
                            return Ok(IncrementalPublishOutcome::Published(
                                GraphPublicationReceipt {
                                    build_run_id,
                                    published_graph_generation: receipt.published_graph_generation,
                                    publication_epoch: receipt.publication_epoch,
                                    published_source_revision: receipt.source_revision,
                                    replayed: true,
                                },
                            ));
                        }

                        let build = diesel::sql_query(
                            "SELECT dag_source_revision AS source_revision, \
                                    dag_build_kind AS build_kind, \
                                    dag_baseline_generation AS baseline_generation, \
                                    dag_baseline_source_revision AS baseline_source_revision, \
                                    dag_baseline_publication_epoch AS baseline_publication_epoch \
                             FROM console_graph_build_runs \
                             WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                               AND status = 'building' AND lease_expires_at_ms > ?",
                        )
                        .bind::<BigInt, _>(build_run_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(build_lease_epoch)
                        .bind::<BigInt, _>(now_ms)
                        .get_result::<PublishBuildRow>(connection)
                        .optional()?;
                        let Some(build) = build else {
                            return Ok(IncrementalPublishOutcome::LeaseLost);
                        };
                        let active = diesel::sql_query(
                            "SELECT COALESCE(( \
                                        SELECT source_revision \
                                        FROM console_graph_generation_source_revisions \
                                        WHERE generation = state.active_generation \
                                    ), -1) AS source_revision \
                             FROM console_graph_generation_state AS state WHERE state.id = 1",
                        )
                        .get_result::<ActivePublicationRow>(connection)?;
                        if active.source_revision > build.source_revision {
                            diesel::sql_query(
                                "UPDATE console_graph_build_runs \
                                 SET status = 'abandoned', lease_expires_at_ms = 0 \
                                 WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                                   AND status = 'building'",
                            )
                            .bind::<BigInt, _>(build_run_id)
                            .bind::<Text, _>(&owner_id)
                            .bind::<BigInt, _>(build_lease_epoch)
                            .execute(connection)?;
                            return Ok(IncrementalPublishOutcome::Superseded);
                        }

                        let modes = console_graph_materializations::table
                            .inner_join(
                                console_graph_materialization_shells::table.on(
                                    console_graph_materializations::generation
                                        .eq(console_graph_materialization_shells::generation)
                                        .and(
                                            console_graph_materializations::mode
                                                .eq(console_graph_materialization_shells::mode),
                                        ),
                                ),
                            )
                            .filter(console_graph_materializations::generation.eq(build_run_id))
                            .filter(
                                console_graph_materializations::source_version.eq(expected_version),
                            )
                            .filter(
                                console_graph_materializations::coordinate_space
                                    .eq(COORDINATE_SPACE),
                            )
                            .select(console_graph_materializations::mode)
                            .load::<String>(connection)?;
                        let build_ready = diesel::sql_query(
                            "SELECT COUNT(*) AS count FROM console_graph_build_runs \
                             WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                               AND status = 'building' AND dag_initialized = 1 \
                               AND dag_finalize_phase = 'ready' \
                               AND dag_source_revision = ?",
                        )
                        .bind::<BigInt, _>(build_run_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(build_lease_epoch)
                        .bind::<BigInt, _>(build.source_revision)
                        .get_result::<CountRow>(connection)?
                        .count
                            == 1;
                        let complete = build_ready
                            && modes.into_iter().collect::<BTreeSet<_>>()
                                == BTreeSet::from([
                                    GraphMode::Anchors.as_query_value().to_owned(),
                                    GraphMode::All.as_query_value().to_owned(),
                                ]);
                        if !complete {
                            return Ok(IncrementalPublishOutcome::Incomplete);
                        }

                        if build.build_kind != "full" {
                            return Err(diesel::result::Error::NotFound);
                        }
                        diesel::sql_query(
                            "INSERT INTO console_graph_generation_source_revisions ( \
                                         generation, source_revision \
                                     ) VALUES (?, ?) \
                                     ON CONFLICT(generation) DO UPDATE SET \
                                         source_revision = excluded.source_revision",
                        )
                        .bind::<BigInt, _>(build_run_id)
                        .bind::<BigInt, _>(build.source_revision)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_generation_delta_capabilities ( \
                                         generation, delta_compatible, publication_epoch \
                                     ) VALUES (?, 1, 1) \
                                     ON CONFLICT(generation) DO UPDATE SET \
                                         delta_compatible = 1, publication_epoch = 1",
                        )
                        .bind::<BigInt, _>(build_run_id)
                        .execute(connection)?;
                        diesel::update(
                            console_graph_generation_state::table
                                .filter(console_graph_generation_state::id.eq(1)),
                        )
                        .set(console_graph_generation_state::active_generation.eq(build_run_id))
                        .execute(connection)?;
                        let published_graph_generation = build_run_id;
                        let publication_epoch = 1;

                        diesel::sql_query(
                            "INSERT INTO console_graph_build_publications ( \
                                 build_run_id, published_graph_generation, publication_epoch, \
                                 build_kind, source_version, source_revision \
                             ) VALUES (?, ?, ?, ?, ?, ?)",
                        )
                        .bind::<BigInt, _>(build_run_id)
                        .bind::<BigInt, _>(published_graph_generation)
                        .bind::<BigInt, _>(publication_epoch)
                        .bind::<Text, _>(&build.build_kind)
                        .bind::<BigInt, _>(expected_version)
                        .bind::<BigInt, _>(build.source_revision)
                        .execute(connection)?;
                        let completed = diesel::sql_query(
                            "UPDATE console_graph_build_runs \
                             SET status = 'completed', lease_expires_at_ms = 0, \
                                 dag_published_graph_generation = ?, \
                                 completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                               AND status = 'building' AND dag_source_revision = ?",
                        )
                        .bind::<BigInt, _>(published_graph_generation)
                        .bind::<BigInt, _>(build_run_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(build_lease_epoch)
                        .bind::<BigInt, _>(build.source_revision)
                        .execute(connection)?;
                        if completed != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(IncrementalPublishOutcome::Published(
                            GraphPublicationReceipt {
                                build_run_id,
                                published_graph_generation,
                                publication_epoch,
                                published_source_revision: build.source_revision,
                                replayed: false,
                            },
                        ))
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;

        let receipt = match outcome {
            IncrementalPublishOutcome::Published(receipt) => receipt,
            IncrementalPublishOutcome::LeaseLost => {
                return InvalidGraphSnapshotStoreValueSnafu {
                    column: "incremental_build_lease",
                    value: format!(
                        "build run {build_run_id} lease epoch {build_lease_epoch} is not owned"
                    ),
                }
                .fail();
            }
            IncrementalPublishOutcome::Incomplete => {
                return InvalidGraphSnapshotStoreValueSnafu {
                    column: "build_run_id",
                    value: format!(
                        "build run {build_run_id} is not ready for source version {source_version}"
                    ),
                }
                .fail();
            }
            IncrementalPublishOutcome::Superseded => {
                return InvalidGraphSnapshotStoreValueSnafu {
                    column: "source_revision",
                    value: format!(
                        "build run {build_run_id} source revision was superseded by the active \
                         publication"
                    ),
                }
                .fail();
            }
            IncrementalPublishOutcome::BaselineChanged {
                expected_generation,
                current_generation,
                expected_epoch,
                current_epoch,
                current_delta_compatible,
            } => {
                return InvalidGraphSnapshotStoreValueSnafu {
                    column: "baseline_publication",
                    value: format!(
                        "build run {build_run_id} expected graph generation \
                         {expected_generation} epoch {expected_epoch}, current graph generation \
                         is {current_generation} epoch {current_epoch}, delta compatible is \
                         {current_delta_compatible}"
                    ),
                }
                .fail();
            }
        };
        Ok(receipt)
    }
    pub(crate) async fn activate_generation(
        &self,
        generation: i64,
        source_version: u64,
    ) -> crate::Result<()> {
        let path = self.path.as_ref().clone();
        let expected_version = source_version.min(i64::MAX as u64) as i64;
        let complete = self
            .with_write_connection("activate graph generation", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let modes = console_graph_materializations::table
                            .inner_join(
                                console_graph_materialization_shells::table.on(
                                    console_graph_materializations::generation
                                        .eq(console_graph_materialization_shells::generation)
                                        .and(
                                            console_graph_materializations::mode
                                                .eq(console_graph_materialization_shells::mode),
                                        ),
                                ),
                            )
                            .filter(console_graph_materializations::generation.eq(generation))
                            .filter(
                                console_graph_materializations::source_version.eq(expected_version),
                            )
                            .filter(
                                console_graph_materializations::coordinate_space
                                    .eq(COORDINATE_SPACE),
                            )
                            .select(console_graph_materializations::mode)
                            .load::<String>(connection)?;
                        let complete = modes.into_iter().collect::<BTreeSet<_>>()
                            == BTreeSet::from([
                                GraphMode::Anchors.as_query_value().to_owned(),
                                GraphMode::All.as_query_value().to_owned(),
                            ]);
                        if !complete {
                            return Ok(false);
                        }
                        diesel::update(
                            console_graph_generation_state::table
                                .filter(console_graph_generation_state::id.eq(1)),
                        )
                        .set(console_graph_generation_state::active_generation.eq(generation))
                        .execute(connection)?;
                        Ok(true)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        ensure!(
            complete,
            InvalidGraphSnapshotStoreValueSnafu {
                column: "generation",
                value: format!(
                    "{generation} does not contain both graph modes for source version {source_version}"
                ),
            }
        );
        Ok(())
    }

    pub async fn latest_materialization_version(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Option<u64>> {
        Ok(self
            .latest_materialization_row(mode)
            .await?
            .map(|row| row.source_version.max(0) as u64))
    }

    pub async fn latest_materialization_row(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Option<MaterializationRow>> {
        let path = self.path.as_ref().clone();
        self.with_connection(move |connection| {
            read_snapshot(connection, |connection| {
                let generation =
                    query_active_snapshot_descriptor(connection)?.effective_generation();
                query_materialization_row_for_generation(connection, generation, mode)
            })
            .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    pub async fn has_materialization(&self, mode: GraphMode) -> crate::Result<bool> {
        Ok(self.latest_materialization_version(mode).await?.is_some())
    }

    #[cfg(test)]
    pub async fn materialize_snapshot(&self, snapshot: &GraphSnapshot) -> crate::Result<()> {
        let generation = self.active_generation().await?;
        self.materialize_snapshot_in_generation(snapshot, generation)
            .await
    }

    pub(crate) async fn active_publication_state(
        &self,
    ) -> crate::Result<ActiveGraphPublicationState> {
        let path = self.path.as_ref().clone();
        let row = self
            .with_connection(move |connection| {
                read_snapshot(connection, |connection| {
                    diesel::sql_query(
                        "SELECT state.active_generation AS graph_generation, \
                                (SELECT source_version \
                                 FROM console_graph_materializations \
                                 WHERE generation = COALESCE( \
                                           state.active_overlay_run_id, \
                                           state.active_generation \
                                       ) \
                                   AND mode = 'anchors') AS anchors_source_version, \
                                (SELECT source_version \
                                 FROM console_graph_materializations \
                                 WHERE generation = COALESCE( \
                                           state.active_overlay_run_id, \
                                           state.active_generation \
                                       ) \
                                   AND mode = 'all') AS all_source_version, \
                                COALESCE(overlay.target_source_revision, \
                                         revision.source_revision) AS source_revision, \
                                COALESCE(overlay.target_publication_epoch, \
                                         capability.publication_epoch) AS publication_epoch, \
                                source.revision AS current_source_revision, \
                                CASE WHEN overlay.status = 'compacting' THEN 1 ELSE 0 END \
                                    AS overlay_compaction_pending \
                         FROM console_graph_generation_state AS state \
                         CROSS JOIN console_graph_source_identity AS source \
                         LEFT JOIN console_graph_generation_source_revisions AS revision \
                             ON revision.generation = state.active_generation \
                         LEFT JOIN console_graph_generation_delta_capabilities AS capability \
                             ON capability.generation = state.active_generation \
                         LEFT JOIN console_graph_overlay_runs AS overlay \
                             ON overlay.run_id = state.active_overlay_run_id \
                         WHERE state.id = 1 AND source.id = 1",
                    )
                    .get_result::<ActivePublicationStateRow>(connection)
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        let anchors_source_version = row
            .anchors_source_version
            .and_then(|value| u64::try_from(value).ok());
        let all_source_version = row
            .all_source_version
            .and_then(|value| u64::try_from(value).ok());
        Ok(ActiveGraphPublicationState {
            graph_generation: row.graph_generation,
            anchors_source_version,
            all_source_version,
            source_revision: row.source_revision,
            publication_epoch: row.publication_epoch,
            matches_current_source: row.source_revision == Some(row.current_source_revision),
            overlay_compaction_pending: row.overlay_compaction_pending != 0,
        })
    }

    pub(crate) async fn advance_current_publication_version(
        &self,
        source_version: u64,
    ) -> crate::Result<bool> {
        let path = self.path.as_ref().clone();
        let expected_version = source_version.min(i64::MAX as u64) as i64;
        self.with_write_connection(
            "advance current graph publication version",
            move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_materializations \
                         SET source_version = MAX(source_version, ?), \
                             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                         WHERE generation = ( \
                                   SELECT active_generation \
                                   FROM console_graph_generation_state WHERE id = 1 \
                               ) \
                           AND mode IN ('anchors', 'all') \
                           AND ( \
                               SELECT COUNT(*) FROM console_graph_materializations AS complete \
                               WHERE complete.generation = ( \
                                   SELECT active_generation \
                                   FROM console_graph_generation_state WHERE id = 1 \
                               ) AND complete.mode IN ('anchors', 'all') \
                           ) = 2 \
                           AND EXISTS ( \
                               SELECT 1 \
                               FROM console_graph_generation_state AS state \
                               INNER JOIN console_graph_generation_source_revisions AS revision \
                                   ON revision.generation = state.active_generation \
                               INNER JOIN console_graph_source_identity AS source ON source.id = 1 \
                               WHERE state.id = 1 AND state.active_overlay_run_id IS NULL \
                                 AND revision.source_revision = source.revision \
                           )",
                        )
                        .bind::<BigInt, _>(expected_version)
                        .execute(connection)?;
                        Ok(updated == 2)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            },
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn materialize_checkpoint(
        &self,
        snapshot: &GraphSnapshot,
        baseline_generation: i64,
        generation: i64,
    ) -> crate::Result<()> {
        let layout_started = Instant::now();
        let previous = self
            .load_stored_graph_for_generation(baseline_generation, snapshot.mode)
            .await?;
        let plan = plan_materialization(snapshot, &previous, self.policy)?;
        self.write_checkpoint(snapshot, generation, plan, layout_started)
            .await
    }

    #[cfg(test)]
    async fn materialize_snapshot_in_generation(
        &self,
        snapshot: &GraphSnapshot,
        generation: i64,
    ) -> crate::Result<()> {
        let layout_started = Instant::now();
        let previous = self
            .load_stored_graph_for_generation(generation, snapshot.mode)
            .await?;
        let plan = plan_materialization(snapshot, &previous, self.policy)?;
        let layout_duration = layout_started.elapsed();
        let node_count = plan.layout.nodes.len();
        let edge_count = plan.layout.edges.len();
        tracing::info!(
            graph_mode = snapshot.mode.as_query_value(),
            process_local_invalidation_version = snapshot.version,
            layout_strategy = plan.strategy.as_str(),
            output_node_count = node_count,
            output_edge_count = edge_count,
            affected_node_count = plan.affected_nodes,
            affected_rank_count = plan.affected_ranks,
            layout_duration_ms = layout_duration.as_millis(),
            layout_fallback_reason = plan.fallback_reason.unwrap_or("none"),
            "console graph layout planned",
        );

        let write_started = Instant::now();
        let outcome = self
            .write_materialization(generation, snapshot.version, snapshot.mode, plan, previous)
            .await?;
        if matches!(&outcome, MaterializationWriteOutcome::Committed { .. }) {
            self.snapshot_materialization_branches(generation, snapshot)
                .await?;
        }
        match outcome {
            MaterializationWriteOutcome::Committed {
                strategy,
                fallback_reason,
            } => {
                tracing::info!(
                    graph_mode = snapshot.mode.as_query_value(),
                    process_local_invalidation_version = snapshot.version,
                    layout_strategy = strategy.as_str(),
                    output_node_count = node_count,
                    output_edge_count = edge_count,
                    write_duration_ms = write_started.elapsed().as_millis(),
                    layout_fallback_reason = fallback_reason.unwrap_or("none"),
                    "console graph materialization committed",
                );
            }
            MaterializationWriteOutcome::SkippedStale { current_version } => {
                tracing::info!(
                    graph_mode = snapshot.mode.as_query_value(),
                    process_local_invalidation_version = snapshot.version,
                    stored_source_version = current_version,
                    planned_node_count = node_count,
                    planned_edge_count = edge_count,
                    write_duration_ms = write_started.elapsed().as_millis(),
                    "console graph stale materialization skipped",
                );
            }
        }
        Ok(())
    }

    #[cfg(test)]
    async fn write_checkpoint(
        &self,
        snapshot: &GraphSnapshot,
        generation: i64,
        plan: PlannedMaterialization,
        layout_started: Instant,
    ) -> crate::Result<()> {
        let node_count = plan.layout.nodes.len();
        let edge_count = plan.layout.edges.len();
        tracing::info!(
            graph_mode = snapshot.mode.as_query_value(),
            process_local_invalidation_version = snapshot.version,
            checkpoint_generation = generation,
            layout_strategy = plan.strategy.as_str(),
            output_node_count = node_count,
            output_edge_count = edge_count,
            affected_node_count = plan.affected_nodes,
            affected_rank_count = plan.affected_ranks,
            layout_duration_ms = layout_started.elapsed().as_millis(),
            layout_fallback_reason = plan.fallback_reason.unwrap_or("none"),
            "console graph checkpoint layout planned",
        );

        let width = plan.layout.width;
        let height = plan.layout.height;
        let nodes = plan
            .layout
            .nodes
            .iter()
            .map(PersistedNode::try_from)
            .collect::<crate::Result<Vec<_>>>()?;
        let edges = plan
            .layout
            .edges
            .iter()
            .map(PersistedEdge::from)
            .collect::<Vec<_>>();
        let write_started = Instant::now();
        let mode = snapshot.mode;
        for batch in nodes.chunks(MATERIALIZATION_WRITE_BATCH_SIZE) {
            let batch = batch.to_vec();
            let path = self.path.as_ref().clone();
            self.with_write_connection("write checkpoint node batch", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        for node in &batch {
                            upsert_node(connection, generation, mode, node)?;
                        }
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
            tokio::task::yield_now().await;
        }
        for batch in edges.chunks(MATERIALIZATION_WRITE_BATCH_SIZE) {
            let batch = batch.to_vec();
            let path = self.path.as_ref().clone();
            self.with_write_connection("write checkpoint edge batch", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        for edge in &batch {
                            upsert_edge(connection, generation, mode, edge)?;
                        }
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
            tokio::task::yield_now().await;
        }
        let path = self.path.as_ref().clone();
        let source_version = snapshot.version;
        self.with_write_connection("commit graph checkpoint", move |connection| {
            connection
                .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                    upsert_materialization(
                        connection,
                        generation,
                        mode,
                        source_version,
                        width,
                        height,
                    )?;
                    write_materialized_shell_projection(connection, generation, mode)
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await?;
        self.snapshot_materialization_branches(generation, snapshot)
            .await?;
        tracing::info!(
            graph_mode = snapshot.mode.as_query_value(),
            process_local_invalidation_version = snapshot.version,
            checkpoint_generation = generation,
            output_node_count = node_count,
            output_edge_count = edge_count,
            write_duration_ms = write_started.elapsed().as_millis(),
            "console graph checkpoint committed",
        );
        Ok(())
    }

    pub async fn latest_viewport(
        &self,
        mode: GraphMode,
        request: GraphViewportRequest,
    ) -> crate::Result<Option<GraphViewportResponse>> {
        let path = self.path.as_ref().clone();
        self.with_connection(move |connection| {
            let stored = read_snapshot(connection, |connection| {
                query_viewport(connection, mode, request)
            })
            .context(QueryGraphSnapshotStoreSnafu { path })?;
            stored.map(stored_viewport_response).transpose()
        })
        .await
    }

    pub async fn latest_viewport_diff(
        &self,
        mode: GraphMode,
        request: GraphViewportDiffRequest,
    ) -> crate::Result<Option<GraphViewportDiffResponse>> {
        let path = self.path.as_ref().clone();
        self.with_connection(move |connection| {
            let (previous, current) = read_snapshot(connection, |connection| {
                Ok((
                    query_viewport(connection, mode, request.previous)?,
                    query_viewport(connection, mode, request.current)?,
                ))
            })
            .context(QueryGraphSnapshotStoreSnafu { path })?;
            let Some(previous) = previous else {
                return Ok(None);
            };
            let Some(current) = current else {
                return Ok(None);
            };
            Ok(Some(diff_graph_viewport_responses(
                stored_viewport_response(previous)?,
                stored_viewport_response(current)?,
                request.known.as_ref(),
            )))
        })
        .await
    }

    pub(crate) async fn materialized_node_reference(
        &self,
        mode: GraphMode,
        target: &str,
    ) -> crate::Result<Option<MaterializedNodeReference>> {
        let this = self.clone();
        let target = target.to_owned();
        self.with_connection(move |connection| {
            let row = read_snapshot(connection, |connection| {
                let descriptor = query_active_snapshot_descriptor(connection)?;
                let mut row = diesel::sql_query(
                    "SELECT node_id, node_key, node_target, short_id, node_kind, summary, \
                            labels_json, rank, sort_order, x, y, created_at, created_at_ns, \
                            min_x, min_y, max_x, max_y \
                     FROM console_graph_node_locations AS overlay \
                     WHERE overlay.generation = ? AND overlay.mode = ? \
                       AND (overlay.node_target = ? OR overlay.node_id = ? \
                            OR overlay.node_key = ?) \
                       AND NOT EXISTS ( \
                           SELECT 1 \
                           FROM console_graph_build_node_tombstones AS removed \
                           WHERE removed.run_id = ? AND removed.node_id = overlay.node_id \
                       ) \
                       AND (? <> 'anchors' OR NOT EXISTS ( \
                           SELECT 1 \
                           FROM console_graph_build_anchor_node_tombstones AS removed \
                           WHERE removed.run_id = ? AND removed.node_id = overlay.node_id \
                       )) \
                     UNION ALL \
                     SELECT base.node_id, base.node_key, base.node_target, base.short_id, \
                            base.node_kind, base.summary, base.labels_json, base.rank, \
                            base.sort_order, base.x, base.y, base.created_at, \
                            base.created_at_ns, base.min_x, base.min_y, base.max_x, base.max_y \
                     FROM console_graph_node_locations AS base \
                     WHERE base.generation = ? AND base.mode = ? \
                       AND (base.node_target = ? OR base.node_id = ? OR base.node_key = ?) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_node_locations AS overlay \
                           WHERE overlay.generation = ? AND overlay.mode = base.mode \
                             AND overlay.node_id = base.node_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 \
                           FROM console_graph_build_node_tombstones AS removed \
                           WHERE removed.run_id = ? AND removed.node_id = base.node_id \
                       ) \
                       AND (? <> 'anchors' OR NOT EXISTS ( \
                           SELECT 1 \
                           FROM console_graph_build_anchor_node_tombstones AS removed \
                           WHERE removed.run_id = ? AND removed.node_id = base.node_id \
                       )) \
                     ORDER BY node_id LIMIT 1",
                )
                .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
                .bind::<Text, _>(mode.as_query_value())
                .bind::<Text, _>(&target)
                .bind::<Text, _>(&target)
                .bind::<Text, _>(&target)
                .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
                .bind::<Text, _>(mode.as_query_value())
                .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
                .bind::<BigInt, _>(descriptor.base_generation)
                .bind::<Text, _>(mode.as_query_value())
                .bind::<Text, _>(&target)
                .bind::<Text, _>(&target)
                .bind::<Text, _>(&target)
                .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
                .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
                .bind::<Text, _>(mode.as_query_value())
                .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
                .get_result::<StoredNodeRow>(connection)
                .optional()?;
                if let Some(node) = row.as_mut() {
                    hydrate_effective_node_labels(
                        connection,
                        descriptor,
                        mode,
                        std::slice::from_mut(node),
                    )?;
                }
                Ok(row)
            })
            .context(QueryGraphSnapshotStoreSnafu {
                path: this.path.as_ref().clone(),
            })?;
            row.map(|row| {
                Ok(MaterializedNodeReference {
                    node_id: row.node_id,
                    labels: parse_labels(&row.labels_json)?,
                })
            })
            .transpose()
        })
        .await
    }

    pub(crate) async fn materialized_node_points(
        &self,
        mode: GraphMode,
        node_ids: &BTreeSet<String>,
    ) -> crate::Result<BTreeMap<String, Point>> {
        let this = self.clone();
        let node_ids = node_ids.iter().cloned().collect::<Vec<_>>();
        self.with_connection(move |connection| {
            if node_ids.is_empty() {
                return Ok(BTreeMap::new());
            }
            let rows = read_snapshot(connection, |connection| {
                let descriptor = query_active_snapshot_descriptor(connection)?;
                let mut rows = Vec::with_capacity(node_ids.len());
                for batch in node_ids.chunks(128) {
                    let requested_json = serde_json::to_string(batch).map_err(|error| {
                        diesel::result::Error::SerializationError(Box::new(error))
                    })?;
                    rows.extend(
                        diesel::sql_query(
                            "WITH requested(node_id) AS (SELECT value FROM json_each(?)) \
                             SELECT overlay.node_id, overlay.x, overlay.y \
                             FROM console_graph_node_locations AS overlay \
                             INNER JOIN requested ON requested.node_id = overlay.node_id \
                             WHERE overlay.generation = ? AND overlay.mode = ? \
                               AND NOT EXISTS ( \
                                   SELECT 1 \
                                   FROM console_graph_build_node_tombstones AS removed \
                                   WHERE removed.run_id = ? \
                                     AND removed.node_id = overlay.node_id \
                               ) \
                               AND (? <> 'anchors' OR NOT EXISTS ( \
                                   SELECT 1 \
                                   FROM console_graph_build_anchor_node_tombstones AS removed \
                                   WHERE removed.run_id = ? \
                                     AND removed.node_id = overlay.node_id \
                               )) \
                             UNION ALL \
                             SELECT base.node_id, base.x, base.y \
                             FROM console_graph_node_locations AS base \
                             INNER JOIN requested ON requested.node_id = base.node_id \
                             WHERE base.generation = ? AND base.mode = ? \
                               AND NOT EXISTS ( \
                                   SELECT 1 FROM console_graph_node_locations AS overlay \
                                   WHERE overlay.generation = ? AND overlay.mode = base.mode \
                                     AND overlay.node_id = base.node_id \
                               ) \
                               AND NOT EXISTS ( \
                                   SELECT 1 \
                                   FROM console_graph_build_node_tombstones AS removed \
                                   WHERE removed.run_id = ? AND removed.node_id = base.node_id \
                               ) \
                               AND (? <> 'anchors' OR NOT EXISTS ( \
                                   SELECT 1 \
                                   FROM console_graph_build_anchor_node_tombstones AS removed \
                                   WHERE removed.run_id = ? AND removed.node_id = base.node_id \
                               ))",
                        )
                        .bind::<Text, _>(requested_json)
                        .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
                        .bind::<Text, _>(mode.as_query_value())
                        .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
                        .bind::<Text, _>(mode.as_query_value())
                        .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
                        .bind::<BigInt, _>(descriptor.base_generation)
                        .bind::<Text, _>(mode.as_query_value())
                        .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
                        .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
                        .bind::<Text, _>(mode.as_query_value())
                        .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
                        .load::<EffectiveNodePointRow>(connection)?,
                    );
                }
                Ok(rows)
            })
            .context(QueryGraphSnapshotStoreSnafu {
                path: this.path.as_ref().clone(),
            })?;
            Ok(rows
                .into_iter()
                .map(|row| (row.node_id, Point { x: row.x, y: row.y }))
                .collect())
        })
        .await
    }

    pub(crate) async fn materialized_shell_facts(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Option<MaterializedGraphShellFacts>> {
        let path = self.path.as_ref().clone();
        self.with_connection(move |connection| {
            let stored =
                read_snapshot(connection, |connection| query_shell_facts(connection, mode))
                    .context(QueryGraphSnapshotStoreSnafu { path })?;
            let Some(stored) = stored else {
                return Ok(None);
            };
            let branches = stored
                .branches
                .into_iter()
                .map(|row| {
                    let state = serde_json::from_str(&row.state_json).context(
                        ParseGraphSnapshotStoreValueSnafu {
                            column: "state_json",
                        },
                    )?;
                    Ok(MaterializedGraphShellBranchFact {
                        name: row.name,
                        head_id: row.head_id,
                        state,
                    })
                })
                .collect::<crate::Result<Vec<_>>>()?;
            Ok(Some(MaterializedGraphShellFacts {
                version: stored.materialization.source_version.max(0) as u64,
                node_count: stored.node_count.max(0) as usize,
                nodes: stored
                    .nodes
                    .into_iter()
                    .map(|row| MaterializedGraphShellNode {
                        node_target: row.node_target,
                        point: Point { x: row.x, y: row.y },
                        created_at: row.created_at,
                        created_at_ns: i128::from(row.created_at_ns),
                    })
                    .collect(),
                edge_count: stored.edge_count.max(0) as usize,
                branches,
            }))
        })
        .await
    }

    #[cfg(test)]
    async fn snapshot_materialization_branches(
        &self,
        generation: i64,
        snapshot: &GraphSnapshot,
    ) -> crate::Result<()> {
        let branches = snapshot
            .branches
            .iter()
            .map(|branch| {
                Ok(PersistedMaterializationBranch {
                    name: branch.name.clone(),
                    head_id: branch.head_id.clone(),
                    state_json: serde_json::to_string(&branch.state).context(
                        SerializeGraphSnapshotStoreValueSnafu {
                            column: "state_json",
                        },
                    )?,
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let path = self.path.as_ref().clone();
        self.with_write_connection("snapshot materialization branches", move |connection| {
            connection
                .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                    diesel::sql_query(
                        "DELETE FROM console_graph_materialization_branches \
                         WHERE generation = ?",
                    )
                    .bind::<BigInt, _>(generation)
                    .execute(connection)?;
                    for branch in branches {
                        diesel::sql_query(
                            "INSERT INTO console_graph_materialization_branches \
                             (generation, name, head_id, state_json) \
                             VALUES (?, ?, ?, ?)",
                        )
                        .bind::<BigInt, _>(generation)
                        .bind::<Text, _>(branch.name)
                        .bind::<Text, _>(branch.head_id)
                        .bind::<Text, _>(branch.state_json)
                        .execute(connection)?;
                    }
                    Ok(())
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    #[cfg(test)]
    async fn load_stored_graph(&self, mode: GraphMode) -> crate::Result<StoredGraph> {
        let generation = self.active_generation().await?;
        self.load_stored_graph_for_generation(generation, mode)
            .await
    }

    #[cfg(test)]
    async fn load_stored_graph_for_generation(
        &self,
        generation: i64,
        mode: GraphMode,
    ) -> crate::Result<StoredGraph> {
        let path = self.path.as_ref().clone();
        self.with_connection(move |connection| {
            read_snapshot(connection, |connection| {
                let materialization =
                    query_materialization_row_for_generation(connection, generation, mode)?;
                let nodes = query_nodes_for_generation(connection, generation, mode)?
                    .into_iter()
                    .map(|row| (row.node_id.clone(), row))
                    .collect();
                let edges = query_edges_for_generation(connection, generation, mode)?
                    .into_iter()
                    .map(|row| (row.edge_key.clone(), row))
                    .collect();
                Ok(StoredGraph {
                    materialization,
                    nodes,
                    edges,
                })
            })
            .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    #[cfg(test)]
    async fn write_materialization(
        &self,
        generation: i64,
        source_version: u64,
        mode: GraphMode,
        plan: PlannedMaterialization,
        previous: StoredGraph,
    ) -> crate::Result<MaterializationWriteOutcome> {
        let strategy = plan.strategy;
        let previous_version = previous
            .materialization
            .as_ref()
            .map(|row| row.source_version.max(0) as u64);
        let width = plan.layout.width;
        let height = plan.layout.height;
        let nodes = plan
            .layout
            .nodes
            .iter()
            .map(PersistedNode::try_from)
            .collect::<crate::Result<Vec<_>>>()?;
        let edges = plan
            .layout
            .edges
            .iter()
            .map(PersistedEdge::from)
            .collect::<Vec<_>>();
        let path = self.path.as_ref().clone();
        self.with_write_connection("write graph materialization", move |connection| {
            connection
                .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                    let current_version = console_graph_materializations::table
                        .filter(console_graph_materializations::generation.eq(generation))
                        .filter(console_graph_materializations::mode.eq(mode.as_query_value()))
                        .filter(
                            console_graph_materializations::coordinate_space.eq(COORDINATE_SPACE),
                        )
                        .select(console_graph_materializations::source_version)
                        .first::<i64>(connection)
                        .optional()?
                        .map(|version| version.max(0) as u64);
                    if let Some(current_version) =
                        current_version.filter(|current| *current >= source_version)
                    {
                        return Ok(MaterializationWriteOutcome::SkippedStale { current_version });
                    }

                    let baseline_changed = strategy != MaterializationStrategy::Full
                        && current_version != previous_version;
                    let effective_strategy = if baseline_changed {
                        MaterializationStrategy::Full
                    } else {
                        strategy
                    };
                    if effective_strategy == MaterializationStrategy::Full {
                        delete_mode_rows(connection, generation, mode)?;
                        for node in &nodes {
                            upsert_node(connection, generation, mode, node)?;
                        }
                        for edge in &edges {
                            upsert_edge(connection, generation, mode, edge)?;
                        }
                    } else {
                        write_changed_rows(
                            connection, generation, mode, &previous, &nodes, &edges,
                        )?;
                    }
                    upsert_materialization(
                        connection,
                        generation,
                        mode,
                        source_version,
                        width,
                        height,
                    )?;
                    write_materialized_shell_projection(connection, generation, mode)?;
                    Ok(MaterializationWriteOutcome::Committed {
                        strategy: effective_strategy,
                        fallback_reason: baseline_changed
                            .then_some("materialization_baseline_changed"),
                    })
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }
}

fn read_snapshot<T>(
    connection: &mut SqliteConnection,
    operation: impl FnOnce(&mut SqliteConnection) -> QueryResult<T>,
) -> QueryResult<T> {
    connection.transaction(operation)
}

fn query_active_generation(connection: &mut SqliteConnection) -> QueryResult<i64> {
    console_graph_generation_state::table
        .filter(console_graph_generation_state::id.eq(1))
        .select(console_graph_generation_state::active_generation)
        .first(connection)
}

fn query_active_snapshot_descriptor(
    connection: &mut SqliteConnection,
) -> QueryResult<ActiveSnapshotDescriptor> {
    diesel::sql_query(
        "SELECT active_generation AS base_generation, \
                active_overlay_run_id AS overlay_run_id \
         FROM console_graph_generation_state WHERE id = 1",
    )
    .get_result(connection)
}

fn require_incremental_build_fence(
    connection: &mut SqliteConnection,
    run_id: i64,
    owner_id: &str,
    lease_epoch: i64,
) -> QueryResult<()> {
    let owned = diesel::sql_query(
        "SELECT COUNT(*) AS count FROM console_graph_build_runs \
         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? AND status = 'building'",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(owner_id)
    .bind::<BigInt, _>(lease_epoch)
    .get_result::<CountRow>(connection)?
    .count
        == 1;
    if owned {
        Ok(())
    } else {
        Err(diesel::result::Error::NotFound)
    }
}

fn query_overlay_run_state(
    connection: &mut SqliteConnection,
    run_id: i64,
) -> QueryResult<OverlayRunStateRow> {
    diesel::sql_query(
        "SELECT base_generation, baseline_source_revision, \
                baseline_publication_epoch, target_source_version, \
                target_source_revision, target_publication_epoch, status, phase, \
                cursor_node_id, cursor_endpoint, cursor_mode, cursor_work_kind, cursor_key \
         FROM console_graph_overlay_runs WHERE run_id = ?",
    )
    .bind::<BigInt, _>(run_id)
    .get_result(connection)
}

fn checkpoint_overlay_preparation_phase(
    connection: &mut SqliteConnection,
    run_id: i64,
    owner_id: &str,
    lease_epoch: i64,
    current_phase: &str,
    next_phase: &str,
) -> QueryResult<()> {
    let updated = diesel::sql_query(
        "UPDATE console_graph_overlay_runs \
         SET phase = ?, cursor_node_id = '', cursor_endpoint = '', \
             cursor_mode = '', cursor_work_kind = '', cursor_key = '', \
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
           AND status = 'preparing' AND phase = ?",
    )
    .bind::<Text, _>(next_phase)
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(owner_id)
    .bind::<BigInt, _>(lease_epoch)
    .bind::<Text, _>(current_phase)
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(())
}

fn advance_overlay_preparation(
    connection: &mut SqliteConnection,
    run_id: i64,
    owner_id: &str,
    lease_epoch: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    match state.phase.as_str() {
        "route_tombstones" => advance_overlay_edge_tombstones(
            connection,
            run_id,
            owner_id,
            lease_epoch,
            state,
            "console_graph_edge_routes",
            "console_graph_build_edge_route_tombstones",
            "port_tombstones",
        ),
        "port_tombstones" => advance_overlay_edge_tombstones(
            connection,
            run_id,
            owner_id,
            lease_epoch,
            state,
            "console_graph_edge_ports",
            "console_graph_build_edge_port_tombstones",
            "ready",
        ),
        "ready" => Ok(()),
        _ => Err(diesel::result::Error::NotFound),
    }
}

#[allow(clippy::too_many_arguments)]
fn advance_overlay_edge_tombstones(
    connection: &mut SqliteConnection,
    run_id: i64,
    owner_id: &str,
    lease_epoch: i64,
    state: &OverlayRunStateRow,
    source_table: &str,
    tombstone_table: &str,
    next_phase: &str,
) -> QueryResult<()> {
    const PAGE_SIZE: i64 = 128;
    if state.cursor_work_kind.is_empty() {
        let updated = diesel::sql_query(
            "UPDATE console_graph_overlay_runs \
             SET cursor_work_kind = 'global', cursor_mode = 'all' \
             WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
               AND status = 'preparing' AND phase = ? \
               AND cursor_work_kind = '' AND cursor_mode = '' \
               AND cursor_node_id = '' AND cursor_endpoint = '' AND cursor_key = ''",
        )
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(owner_id)
        .bind::<BigInt, _>(lease_epoch)
        .bind::<Text, _>(&state.phase)
        .execute(connection)?;
        if updated != 1 {
            return Err(diesel::result::Error::NotFound);
        }
        return Ok(());
    }

    let removed_table = match state.cursor_work_kind.as_str() {
        "global" => "console_graph_build_node_tombstones",
        "anchor_only" if state.cursor_mode == "anchors" => {
            "console_graph_build_anchor_node_tombstones"
        }
        _ => return Err(diesel::result::Error::NotFound),
    };
    if state.cursor_endpoint.is_empty() {
        let next = diesel::sql_query(format!(
            "SELECT node_id AS value FROM {removed_table} \
             WHERE run_id = ? AND node_id > ? ORDER BY node_id LIMIT 1"
        ))
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&state.cursor_node_id)
        .get_result::<ScalarTextRow>(connection)
        .optional()?;
        let Some(next) = next else {
            let next_work = match (state.cursor_work_kind.as_str(), state.cursor_mode.as_str()) {
                ("global", "all") => Some(("global", "anchors")),
                ("global", "anchors") => Some(("anchor_only", "anchors")),
                ("anchor_only", "anchors") => None,
                _ => return Err(diesel::result::Error::NotFound),
            };
            if let Some((work_kind, mode)) = next_work {
                let updated = diesel::sql_query(
                    "UPDATE console_graph_overlay_runs \
                     SET cursor_work_kind = ?, cursor_mode = ?, cursor_node_id = '', \
                         cursor_endpoint = '', cursor_key = '' \
                     WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                       AND status = 'preparing' AND phase = ? \
                       AND cursor_work_kind = ? AND cursor_mode = ? \
                       AND cursor_node_id = ? AND cursor_endpoint = '' AND cursor_key = ''",
                )
                .bind::<Text, _>(work_kind)
                .bind::<Text, _>(mode)
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(owner_id)
                .bind::<BigInt, _>(lease_epoch)
                .bind::<Text, _>(&state.phase)
                .bind::<Text, _>(&state.cursor_work_kind)
                .bind::<Text, _>(&state.cursor_mode)
                .bind::<Text, _>(&state.cursor_node_id)
                .execute(connection)?;
                if updated != 1 {
                    return Err(diesel::result::Error::NotFound);
                }
                return Ok(());
            }
            return checkpoint_overlay_preparation_phase(
                connection,
                run_id,
                owner_id,
                lease_epoch,
                &state.phase,
                next_phase,
            );
        };
        let updated = diesel::sql_query(
            "UPDATE console_graph_overlay_runs \
             SET cursor_node_id = ?, cursor_endpoint = 'source', cursor_key = '' \
             WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
               AND status = 'preparing' AND phase = ? \
               AND cursor_work_kind = ? AND cursor_node_id = ? AND cursor_mode = ? \
               AND cursor_endpoint = '' AND cursor_key = ''",
        )
        .bind::<Text, _>(next.value)
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(owner_id)
        .bind::<BigInt, _>(lease_epoch)
        .bind::<Text, _>(&state.phase)
        .bind::<Text, _>(&state.cursor_work_kind)
        .bind::<Text, _>(&state.cursor_node_id)
        .bind::<Text, _>(&state.cursor_mode)
        .execute(connection)?;
        if updated != 1 {
            return Err(diesel::result::Error::NotFound);
        }
        return Ok(());
    }

    let endpoint_column = match state.cursor_endpoint.as_str() {
        "source" => "source_id",
        "target" => "target_id",
        _ => return Err(diesel::result::Error::NotFound),
    };
    let edge_keys = diesel::sql_query(format!(
        "SELECT edge_key AS value FROM {source_table} \
         WHERE generation = ? AND mode = ? AND {endpoint_column} = ? \
           AND edge_key > ? ORDER BY edge_key LIMIT ?"
    ))
    .bind::<BigInt, _>(state.base_generation)
    .bind::<Text, _>(&state.cursor_mode)
    .bind::<Text, _>(&state.cursor_node_id)
    .bind::<Text, _>(&state.cursor_key)
    .bind::<BigInt, _>(PAGE_SIZE)
    .load::<ScalarTextRow>(connection)?;
    if edge_keys.is_empty() {
        let (next_endpoint, next_key) = if state.cursor_endpoint == "source" {
            ("target", "")
        } else {
            ("", "")
        };
        let updated = diesel::sql_query(
            "UPDATE console_graph_overlay_runs \
             SET cursor_endpoint = ?, cursor_key = ? \
             WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
               AND status = 'preparing' AND phase = ? \
               AND cursor_work_kind = ? AND cursor_node_id = ? AND cursor_mode = ? \
               AND cursor_endpoint = ? AND cursor_key = ?",
        )
        .bind::<Text, _>(next_endpoint)
        .bind::<Text, _>(next_key)
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(owner_id)
        .bind::<BigInt, _>(lease_epoch)
        .bind::<Text, _>(&state.phase)
        .bind::<Text, _>(&state.cursor_work_kind)
        .bind::<Text, _>(&state.cursor_node_id)
        .bind::<Text, _>(&state.cursor_mode)
        .bind::<Text, _>(&state.cursor_endpoint)
        .bind::<Text, _>(&state.cursor_key)
        .execute(connection)?;
        if updated != 1 {
            return Err(diesel::result::Error::NotFound);
        }
        return Ok(());
    }
    for edge_key in &edge_keys {
        diesel::sql_query(format!(
            "INSERT OR IGNORE INTO {tombstone_table} (run_id, mode, edge_key) \
             VALUES (?, ?, ?)"
        ))
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&state.cursor_mode)
        .bind::<Text, _>(&edge_key.value)
        .execute(connection)?;
    }
    let next_key = &edge_keys
        .last()
        .expect("non-empty edge tombstone page")
        .value;
    let updated = diesel::sql_query(
        "UPDATE console_graph_overlay_runs SET cursor_key = ? \
         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
           AND status = 'preparing' AND phase = ? \
           AND cursor_work_kind = ? AND cursor_node_id = ? AND cursor_mode = ? \
           AND cursor_endpoint = ? AND cursor_key = ?",
    )
    .bind::<Text, _>(next_key)
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(owner_id)
    .bind::<BigInt, _>(lease_epoch)
    .bind::<Text, _>(&state.phase)
    .bind::<Text, _>(&state.cursor_work_kind)
    .bind::<Text, _>(&state.cursor_node_id)
    .bind::<Text, _>(&state.cursor_mode)
    .bind::<Text, _>(&state.cursor_endpoint)
    .bind::<Text, _>(&state.cursor_key)
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(())
}

fn require_overlay_compaction_fence(
    connection: &mut SqliteConnection,
    run_id: i64,
    owner_id: &str,
    lease_epoch: i64,
) -> QueryResult<OverlayRunStateRow> {
    diesel::sql_query(
        "SELECT overlay.base_generation, overlay.baseline_source_revision, \
                overlay.baseline_publication_epoch, overlay.target_source_version, \
                overlay.target_source_revision, overlay.target_publication_epoch, \
                overlay.status, overlay.phase, overlay.cursor_node_id, \
                overlay.cursor_endpoint, overlay.cursor_mode, \
                overlay.cursor_work_kind, overlay.cursor_key \
         FROM console_graph_overlay_runs AS overlay \
         INNER JOIN console_graph_build_runs AS build ON build.run_id = overlay.run_id \
         INNER JOIN console_graph_generation_state AS state ON state.id = 1 \
         INNER JOIN console_graph_generation_source_revisions AS revision \
             ON revision.generation = overlay.base_generation \
         INNER JOIN console_graph_generation_delta_capabilities AS capability \
             ON capability.generation = overlay.base_generation \
         INNER JOIN console_graph_build_publications AS publication \
             ON publication.build_run_id = overlay.run_id \
         WHERE overlay.run_id = ? AND overlay.owner_id = ? \
           AND overlay.lease_epoch = ? AND overlay.status = 'compacting' \
           AND build.owner_id = overlay.owner_id \
           AND build.lease_epoch = overlay.lease_epoch \
           AND build.status = 'compacting' \
           AND state.active_generation = overlay.base_generation \
           AND state.active_overlay_run_id = overlay.run_id \
           AND revision.source_revision = overlay.baseline_source_revision \
           AND capability.publication_epoch = overlay.baseline_publication_epoch \
           AND publication.published_graph_generation = overlay.base_generation \
           AND publication.publication_epoch = overlay.target_publication_epoch \
           AND publication.source_version = overlay.target_source_version \
           AND publication.source_revision = overlay.target_source_revision",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(owner_id)
    .bind::<BigInt, _>(lease_epoch)
    .get_result(connection)
}

fn checkpoint_overlay_compaction_phase(
    connection: &mut SqliteConnection,
    run_id: i64,
    current_phase: &str,
    next_phase: &str,
) -> QueryResult<()> {
    let updated = diesel::sql_query(
        "UPDATE console_graph_overlay_runs \
         SET phase = ?, cursor_node_id = '', cursor_endpoint = '', \
             cursor_mode = '', cursor_work_kind = '', cursor_key = '', \
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE run_id = ? AND status = 'compacting' AND phase = ?",
    )
    .bind::<Text, _>(next_phase)
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(current_phase)
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(())
}

fn advance_overlay_compaction(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    match state.phase.as_str() {
        "branch_delete" => advance_overlay_branch_delete(connection, run_id, state),
        "scope_delete" => advance_overlay_scope_delete(connection, run_id, state),
        "route_delete" => advance_overlay_edge_delete(
            connection,
            run_id,
            state,
            "console_graph_build_edge_route_tombstones",
            "console_graph_edge_routes",
            "port_delete",
        ),
        "port_delete" => advance_overlay_edge_delete(
            connection,
            run_id,
            state,
            "console_graph_build_edge_port_tombstones",
            "console_graph_edge_ports",
            "node_delete",
        ),
        "node_delete" => advance_overlay_node_delete(connection, run_id, state),
        "node_upsert" => advance_overlay_node_upsert(connection, run_id, state),
        "route_upsert" => advance_overlay_edge_upsert(
            connection,
            run_id,
            state,
            "console_graph_edge_routes",
            "port_upsert",
        ),
        "port_upsert" => advance_overlay_edge_upsert(
            connection,
            run_id,
            state,
            "console_graph_edge_ports",
            "branch_upsert",
        ),
        "branch_upsert" => advance_overlay_branch_upsert(connection, run_id, state),
        "scope_upsert" => advance_overlay_scope_upsert(connection, run_id, state),
        "assignment_delete" => advance_overlay_assignment_delete(connection, run_id, state),
        "assignment_upsert" => advance_overlay_assignment_upsert(connection, run_id, state),
        "manifest" => advance_overlay_manifest(connection, run_id, state),
        "materialization" => advance_overlay_materialization(connection, run_id, state),
        "shell" => advance_overlay_shell(connection, run_id, state),
        "tick_delete" => advance_overlay_tick_delete(connection, run_id, state),
        "tick_upsert" => advance_overlay_tick_upsert(connection, run_id, state),
        "finalize" => finalize_overlay_compaction(connection, run_id, state),
        _ => Err(diesel::result::Error::NotFound),
    }
}

fn update_overlay_compaction_cursor(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
    mode: &str,
    node_id: &str,
    key: &str,
) -> QueryResult<()> {
    let updated = diesel::sql_query(
        "UPDATE console_graph_overlay_runs \
         SET cursor_mode = ?, cursor_node_id = ?, cursor_key = ?, \
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE run_id = ? AND status = 'compacting' AND phase = ? \
           AND cursor_work_kind = ? \
           AND cursor_mode = ? AND cursor_node_id = ? AND cursor_key = ?",
    )
    .bind::<Text, _>(mode)
    .bind::<Text, _>(node_id)
    .bind::<Text, _>(key)
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&state.phase)
    .bind::<Text, _>(&state.cursor_work_kind)
    .bind::<Text, _>(&state.cursor_mode)
    .bind::<Text, _>(&state.cursor_node_id)
    .bind::<Text, _>(&state.cursor_key)
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(())
}

fn advance_overlay_branch_delete(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    let branches = diesel::sql_query(
        "SELECT branch_name AS value FROM console_graph_build_changed_branches \
         WHERE run_id = ? AND branch_name > ? ORDER BY branch_name LIMIT 128",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&state.cursor_key)
    .load::<ScalarTextRow>(connection)?;
    if branches.is_empty() {
        return checkpoint_overlay_compaction_phase(
            connection,
            run_id,
            &state.phase,
            "scope_delete",
        );
    }
    for branch in &branches {
        diesel::sql_query(
            "DELETE FROM console_graph_materialization_branches \
             WHERE generation = ? AND name = ?",
        )
        .bind::<BigInt, _>(state.base_generation)
        .bind::<Text, _>(&branch.value)
        .execute(connection)?;
    }
    update_overlay_compaction_cursor(
        connection,
        run_id,
        state,
        "",
        "",
        &branches.last().expect("non-empty branch delete page").value,
    )
}

fn advance_overlay_scope_delete(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    let scopes = diesel::sql_query(
        "SELECT branch_name, node_id FROM console_graph_build_scope_tombstones \
         WHERE run_id = ? \
           AND (branch_name > ? OR (branch_name = ? AND node_id > ?)) \
         ORDER BY branch_name, node_id LIMIT 128",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&state.cursor_key)
    .bind::<Text, _>(&state.cursor_key)
    .bind::<Text, _>(&state.cursor_node_id)
    .load::<OverlayScopeRow>(connection)?;
    if scopes.is_empty() {
        return checkpoint_overlay_compaction_phase(
            connection,
            run_id,
            &state.phase,
            "route_delete",
        );
    }
    for scope in &scopes {
        diesel::sql_query(
            "DELETE FROM console_graph_anchor_scopes \
             WHERE generation = ? AND branch_name = ? AND node_id = ?",
        )
        .bind::<BigInt, _>(state.base_generation)
        .bind::<Text, _>(&scope.branch_name)
        .bind::<Text, _>(&scope.node_id)
        .execute(connection)?;
    }
    let last = scopes.last().expect("non-empty scope delete page");
    update_overlay_compaction_cursor(
        connection,
        run_id,
        state,
        "",
        &last.node_id,
        &last.branch_name,
    )
}

fn advance_overlay_edge_delete(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
    tombstone_table: &str,
    target_table: &str,
    next_phase: &str,
) -> QueryResult<()> {
    let edges = diesel::sql_query(format!(
        "SELECT mode, edge_key AS item_key FROM {tombstone_table} \
         WHERE run_id = ? AND (mode > ? OR (mode = ? AND edge_key > ?)) \
         ORDER BY mode, edge_key LIMIT 128"
    ))
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&state.cursor_mode)
    .bind::<Text, _>(&state.cursor_mode)
    .bind::<Text, _>(&state.cursor_key)
    .load::<OverlayModeKeyRow>(connection)?;
    if edges.is_empty() {
        return checkpoint_overlay_compaction_phase(connection, run_id, &state.phase, next_phase);
    }
    for edge in &edges {
        diesel::sql_query(format!(
            "DELETE FROM {target_table} \
             WHERE generation = ? AND mode = ? AND edge_key = ?"
        ))
        .bind::<BigInt, _>(state.base_generation)
        .bind::<Text, _>(&edge.mode)
        .bind::<Text, _>(&edge.item_key)
        .execute(connection)?;
    }
    let last = edges.last().expect("non-empty edge delete page");
    update_overlay_compaction_cursor(connection, run_id, state, &last.mode, "", &last.item_key)
}

fn advance_overlay_node_delete(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    if state.cursor_work_kind.is_empty() {
        let updated = diesel::sql_query(
            "UPDATE console_graph_overlay_runs \
             SET cursor_work_kind = 'global', cursor_mode = 'all' \
             WHERE run_id = ? AND status = 'compacting' AND phase = 'node_delete' \
               AND cursor_work_kind = '' AND cursor_mode = '' \
               AND cursor_node_id = '' AND cursor_key = ''",
        )
        .bind::<BigInt, _>(run_id)
        .execute(connection)?;
        if updated != 1 {
            return Err(diesel::result::Error::NotFound);
        }
        return Ok(());
    }
    let removed_table = match state.cursor_work_kind.as_str() {
        "global" => "console_graph_build_node_tombstones",
        "anchor_only" if state.cursor_mode == "anchors" => {
            "console_graph_build_anchor_node_tombstones"
        }
        _ => return Err(diesel::result::Error::NotFound),
    };
    let nodes = diesel::sql_query(format!(
        "SELECT node_id AS value FROM {removed_table} \
         WHERE run_id = ? AND node_id > ? ORDER BY node_id LIMIT 128"
    ))
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&state.cursor_node_id)
    .load::<ScalarTextRow>(connection)?;
    if nodes.is_empty() {
        let next_work = match (state.cursor_work_kind.as_str(), state.cursor_mode.as_str()) {
            ("global", "all") => Some(("global", "anchors")),
            ("global", "anchors") => Some(("anchor_only", "anchors")),
            ("anchor_only", "anchors") => None,
            _ => return Err(diesel::result::Error::NotFound),
        };
        if let Some((work_kind, mode)) = next_work {
            let updated = diesel::sql_query(
                "UPDATE console_graph_overlay_runs \
                 SET cursor_work_kind = ?, cursor_mode = ?, cursor_node_id = '' \
                 WHERE run_id = ? AND status = 'compacting' AND phase = 'node_delete' \
                   AND cursor_work_kind = ? AND cursor_mode = ? AND cursor_node_id = ?",
            )
            .bind::<Text, _>(work_kind)
            .bind::<Text, _>(mode)
            .bind::<BigInt, _>(run_id)
            .bind::<Text, _>(&state.cursor_work_kind)
            .bind::<Text, _>(&state.cursor_mode)
            .bind::<Text, _>(&state.cursor_node_id)
            .execute(connection)?;
            if updated != 1 {
                return Err(diesel::result::Error::NotFound);
            }
            return Ok(());
        }
        return checkpoint_overlay_compaction_phase(
            connection,
            run_id,
            &state.phase,
            "node_upsert",
        );
    }
    for node in &nodes {
        diesel::sql_query(
            "DELETE FROM console_graph_node_locations \
             WHERE generation = ? AND mode = ? AND node_id = ?",
        )
        .bind::<BigInt, _>(state.base_generation)
        .bind::<Text, _>(&state.cursor_mode)
        .bind::<Text, _>(&node.value)
        .execute(connection)?;
    }
    let last = &nodes.last().expect("non-empty node delete page").value;
    let updated = diesel::sql_query(
        "UPDATE console_graph_overlay_runs SET cursor_node_id = ? \
         WHERE run_id = ? AND status = 'compacting' AND phase = 'node_delete' \
           AND cursor_work_kind = ? AND cursor_mode = ? AND cursor_node_id = ?",
    )
    .bind::<Text, _>(last)
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&state.cursor_work_kind)
    .bind::<Text, _>(&state.cursor_mode)
    .bind::<Text, _>(&state.cursor_node_id)
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(())
}

fn advance_overlay_node_upsert(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    let nodes = diesel::sql_query(
        "SELECT mode, node_id FROM console_graph_node_locations \
         WHERE generation = ? AND (mode > ? OR (mode = ? AND node_id > ?)) \
         ORDER BY mode, node_id LIMIT 128",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&state.cursor_mode)
    .bind::<Text, _>(&state.cursor_mode)
    .bind::<Text, _>(&state.cursor_node_id)
    .load::<OverlayAffectedNodeRow>(connection)?;
    if nodes.is_empty() {
        return checkpoint_overlay_compaction_phase(
            connection,
            run_id,
            &state.phase,
            "route_upsert",
        );
    }
    for node in &nodes {
        diesel::sql_query(
            "INSERT INTO console_graph_node_locations ( \
                 generation, mode, node_id, node_key, node_target, short_id, node_kind, \
                 summary, labels_json, rank, sort_order, x, y, created_at, created_at_ns, \
                 min_x, min_y, max_x, max_y \
             ) \
             SELECT ?, mode, node_id, node_key, node_target, short_id, node_kind, \
                    summary, labels_json, rank, sort_order, x, y, created_at, created_at_ns, \
                    min_x, min_y, max_x, max_y \
             FROM console_graph_node_locations \
             WHERE generation = ? AND mode = ? AND node_id = ? \
             ON CONFLICT(generation, mode, node_id) DO UPDATE SET \
                 node_key = excluded.node_key, node_target = excluded.node_target, \
                 short_id = excluded.short_id, node_kind = excluded.node_kind, \
                 summary = excluded.summary, labels_json = excluded.labels_json, \
                 rank = excluded.rank, sort_order = excluded.sort_order, \
                 x = excluded.x, y = excluded.y, created_at = excluded.created_at, \
                 created_at_ns = excluded.created_at_ns, min_x = excluded.min_x, \
                 min_y = excluded.min_y, max_x = excluded.max_x, max_y = excluded.max_y",
        )
        .bind::<BigInt, _>(state.base_generation)
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&node.mode)
        .bind::<Text, _>(&node.node_id)
        .execute(connection)?;
    }
    let last = nodes.last().expect("non-empty node upsert page");
    update_overlay_compaction_cursor(connection, run_id, state, &last.mode, &last.node_id, "")
}

fn advance_overlay_edge_upsert(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
    table: &str,
    next_phase: &str,
) -> QueryResult<()> {
    let edges = diesel::sql_query(format!(
        "SELECT mode, edge_key AS item_key FROM {table} \
         WHERE generation = ? AND (mode > ? OR (mode = ? AND edge_key > ?)) \
         ORDER BY mode, edge_key LIMIT 128"
    ))
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&state.cursor_mode)
    .bind::<Text, _>(&state.cursor_mode)
    .bind::<Text, _>(&state.cursor_key)
    .load::<OverlayModeKeyRow>(connection)?;
    if edges.is_empty() {
        return checkpoint_overlay_compaction_phase(connection, run_id, &state.phase, next_phase);
    }
    for edge in &edges {
        if table == "console_graph_edge_routes" {
            diesel::sql_query(
                "INSERT INTO console_graph_edge_routes ( \
                     generation, mode, edge_key, edge_kind, source_id, target_id, \
                     source_x, source_y, control_1_x, control_1_y, control_2_x, control_2_y, \
                     target_x, target_y, min_x, min_y, max_x, max_y \
                 ) \
                 SELECT ?, mode, edge_key, edge_kind, source_id, target_id, \
                        source_x, source_y, control_1_x, control_1_y, \
                        control_2_x, control_2_y, target_x, target_y, \
                        min_x, min_y, max_x, max_y \
                 FROM console_graph_edge_routes \
                 WHERE generation = ? AND mode = ? AND edge_key = ? \
                 ON CONFLICT(generation, mode, edge_key) DO UPDATE SET \
                     edge_kind = excluded.edge_kind, source_id = excluded.source_id, \
                     target_id = excluded.target_id, source_x = excluded.source_x, \
                     source_y = excluded.source_y, control_1_x = excluded.control_1_x, \
                     control_1_y = excluded.control_1_y, control_2_x = excluded.control_2_x, \
                     control_2_y = excluded.control_2_y, target_x = excluded.target_x, \
                     target_y = excluded.target_y, min_x = excluded.min_x, \
                     min_y = excluded.min_y, max_x = excluded.max_x, max_y = excluded.max_y",
            )
            .bind::<BigInt, _>(state.base_generation)
            .bind::<BigInt, _>(run_id)
            .bind::<Text, _>(&edge.mode)
            .bind::<Text, _>(&edge.item_key)
            .execute(connection)?;
        } else if table == "console_graph_edge_ports" {
            diesel::sql_query(
                "INSERT INTO console_graph_edge_ports ( \
                     generation, mode, edge_key, source_id, target_id, \
                     source_slot, target_slot \
                 ) \
                 SELECT ?, mode, edge_key, source_id, target_id, source_slot, target_slot \
                 FROM console_graph_edge_ports \
                 WHERE generation = ? AND mode = ? AND edge_key = ? \
                 ON CONFLICT(generation, mode, edge_key) DO UPDATE SET \
                     source_id = excluded.source_id, target_id = excluded.target_id, \
                     source_slot = excluded.source_slot, target_slot = excluded.target_slot",
            )
            .bind::<BigInt, _>(state.base_generation)
            .bind::<BigInt, _>(run_id)
            .bind::<Text, _>(&edge.mode)
            .bind::<Text, _>(&edge.item_key)
            .execute(connection)?;
        } else {
            return Err(diesel::result::Error::NotFound);
        }
    }
    let last = edges.last().expect("non-empty edge upsert page");
    update_overlay_compaction_cursor(connection, run_id, state, &last.mode, "", &last.item_key)
}

fn advance_overlay_branch_upsert(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    let branches = diesel::sql_query(
        "SELECT name AS value FROM console_graph_materialization_branches \
         WHERE generation = ? AND name > ? ORDER BY name LIMIT 128",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&state.cursor_key)
    .load::<ScalarTextRow>(connection)?;
    if branches.is_empty() {
        return checkpoint_overlay_compaction_phase(
            connection,
            run_id,
            &state.phase,
            "scope_upsert",
        );
    }
    for branch in &branches {
        diesel::sql_query(
            "INSERT INTO console_graph_materialization_branches ( \
                 generation, name, head_id, state_json, contribution_generation \
             ) \
             SELECT ?, name, head_id, state_json, contribution_generation \
             FROM console_graph_materialization_branches \
             WHERE generation = ? AND name = ? \
             ON CONFLICT(generation, name) DO UPDATE SET \
                 head_id = excluded.head_id, state_json = excluded.state_json, \
                 contribution_generation = excluded.contribution_generation",
        )
        .bind::<BigInt, _>(state.base_generation)
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&branch.value)
        .execute(connection)?;
    }
    update_overlay_compaction_cursor(
        connection,
        run_id,
        state,
        "",
        "",
        &branches.last().expect("non-empty branch upsert page").value,
    )
}

fn advance_overlay_scope_upsert(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    let scopes = diesel::sql_query(
        "SELECT branch_name, node_id FROM console_graph_anchor_scopes \
         WHERE generation = ? \
           AND (branch_name > ? OR (branch_name = ? AND node_id > ?)) \
         ORDER BY branch_name, node_id LIMIT 128",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&state.cursor_key)
    .bind::<Text, _>(&state.cursor_key)
    .bind::<Text, _>(&state.cursor_node_id)
    .load::<OverlayScopeRow>(connection)?;
    if scopes.is_empty() {
        return checkpoint_overlay_compaction_phase(
            connection,
            run_id,
            &state.phase,
            "assignment_delete",
        );
    }
    for scope in &scopes {
        diesel::sql_query(
            "INSERT OR IGNORE INTO console_graph_anchor_scopes ( \
                 generation, branch_name, node_id \
             ) VALUES (?, ?, ?)",
        )
        .bind::<BigInt, _>(state.base_generation)
        .bind::<Text, _>(&scope.branch_name)
        .bind::<Text, _>(&scope.node_id)
        .execute(connection)?;
    }
    let last = scopes.last().expect("non-empty scope upsert page");
    update_overlay_compaction_cursor(
        connection,
        run_id,
        state,
        "",
        &last.node_id,
        &last.branch_name,
    )
}

fn advance_overlay_assignment_delete(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    let branches = diesel::sql_query(
        "SELECT branch_name AS value FROM console_graph_build_changed_branches \
         WHERE run_id = ? AND branch_name > ? ORDER BY branch_name LIMIT 128",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&state.cursor_key)
    .load::<ScalarTextRow>(connection)?;
    if branches.is_empty() {
        return checkpoint_overlay_compaction_phase(
            connection,
            run_id,
            &state.phase,
            "assignment_upsert",
        );
    }
    for branch in &branches {
        diesel::sql_query(
            "DELETE FROM console_graph_branch_label_assignments \
             WHERE generation = ? AND branch_name = ?",
        )
        .bind::<BigInt, _>(state.base_generation)
        .bind::<Text, _>(&branch.value)
        .execute(connection)?;
    }
    update_overlay_compaction_cursor(
        connection,
        run_id,
        state,
        "",
        "",
        &branches
            .last()
            .expect("non-empty assignment delete page")
            .value,
    )
}

fn advance_overlay_assignment_upsert(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    let assignments = diesel::sql_query(
        "SELECT branch_name, mode \
         FROM console_graph_branch_label_assignments \
         WHERE generation = ? \
           AND (branch_name > ? OR (branch_name = ? AND mode > ?)) \
         ORDER BY branch_name, mode LIMIT 128",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&state.cursor_key)
    .bind::<Text, _>(&state.cursor_key)
    .bind::<Text, _>(&state.cursor_mode)
    .load::<OverlayBranchModeRow>(connection)?;
    if assignments.is_empty() {
        return checkpoint_overlay_compaction_phase(connection, run_id, &state.phase, "manifest");
    }
    for assignment in &assignments {
        diesel::sql_query(
            "INSERT INTO console_graph_branch_label_assignments ( \
                 generation, branch_name, mode, node_id, label \
             ) \
             SELECT ?, branch_name, mode, node_id, label \
             FROM console_graph_branch_label_assignments \
             WHERE generation = ? AND branch_name = ? AND mode = ? \
             ON CONFLICT(generation, branch_name, mode) DO UPDATE SET \
                 node_id = excluded.node_id, label = excluded.label",
        )
        .bind::<BigInt, _>(state.base_generation)
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&assignment.branch_name)
        .bind::<Text, _>(&assignment.mode)
        .execute(connection)?;
    }
    let last = assignments
        .last()
        .expect("non-empty assignment upsert page");
    update_overlay_compaction_cursor(connection, run_id, state, &last.mode, "", &last.branch_name)
}

fn advance_overlay_manifest(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    let inserted = diesel::sql_query(
        "INSERT INTO console_graph_anchor_scope_manifests (generation, scope_count) \
         SELECT ?, scope_count FROM console_graph_anchor_scope_manifests \
         WHERE generation = ? \
         ON CONFLICT(generation) DO UPDATE SET scope_count = excluded.scope_count",
    )
    .bind::<BigInt, _>(state.base_generation)
    .bind::<BigInt, _>(run_id)
    .execute(connection)?;
    if inserted != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    checkpoint_overlay_compaction_phase(connection, run_id, &state.phase, "materialization")
}

fn advance_overlay_materialization(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    let modes = diesel::sql_query(
        "SELECT mode AS value FROM console_graph_materializations \
         WHERE generation = ? AND mode > ? ORDER BY mode LIMIT 128",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&state.cursor_mode)
    .load::<ScalarTextRow>(connection)?;
    if modes.is_empty() {
        return checkpoint_overlay_compaction_phase(connection, run_id, &state.phase, "shell");
    }
    for mode in &modes {
        diesel::sql_query(
            "INSERT INTO console_graph_materializations ( \
                 generation, mode, source_version, coordinate_space, \
                 world_min_x, world_min_y, world_max_x, world_max_y, updated_at \
             ) \
             SELECT ?, mode, source_version, coordinate_space, world_min_x, world_min_y, \
                    world_max_x, world_max_y, updated_at \
             FROM console_graph_materializations \
             WHERE generation = ? AND mode = ? \
             ON CONFLICT(generation, mode) DO UPDATE SET \
                 source_version = excluded.source_version, \
                 coordinate_space = excluded.coordinate_space, \
                 world_min_x = excluded.world_min_x, world_min_y = excluded.world_min_y, \
                 world_max_x = excluded.world_max_x, world_max_y = excluded.world_max_y, \
                 updated_at = excluded.updated_at",
        )
        .bind::<BigInt, _>(state.base_generation)
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&mode.value)
        .execute(connection)?;
    }
    update_overlay_compaction_cursor(
        connection,
        run_id,
        state,
        &modes.last().expect("non-empty materialization page").value,
        "",
        "",
    )
}

fn advance_overlay_shell(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    let modes = diesel::sql_query(
        "SELECT mode AS value FROM console_graph_materialization_shells \
         WHERE generation = ? AND mode > ? ORDER BY mode LIMIT 128",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&state.cursor_mode)
    .load::<ScalarTextRow>(connection)?;
    if modes.is_empty() {
        return checkpoint_overlay_compaction_phase(
            connection,
            run_id,
            &state.phase,
            "tick_delete",
        );
    }
    for mode in &modes {
        diesel::sql_query(
            "INSERT INTO console_graph_materialization_shells ( \
                 generation, mode, node_count, edge_count \
             ) \
             SELECT ?, mode, node_count, edge_count \
             FROM console_graph_materialization_shells \
             WHERE generation = ? AND mode = ? \
             ON CONFLICT(generation, mode) DO UPDATE SET \
                 node_count = excluded.node_count, edge_count = excluded.edge_count",
        )
        .bind::<BigInt, _>(state.base_generation)
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&mode.value)
        .execute(connection)?;
    }
    update_overlay_compaction_cursor(
        connection,
        run_id,
        state,
        &modes.last().expect("non-empty shell page").value,
        "",
        "",
    )
}

fn advance_overlay_tick_delete(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    let deleted = diesel::sql_query(
        "DELETE FROM console_graph_materialization_time_ticks \
         WHERE rowid IN ( \
             SELECT rowid FROM console_graph_materialization_time_ticks \
             WHERE generation = ? ORDER BY mode, sample_index LIMIT 128 \
         )",
    )
    .bind::<BigInt, _>(state.base_generation)
    .execute(connection)?;
    if deleted == 0 {
        checkpoint_overlay_compaction_phase(connection, run_id, &state.phase, "tick_upsert")
    } else {
        Ok(())
    }
}

fn advance_overlay_tick_upsert(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    let ticks = diesel::sql_query(
        "SELECT mode, sample_index \
         FROM console_graph_materialization_time_ticks \
         WHERE generation = ? AND ( \
             mode > ? OR (mode = ? AND (? = '' OR sample_index > CAST(? AS INTEGER))) \
         ) \
         ORDER BY mode, sample_index LIMIT 128",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&state.cursor_mode)
    .bind::<Text, _>(&state.cursor_mode)
    .bind::<Text, _>(&state.cursor_key)
    .bind::<Text, _>(&state.cursor_key)
    .load::<OverlayModeIndexRow>(connection)?;
    if ticks.is_empty() {
        return checkpoint_overlay_compaction_phase(connection, run_id, &state.phase, "finalize");
    }
    for tick in &ticks {
        diesel::sql_query(
            "INSERT INTO console_graph_materialization_time_ticks ( \
                 generation, mode, sample_index, node_id, node_target, x, y, \
                 created_at, created_at_ns \
             ) \
             SELECT ?, mode, sample_index, node_id, node_target, x, y, \
                    created_at, created_at_ns \
             FROM console_graph_materialization_time_ticks \
             WHERE generation = ? AND mode = ? AND sample_index = ? \
             ON CONFLICT(generation, mode, sample_index) DO UPDATE SET \
                 node_id = excluded.node_id, node_target = excluded.node_target, \
                 x = excluded.x, y = excluded.y, created_at = excluded.created_at, \
                 created_at_ns = excluded.created_at_ns",
        )
        .bind::<BigInt, _>(state.base_generation)
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&tick.mode)
        .bind::<Integer, _>(tick.sample_index)
        .execute(connection)?;
    }
    let last = ticks.last().expect("non-empty tick upsert page");
    update_overlay_compaction_cursor(
        connection,
        run_id,
        state,
        &last.mode,
        "",
        &last.sample_index.to_string(),
    )
}

fn finalize_overlay_compaction(
    connection: &mut SqliteConnection,
    run_id: i64,
    state: &OverlayRunStateRow,
) -> QueryResult<()> {
    diesel::sql_query(
        "INSERT INTO console_graph_generation_source_revisions (generation, source_revision) \
         VALUES (?, ?) \
         ON CONFLICT(generation) DO UPDATE SET source_revision = excluded.source_revision",
    )
    .bind::<BigInt, _>(state.base_generation)
    .bind::<BigInt, _>(state.target_source_revision)
    .execute(connection)?;
    diesel::sql_query(
        "INSERT INTO console_graph_generation_delta_capabilities ( \
             generation, delta_compatible, publication_epoch \
         ) VALUES (?, 1, ?) \
         ON CONFLICT(generation) DO UPDATE SET \
             delta_compatible = 1, publication_epoch = excluded.publication_epoch",
    )
    .bind::<BigInt, _>(state.base_generation)
    .bind::<BigInt, _>(state.target_publication_epoch)
    .execute(connection)?;
    let state_updated = diesel::sql_query(
        "UPDATE console_graph_generation_state SET active_overlay_run_id = NULL \
         WHERE id = 1 AND active_generation = ? AND active_overlay_run_id = ?",
    )
    .bind::<BigInt, _>(state.base_generation)
    .bind::<BigInt, _>(run_id)
    .execute(connection)?;
    let overlay_updated = diesel::sql_query(
        "UPDATE console_graph_overlay_runs \
         SET status = 'completed', phase = 'completed', lease_expires_at_ms = 0, \
             completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), \
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE run_id = ? AND status = 'compacting' AND phase = 'finalize'",
    )
    .bind::<BigInt, _>(run_id)
    .execute(connection)?;
    let build_updated = diesel::sql_query(
        "UPDATE console_graph_build_runs \
         SET status = 'completed', lease_expires_at_ms = 0, \
             dag_published_graph_generation = ?, \
             completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE run_id = ? AND status = 'compacting'",
    )
    .bind::<BigInt, _>(state.base_generation)
    .bind::<BigInt, _>(run_id)
    .execute(connection)?;
    if state_updated != 1 || overlay_updated != 1 || build_updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(())
}

#[cfg(test)]
fn query_materialization_row(
    connection: &mut SqliteConnection,
    mode: GraphMode,
) -> QueryResult<Option<MaterializationRow>> {
    let generation = query_active_snapshot_descriptor(connection)?.effective_generation();
    query_materialization_row_for_generation(connection, generation, mode)
}

fn query_materialization_row_for_generation(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: GraphMode,
) -> QueryResult<Option<MaterializationRow>> {
    console_graph_materializations::table
        .filter(console_graph_materializations::generation.eq(generation))
        .filter(console_graph_materializations::mode.eq(mode.as_query_value()))
        .filter(console_graph_materializations::coordinate_space.eq(COORDINATE_SPACE))
        .select(MaterializationRow::as_select())
        .first(connection)
        .optional()
}

#[cfg(test)]
fn query_nodes_for_generation(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: GraphMode,
) -> QueryResult<Vec<StoredNodeRow>> {
    let mut nodes = console_graph_node_locations::table
        .filter(console_graph_node_locations::generation.eq(generation))
        .filter(console_graph_node_locations::mode.eq(mode.as_query_value()))
        .order(console_graph_node_locations::node_id)
        .select(StoredNodeRow::as_select())
        .load(connection)?;
    hydrate_effective_node_labels(
        connection,
        ActiveSnapshotDescriptor {
            base_generation: generation,
            overlay_run_id: None,
        },
        mode,
        &mut nodes,
    )?;
    Ok(nodes)
}

fn hydrate_effective_node_labels(
    connection: &mut SqliteConnection,
    descriptor: ActiveSnapshotDescriptor,
    mode: GraphMode,
    nodes: &mut [StoredNodeRow],
) -> QueryResult<()> {
    let mode = mode.as_query_value();
    for batch in nodes.chunks_mut(128) {
        let requested_json = serde_json::to_string(
            &batch
                .iter()
                .map(|node| node.node_id.as_str())
                .collect::<Vec<_>>(),
        )
        .map_err(|error| diesel::result::Error::SerializationError(Box::new(error)))?;
        let labels = diesel::sql_query(
            "WITH requested(node_id) AS (SELECT value FROM json_each(?)) \
             SELECT effective.node_id, effective.label, effective.branch_name \
             FROM ( \
                 SELECT overlay.node_id, overlay.label, overlay.branch_name \
                 FROM console_graph_branch_label_assignments AS overlay \
                 INNER JOIN requested ON requested.node_id = overlay.node_id \
                 WHERE overlay.generation = ? AND overlay.mode = ? \
                 UNION ALL \
                 SELECT base.node_id, base.label, base.branch_name \
                 FROM console_graph_branch_label_assignments AS base \
                 INNER JOIN requested ON requested.node_id = base.node_id \
                 WHERE base.generation = ? AND base.mode = ? \
                   AND NOT EXISTS ( \
                       SELECT 1 FROM console_graph_build_changed_branches AS changed \
                       WHERE changed.run_id = ? \
                         AND changed.branch_name = base.branch_name \
                   ) \
             ) AS effective \
             ORDER BY effective.node_id, effective.label, effective.branch_name",
        )
        .bind::<Text, _>(requested_json)
        .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(descriptor.base_generation)
        .bind::<Text, _>(mode)
        .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
        .load::<EffectiveNodeLabelRow>(connection)?;
        let mut by_node = BTreeMap::<String, Vec<String>>::new();
        for row in labels {
            let node_labels = by_node.entry(row.node_id).or_default();
            if node_labels.last() != Some(&row.label) {
                node_labels.push(row.label);
            }
            let _ = row.branch_name;
        }
        for node in batch {
            node.labels_json =
                serde_json::to_string(by_node.get(&node.node_id).map(Vec::as_slice).unwrap_or(&[]))
                    .map_err(|error| diesel::result::Error::SerializationError(Box::new(error)))?;
        }
    }
    Ok(())
}

#[cfg(test)]
fn query_edges_for_generation(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: GraphMode,
) -> QueryResult<Vec<StoredEdgeRow>> {
    console_graph_edge_routes::table
        .filter(console_graph_edge_routes::generation.eq(generation))
        .filter(console_graph_edge_routes::mode.eq(mode.as_query_value()))
        .order(console_graph_edge_routes::edge_key)
        .select(StoredEdgeRow::as_select())
        .load(connection)
}

fn query_viewport(
    connection: &mut SqliteConnection,
    mode: GraphMode,
    request: GraphViewportRequest,
) -> QueryResult<Option<StoredViewport>> {
    let descriptor = query_active_snapshot_descriptor(connection)?;
    let generation = descriptor.effective_generation();
    let Some(materialization) =
        query_materialization_row_for_generation(connection, generation, mode)?
    else {
        return Ok(None);
    };
    let request = request.normalized();
    let left = request.x.saturating_sub(request.overscan);
    let top = request.y.saturating_sub(request.overscan);
    let right = request
        .x
        .saturating_add(request.width)
        .saturating_add(request.overscan);
    let bottom = request
        .y
        .saturating_add(request.height)
        .saturating_add(request.overscan);
    let mode_value = mode.as_query_value();
    let mut nodes = diesel::sql_query(
        "SELECT node_id, node_key, node_target, short_id, node_kind, summary, \
                labels_json, rank, sort_order, x, y, created_at, created_at_ns, \
                min_x, min_y, max_x, max_y \
         FROM console_graph_node_locations AS overlay \
         WHERE overlay.generation = ? AND overlay.mode = ? \
           AND overlay.min_x <= ? AND overlay.max_x >= ? \
           AND overlay.min_y <= ? AND overlay.max_y >= ? \
           AND NOT EXISTS ( \
               SELECT 1 FROM console_graph_build_node_tombstones AS removed \
               WHERE removed.run_id = ? AND removed.node_id = overlay.node_id \
           ) \
           AND (? <> 'anchors' OR NOT EXISTS ( \
               SELECT 1 FROM console_graph_build_anchor_node_tombstones AS removed \
               WHERE removed.run_id = ? AND removed.node_id = overlay.node_id \
           )) \
         UNION ALL \
         SELECT base.node_id, base.node_key, base.node_target, base.short_id, \
                base.node_kind, base.summary, base.labels_json, base.rank, \
                base.sort_order, base.x, base.y, base.created_at, base.created_at_ns, \
                base.min_x, base.min_y, base.max_x, base.max_y \
         FROM console_graph_node_locations AS base \
         WHERE base.generation = ? AND base.mode = ? \
           AND base.min_x <= ? AND base.max_x >= ? \
           AND base.min_y <= ? AND base.max_y >= ? \
           AND NOT EXISTS ( \
               SELECT 1 FROM console_graph_node_locations AS overlay \
               WHERE overlay.generation = ? AND overlay.mode = base.mode \
                 AND overlay.node_id = base.node_id \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM console_graph_build_node_tombstones AS removed \
               WHERE removed.run_id = ? AND removed.node_id = base.node_id \
           ) \
           AND (? <> 'anchors' OR NOT EXISTS ( \
               SELECT 1 \
               FROM console_graph_build_anchor_node_tombstones AS removed \
               WHERE removed.run_id = ? AND removed.node_id = base.node_id \
           )) \
         ORDER BY rank, sort_order, node_id",
    )
    .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
    .bind::<Text, _>(mode_value)
    .bind::<Integer, _>(right)
    .bind::<Integer, _>(left)
    .bind::<Integer, _>(bottom)
    .bind::<Integer, _>(top)
    .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
    .bind::<Text, _>(mode_value)
    .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
    .bind::<BigInt, _>(descriptor.base_generation)
    .bind::<Text, _>(mode_value)
    .bind::<Integer, _>(right)
    .bind::<Integer, _>(left)
    .bind::<Integer, _>(bottom)
    .bind::<Integer, _>(top)
    .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
    .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
    .bind::<Text, _>(mode_value)
    .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
    .load::<StoredNodeRow>(connection)?;
    hydrate_effective_node_labels(connection, descriptor, mode, &mut nodes)?;
    let edges = diesel::sql_query(
        "SELECT edge_key, edge_kind, source_id, target_id, source_x, source_y, \
                control_1_x, control_1_y, control_2_x, control_2_y, target_x, \
                target_y, min_x, min_y, max_x, max_y \
         FROM console_graph_edge_routes AS overlay \
         WHERE overlay.generation = ? AND overlay.mode = ? \
           AND overlay.min_x <= ? AND overlay.max_x >= ? \
           AND overlay.min_y <= ? AND overlay.max_y >= ? \
           AND NOT EXISTS ( \
               SELECT 1 FROM console_graph_build_edge_route_tombstones AS removed \
               WHERE removed.run_id = ? AND removed.mode = overlay.mode \
                 AND removed.edge_key = overlay.edge_key \
           ) \
         UNION ALL \
         SELECT base.edge_key, base.edge_kind, base.source_id, base.target_id, \
                base.source_x, base.source_y, base.control_1_x, base.control_1_y, \
                base.control_2_x, base.control_2_y, base.target_x, base.target_y, \
                base.min_x, base.min_y, base.max_x, base.max_y \
         FROM console_graph_edge_routes AS base \
         WHERE base.generation = ? AND base.mode = ? \
           AND base.min_x <= ? AND base.max_x >= ? \
           AND base.min_y <= ? AND base.max_y >= ? \
           AND NOT EXISTS ( \
               SELECT 1 FROM console_graph_edge_routes AS overlay \
               WHERE overlay.generation = ? AND overlay.mode = base.mode \
                 AND overlay.edge_key = base.edge_key \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 \
               FROM console_graph_build_edge_route_tombstones AS removed \
               WHERE removed.run_id = ? AND removed.mode = base.mode \
                 AND removed.edge_key = base.edge_key \
           ) \
         ORDER BY edge_key",
    )
    .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
    .bind::<Text, _>(mode_value)
    .bind::<Integer, _>(right)
    .bind::<Integer, _>(left)
    .bind::<Integer, _>(bottom)
    .bind::<Integer, _>(top)
    .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
    .bind::<BigInt, _>(descriptor.base_generation)
    .bind::<Text, _>(mode_value)
    .bind::<Integer, _>(right)
    .bind::<Integer, _>(left)
    .bind::<Integer, _>(bottom)
    .bind::<Integer, _>(top)
    .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
    .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
    .load::<StoredEdgeRow>(connection)?;
    Ok(Some(StoredViewport {
        materialization,
        request,
        nodes,
        edges,
    }))
}

fn query_shell_facts(
    connection: &mut SqliteConnection,
    mode: GraphMode,
) -> QueryResult<Option<StoredShellFacts>> {
    let descriptor = query_active_snapshot_descriptor(connection)?;
    let generation = descriptor.effective_generation();
    let Some(materialization) =
        query_materialization_row_for_generation(connection, generation, mode)?
    else {
        return Ok(None);
    };
    let (node_count, edge_count) = console_graph_materialization_shells::table
        .filter(console_graph_materialization_shells::generation.eq(generation))
        .filter(console_graph_materialization_shells::mode.eq(mode.as_query_value()))
        .select((
            console_graph_materialization_shells::node_count,
            console_graph_materialization_shells::edge_count,
        ))
        .first(connection)?;
    let nodes = console_graph_materialization_time_ticks::table
        .filter(console_graph_materialization_time_ticks::generation.eq(generation))
        .filter(console_graph_materialization_time_ticks::mode.eq(mode.as_query_value()))
        .order(console_graph_materialization_time_ticks::sample_index)
        .select(StoredShellTickRow::as_select())
        .load(connection)?;
    let branches = diesel::sql_query(
        "SELECT name, head_id, state_json FROM ( \
             SELECT name, head_id, state_json \
             FROM console_graph_materialization_branches AS overlay \
             WHERE overlay.generation = ? \
             UNION ALL \
             SELECT base.name, base.head_id, base.state_json \
             FROM console_graph_materialization_branches AS base \
             WHERE base.generation = ? AND NOT EXISTS ( \
                 SELECT 1 FROM console_graph_build_changed_branches AS changed \
                 WHERE changed.run_id = ? AND changed.branch_name = base.name \
             ) \
         ) AS effective \
         ORDER BY CASE WHEN name = 'main' THEN 0 ELSE 1 END, name",
    )
    .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
    .bind::<BigInt, _>(descriptor.base_generation)
    .bind::<Nullable<BigInt>, _>(descriptor.overlay_run_id)
    .load::<StoredShellBranchRow>(connection)?;
    Ok(Some(StoredShellFacts {
        materialization,
        node_count,
        nodes,
        edge_count,
        branches,
    }))
}

fn stored_viewport_response(stored: StoredViewport) -> crate::Result<GraphViewportResponse> {
    let nodes = stored
        .nodes
        .into_iter()
        .map(stored_viewport_node)
        .collect::<crate::Result<Vec<_>>>()?;
    let edges = stored
        .edges
        .into_iter()
        .map(stored_viewport_edge)
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(GraphViewportResponse {
        version: stored.materialization.source_version.max(0) as u64,
        canvas: GraphCanvas {
            width: stored.materialization.world_max_x,
            height: stored.materialization.world_max_y,
        },
        viewport: GraphViewport {
            x: stored.request.x,
            y: stored.request.y,
            width: stored.request.width,
            height: stored.request.height,
            overscan: stored.request.overscan,
        },
        nodes,
        edges,
    })
}

#[cfg(test)]
fn plan_materialization(
    snapshot: &GraphSnapshot,
    previous: &StoredGraph,
    policy: GraphLayoutPolicy,
) -> crate::Result<PlannedMaterialization> {
    let hints = previous
        .nodes
        .iter()
        .map(|(node_id, row)| {
            (
                node_id.clone(),
                LayoutHint {
                    rank: row.rank.max(0) as usize,
                    order: row.sort_order.max(0) as usize,
                    point: Point { x: row.x, y: row.y },
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let cold_start = previous.materialization.is_none();
    let small_graph = snapshot.nodes.len() <= policy.full_layout_node_limit
        && snapshot.edges.len() <= policy.full_layout_edge_limit;
    if cold_start || small_graph {
        let layout = try_layout_graph_with_hints(snapshot, &hints, None)
            .context(crate::error::GraphLayoutSnafu)?;
        return Ok(PlannedMaterialization {
            layout,
            strategy: MaterializationStrategy::Full,
            affected_nodes: snapshot.nodes.len(),
            affected_ranks: 0,
            fallback_reason: cold_start.then_some("cold_start"),
        });
    }

    let previous_nodes = previous.nodes.keys().cloned().collect::<BTreeSet<_>>();
    let current_nodes = snapshot
        .nodes
        .iter()
        .map(|node| node.id.clone())
        .collect::<BTreeSet<_>>();
    let previous_edges = previous_edge_identities(previous)?;
    let current_edges = snapshot_edge_identities(snapshot);
    if previous_nodes == current_nodes && previous_edges == current_edges {
        let reflow_ranks = BTreeSet::new();
        let layout = try_layout_graph_with_hints(snapshot, &hints, Some(&reflow_ranks))
            .context(crate::error::GraphLayoutSnafu)?;
        return Ok(PlannedMaterialization {
            layout,
            strategy: MaterializationStrategy::PayloadOnly,
            affected_nodes: 0,
            affected_ranks: 0,
            fallback_reason: None,
        });
    }

    let affected = affected_descendant_closure(
        &previous_nodes,
        &current_nodes,
        &previous_edges,
        &current_edges,
    );
    let fresh_rank_by_node = try_graph_ranks(snapshot).context(crate::error::GraphLayoutSnafu)?;
    let mut affected_ranks = BTreeSet::new();
    for node_id in &affected {
        if let Some(rank) = fresh_rank_by_node.get(node_id) {
            affected_ranks.insert(*rank);
        }
        if let Some(hint) = hints.get(node_id) {
            affected_ranks.insert(hint.rank);
        }
    }
    for (node_id, rank) in &fresh_rank_by_node {
        if hints.get(node_id).is_some_and(|hint| hint.rank != *rank) {
            affected_ranks.insert(*rank);
            affected_ranks.insert(hints[node_id].rank);
        }
    }
    affected_ranks = expanded_ranks(&affected_ranks);
    let affected_current_nodes = fresh_rank_by_node
        .values()
        .filter(|rank| affected_ranks.contains(rank))
        .count();
    let removed_nodes = previous_nodes.difference(&current_nodes).count();
    let affected_node_count = affected_current_nodes + removed_nodes;
    let percentage_limit = snapshot
        .nodes
        .len()
        .saturating_mul(policy.local_layout_percent)
        / 100;
    let local_limit = policy.local_layout_node_limit.min(percentage_limit.max(1));
    if affected_node_count <= local_limit {
        let layout = try_layout_graph_with_hints(snapshot, &hints, Some(&affected_ranks))
            .context(crate::error::GraphLayoutSnafu)?;
        Ok(PlannedMaterialization {
            layout,
            strategy: MaterializationStrategy::Local,
            affected_nodes: affected_node_count,
            affected_ranks: affected_ranks.len(),
            fallback_reason: None,
        })
    } else {
        let layout = try_layout_graph_with_hints(snapshot, &hints, None)
            .context(crate::error::GraphLayoutSnafu)?;
        Ok(PlannedMaterialization {
            layout,
            strategy: MaterializationStrategy::Full,
            affected_nodes: affected_node_count,
            affected_ranks: affected_ranks.len(),
            fallback_reason: Some("affected_region_exceeds_limit"),
        })
    }
}

#[cfg(test)]
fn snapshot_edge_identities(snapshot: &GraphSnapshot) -> BTreeSet<(String, String, String)> {
    snapshot
        .edges
        .iter()
        .map(|edge| {
            (
                graph_edge_kind_value(edge.kind).to_owned(),
                edge.source.clone(),
                edge.target.clone(),
            )
        })
        .collect()
}

#[cfg(test)]
fn previous_edge_identities(
    previous: &StoredGraph,
) -> crate::Result<BTreeSet<(String, String, String)>> {
    previous
        .edges
        .values()
        .map(|edge| {
            let kind = parse_edge_kind(&edge.edge_kind)?;
            Ok((
                graph_edge_kind_value(kind).to_owned(),
                edge.source_id.clone(),
                edge.target_id.clone(),
            ))
        })
        .collect()
}

#[cfg(test)]
fn affected_descendant_closure(
    previous_nodes: &BTreeSet<String>,
    current_nodes: &BTreeSet<String>,
    previous_edges: &BTreeSet<(String, String, String)>,
    current_edges: &BTreeSet<(String, String, String)>,
) -> BTreeSet<String> {
    let mut affected = previous_nodes
        .symmetric_difference(current_nodes)
        .cloned()
        .collect::<BTreeSet<_>>();
    for (_, source, target) in previous_edges.symmetric_difference(current_edges) {
        affected.insert(source.clone());
        affected.insert(target.clone());
    }
    let mut outgoing = BTreeMap::<String, Vec<String>>::new();
    for (_, source, target) in previous_edges.iter().chain(current_edges) {
        outgoing
            .entry(source.clone())
            .or_default()
            .push(target.clone());
    }
    let mut pending = affected.iter().cloned().collect::<VecDeque<_>>();
    while let Some(node_id) = pending.pop_front() {
        for target in outgoing.get(&node_id).into_iter().flatten() {
            if affected.insert(target.clone()) {
                pending.push_back(target.clone());
            }
        }
    }
    affected
}

#[cfg(test)]
fn expanded_ranks(ranks: &BTreeSet<usize>) -> BTreeSet<usize> {
    ranks
        .iter()
        .flat_map(|rank| [rank.saturating_sub(1), *rank, rank.saturating_add(1)])
        .collect()
}

impl TryFrom<&GraphLayoutNode> for PersistedNode {
    type Error = crate::Error;

    fn try_from(node: &GraphLayoutNode) -> crate::Result<Self> {
        let labels_json =
            serde_json::to_string(&node.labels).context(SerializeGraphSnapshotStoreValueSnafu {
                column: "labels_json",
            })?;
        let (min_x, min_y, max_x, max_y) = node_bounds(node);
        Ok(Self {
            node_id: node.node_id.clone(),
            node_key: node_key(&node.node_id),
            node_target: node.node_target.clone(),
            short_id: node.short_id.clone(),
            node_kind: node.kind.clone(),
            summary: node.summary.clone(),
            labels_json,
            rank: saturating_i32(node.rank),
            sort_order: saturating_i32(node.order),
            x: node.point.x,
            y: node.point.y,
            created_at: node.created_at.clone(),
            created_at_ns: saturating_i64(node.created_at_ns),
            min_x,
            min_y,
            max_x,
            max_y,
        })
    }
}

#[cfg(test)]
impl From<&StoredNodeRow> for PersistedNode {
    fn from(row: &StoredNodeRow) -> Self {
        Self {
            node_id: row.node_id.clone(),
            node_key: row.node_key.clone(),
            node_target: row.node_target.clone(),
            short_id: row.short_id.clone(),
            node_kind: row.node_kind.clone(),
            summary: row.summary.clone(),
            labels_json: row.labels_json.clone(),
            rank: row.rank,
            sort_order: row.sort_order,
            x: row.x,
            y: row.y,
            created_at: row.created_at.clone(),
            created_at_ns: row.created_at_ns,
            min_x: row.min_x,
            min_y: row.min_y,
            max_x: row.max_x,
            max_y: row.max_y,
        }
    }
}

impl From<&GraphLayoutEdge> for PersistedEdge {
    fn from(edge: &GraphLayoutEdge) -> Self {
        let (min_x, min_y, max_x, max_y) = edge_bounds(edge);
        Self {
            edge_key: edge_key(edge.kind, &edge.source_node_id, &edge.target_node_id),
            edge_kind: edge.kind.key_part().to_owned(),
            source_id: edge.source_node_id.clone(),
            target_id: edge.target_node_id.clone(),
            source_x: edge.route.source.x,
            source_y: edge.route.source.y,
            control_1_x: edge.route.control_1.x,
            control_1_y: edge.route.control_1.y,
            control_2_x: edge.route.control_2.x,
            control_2_y: edge.route.control_2.y,
            target_x: edge.route.target.x,
            target_y: edge.route.target.y,
            min_x,
            min_y,
            max_x,
            max_y,
        }
    }
}

#[cfg(test)]
impl From<&StoredEdgeRow> for PersistedEdge {
    fn from(row: &StoredEdgeRow) -> Self {
        Self {
            edge_key: row.edge_key.clone(),
            edge_kind: row.edge_kind.clone(),
            source_id: row.source_id.clone(),
            target_id: row.target_id.clone(),
            source_x: row.source_x,
            source_y: row.source_y,
            control_1_x: row.control_1_x,
            control_1_y: row.control_1_y,
            control_2_x: row.control_2_x,
            control_2_y: row.control_2_y,
            target_x: row.target_x,
            target_y: row.target_y,
            min_x: row.min_x,
            min_y: row.min_y,
            max_x: row.max_x,
            max_y: row.max_y,
        }
    }
}

#[cfg(test)]
fn write_changed_rows(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: GraphMode,
    previous: &StoredGraph,
    nodes: &[PersistedNode],
    edges: &[PersistedEdge],
) -> QueryResult<()> {
    let next_nodes = nodes
        .iter()
        .map(|node| (node.node_id.as_str(), node))
        .collect::<BTreeMap<_, _>>();
    for node_id in previous.nodes.keys() {
        if !next_nodes.contains_key(node_id.as_str()) {
            diesel::delete(
                console_graph_node_locations::table
                    .filter(console_graph_node_locations::generation.eq(generation))
                    .filter(console_graph_node_locations::mode.eq(mode.as_query_value()))
                    .filter(console_graph_node_locations::node_id.eq(node_id)),
            )
            .execute(connection)?;
        }
    }
    for node in nodes {
        if previous
            .nodes
            .get(&node.node_id)
            .is_none_or(|stored| PersistedNode::from(stored) != *node)
        {
            upsert_node(connection, generation, mode, node)?;
        }
    }

    let next_edges = edges
        .iter()
        .map(|edge| (edge.edge_key.as_str(), edge))
        .collect::<BTreeMap<_, _>>();
    for edge_key in previous.edges.keys() {
        if !next_edges.contains_key(edge_key.as_str()) {
            diesel::delete(
                console_graph_edge_routes::table
                    .filter(console_graph_edge_routes::generation.eq(generation))
                    .filter(console_graph_edge_routes::mode.eq(mode.as_query_value()))
                    .filter(console_graph_edge_routes::edge_key.eq(edge_key)),
            )
            .execute(connection)?;
        }
    }
    for edge in edges {
        if previous
            .edges
            .get(&edge.edge_key)
            .is_none_or(|stored| PersistedEdge::from(stored) != *edge)
        {
            upsert_edge(connection, generation, mode, edge)?;
        }
    }
    Ok(())
}

#[cfg(test)]
fn delete_mode_rows(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: GraphMode,
) -> QueryResult<()> {
    diesel::delete(
        console_graph_edge_routes::table
            .filter(console_graph_edge_routes::generation.eq(generation))
            .filter(console_graph_edge_routes::mode.eq(mode.as_query_value())),
    )
    .execute(connection)?;
    diesel::delete(
        console_graph_node_locations::table
            .filter(console_graph_node_locations::generation.eq(generation))
            .filter(console_graph_node_locations::mode.eq(mode.as_query_value())),
    )
    .execute(connection)?;
    Ok(())
}

fn upsert_node(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: GraphMode,
    node: &PersistedNode,
) -> QueryResult<()> {
    diesel::insert_into(console_graph_node_locations::table)
        .values((
            console_graph_node_locations::generation.eq(generation),
            console_graph_node_locations::mode.eq(mode.as_query_value()),
            node,
        ))
        .on_conflict((
            console_graph_node_locations::generation,
            console_graph_node_locations::mode,
            console_graph_node_locations::node_id,
        ))
        .do_update()
        .set(node)
        .execute(connection)
        .map(|_| ())
}

fn upsert_edge(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: GraphMode,
    edge: &PersistedEdge,
) -> QueryResult<()> {
    diesel::insert_into(console_graph_edge_routes::table)
        .values((
            console_graph_edge_routes::generation.eq(generation),
            console_graph_edge_routes::mode.eq(mode.as_query_value()),
            edge,
        ))
        .on_conflict((
            console_graph_edge_routes::generation,
            console_graph_edge_routes::mode,
            console_graph_edge_routes::edge_key,
        ))
        .do_update()
        .set(edge)
        .execute(connection)
        .map(|_| ())
}

fn upsert_materialization(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: GraphMode,
    source_version: u64,
    width: i32,
    height: i32,
) -> QueryResult<()> {
    let materialization = PersistedMaterialization {
        generation,
        mode: mode.as_query_value(),
        source_version: source_version.min(i64::MAX as u64) as i64,
        coordinate_space: COORDINATE_SPACE,
        world_min_x: 0,
        world_min_y: 0,
        world_max_x: width,
        world_max_y: height,
        updated_at: jiff::Timestamp::now().to_string(),
    };
    diesel::insert_into(console_graph_materializations::table)
        .values(&materialization)
        .on_conflict((
            console_graph_materializations::generation,
            console_graph_materializations::mode,
        ))
        .do_update()
        .set(&materialization)
        .execute(connection)
        .map(|_| ())
}

#[cfg(test)]
fn write_materialized_shell_projection(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: GraphMode,
) -> QueryResult<()> {
    let projection = prepare_materialized_shell_projection(connection, generation, mode)?;
    write_prepared_shell_projection(connection, generation, mode, &projection)
}

fn initialize_incremental_shell_baseline(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: &str,
) -> QueryResult<bool> {
    let state = diesel::sql_query(
        "SELECT build_kind, baseline_generation, baseline_initialized, \
                baseline_phase, baseline_work_kind, baseline_cursor_node_id, \
                baseline_endpoint, baseline_cursor_key, row_cursor, node_count, edge_count, \
                node_max_x, node_max_y, edge_max_x, edge_max_y \
         FROM console_graph_build_shell_projections \
         WHERE run_id = ? AND mode = ?",
    )
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(mode)
    .get_result::<IncrementalShellBaselineRow>(connection)?;
    if state.baseline_initialized == 1 {
        return Ok(true);
    }
    if state.build_kind == "full" {
        let updated = diesel::sql_query(
            "UPDATE console_graph_build_shell_projections \
             SET baseline_initialized = 1, baseline_phase = 'complete' \
             WHERE run_id = ? AND mode = ? AND baseline_initialized = 0 \
               AND build_kind = 'full'",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .execute(connection)?;
        return Ok(updated == 1);
    }
    let baseline_generation = state
        .baseline_generation
        .ok_or(diesel::result::Error::NotFound)?;
    match state.baseline_phase.as_str() {
        "seed" => {
            seed_incremental_shell_baseline(connection, generation, baseline_generation, mode)
        }
        "node_remove" => advance_incremental_shell_baseline_node_removals(
            connection,
            generation,
            baseline_generation,
            mode,
            &state,
        ),
        "edge_remove" => advance_incremental_shell_baseline_edge_removals(
            connection,
            generation,
            baseline_generation,
            mode,
            &state,
        ),
        "node_extents" => advance_incremental_shell_baseline_extents(
            connection,
            generation,
            baseline_generation,
            mode,
            &state,
            true,
        ),
        "edge_extents" => advance_incremental_shell_baseline_extents(
            connection,
            generation,
            baseline_generation,
            mode,
            &state,
            false,
        ),
        "ready" => {
            let updated = diesel::sql_query(
                "UPDATE console_graph_build_shell_projections \
                 SET baseline_initialized = 1, baseline_phase = 'complete', row_cursor = 0 \
                 WHERE run_id = ? AND mode = ? AND build_kind = 'append' \
                   AND baseline_initialized = 0 AND baseline_phase = 'ready'",
            )
            .bind::<BigInt, _>(generation)
            .bind::<Text, _>(mode)
            .execute(connection)?;
            if updated != 1 {
                return Err(diesel::result::Error::NotFound);
            }
            Ok(true)
        }
        _ => Err(diesel::result::Error::NotFound),
    }
}

fn seed_incremental_shell_baseline(
    connection: &mut SqliteConnection,
    generation: i64,
    baseline_generation: i64,
    mode: &str,
) -> QueryResult<bool> {
    let updated = diesel::sql_query(
        "UPDATE console_graph_build_shell_projections \
         SET node_count = COALESCE(( \
                 SELECT node_count FROM console_graph_materialization_shells \
                 WHERE generation = ? AND mode = ? \
             ), 0), \
             edge_count = COALESCE(( \
                 SELECT edge_count FROM console_graph_materialization_shells \
                 WHERE generation = ? AND mode = ? \
             ), 0), \
             node_max_x = NULL, node_max_y = NULL, \
             edge_max_x = NULL, edge_max_y = NULL, row_cursor = 0, \
             baseline_phase = 'node_remove', baseline_work_kind = 'global', \
             baseline_cursor_node_id = '', baseline_endpoint = '', \
             baseline_cursor_key = '' \
         WHERE run_id = ? AND mode = ? AND build_kind = 'append' \
           AND baseline_initialized = 0 AND baseline_phase = 'seed'",
    )
    .bind::<BigInt, _>(baseline_generation)
    .bind::<Text, _>(mode)
    .bind::<BigInt, _>(baseline_generation)
    .bind::<Text, _>(mode)
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(mode)
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(false)
}

fn baseline_removed_table(work_kind: &str, mode: &str) -> QueryResult<&'static str> {
    match (work_kind, mode) {
        ("global", _) => Ok("console_graph_build_node_tombstones"),
        ("anchor_only", "anchors") => Ok("console_graph_build_anchor_node_tombstones"),
        _ => Err(diesel::result::Error::NotFound),
    }
}

fn advance_incremental_shell_baseline_node_removals(
    connection: &mut SqliteConnection,
    generation: i64,
    baseline_generation: i64,
    mode: &str,
    state: &IncrementalShellBaselineRow,
) -> QueryResult<bool> {
    let table = baseline_removed_table(&state.baseline_work_kind, mode)?;
    let exclude_global = if state.baseline_work_kind == "anchor_only" {
        "AND NOT EXISTS ( \
             SELECT 1 FROM console_graph_build_node_tombstones AS global \
             WHERE global.run_id = removed.run_id AND global.node_id = removed.node_id \
         )"
    } else {
        ""
    };
    let nodes = diesel::sql_query(format!(
        "SELECT removed.node_id AS value FROM {table} AS removed \
         WHERE removed.run_id = ? AND removed.node_id > ? {exclude_global} \
         ORDER BY removed.node_id LIMIT 128"
    ))
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(&state.baseline_cursor_node_id)
    .load::<ScalarTextRow>(connection)?;
    if nodes.is_empty() {
        let (phase, work_kind) = if mode == "anchors" && state.baseline_work_kind == "global" {
            ("node_remove", "anchor_only")
        } else {
            ("edge_remove", "global")
        };
        let updated = diesel::sql_query(
            "UPDATE console_graph_build_shell_projections \
             SET baseline_phase = ?, baseline_work_kind = ?, \
                 baseline_cursor_node_id = '', baseline_endpoint = '', \
                 baseline_cursor_key = '' \
             WHERE run_id = ? AND mode = ? AND baseline_phase = 'node_remove' \
               AND baseline_work_kind = ? AND baseline_cursor_node_id = ?",
        )
        .bind::<Text, _>(phase)
        .bind::<Text, _>(work_kind)
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<Text, _>(&state.baseline_work_kind)
        .bind::<Text, _>(&state.baseline_cursor_node_id)
        .execute(connection)?;
        if updated != 1 {
            return Err(diesel::result::Error::NotFound);
        }
        return Ok(false);
    }
    let mut removed_count = 0_i64;
    for node in &nodes {
        removed_count += diesel::sql_query(
            "SELECT COUNT(*) AS count FROM console_graph_node_locations \
             WHERE generation = ? AND mode = ? AND node_id = ?",
        )
        .bind::<BigInt, _>(baseline_generation)
        .bind::<Text, _>(mode)
        .bind::<Text, _>(&node.value)
        .get_result::<CountRow>(connection)?
        .count;
    }
    let last = &nodes.last().expect("non-empty baseline node page").value;
    let updated = diesel::sql_query(
        "UPDATE console_graph_build_shell_projections \
         SET node_count = MAX(node_count - ?, 0), baseline_cursor_node_id = ? \
         WHERE run_id = ? AND mode = ? AND baseline_phase = 'node_remove' \
           AND baseline_work_kind = ? AND baseline_cursor_node_id = ?",
    )
    .bind::<BigInt, _>(removed_count)
    .bind::<Text, _>(last)
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(mode)
    .bind::<Text, _>(&state.baseline_work_kind)
    .bind::<Text, _>(&state.baseline_cursor_node_id)
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(false)
}

fn advance_incremental_shell_baseline_edge_removals(
    connection: &mut SqliteConnection,
    generation: i64,
    baseline_generation: i64,
    mode: &str,
    state: &IncrementalShellBaselineRow,
) -> QueryResult<bool> {
    let table = baseline_removed_table(&state.baseline_work_kind, mode)?;
    let exclude_global = if state.baseline_work_kind == "anchor_only" {
        "AND NOT EXISTS ( \
             SELECT 1 FROM console_graph_build_node_tombstones AS global \
             WHERE global.run_id = removed.run_id AND global.node_id = removed.node_id \
         )"
    } else {
        ""
    };
    if state.baseline_endpoint.is_empty() {
        let next = diesel::sql_query(format!(
            "SELECT removed.node_id AS value FROM {table} AS removed \
             WHERE removed.run_id = ? AND removed.node_id > ? {exclude_global} \
             ORDER BY removed.node_id LIMIT 1"
        ))
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(&state.baseline_cursor_node_id)
        .get_result::<ScalarTextRow>(connection)
        .optional()?;
        let Some(next) = next else {
            let (phase, work_kind) = if mode == "anchors" && state.baseline_work_kind == "global" {
                ("edge_remove", "anchor_only")
            } else {
                ("node_extents", "")
            };
            let updated = diesel::sql_query(
                "UPDATE console_graph_build_shell_projections \
                 SET baseline_phase = ?, baseline_work_kind = ?, \
                     baseline_cursor_node_id = '', baseline_endpoint = '', \
                     baseline_cursor_key = '', row_cursor = 0 \
                 WHERE run_id = ? AND mode = ? AND baseline_phase = 'edge_remove' \
                   AND baseline_work_kind = ? AND baseline_cursor_node_id = ? \
                   AND baseline_endpoint = '' AND baseline_cursor_key = ''",
            )
            .bind::<Text, _>(phase)
            .bind::<Text, _>(work_kind)
            .bind::<BigInt, _>(generation)
            .bind::<Text, _>(mode)
            .bind::<Text, _>(&state.baseline_work_kind)
            .bind::<Text, _>(&state.baseline_cursor_node_id)
            .execute(connection)?;
            if updated != 1 {
                return Err(diesel::result::Error::NotFound);
            }
            return Ok(false);
        };
        let updated = diesel::sql_query(
            "UPDATE console_graph_build_shell_projections \
             SET baseline_cursor_node_id = ?, baseline_endpoint = 'source', \
                 baseline_cursor_key = '' \
             WHERE run_id = ? AND mode = ? AND baseline_phase = 'edge_remove' \
               AND baseline_work_kind = ? AND baseline_cursor_node_id = ? \
               AND baseline_endpoint = '' AND baseline_cursor_key = ''",
        )
        .bind::<Text, _>(next.value)
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<Text, _>(&state.baseline_work_kind)
        .bind::<Text, _>(&state.baseline_cursor_node_id)
        .execute(connection)?;
        if updated != 1 {
            return Err(diesel::result::Error::NotFound);
        }
        return Ok(false);
    }

    let endpoint = match state.baseline_endpoint.as_str() {
        "source" => "source_id",
        "target" => "target_id",
        _ => return Err(diesel::result::Error::NotFound),
    };
    let edges = diesel::sql_query(format!(
        "SELECT edge_key AS value FROM console_graph_edge_routes \
         WHERE generation = ? AND mode = ? AND {endpoint} = ? AND edge_key > ? \
         ORDER BY edge_key LIMIT 128"
    ))
    .bind::<BigInt, _>(baseline_generation)
    .bind::<Text, _>(mode)
    .bind::<Text, _>(&state.baseline_cursor_node_id)
    .bind::<Text, _>(&state.baseline_cursor_key)
    .load::<ScalarTextRow>(connection)?;
    if edges.is_empty() {
        let next_endpoint = if state.baseline_endpoint == "source" {
            "target"
        } else {
            ""
        };
        let updated = diesel::sql_query(
            "UPDATE console_graph_build_shell_projections \
             SET baseline_endpoint = ?, baseline_cursor_key = '' \
             WHERE run_id = ? AND mode = ? AND baseline_phase = 'edge_remove' \
               AND baseline_work_kind = ? AND baseline_cursor_node_id = ? \
               AND baseline_endpoint = ? AND baseline_cursor_key = ?",
        )
        .bind::<Text, _>(next_endpoint)
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<Text, _>(&state.baseline_work_kind)
        .bind::<Text, _>(&state.baseline_cursor_node_id)
        .bind::<Text, _>(&state.baseline_endpoint)
        .bind::<Text, _>(&state.baseline_cursor_key)
        .execute(connection)?;
        if updated != 1 {
            return Err(diesel::result::Error::NotFound);
        }
        return Ok(false);
    }
    let mut removed_count = 0_i64;
    for edge in &edges {
        removed_count += diesel::sql_query(
            "INSERT OR IGNORE INTO console_graph_build_edge_route_tombstones ( \
                 run_id, mode, edge_key \
             ) VALUES (?, ?, ?)",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<Text, _>(&edge.value)
        .execute(connection)? as i64;
    }
    let last = &edges.last().expect("non-empty baseline edge page").value;
    let updated = diesel::sql_query(
        "UPDATE console_graph_build_shell_projections \
         SET edge_count = MAX(edge_count - ?, 0), baseline_cursor_key = ? \
         WHERE run_id = ? AND mode = ? AND baseline_phase = 'edge_remove' \
           AND baseline_work_kind = ? AND baseline_cursor_node_id = ? \
           AND baseline_endpoint = ? AND baseline_cursor_key = ?",
    )
    .bind::<BigInt, _>(removed_count)
    .bind::<Text, _>(last)
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(mode)
    .bind::<Text, _>(&state.baseline_work_kind)
    .bind::<Text, _>(&state.baseline_cursor_node_id)
    .bind::<Text, _>(&state.baseline_endpoint)
    .bind::<Text, _>(&state.baseline_cursor_key)
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(false)
}

fn advance_incremental_shell_baseline_extents(
    connection: &mut SqliteConnection,
    generation: i64,
    baseline_generation: i64,
    mode: &str,
    state: &IncrementalShellBaselineRow,
    nodes: bool,
) -> QueryResult<bool> {
    let rows = if nodes {
        diesel::sql_query(
            "SELECT page.row_id, page.max_x, page.max_y, \
                    CASE WHEN EXISTS ( \
                        SELECT 1 FROM console_graph_build_node_tombstones AS removed \
                        WHERE removed.run_id = ? AND removed.node_id = page.node_id \
                    ) OR (? = 'anchors' AND EXISTS ( \
                        SELECT 1 \
                        FROM console_graph_build_anchor_node_tombstones AS removed \
                        WHERE removed.run_id = ? AND removed.node_id = page.node_id \
                    )) THEN 1 ELSE 0 END AS excluded \
             FROM ( \
                 SELECT rowid AS row_id, node_id, max_x, max_y \
                 FROM console_graph_node_locations \
                 WHERE generation = ? AND mode = ? AND rowid > ? \
                 ORDER BY rowid LIMIT ? \
             ) AS page ORDER BY page.row_id",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(generation)
        .bind::<BigInt, _>(baseline_generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(state.row_cursor)
        .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
        .load::<IncrementalShellBaselineExtentRow>(connection)?
    } else {
        diesel::sql_query(
            "SELECT page.row_id, page.max_x, page.max_y, \
                    CASE WHEN EXISTS ( \
                        SELECT 1 \
                        FROM console_graph_build_edge_route_tombstones AS removed \
                        WHERE removed.run_id = ? AND removed.mode = ? \
                          AND removed.edge_key = page.edge_key \
                    ) THEN 1 ELSE 0 END AS excluded \
             FROM ( \
                 SELECT rowid AS row_id, edge_key, max_x, max_y \
                 FROM console_graph_edge_routes \
                 WHERE generation = ? AND mode = ? AND rowid > ? \
                 ORDER BY rowid LIMIT ? \
             ) AS page ORDER BY page.row_id",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(baseline_generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(state.row_cursor)
        .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
        .load::<IncrementalShellBaselineExtentRow>(connection)?
    };
    if rows.is_empty() {
        let next_phase = if nodes { "edge_extents" } else { "ready" };
        let expected_phase = if nodes {
            "node_extents"
        } else {
            "edge_extents"
        };
        let updated = diesel::sql_query(
            "UPDATE console_graph_build_shell_projections \
             SET baseline_phase = ?, row_cursor = 0 \
             WHERE run_id = ? AND mode = ? AND baseline_phase = ? AND row_cursor = ?",
        )
        .bind::<Text, _>(next_phase)
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<Text, _>(expected_phase)
        .bind::<BigInt, _>(state.row_cursor)
        .execute(connection)?;
        if updated != 1 {
            return Err(diesel::result::Error::NotFound);
        }
        return Ok(false);
    }
    let next_cursor = rows.last().expect("non-empty baseline extent page").row_id;
    let visible = rows.iter().filter(|row| row.excluded == 0);
    let page_max_x = visible.clone().map(|row| row.max_x).max();
    let page_max_y = visible.map(|row| row.max_y).max();
    let (max_x, max_y, max_x_column, max_y_column, phase) = if nodes {
        (
            state.node_max_x.into_iter().chain(page_max_x).max(),
            state.node_max_y.into_iter().chain(page_max_y).max(),
            "node_max_x",
            "node_max_y",
            "node_extents",
        )
    } else {
        (
            state.edge_max_x.into_iter().chain(page_max_x).max(),
            state.edge_max_y.into_iter().chain(page_max_y).max(),
            "edge_max_x",
            "edge_max_y",
            "edge_extents",
        )
    };
    let updated = diesel::sql_query(format!(
        "UPDATE console_graph_build_shell_projections \
         SET row_cursor = ?, {max_x_column} = ?, {max_y_column} = ? \
         WHERE run_id = ? AND mode = ? AND baseline_phase = ? AND row_cursor = ?"
    ))
    .bind::<BigInt, _>(next_cursor)
    .bind::<Nullable<Integer>, _>(max_x)
    .bind::<Nullable<Integer>, _>(max_y)
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(mode)
    .bind::<Text, _>(phase)
    .bind::<BigInt, _>(state.row_cursor)
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(false)
}

fn advance_incremental_shell_projection(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: GraphMode,
    source_version: u64,
) -> QueryResult<bool> {
    let mode_value = mode.as_query_value();
    diesel::sql_query(
        "INSERT OR IGNORE INTO console_graph_build_shell_projections ( \
             run_id, mode, build_kind, baseline_generation \
         ) \
         SELECT run_id, ?, dag_build_kind, dag_baseline_generation \
         FROM console_graph_build_runs WHERE run_id = ?",
    )
    .bind::<Text, _>(mode_value)
    .bind::<BigInt, _>(generation)
    .execute(connection)?;
    if !initialize_incremental_shell_baseline(connection, generation, mode_value)? {
        return Ok(false);
    }
    let state = diesel::sql_query(
        "SELECT phase, row_cursor, node_count, edge_count, \
                node_max_x, node_max_y, edge_max_x, edge_max_y, \
                tick_cursor_created_at_ns, tick_cursor_node_id, tick_ordinal, \
                build_kind, baseline_generation, baseline_initialized \
         FROM console_graph_build_shell_projections \
         WHERE run_id = ? AND mode = ?",
    )
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(mode_value)
    .get_result::<IncrementalShellProjectionRow>(connection)?;

    match state.phase.as_str() {
        "nodes" => {
            advance_incremental_shell_extents(
                connection,
                generation,
                mode_value,
                &state,
                "console_graph_node_locations",
                "node_count",
                "node_max_x",
                "node_max_y",
                "edges",
            )?;
            Ok(false)
        }
        "edges" => {
            let complete = advance_incremental_shell_extents(
                connection,
                generation,
                mode_value,
                &state,
                "console_graph_edge_routes",
                "edge_count",
                "edge_max_x",
                "edge_max_y",
                "ticks",
            )?;
            if !complete {
                return Ok(false);
            }
            let width = shell_extent(state.node_max_x, state.edge_max_x);
            let height = shell_extent(state.node_max_y, state.edge_max_y);
            upsert_materialization(connection, generation, mode, source_version, width, height)?;
            upsert_materialized_shell_counts(
                connection,
                generation,
                mode_value,
                state.node_count,
                state.edge_count,
            )?;
            diesel::delete(
                console_graph_materialization_time_ticks::table
                    .filter(console_graph_materialization_time_ticks::generation.eq(generation))
                    .filter(console_graph_materialization_time_ticks::mode.eq(mode_value)),
            )
            .execute(connection)?;
            Ok(false)
        }
        "ticks" if state.build_kind == "append" => {
            advance_append_shell_ticks(connection, generation, mode_value, &state)
        }
        "baseline_ticks" if state.build_kind == "append" => {
            advance_append_shell_baseline_ticks(connection, generation, mode_value, &state)
        }
        "ticks" => advance_incremental_shell_ticks(connection, generation, mode_value, &state),
        "tick_samples" if state.build_kind == "append" => {
            advance_append_shell_tick_samples(connection, generation, mode_value, &state)
        }
        "ready" => {
            let width = shell_extent(state.node_max_x, state.edge_max_x);
            let height = shell_extent(state.node_max_y, state.edge_max_y);
            upsert_materialization(connection, generation, mode, source_version, width, height)?;
            upsert_materialized_shell_counts(
                connection,
                generation,
                mode_value,
                state.node_count,
                state.edge_count,
            )?;
            let updated = diesel::sql_query(
                "UPDATE console_graph_build_shell_projections SET phase = 'complete' \
                 WHERE run_id = ? AND mode = ? AND phase = 'ready'",
            )
            .bind::<BigInt, _>(generation)
            .bind::<Text, _>(mode_value)
            .execute(connection)?;
            if updated != 1 {
                return Err(diesel::result::Error::NotFound);
            }
            Ok(true)
        }
        "complete" => Ok(true),
        _ => Err(diesel::result::Error::NotFound),
    }
}

#[allow(clippy::too_many_arguments)]
fn advance_incremental_shell_extents(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: &str,
    state: &IncrementalShellProjectionRow,
    table: &str,
    count_column: &str,
    max_x_column: &str,
    max_y_column: &str,
    next_phase: &str,
) -> QueryResult<bool> {
    let rows = diesel::sql_query(format!(
        "SELECT rowid AS row_id, max_x, max_y FROM {table} \
         WHERE generation = ? AND mode = ? AND rowid > ? \
         ORDER BY rowid LIMIT ?"
    ))
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(mode)
    .bind::<BigInt, _>(state.row_cursor)
    .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
    .load::<IncrementalShellExtentRow>(connection)?;
    if rows.is_empty() {
        let updated = diesel::sql_query(
            "UPDATE console_graph_build_shell_projections \
             SET phase = ?, row_cursor = 0 \
             WHERE run_id = ? AND mode = ? AND phase = ? AND row_cursor = ?",
        )
        .bind::<Text, _>(next_phase)
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<Text, _>(&state.phase)
        .bind::<BigInt, _>(state.row_cursor)
        .execute(connection)?;
        if updated != 1 {
            return Err(diesel::result::Error::NotFound);
        }
        return Ok(true);
    }

    let next_cursor = rows
        .last()
        .expect("non-empty shell extent page must have a last row")
        .row_id;
    let current_max_x = if count_column == "node_count" {
        state.node_max_x
    } else {
        state.edge_max_x
    };
    let current_max_y = if count_column == "node_count" {
        state.node_max_y
    } else {
        state.edge_max_y
    };
    let max_x = current_max_x
        .into_iter()
        .chain(rows.iter().map(|row| row.max_x))
        .max();
    let max_y = current_max_y
        .into_iter()
        .chain(rows.iter().map(|row| row.max_y))
        .max();
    let updated = diesel::sql_query(format!(
        "UPDATE console_graph_build_shell_projections \
         SET row_cursor = ?, {count_column} = {count_column} + ?, \
             {max_x_column} = ?, {max_y_column} = ? \
         WHERE run_id = ? AND mode = ? AND phase = ? AND row_cursor = ?"
    ))
    .bind::<BigInt, _>(next_cursor)
    .bind::<BigInt, _>(rows.len() as i64)
    .bind::<Nullable<Integer>, _>(max_x)
    .bind::<Nullable<Integer>, _>(max_y)
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(mode)
    .bind::<Text, _>(&state.phase)
    .bind::<BigInt, _>(state.row_cursor)
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(false)
}

fn advance_append_shell_ticks(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: &str,
    state: &IncrementalShellProjectionRow,
) -> QueryResult<bool> {
    let rows = match (
        state.tick_cursor_created_at_ns,
        state.tick_cursor_node_id.as_deref(),
    ) {
        (Some(created_at_ns), Some(node_id)) => diesel::sql_query(
            "SELECT node_id, node_target, x, y, created_at, created_at_ns \
             FROM console_graph_node_locations \
             WHERE generation = ? AND mode = ? \
               AND (created_at_ns > ? OR (created_at_ns = ? AND node_id > ?)) \
             ORDER BY created_at_ns, node_id LIMIT ?",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(created_at_ns)
        .bind::<BigInt, _>(created_at_ns)
        .bind::<Text, _>(node_id)
        .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
        .load::<IncrementalShellTickSourceRow>(connection)?,
        (None, None) => diesel::sql_query(
            "SELECT node_id, node_target, x, y, created_at, created_at_ns \
             FROM console_graph_node_locations \
             WHERE generation = ? AND mode = ? \
             ORDER BY created_at_ns, node_id LIMIT ?",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
        .load::<IncrementalShellTickSourceRow>(connection)?,
        _ => return Err(diesel::result::Error::NotFound),
    };
    if rows.is_empty() {
        let updated = diesel::sql_query(
            "UPDATE console_graph_build_shell_projections \
             SET phase = 'baseline_ticks', tick_cursor_created_at_ns = NULL, \
                 tick_cursor_node_id = NULL \
             WHERE run_id = ? AND mode = ? AND phase = 'ticks' \
               AND row_cursor = ? AND tick_ordinal = 0 \
               AND ((tick_cursor_created_at_ns IS NULL AND ? IS NULL) \
                    OR tick_cursor_created_at_ns = ?) \
               AND ((tick_cursor_node_id IS NULL AND ? IS NULL) \
                    OR tick_cursor_node_id = ?)",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(state.row_cursor)
        .bind::<Nullable<BigInt>, _>(state.tick_cursor_created_at_ns)
        .bind::<Nullable<BigInt>, _>(state.tick_cursor_created_at_ns)
        .bind::<Nullable<Text>, _>(state.tick_cursor_node_id.clone())
        .bind::<Nullable<Text>, _>(state.tick_cursor_node_id.clone())
        .execute(connection)?;
        if updated != 1 {
            return Err(diesel::result::Error::NotFound);
        }
        return Ok(false);
    }

    let mut inserted_count = 0_i64;
    for row in &rows {
        inserted_count += diesel::sql_query(
            "INSERT OR IGNORE INTO console_graph_build_shell_tick_candidates ( \
                 run_id, mode, created_at_ns, node_id, node_target, x, y, created_at \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(row.created_at_ns)
        .bind::<Text, _>(&row.node_id)
        .bind::<Text, _>(&row.node_target)
        .bind::<Integer, _>(row.x)
        .bind::<Integer, _>(row.y)
        .bind::<Text, _>(&row.created_at)
        .execute(connection)? as i64;
    }
    let last = rows
        .last()
        .expect("non-empty append shell tick page must have a last row");
    let next_candidate_count = state.row_cursor.saturating_add(inserted_count);
    let updated = diesel::sql_query(
        "UPDATE console_graph_build_shell_projections \
         SET tick_cursor_created_at_ns = ?, tick_cursor_node_id = ?, row_cursor = ? \
         WHERE run_id = ? AND mode = ? AND phase = 'ticks' \
           AND row_cursor = ? AND tick_ordinal = 0 \
           AND ((tick_cursor_created_at_ns IS NULL AND ? IS NULL) \
                OR tick_cursor_created_at_ns = ?) \
           AND ((tick_cursor_node_id IS NULL AND ? IS NULL) OR tick_cursor_node_id = ?)",
    )
    .bind::<BigInt, _>(last.created_at_ns)
    .bind::<Text, _>(&last.node_id)
    .bind::<BigInt, _>(next_candidate_count)
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(mode)
    .bind::<BigInt, _>(state.row_cursor)
    .bind::<Nullable<BigInt>, _>(state.tick_cursor_created_at_ns)
    .bind::<Nullable<BigInt>, _>(state.tick_cursor_created_at_ns)
    .bind::<Nullable<Text>, _>(state.tick_cursor_node_id.clone())
    .bind::<Nullable<Text>, _>(state.tick_cursor_node_id.clone())
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(false)
}

fn advance_append_shell_baseline_ticks(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: &str,
    state: &IncrementalShellProjectionRow,
) -> QueryResult<bool> {
    let baseline_generation = state
        .baseline_generation
        .ok_or(diesel::result::Error::NotFound)?;
    let rows = match (
        state.tick_cursor_created_at_ns,
        state.tick_cursor_node_id.as_deref(),
    ) {
        (Some(created_at_ns), Some(node_id)) => diesel::sql_query(
            "SELECT baseline.node_id, baseline.node_target, baseline.x, baseline.y, \
                    baseline.created_at, baseline.created_at_ns, \
                    CASE WHEN EXISTS ( \
                        SELECT 1 FROM console_graph_build_node_tombstones AS removed \
                        WHERE removed.run_id = ? AND removed.node_id = baseline.node_id \
                    ) OR (? = 'anchors' AND EXISTS ( \
                        SELECT 1 \
                        FROM console_graph_build_anchor_node_tombstones AS removed \
                        WHERE removed.run_id = ? AND removed.node_id = baseline.node_id \
                    )) THEN 1 ELSE 0 END AS excluded \
             FROM console_graph_node_locations AS baseline \
             WHERE baseline.generation = ? AND baseline.mode = ? \
               AND (baseline.created_at_ns > ? OR ( \
                   baseline.created_at_ns = ? AND baseline.node_id > ? \
               )) \
             ORDER BY baseline.created_at_ns, baseline.node_id LIMIT ?",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(generation)
        .bind::<BigInt, _>(baseline_generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(created_at_ns)
        .bind::<BigInt, _>(created_at_ns)
        .bind::<Text, _>(node_id)
        .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
        .load::<IncrementalShellBaselineTickSourceRow>(connection)?,
        (None, None) => diesel::sql_query(
            "SELECT baseline.node_id, baseline.node_target, baseline.x, baseline.y, \
                    baseline.created_at, baseline.created_at_ns, \
                    CASE WHEN EXISTS ( \
                        SELECT 1 FROM console_graph_build_node_tombstones AS removed \
                        WHERE removed.run_id = ? AND removed.node_id = baseline.node_id \
                    ) OR (? = 'anchors' AND EXISTS ( \
                        SELECT 1 \
                        FROM console_graph_build_anchor_node_tombstones AS removed \
                        WHERE removed.run_id = ? AND removed.node_id = baseline.node_id \
                    )) THEN 1 ELSE 0 END AS excluded \
             FROM console_graph_node_locations AS baseline \
             WHERE baseline.generation = ? AND baseline.mode = ? \
             ORDER BY baseline.created_at_ns, baseline.node_id LIMIT ?",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(generation)
        .bind::<BigInt, _>(baseline_generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
        .load::<IncrementalShellBaselineTickSourceRow>(connection)?,
        _ => return Err(diesel::result::Error::NotFound),
    };
    if rows.is_empty() {
        if state.row_cursor != state.node_count || state.tick_ordinal != 0 {
            return Err(diesel::result::Error::NotFound);
        }
        diesel::sql_query(
            "DELETE FROM console_graph_materialization_time_ticks \
             WHERE generation = ? AND mode = ?",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .execute(connection)?;
        let updated = diesel::sql_query(
            "UPDATE console_graph_build_shell_projections \
             SET phase = 'tick_samples', tick_cursor_created_at_ns = NULL, \
                 tick_cursor_node_id = NULL \
             WHERE run_id = ? AND mode = ? AND phase = 'baseline_ticks' \
               AND row_cursor = ? AND tick_ordinal = 0 \
               AND ((tick_cursor_created_at_ns IS NULL AND ? IS NULL) \
                    OR tick_cursor_created_at_ns = ?) \
               AND ((tick_cursor_node_id IS NULL AND ? IS NULL) \
                    OR tick_cursor_node_id = ?)",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(state.row_cursor)
        .bind::<Nullable<BigInt>, _>(state.tick_cursor_created_at_ns)
        .bind::<Nullable<BigInt>, _>(state.tick_cursor_created_at_ns)
        .bind::<Nullable<Text>, _>(state.tick_cursor_node_id.clone())
        .bind::<Nullable<Text>, _>(state.tick_cursor_node_id.clone())
        .execute(connection)?;
        if updated != 1 {
            return Err(diesel::result::Error::NotFound);
        }
        return Ok(false);
    }

    let mut inserted_count = 0_i64;
    for row in &rows {
        if row.excluded != 0 {
            continue;
        }
        inserted_count += diesel::sql_query(
            "INSERT OR IGNORE INTO console_graph_build_shell_tick_candidates ( \
                 run_id, mode, created_at_ns, node_id, node_target, x, y, created_at \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(row.created_at_ns)
        .bind::<Text, _>(&row.node_id)
        .bind::<Text, _>(&row.node_target)
        .bind::<Integer, _>(row.x)
        .bind::<Integer, _>(row.y)
        .bind::<Text, _>(&row.created_at)
        .execute(connection)? as i64;
    }
    let last = rows
        .last()
        .expect("non-empty baseline shell tick page must have a last row");
    let next_candidate_count = state.row_cursor.saturating_add(inserted_count);
    let updated = diesel::sql_query(
        "UPDATE console_graph_build_shell_projections \
         SET tick_cursor_created_at_ns = ?, tick_cursor_node_id = ?, row_cursor = ? \
         WHERE run_id = ? AND mode = ? AND phase = 'baseline_ticks' \
           AND row_cursor = ? AND tick_ordinal = 0 \
           AND ((tick_cursor_created_at_ns IS NULL AND ? IS NULL) \
                OR tick_cursor_created_at_ns = ?) \
           AND ((tick_cursor_node_id IS NULL AND ? IS NULL) OR tick_cursor_node_id = ?)",
    )
    .bind::<BigInt, _>(last.created_at_ns)
    .bind::<Text, _>(&last.node_id)
    .bind::<BigInt, _>(next_candidate_count)
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(mode)
    .bind::<BigInt, _>(state.row_cursor)
    .bind::<Nullable<BigInt>, _>(state.tick_cursor_created_at_ns)
    .bind::<Nullable<BigInt>, _>(state.tick_cursor_created_at_ns)
    .bind::<Nullable<Text>, _>(state.tick_cursor_node_id.clone())
    .bind::<Nullable<Text>, _>(state.tick_cursor_node_id.clone())
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(false)
}

fn advance_append_shell_tick_samples(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: &str,
    state: &IncrementalShellProjectionRow,
) -> QueryResult<bool> {
    let rows = match (
        state.tick_cursor_created_at_ns,
        state.tick_cursor_node_id.as_deref(),
    ) {
        (Some(created_at_ns), Some(node_id)) => diesel::sql_query(
            "SELECT node_id, node_target, x, y, created_at, created_at_ns \
             FROM console_graph_build_shell_tick_candidates \
             WHERE run_id = ? AND mode = ? \
               AND (created_at_ns > ? OR (created_at_ns = ? AND node_id > ?)) \
             ORDER BY created_at_ns, node_id LIMIT ?",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(created_at_ns)
        .bind::<BigInt, _>(created_at_ns)
        .bind::<Text, _>(node_id)
        .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
        .load::<IncrementalShellTickSourceRow>(connection)?,
        (None, None) => diesel::sql_query(
            "SELECT node_id, node_target, x, y, created_at, created_at_ns \
             FROM console_graph_build_shell_tick_candidates \
             WHERE run_id = ? AND mode = ? \
             ORDER BY created_at_ns, node_id LIMIT ?",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
        .load::<IncrementalShellTickSourceRow>(connection)?,
        _ => return Err(diesel::result::Error::NotFound),
    };
    if rows.is_empty() {
        if state.row_cursor != state.node_count || state.tick_ordinal != state.node_count {
            return Err(diesel::result::Error::NotFound);
        }
        let updated = diesel::sql_query(
            "UPDATE console_graph_build_shell_projections SET phase = 'ready' \
             WHERE run_id = ? AND mode = ? AND phase = 'tick_samples' \
               AND row_cursor = ? AND tick_ordinal = ? \
               AND ((tick_cursor_created_at_ns IS NULL AND ? IS NULL) \
                    OR tick_cursor_created_at_ns = ?) \
               AND ((tick_cursor_node_id IS NULL AND ? IS NULL) \
                    OR tick_cursor_node_id = ?)",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(state.row_cursor)
        .bind::<BigInt, _>(state.tick_ordinal)
        .bind::<Nullable<BigInt>, _>(state.tick_cursor_created_at_ns)
        .bind::<Nullable<BigInt>, _>(state.tick_cursor_created_at_ns)
        .bind::<Nullable<Text>, _>(state.tick_cursor_node_id.clone())
        .bind::<Nullable<Text>, _>(state.tick_cursor_node_id.clone())
        .execute(connection)?;
        if updated != 1 {
            return Err(diesel::result::Error::NotFound);
        }
        return Ok(false);
    }

    for (index, row) in rows.iter().enumerate() {
        let ordinal = state.tick_ordinal.saturating_add(index as i64);
        let Some(sample_index) = shell_tick_sample_index(ordinal, state.node_count) else {
            continue;
        };
        diesel::sql_query(
            "INSERT INTO console_graph_materialization_time_ticks ( \
                 generation, mode, sample_index, node_id, node_target, x, y, \
                 created_at, created_at_ns \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(generation, mode, sample_index) DO UPDATE SET \
                 node_id = excluded.node_id, node_target = excluded.node_target, \
                 x = excluded.x, y = excluded.y, created_at = excluded.created_at, \
                 created_at_ns = excluded.created_at_ns",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<Integer, _>(sample_index)
        .bind::<Text, _>(&row.node_id)
        .bind::<Text, _>(&row.node_target)
        .bind::<Integer, _>(row.x)
        .bind::<Integer, _>(row.y)
        .bind::<Text, _>(&row.created_at)
        .bind::<BigInt, _>(row.created_at_ns)
        .execute(connection)?;
    }

    let last = rows
        .last()
        .expect("non-empty append shell sample page must have a last row");
    let next_ordinal = state.tick_ordinal.saturating_add(rows.len() as i64);
    let updated = diesel::sql_query(
        "UPDATE console_graph_build_shell_projections \
         SET tick_cursor_created_at_ns = ?, tick_cursor_node_id = ?, tick_ordinal = ? \
         WHERE run_id = ? AND mode = ? AND phase = 'tick_samples' \
           AND row_cursor = ? AND tick_ordinal = ? \
           AND ((tick_cursor_created_at_ns IS NULL AND ? IS NULL) \
                OR tick_cursor_created_at_ns = ?) \
           AND ((tick_cursor_node_id IS NULL AND ? IS NULL) OR tick_cursor_node_id = ?)",
    )
    .bind::<BigInt, _>(last.created_at_ns)
    .bind::<Text, _>(&last.node_id)
    .bind::<BigInt, _>(next_ordinal)
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(mode)
    .bind::<BigInt, _>(state.row_cursor)
    .bind::<BigInt, _>(state.tick_ordinal)
    .bind::<Nullable<BigInt>, _>(state.tick_cursor_created_at_ns)
    .bind::<Nullable<BigInt>, _>(state.tick_cursor_created_at_ns)
    .bind::<Nullable<Text>, _>(state.tick_cursor_node_id.clone())
    .bind::<Nullable<Text>, _>(state.tick_cursor_node_id.clone())
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(false)
}

fn advance_incremental_shell_ticks(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: &str,
    state: &IncrementalShellProjectionRow,
) -> QueryResult<bool> {
    let rows = match (
        state.tick_cursor_created_at_ns,
        state.tick_cursor_node_id.as_deref(),
    ) {
        (Some(created_at_ns), Some(node_id)) => diesel::sql_query(
            "SELECT node_id, node_target, x, y, created_at, created_at_ns \
             FROM console_graph_node_locations \
             WHERE generation = ? AND mode = ? \
               AND (created_at_ns > ? OR (created_at_ns = ? AND node_id > ?)) \
             ORDER BY created_at_ns, node_id LIMIT ?",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(created_at_ns)
        .bind::<BigInt, _>(created_at_ns)
        .bind::<Text, _>(node_id)
        .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
        .load::<IncrementalShellTickSourceRow>(connection)?,
        (None, None) => diesel::sql_query(
            "SELECT node_id, node_target, x, y, created_at, created_at_ns \
             FROM console_graph_node_locations \
             WHERE generation = ? AND mode = ? \
             ORDER BY created_at_ns, node_id LIMIT ?",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
        .load::<IncrementalShellTickSourceRow>(connection)?,
        _ => return Err(diesel::result::Error::NotFound),
    };
    if rows.is_empty() {
        if state.tick_ordinal != state.node_count {
            return Err(diesel::result::Error::NotFound);
        }
        let updated = diesel::sql_query(
            "UPDATE console_graph_build_shell_projections SET phase = 'ready' \
             WHERE run_id = ? AND mode = ? AND phase = 'ticks' \
               AND tick_ordinal = ?",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<BigInt, _>(state.tick_ordinal)
        .execute(connection)?;
        if updated != 1 {
            return Err(diesel::result::Error::NotFound);
        }
        return Ok(false);
    }

    for (index, row) in rows.iter().enumerate() {
        let ordinal = state.tick_ordinal.saturating_add(index as i64);
        let Some(sample_index) = shell_tick_sample_index(ordinal, state.node_count) else {
            continue;
        };
        diesel::sql_query(
            "INSERT INTO console_graph_materialization_time_ticks ( \
                 generation, mode, sample_index, node_id, node_target, x, y, \
                 created_at, created_at_ns \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(generation, mode, sample_index) DO UPDATE SET \
                 node_id = excluded.node_id, node_target = excluded.node_target, \
                 x = excluded.x, y = excluded.y, created_at = excluded.created_at, \
                 created_at_ns = excluded.created_at_ns",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<Integer, _>(sample_index)
        .bind::<Text, _>(&row.node_id)
        .bind::<Text, _>(&row.node_target)
        .bind::<Integer, _>(row.x)
        .bind::<Integer, _>(row.y)
        .bind::<Text, _>(&row.created_at)
        .bind::<BigInt, _>(row.created_at_ns)
        .execute(connection)?;
    }
    let last = rows
        .last()
        .expect("non-empty shell tick page must have a last row");
    let next_ordinal = state.tick_ordinal.saturating_add(rows.len() as i64);
    let updated = diesel::sql_query(
        "UPDATE console_graph_build_shell_projections \
         SET tick_cursor_created_at_ns = ?, tick_cursor_node_id = ?, tick_ordinal = ? \
         WHERE run_id = ? AND mode = ? AND phase = 'ticks' AND tick_ordinal = ? \
           AND ((tick_cursor_created_at_ns IS NULL AND ? IS NULL) \
                OR tick_cursor_created_at_ns = ?) \
           AND ((tick_cursor_node_id IS NULL AND ? IS NULL) OR tick_cursor_node_id = ?)",
    )
    .bind::<BigInt, _>(last.created_at_ns)
    .bind::<Text, _>(&last.node_id)
    .bind::<BigInt, _>(next_ordinal)
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(mode)
    .bind::<BigInt, _>(state.tick_ordinal)
    .bind::<Nullable<BigInt>, _>(state.tick_cursor_created_at_ns)
    .bind::<Nullable<BigInt>, _>(state.tick_cursor_created_at_ns)
    .bind::<Nullable<Text>, _>(state.tick_cursor_node_id.clone())
    .bind::<Nullable<Text>, _>(state.tick_cursor_node_id.clone())
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(false)
}

fn shell_tick_sample_index(ordinal: i64, node_count: i64) -> Option<i32> {
    if ordinal < 0 || ordinal >= node_count || node_count <= 0 {
        return None;
    }
    if node_count <= MATERIALIZED_SHELL_TIME_TICK_LIMIT as i64 {
        return Some(ordinal as i32);
    }
    if ordinal == 0 {
        return Some(0);
    }
    let span = (MATERIALIZED_SHELL_TIME_TICK_LIMIT - 1) as i64;
    let denominator = node_count - 1;
    let bucket = ordinal.saturating_mul(span) / denominator;
    let previous_bucket = (ordinal - 1).saturating_mul(span) / denominator;
    (previous_bucket != bucket).then_some(bucket as i32)
}

fn shell_extent(node_max: Option<i32>, edge_max: Option<i32>) -> i32 {
    node_max
        .into_iter()
        .chain(edge_max)
        .max()
        .unwrap_or(GRAPH_PADDING)
        .max(GRAPH_PADDING)
        .saturating_add(GRAPH_PADDING)
}

fn upsert_materialized_shell_counts(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: &str,
    node_count: i64,
    edge_count: i64,
) -> QueryResult<()> {
    diesel::insert_into(console_graph_materialization_shells::table)
        .values((
            console_graph_materialization_shells::generation.eq(generation),
            console_graph_materialization_shells::mode.eq(mode),
            console_graph_materialization_shells::node_count.eq(node_count),
            console_graph_materialization_shells::edge_count.eq(edge_count),
        ))
        .on_conflict((
            console_graph_materialization_shells::generation,
            console_graph_materialization_shells::mode,
        ))
        .do_update()
        .set((
            console_graph_materialization_shells::node_count.eq(node_count),
            console_graph_materialization_shells::edge_count.eq(edge_count),
        ))
        .execute(connection)
        .map(|_| ())
}

#[cfg(test)]
fn prepare_materialized_shell_projection(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: GraphMode,
) -> QueryResult<PreparedShellProjection> {
    let mode = mode.as_query_value();
    let summary = diesel::sql_query(
        "WITH node_summary AS (\
             SELECT MAX(max_x) AS node_max_x, MAX(max_y) AS node_max_y, \
                    COUNT(*) AS node_count \
             FROM console_graph_node_locations \
             WHERE generation = ? AND mode = ?\
         ), edge_summary AS (\
             SELECT MAX(max_x) AS edge_max_x, MAX(max_y) AS edge_max_y, \
                    COUNT(*) AS edge_count \
             FROM console_graph_edge_routes \
             WHERE generation = ? AND mode = ?\
         ) \
         SELECT node_max_x, node_max_y, edge_max_x, edge_max_y, \
                node_count, edge_count \
         FROM node_summary CROSS JOIN edge_summary",
    )
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(mode)
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(mode)
    .get_result::<MaterializedShellSummaryRow>(connection)?;

    let sample_span = MATERIALIZED_SHELL_TIME_TICK_LIMIT - 1;
    let time_ticks = diesel::sql_query(format!(
        "WITH ordered_nodes AS (\
             SELECT node_id, node_target, x, y, created_at, created_at_ns, \
                    ROW_NUMBER() OVER (ORDER BY created_at_ns, node_id) AS row_number \
             FROM console_graph_node_locations \
             WHERE generation = ? AND mode = ?\
         ) \
         SELECT CAST(\
                    CASE WHEN ? <= {MATERIALIZED_SHELL_TIME_TICK_LIMIT} \
                         THEN row_number - 1 \
                         ELSE (row_number - 1) * {sample_span} / (? - 1) END \
                    AS INTEGER\
                ) AS sample_index, \
                node_id, node_target, x, y, created_at, created_at_ns \
         FROM ordered_nodes \
         WHERE ? <= {MATERIALIZED_SHELL_TIME_TICK_LIMIT} \
            OR row_number = 1 \
            OR ((row_number - 1) * {sample_span} / (? - 1)) \
               > ((row_number - 2) * {sample_span} / (? - 1)) \
         ORDER BY row_number \
         LIMIT {MATERIALIZED_SHELL_TIME_TICK_LIMIT}"
    ))
    .bind::<BigInt, _>(generation)
    .bind::<Text, _>(mode)
    .bind::<BigInt, _>(summary.node_count)
    .bind::<BigInt, _>(summary.node_count)
    .bind::<BigInt, _>(summary.node_count)
    .bind::<BigInt, _>(summary.node_count)
    .bind::<BigInt, _>(summary.node_count)
    .load::<PreparedShellTickRow>(connection)?;

    let width = summary
        .node_max_x
        .into_iter()
        .chain(summary.edge_max_x)
        .max()
        .unwrap_or(GRAPH_PADDING)
        .max(GRAPH_PADDING)
        .saturating_add(GRAPH_PADDING);
    let height = summary
        .node_max_y
        .into_iter()
        .chain(summary.edge_max_y)
        .max()
        .unwrap_or(GRAPH_PADDING)
        .max(GRAPH_PADDING)
        .saturating_add(GRAPH_PADDING);

    Ok(PreparedShellProjection {
        width,
        height,
        node_count: summary.node_count,
        edge_count: summary.edge_count,
        time_ticks,
    })
}

#[cfg(test)]
fn write_prepared_shell_projection(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: GraphMode,
    projection: &PreparedShellProjection,
) -> QueryResult<()> {
    let mode = mode.as_query_value();

    diesel::insert_into(console_graph_materialization_shells::table)
        .values((
            console_graph_materialization_shells::generation.eq(generation),
            console_graph_materialization_shells::mode.eq(mode),
            console_graph_materialization_shells::node_count.eq(projection.node_count),
            console_graph_materialization_shells::edge_count.eq(projection.edge_count),
        ))
        .on_conflict((
            console_graph_materialization_shells::generation,
            console_graph_materialization_shells::mode,
        ))
        .do_update()
        .set((
            console_graph_materialization_shells::node_count.eq(projection.node_count),
            console_graph_materialization_shells::edge_count.eq(projection.edge_count),
        ))
        .execute(connection)?;
    diesel::delete(
        console_graph_materialization_time_ticks::table
            .filter(console_graph_materialization_time_ticks::generation.eq(generation))
            .filter(console_graph_materialization_time_ticks::mode.eq(mode)),
    )
    .execute(connection)?;
    for time_tick in &projection.time_ticks {
        diesel::sql_query(
            "INSERT INTO console_graph_materialization_time_ticks (\
                 generation, mode, sample_index, node_id, node_target, x, y, \
                 created_at, created_at_ns\
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind::<BigInt, _>(generation)
        .bind::<Text, _>(mode)
        .bind::<Integer, _>(time_tick.sample_index)
        .bind::<Text, _>(&time_tick.node_id)
        .bind::<Text, _>(&time_tick.node_target)
        .bind::<Integer, _>(time_tick.x)
        .bind::<Integer, _>(time_tick.y)
        .bind::<Text, _>(&time_tick.created_at)
        .bind::<BigInt, _>(time_tick.created_at_ns)
        .execute(connection)?;
    }
    Ok(())
}

fn stored_viewport_node(row: StoredNodeRow) -> crate::Result<GraphViewportNode> {
    Ok(GraphViewportNode {
        key: row.node_key,
        id: row.node_id,
        node_target: row.node_target,
        short_id: row.short_id,
        kind: row.node_kind,
        summary: row.summary,
        labels: parse_labels(&row.labels_json)?,
        x: row.x,
        y: row.y,
    })
}

fn stored_viewport_edge(row: StoredEdgeRow) -> crate::Result<GraphViewportEdge> {
    Ok(GraphViewportEdge {
        key: row.edge_key,
        kind: parse_viewport_edge_kind(&row.edge_kind)?,
        source_id: row.source_id,
        target_id: row.target_id,
        route: GraphBezierRoute {
            source: Point {
                x: row.source_x,
                y: row.source_y,
            },
            control_1: Point {
                x: row.control_1_x,
                y: row.control_1_y,
            },
            control_2: Point {
                x: row.control_2_x,
                y: row.control_2_y,
            },
            target: Point {
                x: row.target_x,
                y: row.target_y,
            },
        },
    })
}

fn parse_labels(value: &str) -> crate::Result<Vec<String>> {
    serde_json::from_str(value).context(ParseGraphSnapshotStoreValueSnafu {
        column: "labels_json",
    })
}

fn parse_viewport_edge_kind(value: &str) -> crate::Result<GraphViewportEdgeKind> {
    match value {
        "primary_parent" => Ok(GraphViewportEdgeKind::Primary),
        "merge_parent" => Ok(GraphViewportEdgeKind::Merge),
        "shadow_parent" => Ok(GraphViewportEdgeKind::Shadow),
        _ => InvalidGraphSnapshotStoreValueSnafu {
            column: "edge_kind",
            value: value.to_owned(),
        }
        .fail(),
    }
}

#[cfg(test)]
fn parse_edge_kind(value: &str) -> crate::Result<GraphEdgeKind> {
    match parse_viewport_edge_kind(value)? {
        GraphViewportEdgeKind::Primary => Ok(GraphEdgeKind::Primary),
        GraphViewportEdgeKind::Merge => Ok(GraphEdgeKind::Merge),
        GraphViewportEdgeKind::Shadow => Ok(GraphEdgeKind::Shadow),
    }
}

#[cfg(test)]
fn graph_edge_kind_value(kind: GraphEdgeKind) -> &'static str {
    match kind {
        GraphEdgeKind::Primary => "primary_parent",
        GraphEdgeKind::Merge => "merge_parent",
        GraphEdgeKind::Shadow => "shadow_parent",
    }
}

fn saturating_i32(value: usize) -> i32 {
    value.min(i32::MAX as usize) as i32
}

fn saturating_i64(value: i128) -> i64 {
    value.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn unix_time_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    millis.min(i64::MAX as u128) as i64
}

fn incremental_build_lease_deadline(now_ms: i64) -> i64 {
    let ttl_ms = INCREMENTAL_BUILD_LEASE_TTL
        .as_millis()
        .min(i64::MAX as u128) as i64;
    now_ms.saturating_add(ttl_ms)
}

fn incremental_build_progress_from_row(
    row: &IncrementalBuildProgressRow,
) -> IncrementalBuildProgress {
    if let Some(overlay_phase) = row.overlay_phase.as_deref() {
        let (stage, message) = match overlay_phase {
            "route_tombstones" => (
                "overlay_prepare_edge_routes",
                "Preparing removed graph edge routes",
            ),
            "port_tombstones" => (
                "overlay_prepare_edge_ports",
                "Preparing removed graph edge ports",
            ),
            "ready" => ("overlay_activation", "Activating incremental graph overlay"),
            "branch_delete" | "scope_delete" | "route_delete" | "port_delete" | "node_delete"
            | "assignment_delete" | "tick_delete" => (
                "overlay_compaction_delete",
                "Compacting removed graph publication rows",
            ),
            "node_upsert" | "route_upsert" | "port_upsert" | "branch_upsert" | "scope_upsert"
            | "assignment_upsert" | "manifest" | "materialization" | "shell" | "tick_upsert" => (
                "overlay_compaction_upsert",
                "Compacting updated graph publication rows",
            ),
            "finalize" => (
                "overlay_compaction_finalize",
                "Finalizing incremental graph publication",
            ),
            _ => (
                "overlay_publication",
                "Publishing incremental graph overlay",
            ),
        };
        return IncrementalBuildProgress {
            stage,
            phase: GraphBuildPhase::Snapshot,
            unit: "publication_page",
            completed_units: 0,
            total_units: 0,
            message,
        };
    }
    debug_assert_eq!(row.build_status, "building");
    if row.dag_initialized == 0 {
        let (stage, unit, message) = match row.dag_init_phase.as_str() {
            "new"
            | "full_reset"
            | "manifest_refresh_reset"
            | "manifest_branch_reset"
            | "manifest_copy"
            | "manifest_refresh_copy"
            | "manifest_reset"
            | "manifest_seed" => (
                "source_manifest",
                "source_branch",
                "Preparing graph source manifest",
            ),
            "scope" => (
                "branch_scope",
                "scope_entry",
                "Expanding graph branch scopes",
            ),
            "kind" => (
                "baseline_validation",
                "validation_step",
                "Validating incremental graph baseline",
            ),
            "kind_journal" => (
                "source_change_journal",
                "source_change",
                "Reading incremental source changes",
            ),
            "delta_remove" => (
                "delta_tombstones",
                "graph_node",
                "Capturing removed graph nodes",
            ),
            "delta_scope_remove" => (
                "delta_scope_tombstones",
                "scope_entry",
                "Capturing removed graph branch scopes",
            ),
            "delta_seed" => (
                "delta_membership",
                "graph_node",
                "Capturing changed graph nodes",
            ),
            "rank_slots" | "nodes" | "edges" | "ports" => (
                "layout_initialization",
                "layout_checkpoint",
                "Preparing stable graph layout",
            ),
            "branches" => (
                "branch_metadata",
                "source_branch",
                "Copying graph branch metadata",
            ),
            _ => (
                "initialization",
                "checkpoint_row",
                "Initializing incremental graph build",
            ),
        };
        return IncrementalBuildProgress {
            stage,
            phase: GraphBuildPhase::Branches,
            unit,
            completed_units: nonnegative_usize(row.dag_init_counter),
            total_units: 0,
            message,
        };
    }

    let completed_units = nonnegative_usize(row.dag_processed_node_count);
    let total_units = nonnegative_usize(row.dag_discovered_node_count).max(completed_units);
    let (stage, phase, unit, message, completed_units, total_units) =
        match row.dag_finalize_phase.as_str() {
            "traversal" => (
                "dag_traversal",
                GraphBuildPhase::Entries,
                "graph_node",
                "Traversing and laying out graph nodes",
                completed_units,
                total_units,
            ),
            "ports" => (
                "edge_port_finalization",
                GraphBuildPhase::Snapshot,
                "graph_edge",
                "Finalizing graph edge ports",
                0,
                0,
            ),
            "labels" => (
                "branch_label_finalization",
                GraphBuildPhase::Snapshot,
                "source_branch",
                "Finalizing graph branch labels",
                0,
                0,
            ),
            "anchors" => shell_progress("anchors", row.shell_phase.as_deref()),
            "all" => shell_progress("all", row.shell_phase.as_deref()),
            "ready" => (
                "publish_ready",
                GraphBuildPhase::Snapshot,
                "publish_step",
                "Graph generation ready to publish",
                0,
                0,
            ),
            _ => (
                "finalization",
                GraphBuildPhase::Snapshot,
                "checkpoint_row",
                "Finalizing graph generation",
                0,
                0,
            ),
        };
    IncrementalBuildProgress {
        stage,
        phase,
        unit,
        completed_units,
        total_units,
        message,
    }
}

fn shell_progress(
    mode: &str,
    phase: Option<&str>,
) -> (
    &'static str,
    GraphBuildPhase,
    &'static str,
    &'static str,
    usize,
    usize,
) {
    match (mode, phase) {
        ("anchors", Some("nodes")) => (
            "anchors_shell_nodes",
            GraphBuildPhase::Snapshot,
            "graph_node",
            "Measuring anchor graph nodes",
            0,
            0,
        ),
        ("anchors", Some("edges")) => (
            "anchors_shell_edges",
            GraphBuildPhase::Snapshot,
            "graph_edge",
            "Measuring anchor graph edges",
            0,
            0,
        ),
        ("anchors", Some("ticks")) => (
            "anchors_shell_timeline",
            GraphBuildPhase::Snapshot,
            "timeline_sample",
            "Sampling anchor graph timeline",
            0,
            0,
        ),
        ("all", Some("nodes")) => (
            "all_shell_nodes",
            GraphBuildPhase::Snapshot,
            "graph_node",
            "Measuring full graph nodes",
            0,
            0,
        ),
        ("all", Some("edges")) => (
            "all_shell_edges",
            GraphBuildPhase::Snapshot,
            "graph_edge",
            "Measuring full graph edges",
            0,
            0,
        ),
        ("all", Some("ticks")) => (
            "all_shell_timeline",
            GraphBuildPhase::Snapshot,
            "timeline_sample",
            "Sampling full graph timeline",
            0,
            0,
        ),
        ("anchors", _) => (
            "anchors_materialization",
            GraphBuildPhase::Snapshot,
            "materialization_step",
            "Finalizing anchor graph materialization",
            0,
            0,
        ),
        _ => (
            "all_materialization",
            GraphBuildPhase::Snapshot,
            "materialization_step",
            "Finalizing full graph materialization",
            0,
            0,
        ),
    }
}

fn nonnegative_usize(value: i64) -> usize {
    value.max(0) as usize
}

fn new_incremental_build_owner_id() -> String {
    let nonce = INCREMENTAL_BUILD_OWNER_NONCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "{}-{now_ms}-{nonce}",
        std::process::id(),
        now_ms = unix_time_millis(),
    )
}

pub fn database_path(dir: impl AsRef<Path>) -> PathBuf {
    dir.as_ref().join(SQLITE_DATABASE_FILE_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{GraphBranch, GraphEdge, GraphNode};
    use crate::layout::layout_graph;
    use coco_mem::SessionState;
    use diesel::connection::{Connection, InstrumentationEvent, SimpleConnection};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[derive(Debug, QueryableByName)]
    struct IncrementalPublishStateRow {
        #[diesel(sql_type = BigInt)]
        active_generation: i64,
        #[diesel(sql_type = diesel::sql_types::Text)]
        status: String,
    }

    #[derive(Debug, QueryableByName)]
    struct TextValueRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        value: String,
    }

    #[derive(Debug, PartialEq, Eq, QueryableByName)]
    struct SourceRefreshCleanupStateRow {
        #[diesel(sql_type = Nullable<BigInt>)]
        upper_bound_refresh_id: Option<i64>,
        #[diesel(sql_type = BigInt)]
        raw_refresh_id_cursor: i64,
        #[diesel(sql_type = Nullable<BigInt>)]
        active_refresh_id: Option<i64>,
    }

    #[derive(Debug, QueryableByName)]
    struct ExplainQueryPlanRow {
        #[diesel(sql_type = Integer)]
        id: i32,
        #[diesel(sql_type = Integer)]
        parent: i32,
        #[diesel(sql_type = Integer)]
        notused: i32,
        #[diesel(sql_type = Text)]
        detail: String,
    }

    const CONSOLE_MIGRATION_UP_SQL: [&str; 23] = [
        include_str!("../../migrations/00000000000001_initial_console_graph_schema/up.sql"),
        include_str!("../../migrations/00000000000002_unique_dag_layout/up.sql"),
        include_str!("../../migrations/00000000000003_materialization_generations/up.sql"),
        include_str!("../../migrations/00000000000004_persistent_source_cache/up.sql"),
        include_str!("../../migrations/00000000000005_adaptive_incremental_build/up.sql"),
        include_str!("../../migrations/00000000000006_anchor_scope_provenance/up.sql"),
        include_str!("../../migrations/00000000000007_materialized_shell_projection/up.sql"),
        include_str!("../../migrations/00000000000008_resumable_source_refresh/up.sql"),
        include_str!("../../migrations/00000000000009_resumable_dag_build/up.sql"),
        include_str!("../../migrations/00000000000010_resumable_build_leases/up.sql"),
        include_str!("../../migrations/00000000000011_incremental_parent_requirements/up.sql"),
        include_str!("../../migrations/00000000000012_generation_source_revision/up.sql"),
        include_str!("../../migrations/00000000000013_resumable_node_projection/up.sql"),
        include_str!("../../migrations/00000000000014_resumable_branch_labels/up.sql"),
        include_str!("../../migrations/00000000000015_dag_progress_counters/up.sql"),
        include_str!("../../migrations/00000000000016_incremental_source_contributions/up.sql"),
        include_str!("../../migrations/00000000000017_delta_graph_materialization/up.sql"),
        include_str!("../../migrations/00000000000018_incremental_overlay_publication/up.sql"),
        include_str!("../../migrations/00000000000019_durable_source_mutation_cursor/up.sql"),
        include_str!("../../migrations/00000000000020_frozen_source_branch_history/up.sql"),
        include_str!("../../migrations/00000000000021_bounded_delta_removal/up.sql"),
        include_str!("../../migrations/00000000000022_bounded_dynamic_branch_scan/up.sql"),
        include_str!("../../migrations/00000000000023_resumable_refresh_cleanup/up.sql"),
    ];

    fn apply_console_migrations_through(connection: &mut SqliteConnection, version: usize) {
        connection
            .batch_execute("PRAGMA foreign_keys = ON")
            .unwrap();
        for migration in &CONSOLE_MIGRATION_UP_SQL[..version] {
            connection.batch_execute(migration).unwrap();
        }
    }

    fn node(index: usize) -> GraphNode {
        let id = format!("node-{index:04}");
        GraphNode {
            short_id: id.chars().take(8).collect(),
            id: id.clone(),
            kind: "text".to_owned(),
            role: "User".to_owned(),
            created_at: format!("time-{index:04}"),
            created_at_ns: index as i128,
            content: id.clone(),
            summary: id,
            labels: Vec::new(),
            provider_context_ids: Vec::new(),
        }
    }

    fn linear_snapshot(version: u64, mode: GraphMode, node_count: usize) -> GraphSnapshot {
        let nodes = (0..node_count).map(node).collect::<Vec<_>>();
        let edges = (1..node_count)
            .map(|index| GraphEdge {
                source: nodes[index - 1].id.clone(),
                target: nodes[index].id.clone(),
                kind: GraphEdgeKind::Primary,
            })
            .collect();
        GraphSnapshot {
            version,
            mode,
            root_id: "root".to_owned(),
            nodes,
            edges,
            branches: vec![GraphBranch {
                name: "main".to_owned(),
                head_id: format!("node-{:04}", node_count.saturating_sub(1)),
                visible_head_id: node_count
                    .checked_sub(1)
                    .map(|index| format!("node-{index:04}")),
                state: SessionState::Active,
            }],
            provider_contexts: Vec::new(),
        }
    }

    fn stored_graph(snapshot: &GraphSnapshot) -> StoredGraph {
        let layout = layout_graph(snapshot);
        let nodes = layout
            .nodes
            .iter()
            .map(|node| {
                let persisted = PersistedNode::try_from(node).unwrap();
                (
                    persisted.node_id.clone(),
                    StoredNodeRow {
                        node_id: persisted.node_id,
                        node_key: persisted.node_key,
                        node_target: persisted.node_target,
                        short_id: persisted.short_id,
                        node_kind: persisted.node_kind,
                        summary: persisted.summary,
                        labels_json: persisted.labels_json,
                        rank: persisted.rank,
                        sort_order: persisted.sort_order,
                        x: persisted.x,
                        y: persisted.y,
                        created_at: persisted.created_at,
                        created_at_ns: persisted.created_at_ns,
                        min_x: persisted.min_x,
                        min_y: persisted.min_y,
                        max_x: persisted.max_x,
                        max_y: persisted.max_y,
                    },
                )
            })
            .collect();
        let edges = layout
            .edges
            .iter()
            .map(|edge| {
                let persisted = PersistedEdge::from(edge);
                (
                    persisted.edge_key.clone(),
                    StoredEdgeRow {
                        edge_key: persisted.edge_key,
                        edge_kind: persisted.edge_kind,
                        source_id: persisted.source_id,
                        target_id: persisted.target_id,
                        source_x: persisted.source_x,
                        source_y: persisted.source_y,
                        control_1_x: persisted.control_1_x,
                        control_1_y: persisted.control_1_y,
                        control_2_x: persisted.control_2_x,
                        control_2_y: persisted.control_2_y,
                        target_x: persisted.target_x,
                        target_y: persisted.target_y,
                        min_x: persisted.min_x,
                        min_y: persisted.min_y,
                        max_x: persisted.max_x,
                        max_y: persisted.max_y,
                    },
                )
            })
            .collect();
        StoredGraph {
            materialization: Some(MaterializationRow {
                source_version: snapshot.version as i64,
                world_max_x: layout.width,
                world_max_y: layout.height,
            }),
            nodes,
            edges,
        }
    }

    fn local_policy(local_limit: usize) -> GraphLayoutPolicy {
        GraphLayoutPolicy {
            full_layout_node_limit: 0,
            full_layout_edge_limit: 0,
            local_layout_node_limit: local_limit,
            local_layout_percent: 100,
        }
    }

    fn test_connection(path: &Path) -> SqliteConnection {
        let mut connection = SqliteConnection::establish(path.to_str().unwrap()).unwrap();
        connection
            .batch_execute("PRAGMA busy_timeout = 10000; PRAGMA foreign_keys = ON")
            .unwrap();
        connection
    }

    fn replace_snapshot(
        connection: &mut SqliteConnection,
        snapshot: &GraphSnapshot,
    ) -> QueryResult<()> {
        let layout = layout_graph(snapshot);
        let nodes = layout
            .nodes
            .iter()
            .map(|node| PersistedNode::try_from(node).unwrap())
            .collect::<Vec<_>>();
        let edges = layout
            .edges
            .iter()
            .map(PersistedEdge::from)
            .collect::<Vec<_>>();
        let branches = snapshot
            .branches
            .iter()
            .map(|branch| PersistedMaterializationBranch {
                name: branch.name.clone(),
                head_id: branch.head_id.clone(),
                state_json: serde_json::to_string(&branch.state).unwrap(),
            })
            .collect::<Vec<_>>();
        connection.transaction(|connection| {
            let generation = query_active_generation(connection)?;
            delete_mode_rows(connection, generation, snapshot.mode)?;
            for node in &nodes {
                upsert_node(connection, generation, snapshot.mode, node)?;
            }
            for edge in &edges {
                upsert_edge(connection, generation, snapshot.mode, edge)?;
            }
            diesel::sql_query(
                "DELETE FROM console_graph_materialization_branches WHERE generation = ?",
            )
            .bind::<BigInt, _>(generation)
            .execute(connection)?;
            for branch in &branches {
                diesel::sql_query(
                    "INSERT INTO console_graph_materialization_branches \
                     (generation, name, head_id, state_json) VALUES (?, ?, ?, ?)",
                )
                .bind::<BigInt, _>(generation)
                .bind::<Text, _>(&branch.name)
                .bind::<Text, _>(&branch.head_id)
                .bind::<Text, _>(&branch.state_json)
                .execute(connection)?;
            }
            upsert_materialization(
                connection,
                generation,
                snapshot.mode,
                snapshot.version,
                layout.width,
                layout.height,
            )?;
            write_materialized_shell_projection(connection, generation, snapshot.mode)
        })
    }

    fn commit_snapshot_after_materialization_read(
        reader: &mut SqliteConnection,
        path: PathBuf,
        snapshot: GraphSnapshot,
    ) -> thread::JoinHandle<()> {
        let (materialization_read, wait_for_materialization) = mpsc::sync_channel(0);
        let (snapshot_committed, wait_for_snapshot) = mpsc::sync_channel(0);
        let writer = thread::spawn(move || {
            wait_for_materialization
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
            replace_snapshot(&mut test_connection(&path), &snapshot).unwrap();
            snapshot_committed.send(()).unwrap();
        });
        let mut paused = false;
        reader.set_instrumentation(move |event: InstrumentationEvent<'_>| {
            if paused {
                return;
            }
            let InstrumentationEvent::FinishQuery { query, error, .. } = event else {
                return;
            };
            if error.is_none() && query.to_string().contains("console_graph_materializations") {
                paused = true;
                materialization_read.send(()).unwrap();
                wait_for_snapshot
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap();
            }
        });
        writer
    }

    #[tokio::test]
    async fn viewport_reads_use_one_sqlite_snapshot() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        store
            .materialize_snapshot(&linear_snapshot(1, GraphMode::All, 2))
            .await
            .unwrap();
        let path = database_path(&dir);
        let mut reader = test_connection(&path);
        let writer = commit_snapshot_after_materialization_read(
            &mut reader,
            path,
            linear_snapshot(2, GraphMode::All, 3),
        );

        let viewport = read_snapshot(&mut reader, |connection| {
            query_viewport(connection, GraphMode::All, GraphViewportRequest::default())
        })
        .unwrap()
        .unwrap();
        writer.join().unwrap();

        assert_eq!(viewport.materialization.source_version, 1);
        assert_eq!(viewport.nodes.len(), 2);
        assert_eq!(viewport.edges.len(), 1);
        assert_eq!(
            query_materialization_row(&mut reader, GraphMode::All)
                .unwrap()
                .unwrap()
                .source_version,
            2
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn viewport_diff_reads_use_one_sqlite_snapshot() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        store
            .materialize_snapshot(&linear_snapshot(1, GraphMode::All, 2))
            .await
            .unwrap();
        let path = database_path(&dir);
        let mut reader = test_connection(&path);
        let writer = commit_snapshot_after_materialization_read(
            &mut reader,
            path,
            linear_snapshot(2, GraphMode::All, 3),
        );

        let (previous, current) = read_snapshot(&mut reader, |connection| {
            Ok((
                query_viewport(connection, GraphMode::All, GraphViewportRequest::default())?,
                query_viewport(
                    connection,
                    GraphMode::All,
                    GraphViewportRequest {
                        x: 1,
                        ..GraphViewportRequest::default()
                    },
                )?,
            ))
        })
        .unwrap();
        writer.join().unwrap();

        let previous = previous.unwrap();
        let current = current.unwrap();
        assert_eq!(previous.materialization.source_version, 1);
        assert_eq!(current.materialization.source_version, 1);
        assert_eq!(previous.nodes.len(), 2);
        assert_eq!(current.nodes.len(), 2);
        assert_eq!(
            query_materialization_row(&mut reader, GraphMode::All)
                .unwrap()
                .unwrap()
                .source_version,
            2
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn shell_facts_reads_use_one_sqlite_snapshot() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        store
            .materialize_snapshot(&linear_snapshot(1, GraphMode::All, 2))
            .await
            .unwrap();
        let path = database_path(&dir);
        let mut reader = test_connection(&path);
        let writer = commit_snapshot_after_materialization_read(
            &mut reader,
            path,
            linear_snapshot(2, GraphMode::All, 3),
        );

        let facts = read_snapshot(&mut reader, |connection| {
            query_shell_facts(connection, GraphMode::All)
        })
        .unwrap()
        .unwrap();
        writer.join().unwrap();

        assert_eq!(facts.materialization.source_version, 1);
        assert_eq!(facts.node_count, 2);
        assert_eq!(facts.nodes.len(), 2);
        assert_eq!(facts.edge_count, 1);
        assert_eq!(facts.branches.len(), 1);
        assert_eq!(facts.branches[0].name, "main");
        assert_eq!(facts.branches[0].head_id, "node-0001");
        assert_eq!(
            query_materialization_row(&mut reader, GraphMode::All)
                .unwrap()
                .unwrap()
                .source_version,
            2
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn shell_facts_bound_time_ticks_without_losing_node_count() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        let node_count = MATERIALIZED_SHELL_TIME_TICK_LIMIT * 2 + 17;
        store
            .materialize_snapshot(&linear_snapshot(1, GraphMode::All, node_count))
            .await
            .unwrap();

        let facts = store
            .materialized_shell_facts(GraphMode::All)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(facts.node_count, node_count);
        assert_eq!(facts.edge_count, node_count - 1);
        assert_eq!(facts.nodes.len(), MATERIALIZED_SHELL_TIME_TICK_LIMIT);
        let sample_span = MATERIALIZED_SHELL_TIME_TICK_LIMIT - 1;
        let node_span = node_count - 1;
        let expected_time_ticks = (0..node_count)
            .filter(|index| {
                let index = *index;
                index == 0
                    || index * sample_span / node_span > (index - 1) * sample_span / node_span
            })
            .map(|index| index as i128)
            .collect::<Vec<_>>();
        let actual_time_ticks = facts
            .nodes
            .iter()
            .map(|node| node.created_at_ns)
            .collect::<Vec<_>>();
        assert_eq!(actual_time_ticks, expected_time_ticks);
        assert_eq!(actual_time_ticks.first(), Some(&0));
        assert_eq!(actual_time_ticks.last().copied(), Some(node_span as i128));
        let expected_targets = facts
            .nodes
            .iter()
            .map(|node| node.node_target.clone())
            .collect::<Vec<_>>();
        let generation = store.active_generation().await.unwrap();
        store
            .with_connection(move |connection| {
                connection
                    .transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::delete(
                            console_graph_edge_routes::table
                                .filter(console_graph_edge_routes::generation.eq(generation))
                                .filter(console_graph_edge_routes::mode.eq("all")),
                        )
                        .execute(connection)?;
                        diesel::delete(
                            console_graph_node_locations::table
                                .filter(console_graph_node_locations::generation.eq(generation))
                                .filter(console_graph_node_locations::mode.eq("all")),
                        )
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("shell-projection-source-removal"),
                    })
            })
            .await
            .unwrap();

        let projected = store
            .materialized_shell_facts(GraphMode::All)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(projected.node_count, node_count);
        assert_eq!(projected.edge_count, node_count - 1);
        assert_eq!(
            projected
                .nodes
                .iter()
                .map(|node| node.created_at_ns)
                .collect::<Vec<_>>(),
            expected_time_ticks
        );
        assert_eq!(
            projected
                .nodes
                .iter()
                .map(|node| node.node_target.clone())
                .collect::<Vec<_>>(),
            expected_targets
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn payload_only_updates_keep_all_coordinates() {
        let previous_snapshot = linear_snapshot(1, GraphMode::All, 5);
        let previous = stored_graph(&previous_snapshot);
        let mut next = previous_snapshot.clone();
        next.version = 2;
        next.nodes[2].summary = "updated".to_owned();

        let plan = plan_materialization(&next, &previous, local_policy(10)).unwrap();

        assert_eq!(plan.strategy, MaterializationStrategy::PayloadOnly);
        for node in &plan.layout.nodes {
            let stored = &previous.nodes[&node.node_id];
            assert_eq!(
                node.point,
                Point {
                    x: stored.x,
                    y: stored.y
                }
            );
        }
    }

    #[test]
    fn local_updates_pin_ranks_outside_the_affected_region() {
        let previous_snapshot = linear_snapshot(1, GraphMode::All, 8);
        let mut previous = stored_graph(&previous_snapshot);
        for index in 0..=4 {
            let row = previous.nodes.get_mut(&format!("node-{index:04}")).unwrap();
            row.x += index + 1;
            row.y += (index + 1) * 7;
        }
        let mut next = previous_snapshot.clone();
        next.version = 2;
        next.nodes.push(node(8));
        next.edges.push(GraphEdge {
            source: "node-0006".to_owned(),
            target: "node-0008".to_owned(),
            kind: GraphEdgeKind::Primary,
        });

        let plan = plan_materialization(&next, &previous, local_policy(10)).unwrap();
        let full = layout_graph(&next);

        assert_eq!(plan.strategy, MaterializationStrategy::Local);
        for index in 0..=4 {
            let node_id = format!("node-{index:04}");
            let node = plan
                .layout
                .nodes
                .iter()
                .find(|node| node.node_id == node_id)
                .unwrap();
            let stored = &previous.nodes[&node_id];
            assert_eq!(
                node.point,
                Point {
                    x: stored.x,
                    y: stored.y
                }
            );
        }
        assert_eq!(
            plan.layout
                .nodes
                .iter()
                .map(|node| node.node_id.as_str())
                .collect::<BTreeSet<_>>(),
            full.nodes
                .iter()
                .map(|node| node.node_id.as_str())
                .collect()
        );
        assert_eq!(
            plan.layout
                .edges
                .iter()
                .map(|edge| (
                    edge.kind,
                    edge.source_node_id.as_str(),
                    edge.target_node_id.as_str()
                ))
                .collect::<BTreeSet<_>>(),
            full.edges
                .iter()
                .map(|edge| (
                    edge.kind,
                    edge.source_node_id.as_str(),
                    edge.target_node_id.as_str()
                ))
                .collect()
        );
    }

    #[test]
    fn large_affected_regions_fall_back_to_full_layout() {
        let previous_snapshot = linear_snapshot(1, GraphMode::All, 8);
        let previous = stored_graph(&previous_snapshot);
        let mut next = previous_snapshot.clone();
        next.version = 2;
        next.nodes.push(node(8));
        next.edges.push(GraphEdge {
            source: "node-0000".to_owned(),
            target: "node-0008".to_owned(),
            kind: GraphEdgeKind::Primary,
        });

        let plan = plan_materialization(&next, &previous, local_policy(1)).unwrap();

        assert_eq!(plan.strategy, MaterializationStrategy::Full);
        assert_eq!(plan.fallback_reason, Some("affected_region_exceeds_limit"));
    }

    #[tokio::test]
    async fn graph_modes_are_materialized_independently() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        store
            .materialize_snapshot(&linear_snapshot(3, GraphMode::All, 3))
            .await
            .unwrap();
        store
            .materialize_snapshot(&linear_snapshot(5, GraphMode::Anchors, 2))
            .await
            .unwrap();

        assert_eq!(
            store
                .latest_materialization_version(GraphMode::All)
                .await
                .unwrap(),
            Some(3)
        );
        assert_eq!(
            store
                .latest_materialization_version(GraphMode::Anchors)
                .await
                .unwrap(),
            Some(5)
        );
        assert_eq!(
            store
                .latest_viewport(GraphMode::All, GraphViewportRequest::default())
                .await
                .unwrap()
                .unwrap()
                .nodes
                .len(),
            3
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn checkpoint_generation_is_invisible_until_both_modes_are_activated() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        store
            .materialize_snapshot(&linear_snapshot(1, GraphMode::Anchors, 2))
            .await
            .unwrap();
        store
            .materialize_snapshot(&linear_snapshot(1, GraphMode::All, 2))
            .await
            .unwrap();

        let baseline_generation = store.active_generation().await.unwrap();
        let generation = store.allocate_staging_generation().await.unwrap();
        store
            .materialize_checkpoint(
                &linear_snapshot(2, GraphMode::Anchors, 3),
                baseline_generation,
                generation,
            )
            .await
            .unwrap();
        store
            .materialize_checkpoint(
                &linear_snapshot(2, GraphMode::All, 3),
                baseline_generation,
                generation,
            )
            .await
            .unwrap();

        assert_eq!(
            store.active_generation().await.unwrap(),
            baseline_generation
        );
        assert_eq!(
            store
                .latest_materialization_version(GraphMode::Anchors)
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            store
                .latest_materialization_version(GraphMode::All)
                .await
                .unwrap(),
            Some(1)
        );

        store.activate_generation(generation, 2).await.unwrap();

        assert_eq!(store.active_generation().await.unwrap(), generation);
        assert_eq!(
            store
                .latest_materialization_version(GraphMode::Anchors)
                .await
                .unwrap(),
            Some(2)
        );
        assert_eq!(
            store
                .latest_materialization_version(GraphMode::All)
                .await
                .unwrap(),
            Some(2)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn incremental_staging_is_invisible_and_supports_an_empty_mode() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        store
            .materialize_snapshot(&linear_snapshot(1, GraphMode::Anchors, 2))
            .await
            .unwrap();
        store
            .materialize_snapshot(&linear_snapshot(1, GraphMode::All, 2))
            .await
            .unwrap();

        let baseline_generation = store.active_generation().await.unwrap();
        let lease = store
            .acquire_incremental_build_lease(2)
            .await
            .unwrap()
            .unwrap();
        mark_incremental_run_ready(&store, &lease).await;
        let generation = lease.generation();
        let layout = layout_graph(&linear_snapshot(2, GraphMode::All, 3));
        store
            .write_incremental_batch(&lease, GraphMode::All, layout.nodes, layout.edges)
            .await
            .unwrap();
        store
            .finish_incremental_mode(&lease, 2, GraphMode::All)
            .await
            .unwrap();
        store
            .finish_incremental_mode(&lease, 2, GraphMode::Anchors)
            .await
            .unwrap();

        assert_eq!(
            store.active_generation().await.unwrap(),
            baseline_generation
        );
        assert_eq!(
            store
                .latest_materialization_version(GraphMode::All)
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            store
                .latest_materialization_version(GraphMode::Anchors)
                .await
                .unwrap(),
            Some(1)
        );

        store.activate_generation(generation, 2).await.unwrap();

        let all = store
            .latest_viewport(GraphMode::All, GraphViewportRequest::default())
            .await
            .unwrap()
            .unwrap();
        let anchors = store
            .latest_viewport(GraphMode::Anchors, GraphViewportRequest::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(all.version, 2);
        assert_eq!(all.nodes.len(), 3);
        assert_eq!(anchors.version, 2);
        assert_eq!(anchors.nodes, Vec::new());
        assert_eq!(anchors.edges, Vec::new());
        let all_shell = store
            .materialized_shell_facts(GraphMode::All)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(all_shell.node_count, 3);
        assert_eq!(all_shell.edge_count, 2);
        assert_eq!(all_shell.nodes.len(), 3);
        let anchors_shell = store
            .materialized_shell_facts(GraphMode::Anchors)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(anchors_shell.node_count, 0);
        assert_eq!(anchors_shell.edge_count, 0);
        assert!(anchors_shell.nodes.is_empty());
        assert_eq!(
            anchors.canvas,
            GraphCanvas {
                width: GRAPH_PADDING * 2,
                height: GRAPH_PADDING * 2,
            }
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn incremental_shell_projection_resumes_from_a_persisted_page() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        let lease = store
            .acquire_incremental_build_lease(21)
            .await
            .unwrap()
            .unwrap();
        mark_incremental_run_ready(&store, &lease).await;
        let node_count = MATERIALIZED_SHELL_TIME_TICK_LIMIT * 2 + 17;
        let layout = layout_graph(&linear_snapshot(21, GraphMode::All, node_count));
        store
            .write_incremental_batch(&lease, GraphMode::All, layout.nodes, layout.edges)
            .await
            .unwrap();
        let generation = lease.generation();
        let owner_id = lease.owner_id().to_owned();
        let lease_epoch = lease.lease_epoch();
        let path = store.path.as_ref().clone();
        store
            .with_write_connection(
                "advance shell projection once for test",
                move |connection| {
                    connection
                        .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                            require_incremental_build_fence(
                                connection,
                                generation,
                                &owner_id,
                                lease_epoch,
                            )?;
                            assert!(!advance_incremental_shell_projection(
                                connection,
                                generation,
                                GraphMode::All,
                                21,
                            )?);
                            Ok(())
                        })
                        .context(QueryGraphSnapshotStoreSnafu { path })
                },
            )
            .await
            .unwrap();
        assert!(store.pause_incremental_build(&lease).await.unwrap());
        let resumed = store
            .acquire_incremental_build_lease(21)
            .await
            .unwrap()
            .unwrap();
        assert!(resumed.is_resumed());

        store
            .finish_incremental_mode(&resumed, 21, GraphMode::All)
            .await
            .unwrap();

        let generation = resumed.generation();
        let path = store.path.as_ref().clone();
        let (phase, stored_nodes, stored_edges, ticks) = store
            .with_connection(move |connection| {
                (|| -> QueryResult<_> {
                    let phase = diesel::sql_query(
                        "SELECT phase AS value FROM console_graph_build_shell_projections \
                         WHERE run_id = ? AND mode = 'all'",
                    )
                    .bind::<BigInt, _>(generation)
                    .get_result::<TextValueRow>(connection)?
                    .value;
                    let (stored_nodes, stored_edges) = console_graph_materialization_shells::table
                        .filter(console_graph_materialization_shells::generation.eq(generation))
                        .filter(console_graph_materialization_shells::mode.eq("all"))
                        .select((
                            console_graph_materialization_shells::node_count,
                            console_graph_materialization_shells::edge_count,
                        ))
                        .first::<(i64, i64)>(connection)?;
                    let ticks = console_graph_materialization_time_ticks::table
                        .filter(console_graph_materialization_time_ticks::generation.eq(generation))
                        .filter(console_graph_materialization_time_ticks::mode.eq("all"))
                        .count()
                        .get_result::<i64>(connection)?;
                    Ok((phase, stored_nodes, stored_edges, ticks))
                })()
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        assert_eq!(phase, "complete");
        assert_eq!(stored_nodes, node_count as i64);
        assert_eq!(stored_edges, node_count.saturating_sub(1) as i64);
        assert_eq!(ticks, MATERIALIZED_SHELL_TIME_TICK_LIMIT as i64);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn incremental_port_publication_and_pruning_track_active_generation() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        let lease = store
            .acquire_incremental_build_lease(2)
            .await
            .unwrap()
            .unwrap();
        let generation = lease.generation();
        store
            .with_connection(move |connection| {
                diesel::sql_query(
                    "INSERT INTO console_graph_build_edge_ports \
                     (run_id, mode, edge_key, source_id, target_id, source_slot, target_slot, active) \
                     VALUES \
                     (?, 'all', 'edge:active', 'source', 'active', 0, 0, 1), \
                     (?, 'all', 'edge:inactive', 'source', 'inactive', 1, 1, 0)",
                )
                .bind::<BigInt, _>(generation)
                .bind::<BigInt, _>(generation)
                .execute(connection)
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("port-publication"),
                })?;
                Ok(())
            })
            .await
            .unwrap();

        store.publish_incremental_ports(&lease).await.unwrap();

        let published = store
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count FROM console_graph_edge_ports \
                     WHERE generation = ?",
                )
                .bind::<BigInt, _>(generation)
                .get_result::<CountRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("port-publication"),
                })
            })
            .await
            .unwrap();
        assert_eq!(published.count, 1);

        store.cleanup_abandoned_generations().await.unwrap();
        let retained = store
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count FROM console_graph_edge_ports \
                     WHERE generation = ?",
                )
                .bind::<BigInt, _>(generation)
                .get_result::<CountRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("port-retention"),
                })
            })
            .await
            .unwrap();
        assert_eq!(retained.count, 1);

        assert!(store.abandon_incremental_build(&lease).await.unwrap());
        store.cleanup_abandoned_generations().await.unwrap();

        let remaining = store
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count FROM console_graph_edge_ports \
                     WHERE generation = ?",
                )
                .bind::<BigInt, _>(generation)
                .get_result::<CountRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("port-pruning"),
                })
            })
            .await
            .unwrap();
        assert_eq!(remaining.count, 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn incremental_publish_atomically_switches_generation_and_run() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        let baseline_generation = store.active_generation().await.unwrap();
        let lease = store
            .acquire_incremental_build_lease(11)
            .await
            .unwrap()
            .unwrap();
        mark_incremental_run_ready(&store, &lease).await;
        let generation = lease.generation();
        store
            .finish_incremental_mode(&lease, 11, GraphMode::All)
            .await
            .unwrap();

        let error = store
            .publish_incremental_generation(&lease, 11)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            crate::Error::InvalidGraphSnapshotStoreValue { .. }
        ));
        let before = incremental_publish_state(&store, generation).await;
        assert_eq!(before.active_generation, baseline_generation);
        assert_eq!(before.status, "building");

        store
            .finish_incremental_mode(&lease, 11, GraphMode::Anchors)
            .await
            .unwrap();
        store
            .publish_incremental_generation(&lease, 11)
            .await
            .unwrap();

        let after = incremental_publish_state(&store, generation).await;
        assert_eq!(after.active_generation, generation);
        assert_eq!(after.status, "completed");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn append_publication_receipt_replays_after_all_build_cleanup() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();

        let baseline = store
            .acquire_incremental_build_lease(1)
            .await
            .unwrap()
            .unwrap();
        mark_incremental_run_ready(&store, &baseline).await;
        for mode in [GraphMode::Anchors, GraphMode::All] {
            store
                .finish_incremental_mode(&baseline, 1, mode)
                .await
                .unwrap();
        }
        let baseline_receipt = store
            .publish_incremental_generation(&baseline, 1)
            .await
            .unwrap();

        let append = store
            .acquire_incremental_build_lease(2)
            .await
            .unwrap()
            .unwrap();
        let append_run_id = append.generation();
        let append_owner_id = append.owner_id().to_owned();
        let append_lease_epoch = append.lease_epoch();
        let append_frozen_source_revision = append.frozen_source_revision();
        let baseline_generation = baseline_receipt.published_graph_generation;
        let baseline_epoch = baseline_receipt.publication_epoch;
        let baseline_source_revision = source_revision(&store).await;
        store
            .with_write_connection(
                "prepare append publication replay test",
                move |connection| {
                    (|| -> QueryResult<()> {
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_build_runs \
                             SET dag_build_kind = 'append', dag_baseline_generation = ?, \
                                 dag_baseline_source_revision = ?, \
                                 dag_baseline_publication_epoch = ?, dag_initialized = 1, \
                                 dag_finalize_phase = 'ready' \
                             WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                               AND status = 'building'",
                        )
                        .bind::<BigInt, _>(baseline_generation)
                        .bind::<BigInt, _>(baseline_source_revision)
                        .bind::<BigInt, _>(baseline_epoch)
                        .bind::<BigInt, _>(append_run_id)
                        .bind::<Text, _>(&append_owner_id)
                        .bind::<BigInt, _>(append_lease_epoch)
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        diesel::sql_query(
                            "INSERT INTO console_graph_anchor_scope_manifests ( \
                                 generation, scope_count \
                             ) VALUES (?, COALESCE(( \
                                 SELECT scope_count \
                                 FROM console_graph_anchor_scope_manifests \
                                 WHERE generation = ? \
                             ), 0))",
                        )
                        .bind::<BigInt, _>(append_run_id)
                        .bind::<BigInt, _>(baseline_generation)
                        .execute(connection)?;
                        Ok(())
                    })()
                    .context(QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("prepare-append-publication-replay"),
                    })
                },
            )
            .await
            .unwrap();
        let delta_layout = layout_graph(&linear_snapshot(2, GraphMode::All, 1));
        store
            .write_incremental_batch(
                &append,
                GraphMode::All,
                delta_layout.nodes,
                delta_layout.edges,
            )
            .await
            .unwrap();
        for mode in [GraphMode::Anchors, GraphMode::All] {
            store
                .finish_incremental_mode(&append, 2, mode)
                .await
                .unwrap();
        }

        let building_progress = store.incremental_build_progress(&append).await.unwrap();
        assert_eq!(building_progress.stage, "publish_ready");
        store
            .ensure_overlay_publication_run(&append, 2)
            .await
            .unwrap();
        loop {
            let state = store.overlay_run_state(append_run_id).await.unwrap();
            if state.phase == "ready" {
                break;
            }
            store
                .advance_overlay_preparation_page(&append)
                .await
                .unwrap();
        }
        store
            .activate_incremental_overlay(&append, 2)
            .await
            .unwrap();
        let compacting_progress = store.incremental_build_progress(&append).await.unwrap();
        assert!(compacting_progress.stage.starts_with("overlay_compaction_"));
        assert_eq!(append.phase(), IncrementalBuildLeasePhase::Building);
        assert!(store.pause_incremental_build(&append).await.unwrap());
        advance_source_identity(&store, "source-advanced-during-compaction").await;
        let resumed = store
            .acquire_incremental_build_lease(99)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resumed.generation(), append_run_id);
        assert_eq!(resumed.phase(), IncrementalBuildLeasePhase::Compacting);
        assert_eq!(resumed.target_source_version(), 2);
        assert_eq!(resumed.lease_epoch(), append.lease_epoch() + 1);
        assert_eq!(
            resumed.frozen_source_revision(),
            append_frozen_source_revision
        );

        let committed = store
            .publish_incremental_generation(&resumed, resumed.target_source_version())
            .await
            .unwrap();
        assert_eq!(committed.published_graph_generation, baseline_generation);
        assert_eq!(committed.publication_epoch, baseline_epoch + 1);
        assert_eq!(
            committed.published_source_revision,
            append_frozen_source_revision
        );
        assert!(committed.replayed);
        let committed_state = store.active_publication_state().await.unwrap();
        let committed_view = store
            .latest_viewport(GraphMode::All, GraphViewportRequest::default())
            .await
            .unwrap()
            .unwrap();

        store.cleanup_completed_build_work().await.unwrap();
        store.cleanup_abandoned_generations().await.unwrap();
        store.cleanup_obsolete_generations().await.unwrap();

        let replayed = store
            .publish_incremental_generation(&append, 2)
            .await
            .unwrap();
        assert_eq!(replayed.build_run_id, committed.build_run_id);
        assert_eq!(
            replayed.published_graph_generation,
            committed.published_graph_generation
        );
        assert_eq!(replayed.publication_epoch, committed.publication_epoch);
        assert!(replayed.replayed);
        assert_eq!(
            store.active_publication_state().await.unwrap(),
            committed_state
        );
        assert_eq!(
            store
                .latest_viewport(GraphMode::All, GraphViewportRequest::default())
                .await
                .unwrap()
                .unwrap(),
            committed_view
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn finished_build_run_metadata_cleanup_is_batched_and_resumes_after_reopen() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        let run_count = MATERIALIZATION_WRITE_BATCH_SIZE + 17;
        store
            .with_write_connection("seed finished build run cleanup test", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        for (status, first_run_id) in [
                            ("completed", 9_100_000_000_i64),
                            ("abandoned", 9_200_000_000_i64),
                        ] {
                            diesel::sql_query(
                                "WITH RECURSIVE sequence(offset) AS ( \
                                     SELECT 0 \
                                     UNION ALL \
                                     SELECT offset + 1 FROM sequence WHERE offset + 1 < ? \
                                 ) \
                                 INSERT INTO console_graph_build_runs ( \
                                     run_id, source_version, status, owner_id, lease_expires_at_ms \
                                 ) \
                                 SELECT ? + offset, 1, ?, '', 0 FROM sequence",
                            )
                            .bind::<BigInt, _>(run_count as i64)
                            .bind::<BigInt, _>(first_run_id)
                            .bind::<Text, _>(status)
                            .execute(connection)?;
                        }
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("seed-finished-build-run-cleanup"),
                    })
            })
            .await
            .unwrap();

        assert_eq!(
            store
                .delete_finished_build_run_batch("completed", false)
                .await
                .unwrap(),
            MATERIALIZATION_WRITE_BATCH_SIZE
        );
        assert_eq!(
            store
                .delete_finished_build_run_batch("abandoned", true)
                .await
                .unwrap(),
            MATERIALIZATION_WRITE_BATCH_SIZE
        );
        assert_eq!(build_run_status_count(&store, "completed").await, 17);
        assert_eq!(build_run_status_count(&store, "abandoned").await, 17);

        drop(store);
        let reopened = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        reopened.cleanup_completed_build_work().await.unwrap();
        reopened.cleanup_abandoned_generations().await.unwrap();
        assert_eq!(build_run_status_count(&reopened, "completed").await, 0);
        assert_eq!(build_run_status_count(&reopened, "abandoned").await, 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn publication_receipt_cleanup_retains_a_bounded_replay_window() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        store
            .with_write_connection(
                "seed publication receipt retention test",
                move |connection| {
                    connection
                        .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                            for build_run_id in 1..=PUBLICATION_RECEIPT_RETENTION + 7 {
                                diesel::sql_query(
                                    "INSERT INTO console_graph_build_publications ( \
                                     build_run_id, published_graph_generation, \
                                     publication_epoch, build_kind, source_version, \
                                     source_revision, committed_at \
                                 ) VALUES (?, ?, ?, 'append', ?, ?, ?)",
                                )
                                .bind::<BigInt, _>(build_run_id)
                                .bind::<BigInt, _>(1)
                                .bind::<BigInt, _>(build_run_id)
                                .bind::<BigInt, _>(build_run_id)
                                .bind::<BigInt, _>(build_run_id)
                                .bind::<Text, _>(format!("{build_run_id:020}"))
                                .execute(connection)?;
                            }
                            Ok(())
                        })
                        .context(QueryGraphSnapshotStoreSnafu {
                            path: PathBuf::from("publication-receipt-retention-seed"),
                        })
                },
            )
            .await
            .unwrap();

        store.cleanup_publication_receipts().await.unwrap();
        let (count, minimum) = store
            .with_connection(move |connection| {
                (|| -> QueryResult<_> {
                    let count = diesel::sql_query(
                        "SELECT COUNT(*) AS count FROM console_graph_build_publications",
                    )
                    .get_result::<CountRow>(connection)?
                    .count;
                    let minimum = diesel::sql_query(
                        "SELECT MIN(build_run_id) AS value \
                         FROM console_graph_build_publications",
                    )
                    .get_result::<OptionalBigIntRow>(connection)?
                    .value;
                    Ok((count, minimum))
                })()
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("publication-receipt-retention-count"),
                })
            })
            .await
            .unwrap();
        assert_eq!(count, PUBLICATION_RECEIPT_RETENTION);
        assert_eq!(minimum, Some(8));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn published_generation_tracks_the_source_revision_in_constant_time() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        assert!(!store.active_generation_matches_source().await.unwrap());
        let revision = source_revision(&store).await;
        let lease = store
            .acquire_incremental_build_lease(13)
            .await
            .unwrap()
            .unwrap();
        mark_incremental_run_ready(&store, &lease).await;
        for mode in [GraphMode::Anchors, GraphMode::All] {
            store
                .finish_incremental_mode(&lease, 13, mode)
                .await
                .unwrap();
        }

        store
            .publish_incremental_generation(&lease, 13)
            .await
            .unwrap();

        assert_eq!(
            generation_source_revision(&store, lease.generation()).await,
            Some(revision)
        );
        assert!(store.active_generation_matches_source().await.unwrap());

        advance_source_identity(&store, "published-source-changed").await;

        assert!(!store.active_generation_matches_source().await.unwrap());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn current_publication_version_advances_without_rebuilding_graph_rows() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        let lease = store
            .acquire_incremental_build_lease(1)
            .await
            .unwrap()
            .unwrap();
        mark_incremental_run_ready(&store, &lease).await;
        for mode in [GraphMode::Anchors, GraphMode::All] {
            store
                .finish_incremental_mode(&lease, 1, mode)
                .await
                .unwrap();
        }
        let publication = store
            .publish_incremental_generation(&lease, 1)
            .await
            .unwrap();

        assert!(store.advance_current_publication_version(2).await.unwrap());
        assert_eq!(
            store.active_generation().await.unwrap(),
            publication.published_graph_generation
        );
        assert_eq!(
            store
                .latest_materialization_version(GraphMode::Anchors)
                .await
                .unwrap(),
            Some(2)
        );
        assert_eq!(
            store
                .latest_materialization_version(GraphMode::All)
                .await
                .unwrap(),
            Some(2)
        );

        advance_source_identity(&store, "version-fence").await;
        assert!(!store.advance_current_publication_version(3).await.unwrap());
        assert_eq!(
            store
                .latest_materialization_version(GraphMode::All)
                .await
                .unwrap(),
            Some(2)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn frozen_full_build_publishes_after_source_identity_advances() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        let captured_revision = source_revision(&store).await;
        let lease = store
            .acquire_incremental_build_lease(14)
            .await
            .unwrap()
            .unwrap();
        mark_incremental_run_ready(&store, &lease).await;
        for mode in [GraphMode::Anchors, GraphMode::All] {
            store
                .finish_incremental_mode(&lease, 14, mode)
                .await
                .unwrap();
        }
        advance_source_identity(&store, "publish-fence").await;

        let publication = store
            .publish_incremental_generation(&lease, 14)
            .await
            .unwrap();

        assert_eq!(lease.frozen_source_revision(), captured_revision);
        assert_eq!(publication.published_source_revision, captured_revision);
        let state = incremental_publish_state(&store, lease.generation()).await;
        assert_eq!(
            state.active_generation,
            publication.published_graph_generation
        );
        assert_eq!(state.status, "completed");
        assert_eq!(
            generation_source_revision(&store, lease.generation()).await,
            Some(captured_revision)
        );
        assert!(!store.active_generation_matches_source().await.unwrap());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn build_lease_excludes_competitors_and_only_expired_staging_is_cleaned() {
        let dir = temp_dir();
        let first_store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        let second_store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        let first = first_store
            .acquire_incremental_build_lease(7)
            .await
            .unwrap()
            .unwrap();
        mark_incremental_run_ready(&first_store, &first).await;
        assert!(
            second_store
                .acquire_incremental_build_lease(7)
                .await
                .unwrap()
                .is_none()
        );
        first_store
            .finish_incremental_mode(&first, 7, GraphMode::All)
            .await
            .unwrap();

        second_store.cleanup_abandoned_generations().await.unwrap();
        assert_eq!(
            generation_materialization_count(&second_store, first.generation()).await,
            1
        );

        let expired_generation = first.generation();
        first_store
            .with_write_connection("expire build lease for test", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_build_runs SET lease_expires_at_ms = 0 \
                     WHERE run_id = ?",
                )
                .bind::<BigInt, _>(expired_generation)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("expire-build-lease"),
                })
            })
            .await
            .unwrap();
        let resumed = second_store
            .acquire_incremental_build_lease(7)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.generation(), resumed.generation());
        assert_eq!(first.lease_epoch() + 1, resumed.lease_epoch());
        assert!(
            !first_store
                .renew_incremental_build_lease(&first)
                .await
                .unwrap()
        );

        assert!(
            second_store
                .acquire_incremental_build_lease(8)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            second_store
                .renew_incremental_build_lease(&resumed)
                .await
                .unwrap()
        );
        assert!(
            second_store
                .abandon_incremental_build(&resumed)
                .await
                .unwrap()
        );

        second_store.cleanup_abandoned_generations().await.unwrap();
        assert_eq!(
            generation_materialization_count(&second_store, first.generation()).await,
            0
        );
        let second = second_store
            .acquire_incremental_build_lease(8)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(first.generation(), second.generation());
        assert!(
            second_store
                .renew_incremental_build_lease(&second)
                .await
                .unwrap()
        );
        assert!(
            second_store
                .abandon_incremental_build(&second)
                .await
                .unwrap()
        );
        second_store.cleanup_abandoned_generations().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn paused_build_lease_resumes_the_same_run_with_a_new_fencing_epoch() {
        let dir = temp_dir();
        let first_store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        let second_store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        let first = first_store
            .acquire_incremental_build_lease(9)
            .await
            .unwrap()
            .unwrap();
        mark_incremental_run_ready(&first_store, &first).await;
        assert!(!first.is_resumed());
        first_store
            .finish_incremental_mode(&first, 9, GraphMode::All)
            .await
            .unwrap();
        assert!(first_store.pause_incremental_build(&first).await.unwrap());

        let resumed = second_store
            .acquire_incremental_build_lease(9)
            .await
            .unwrap()
            .unwrap();
        assert!(resumed.is_resumed());
        assert_eq!(resumed.generation(), first.generation());
        assert_eq!(resumed.lease_epoch(), first.lease_epoch() + 1);
        assert_ne!(resumed.owner_id(), first.owner_id());
        let stale_error = first_store
            .finish_incremental_mode(&first, 9, GraphMode::Anchors)
            .await
            .unwrap_err();
        assert!(matches!(
            stale_error,
            crate::Error::QueryGraphSnapshotStore { .. }
        ));
        second_store
            .finish_incremental_mode(&resumed, 9, GraphMode::Anchors)
            .await
            .unwrap();
        assert_eq!(
            generation_materialization_count(&second_store, resumed.generation()).await,
            2
        );
        assert!(
            !first_store
                .renew_incremental_build_lease(&first)
                .await
                .unwrap()
        );
        assert!(
            second_store
                .renew_incremental_build_lease(&resumed)
                .await
                .unwrap()
        );

        assert!(
            second_store
                .abandon_incremental_build(&resumed)
                .await
                .unwrap()
        );
        second_store.cleanup_abandoned_generations().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn source_identity_change_resumes_paused_build_with_its_persisted_target() {
        let dir = temp_dir();
        let first_store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        let second_store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        let first = first_store
            .acquire_incremental_build_lease(15)
            .await
            .unwrap()
            .unwrap();
        assert!(first_store.pause_incremental_build(&first).await.unwrap());
        advance_source_identity(&first_store, "resume-fence").await;

        let resumed = second_store
            .acquire_incremental_build_lease(99)
            .await
            .unwrap()
            .unwrap();

        assert!(resumed.is_resumed());
        assert_eq!(resumed.generation(), first.generation());
        assert_eq!(resumed.target_source_version(), 15);
        assert_eq!(
            incremental_publish_state(&second_store, first.generation())
                .await
                .status,
            "building"
        );
        assert!(
            !first_store
                .renew_incremental_build_lease(&first)
                .await
                .unwrap()
        );
        assert!(
            second_store
                .abandon_incremental_build(&resumed)
                .await
                .unwrap()
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn abandoned_cleanup_removes_resumable_dag_work_before_the_run() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        let lease = store
            .acquire_incremental_build_lease(12)
            .await
            .unwrap()
            .unwrap();
        let run_id = lease.generation();
        let path = store.path.as_ref().clone();
        store
            .with_write_connection("seed abandoned DAG work for test", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "INSERT INTO console_graph_build_source_manifest \
                             (run_id, branch_name, contribution_generation, head_id, state_json) \
                             VALUES (?, 'main', 1, 'head', '{}')",
                        )
                        .bind::<BigInt, _>(run_id)
                        .execute(connection)?;
                        for index in 0..=MATERIALIZATION_WRITE_BATCH_SIZE {
                            let parent_id = format!("parent-{index}");
                            let child_id = format!("child-{index}");
                            diesel::sql_query(
                                "INSERT INTO console_graph_build_parent_expansions \
                                 (run_id, parent_id, next_child_id, complete) \
                                 VALUES (?, ?, '', 0)",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&parent_id)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_build_parent_satisfactions \
                                 (run_id, parent_id, child_id) VALUES (?, ?, ?)",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(parent_id)
                            .bind::<Text, _>(child_id)
                            .execute(connection)?;
                        }
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        assert!(store.abandon_incremental_build(&lease).await.unwrap());
        store.cleanup_abandoned_generations().await.unwrap();

        let remaining = store
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count FROM console_graph_build_runs WHERE run_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .get_result::<CountRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("abandoned-dag-cleanup"),
                })
            })
            .await
            .unwrap();
        assert_eq!(remaining.count, 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn obsolete_cleanup_keeps_the_active_and_leased_generations() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();

        let first = store
            .acquire_incremental_build_lease(1)
            .await
            .unwrap()
            .unwrap();
        mark_incremental_run_ready(&store, &first).await;
        for mode in [GraphMode::Anchors, GraphMode::All] {
            store
                .finish_incremental_mode(&first, 1, mode)
                .await
                .unwrap();
        }
        store
            .publish_incremental_generation(&first, 1)
            .await
            .unwrap();

        let second = store
            .acquire_incremental_build_lease(2)
            .await
            .unwrap()
            .unwrap();
        mark_incremental_run_ready(&store, &second).await;
        for mode in [GraphMode::Anchors, GraphMode::All] {
            store
                .finish_incremental_mode(&second, 2, mode)
                .await
                .unwrap();
        }
        store
            .publish_incremental_generation(&second, 2)
            .await
            .unwrap();

        let staging = store
            .acquire_incremental_build_lease(3)
            .await
            .unwrap()
            .unwrap();
        mark_incremental_run_ready(&store, &staging).await;
        store
            .finish_incremental_mode(&staging, 3, GraphMode::All)
            .await
            .unwrap();

        store.cleanup_obsolete_generations().await.unwrap();
        assert_eq!(
            generation_materialization_count(&store, first.generation()).await,
            0
        );
        assert_eq!(
            generation_materialization_count(&store, second.generation()).await,
            2
        );
        assert_eq!(
            generation_materialization_count(&store, staging.generation()).await,
            1
        );
        assert_eq!(generation_shell_count(&store, first.generation()).await, 0);
        assert_eq!(generation_shell_count(&store, second.generation()).await, 2);
        assert_eq!(
            generation_shell_count(&store, staging.generation()).await,
            1
        );

        assert!(store.abandon_incremental_build(&staging).await.unwrap());
        store.cleanup_abandoned_generations().await.unwrap();
        assert_eq!(
            generation_shell_count(&store, staging.generation()).await,
            0
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn incomplete_checkpoint_generation_cannot_be_activated() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        store
            .materialize_snapshot(&linear_snapshot(1, GraphMode::Anchors, 2))
            .await
            .unwrap();
        store
            .materialize_snapshot(&linear_snapshot(1, GraphMode::All, 2))
            .await
            .unwrap();

        let baseline_generation = store.active_generation().await.unwrap();
        let generation = store.allocate_staging_generation().await.unwrap();
        store
            .materialize_checkpoint(
                &linear_snapshot(2, GraphMode::All, 3),
                baseline_generation,
                generation,
            )
            .await
            .unwrap();

        let error = store.activate_generation(generation, 2).await.unwrap_err();

        assert!(matches!(
            error,
            crate::Error::InvalidGraphSnapshotStoreValue { .. }
        ));
        assert_eq!(
            store.active_generation().await.unwrap(),
            baseline_generation
        );
        assert_eq!(
            store
                .latest_materialization_version(GraphMode::Anchors)
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            store
                .latest_materialization_version(GraphMode::All)
                .await
                .unwrap(),
            Some(1)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn stale_rebuild_does_not_replace_newer_materialization() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open_with_policy(&dir, local_policy(10))
            .await
            .unwrap();
        let initial = linear_snapshot(1, GraphMode::All, 2);
        store.materialize_snapshot(&initial).await.unwrap();
        let previous = store.load_stored_graph(GraphMode::All).await.unwrap();

        let mut stale = initial.clone();
        stale.version = 2;
        stale.nodes[1].summary = "stale payload".to_owned();
        let stale_plan = plan_materialization(&stale, &previous, local_policy(10)).unwrap();
        assert_eq!(stale_plan.strategy, MaterializationStrategy::PayloadOnly);

        let mut newer = initial;
        newer.version = 3;
        newer.nodes[1].summary = "newer payload".to_owned();
        store.materialize_snapshot(&newer).await.unwrap();

        let outcome = store
            .write_materialization(
                store.active_generation().await.unwrap(),
                stale.version,
                stale.mode,
                stale_plan,
                previous,
            )
            .await
            .unwrap();

        assert_eq!(
            outcome,
            MaterializationWriteOutcome::SkippedStale { current_version: 3 }
        );
        let stored = store.load_stored_graph(GraphMode::All).await.unwrap();
        assert_eq!(stored.materialization.unwrap().source_version, 3);
        assert_eq!(stored.nodes["node-0001"].summary, "newer payload");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn changed_write_baseline_falls_back_to_full_rewrite() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open_with_policy(&dir, local_policy(10))
            .await
            .unwrap();
        let initial = linear_snapshot(1, GraphMode::All, 2);
        store.materialize_snapshot(&initial).await.unwrap();
        let previous = store.load_stored_graph(GraphMode::All).await.unwrap();

        let mut target = initial;
        target.version = 3;
        target.nodes[1].summary = "target payload".to_owned();
        let target_plan = plan_materialization(&target, &previous, local_policy(10)).unwrap();
        assert_eq!(target_plan.strategy, MaterializationStrategy::PayloadOnly);

        store
            .materialize_snapshot(&linear_snapshot(2, GraphMode::All, 3))
            .await
            .unwrap();
        let outcome = store
            .write_materialization(
                store.active_generation().await.unwrap(),
                target.version,
                target.mode,
                target_plan,
                previous,
            )
            .await
            .unwrap();

        assert_eq!(
            outcome,
            MaterializationWriteOutcome::Committed {
                strategy: MaterializationStrategy::Full,
                fallback_reason: Some("materialization_baseline_changed"),
            }
        );
        let stored = store.load_stored_graph(GraphMode::All).await.unwrap();
        assert_eq!(stored.materialization.unwrap().source_version, 3);
        assert_eq!(stored.nodes.len(), 2);
        assert_eq!(stored.nodes["node-0001"].summary, "target payload");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn failed_rebuild_keeps_the_previous_ready_version() {
        let dir = temp_dir();
        let store = ConsoleGraphSnapshotStore::open(&dir).await.unwrap();
        let initial = linear_snapshot(1, GraphMode::All, 2);
        store.materialize_snapshot(&initial).await.unwrap();
        store
            .with_connection(|connection| {
                connection
                    .batch_execute(
                        "CREATE TRIGGER reject_graph_node_insert \
                         BEFORE INSERT ON console_graph_node_locations \
                         BEGIN SELECT RAISE(ABORT, 'injected rebuild failure'); END;",
                    )
                    .context(QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("injected"),
                    })?;
                Ok(())
            })
            .await
            .unwrap();
        let mut next = initial;
        next.version = 2;
        next.nodes[1].summary = "new payload".to_owned();

        let error = store.materialize_snapshot(&next).await.unwrap_err();
        assert!(matches!(
            error,
            crate::Error::QueryGraphSnapshotStore { .. }
        ));
        assert_eq!(
            store
                .latest_materialization_version(GraphMode::All)
                .await
                .unwrap(),
            Some(1)
        );
        let viewport = store
            .latest_viewport(GraphMode::All, GraphViewportRequest::default())
            .await
            .unwrap()
            .unwrap();
        assert_ne!(viewport.nodes[1].summary, "new payload");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn v2_migration_clears_v1_derived_facts() {
        let mut connection = SqliteConnection::establish(":memory:").unwrap();
        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000001_initial_console_graph_schema/up.sql"
            ))
            .unwrap();
        connection
            .batch_execute(
                "INSERT INTO console_graph_materializations \
                 (mode, source_version, coordinate_space, world_min_x, world_min_y, world_max_x, world_max_y) \
                 VALUES ('all', 1, 'graph_layout_v1', 0, 0, 100, 100); \
                 INSERT INTO console_graph_node_locations \
                 (mode, node_key, node_id, node_target, short_id, node_kind, summary, labels_json, \
                  lane_key, lane_label, lane_y, x, y, min_x, min_y, max_x, max_y) \
                 VALUES ('all', 'node:a:1:1', 'a', 'detail-a', 'a', 'text', 'a', '[]', \
                         'lane:main', 'main', 1, 1, 1, 0, 0, 2, 2);",
            )
            .unwrap();
        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000002_unique_dag_layout/up.sql"
            ))
            .unwrap();

        let materializations = console_graph_materializations::table
            .count()
            .get_result::<i64>(&mut connection)
            .unwrap();
        let nodes = console_graph_node_locations::table
            .count()
            .get_result::<i64>(&mut connection)
            .unwrap();
        let legacy_node_columns = diesel::sql_query(
            "SELECT COUNT(*) AS count \
             FROM pragma_table_info('console_graph_node_locations') \
             WHERE name IN ('lane_key', 'lane_label', 'lane_y')",
        )
        .get_result::<CountRow>(&mut connection)
        .unwrap();
        let bezier_columns = diesel::sql_query(
            "SELECT COUNT(*) AS count \
             FROM pragma_table_info('console_graph_edge_routes') \
             WHERE name IN ('control_1_x', 'control_1_y', 'control_2_x', 'control_2_y')",
        )
        .get_result::<CountRow>(&mut connection)
        .unwrap();

        assert_eq!(materializations, 0);
        assert_eq!(nodes, 0);
        assert_eq!(legacy_node_columns.count, 0);
        assert_eq!(bezier_columns.count, 4);
    }

    #[test]
    fn v3_migration_preserves_v2_materializations_in_generation_zero() {
        let mut connection = SqliteConnection::establish(":memory:").unwrap();
        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000001_initial_console_graph_schema/up.sql"
            ))
            .unwrap();
        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000002_unique_dag_layout/up.sql"
            ))
            .unwrap();
        connection
            .batch_execute(
                "INSERT INTO console_graph_materializations \
                 (mode, source_version, coordinate_space, world_min_x, world_min_y, world_max_x, world_max_y) \
                 VALUES ('all', 7, 'graph_layout_v2', 0, 0, 100, 100); \
                 INSERT INTO console_graph_node_locations \
                 (mode, node_id, node_key, node_target, short_id, node_kind, summary, labels_json, \
                  rank, sort_order, x, y, created_at, created_at_ns, min_x, min_y, max_x, max_y) \
                 VALUES ('all', 'node-1', 'node:node-1', 'detail-node-1', 'node-1', 'text', \
                         'node-1', '[]', 0, 0, 1, 2, 'time-1', 1, 0, 1, 2, 3);",
            )
            .unwrap();

        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000003_materialization_generations/up.sql"
            ))
            .unwrap();

        assert_eq!(query_active_generation(&mut connection).unwrap(), 0);
        assert_eq!(
            console_graph_generation_state::table
                .select(console_graph_generation_state::next_generation)
                .first::<i64>(&mut connection)
                .unwrap(),
            1
        );
        assert_eq!(
            query_materialization_row_for_generation(&mut connection, 0, GraphMode::All)
                .unwrap()
                .unwrap()
                .source_version,
            7
        );
        assert_eq!(
            console_graph_node_locations::table
                .filter(console_graph_node_locations::generation.eq(0))
                .count()
                .get_result::<i64>(&mut connection)
                .unwrap(),
            1
        );

        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000007_materialized_shell_projection/up.sql"
            ))
            .unwrap();
        assert_eq!(
            console_graph_materialization_shells::table
                .filter(console_graph_materialization_shells::generation.eq(0))
                .filter(console_graph_materialization_shells::mode.eq("all"))
                .select((
                    console_graph_materialization_shells::node_count,
                    console_graph_materialization_shells::edge_count,
                ))
                .first::<(i64, i64)>(&mut connection)
                .unwrap(),
            (1, 0)
        );
        assert_eq!(
            console_graph_materialization_time_ticks::table
                .filter(console_graph_materialization_time_ticks::generation.eq(0))
                .filter(console_graph_materialization_time_ticks::mode.eq("all"))
                .select((
                    console_graph_materialization_time_ticks::sample_index,
                    console_graph_materialization_time_ticks::node_target,
                ))
                .first::<(i32, String)>(&mut connection)
                .unwrap(),
            (0, "detail-node-1".to_owned())
        );
    }

    #[test]
    fn v16_migration_indexes_bounded_recovery_queries_and_down_clears_composed_cache() {
        let mut connection = SqliteConnection::establish(":memory:").unwrap();
        apply_console_migrations_through(&mut connection, 16);

        let recovery_plans = [
            (
                "EXPLAIN QUERY PLAN \
                 SELECT target_invalidation_incarnation \
                 FROM console_graph_source_sweep_runs \
                 WHERE status = 'building' \
                 ORDER BY relation_revision, target_invalidation_incarnation, \
                          target_invalidation_version LIMIT 1",
                "console_graph_source_sweep_runs_resume_idx",
            ),
            (
                "EXPLAIN QUERY PLAN \
                 SELECT target_invalidation_incarnation \
                 FROM console_graph_source_invalidation_boundaries \
                 WHERE status = 'building' \
                 ORDER BY relation_revision, target_invalidation_incarnation, \
                          target_invalidation_version LIMIT 1",
                "console_graph_source_invalidation_boundaries_resume_idx",
            ),
            (
                "EXPLAIN QUERY PLAN \
                 SELECT rowid FROM console_graph_source_sweep_runs \
                 WHERE status = 'completed' \
                 ORDER BY updated_at, target_invalidation_incarnation, \
                          target_invalidation_version LIMIT 128",
                "console_graph_source_sweep_runs_gc_idx",
            ),
            (
                "EXPLAIN QUERY PLAN \
                 SELECT rowid FROM console_graph_source_invalidation_boundaries \
                 WHERE status = 'completed' \
                 ORDER BY updated_at, target_invalidation_incarnation, \
                          target_invalidation_version LIMIT 128",
                "console_graph_source_invalidation_boundaries_gc_idx",
            ),
        ];
        for (sql, expected_index) in recovery_plans {
            let plan = diesel::sql_query(sql)
                .load::<ExplainQueryPlanRow>(&mut connection)
                .unwrap();
            assert!(plan.iter().any(|row| row.detail.contains(expected_index)));
            assert!(
                plan.iter()
                    .all(|row| !row.detail.contains("USE TEMP B-TREE"))
            );
        }

        connection
            .batch_execute(
                "INSERT INTO console_graph_source_nodes (node_id, parent_id, node_json) \
                 VALUES ('node', '', '{}'); \
                 INSERT INTO console_graph_source_refresh_runs ( \
                     refresh_id, branch_name, target_head_id, target_state_json, status, \
                     target_contribution_generation, published_source_revision, \
                     target_invalidation_version, target_invalidation_incarnation, \
                     relation_revision \
                 ) VALUES (1, 'main', 'node', '{}', 'published', 7, 1, 1, 'test', 1); \
                 INSERT INTO console_graph_source_branches ( \
                     name, head_id, state_json, contribution_generation \
                 ) VALUES ('main', 'node', '{}', 7); \
                 INSERT INTO console_graph_source_branch_publications ( \
                     branch_name, target_contribution_generation, source_revision \
                 ) VALUES ('main', 7, 1); \
                 INSERT INTO console_graph_source_branch_nodes ( \
                     branch_name, contribution_generation, node_id \
                 ) VALUES ('main', 1, 'node'); \
                 INSERT INTO console_graph_source_refresh_queue ( \
                     refresh_id, branch_name, node_id, traversal_kind, processed \
                 ) VALUES (1, 'main', 'node', 'primary', 1); \
                 INSERT INTO console_graph_source_child_rechecks ( \
                     branch_name, contribution_generation, node_id, traversal_kind \
                 ) VALUES ('main', 1, 'node', 'primary');",
            )
            .unwrap();
        assert_eq!(
            diesel::sql_query(
                "SELECT COUNT(*) AS count FROM console_graph_source_current_branch_nodes",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            1
        );

        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000016_incremental_source_contributions/down.sql"
            ))
            .unwrap();
        for table in [
            "console_graph_source_branches",
            "console_graph_source_branch_nodes",
            "console_graph_source_refresh_queue",
            "console_graph_source_refresh_runs",
            "console_graph_source_child_rechecks",
        ] {
            let count = diesel::sql_query(format!("SELECT COUNT(*) AS count FROM {table}"))
                .get_result::<CountRow>(&mut connection)
                .unwrap();
            assert_eq!(count.count, 0, "{table} should be cleared");
        }
        assert_eq!(
            diesel::sql_query("SELECT COUNT(*) AS count FROM pragma_foreign_key_check")
                .get_result::<CountRow>(&mut connection)
                .unwrap()
                .count,
            0
        );
    }

    #[test]
    fn v18_down_discards_incomplete_overlays_and_preserves_published_generations() {
        let mut connection = SqliteConnection::establish(":memory:").unwrap();
        apply_console_migrations_through(&mut connection, 18);
        connection
            .batch_execute(
                "INSERT INTO console_graph_build_runs ( \
                     run_id, source_version, status, owner_id, lease_expires_at_ms \
                 ) VALUES \
                     (42, 2, 'building', 'test', 100), \
                     (43, 3, 'completed', 'test', 0); \
                 INSERT INTO console_graph_overlay_runs ( \
                     run_id, base_generation, baseline_source_revision, \
                     baseline_publication_epoch, target_source_version, \
                     target_source_revision, target_publication_epoch, status, phase, \
                     completed_at \
                 ) VALUES \
                     (42, 0, 1, 1, 2, 2, 2, 'compacting', 'nodes', NULL), \
                     (43, 0, 1, 1, 3, 3, 3, 'completed', 'done', \
                      '2026-01-01T00:00:00Z'); \
                 UPDATE console_graph_generation_state \
                 SET active_generation = 43, next_generation = 44, active_overlay_run_id = 42 \
                 WHERE id = 1; \
                 INSERT INTO console_graph_materializations ( \
                     generation, mode, source_version, coordinate_space, \
                     world_min_x, world_min_y, world_max_x, world_max_y \
                 ) VALUES \
                     (42, 'all', 2, 'test', 0, 0, 1, 1), \
                     (43, 'all', 3, 'test', 0, 0, 1, 1); \
                 INSERT INTO console_graph_generation_source_revisions \
                     (generation, source_revision) VALUES (42, 2), (43, 3); \
                 INSERT INTO console_graph_generation_delta_capabilities \
                     (generation, delta_compatible, publication_epoch) \
                     VALUES (42, 1, 2), (43, 1, 3); \
                 INSERT INTO console_graph_build_publications ( \
                     build_run_id, published_graph_generation, publication_epoch, \
                     build_kind, source_version, source_revision \
                 ) VALUES \
                     (42, 42, 2, 'append', 2, 2), \
                     (43, 43, 3, 'append', 3, 3);",
            )
            .unwrap();

        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000018_incremental_overlay_publication/down.sql"
            ))
            .unwrap();

        let abandoned = diesel::sql_query(
            "SELECT COUNT(*) AS count FROM console_graph_build_runs \
             WHERE run_id = 42 AND status = 'abandoned' AND lease_expires_at_ms = 0",
        )
        .get_result::<CountRow>(&mut connection)
        .unwrap();
        assert_eq!(abandoned.count, 1);
        for table in [
            "console_graph_materializations",
            "console_graph_generation_source_revisions",
            "console_graph_generation_delta_capabilities",
        ] {
            let incomplete = diesel::sql_query(format!(
                "SELECT COUNT(*) AS count FROM {table} WHERE generation = 42"
            ))
            .get_result::<CountRow>(&mut connection)
            .unwrap();
            let published = diesel::sql_query(format!(
                "SELECT COUNT(*) AS count FROM {table} WHERE generation = 43"
            ))
            .get_result::<CountRow>(&mut connection)
            .unwrap();
            assert_eq!(incomplete.count, 0, "{table} retained incomplete overlay");
            assert_eq!(published.count, 1, "{table} lost published generation");
        }
        assert_eq!(
            diesel::sql_query(
                "SELECT COUNT(*) AS count FROM console_graph_build_publications \
                 WHERE build_run_id = 42",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            0
        );
        assert_eq!(
            diesel::sql_query(
                "SELECT COUNT(*) AS count FROM console_graph_build_publications \
                 WHERE build_run_id = 43",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            1
        );
        assert_eq!(
            diesel::sql_query("SELECT COUNT(*) AS count FROM pragma_foreign_key_check")
                .get_result::<CountRow>(&mut connection)
                .unwrap()
                .count,
            0
        );
    }

    #[test]
    fn v20_migration_freezes_current_branches_resets_legacy_manifest_and_cleans_down() {
        let mut connection = SqliteConnection::establish(":memory:").unwrap();
        apply_console_migrations_through(&mut connection, 19);
        connection
            .batch_execute(
                "INSERT INTO console_graph_source_refresh_runs ( \
                     refresh_id, branch_name, target_head_id, target_state_json, status, \
                     target_contribution_generation, published_source_revision \
                 ) VALUES (77, 'main', 'head-77', '{}', 'published', 77, 7); \
                 INSERT INTO console_graph_source_branches ( \
                     name, head_id, state_json, contribution_generation \
                 ) VALUES ('main', 'head-77', '{}', 77); \
                 INSERT INTO console_graph_source_branch_publications ( \
                     branch_name, target_contribution_generation, source_revision \
                 ) VALUES ('main', 77, 7); \
                 UPDATE console_graph_source_identity SET revision = 7 WHERE id = 1; \
                 INSERT INTO console_graph_build_runs ( \
                     run_id, source_version, status, owner_id, lease_expires_at_ms, \
                     dag_initialized, dag_init_phase, dag_init_row_cursor, \
                     dag_init_text_cursor, dag_init_text_cursor_secondary, dag_init_counter \
                 ) VALUES (77, 7, 'paused', 'migration-test', 0, 0, 'manifest_copy', \
                           11, 'main', 'secondary', 12);",
            )
            .unwrap();

        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000020_frozen_source_branch_history/up.sql"
            ))
            .unwrap();

        let frozen_head = diesel::sql_query(
            "SELECT head_id AS value FROM console_graph_source_branch_history \
             WHERE branch_name = 'main' AND source_revision <= 7 \
             ORDER BY source_revision DESC LIMIT 1",
        )
        .get_result::<TextValueRow>(&mut connection)
        .unwrap();
        assert_eq!(frozen_head.value, "head-77");
        assert_eq!(
            diesel::sql_query(
                "SELECT COUNT(*) AS count FROM console_graph_source_branch_names \
                 WHERE branch_name = 'main' AND first_source_revision = 7",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            1
        );
        assert_eq!(
            diesel::sql_query(
                "SELECT dag_init_phase AS value FROM console_graph_build_runs \
                 WHERE run_id = 77",
            )
            .get_result::<TextValueRow>(&mut connection)
            .unwrap()
            .value,
            "full_reset"
        );
        assert_eq!(
            diesel::sql_query(
                "SELECT dag_init_row_cursor + dag_init_counter + \
                        length(dag_init_text_cursor) + \
                        length(dag_init_text_cursor_secondary) AS count \
                 FROM console_graph_build_runs WHERE run_id = 77",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            0
        );

        connection
            .batch_execute(
                "UPDATE console_graph_build_runs \
                 SET dag_init_phase = 'manifest_refresh_copy', \
                     dag_init_row_cursor = 9, dag_init_text_cursor = 'cursor', \
                     dag_init_text_cursor_secondary = 'secondary', dag_init_counter = 10 \
                 WHERE run_id = 77;",
            )
            .unwrap();
        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000020_frozen_source_branch_history/down.sql"
            ))
            .unwrap();

        assert_eq!(
            diesel::sql_query(
                "SELECT dag_init_phase AS value FROM console_graph_build_runs \
                 WHERE run_id = 77",
            )
            .get_result::<TextValueRow>(&mut connection)
            .unwrap()
            .value,
            "full_reset"
        );
        for object in [
            "console_graph_source_branch_history",
            "console_graph_source_branch_history_revision_idx",
            "console_graph_source_branch_names",
            "console_graph_source_branch_names_revision_idx",
            "console_graph_source_refresh_runs_manifest_idx",
        ] {
            assert_eq!(
                diesel::sql_query("SELECT COUNT(*) AS count FROM sqlite_master WHERE name = ?",)
                    .bind::<Text, _>(object)
                    .get_result::<CountRow>(&mut connection)
                    .unwrap()
                    .count,
                0,
                "{object} should be removed by the down migration"
            );
        }
        assert_eq!(
            diesel::sql_query("SELECT COUNT(*) AS count FROM pragma_foreign_key_check")
                .get_result::<CountRow>(&mut connection)
                .unwrap()
                .count,
            0
        );
    }

    #[test]
    fn v21_migration_initializes_delta_removal_cursor_and_cleans_down() {
        let mut connection = SqliteConnection::establish(":memory:").unwrap();
        apply_console_migrations_through(&mut connection, 20);
        connection
            .batch_execute(
                "INSERT INTO console_graph_build_runs ( \
                     run_id, source_version, status, owner_id, lease_expires_at_ms \
                 ) VALUES (21, 1, 'paused', 'migration-test', 0); \
                 INSERT INTO console_graph_build_changed_branches ( \
                     run_id, branch_name, change_kind, removal_cursor, removal_complete \
                 ) VALUES \
                     (21, 'incomplete', 'replace', 'legacy-node', 0), \
                     (21, 'completed', 'append', 'completed-node', 1);",
            )
            .unwrap();

        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000021_bounded_delta_removal/up.sql"
            ))
            .unwrap();

        assert_eq!(
            diesel::sql_query(
                "SELECT COUNT(*) AS count \
                 FROM pragma_table_info('console_graph_build_changed_branches') \
                 WHERE (name = 'removal_refresh_id_cursor' \
                        AND \"notnull\" = 1 AND dflt_value = '-1') \
                    OR (name = 'removal_refresh_id_upper_bound' \
                        AND \"notnull\" = 1 AND dflt_value = '-1') \
                    OR (name = 'removal_bound_frozen' \
                        AND \"notnull\" = 1 AND dflt_value = '0')",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            3
        );
        assert_eq!(
            diesel::sql_query(
                "SELECT removal_refresh_id_cursor + \
                        removal_refresh_id_upper_bound + removal_bound_frozen AS count \
                 FROM console_graph_build_changed_branches \
                 WHERE run_id = 21 AND branch_name = 'incomplete'",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            -2
        );
        assert_eq!(
            diesel::sql_query(
                "SELECT removal_cursor AS value \
                 FROM console_graph_build_changed_branches \
                 WHERE run_id = 21 AND branch_name = 'incomplete'",
            )
            .get_result::<TextValueRow>(&mut connection)
            .unwrap()
            .value,
            ""
        );
        assert_eq!(
            diesel::sql_query(
                "SELECT removal_cursor AS value \
                 FROM console_graph_build_changed_branches \
                 WHERE run_id = 21 AND branch_name = 'completed'",
            )
            .get_result::<TextValueRow>(&mut connection)
            .unwrap()
            .value,
            "completed-node"
        );
        connection
            .batch_execute(
                "INSERT INTO console_graph_build_changed_branches ( \
                     run_id, branch_name, change_kind \
                 ) VALUES (21, 'fresh', 'append'); \
                 UPDATE console_graph_build_changed_branches \
                 SET removal_refresh_id_cursor = 77, \
                     removal_refresh_id_upper_bound = 99, removal_bound_frozen = 1 \
                 WHERE run_id = 21 AND branch_name = 'incomplete';",
            )
            .unwrap();
        assert_eq!(
            diesel::sql_query(
                "SELECT removal_refresh_id_cursor + \
                        removal_refresh_id_upper_bound + removal_bound_frozen AS count \
                 FROM console_graph_build_changed_branches \
                 WHERE run_id = 21 AND branch_name = 'fresh'",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            -2
        );

        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000021_bounded_delta_removal/down.sql"
            ))
            .unwrap();
        assert_eq!(
            diesel::sql_query(
                "SELECT COUNT(*) AS count FROM pragma_table_info( \
                     'console_graph_build_changed_branches' \
                 ) WHERE name IN ( \
                     'removal_refresh_id_cursor', \
                     'removal_refresh_id_upper_bound', \
                     'removal_bound_frozen' \
                 )",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            0
        );
        assert_eq!(
            diesel::sql_query(
                "SELECT removal_cursor AS value \
                 FROM console_graph_build_changed_branches \
                 WHERE run_id = 21 AND branch_name = 'incomplete'",
            )
            .get_result::<TextValueRow>(&mut connection)
            .unwrap()
            .value,
            ""
        );
        assert_eq!(
            diesel::sql_query("SELECT COUNT(*) AS count FROM pragma_foreign_key_check")
                .get_result::<CountRow>(&mut connection)
                .unwrap()
                .count,
            0
        );
    }

    #[test]
    fn v22_migration_creates_resumable_dynamic_scans_and_cleans_down() {
        let mut connection = SqliteConnection::establish(":memory:").unwrap();
        apply_console_migrations_through(&mut connection, 21);
        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000022_bounded_dynamic_branch_scan/up.sql"
            ))
            .unwrap();
        connection
            .batch_execute(
                "INSERT INTO console_graph_source_dynamic_branch_scans ( \
                     scan_kind, request_key, source_revision, raw_refresh_id_upper_bound, \
                     targeted_limit, status, owner_id, lease_epoch \
                 ) VALUES ('affected', 'migration-v22', 7, 11, 128, 'building', 'owner', 1); \
                 INSERT INTO console_graph_source_dynamic_branch_scan_origins ( \
                     scan_id, branch_name \
                 ) VALUES (1, 'origin'); \
                 INSERT INTO console_graph_source_dynamic_branch_scan_results ( \
                     scan_id, branch_name \
                 ) VALUES (1, 'peer'); \
                 DELETE FROM console_graph_source_dynamic_branch_scans WHERE scan_id = 1;",
            )
            .unwrap();
        for table in [
            "console_graph_source_dynamic_branch_scan_origins",
            "console_graph_source_dynamic_branch_scan_results",
        ] {
            assert_eq!(
                diesel::sql_query(format!("SELECT COUNT(*) AS count FROM {table}"))
                    .get_result::<CountRow>(&mut connection)
                    .unwrap()
                    .count,
                0
            );
        }
        for object in [
            "console_graph_source_child_rechecks_node_raw_idx",
            "console_graph_source_refresh_runs_published_raw_upper_idx",
            "console_graph_source_dynamic_branch_scans_mutation_idx",
            "console_graph_source_dynamic_branch_scans_retention_idx",
            "console_graph_source_dynamic_branch_scans_active_retention_idx",
        ] {
            assert_eq!(
                diesel::sql_query("SELECT COUNT(*) AS count FROM sqlite_master WHERE name = ?",)
                    .bind::<Text, _>(object)
                    .get_result::<CountRow>(&mut connection)
                    .unwrap()
                    .count,
                1,
                "{object} should be created by the migration"
            );
        }

        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000022_bounded_dynamic_branch_scan/down.sql"
            ))
            .unwrap();
        for object in [
            "console_graph_source_dynamic_branch_scans",
            "console_graph_source_dynamic_branch_scan_origins",
            "console_graph_source_dynamic_branch_scan_results",
            "console_graph_source_child_rechecks_node_raw_idx",
            "console_graph_source_refresh_runs_published_raw_upper_idx",
        ] {
            assert_eq!(
                diesel::sql_query("SELECT COUNT(*) AS count FROM sqlite_master WHERE name = ?",)
                    .bind::<Text, _>(object)
                    .get_result::<CountRow>(&mut connection)
                    .unwrap()
                    .count,
                0,
                "{object} should be removed by the down migration"
            );
        }
        assert_eq!(
            diesel::sql_query("SELECT COUNT(*) AS count FROM pragma_foreign_key_check")
                .get_result::<CountRow>(&mut connection)
                .unwrap()
                .count,
            0
        );
    }

    #[test]
    fn v23_migration_preserves_refresh_runs_and_cleans_down() {
        let mut connection = SqliteConnection::establish(":memory:").unwrap();
        apply_console_migrations_through(&mut connection, 22);
        connection
            .batch_execute(
                "INSERT INTO console_graph_source_refresh_runs ( \
                 refresh_id, branch_name, target_head_id, target_state_json, status, \
                     target_contribution_generation \
                 ) VALUES (2301, 'migration-v23', 'head-v23', '{}', 'superseded', 2301); \
                 INSERT INTO console_graph_source_refresh_dirty_seeds ( \
                     refresh_id, node_id \
                 ) VALUES (2301, 'migration-v23-node');",
            )
            .unwrap();

        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000023_resumable_refresh_cleanup/up.sql"
            ))
            .unwrap();

        assert_eq!(
            diesel::sql_query(
                "SELECT upper_bound_refresh_id, raw_refresh_id_cursor, active_refresh_id \
                 FROM console_graph_source_refresh_cleanup_state WHERE id = 1",
            )
            .get_result::<SourceRefreshCleanupStateRow>(&mut connection)
            .unwrap(),
            SourceRefreshCleanupStateRow {
                upper_bound_refresh_id: None,
                raw_refresh_id_cursor: 0,
                active_refresh_id: None,
            }
        );
        assert_eq!(
            diesel::sql_query(
                "SELECT COUNT(*) AS count FROM sqlite_schema \
                 WHERE type = 'index' AND name IN ( \
                     'console_graph_source_branch_change_journal_refresh_idx', \
                     'console_graph_source_dynamic_branch_scans_protection_idx', \
                     'console_graph_materialization_branches_contribution_idx' \
                 )",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            3
        );
        assert_eq!(
            diesel::sql_query(
                "SELECT COUNT(*) AS count \
                 FROM pragma_foreign_key_list('console_graph_source_refresh_dirty_seeds')",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            0
        );
        assert_eq!(
            diesel::sql_query(
                "SELECT COUNT(*) AS count FROM console_graph_source_refresh_dirty_seeds \
                 WHERE refresh_id = 2301",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            1
        );

        connection
            .batch_execute(
                "UPDATE console_graph_source_refresh_cleanup_state \
                 SET upper_bound_refresh_id = 2301, raw_refresh_id_cursor = 2301, \
                     active_refresh_id = 2301 \
                 WHERE id = 1;",
            )
            .unwrap();
        assert_eq!(
            diesel::sql_query(
                "SELECT upper_bound_refresh_id, raw_refresh_id_cursor, active_refresh_id \
                 FROM console_graph_source_refresh_cleanup_state WHERE id = 1",
            )
            .get_result::<SourceRefreshCleanupStateRow>(&mut connection)
            .unwrap(),
            SourceRefreshCleanupStateRow {
                upper_bound_refresh_id: Some(2301),
                raw_refresh_id_cursor: 2301,
                active_refresh_id: Some(2301),
            }
        );

        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000023_resumable_refresh_cleanup/down.sql"
            ))
            .unwrap();
        assert_eq!(
            diesel::sql_query(
                "SELECT COUNT(*) AS count FROM sqlite_schema WHERE name IN ( \
                     'console_graph_source_refresh_cleanup_state', \
                     'console_graph_source_branch_change_journal_refresh_idx', \
                     'console_graph_source_dynamic_branch_scans_protection_idx', \
                     'console_graph_materialization_branches_contribution_idx' \
                 )",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            0
        );
        assert_eq!(
            diesel::sql_query(
                "SELECT COUNT(*) AS count FROM console_graph_source_refresh_runs \
                 WHERE refresh_id = 2301",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            1
        );
        assert_eq!(
            diesel::sql_query(
                "SELECT COUNT(*) AS count \
                 FROM pragma_foreign_key_list('console_graph_source_refresh_dirty_seeds') \
                 WHERE \"table\" = 'console_graph_source_refresh_runs' \
                   AND \"from\" = 'refresh_id' AND \"on_delete\" = 'CASCADE'",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            1
        );
        assert_eq!(
            diesel::sql_query(
                "SELECT COUNT(*) AS count FROM console_graph_source_refresh_dirty_seeds \
                 WHERE refresh_id = 2301",
            )
            .get_result::<CountRow>(&mut connection)
            .unwrap()
            .count,
            1
        );
        assert_eq!(
            diesel::sql_query("SELECT COUNT(*) AS count FROM pragma_foreign_key_check")
                .get_result::<CountRow>(&mut connection)
                .unwrap()
                .count,
            0
        );
    }

    async fn advance_source_identity(store: &ConsoleGraphSnapshotStore, branch_name: &str) {
        let branch_name = branch_name.to_owned();
        let head_id = format!("head-{branch_name}");
        store
            .with_write_connection("advance source identity for test", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branches ( \
                                 name, head_id, state_json, contribution_generation \
                             ) VALUES (?, ?, '{}', 1)",
                        )
                        .bind::<Text, _>(branch_name)
                        .bind::<Text, _>(head_id)
                        .execute(connection)?;
                        diesel::sql_query(
                            "UPDATE console_graph_source_identity SET revision = revision + 1 \
                             WHERE id = 1",
                        )
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("advance-source-identity"),
                    })
            })
            .await
            .unwrap();
    }

    async fn source_revision(store: &ConsoleGraphSnapshotStore) -> i64 {
        store
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT revision AS source_revision \
                     FROM console_graph_source_identity WHERE id = 1",
                )
                .get_result::<SourceRevisionRow>(connection)
                .map(|row| row.source_revision)
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("source-revision"),
                })
            })
            .await
            .unwrap()
    }

    async fn generation_source_revision(
        store: &ConsoleGraphSnapshotStore,
        generation: i64,
    ) -> Option<i64> {
        store
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT source_revision FROM console_graph_generation_source_revisions \
                     WHERE generation = ?",
                )
                .bind::<BigInt, _>(generation)
                .get_result::<SourceRevisionRow>(connection)
                .optional()
                .map(|row| row.map(|row| row.source_revision))
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("generation-source-revision"),
                })
            })
            .await
            .unwrap()
    }

    async fn mark_incremental_run_ready(
        store: &ConsoleGraphSnapshotStore,
        lease: &IncrementalBuildLease,
    ) {
        let run_id = lease.generation();
        let owner_id = lease.owner_id().to_owned();
        let lease_epoch = lease.lease_epoch();
        store
            .with_write_connection("mark incremental run ready for test", move |connection| {
                let updated = diesel::sql_query(
                    "UPDATE console_graph_build_runs \
                     SET dag_build_kind = 'full', dag_initialized = 1, \
                         dag_finalize_phase = 'ready' \
                     WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                       AND status = 'building'",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(owner_id)
                .bind::<BigInt, _>(lease_epoch)
                .execute(connection)
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("mark-incremental-run-ready"),
                })?;
                assert_eq!(updated, 1);
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn incremental_publish_state(
        store: &ConsoleGraphSnapshotStore,
        generation: i64,
    ) -> IncrementalPublishStateRow {
        store
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT generation_state.active_generation AS active_generation, \
                            runs.status AS status \
                     FROM console_graph_generation_state AS generation_state \
                     INNER JOIN console_graph_build_runs AS runs ON runs.run_id = ? \
                     WHERE generation_state.id = 1",
                )
                .bind::<BigInt, _>(generation)
                .get_result::<IncrementalPublishStateRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("incremental-publish-state"),
                })
            })
            .await
            .unwrap()
    }

    async fn generation_materialization_count(
        store: &ConsoleGraphSnapshotStore,
        generation: i64,
    ) -> i64 {
        store
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count FROM console_graph_materializations \
                     WHERE generation = ?",
                )
                .bind::<BigInt, _>(generation)
                .get_result::<CountRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("generation-materialization-count"),
                })
            })
            .await
            .unwrap()
            .count
    }

    async fn generation_shell_count(store: &ConsoleGraphSnapshotStore, generation: i64) -> i64 {
        store
            .with_connection(move |connection| {
                console_graph_materialization_shells::table
                    .filter(console_graph_materialization_shells::generation.eq(generation))
                    .count()
                    .get_result::<i64>(connection)
                    .context(QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("generation-shell-count"),
                    })
            })
            .await
            .unwrap()
    }

    async fn build_run_status_count(
        store: &ConsoleGraphSnapshotStore,
        status: &'static str,
    ) -> i64 {
        store
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count FROM console_graph_build_runs WHERE status = ?",
                )
                .bind::<Text, _>(status)
                .get_result::<CountRow>(connection)
                .map(|row| row.count)
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("build-run-status-count"),
                })
            })
            .await
            .unwrap()
    }

    fn temp_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "coco-console-layout-v2-{}-{nonce}-{counter}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
