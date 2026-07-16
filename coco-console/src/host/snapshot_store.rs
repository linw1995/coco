use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use diesel::prelude::*;
#[cfg(test)]
use diesel::sql_types::BigInt;
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
use crate::graph::{GraphEdgeKind, GraphMode, GraphSnapshot};
use crate::host::api::{GraphViewportDiffRequest, GraphViewportRequest};
use crate::layout::{
    GraphLayout, GraphLayoutEdge, GraphLayoutNode, LayoutHint, diff_graph_viewport_responses,
    edge_bounds, edge_key, node_bounds, node_key, try_graph_ranks, try_layout_graph_with_hints,
};
use crate::schema::{
    console_graph_edge_routes, console_graph_generation_state, console_graph_materializations,
    console_graph_node_locations,
};

const SQLITE_DATABASE_FILE_NAME: &str = "console-graph.sqlite3";
const SQLITE_POOL_MAX_SIZE: u32 = 4;
const MATERIALIZATION_WRITE_BATCH_SIZE: usize = 128;
const COORDINATE_SPACE: &str = "graph_layout_v2";
const CONSOLE_GRAPH_MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaterializationStrategy {
    Full,
    Local,
    PayloadOnly,
}

impl MaterializationStrategy {
    fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Local => "local",
            Self::PayloadOnly => "payload_only",
        }
    }
}

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

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = console_graph_materializations)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct MaterializationRow {
    pub source_version: i64,
    pub world_max_x: i32,
    pub world_max_y: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, PartialEq, Eq)]
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

#[derive(Debug, Clone, Queryable, Selectable, PartialEq, Eq)]
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

#[cfg(test)]
#[derive(Debug, QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

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
    nodes: Vec<StoredNodeRow>,
    edge_count: i64,
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
    pub nodes: Vec<MaterializedGraphShellNode>,
    pub edge_count: usize,
}

#[derive(Clone, Debug)]
pub struct MaterializedGraphShellNode {
    pub node_target: String,
    pub point: Point,
    pub created_at: String,
    pub created_at_ns: i128,
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

    async fn ensure_schema(&self) -> crate::Result<()> {
        let path = self.path.as_ref().clone();
        self.with_connection(move |connection| {
            connection
                .run_pending_migrations(CONSOLE_GRAPH_MIGRATIONS)
                .map(|_| ())
                .context(crate::error::MigrateGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    pub(crate) async fn active_generation(&self) -> crate::Result<i64> {
        let path = self.path.as_ref().clone();
        self.with_connection(move |connection| {
            query_active_generation(connection).context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    pub(crate) async fn allocate_staging_generation(&self) -> crate::Result<i64> {
        let path = self.path.as_ref().clone();
        self.with_connection(move |connection| {
            connection
                .transaction::<_, diesel::result::Error, _>(|connection| {
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

    pub(crate) async fn prune_inactive_generations(&self) -> crate::Result<()> {
        let active_generation = self.active_generation().await?;
        loop {
            let path = self.path.as_ref().clone();
            let deleted = self
                .with_connection(move |connection| {
                    connection
                        .transaction::<_, diesel::result::Error, _>(|connection| {
                            let keys = console_graph_node_locations::table
                                .filter(
                                    console_graph_node_locations::generation.ne(active_generation),
                                )
                                .select((
                                    console_graph_node_locations::generation,
                                    console_graph_node_locations::mode,
                                    console_graph_node_locations::node_id,
                                ))
                                .limit(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
                                .load::<(i64, String, String)>(connection)?;
                            for (generation, mode, node_id) in &keys {
                                diesel::delete(
                                    console_graph_node_locations::table
                                        .filter(
                                            console_graph_node_locations::generation.eq(generation),
                                        )
                                        .filter(console_graph_node_locations::mode.eq(mode))
                                        .filter(console_graph_node_locations::node_id.eq(node_id)),
                                )
                                .execute(connection)?;
                            }
                            Ok(keys.len())
                        })
                        .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
            if deleted == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        loop {
            let path = self.path.as_ref().clone();
            let deleted = self
                .with_connection(move |connection| {
                    connection
                        .transaction::<_, diesel::result::Error, _>(|connection| {
                            let keys = console_graph_edge_routes::table
                                .filter(console_graph_edge_routes::generation.ne(active_generation))
                                .select((
                                    console_graph_edge_routes::generation,
                                    console_graph_edge_routes::mode,
                                    console_graph_edge_routes::edge_key,
                                ))
                                .limit(MATERIALIZATION_WRITE_BATCH_SIZE as i64)
                                .load::<(i64, String, String)>(connection)?;
                            for (generation, mode, edge_key) in &keys {
                                diesel::delete(
                                    console_graph_edge_routes::table
                                        .filter(
                                            console_graph_edge_routes::generation.eq(generation),
                                        )
                                        .filter(console_graph_edge_routes::mode.eq(mode))
                                        .filter(console_graph_edge_routes::edge_key.eq(edge_key)),
                                )
                                .execute(connection)?;
                            }
                            Ok(keys.len())
                        })
                        .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
            if deleted == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let path = self.path.as_ref().clone();
        self.with_connection(move |connection| {
            diesel::delete(
                console_graph_materializations::table
                    .filter(console_graph_materializations::generation.ne(active_generation)),
            )
            .execute(connection)
            .map(|_| ())
            .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await
    }

    pub(crate) async fn activate_generation(
        &self,
        generation: i64,
        source_version: u64,
    ) -> crate::Result<()> {
        let path = self.path.as_ref().clone();
        let expected_version = source_version.min(i64::MAX as u64) as i64;
        let complete = self
            .with_connection(move |connection| {
                connection
                    .transaction::<_, diesel::result::Error, _>(|connection| {
                        let modes = console_graph_materializations::table
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
                let generation = query_active_generation(connection)?;
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
            mode = ?snapshot.mode,
            source_version = snapshot.version,
            strategy = plan.strategy.as_str(),
            node_count,
            edge_count,
            affected_nodes = plan.affected_nodes,
            affected_ranks = plan.affected_ranks,
            layout_duration_ms = layout_duration.as_millis(),
            fallback_reason = plan.fallback_reason.unwrap_or("none"),
            "console graph layout planned",
        );

        let write_started = Instant::now();
        let outcome = self
            .write_materialization(generation, snapshot.version, snapshot.mode, plan, previous)
            .await?;
        match outcome {
            MaterializationWriteOutcome::Committed {
                strategy,
                fallback_reason,
            } => {
                tracing::info!(
                    mode = ?snapshot.mode,
                    source_version = snapshot.version,
                    strategy = strategy.as_str(),
                    node_count,
                    edge_count,
                    write_duration_ms = write_started.elapsed().as_millis(),
                    fallback_reason = fallback_reason.unwrap_or("none"),
                    "console graph materialization committed",
                );
            }
            MaterializationWriteOutcome::SkippedStale { current_version } => {
                tracing::info!(
                    mode = ?snapshot.mode,
                    source_version = snapshot.version,
                    current_version,
                    node_count,
                    edge_count,
                    write_duration_ms = write_started.elapsed().as_millis(),
                    "console graph stale materialization skipped",
                );
            }
        }
        Ok(())
    }

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
            mode = ?snapshot.mode,
            source_version = snapshot.version,
            generation,
            strategy = plan.strategy.as_str(),
            node_count,
            edge_count,
            affected_nodes = plan.affected_nodes,
            affected_ranks = plan.affected_ranks,
            layout_duration_ms = layout_started.elapsed().as_millis(),
            fallback_reason = plan.fallback_reason.unwrap_or("none"),
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
            self.with_connection(move |connection| {
                connection
                    .transaction::<_, diesel::result::Error, _>(|connection| {
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
            self.with_connection(move |connection| {
                connection
                    .transaction::<_, diesel::result::Error, _>(|connection| {
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
        self.with_connection(move |connection| {
            upsert_materialization(connection, generation, mode, source_version, width, height)
                .context(QueryGraphSnapshotStoreSnafu { path })
        })
        .await?;
        tracing::info!(
            mode = ?snapshot.mode,
            source_version = snapshot.version,
            generation,
            node_count,
            edge_count,
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
                let generation = query_active_generation(connection)?;
                console_graph_node_locations::table
                    .filter(console_graph_node_locations::generation.eq(generation))
                    .filter(console_graph_node_locations::mode.eq(mode.as_query_value()))
                    .filter(
                        console_graph_node_locations::node_target
                            .eq(&target)
                            .or(console_graph_node_locations::node_id.eq(&target))
                            .or(console_graph_node_locations::node_key.eq(&target)),
                    )
                    .order(console_graph_node_locations::node_id)
                    .select((
                        console_graph_node_locations::node_id,
                        console_graph_node_locations::labels_json,
                    ))
                    .first::<(String, String)>(connection)
                    .optional()
            })
            .context(QueryGraphSnapshotStoreSnafu {
                path: this.path.as_ref().clone(),
            })?;
            row.map(|(node_id, labels_json)| {
                Ok(MaterializedNodeReference {
                    node_id,
                    labels: parse_labels(&labels_json)?,
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
                let generation = query_active_generation(connection)?;
                console_graph_node_locations::table
                    .filter(console_graph_node_locations::generation.eq(generation))
                    .filter(console_graph_node_locations::mode.eq(mode.as_query_value()))
                    .filter(console_graph_node_locations::node_id.eq_any(&node_ids))
                    .select((
                        console_graph_node_locations::node_id,
                        console_graph_node_locations::x,
                        console_graph_node_locations::y,
                    ))
                    .load::<(String, i32, i32)>(connection)
            })
            .context(QueryGraphSnapshotStoreSnafu {
                path: this.path.as_ref().clone(),
            })?;
            Ok(rows
                .into_iter()
                .map(|(node_id, x, y)| (node_id, Point { x, y }))
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
            let Some(mut stored) = stored else {
                return Ok(None);
            };
            stored.nodes.sort_by(|left, right| {
                left.created_at_ns
                    .cmp(&right.created_at_ns)
                    .then_with(|| left.node_id.cmp(&right.node_id))
            });
            Ok(Some(MaterializedGraphShellFacts {
                version: stored.materialization.source_version.max(0) as u64,
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
            }))
        })
        .await
    }

    #[cfg(test)]
    async fn load_stored_graph(&self, mode: GraphMode) -> crate::Result<StoredGraph> {
        let generation = self.active_generation().await?;
        self.load_stored_graph_for_generation(generation, mode)
            .await
    }

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
        self.with_connection(move |connection| {
            connection
                .transaction::<_, diesel::result::Error, _>(|connection| {
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

#[cfg(test)]
fn query_materialization_row(
    connection: &mut SqliteConnection,
    mode: GraphMode,
) -> QueryResult<Option<MaterializationRow>> {
    let generation = query_active_generation(connection)?;
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

fn query_nodes_for_generation(
    connection: &mut SqliteConnection,
    generation: i64,
    mode: GraphMode,
) -> QueryResult<Vec<StoredNodeRow>> {
    console_graph_node_locations::table
        .filter(console_graph_node_locations::generation.eq(generation))
        .filter(console_graph_node_locations::mode.eq(mode.as_query_value()))
        .order(console_graph_node_locations::node_id)
        .select(StoredNodeRow::as_select())
        .load(connection)
}

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
    let generation = query_active_generation(connection)?;
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
    let nodes = console_graph_node_locations::table
        .filter(console_graph_node_locations::generation.eq(generation))
        .filter(console_graph_node_locations::mode.eq(mode.as_query_value()))
        .filter(console_graph_node_locations::min_x.le(right))
        .filter(console_graph_node_locations::max_x.ge(left))
        .filter(console_graph_node_locations::min_y.le(bottom))
        .filter(console_graph_node_locations::max_y.ge(top))
        .order((
            console_graph_node_locations::rank,
            console_graph_node_locations::sort_order,
            console_graph_node_locations::node_id,
        ))
        .select(StoredNodeRow::as_select())
        .load(connection)?;
    let edges = console_graph_edge_routes::table
        .filter(console_graph_edge_routes::generation.eq(generation))
        .filter(console_graph_edge_routes::mode.eq(mode.as_query_value()))
        .filter(console_graph_edge_routes::min_x.le(right))
        .filter(console_graph_edge_routes::max_x.ge(left))
        .filter(console_graph_edge_routes::min_y.le(bottom))
        .filter(console_graph_edge_routes::max_y.ge(top))
        .order(console_graph_edge_routes::edge_key)
        .select(StoredEdgeRow::as_select())
        .load(connection)?;
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
    let generation = query_active_generation(connection)?;
    let Some(materialization) =
        query_materialization_row_for_generation(connection, generation, mode)?
    else {
        return Ok(None);
    };
    let nodes = query_nodes_for_generation(connection, generation, mode)?;
    let edge_count = console_graph_edge_routes::table
        .filter(console_graph_edge_routes::generation.eq(generation))
        .filter(console_graph_edge_routes::mode.eq(mode.as_query_value()))
        .count()
        .get_result(connection)?;
    Ok(Some(StoredShellFacts {
        materialization,
        nodes,
        edge_count,
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

fn parse_edge_kind(value: &str) -> crate::Result<GraphEdgeKind> {
    match parse_viewport_edge_kind(value)? {
        GraphViewportEdgeKind::Primary => Ok(GraphEdgeKind::Primary),
        GraphViewportEdgeKind::Merge => Ok(GraphEdgeKind::Merge),
        GraphViewportEdgeKind::Shadow => Ok(GraphEdgeKind::Shadow),
    }
}

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
        connection.transaction(|connection| {
            let generation = query_active_generation(connection)?;
            delete_mode_rows(connection, generation, snapshot.mode)?;
            for node in &nodes {
                upsert_node(connection, generation, snapshot.mode, node)?;
            }
            for edge in &edges {
                upsert_edge(connection, generation, snapshot.mode, edge)?;
            }
            upsert_materialization(
                connection,
                generation,
                snapshot.mode,
                snapshot.version,
                layout.width,
                layout.height,
            )
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
        assert_eq!(facts.nodes.len(), 2);
        assert_eq!(facts.edge_count, 1);
        assert_eq!(
            query_materialization_row(&mut reader, GraphMode::All)
                .unwrap()
                .unwrap()
                .source_version,
            2
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
