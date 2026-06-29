use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use diesel::prelude::*;
use diesel::sql_types::{BigInt, Double, Integer, Text};
use diesel::sqlite::SqliteConnection;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use snafu::prelude::*;

use crate::api::{
    GraphCanvas, GraphViewport, GraphViewportDiffResponse, GraphViewportEdge,
    GraphViewportEdgeKind, GraphViewportLane, GraphViewportNode, GraphViewportResponse, Point,
};
use crate::error::{
    ManageGraphSnapshotStoreSnafu, MigrateGraphSnapshotStoreSnafu,
    ParseGraphSnapshotStoreValueSnafu, QueryGraphSnapshotStoreSnafu,
};
use crate::graph::{
    GraphMode, graph_kind_name, initial_visible_graph_lane_nodes, node_target_id,
    provider_context_ancestry_nodes, shorten_id, summarize_node,
    visible_skill_invocation_subtree_nodes,
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
    SessionState, SessionStore, SqliteDatabase,
};

const SQLITE_DATABASE_FILE_NAME: &str = "console-graph.sqlite3";
const COORDINATE_SPACE: &str = "graph_layout_v1";
const NODE_RADIUS: i32 = 26;
const EDGE_TARGET_APPROACH: i32 = 48;
const GRAPH_LANE_HEIGHT: i32 = 140;
const EDGE_ROUTE_STEP: i32 = 12;
const MAX_EDGE_COLUMN_GAP: usize = 5;
const DERIVED_ORPHAN_LANE_KEY_PREFIX: &str = "derived:orphan:";
const DERIVED_SKILL_LANE_KEY_PREFIX: &str = "derived:skill:";
const LEGACY_MATERIALIZATION_TABLES: &[&str] = &[
    "console_graph_snapshots",
    "console_graph_viewports",
    "console_graph_viewport_lanes",
    "console_graph_viewport_nodes",
    "console_graph_viewport_edges",
];
const CONSOLE_GRAPH_MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

diesel::table! {
    sqlite_master (name) {
        #[sql_name = "type"]
        object_type -> Text,
        name -> Text,
    }
}

#[derive(Clone, Debug)]
pub struct ConsoleGraphSnapshotStore {
    path: Arc<PathBuf>,
    database: SqliteDatabase,
}

struct MaterializationSourceSnapshot {
    root_id: String,
    nodes: BTreeMap<String, Node>,
    children: BTreeMap<String, Vec<Node>>,
    branches: BTreeMap<String, String>,
    sessions: HashMap<String, SessionState>,
}

impl MaterializationSourceSnapshot {
    fn from_store(
        store: &(impl BranchStore + NodeStore + SessionStore),
        session_states: &[(String, SessionState)],
    ) -> crate::Result<Self> {
        let mut branches = BTreeMap::new();
        for (branch, _) in session_states {
            branches.insert(
                branch.clone(),
                store
                    .get_branch_head(branch)
                    .context(crate::error::StoreSnafu)?,
            );
        }

        let root_id = store.root_id();
        let mut nodes = BTreeMap::new();
        let mut children = BTreeMap::new();
        let mut pending = vec![root_id.clone()];
        while let Some(node_id) = pending.pop() {
            if nodes.contains_key(&node_id) {
                continue;
            }
            let node = store.get_node(&node_id).context(crate::error::StoreSnafu)?;
            let node_children = store
                .list_children(&node_id)
                .context(crate::error::StoreSnafu)?;
            pending.extend(node_children.iter().map(|child| child.id.clone()));
            children.insert(node_id, node_children);
            nodes.insert(node.id.clone(), node);
        }

        Ok(Self {
            root_id,
            nodes,
            children,
            branches,
            sessions: session_states.iter().cloned().collect(),
        })
    }

    fn resolve_ref_id(&self, reference: &str) -> coco_mem::StoreResult<String> {
        if self.nodes.contains_key(reference) {
            return Ok(reference.to_owned());
        }
        if let Some(head_id) = self.branches.get(reference) {
            return Ok(head_id.clone());
        }
        let matches = self
            .nodes
            .keys()
            .filter(|node_id| node_id.starts_with(reference))
            .cloned()
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [matched] => Ok(matched.clone()),
            [] => Err(coco_mem::StoreError::NotFound {
                id: reference.to_owned(),
            }),
            matches => Err(coco_mem::StoreError::AmbiguousNodePrefix {
                prefix: reference.to_owned(),
                matches: matches.to_vec(),
            }),
        }
    }

    fn read_only_error<T>() -> coco_mem::StoreResult<T> {
        Err(coco_mem::StoreError::StoreReadOnly {
            path: PathBuf::from("console graph materialization source snapshot"),
        })
    }
}

impl NodeStore for MaterializationSourceSnapshot {
    fn root_id(&self) -> String {
        self.root_id.clone()
    }

    fn append(&self, _node: NewNode) -> coco_mem::StoreResult<String> {
        Self::read_only_error()
    }

    fn ancestry(&self, head_ref: &str) -> coco_mem::StoreResult<Vec<Node>> {
        let mut node_id = self.resolve_ref_id(head_ref)?;
        let mut nodes = Vec::new();
        loop {
            let node = self.nodes.get(&node_id).cloned().ok_or_else(|| {
                coco_mem::StoreError::NotFound {
                    id: node_id.clone(),
                }
            })?;
            let parent = node.parent.clone();
            nodes.push(node);
            if parent.is_empty() {
                return Ok(nodes);
            }
            if !self.nodes.contains_key(&parent) {
                return Err(coco_mem::StoreError::ParentNotFound { id: parent });
            }
            node_id = parent;
        }
    }

    fn log(&self, base_ref: &str, head_ref: &str) -> coco_mem::StoreResult<Vec<Node>> {
        let base_id = self.resolve_ref_id(base_ref)?;
        let mut nodes = self.ancestry(head_ref)?;
        let Some(index) = nodes.iter().position(|node| node.id == base_id) else {
            return Err(coco_mem::StoreError::RefsNotConnected {
                base_ref: base_ref.to_owned(),
                head_ref: head_ref.to_owned(),
            });
        };
        nodes.truncate(index + 1);
        Ok(nodes)
    }

    fn get_node(&self, id: &str) -> coco_mem::StoreResult<Node> {
        let id = self.resolve_ref_id(id)?;
        self.nodes
            .get(&id)
            .cloned()
            .ok_or(coco_mem::StoreError::NotFound { id })
    }

    fn list_children(&self, node_id: &str) -> coco_mem::StoreResult<Vec<Node>> {
        self.nodes
            .get(node_id)
            .ok_or_else(|| coco_mem::StoreError::NotFound {
                id: node_id.to_owned(),
            })?;
        Ok(self.children.get(node_id).cloned().unwrap_or_default())
    }
}

impl BranchStore for MaterializationSourceSnapshot {
    fn fork(&self, _name: &str, _from_ref: &str) -> coco_mem::StoreResult<String> {
        Self::read_only_error()
    }

    fn get_branch_head(&self, name: &str) -> coco_mem::StoreResult<String> {
        self.branches
            .get(name)
            .cloned()
            .ok_or_else(|| coco_mem::StoreError::BranchNotFound {
                name: name.to_owned(),
            })
    }

    fn delete_branch(&self, _name: &str) -> coco_mem::StoreResult<()> {
        Self::read_only_error()
    }

    fn set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _new_head: &str,
    ) -> coco_mem::StoreResult<()> {
        Self::read_only_error()
    }
}

impl SessionStore for MaterializationSourceSnapshot {
    fn list_session_states(&self) -> coco_mem::StoreResult<HashMap<String, SessionState>> {
        Ok(self.sessions.clone())
    }

    fn get_session_state(&self, name: &str) -> coco_mem::StoreResult<SessionState> {
        self.sessions
            .get(name)
            .cloned()
            .ok_or_else(|| coco_mem::StoreError::BranchNotFound {
                name: name.to_owned(),
            })
    }

    fn set_session_state(
        &self,
        _name: &str,
        _expected: Option<&SessionState>,
        _next: SessionState,
    ) -> coco_mem::StoreResult<SessionState> {
        Self::read_only_error()
    }

    fn rebase_session(
        &self,
        _name: &str,
        _patch: &SessionAnchorPatch,
    ) -> coco_mem::StoreResult<String> {
        Self::read_only_error()
    }

    fn handoff_session(
        &self,
        _name: &str,
        _patch: &SessionAnchorPatch,
        _prompt: &str,
    ) -> coco_mem::StoreResult<String> {
        Self::read_only_error()
    }
}

#[derive(Clone, Queryable, QueryableByName)]
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

#[derive(Clone, Queryable, QueryableByName)]
struct LaneRow {
    #[diesel(sql_type = Text)]
    lane_key: String,
    #[diesel(sql_type = Text)]
    lane_label: String,
    #[diesel(sql_type = Integer)]
    lane_y: i32,
}

#[derive(Queryable, QueryableByName)]
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

#[derive(Clone, Queryable, QueryableByName)]
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

#[derive(Clone, Queryable, QueryableByName)]
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

#[derive(Queryable, QueryableByName)]
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

#[derive(Insertable)]
#[diesel(table_name = console_graph_materializations)]
struct MaterializationInsert<'a> {
    mode: &'a str,
    source_version: i64,
    coordinate_space: &'a str,
    world_min_x: i32,
    world_min_y: i32,
    world_max_x: i32,
    world_max_y: i32,
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

struct MergeColumnConstraintInput<'a> {
    mode: GraphMode,
    node: &'a Node,
    primary_parent_id: &'a str,
    point: Point,
    event_order: &'a BTreeMap<String, usize>,
    reserved_lane_y: Option<i32>,
    context_start_id: Option<&'a str>,
}

struct NodeMergeEdgesInput<'a> {
    mode: GraphMode,
    node: &'a Node,
    primary_parent_id: &'a str,
    target: Point,
    context_start_id: Option<&'a str>,
}

struct AnchorBranchLaneInsert {
    lane_y: i32,
    nodes: Vec<Node>,
    previous: Option<(String, Point)>,
    context_start_id: Option<String>,
}

struct VisibleMergeParentPoint {
    node_id: String,
    point: Point,
}

enum MergeParentPoint {
    Visible(VisibleMergeParentPoint),
    Skipped,
    Unsupported,
}

struct OrphanMergeParentLane {
    source_id: String,
    lane: GraphViewportLane,
    nodes: Vec<Node>,
    fork_source: Option<(String, Point)>,
    context_start_id: Option<String>,
}

struct OrphanMergeParentNodeEdgeInput<'a> {
    mode: GraphMode,
    node: &'a Node,
    point: Point,
    previous: Option<&'a (String, Point)>,
    first_node: bool,
    force_fork: bool,
    context_start_id: Option<&'a str>,
}

struct OrphanMergeParentLaneInput<'a> {
    mode: GraphMode,
    ancestry: &'a [Node],
    source_index: usize,
    reserved_lane_y: Option<i32>,
    context_start_id: Option<&'a str>,
}

enum SkillSubtreeAppend {
    Absent,
    Applied,
    Unsupported,
}

struct MaterializationMetaInput {
    source_version: u64,
    mode: GraphMode,
    world_min_x: i32,
    world_min_y: i32,
    world_max_x: i32,
    world_max_y: i32,
}

enum SnapshotTransactionError {
    Query(diesel::result::Error),
    Operation(crate::Error),
    RollbackFalse,
}

impl From<diesel::result::Error> for SnapshotTransactionError {
    fn from(source: diesel::result::Error) -> Self {
        Self::Query(source)
    }
}

impl ConsoleGraphSnapshotStore {
    pub fn open(dir: impl AsRef<Path>) -> crate::Result<Self> {
        let dir = dir.as_ref();
        let path = database_path(dir);
        drop_stale_snapshot_materialization_tables(&path)?;
        let database =
            SqliteDatabase::open_unshared_file_path(&path).context(crate::error::StoreSnafu)?;
        let store = Self {
            path: Arc::new(path),
            database,
        };
        store.ensure_schema()?;
        Ok(store)
    }

    pub fn latest_viewport(
        &self,
        mode: GraphMode,
        request: GraphViewportRequest,
    ) -> crate::Result<Option<GraphViewportResponse>> {
        let this = self.clone();
        self.with_connection(move |connection| {
            this.run_read_transaction(connection, |this, connection| {
                let Some(meta) = this.latest_materialization_row_in_connection(connection, mode)?
                else {
                    return Ok(None);
                };
                this.viewport_from_row(connection, mode, meta, request)
            })
        })
    }

    pub fn latest_viewport_diff(
        &self,
        mode: GraphMode,
        request: GraphViewportDiffRequest,
    ) -> crate::Result<Option<GraphViewportDiffResponse>> {
        let this = self.clone();
        self.with_connection(move |connection| {
            this.run_read_transaction(connection, |this, connection| {
                let Some(meta) = this.latest_materialization_row_in_connection(connection, mode)?
                else {
                    return Ok(None);
                };
                this.viewport_diff_from_row(connection, mode, meta, request)
            })
        })
    }

    pub(crate) fn materialized_node_reference(
        &self,
        mode: GraphMode,
        target: &str,
    ) -> crate::Result<Option<MaterializedNodeReference>> {
        let this = self.clone();
        let target = target.to_owned();
        self.with_connection(move |connection| {
            this.run_read_transaction(connection, |this, connection| {
                this.materialized_node_reference_in_connection(connection, mode, &target)
            })
        })
    }

    pub(crate) fn materialized_node_points(
        &self,
        mode: GraphMode,
        node_ids: &BTreeSet<String>,
    ) -> crate::Result<BTreeMap<String, Point>> {
        let this = self.clone();
        let node_ids = node_ids.clone();
        self.with_connection(move |connection| {
            this.run_read_transaction(connection, |this, connection| {
                this.materialized_node_points_in_connection(connection, mode, &node_ids)
            })
        })
    }

    pub(crate) fn has_materialization(&self, mode: GraphMode) -> crate::Result<bool> {
        Ok(self.latest_materialization_row(mode)?.is_some())
    }

    pub(crate) fn has_non_empty_materialization(&self, mode: GraphMode) -> crate::Result<bool> {
        let this = self.clone();
        self.with_connection(move |connection| {
            Ok(this
                .latest_materialization_row_in_connection(connection, mode)?
                .is_some()
                && !this
                    .materialized_node_rows_in_connection(connection, mode)?
                    .is_empty())
        })
    }

    pub(crate) fn latest_materialization_version(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Option<u64>> {
        Ok(self
            .latest_materialization_row(mode)?
            .map(|meta| meta.source_version.max(0) as u64))
    }

    pub(crate) fn materialized_shell_facts(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Option<MaterializedGraphShellFacts>> {
        let this = self.clone();
        self.with_connection(move |connection| {
            this.run_read_transaction(connection, |this, connection| {
                let Some(meta) = this.latest_materialization_row_in_connection(connection, mode)?
                else {
                    return Ok(None);
                };
                let lanes = this
                    .materialized_lanes_in_connection(connection, mode)?
                    .into_iter()
                    .map(|row| GraphViewportLane {
                        key: row.lane_key,
                        label: row.lane_label,
                        y: row.lane_y,
                    })
                    .collect();
                let mut nodes_by_id = BTreeMap::new();
                for row in this.materialized_node_rows_in_connection(connection, mode)? {
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
                    edge_count: this.materialized_edge_count_in_connection(connection, mode)?,
                }))
            })
        })
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
        session_states.sort_by(|(left, _), (right, _)| {
            branch_lane_priority(left).cmp(&branch_lane_priority(right))
        });
        let source = MaterializationSourceSnapshot::from_store(store, &session_states)?;

        if session_states.is_empty() {
            let this = self.clone();
            return self.with_connection(move |connection| {
                this.run_bool_write_transaction(connection, |this, connection| {
                    this.put_empty_materialization_in_transaction(connection, source_version, mode)
                })
            });
        }

        let this = self.clone();
        let materialization_is_empty = self.with_connection(move |connection| {
            let has_materialization = this
                .latest_materialization_row_in_connection(connection, mode)?
                .is_some();
            Ok(!has_materialization
                || this
                    .materialized_node_rows_in_connection(connection, mode)?
                    .is_empty())
        })?;

        if materialization_is_empty {
            return self.try_seed_initial_branch_materialization_in_batches(
                source,
                source_version,
                mode,
                session_states,
            );
        }

        let this = self.clone();
        self.with_connection(move |connection| {
            this.run_bool_write_transaction(connection, |this, connection| match mode {
                GraphMode::Anchors => this.try_update_anchor_materialization_in_transaction(
                    connection,
                    &source,
                    source_version,
                    &session_states,
                ),
                GraphMode::All => this.try_append_linear_branches_in_transaction(
                    connection,
                    &source,
                    source_version,
                    mode,
                    &session_states,
                ),
            })
        })
    }

    fn try_seed_initial_branch_materialization_in_batches(
        &self,
        source: MaterializationSourceSnapshot,
        source_version: u64,
        mode: GraphMode,
        session_states: Vec<(String, SessionState)>,
    ) -> crate::Result<bool> {
        let this = self.clone();
        self.with_connection(move |connection| {
            this.run_bool_write_transaction(connection, |this, connection| {
                let Some(first_index) =
                    this.first_visible_initial_branch_index(&source, mode, &session_states)?
                else {
                    this.delete_materialization_meta(connection, mode)?;
                    return this.put_empty_materialization_in_transaction(
                        connection,
                        source_version,
                        mode,
                    );
                };

                this.delete_materialization_meta(connection, mode)?;
                this.clear_materialized_mode_facts(connection, mode)?;

                let (first_branch, first_state) = &session_states[first_index];
                if !this.try_seed_first_branch_materialization_in_transaction(
                    connection,
                    &source,
                    mode,
                    first_branch,
                    first_state,
                )? {
                    return Ok(false);
                }

                let mut next_lane_y = crate::layout::GRAPH_TOP_Y + GRAPH_LANE_HEIGHT;
                for (branch, state) in session_states[first_index..].iter().skip(1) {
                    if !this.branch_has_initial_visible_nodes(&source, mode, branch)? {
                        continue;
                    }
                    let head_id = source
                        .get_branch_head(branch)
                        .context(crate::error::StoreSnafu)?;
                    this.shift_lanes_for_insertion(connection, mode, next_lane_y)?;
                    let input = AppendLinearBranchInput {
                        mode,
                        branch,
                        state,
                        head_id: &head_id,
                    };
                    let appended = match mode {
                        GraphMode::Anchors => this
                            .try_append_new_anchor_branch_lane_in_transaction(
                                connection,
                                &source,
                                input,
                                next_lane_y,
                            ),
                        GraphMode::All => this.try_append_new_branch_lane_in_transaction(
                            connection,
                            &source,
                            input,
                            next_lane_y,
                        ),
                    }?;
                    if !appended {
                        return Ok(false);
                    }
                    next_lane_y += GRAPH_LANE_HEIGHT;
                }

                this.rebalance_routed_edge_slots(connection, mode)?;
                if this
                    .refresh_materialized_node_labels(connection, &source, mode, &session_states)?
                    .is_none()
                {
                    return Ok(false);
                }
                this.put_materialization_meta_from_materialized_rows(
                    connection,
                    source_version,
                    mode,
                )?;
                Ok(true)
            })
        })
    }

    #[cfg(test)]
    fn restore_empty_materialization_after_failed_batch_seed(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        previous_meta: Option<MaterializationRow>,
    ) -> crate::Result<()> {
        self.run_write_transaction(connection, |this, connection| {
            this.delete_materialization_meta(connection, mode)?;
            this.clear_materialized_mode_facts(connection, mode)?;
            if let Some(meta) = previous_meta {
                this.put_materialization_meta(
                    connection,
                    MaterializationMetaInput {
                        source_version: meta.source_version.max(0) as u64,
                        mode,
                        world_min_x: meta.world_min_x,
                        world_min_y: meta.world_min_y,
                        world_max_x: meta.world_max_x,
                        world_max_y: meta.world_max_y,
                    },
                )?;
            }
            Ok(())
        })
    }

    #[cfg(test)]
    fn run_write_transaction<T, F>(
        &self,
        connection: &mut SqliteConnection,
        operation: F,
    ) -> crate::Result<T>
    where
        F: FnOnce(&Self, &mut SqliteConnection) -> crate::Result<T>,
    {
        self.finish_transaction(connection.immediate_transaction(|connection| {
            operation(self, connection).map_err(SnapshotTransactionError::Operation)
        }))
    }

    fn run_read_transaction<T, F>(
        &self,
        connection: &mut SqliteConnection,
        operation: F,
    ) -> crate::Result<T>
    where
        F: FnOnce(&Self, &mut SqliteConnection) -> crate::Result<T>,
    {
        self.finish_transaction(connection.transaction(|connection| {
            operation(self, connection).map_err(SnapshotTransactionError::Operation)
        }))
    }

    fn run_bool_write_transaction<F>(
        &self,
        connection: &mut SqliteConnection,
        operation: F,
    ) -> crate::Result<bool>
    where
        F: FnOnce(&Self, &mut SqliteConnection) -> crate::Result<bool>,
    {
        match connection.immediate_transaction(|connection| match operation(self, connection) {
            Ok(true) => Ok(true),
            Ok(false) => Err(SnapshotTransactionError::RollbackFalse),
            Err(error) => Err(SnapshotTransactionError::Operation(error)),
        }) {
            Ok(true) => Ok(true),
            Err(SnapshotTransactionError::RollbackFalse) => Ok(false),
            result => self.finish_transaction(result),
        }
    }

    pub fn replace_materialization_from_viewport(
        &self,
        mode: GraphMode,
        viewport: GraphViewportResponse,
        branch_labels: BTreeSet<String>,
    ) -> crate::Result<()> {
        let this = self.clone();
        self.with_connection(move |connection| {
            this.finish_transaction(connection.immediate_transaction(|connection| {
                this.replace_materialization_from_viewport_in_transaction(
                    connection,
                    mode,
                    viewport,
                    branch_labels,
                )
                .map_err(SnapshotTransactionError::Operation)
            }))
        })
    }

    fn replace_materialization_from_viewport_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        viewport: GraphViewportResponse,
        branch_labels: BTreeSet<String>,
    ) -> crate::Result<()> {
        let mut nodes_by_y = BTreeMap::<i32, Vec<&GraphViewportNode>>::new();
        for node in &viewport.nodes {
            nodes_by_y.entry(node.y).or_default().push(node);
        }
        let lanes_by_y = viewport
            .lanes
            .iter()
            .map(|lane| {
                (
                    lane.y,
                    full_layout_materialization_lane(lane, &nodes_by_y, &branch_labels),
                )
            })
            .collect::<BTreeMap<_, _>>();
        self.clear_materialized_mode_facts(connection, mode)?;
        for node in &viewport.nodes {
            let fallback_lane;
            let lane = if let Some(lane) = lanes_by_y.get(&node.y) {
                lane
            } else {
                fallback_lane = GraphViewportLane {
                    key: format!("layout:y:{}", node.y),
                    label: String::new(),
                    y: node.y,
                };
                &fallback_lane
            };
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
        for edge in &viewport.edges {
            self.insert_edge_route(
                connection,
                EdgeRouteInsert {
                    mode,
                    edge,
                    bounds: edge_bounds(edge),
                },
            )?;
        }
        self.put_materialization_meta(
            connection,
            MaterializationMetaInput {
                source_version: viewport.version,
                mode,
                world_min_x: 0,
                world_min_y: 0,
                world_max_x: viewport.canvas.width,
                world_max_y: viewport.canvas.height,
            },
        )
    }

    fn put_empty_materialization_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        source_version: u64,
        mode: GraphMode,
    ) -> crate::Result<bool> {
        self.clear_materialized_mode_facts(connection, mode)?;
        self.put_materialization_meta(
            connection,
            MaterializationMetaInput {
                source_version,
                mode,
                world_min_x: 0,
                world_min_y: 0,
                world_max_x: GRAPH_LEFT_X + 120,
                world_max_y: crate::layout::GRAPH_TOP_Y + 120,
            },
        )?;
        Ok(true)
    }

    fn first_visible_initial_branch_index(
        &self,
        store: &(impl BranchStore + NodeStore),
        mode: GraphMode,
        session_states: &[(String, SessionState)],
    ) -> crate::Result<Option<usize>> {
        for (index, (branch, _)) in session_states.iter().enumerate() {
            if self.branch_has_initial_visible_nodes(store, mode, branch)? {
                return Ok(Some(index));
            }
        }
        Ok(None)
    }

    fn branch_has_initial_visible_nodes(
        &self,
        store: &(impl BranchStore + NodeStore),
        mode: GraphMode,
        branch: &str,
    ) -> crate::Result<bool> {
        let head_id = store
            .get_branch_head(branch)
            .context(crate::error::StoreSnafu)?;
        let ancestry = store.ancestry(&head_id).context(crate::error::StoreSnafu)?;
        Ok(!initial_visible_graph_lane_nodes(store, mode, ancestry)?.is_empty())
    }

    fn try_seed_first_branch_materialization_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &(impl BranchStore + NodeStore),
        mode: GraphMode,
        first_branch: &str,
        first_state: &SessionState,
    ) -> crate::Result<bool> {
        let head_id = store
            .get_branch_head(first_branch)
            .context(crate::error::StoreSnafu)?;
        let ancestry = store.ancestry(&head_id).context(crate::error::StoreSnafu)?;
        let context_start_id = merge_parent_context_start_id(mode, &ancestry);
        let nodes = initial_visible_graph_lane_nodes(store, mode, ancestry)?;
        if nodes.is_empty() || !initial_visible_lane_is_linear(mode, &nodes) {
            return Ok(false);
        }

        let lane = GraphViewportLane {
            key: lane_key(first_branch),
            label: first_branch.to_owned(),
            y: crate::layout::GRAPH_TOP_Y,
        };
        let branch_label = branch_label(first_branch, first_state);
        let event_order =
            self.event_order_by_materialized_and_new_nodes(connection, store, mode, &nodes)?;
        let mut previous = None::<(String, Point)>;
        let appended_len = nodes.len();
        for (index, node) in nodes.into_iter().enumerate() {
            let candidate = match previous.as_ref() {
                Some((previous_id, previous_point)) => Point {
                    x: previous_point.x
                        + required_column_gap(previous_id, &node.id, &event_order)
                            * GRAPH_COLUMN_WIDTH,
                    y: lane.y,
                },
                None => Point {
                    x: GRAPH_LEFT_X,
                    y: lane.y,
                },
            };
            let primary_parent_id = previous
                .as_ref()
                .map(|(previous_id, _)| previous_id.as_str())
                .unwrap_or("");
            let Some(point) = self.point_with_merge_parent_column_constraints(
                connection,
                store,
                MergeColumnConstraintInput {
                    mode,
                    node: &node,
                    primary_parent_id,
                    point: candidate,
                    event_order: &event_order,
                    reserved_lane_y: Some(lane.y),
                    context_start_id: context_start_id.as_deref(),
                },
            )?
            else {
                return Ok(false);
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
                    mode,
                    node: &viewport_node,
                    lane: &lane,
                    bounds: node_bounds(&viewport_node),
                },
            )?;
            if let Some((previous_id, previous_point)) = previous.as_ref() {
                let edge = primary_parent_edge(previous_id, *previous_point, &node.id, point);
                self.insert_edge_route(
                    connection,
                    EdgeRouteInsert {
                        mode,
                        edge: &edge,
                        bounds: edge_bounds(&edge),
                    },
                )?;
                if !self.insert_node_merge_edges(
                    connection,
                    store,
                    NodeMergeEdgesInput {
                        mode,
                        node: &node,
                        primary_parent_id: previous_id,
                        target: point,
                        context_start_id: context_start_id.as_deref(),
                    },
                )? {
                    return Ok(false);
                }
            } else if !self.insert_node_merge_edges(
                connection,
                store,
                NodeMergeEdgesInput {
                    mode,
                    node: &node,
                    primary_parent_id: "",
                    target: point,
                    context_start_id: context_start_id.as_deref(),
                },
            )? {
                return Ok(false);
            }
            if matches!(
                self.try_append_skill_invocation_subtree_in_transaction(
                    connection, store, mode, &node.id, point, &lane,
                )?,
                SkillSubtreeAppend::Unsupported
            ) {
                return Ok(false);
            }
            previous = Some((node.id, point));
        }

        Ok(true)
    }

    fn put_materialization_meta_from_materialized_rows(
        &self,
        connection: &mut SqliteConnection,
        source_version: u64,
        mode: GraphMode,
    ) -> crate::Result<()> {
        let materialized_nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        let world_max_x = materialized_nodes
            .iter()
            .map(|row| row.x)
            .max()
            .unwrap_or(GRAPH_LEFT_X)
            + 120;
        let world_max_y = self
            .materialized_lanes_in_connection(connection, mode)?
            .iter()
            .map(|lane| lane.lane_y)
            .max()
            .unwrap_or(crate::layout::GRAPH_TOP_Y)
            + 120;
        self.put_materialization_meta(
            connection,
            MaterializationMetaInput {
                source_version,
                mode,
                world_min_x: 0,
                world_min_y: 0,
                world_max_x,
                world_max_y,
            },
        )?;
        Ok(())
    }

    fn delete_materialized_branch_lane_if_isolated(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
    ) -> crate::Result<bool> {
        let Some(lane) = self
            .materialized_lanes_in_connection(connection, mode)?
            .into_iter()
            .find(|lane| lane.lane_key == lane_key(branch))
        else {
            return Ok(true);
        };
        if self.lanes_have_retained_downstream_edges(
            connection,
            mode,
            std::slice::from_ref(&lane),
        )? {
            return Ok(false);
        }
        self.delete_materialized_lanes(connection, mode, std::slice::from_ref(&lane))?;
        self.shift_lanes_after_deletion(connection, mode, std::slice::from_ref(&lane))?;
        Ok(true)
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
        if let Some(appended) = self.try_append_unchanged_head_skill_subtree_in_transaction(
            connection, store, &input, &tail,
        )? {
            return Ok(appended);
        }
        if let Some(appended) = self.try_refresh_materialized_branch_head_in_transaction(
            connection,
            store,
            &input,
            &tail,
            &branch_label,
        )? {
            return Ok(appended);
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
        if matches!(
            self.try_append_skill_invocation_subtree_in_transaction(
                connection,
                store,
                input.mode,
                &tail.node_id,
                Point {
                    x: tail.x,
                    y: tail.y,
                },
                &lane,
            )?,
            SkillSubtreeAppend::Unsupported
        ) {
            return Ok(false);
        }
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
            let Some(point) = self.point_with_merge_parent_column_constraints(
                connection,
                store,
                MergeColumnConstraintInput {
                    mode: input.mode,
                    node: &node,
                    primary_parent_id: &previous_id,
                    point,
                    event_order: &event_order,
                    reserved_lane_y: None,
                    context_start_id: None,
                },
            )?
            else {
                return Ok(false);
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
                NodeMergeEdgesInput {
                    mode: input.mode,
                    node: &node,
                    primary_parent_id: &previous_id,
                    target: point,
                    context_start_id: None,
                },
            )? {
                return Ok(false);
            }
            if matches!(
                self.try_append_skill_invocation_subtree_in_transaction(
                    connection, store, input.mode, &node.id, point, &lane,
                )?,
                SkillSubtreeAppend::Unsupported
            ) {
                return Ok(false);
            }
            previous_id = node.id;
            previous_point = point;
        }
        Ok(true)
    }

    fn try_append_unchanged_head_skill_subtree_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        input: &AppendLinearBranchInput<'_>,
        tail: &MaterializedTailNodeRow,
    ) -> crate::Result<Option<bool>> {
        if input.head_id != tail.node_id {
            return Ok(None);
        }
        let lane = GraphViewportLane {
            key: tail.lane_key.clone(),
            label: tail.lane_label.clone(),
            y: tail.lane_y,
        };
        match self.try_append_skill_invocation_subtree_in_transaction(
            connection,
            store,
            input.mode,
            &tail.node_id,
            Point {
                x: tail.x,
                y: tail.y,
            },
            &lane,
        )? {
            SkillSubtreeAppend::Unsupported => Ok(Some(false)),
            SkillSubtreeAppend::Absent | SkillSubtreeAppend::Applied => {
                self.trim_branch_lane_covered_prefix(connection, input.mode, input.branch)?;
                Ok(Some(true))
            }
        }
    }

    fn try_refresh_materialized_branch_head_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        input: &AppendLinearBranchInput<'_>,
        tail: &MaterializedTailNodeRow,
        branch_label: &str,
    ) -> crate::Result<Option<bool>> {
        let Some(head) = self.materialized_lane_node_in_connection(
            connection,
            input.mode,
            input.branch,
            input.head_id,
        )?
        else {
            return Ok(None);
        };
        if head.x >= tail.x {
            return Ok(None);
        }

        let lane = GraphViewportLane {
            key: head.lane_key.clone(),
            label: head.lane_label.clone(),
            y: head.lane_y,
        };
        match self.try_append_skill_invocation_subtree_in_transaction(
            connection,
            store,
            input.mode,
            input.head_id,
            Point {
                x: head.x,
                y: head.y,
            },
            &lane,
        )? {
            SkillSubtreeAppend::Applied => {
                if self.lane_suffix_has_retained_downstream_edges(
                    connection,
                    input.mode,
                    input.branch,
                    head.x,
                )? {
                    return Ok(Some(false));
                }
                self.delete_materialized_lane_suffix(connection, input.mode, input.branch, head.x)?;
                self.update_node_labels(
                    connection,
                    input.mode,
                    &head.node_key,
                    vec![branch_label.to_owned()],
                )?;
                return Ok(Some(true));
            }
            SkillSubtreeAppend::Unsupported => return Ok(Some(false)),
            SkillSubtreeAppend::Absent => {}
        }
        if self.lane_suffix_has_retained_downstream_edges(
            connection,
            input.mode,
            input.branch,
            head.x,
        )? {
            return Ok(Some(false));
        }
        self.delete_materialized_lane_suffix(connection, input.mode, input.branch, head.x)?;
        self.update_node_labels(
            connection,
            input.mode,
            &head.node_key,
            vec![branch_label.to_owned()],
        )?;
        Ok(Some(true))
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
        let Some(materialized_lane_labels) =
            self.prune_anchor_materialized_lanes(connection, session_states)?
        else {
            return Ok(false);
        };
        if !self.try_update_anchor_branch_lanes(
            connection,
            store,
            session_states,
            materialized_lane_labels,
        )? {
            return Ok(false);
        }
        self.prune_removable_derived_lanes(connection, mode)?;
        self.rebalance_routed_edge_slots(connection, mode)?;
        let Some(materialized_nodes) = self.refresh_materialized_node_labels(
            connection,
            store,
            GraphMode::Anchors,
            session_states,
        )?
        else {
            return Ok(false);
        };
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

    fn prune_anchor_materialized_lanes(
        &self,
        connection: &mut SqliteConnection,
        session_states: &[(String, SessionState)],
    ) -> crate::Result<Option<BTreeSet<String>>> {
        let mode = GraphMode::Anchors;
        let mut materialized_lanes = self.materialized_lanes_in_connection(connection, mode)?;
        let branch_names = session_states
            .iter()
            .map(|(branch, _)| branch.clone())
            .collect::<BTreeSet<_>>();
        let removed_lanes = removed_lanes_in_order(&materialized_lanes, &branch_names);
        if !removed_lanes.is_empty() {
            if self.lanes_have_retained_downstream_edges(connection, mode, &removed_lanes)? {
                return Ok(None);
            }
            self.delete_materialized_lanes(connection, mode, &removed_lanes)?;
            self.shift_lanes_after_deletion(connection, mode, &removed_lanes)?;
            materialized_lanes = self.materialized_lanes_in_connection(connection, mode)?;
        }
        let materialized_lane_labels = materialized_lanes
            .iter()
            .filter(|lane| !is_derived_lane_key(&lane.lane_key))
            .map(|lane| lane.lane_label.clone())
            .collect::<BTreeSet<_>>();
        if !existing_branch_lanes_preserve_order(
            session_states,
            &materialized_lanes,
            &materialized_lane_labels,
        ) {
            return Ok(None);
        }
        Ok(Some(materialized_lane_labels))
    }

    fn try_update_anchor_branch_lanes(
        &self,
        connection: &mut SqliteConnection,
        store: &(impl BranchStore + NodeStore),
        session_states: &[(String, SessionState)],
        materialized_lane_labels: BTreeSet<String>,
    ) -> crate::Result<bool> {
        let mode = GraphMode::Anchors;
        let mut materialized_lane_labels = materialized_lane_labels;
        let mut next_lane_y = crate::layout::GRAPH_TOP_Y;
        for (branch, state) in session_states {
            let head_id = store
                .get_branch_head(branch)
                .context(crate::error::StoreSnafu)?;
            let has_visible_nodes = self.branch_has_initial_visible_nodes(store, mode, branch)?;
            let appended = if materialized_lane_labels.contains(branch) {
                if !has_visible_nodes {
                    if !self
                        .delete_materialized_branch_lane_if_isolated(connection, mode, branch)?
                    {
                        return Ok(false);
                    }
                    materialized_lane_labels.remove(branch);
                    continue;
                }
                self.try_update_existing_anchor_branch_lane(
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
                if !has_visible_nodes {
                    continue;
                }
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
        Ok(true)
    }

    fn try_update_existing_anchor_branch_lane(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        input: AppendLinearBranchInput<'_>,
    ) -> crate::Result<bool> {
        let ancestry = store
            .ancestry(input.head_id)
            .context(crate::error::StoreSnafu)?;
        let scoped_ancestry = provider_context_ancestry_nodes(ancestry);
        let Some(tail) =
            self.latest_lane_tail_in_connection(connection, input.mode, input.branch)?
        else {
            return Ok(false);
        };
        let Some(visible_head) = self.first_materialized_lane_ancestry_node(
            connection,
            input.mode,
            input.branch,
            &scoped_ancestry,
        )?
        else {
            return self.replace_anchor_branch_lane_for_context_shift(
                connection,
                store,
                input,
                tail,
                scoped_ancestry,
            );
        };
        if visible_head.x < tail.x {
            if self.lane_suffix_has_retained_downstream_edges(
                connection,
                input.mode,
                input.branch,
                visible_head.x,
            )? {
                return Ok(false);
            }
            self.delete_materialized_lane_suffix(
                connection,
                input.mode,
                input.branch,
                visible_head.x,
            )?;
        }
        self.try_append_anchor_branch_after_row(connection, store, input, visible_head)
    }

    fn replace_anchor_branch_lane_for_context_shift(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        input: AppendLinearBranchInput<'_>,
        tail: MaterializedTailNodeRow,
        scoped_ancestry: Vec<Node>,
    ) -> crate::Result<bool> {
        if self.lane_suffix_has_retained_downstream_edges(
            connection,
            input.mode,
            input.branch,
            i32::MIN,
        )? {
            return Ok(false);
        }
        let context_start_id = context_start_id_from_scoped_ancestry(&scoped_ancestry);
        let visible_chain = scoped_ancestry
            .iter()
            .rev()
            .filter(|node| is_anchor_node(node))
            .cloned()
            .collect::<Vec<_>>();
        if visible_chain.is_empty() {
            return Ok(false);
        }

        let lane_y = tail.lane_y;
        self.delete_materialized_lanes(
            connection,
            input.mode,
            &[LaneRow {
                lane_key: tail.lane_key,
                lane_label: tail.lane_label,
                lane_y,
            }],
        )?;
        self.insert_anchor_branch_lane_nodes(
            connection,
            store,
            &input,
            AnchorBranchLaneInsert {
                lane_y,
                nodes: visible_chain,
                previous: None,
                context_start_id,
            },
        )
    }

    fn refresh_materialized_node_labels(
        &self,
        connection: &mut SqliteConnection,
        store: &(impl BranchStore + NodeStore),
        mode: GraphMode,
        session_states: &[(String, SessionState)],
    ) -> crate::Result<Option<Vec<MaterializedTailNodeRow>>> {
        let mut labels_by_node_id = BTreeMap::<String, Vec<String>>::new();
        for (branch, state) in session_states {
            if !self.branch_has_initial_visible_nodes(store, mode, branch)? {
                continue;
            }
            let head_id = store
                .get_branch_head(branch)
                .context(crate::error::StoreSnafu)?;
            let ancestry = store.ancestry(&head_id).context(crate::error::StoreSnafu)?;
            let Some(row) =
                self.first_materialized_lane_ancestry_node(connection, mode, branch, &ancestry)?
            else {
                return Ok(None);
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
        Ok(Some(materialized_nodes))
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
        let scoped_ancestry = provider_context_ancestry_nodes(ancestry);
        let context_start_id = context_start_id_from_scoped_ancestry(&scoped_ancestry);
        let visible_chain = scoped_ancestry
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

        let branch_label = branch_label(input.branch, input.state);
        self.insert_anchor_branch_lane_nodes(
            connection,
            store,
            &input,
            AnchorBranchLaneInsert {
                lane_y,
                nodes,
                previous: previous.take(),
                context_start_id,
            },
        )?;
        if let Some(row) =
            self.latest_lane_tail_in_connection(connection, input.mode, input.branch)?
        {
            self.update_node_labels(connection, input.mode, &row.node_key, vec![branch_label])?;
        }
        Ok(true)
    }

    fn insert_anchor_branch_lane_nodes(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        input: &AppendLinearBranchInput<'_>,
        lane_insert: AnchorBranchLaneInsert,
    ) -> crate::Result<bool> {
        let AnchorBranchLaneInsert {
            lane_y,
            nodes,
            mut previous,
            context_start_id,
        } = lane_insert;
        let context_start_id = context_start_id.as_deref();
        let lane = GraphViewportLane {
            key: lane_key(input.branch),
            label: input.branch.to_owned(),
            y: lane_y,
        };
        let branch_label = branch_label(input.branch, input.state);
        let appended_len = nodes.len();
        let starts_from_fork = previous.is_some();
        let event_order =
            self.event_order_by_materialized_and_new_nodes(connection, store, input.mode, &nodes)?;
        for (index, node) in nodes.into_iter().enumerate() {
            let candidate = match previous.as_ref() {
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
            let primary_parent_id = previous
                .as_ref()
                .map(|(previous_id, _)| previous_id.as_str())
                .unwrap_or("");
            let Some(point) = self.point_with_merge_parent_column_constraints(
                connection,
                store,
                MergeColumnConstraintInput {
                    mode: input.mode,
                    node: &node,
                    primary_parent_id,
                    point: candidate,
                    event_order: &event_order,
                    reserved_lane_y: Some(lane.y),
                    context_start_id,
                },
            )?
            else {
                return Ok(false);
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
                let edge = if index == 0 && starts_from_fork {
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
                    NodeMergeEdgesInput {
                        mode: input.mode,
                        node: &node,
                        primary_parent_id: previous_id,
                        target: point,
                        context_start_id,
                    },
                )? {
                    return Ok(false);
                }
            } else if !self.insert_node_merge_edges(
                connection,
                store,
                NodeMergeEdgesInput {
                    mode: input.mode,
                    node: &node,
                    primary_parent_id: "",
                    target: point,
                    context_start_id,
                },
            )? {
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
            self.trim_branch_lane_covered_prefix(connection, input.mode, input.branch)?;
            return Ok(true);
        }
        let ancestry = store
            .ancestry(input.head_id)
            .context(crate::error::StoreSnafu)?;
        let context_start_id = merge_parent_context_start_id(input.mode, &ancestry);
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
            let Some(point) = self.point_with_merge_parent_column_constraints(
                connection,
                store,
                MergeColumnConstraintInput {
                    mode: input.mode,
                    node: &node,
                    primary_parent_id: &previous_id,
                    point,
                    event_order: &event_order,
                    reserved_lane_y: None,
                    context_start_id: context_start_id.as_deref(),
                },
            )?
            else {
                return Ok(false);
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
                NodeMergeEdgesInput {
                    mode: input.mode,
                    node: &node,
                    primary_parent_id: &previous_id,
                    target: point,
                    context_start_id: context_start_id.as_deref(),
                },
            )? {
                return Ok(false);
            }
            previous_id = node.id;
            previous_point = point;
        }
        Ok(true)
    }

    fn insert_branch_alias_lane(
        &self,
        connection: &mut SqliteConnection,
        input: AppendLinearBranchInput<'_>,
        lane_y: i32,
        node: &Node,
        source_point: Point,
    ) -> crate::Result<bool> {
        let mut labels = self.materialized_node_label_set(connection, input.mode, &node.id)?;
        labels.insert(branch_label(input.branch, input.state));
        let labels = labels.into_iter().collect::<Vec<_>>();
        let lane = GraphViewportLane {
            key: lane_key(input.branch),
            label: input.branch.to_owned(),
            y: lane_y,
        };
        let point = Point {
            x: source_point.x,
            y: lane_y,
        };
        let viewport_node = graph_viewport_node_from_node(node, point, labels.clone());
        self.insert_node_location(
            connection,
            NodeLocationInsert {
                mode: input.mode,
                node: &viewport_node,
                lane: &lane,
                bounds: node_bounds(&viewport_node),
            },
        )?;
        self.migrate_orphan_occurrences_to_point(connection, input.mode, &node.id, point)?;
        self.update_node_id_labels(connection, input.mode, &node.id, labels)?;
        self.insert_branch_alias_fork_edge(connection, input.mode, node, point)?;
        Ok(true)
    }

    fn insert_branch_alias_fork_edge(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node: &Node,
        point: Point,
    ) -> crate::Result<()> {
        let Some(parent_point) =
            self.materialized_node_point_in_connection(connection, mode, &node.parent)?
        else {
            return Ok(());
        };
        let edge = routed_edge(
            GraphViewportEdgeKind::Fork,
            &node.parent,
            parent_point,
            &node.id,
            point,
            self.next_routed_edge_slot_in_connection(connection, mode, parent_point, point)?,
        );
        self.insert_edge_route(
            connection,
            EdgeRouteInsert {
                mode,
                edge: &edge,
                bounds: edge_bounds(&edge),
            },
        )?;
        self.rebalance_target_port_offsets(connection, mode, point)
    }

    fn migrate_orphan_occurrences_to_point(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        point: Point,
    ) -> crate::Result<()> {
        let lanes = self.orphan_lanes_for_node_in_connection(connection, mode, node_id)?;
        if lanes.is_empty() {
            return Ok(());
        }

        let outgoing_edges =
            self.outgoing_edge_routes_from_lanes(connection, mode, node_id, &lanes)?;
        self.delete_materialized_lanes(connection, mode, &lanes)?;
        self.insert_migrated_outgoing_edge_routes(connection, mode, point, outgoing_edges)
    }

    fn orphan_lanes_for_node_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
    ) -> crate::Result<Vec<LaneRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id))
                    .and(node_locations::lane_key.like("derived:orphan:%")),
            )
            .select((
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
            ))
            .distinct()
            .order(node_locations::lane_y)
            .load::<LaneRow>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    fn outgoing_edge_routes_from_lanes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        lanes: &[LaneRow],
    ) -> crate::Result<Vec<EdgeRouteRow>> {
        let mut outgoing_edges = Vec::new();
        for lane in lanes {
            outgoing_edges.extend(self.outgoing_edge_routes_from_lane_node(
                connection,
                mode,
                node_id,
                lane.lane_y,
            )?);
        }
        Ok(outgoing_edges)
    }

    fn outgoing_edge_routes_from_lane_node(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        lane_y: i32,
    ) -> crate::Result<Vec<EdgeRouteRow>> {
        use console_graph_edge_routes::dsl as edge_routes;

        edge_routes::console_graph_edge_routes
            .filter(
                edge_routes::mode
                    .eq(mode.as_query_value())
                    .and(edge_routes::source_id.eq(node_id))
                    .and(edge_routes::source_y.eq(lane_y)),
            )
            .select((
                edge_routes::edge_key,
                edge_routes::edge_kind,
                edge_routes::source_id,
                edge_routes::target_id,
                edge_routes::source_x,
                edge_routes::source_y,
                edge_routes::target_x,
                edge_routes::target_y,
                edge_routes::route_slot,
                edge_routes::target_port_offset,
            ))
            .load::<EdgeRouteRow>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    fn insert_migrated_outgoing_edge_routes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        point: Point,
        rows: Vec<EdgeRouteRow>,
    ) -> crate::Result<()> {
        for row in rows {
            self.insert_migrated_outgoing_edge_route(connection, mode, point, row)?;
        }
        Ok(())
    }

    fn insert_migrated_outgoing_edge_route(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        point: Point,
        row: EdgeRouteRow,
    ) -> crate::Result<()> {
        let kind = parse_edge_kind(&row.edge_kind)?;
        let target = Point {
            x: row.target_x,
            y: row.target_y,
        };
        let edge = GraphViewportEdge {
            key: edge_key(kind, &row.source_id, point, &row.target_id, target),
            kind,
            source_id: row.source_id,
            target_id: row.target_id,
            source: point,
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
        self.rebalance_target_port_offsets(connection, mode, target)
    }

    fn point_with_merge_parent_column_constraints(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        input: MergeColumnConstraintInput<'_>,
    ) -> crate::Result<Option<Point>> {
        let mut parent_ids = BTreeSet::from([input.primary_parent_id.to_owned()]);
        let mut refreshed_event_order = None;
        let mut x = input.point.x;
        for merge_parent in node_anchor_merge_parents(input.node) {
            let source = match self.ensure_visible_merge_parent_point(
                connection,
                store,
                input.mode,
                merge_parent,
                input.reserved_lane_y,
                input.context_start_id,
            )? {
                MergeParentPoint::Visible(source) => source,
                MergeParentPoint::Skipped => continue,
                MergeParentPoint::Unsupported => return Ok(None),
            };
            if parent_ids.insert(source.node_id.clone()) {
                let event_order = if input.event_order.contains_key(&source.node_id) {
                    input.event_order
                } else {
                    refreshed_event_order.get_or_insert(
                        self.event_order_by_materialized_and_new_nodes(
                            connection,
                            store,
                            input.mode,
                            std::slice::from_ref(input.node),
                        )?,
                    )
                };
                x = x.max(
                    source.point.x
                        + required_column_gap(&source.node_id, &input.node.id, event_order)
                            * GRAPH_COLUMN_WIDTH,
                );
            }
        }
        if let Some(event_order) = refreshed_event_order.as_ref()
            && !input.primary_parent_id.is_empty()
            && let Some(primary_point) = self.materialized_node_point_in_connection(
                connection,
                input.mode,
                input.primary_parent_id,
            )?
        {
            x = x.max(
                primary_point.x
                    + required_column_gap(input.primary_parent_id, &input.node.id, event_order)
                        * GRAPH_COLUMN_WIDTH,
            );
        }
        Ok(Some(Point {
            x,
            y: input.point.y,
        }))
    }

    fn insert_node_merge_edges(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        input: NodeMergeEdgesInput<'_>,
    ) -> crate::Result<bool> {
        let mut parent_ids = BTreeSet::from([input.primary_parent_id.to_owned()]);
        for merge_parent in node_anchor_merge_parents(input.node) {
            let source = match self.ensure_visible_merge_parent_point(
                connection,
                store,
                input.mode,
                merge_parent,
                None,
                input.context_start_id,
            )? {
                MergeParentPoint::Visible(source) => source,
                MergeParentPoint::Skipped => continue,
                MergeParentPoint::Unsupported => return Ok(false),
            };
            if !parent_ids.insert(source.node_id.clone()) {
                continue;
            }
            let edge = routed_edge(
                GraphViewportEdgeKind::MergeParent,
                &source.node_id,
                source.point,
                &input.node.id,
                input.target,
                self.next_routed_edge_slot_in_connection(
                    connection,
                    input.mode,
                    source.point,
                    input.target,
                )?,
            );
            self.insert_edge_route(
                connection,
                EdgeRouteInsert {
                    mode: input.mode,
                    edge: &edge,
                    bounds: edge_bounds(&edge),
                },
            )?;
            self.rebalance_target_port_offsets(connection, input.mode, input.target)?;
        }
        Ok(true)
    }

    fn ensure_visible_merge_parent_point(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        mode: GraphMode,
        merge_parent: &MergeParent,
        reserved_lane_y: Option<i32>,
        context_start_id: Option<&str>,
    ) -> crate::Result<MergeParentPoint> {
        let ancestry = store
            .ancestry(merge_parent.node_id())
            .context(crate::error::StoreSnafu)?;
        let Some(source_index) =
            visible_scoped_merge_parent_source_index(mode, &ancestry, context_start_id)
        else {
            return Ok(MergeParentPoint::Skipped);
        };
        let source = &ancestry[source_index];
        if let Some(point) =
            self.materialized_node_point_in_connection(connection, mode, &source.id)?
        {
            return Ok(MergeParentPoint::Visible(VisibleMergeParentPoint {
                node_id: source.id.clone(),
                point,
            }));
        }
        match self.insert_orphan_merge_parent_lane(
            connection,
            store,
            OrphanMergeParentLaneInput {
                mode,
                ancestry: &ancestry,
                source_index,
                reserved_lane_y,
                context_start_id,
            },
        )? {
            Some(point) => Ok(MergeParentPoint::Visible(point)),
            None => Ok(MergeParentPoint::Unsupported),
        }
    }

    fn insert_orphan_merge_parent_lane(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        input: OrphanMergeParentLaneInput<'_>,
    ) -> crate::Result<Option<VisibleMergeParentPoint>> {
        let Some(orphan) = self.orphan_merge_parent_lane(
            connection,
            input.mode,
            input.ancestry,
            input.source_index,
            input.reserved_lane_y,
            input.context_start_id,
        )?
        else {
            return Ok(None);
        };
        let Some(point) =
            self.insert_orphan_merge_parent_nodes(connection, store, input.mode, &orphan)?
        else {
            return Ok(None);
        };
        Ok(Some(VisibleMergeParentPoint {
            node_id: orphan.source_id,
            point,
        }))
    }

    fn orphan_merge_parent_lane(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        ancestry: &[Node],
        source_index: usize,
        reserved_lane_y: Option<i32>,
        context_start_id: Option<&str>,
    ) -> crate::Result<Option<OrphanMergeParentLane>> {
        let (fork_source, end_index) = self.orphan_merge_parent_fork_source(
            connection,
            mode,
            ancestry,
            source_index,
            context_start_id,
        )?;
        let nodes = visible_orphan_merge_parent_nodes(mode, ancestry, end_index);
        let Some(source_id) = nodes.last().map(|source| source.id.clone()) else {
            return Ok(None);
        };
        let lane = orphan_merge_parent_lane(
            source_id.as_str(),
            self.next_materialized_lane_y_after_reserved(connection, mode, reserved_lane_y)?,
        );
        Ok(Some(OrphanMergeParentLane {
            source_id,
            lane,
            nodes,
            fork_source,
            context_start_id: context_start_id.map(str::to_owned),
        }))
    }

    fn orphan_merge_parent_fork_source(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        ancestry: &[Node],
        source_index: usize,
        context_start_id: Option<&str>,
    ) -> crate::Result<(Option<(String, Point)>, usize)> {
        let end_index =
            scoped_merge_parent_end_index(ancestry, context_start_id).unwrap_or(ancestry.len());
        for (index, node) in ancestry.iter().enumerate().skip(source_index + 1) {
            if index >= end_index {
                break;
            }
            if let Some(point) =
                self.materialized_node_point_in_connection(connection, mode, &node.id)?
            {
                return Ok((Some((node.id.clone(), point)), index));
            }
        }
        Ok((None, end_index))
    }

    fn insert_orphan_merge_parent_nodes(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        mode: GraphMode,
        orphan: &OrphanMergeParentLane,
    ) -> crate::Result<Option<Point>> {
        let event_order =
            self.event_order_by_materialized_and_new_nodes(connection, store, mode, &orphan.nodes)?;
        let mut previous = orphan.fork_source.clone();
        let mut source_point = None;
        for (index, node) in orphan.nodes.iter().enumerate() {
            let point = match previous.as_ref() {
                Some((previous_id, previous_point)) => Point {
                    x: previous_point.x
                        + required_column_gap(previous_id, &node.id, &event_order)
                            * GRAPH_COLUMN_WIDTH,
                    y: orphan.lane.y,
                },
                None => Point {
                    x: GRAPH_LEFT_X,
                    y: orphan.lane.y,
                },
            };
            let primary_parent_id = previous
                .as_ref()
                .map(|(previous_id, _)| previous_id.as_str())
                .unwrap_or("");
            let Some(point) = self.point_with_merge_parent_column_constraints(
                connection,
                store,
                MergeColumnConstraintInput {
                    mode,
                    node,
                    primary_parent_id,
                    point,
                    event_order: &event_order,
                    reserved_lane_y: Some(orphan.lane.y),
                    context_start_id: orphan.context_start_id.as_deref(),
                },
            )?
            else {
                return Ok(None);
            };
            let viewport_node = graph_viewport_node_from_node(node, point, Vec::new());
            self.insert_node_location(
                connection,
                NodeLocationInsert {
                    mode,
                    node: &viewport_node,
                    lane: &orphan.lane,
                    bounds: node_bounds(&viewport_node),
                },
            )?;
            if !self.insert_orphan_merge_parent_node_edges(
                connection,
                store,
                OrphanMergeParentNodeEdgeInput {
                    mode,
                    node,
                    point,
                    previous: previous.as_ref(),
                    first_node: index == 0,
                    force_fork: false,
                    context_start_id: orphan.context_start_id.as_deref(),
                },
            )? {
                return Ok(None);
            }
            if matches!(
                self.try_append_skill_invocation_subtree_in_transaction(
                    connection,
                    store,
                    mode,
                    &node.id,
                    point,
                    &orphan.lane,
                )?,
                SkillSubtreeAppend::Unsupported
            ) {
                return Ok(None);
            }
            source_point = Some(point);
            previous = Some((node.id.clone(), point));
        }
        Ok(source_point)
    }

    fn insert_orphan_merge_parent_node_edges(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        input: OrphanMergeParentNodeEdgeInput<'_>,
    ) -> crate::Result<bool> {
        let Some((previous_id, previous_point)) = input.previous else {
            return self.insert_node_merge_edges(
                connection,
                store,
                NodeMergeEdgesInput {
                    mode: input.mode,
                    node: input.node,
                    primary_parent_id: "",
                    target: input.point,
                    context_start_id: input.context_start_id,
                },
            );
        };
        let edge = if input.force_fork || input.first_node && previous_point.y != input.point.y {
            routed_edge(
                GraphViewportEdgeKind::Fork,
                previous_id,
                *previous_point,
                &input.node.id,
                input.point,
                self.next_routed_edge_slot_in_connection(
                    connection,
                    input.mode,
                    *previous_point,
                    input.point,
                )?,
            )
        } else {
            primary_parent_edge(previous_id, *previous_point, &input.node.id, input.point)
        };
        self.insert_edge_route(
            connection,
            EdgeRouteInsert {
                mode: input.mode,
                edge: &edge,
                bounds: edge_bounds(&edge),
            },
        )?;
        self.insert_node_merge_edges(
            connection,
            store,
            NodeMergeEdgesInput {
                mode: input.mode,
                node: input.node,
                primary_parent_id: previous_id,
                target: input.point,
                context_start_id: input.context_start_id,
            },
        )
    }

    fn rebalance_target_port_offsets(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        target: Point,
    ) -> crate::Result<()> {
        use console_graph_edge_routes::dsl as edge_routes;

        let mut rows = edge_routes::console_graph_edge_routes
            .filter(edge_routes::mode.eq(mode.as_query_value()))
            .filter(edge_routes::target_x.eq(target.x))
            .filter(edge_routes::target_y.eq(target.y))
            .select((
                edge_routes::edge_key,
                edge_routes::edge_kind,
                edge_routes::source_id,
                edge_routes::target_id,
                edge_routes::source_x,
                edge_routes::source_y,
                edge_routes::target_x,
                edge_routes::target_y,
                edge_routes::route_slot,
                edge_routes::target_port_offset,
            ))
            .load::<EdgeRouteRow>(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        rows.sort_by(|left, right| {
            target_port_rebalance_order(&left.edge_kind)
                .cmp(&target_port_rebalance_order(&right.edge_kind))
                .then_with(|| left.edge_key.cmp(&right.edge_key))
        });
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

    fn rebalance_routed_edge_slots(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<()> {
        use console_graph_edge_routes::dsl as edge_routes;

        let mut rows = edge_routes::console_graph_edge_routes
            .filter(edge_routes::mode.eq(mode.as_query_value()))
            .filter(edge_routes::edge_kind.ne("primary_parent"))
            .select((
                edge_routes::edge_key,
                edge_routes::edge_kind,
                edge_routes::source_id,
                edge_routes::target_id,
                edge_routes::source_x,
                edge_routes::source_y,
                edge_routes::target_x,
                edge_routes::target_y,
                edge_routes::route_slot,
                edge_routes::target_port_offset,
            ))
            .load::<EdgeRouteRow>(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        rows.sort_by(|left, right| {
            left.source_y
                .cmp(&right.source_y)
                .then_with(|| {
                    (left.target_y - left.source_y)
                        .signum()
                        .cmp(&(right.target_y - right.source_y).signum())
                })
                .then_with(|| {
                    routed_edge_kind_order(&left.edge_kind)
                        .cmp(&routed_edge_kind_order(&right.edge_kind))
                })
                .then_with(|| left.target_y.cmp(&right.target_y))
                .then_with(|| left.target_x.cmp(&right.target_x))
                .then_with(|| left.edge_key.cmp(&right.edge_key))
        });
        let mut next_slot_by_corridor = BTreeMap::<(i32, i32), i32>::new();
        for row in rows {
            let kind = parse_edge_kind(&row.edge_kind)?;
            let source = Point {
                x: row.source_x,
                y: row.source_y,
            };
            let target = Point {
                x: row.target_x,
                y: row.target_y,
            };
            let direction = (target.y - source.y).signum();
            let next_slot = next_slot_by_corridor
                .entry((source.y, direction))
                .or_default();
            let edge = GraphViewportEdge {
                key: row.edge_key,
                kind,
                source_id: row.source_id,
                target_id: row.target_id,
                source,
                target,
                route_slot: *next_slot,
                target_port_offset: row.target_port_offset,
            };
            *next_slot += 1;
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
        let (source, source_point, nodes): (Option<String>, Option<Point>, Vec<Node>) = match self
            .first_materialized_ancestry_point(
            connection, input.mode, &ancestry, lane_y,
        )? {
            Some((0, source_point)) => {
                return self.insert_branch_alias_lane(
                    connection,
                    input,
                    lane_y,
                    &ancestry[0],
                    source_point,
                );
            }
            Some((source_index, source_point)) => {
                let source = &ancestry[source_index];
                let mut nodes = ancestry[..source_index].to_vec();
                nodes.reverse();
                if nodes.is_empty() || !is_linear_new_nodes(&source.id, &nodes) {
                    return Ok(false);
                }
                (Some(source.id.clone()), Some(source_point), nodes)
            }
            None => {
                let mut nodes = ancestry
                    .iter()
                    .take_while(|node| !node.is_root())
                    .filter(|node| is_visible_mode_node(input.mode, node))
                    .cloned()
                    .collect::<Vec<_>>();
                nodes.reverse();
                if nodes.is_empty() || !initial_visible_lane_is_linear(input.mode, &nodes) {
                    return Ok(false);
                }
                (None, None, nodes)
            }
        };

        let lane = GraphViewportLane {
            key: lane_key(input.branch),
            label: input.branch.to_owned(),
            y: lane_y,
        };
        let branch_label = branch_label(input.branch, input.state);
        let mut previous = source.zip(source_point);
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
            let primary_parent_id = previous
                .as_ref()
                .map(|(previous_id, _)| previous_id.as_str())
                .unwrap_or("");
            let Some(point) = self.point_with_merge_parent_column_constraints(
                connection,
                store,
                MergeColumnConstraintInput {
                    mode: input.mode,
                    node: &node,
                    primary_parent_id,
                    point,
                    event_order: &event_order,
                    reserved_lane_y: Some(lane_y),
                    context_start_id: None,
                },
            )?
            else {
                return Ok(false);
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
                let edge = if index == 0 {
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
                    NodeMergeEdgesInput {
                        mode: input.mode,
                        node: &node,
                        primary_parent_id: previous_id,
                        target: point,
                        context_start_id: None,
                    },
                )? {
                    return Ok(false);
                }
            } else if !self.insert_node_merge_edges(
                connection,
                store,
                NodeMergeEdgesInput {
                    mode: input.mode,
                    node: &node,
                    primary_parent_id: "",
                    target: point,
                    context_start_id: None,
                },
            )? {
                return Ok(false);
            }
            if matches!(
                self.try_append_skill_invocation_subtree_in_transaction(
                    connection, store, input.mode, &node.id, point, &lane,
                )?,
                SkillSubtreeAppend::Unsupported
            ) {
                return Ok(false);
            }
            previous = Some((node.id, point));
        }
        Ok(true)
    }

    fn try_append_skill_invocation_subtree_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &impl NodeStore,
        mode: GraphMode,
        source_id: &str,
        source_point: Point,
        lane: &GraphViewportLane,
    ) -> crate::Result<SkillSubtreeAppend> {
        if mode != GraphMode::All {
            return Ok(SkillSubtreeAppend::Absent);
        }
        let source = store
            .get_node(source_id)
            .context(crate::error::StoreSnafu)?;
        if source.kind.as_tool_uses().is_none() {
            return Ok(SkillSubtreeAppend::Absent);
        }
        let subtrees = visible_skill_invocation_linear_subtrees(
            source_id,
            visible_skill_invocation_subtree_nodes(store, mode, source_id)?,
        );
        let Some(subtrees) = subtrees else {
            return Ok(SkillSubtreeAppend::Unsupported);
        };
        if subtrees.is_empty() {
            return Ok(SkillSubtreeAppend::Absent);
        }

        for nodes in subtrees {
            let (subtree_lane, fork_first_inserted) = match self
                .materialized_skill_subtree_attach_row_in_connection(connection, mode, &nodes)?
            {
                Some((row, fork_first_inserted)) => (
                    GraphViewportLane {
                        key: row.lane_key,
                        label: row.lane_label,
                        y: row.lane_y,
                    },
                    fork_first_inserted,
                ),
                None => {
                    let subtree_source_id = nodes
                        .last()
                        .map(|node| node.id.as_str())
                        .unwrap_or(source_id);
                    (
                        skill_invocation_subtree_lane(
                            subtree_source_id,
                            self.next_materialized_lane_y_after_reserved(
                                connection,
                                mode,
                                Some(lane.y),
                            )?,
                        ),
                        false,
                    )
                }
            };
            let event_order =
                self.event_order_by_materialized_and_new_nodes(connection, store, mode, &nodes)?;
            let mut previous_id = source_id.to_owned();
            let mut previous_point = source_point;
            let mut first_inserted_node = true;
            for node in nodes {
                if let Some(row) = self.materialized_node_row_by_id_on_lane_in_connection(
                    connection,
                    mode,
                    &node.id,
                    &subtree_lane.key,
                )? {
                    let point = Point { x: row.x, y: row.y };
                    previous_id = node.id;
                    previous_point = point;
                    continue;
                }
                let candidate = Point {
                    x: previous_point.x
                        + required_column_gap(&previous_id, &node.id, &event_order)
                            * GRAPH_COLUMN_WIDTH,
                    y: subtree_lane.y,
                };
                let Some(point) = self.point_with_merge_parent_column_constraints(
                    connection,
                    store,
                    MergeColumnConstraintInput {
                        mode,
                        node: &node,
                        primary_parent_id: &previous_id,
                        point: candidate,
                        event_order: &event_order,
                        reserved_lane_y: Some(subtree_lane.y),
                        context_start_id: None,
                    },
                )?
                else {
                    return Ok(SkillSubtreeAppend::Unsupported);
                };
                let viewport_node = graph_viewport_node_from_node(&node, point, Vec::new());
                self.insert_node_location(
                    connection,
                    NodeLocationInsert {
                        mode,
                        node: &viewport_node,
                        lane: &subtree_lane,
                        bounds: node_bounds(&viewport_node),
                    },
                )?;
                let previous = (previous_id.clone(), previous_point);
                if !self.insert_orphan_merge_parent_node_edges(
                    connection,
                    store,
                    OrphanMergeParentNodeEdgeInput {
                        mode,
                        node: &node,
                        point,
                        previous: Some(&previous),
                        first_node: previous_id == source_id,
                        force_fork: first_inserted_node && fork_first_inserted,
                        context_start_id: None,
                    },
                )? {
                    return Ok(SkillSubtreeAppend::Unsupported);
                }
                previous_id = node.id;
                previous_point = point;
                first_inserted_node = false;
            }
        }
        Ok(SkillSubtreeAppend::Applied)
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
            if self.lanes_have_retained_downstream_edges(connection, mode, &removed_lanes)? {
                return Ok(false);
            }
            self.delete_materialized_lanes(connection, mode, &removed_lanes)?;
            self.shift_lanes_after_deletion(connection, mode, &removed_lanes)?;
            materialized_lanes = self.materialized_lanes_in_connection(connection, mode)?;
        }
        let materialized_lane_labels = materialized_lanes
            .iter()
            .filter(|lane| !is_derived_lane_key(&lane.lane_key))
            .map(|lane| lane.lane_label.clone())
            .collect::<BTreeSet<_>>();
        if !existing_branch_lanes_preserve_order(
            session_states,
            &materialized_lanes,
            &materialized_lane_labels,
        ) {
            return Ok(false);
        }

        if !self.try_update_all_branch_lanes(
            connection,
            store,
            session_states,
            materialized_lane_labels,
        )? {
            return Ok(false);
        }
        self.prune_removable_derived_lanes(connection, mode)?;
        self.rebalance_routed_edge_slots(connection, mode)?;
        let Some(materialized_nodes) =
            self.refresh_materialized_node_labels(connection, store, mode, session_states)?
        else {
            return Ok(false);
        };
        let world_max_x = materialized_nodes
            .iter()
            .map(|row| row.x)
            .max()
            .unwrap_or(meta.world_max_x - 120)
            + 120;
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

    fn try_update_all_branch_lanes(
        &self,
        connection: &mut SqliteConnection,
        store: &(impl BranchStore + NodeStore),
        session_states: &[(String, SessionState)],
        materialized_lane_labels: BTreeSet<String>,
    ) -> crate::Result<bool> {
        let mode = GraphMode::All;
        let mut materialized_lane_labels = materialized_lane_labels;
        let mut next_lane_y = crate::layout::GRAPH_TOP_Y;
        for (branch, state) in session_states {
            let head_id = store
                .get_branch_head(branch)
                .context(crate::error::StoreSnafu)?;
            let has_visible_nodes = self.branch_has_initial_visible_nodes(store, mode, branch)?;
            let appended = if materialized_lane_labels.contains(branch) {
                if !has_visible_nodes {
                    if !self
                        .delete_materialized_branch_lane_if_isolated(connection, mode, branch)?
                    {
                        return Ok(false);
                    }
                    materialized_lane_labels.remove(branch);
                    continue;
                }
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
                if !has_visible_nodes {
                    continue;
                }
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
            if materialized_lane_labels.contains(branch) {
                next_lane_y += GRAPH_LANE_HEIGHT;
            }
        }
        Ok(true)
    }

    fn finish_transaction<T>(
        &self,
        result: std::result::Result<T, SnapshotTransactionError>,
    ) -> crate::Result<T> {
        result.map_err(|error| match error {
            SnapshotTransactionError::Query(source) => crate::Error::QueryGraphSnapshotStore {
                path: self.path.as_ref().clone(),
                source,
            },
            SnapshotTransactionError::Operation(error) => error,
            SnapshotTransactionError::RollbackFalse => crate::Error::ConsoleGraphRebuild {
                mode: "unknown",
                source_version: 0,
                message: "unexpected rollback sentinel".to_owned(),
            },
        })
    }

    fn ensure_schema(&self) -> crate::Result<()> {
        let this = self.clone();
        self.with_connection(move |connection| {
            connection
                .run_pending_migrations(CONSOLE_GRAPH_MIGRATIONS)
                .map(|_| ())
                .context(MigrateGraphSnapshotStoreSnafu {
                    path: this.path.as_ref().clone(),
                })?;
            Ok(())
        })
    }

    fn delete_materialization_meta(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<()> {
        use console_graph_materializations::dsl as materializations;

        diesel::delete(
            materializations::console_graph_materializations
                .filter(materializations::mode.eq(mode.as_query_value())),
        )
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    fn put_materialization_meta(
        &self,
        connection: &mut SqliteConnection,
        input: MaterializationMetaInput,
    ) -> crate::Result<()> {
        use console_graph_materializations::dsl as materializations;

        let row = MaterializationInsert {
            mode: input.mode.as_query_value(),
            source_version: input.source_version as i64,
            coordinate_space: COORDINATE_SPACE,
            world_min_x: input.world_min_x,
            world_min_y: input.world_min_y,
            world_max_x: input.world_max_x,
            world_max_y: input.world_max_y,
        };
        let updated_at = jiff::Timestamp::now().to_string();
        diesel::insert_into(materializations::console_graph_materializations)
            .values(&row)
            .on_conflict(materializations::mode)
            .do_update()
            .set((
                materializations::source_version
                    .eq(diesel::upsert::excluded(materializations::source_version)),
                materializations::coordinate_space
                    .eq(diesel::upsert::excluded(materializations::coordinate_space)),
                materializations::world_min_x
                    .eq(diesel::upsert::excluded(materializations::world_min_x)),
                materializations::world_min_y
                    .eq(diesel::upsert::excluded(materializations::world_min_y)),
                materializations::world_max_x
                    .eq(diesel::upsert::excluded(materializations::world_max_x)),
                materializations::world_max_y
                    .eq(diesel::upsert::excluded(materializations::world_max_y)),
                materializations::updated_at.eq(updated_at),
            ))
            .execute(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        Ok(())
    }

    fn insert_node_location(
        &self,
        connection: &mut SqliteConnection,
        insert: NodeLocationInsert<'_>,
    ) -> crate::Result<()> {
        use console_graph_node_locations::dsl as node_locations;

        let labels_json = serde_json::to_string(&insert.node.labels).context(
            ParseGraphSnapshotStoreValueSnafu {
                column: "console_graph_node_locations.labels_json",
            },
        )?;
        diesel::query_dsl::methods::FilterDsl::filter(
            diesel::insert_into(node_locations::console_graph_node_locations)
                .values((
                    node_locations::mode.eq(insert.mode.as_query_value()),
                    node_locations::node_key.eq(&insert.node.key),
                    node_locations::node_id.eq(&insert.node.id),
                    node_locations::node_target.eq(&insert.node.node_target),
                    node_locations::short_id.eq(&insert.node.short_id),
                    node_locations::node_kind.eq(&insert.node.kind),
                    node_locations::summary.eq(&insert.node.summary),
                    node_locations::labels_json.eq(labels_json),
                    node_locations::lane_key.eq(&insert.lane.key),
                    node_locations::lane_label.eq(&insert.lane.label),
                    node_locations::lane_y.eq(insert.lane.y),
                    node_locations::x.eq(insert.node.x),
                    node_locations::y.eq(insert.node.y),
                    node_locations::min_x.eq(insert.bounds.left),
                    node_locations::min_y.eq(insert.bounds.top),
                    node_locations::max_x.eq(insert.bounds.right),
                    node_locations::max_y.eq(insert.bounds.bottom),
                ))
                .on_conflict((node_locations::mode, node_locations::node_key))
                .do_update()
                .set((
                    node_locations::node_id.eq(diesel::upsert::excluded(node_locations::node_id)),
                    node_locations::node_target
                        .eq(diesel::upsert::excluded(node_locations::node_target)),
                    node_locations::short_id.eq(diesel::upsert::excluded(node_locations::short_id)),
                    node_locations::node_kind
                        .eq(diesel::upsert::excluded(node_locations::node_kind)),
                    node_locations::summary.eq(diesel::upsert::excluded(node_locations::summary)),
                    node_locations::labels_json
                        .eq(diesel::upsert::excluded(node_locations::labels_json)),
                    node_locations::lane_key.eq(diesel::upsert::excluded(node_locations::lane_key)),
                    node_locations::lane_label
                        .eq(diesel::upsert::excluded(node_locations::lane_label)),
                    node_locations::lane_y.eq(diesel::upsert::excluded(node_locations::lane_y)),
                    node_locations::x.eq(diesel::upsert::excluded(node_locations::x)),
                    node_locations::y.eq(diesel::upsert::excluded(node_locations::y)),
                    node_locations::min_x.eq(diesel::upsert::excluded(node_locations::min_x)),
                    node_locations::min_y.eq(diesel::upsert::excluded(node_locations::min_y)),
                    node_locations::max_x.eq(diesel::upsert::excluded(node_locations::max_x)),
                    node_locations::max_y.eq(diesel::upsert::excluded(node_locations::max_y)),
                )),
            node_locations::node_id
                .ne(diesel::upsert::excluded(node_locations::node_id))
                .or(node_locations::node_target
                    .ne(diesel::upsert::excluded(node_locations::node_target)))
                .or(node_locations::short_id.ne(diesel::upsert::excluded(node_locations::short_id)))
                .or(node_locations::node_kind
                    .ne(diesel::upsert::excluded(node_locations::node_kind)))
                .or(node_locations::summary.ne(diesel::upsert::excluded(node_locations::summary)))
                .or(node_locations::labels_json
                    .ne(diesel::upsert::excluded(node_locations::labels_json)))
                .or(node_locations::lane_key.ne(diesel::upsert::excluded(node_locations::lane_key)))
                .or(node_locations::lane_label
                    .ne(diesel::upsert::excluded(node_locations::lane_label)))
                .or(node_locations::lane_y.ne(diesel::upsert::excluded(node_locations::lane_y)))
                .or(node_locations::x.ne(diesel::upsert::excluded(node_locations::x)))
                .or(node_locations::y.ne(diesel::upsert::excluded(node_locations::y)))
                .or(node_locations::min_x.ne(diesel::upsert::excluded(node_locations::min_x)))
                .or(node_locations::min_y.ne(diesel::upsert::excluded(node_locations::min_y)))
                .or(node_locations::max_x.ne(diesel::upsert::excluded(node_locations::max_x)))
                .or(node_locations::max_y.ne(diesel::upsert::excluded(node_locations::max_y))),
        )
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
        use console_graph_edge_routes::dsl as edge_routes;

        diesel::query_dsl::methods::FilterDsl::filter(
            diesel::insert_into(edge_routes::console_graph_edge_routes)
                .values((
                    edge_routes::mode.eq(insert.mode.as_query_value()),
                    edge_routes::edge_key.eq(&insert.edge.key),
                    edge_routes::edge_kind.eq(edge_kind_query_value(insert.edge.kind)),
                    edge_routes::source_id.eq(&insert.edge.source_id),
                    edge_routes::target_id.eq(&insert.edge.target_id),
                    edge_routes::source_x.eq(insert.edge.source.x),
                    edge_routes::source_y.eq(insert.edge.source.y),
                    edge_routes::target_x.eq(insert.edge.target.x),
                    edge_routes::target_y.eq(insert.edge.target.y),
                    edge_routes::route_slot.eq(insert.edge.route_slot),
                    edge_routes::target_port_offset.eq(insert.edge.target_port_offset),
                    edge_routes::min_x.eq(insert.bounds.left),
                    edge_routes::min_y.eq(insert.bounds.top),
                    edge_routes::max_x.eq(insert.bounds.right),
                    edge_routes::max_y.eq(insert.bounds.bottom),
                ))
                .on_conflict((edge_routes::mode, edge_routes::edge_key))
                .do_update()
                .set((
                    edge_routes::edge_kind.eq(diesel::upsert::excluded(edge_routes::edge_kind)),
                    edge_routes::source_id.eq(diesel::upsert::excluded(edge_routes::source_id)),
                    edge_routes::target_id.eq(diesel::upsert::excluded(edge_routes::target_id)),
                    edge_routes::source_x.eq(diesel::upsert::excluded(edge_routes::source_x)),
                    edge_routes::source_y.eq(diesel::upsert::excluded(edge_routes::source_y)),
                    edge_routes::target_x.eq(diesel::upsert::excluded(edge_routes::target_x)),
                    edge_routes::target_y.eq(diesel::upsert::excluded(edge_routes::target_y)),
                    edge_routes::route_slot.eq(diesel::upsert::excluded(edge_routes::route_slot)),
                    edge_routes::target_port_offset
                        .eq(diesel::upsert::excluded(edge_routes::target_port_offset)),
                    edge_routes::min_x.eq(diesel::upsert::excluded(edge_routes::min_x)),
                    edge_routes::min_y.eq(diesel::upsert::excluded(edge_routes::min_y)),
                    edge_routes::max_x.eq(diesel::upsert::excluded(edge_routes::max_x)),
                    edge_routes::max_y.eq(diesel::upsert::excluded(edge_routes::max_y)),
                )),
            edge_routes::edge_kind
                .ne(diesel::upsert::excluded(edge_routes::edge_kind))
                .or(edge_routes::source_id.ne(diesel::upsert::excluded(edge_routes::source_id)))
                .or(edge_routes::target_id.ne(diesel::upsert::excluded(edge_routes::target_id)))
                .or(edge_routes::source_x.ne(diesel::upsert::excluded(edge_routes::source_x)))
                .or(edge_routes::source_y.ne(diesel::upsert::excluded(edge_routes::source_y)))
                .or(edge_routes::target_x.ne(diesel::upsert::excluded(edge_routes::target_x)))
                .or(edge_routes::target_y.ne(diesel::upsert::excluded(edge_routes::target_y)))
                .or(edge_routes::route_slot.ne(diesel::upsert::excluded(edge_routes::route_slot)))
                .or(edge_routes::target_port_offset
                    .ne(diesel::upsert::excluded(edge_routes::target_port_offset)))
                .or(edge_routes::min_x.ne(diesel::upsert::excluded(edge_routes::min_x)))
                .or(edge_routes::min_y.ne(diesel::upsert::excluded(edge_routes::min_y)))
                .or(edge_routes::max_x.ne(diesel::upsert::excluded(edge_routes::max_x)))
                .or(edge_routes::max_y.ne(diesel::upsert::excluded(edge_routes::max_y))),
        )
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
        use console_graph_node_locations::dsl as node_locations;

        diesel::update(
            node_locations::console_graph_node_locations.filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_key.eq(node_key))
                    .and(node_locations::labels_json.ne(&labels_json)),
            ),
        )
        .set(node_locations::labels_json.eq(&labels_json))
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    fn update_node_id_labels(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        labels: Vec<String>,
    ) -> crate::Result<()> {
        let labels_json =
            serde_json::to_string(&labels).context(ParseGraphSnapshotStoreValueSnafu {
                column: "console_graph_node_locations.labels_json",
            })?;
        use console_graph_node_locations::dsl as node_locations;

        diesel::update(
            node_locations::console_graph_node_locations.filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id))
                    .and(node_locations::labels_json.ne(&labels_json)),
            ),
        )
        .set(node_locations::labels_json.eq(&labels_json))
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    fn materialized_node_label_set(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
    ) -> crate::Result<BTreeSet<String>> {
        use console_graph_node_locations::dsl as node_locations;

        let rows = node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id)),
            )
            .select(node_locations::labels_json)
            .load::<String>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        let mut labels = BTreeSet::new();
        for labels_json in rows {
            let row_labels = serde_json::from_str::<Vec<String>>(&labels_json).context(
                ParseGraphSnapshotStoreValueSnafu {
                    column: "console_graph_node_locations.labels_json",
                },
            )?;
            labels.extend(row_labels);
        }
        Ok(labels)
    }

    fn delete_materialized_lanes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lanes: &[LaneRow],
    ) -> crate::Result<()> {
        for lane in lanes {
            use console_graph_edge_routes::dsl as edge_routes;
            use console_graph_node_locations::dsl as node_locations;

            diesel::delete(
                edge_routes::console_graph_edge_routes.filter(
                    edge_routes::mode.eq(mode.as_query_value()).and(
                        edge_routes::source_y
                            .eq(lane.lane_y)
                            .or(edge_routes::target_y.eq(lane.lane_y)),
                    ),
                ),
            )
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
            diesel::delete(
                node_locations::console_graph_node_locations.filter(
                    node_locations::mode
                        .eq(mode.as_query_value())
                        .and(node_locations::lane_key.eq(&lane.lane_key)),
                ),
            )
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        }
        Ok(())
    }

    fn delete_materialized_node_occurrences(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        nodes: &[MaterializedTailNodeRow],
    ) -> crate::Result<()> {
        for node in nodes {
            use console_graph_edge_routes::dsl as edge_routes;
            use console_graph_node_locations::dsl as node_locations;

            diesel::delete(
                edge_routes::console_graph_edge_routes.filter(
                    edge_routes::mode.eq(mode.as_query_value()).and(
                        edge_routes::source_id
                            .eq(&node.node_id)
                            .and(edge_routes::source_x.eq(node.x))
                            .and(edge_routes::source_y.eq(node.y))
                            .or(edge_routes::target_id
                                .eq(&node.node_id)
                                .and(edge_routes::target_x.eq(node.x))
                                .and(edge_routes::target_y.eq(node.y))),
                    ),
                ),
            )
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
            diesel::delete(
                node_locations::console_graph_node_locations.filter(
                    node_locations::mode
                        .eq(mode.as_query_value())
                        .and(node_locations::node_key.eq(&node.node_key)),
                ),
            )
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        }
        Ok(())
    }

    fn prune_removable_derived_lanes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<()> {
        let mut lanes = Vec::new();
        for lane in self.materialized_lanes_in_connection(connection, mode)? {
            self.trim_covered_derived_lane_prefix(connection, mode, &lane.lane_key)?;
            let covered = is_derived_lane_key(&lane.lane_key)
                && self.derived_lane_nodes_are_covered_by_branch_lanes(
                    connection,
                    mode,
                    &lane.lane_key,
                )?;
            let should_prune = is_orphan_lane_key(&lane.lane_key)
                && (!self.lane_has_external_outgoing_edge(connection, mode, &lane.lane_key)?
                    || covered);
            let should_prune = should_prune
                || is_skill_invocation_lane_key(&lane.lane_key)
                    && (!self.lane_has_external_edge(connection, mode, &lane.lane_key)? || covered);
            if should_prune {
                if covered {
                    self.migrate_covered_derived_lane_outgoing_edges(
                        connection,
                        mode,
                        &lane.lane_key,
                    )?;
                }
                lanes.push(lane);
            }
        }
        self.delete_materialized_lanes(connection, mode, &lanes)?;
        self.shift_lanes_after_deletion(connection, mode, &lanes)
    }

    fn trim_covered_derived_lane_prefix(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_key: &str,
    ) -> crate::Result<()> {
        if !is_derived_lane_key(lane_key) {
            return Ok(());
        }
        let nodes =
            self.materialized_node_rows_by_lane_key_in_connection(connection, mode, lane_key)?;
        let mut covered_prefix = Vec::new();
        for node in &nodes {
            let Some(cover) =
                self.materialized_branch_node_point_in_connection(connection, mode, &node.node_id)?
            else {
                break;
            };
            covered_prefix.push((node.clone(), cover));
        }
        if covered_prefix.is_empty() || covered_prefix.len() == nodes.len() {
            return Ok(());
        }
        if is_skill_invocation_lane_key(lane_key) && covered_prefix.len() < 2 {
            return Ok(());
        }

        self.migrate_covered_derived_lane_outgoing_edges(connection, mode, lane_key)?;
        self.delete_materialized_node_occurrences(
            connection,
            mode,
            &covered_prefix
                .iter()
                .map(|(node, _)| node.clone())
                .collect::<Vec<_>>(),
        )?;
        let (source, source_point) = covered_prefix.last().expect("prefix is not empty");
        let target = &nodes[covered_prefix.len()];
        let target_point = Point {
            x: target.x,
            y: target.y,
        };
        let edge = routed_edge(
            GraphViewportEdgeKind::Fork,
            &source.node_id,
            *source_point,
            &target.node_id,
            target_point,
            self.next_routed_edge_slot_in_connection(
                connection,
                mode,
                *source_point,
                target_point,
            )?,
        );
        self.insert_edge_route(
            connection,
            EdgeRouteInsert {
                mode,
                edge: &edge,
                bounds: edge_bounds(&edge),
            },
        )?;
        self.rebalance_target_port_offsets(connection, mode, target_point)
    }

    fn trim_branch_lane_covered_prefix(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
    ) -> crate::Result<()> {
        let lane_key = lane_key(branch);
        let nodes =
            self.materialized_node_rows_by_lane_key_in_connection(connection, mode, &lane_key)?;
        if nodes.len() < 2 {
            return Ok(());
        }
        let lane_y = nodes[0].y;
        let mut covered_prefix = Vec::new();
        for node in &nodes {
            let Some(cover) = self.materialized_branch_node_point_before_lane_in_connection(
                connection,
                mode,
                &node.node_id,
                lane_y,
            )?
            else {
                break;
            };
            covered_prefix.push((node.clone(), cover));
        }
        if covered_prefix.is_empty() {
            return Ok(());
        }

        if covered_prefix.len() == nodes.len() {
            self.trim_fully_covered_branch_lane(connection, mode, &nodes, &covered_prefix)
        } else {
            self.trim_partially_covered_branch_lane_prefix(
                connection,
                mode,
                &nodes,
                &covered_prefix,
            )
        }
    }

    fn trim_fully_covered_branch_lane(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        nodes: &[MaterializedTailNodeRow],
        covered_prefix: &[(MaterializedTailNodeRow, Point)],
    ) -> crate::Result<()> {
        let Some((alias, _)) = covered_prefix.last() else {
            return Ok(());
        };
        let incoming = self.primary_incoming_edge_to_node_occurrence(
            connection,
            mode,
            &alias.node_id,
            alias.x,
            alias.y,
        )?;
        self.delete_materialized_node_occurrences(
            connection,
            mode,
            &nodes[..nodes.len().saturating_sub(1)],
        )?;
        let Some(incoming) = incoming else {
            return Ok(());
        };
        let Some(source) = self.materialized_branch_node_point_before_lane_in_connection(
            connection,
            mode,
            &incoming.source_id,
            alias.y,
        )?
        else {
            return Ok(());
        };
        let target = Point {
            x: alias.x,
            y: alias.y,
        };
        self.insert_trimmed_branch_fork_edge(
            connection,
            mode,
            &incoming.source_id,
            source,
            alias,
            target,
        )
    }

    fn trim_partially_covered_branch_lane_prefix(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        nodes: &[MaterializedTailNodeRow],
        covered_prefix: &[(MaterializedTailNodeRow, Point)],
    ) -> crate::Result<()> {
        self.delete_materialized_node_occurrences(
            connection,
            mode,
            &covered_prefix
                .iter()
                .map(|(node, _)| node.clone())
                .collect::<Vec<_>>(),
        )?;
        let (source, source_point) = covered_prefix.last().expect("prefix is not empty");
        let target = &nodes[covered_prefix.len()];
        let target_point = Point {
            x: target.x,
            y: target.y,
        };
        self.insert_trimmed_branch_fork_edge(
            connection,
            mode,
            &source.node_id,
            *source_point,
            target,
            target_point,
        )
    }

    fn insert_trimmed_branch_fork_edge(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        source_id: &str,
        source: Point,
        target: &MaterializedTailNodeRow,
        target_point: Point,
    ) -> crate::Result<()> {
        let edge = routed_edge(
            GraphViewportEdgeKind::Fork,
            source_id,
            source,
            &target.node_id,
            target_point,
            self.next_routed_edge_slot_in_connection(connection, mode, source, target_point)?,
        );
        self.insert_edge_route(
            connection,
            EdgeRouteInsert {
                mode,
                edge: &edge,
                bounds: edge_bounds(&edge),
            },
        )?;
        self.rebalance_target_port_offsets(connection, mode, target_point)
    }

    fn migrate_covered_derived_lane_outgoing_edges(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_key: &str,
    ) -> crate::Result<()> {
        let edges = self.materialized_edge_route_rows_in_connection(connection, mode)?;
        let nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        let mut rows = Vec::new();
        for edge in edges {
            if !node_point_on_lane(
                &nodes,
                lane_key,
                &edge.source_id,
                Point {
                    x: edge.source_x,
                    y: edge.source_y,
                },
            ) || node_point_on_lane(
                &nodes,
                lane_key,
                &edge.target_id,
                Point {
                    x: edge.target_x,
                    y: edge.target_y,
                },
            ) {
                continue;
            }
            for cover in nodes.iter().filter(|node| {
                node.node_id == edge.source_id
                    && node.lane_key != lane_key
                    && !is_derived_orphan_or_skill_lane(&node.lane_key)
            }) {
                rows.push((
                    edge.clone(),
                    Point {
                        x: cover.x,
                        y: cover.y,
                    },
                ));
            }
        }
        rows.sort_by(|(left_edge, left_cover), (right_edge, right_cover)| {
            left_edge
                .edge_key
                .cmp(&right_edge.edge_key)
                .then_with(|| left_cover.y.cmp(&right_cover.y))
                .then_with(|| left_cover.x.cmp(&right_cover.x))
        });
        for (row, source) in rows {
            let kind = parse_edge_kind(&row.edge_kind)?;
            let target = Point {
                x: row.target_x,
                y: row.target_y,
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
            self.rebalance_target_port_offsets(connection, mode, target)?;
        }
        Ok(())
    }

    fn lane_has_external_outgoing_edge(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_key: &str,
    ) -> crate::Result<bool> {
        let edges = self.materialized_edge_route_rows_in_connection(connection, mode)?;
        let nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        Ok(edges.iter().any(|edge| {
            node_point_on_lane(
                &nodes,
                lane_key,
                &edge.source_id,
                Point {
                    x: edge.source_x,
                    y: edge.source_y,
                },
            ) && !node_point_on_lane(
                &nodes,
                lane_key,
                &edge.target_id,
                Point {
                    x: edge.target_x,
                    y: edge.target_y,
                },
            )
        }))
    }

    fn lane_has_external_edge(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_key: &str,
    ) -> crate::Result<bool> {
        let edges = self.materialized_edge_route_rows_in_connection(connection, mode)?;
        let nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        Ok(edges.iter().any(|edge| {
            let source_on_lane = node_point_on_lane(
                &nodes,
                lane_key,
                &edge.source_id,
                Point {
                    x: edge.source_x,
                    y: edge.source_y,
                },
            );
            let target_on_lane = node_point_on_lane(
                &nodes,
                lane_key,
                &edge.target_id,
                Point {
                    x: edge.target_x,
                    y: edge.target_y,
                },
            );
            source_on_lane != target_on_lane
        }))
    }

    fn derived_lane_nodes_are_covered_by_branch_lanes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_key: &str,
    ) -> crate::Result<bool> {
        let nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        Ok(nodes
            .iter()
            .filter(|node| node.lane_key == lane_key)
            .all(|node| {
                nodes.iter().any(|cover| {
                    cover.node_id == node.node_id
                        && cover.lane_key != node.lane_key
                        && !is_derived_orphan_or_skill_lane(&cover.lane_key)
                })
            }))
    }

    fn clear_materialized_mode_facts(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<()> {
        use console_graph_edge_routes::dsl as edge_routes;
        use console_graph_node_locations::dsl as node_locations;

        diesel::delete(
            edge_routes::console_graph_edge_routes
                .filter(edge_routes::mode.eq(mode.as_query_value())),
        )
        .execute(&mut *connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        diesel::delete(
            node_locations::console_graph_node_locations
                .filter(node_locations::mode.eq(mode.as_query_value())),
        )
        .execute(&mut *connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    fn lane_suffix_has_retained_downstream_edges(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
        head_x: i32,
    ) -> crate::Result<bool> {
        let branch_lane_key = lane_key(branch);
        let edges = self.materialized_edge_route_rows_in_connection(connection, mode)?;
        let nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        Ok(edges.iter().any(|edge| {
            node_point_on_lane_suffix(
                &nodes,
                &branch_lane_key,
                head_x,
                &edge.source_id,
                Point {
                    x: edge.source_x,
                    y: edge.source_y,
                },
            ) && !node_point_on_lane_suffix(
                &nodes,
                &branch_lane_key,
                head_x,
                &edge.target_id,
                Point {
                    x: edge.target_x,
                    y: edge.target_y,
                },
            ) && !node_point_on_derived_lane(
                &nodes,
                &edge.target_id,
                Point {
                    x: edge.target_x,
                    y: edge.target_y,
                },
            )
        }))
    }

    fn lanes_have_retained_downstream_edges(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lanes: &[LaneRow],
    ) -> crate::Result<bool> {
        for lane in lanes {
            if self.lane_suffix_has_retained_downstream_edges(
                connection,
                mode,
                &lane.lane_label,
                i32::MIN,
            )? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn delete_materialized_lane_suffix(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
        head_x: i32,
    ) -> crate::Result<()> {
        let branch_lane_key = lane_key(branch);
        let edges = self.materialized_edge_route_rows_in_connection(connection, mode)?;
        let nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        let edge_keys = edges
            .iter()
            .filter(|edge| {
                node_point_on_lane_suffix(
                    &nodes,
                    &branch_lane_key,
                    head_x,
                    &edge.source_id,
                    Point {
                        x: edge.source_x,
                        y: edge.source_y,
                    },
                ) || node_point_on_lane_suffix(
                    &nodes,
                    &branch_lane_key,
                    head_x,
                    &edge.target_id,
                    Point {
                        x: edge.target_x,
                        y: edge.target_y,
                    },
                )
            })
            .map(|edge| edge.edge_key.clone())
            .collect::<Vec<_>>();
        use console_graph_edge_routes::dsl as edge_routes;
        use console_graph_node_locations::dsl as node_locations;

        for edge_key in edge_keys {
            diesel::delete(
                edge_routes::console_graph_edge_routes.filter(
                    edge_routes::mode
                        .eq(mode.as_query_value())
                        .and(edge_routes::edge_key.eq(edge_key)),
                ),
            )
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        }

        diesel::delete(
            node_locations::console_graph_node_locations.filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::lane_key.eq(&branch_lane_key))
                    .and(node_locations::x.gt(head_x)),
            ),
        )
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
        use console_graph_node_locations::dsl as node_locations;

        let rows = node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::lane_key.eq(&lane.lane_key)),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::x,
                node_locations::y,
                node_locations::lane_y,
                node_locations::min_y,
                node_locations::max_y,
            ))
            .load::<(String, String, i32, i32, i32, i32, i32)>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        for (node_key, node_id, x, y, lane_y, min_y, max_y) in rows {
            let next_y = y - delta;
            diesel::update(
                node_locations::console_graph_node_locations.filter(
                    node_locations::mode
                        .eq(mode.as_query_value())
                        .and(node_locations::node_key.eq(&node_key)),
                ),
            )
            .set((
                node_locations::node_key.eq(format!("node:{node_id}:{x}:{next_y}")),
                node_locations::lane_y.eq(lane_y - delta),
                node_locations::y.eq(next_y),
                node_locations::min_y.eq(min_y - delta),
                node_locations::max_y.eq(max_y - delta),
            ))
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        }
        Ok(())
    }

    fn shift_lane_edges(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_y: i32,
        delta: i32,
    ) -> crate::Result<()> {
        use console_graph_edge_routes::dsl as edge_routes;

        let rows = edge_routes::console_graph_edge_routes
            .filter(
                edge_routes::mode.eq(mode.as_query_value()).and(
                    edge_routes::source_y
                        .eq(lane_y)
                        .or(edge_routes::target_y.eq(lane_y)),
                ),
            )
            .select((
                edge_routes::edge_key,
                edge_routes::edge_kind,
                edge_routes::source_id,
                edge_routes::target_id,
                edge_routes::source_x,
                edge_routes::source_y,
                edge_routes::target_x,
                edge_routes::target_y,
                edge_routes::route_slot,
                edge_routes::target_port_offset,
            ))
            .load::<EdgeRouteRow>(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        for row in rows {
            diesel::delete(
                edge_routes::console_graph_edge_routes.filter(
                    edge_routes::mode
                        .eq(mode.as_query_value())
                        .and(edge_routes::edge_key.eq(&row.edge_key)),
                ),
            )
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
        let this = self.clone();
        self.with_connection(move |connection| {
            this.latest_materialization_row_in_connection(connection, mode)
        })
    }

    fn latest_materialization_row_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<Option<MaterializationRow>> {
        use console_graph_materializations::dsl as materializations;

        materializations::console_graph_materializations
            .filter(
                materializations::mode
                    .eq(mode.as_query_value())
                    .and(materializations::coordinate_space.eq(COORDINATE_SPACE)),
            )
            .select((
                materializations::source_version,
                materializations::world_min_x,
                materializations::world_min_y,
                materializations::world_max_x,
                materializations::world_max_y,
            ))
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
        self.latest_lane_tail_by_key_in_connection(connection, mode, &lane_key(branch))
    }

    fn materialized_lane_node_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
        node_id: &str,
    ) -> crate::Result<Option<MaterializedTailNodeRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::lane_key.eq(lane_key(branch)))
                    .and(node_locations::node_id.eq(node_id)),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
                node_locations::x,
                node_locations::y,
            ))
            .order((node_locations::x.desc(), node_locations::node_key.desc()))
            .limit(1)
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
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(node_locations::mode.eq(mode.as_query_value()))
            .select((
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
            ))
            .distinct()
            .order((node_locations::lane_y, node_locations::lane_key))
            .load::<LaneRow>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    fn next_materialized_lane_y(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<i32> {
        Ok(self
            .materialized_lanes_in_connection(connection, mode)?
            .iter()
            .map(|lane| lane.lane_y)
            .max()
            .unwrap_or(crate::layout::GRAPH_TOP_Y - GRAPH_LANE_HEIGHT)
            + GRAPH_LANE_HEIGHT)
    }

    fn next_materialized_lane_y_after_reserved(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        reserved_lane_y: Option<i32>,
    ) -> crate::Result<i32> {
        let next_y = self.next_materialized_lane_y(connection, mode)?;
        Ok(reserved_lane_y
            .map(|lane_y| next_y.max(lane_y + GRAPH_LANE_HEIGHT))
            .unwrap_or(next_y))
    }

    fn first_materialized_ancestry_point(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        ancestry: &[Node],
        before_lane_y: i32,
    ) -> crate::Result<Option<(usize, Point)>> {
        for (index, node) in ancestry.iter().enumerate() {
            let Some(row) = self
                .materialized_non_skill_node_row_by_id_in_connection(connection, mode, &node.id)?
            else {
                continue;
            };
            if row.y >= before_lane_y || is_orphan_lane_key(&row.lane_key) {
                continue;
            }
            return Ok(Some((index, Point { x: row.x, y: row.y })));
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

    fn materialized_skill_subtree_attach_row_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        nodes: &[Node],
    ) -> crate::Result<Option<(MaterializedTailNodeRow, bool)>> {
        for node in nodes.iter().rev() {
            let Some(row) = self.materialized_node_row_by_id_with_lane_prefix_in_connection(
                connection,
                mode,
                &node.id,
                DERIVED_SKILL_LANE_KEY_PREFIX,
            )?
            else {
                continue;
            };
            let Some(tail) =
                self.latest_lane_tail_by_key_in_connection(connection, mode, &row.lane_key)?
            else {
                continue;
            };
            let fork_first_inserted = tail.node_key != row.node_key;
            return Ok(Some((row, fork_first_inserted)));
        }
        Ok(None)
    }

    fn latest_lane_tail_by_key_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_key: &str,
    ) -> crate::Result<Option<MaterializedTailNodeRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::lane_key.eq(lane_key)),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
                node_locations::x,
                node_locations::y,
            ))
            .order((node_locations::x.desc(), node_locations::node_key.desc()))
            .limit(1)
            .get_result::<MaterializedTailNodeRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    fn materialized_node_row_by_id_on_lane_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        lane_key: &str,
    ) -> crate::Result<Option<MaterializedTailNodeRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id))
                    .and(node_locations::lane_key.eq(lane_key)),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
                node_locations::x,
                node_locations::y,
            ))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .limit(1)
            .get_result::<MaterializedTailNodeRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    fn materialized_node_row_by_id_with_lane_prefix_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        lane_key_prefix: &str,
    ) -> crate::Result<Option<MaterializedTailNodeRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id))
                    .and(node_locations::lane_key.like(format!("{lane_key_prefix}%"))),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
                node_locations::x,
                node_locations::y,
            ))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .limit(1)
            .get_result::<MaterializedTailNodeRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    fn materialized_non_skill_node_row_by_id_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
    ) -> crate::Result<Option<MaterializedTailNodeRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id))
                    .and(node_locations::lane_key.not_like("derived:skill:%")),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
                node_locations::x,
                node_locations::y,
            ))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .limit(1)
            .get_result::<MaterializedTailNodeRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    fn materialized_node_rows_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<Vec<MaterializedTailNodeRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(node_locations::mode.eq(mode.as_query_value()))
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
                node_locations::x,
                node_locations::y,
            ))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .load::<MaterializedTailNodeRow>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    fn materialized_node_rows_by_lane_key_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_key: &str,
    ) -> crate::Result<Vec<MaterializedTailNodeRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::lane_key.eq(lane_key)),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
                node_locations::x,
                node_locations::y,
            ))
            .order((node_locations::x, node_locations::node_key))
            .load::<MaterializedTailNodeRow>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    fn materialized_branch_node_point_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
    ) -> crate::Result<Option<Point>> {
        use console_graph_node_locations::dsl as node_locations;

        let row = node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id))
                    .and(node_locations::lane_key.not_like("derived:orphan:%"))
                    .and(node_locations::lane_key.not_like("derived:skill:%")),
            )
            .select((node_locations::x, node_locations::y))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .limit(1)
            .get_result::<MaterializedNodePointRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        Ok(row.map(|row| Point { x: row.x, y: row.y }))
    }

    fn materialized_branch_node_point_before_lane_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        before_lane_y: i32,
    ) -> crate::Result<Option<Point>> {
        use console_graph_node_locations::dsl as node_locations;

        let row = node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id))
                    .and(node_locations::y.lt(before_lane_y))
                    .and(node_locations::lane_key.not_like("derived:orphan:%"))
                    .and(node_locations::lane_key.not_like("derived:skill:%")),
            )
            .select((node_locations::x, node_locations::y))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .limit(1)
            .get_result::<MaterializedNodePointRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        Ok(row.map(|row| Point { x: row.x, y: row.y }))
    }

    fn primary_incoming_edge_to_node_occurrence(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        x: i32,
        y: i32,
    ) -> crate::Result<Option<EdgeRouteRow>> {
        use console_graph_edge_routes::dsl as edge_routes;

        edge_routes::console_graph_edge_routes
            .filter(
                edge_routes::mode
                    .eq(mode.as_query_value())
                    .and(edge_routes::edge_kind.eq("primary_parent"))
                    .and(edge_routes::target_id.eq(node_id))
                    .and(edge_routes::target_x.eq(x))
                    .and(edge_routes::target_y.eq(y)),
            )
            .select((
                edge_routes::edge_key,
                edge_routes::edge_kind,
                edge_routes::source_id,
                edge_routes::target_id,
                edge_routes::source_x,
                edge_routes::source_y,
                edge_routes::target_x,
                edge_routes::target_y,
                edge_routes::route_slot,
                edge_routes::target_port_offset,
            ))
            .limit(1)
            .get_result::<EdgeRouteRow>(connection)
            .optional()
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
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id)),
            )
            .select((node_locations::x, node_locations::y))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .limit(1)
            .get_result::<MaterializedNodePointRow>(connection)
            .optional()
            .map(|row| row.map(|row| Point { x: row.x, y: row.y }))
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    fn materialized_node_reference_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        target: &str,
    ) -> crate::Result<Option<MaterializedNodeReference>> {
        if self
            .latest_materialization_row_in_connection(connection, mode)?
            .is_none()
        {
            return Ok(None);
        }
        use console_graph_node_locations::dsl as node_locations;

        let row = node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_target.eq(target)),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::node_target,
                node_locations::short_id,
                node_locations::node_kind,
                node_locations::summary,
                node_locations::labels_json,
                node_locations::x,
                node_locations::y,
            ))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .limit(1)
            .get_result::<NodeLocationRow>(connection)
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

    fn materialized_node_points_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_ids: &BTreeSet<String>,
    ) -> crate::Result<BTreeMap<String, Point>> {
        if self
            .latest_materialization_row_in_connection(connection, mode)?
            .is_none()
        {
            return Ok(BTreeMap::new());
        }
        let mut points = BTreeMap::new();
        for node_id in node_ids {
            if let Some(point) =
                self.materialized_node_point_in_connection(connection, mode, node_id)?
            {
                points.insert(node_id.clone(), point);
            }
        }
        Ok(points)
    }

    fn materialized_edge_count_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<usize> {
        use console_graph_edge_routes::dsl as edge_routes;

        edge_routes::console_graph_edge_routes
            .filter(edge_routes::mode.eq(mode.as_query_value()))
            .select(diesel::dsl::count(edge_routes::edge_key).aggregate_distinct())
            .get_result::<i64>(connection)
            .map(|count| count.max(0) as usize)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    fn materialized_edge_route_rows_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<Vec<EdgeRouteRow>> {
        use console_graph_edge_routes::dsl as edge_routes;

        edge_routes::console_graph_edge_routes
            .filter(edge_routes::mode.eq(mode.as_query_value()))
            .select((
                edge_routes::edge_key,
                edge_routes::edge_kind,
                edge_routes::source_id,
                edge_routes::target_id,
                edge_routes::source_x,
                edge_routes::source_y,
                edge_routes::target_x,
                edge_routes::target_y,
                edge_routes::route_slot,
                edge_routes::target_port_offset,
            ))
            .load::<EdgeRouteRow>(connection)
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
        Ok(self
            .materialized_edge_route_rows_in_connection(connection, mode)?
            .into_iter()
            .filter(|edge| {
                edge.edge_kind != "primary_parent"
                    && edge.source_y == source.y
                    && (edge.target_y - edge.source_y).signum() == direction
            })
            .map(|edge| edge.route_slot + 1)
            .max()
            .unwrap_or(0))
    }

    fn viewport_from_row(
        &self,
        connection: &mut SqliteConnection,
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
            lanes: self.viewport_lanes(connection, mode, bounds)?,
            nodes: self.viewport_nodes(connection, mode, bounds)?,
            edges: self.viewport_edges(connection, mode, bounds)?,
        }))
    }

    fn viewport_diff_from_row(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        meta: MaterializationRow,
        request: GraphViewportDiffRequest,
    ) -> crate::Result<Option<GraphViewportDiffResponse>> {
        let previous = self
            .viewport_from_row(connection, mode, meta.clone(), request.previous)?
            .expect("viewport metadata should produce a response");
        let current = self
            .viewport_from_row(connection, mode, meta, request.current)?
            .expect("viewport metadata should produce a response");
        Ok(Some(diff_graph_viewport_responses(
            previous,
            current,
            request.known.as_ref(),
        )))
    }

    fn viewport_lanes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        bounds: ViewportItemBounds,
    ) -> crate::Result<Vec<GraphViewportLane>> {
        use console_graph_node_locations::dsl as node_locations;

        if bounds.right < 0 || bounds.left > crate::layout::GRAPH_LEFT_X {
            return Ok(Vec::new());
        }
        let lane_top = bounds.top.saturating_sub(24);
        let lane_bottom = bounds.bottom.saturating_add(24);
        let rows = node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::lane_y.le(lane_bottom))
                    .and(node_locations::lane_y.ge(lane_top)),
            )
            .select((
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
            ))
            .distinct()
            .order((node_locations::lane_y, node_locations::lane_key))
            .load::<LaneRow>(connection)
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
        connection: &mut SqliteConnection,
        mode: GraphMode,
        bounds: ViewportItemBounds,
    ) -> crate::Result<Vec<GraphViewportNode>> {
        use console_graph_node_locations::dsl as node_locations;

        let rows = node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::min_x.le(bounds.right))
                    .and(node_locations::max_x.ge(bounds.left))
                    .and(node_locations::min_y.le(bounds.bottom))
                    .and(node_locations::max_y.ge(bounds.top)),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::node_target,
                node_locations::short_id,
                node_locations::node_kind,
                node_locations::summary,
                node_locations::labels_json,
                node_locations::x,
                node_locations::y,
            ))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .load::<NodeLocationRow>(connection)
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
        connection: &mut SqliteConnection,
        mode: GraphMode,
        bounds: ViewportItemBounds,
    ) -> crate::Result<Vec<GraphViewportEdge>> {
        use console_graph_edge_routes::dsl as edge_routes;

        let rows = edge_routes::console_graph_edge_routes
            .filter(
                edge_routes::mode
                    .eq(mode.as_query_value())
                    .and(edge_routes::min_x.le(bounds.right))
                    .and(edge_routes::max_x.ge(bounds.left))
                    .and(edge_routes::min_y.le(bounds.bottom))
                    .and(edge_routes::max_y.ge(bounds.top)),
            )
            .select((
                edge_routes::edge_key,
                edge_routes::edge_kind,
                edge_routes::source_id,
                edge_routes::target_id,
                edge_routes::source_x,
                edge_routes::source_y,
                edge_routes::target_x,
                edge_routes::target_y,
                edge_routes::route_slot,
                edge_routes::target_port_offset,
            ))
            .order((
                edge_routes::min_y,
                edge_routes::min_x,
                edge_routes::edge_key,
            ))
            .load::<EdgeRouteRow>(connection)
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

    fn with_connection<T, F>(&self, operation: F) -> crate::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut SqliteConnection) -> crate::Result<T> + Send + 'static,
    {
        let path = self.path.as_ref().clone();
        self.database.with_sync_connection(
            operation,
            |source| crate::Error::Store { source },
            |source| crate::Error::QueryGraphSnapshotStore { path, source },
        )
    }

    #[cfg(test)]
    pub(crate) fn with_connection_for_tests<T, F>(&self, operation: F) -> crate::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut SqliteConnection) -> crate::Result<T> + Send + 'static,
    {
        self.with_connection(operation)
    }
}

pub(crate) fn database_path(dir: impl AsRef<Path>) -> PathBuf {
    dir.as_ref().join(SQLITE_DATABASE_FILE_NAME)
}

fn full_layout_materialization_lane(
    lane: &GraphViewportLane,
    nodes_by_y: &BTreeMap<i32, Vec<&GraphViewportNode>>,
    branch_labels: &BTreeSet<String>,
) -> GraphViewportLane {
    if branch_labels.contains(&lane.label) {
        return lane.clone();
    }
    let derived_prefix = if lane.label.starts_with("orphan ") {
        Some(DERIVED_ORPHAN_LANE_KEY_PREFIX)
    } else if lane.label.starts_with("skill ") {
        Some(DERIVED_SKILL_LANE_KEY_PREFIX)
    } else {
        None
    };
    let Some(prefix) = derived_prefix else {
        return lane.clone();
    };
    let Some(source_id) = nodes_by_y
        .get(&lane.y)
        .and_then(|nodes| nodes.iter().max_by_key(|node| node.x))
        .map(|node| node.id.as_str())
    else {
        return lane.clone();
    };
    GraphViewportLane {
        key: format!("{prefix}{source_id}"),
        label: lane.label.clone(),
        y: lane.y,
    }
}

fn drop_stale_snapshot_materialization_tables(path: &Path) -> crate::Result<()> {
    if !path.is_file() {
        return Ok(());
    }
    let database =
        SqliteDatabase::open_unshared_file_path(path).context(crate::error::StoreSnafu)?;
    let inspect_path = path.to_owned();
    let error_path = inspect_path.clone();
    let should_recreate = database.with_sync_connection(
        move |connection| {
            Ok(
                !materialization_tables_have_current_schema(connection, &inspect_path)?
                    || any_sqlite_table_exists(
                        connection,
                        &inspect_path,
                        LEGACY_MATERIALIZATION_TABLES,
                    )?,
            )
        },
        |source| crate::Error::Store { source },
        |source| crate::Error::QueryGraphSnapshotStore {
            path: error_path,
            source,
        },
    )?;
    drop(database);

    if should_recreate {
        fs::remove_file(path).context(ManageGraphSnapshotStoreSnafu {
            path: path.to_owned(),
        })?;
    }
    Ok(())
}

fn materialization_tables_have_current_schema(
    connection: &mut SqliteConnection,
    path: &Path,
) -> crate::Result<bool> {
    Ok(
        (!sqlite_table_exists(connection, path, "console_graph_materializations")?
            || materializations_have_source_version_column(connection))
            && (!sqlite_table_exists(connection, path, "console_graph_node_locations")?
                || node_locations_have_node_target_column(connection))
            && (!sqlite_table_exists(connection, path, "console_graph_edge_routes")?
                || edge_routes_have_edge_kind_column(connection)),
    )
}

fn sqlite_table_exists(
    connection: &mut SqliteConnection,
    path: &Path,
    table: &str,
) -> crate::Result<bool> {
    let count = sqlite_master::table
        .filter(sqlite_master::object_type.eq("table"))
        .filter(sqlite_master::name.eq(table))
        .count()
        .get_result::<i64>(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(count > 0)
}

fn any_sqlite_table_exists(
    connection: &mut SqliteConnection,
    path: &Path,
    tables: &[&str],
) -> crate::Result<bool> {
    for table in tables {
        if sqlite_table_exists(connection, path, table)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn materializations_have_source_version_column(connection: &mut SqliteConnection) -> bool {
    console_graph_materializations::table
        .select(console_graph_materializations::source_version)
        .limit(0)
        .load::<i64>(connection)
        .is_ok()
}

fn node_locations_have_node_target_column(connection: &mut SqliteConnection) -> bool {
    console_graph_node_locations::table
        .select(console_graph_node_locations::node_target)
        .limit(0)
        .load::<String>(connection)
        .is_ok()
}

fn edge_routes_have_edge_kind_column(connection: &mut SqliteConnection) -> bool {
    console_graph_edge_routes::table
        .select(console_graph_edge_routes::edge_kind)
        .limit(0)
        .load::<String>(connection)
        .is_ok()
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

fn is_visible_mode_node(mode: GraphMode, node: &Node) -> bool {
    !node.is_root() && (mode == GraphMode::All || is_anchor_node(node))
}

fn merge_parent_context_start_id(mode: GraphMode, ancestry: &[Node]) -> Option<String> {
    (mode == GraphMode::Anchors)
        .then(|| {
            context_start_id_from_scoped_ancestry(&provider_context_ancestry_nodes(
                ancestry.to_vec(),
            ))
        })
        .flatten()
}

fn context_start_id_from_scoped_ancestry(scoped_ancestry: &[Node]) -> Option<String> {
    scoped_ancestry.last().map(|node| node.id.clone())
}

fn visible_scoped_merge_parent_source_index(
    mode: GraphMode,
    ancestry: &[Node],
    context_start_id: Option<&str>,
) -> Option<usize> {
    let end_index = scoped_merge_parent_end_index(ancestry, context_start_id)?;
    ancestry[..=end_index]
        .iter()
        .position(|node| is_visible_mode_node(mode, node))
}

fn scoped_merge_parent_end_index(
    ancestry: &[Node],
    context_start_id: Option<&str>,
) -> Option<usize> {
    match context_start_id {
        Some(context_start_id) => ancestry
            .iter()
            .position(|node| !node.is_root() && node.id == context_start_id),
        None => ancestry.iter().position(|node| node.is_root()),
    }
}

fn is_orphan_lane_key(key: &str) -> bool {
    key.starts_with(DERIVED_ORPHAN_LANE_KEY_PREFIX)
}

fn is_skill_invocation_lane_key(key: &str) -> bool {
    key.starts_with(DERIVED_SKILL_LANE_KEY_PREFIX)
}

fn is_derived_lane_key(key: &str) -> bool {
    is_orphan_lane_key(key) || is_skill_invocation_lane_key(key)
}

fn orphan_merge_parent_lane(source_id: &str, y: i32) -> GraphViewportLane {
    let label = format!("orphan {}", shorten_id(source_id));
    GraphViewportLane {
        key: format!("{DERIVED_ORPHAN_LANE_KEY_PREFIX}{source_id}"),
        label,
        y,
    }
}

fn skill_invocation_subtree_lane(source_id: &str, y: i32) -> GraphViewportLane {
    let label = format!("skill {}", shorten_id(source_id));
    GraphViewportLane {
        key: format!("{DERIVED_SKILL_LANE_KEY_PREFIX}{source_id}"),
        label,
        y,
    }
}

fn visible_orphan_merge_parent_nodes(
    mode: GraphMode,
    ancestry: &[Node],
    end_index: usize,
) -> Vec<Node> {
    ancestry[..end_index]
        .iter()
        .filter(|node| is_visible_mode_node(mode, node))
        .cloned()
        .rev()
        .collect()
}

fn initial_visible_lane_is_linear(mode: GraphMode, nodes: &[Node]) -> bool {
    mode == GraphMode::Anchors || nodes.windows(2).all(|nodes| nodes[1].parent == nodes[0].id)
}

fn is_linear_new_nodes(source_id: &str, nodes: &[Node]) -> bool {
    nodes.first().is_some_and(|node| node.parent == source_id)
        && nodes.windows(2).all(|nodes| nodes[1].parent == nodes[0].id)
}

fn visible_skill_invocation_linear_subtrees(
    source_id: &str,
    nodes: Vec<Node>,
) -> Option<Vec<Vec<Node>>> {
    let mut nodes_by_id = BTreeMap::<String, Node>::new();
    let mut child_ids_by_parent = BTreeMap::<String, Vec<String>>::new();
    for node in nodes {
        child_ids_by_parent
            .entry(node.parent.clone())
            .or_default()
            .push(node.id.clone());
        nodes_by_id.insert(node.id.clone(), node);
    }

    let roots = child_ids_by_parent
        .get(source_id)
        .cloned()
        .unwrap_or_default();
    let mut subtrees = Vec::<Vec<Node>>::new();
    for root_id in roots {
        push_visible_skill_invocation_paths(
            source_id,
            &root_id,
            &nodes_by_id,
            &child_ids_by_parent,
            &mut subtrees,
        )?;
    }
    Some(subtrees)
}

fn push_visible_skill_invocation_paths(
    source_id: &str,
    node_id: &str,
    nodes_by_id: &BTreeMap<String, Node>,
    child_ids_by_parent: &BTreeMap<String, Vec<String>>,
    subtrees: &mut Vec<Vec<Node>>,
) -> Option<()> {
    let mut pending = vec![node_id.to_owned()];
    let mut visited = BTreeSet::new();

    while let Some(node_id) = pending.pop() {
        if !visited.insert(node_id.clone()) {
            return None;
        }
        nodes_by_id.get(&node_id)?;
        let Some(child_ids) = child_ids_by_parent.get(&node_id) else {
            subtrees.push(visible_skill_invocation_path(
                source_id,
                &node_id,
                nodes_by_id,
            )?);
            continue;
        };
        if child_ids.is_empty() {
            subtrees.push(visible_skill_invocation_path(
                source_id,
                &node_id,
                nodes_by_id,
            )?);
            continue;
        }
        for child_id in child_ids.iter().rev() {
            pending.push(child_id.clone());
        }
    }
    Some(())
}

fn visible_skill_invocation_path(
    source_id: &str,
    leaf_id: &str,
    nodes_by_id: &BTreeMap<String, Node>,
) -> Option<Vec<Node>> {
    let mut path = Vec::new();
    let mut node_id = leaf_id;
    let mut visited = BTreeSet::new();
    loop {
        let node = nodes_by_id.get(node_id)?;
        if !visited.insert(node.id.clone()) {
            return None;
        }
        path.push(node.clone());
        if node.parent == source_id {
            break;
        }
        node_id = &node.parent;
    }
    path.reverse();
    Some(path)
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
        .filter(|lane| {
            !is_derived_lane_key(&lane.lane_key)
                && materialized_lane_labels.contains(&lane.lane_label)
        })
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
        .filter(|lane| {
            !is_derived_lane_key(&lane.lane_key) && !branch_names.contains(&lane.lane_label)
        })
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

fn target_port_rebalance_order(kind: &str) -> i32 {
    match kind {
        "primary_parent" => 0,
        "fork" => 1,
        _ => 2,
    }
}

fn routed_edge_kind_order(kind: &str) -> i32 {
    match kind {
        "fork" => 0,
        _ => 1,
    }
}

fn is_derived_orphan_or_skill_lane(lane_key: &str) -> bool {
    lane_key.starts_with(DERIVED_ORPHAN_LANE_KEY_PREFIX)
        || lane_key.starts_with(DERIVED_SKILL_LANE_KEY_PREFIX)
}

fn node_point_on_lane(
    nodes: &[MaterializedTailNodeRow],
    lane_key: &str,
    node_id: &str,
    point: Point,
) -> bool {
    nodes.iter().any(|node| {
        node.lane_key == lane_key
            && node.node_id == node_id
            && node.x == point.x
            && node.y == point.y
    })
}

fn node_point_on_lane_suffix(
    nodes: &[MaterializedTailNodeRow],
    lane_key: &str,
    min_x: i32,
    node_id: &str,
    point: Point,
) -> bool {
    nodes.iter().any(|node| {
        node.lane_key == lane_key
            && node.x > min_x
            && node.node_id == node_id
            && node.x == point.x
            && node.y == point.y
    })
}

fn node_point_on_derived_lane(
    nodes: &[MaterializedTailNodeRow],
    node_id: &str,
    point: Point,
) -> bool {
    nodes.iter().any(|node| {
        node.lane_key.starts_with("derived:")
            && node.node_id == node_id
            && node.x == point.x
            && node.y == point.y
    })
}

fn parse_edge_kind(value: &str) -> crate::Result<GraphViewportEdgeKind> {
    let json = format!("\"{value}\"");
    serde_json::from_str(&json).context(ParseGraphSnapshotStoreValueSnafu {
        column: "console_graph_edge_routes.edge_kind",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::{Cell, RefCell};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use coco_mem::{MemoryStore, NewNode, NodeStore, PersistentStore, Role};

    struct BranchAdvanceDuringWalkStore {
        root: Node,
        old_head: Node,
        new_head_id: String,
        branch_head: RefCell<String>,
        advanced: Cell<bool>,
    }

    impl BranchAdvanceDuringWalkStore {
        fn new() -> Self {
            let memory = MemoryStore::new();
            let root = memory.get_node(&memory.root_id()).unwrap();
            let old_head = Node::new(
                root.id.clone(),
                Role::User,
                None,
                Kind::Text("old head".to_owned()),
                "1970-01-01T00:00:01Z".parse().unwrap(),
            );
            let new_head = Node::new(
                old_head.id.clone(),
                Role::User,
                None,
                Kind::Text("new head".to_owned()),
                "1970-01-01T00:00:02Z".parse().unwrap(),
            );
            Self {
                root,
                branch_head: RefCell::new(old_head.id.clone()),
                old_head,
                new_head_id: new_head.id,
                advanced: Cell::new(false),
            }
        }
    }

    impl NodeStore for BranchAdvanceDuringWalkStore {
        fn root_id(&self) -> String {
            self.root.id.clone()
        }

        fn append(&self, _node: NewNode) -> coco_mem::StoreResult<String> {
            Err(coco_mem::StoreError::StoreReadOnly {
                path: PathBuf::from("branch advance test store"),
            })
        }

        fn ancestry(&self, head_ref: &str) -> coco_mem::StoreResult<Vec<Node>> {
            match head_ref {
                id if id == self.old_head.id => Ok(vec![self.old_head.clone(), self.root.clone()]),
                id if id == self.root.id => Ok(vec![self.root.clone()]),
                id => Err(coco_mem::StoreError::NotFound { id: id.to_owned() }),
            }
        }

        fn log(&self, _base_ref: &str, head_ref: &str) -> coco_mem::StoreResult<Vec<Node>> {
            self.ancestry(head_ref)
        }

        fn get_node(&self, id: &str) -> coco_mem::StoreResult<Node> {
            match id {
                id if id == self.root.id => Ok(self.root.clone()),
                id if id == self.old_head.id => Ok(self.old_head.clone()),
                id => Err(coco_mem::StoreError::NotFound { id: id.to_owned() }),
            }
        }

        fn list_children(&self, node_id: &str) -> coco_mem::StoreResult<Vec<Node>> {
            if node_id == self.root.id {
                if !self.advanced.replace(true) {
                    *self.branch_head.borrow_mut() = self.new_head_id.clone();
                }
                return Ok(vec![self.old_head.clone()]);
            }
            if node_id == self.old_head.id {
                return Ok(Vec::new());
            }
            Err(coco_mem::StoreError::NotFound {
                id: node_id.to_owned(),
            })
        }
    }

    impl BranchStore for BranchAdvanceDuringWalkStore {
        fn fork(&self, _name: &str, _from_ref: &str) -> coco_mem::StoreResult<String> {
            Err(coco_mem::StoreError::StoreReadOnly {
                path: PathBuf::from("branch advance test store"),
            })
        }

        fn get_branch_head(&self, name: &str) -> coco_mem::StoreResult<String> {
            if name == "main" {
                return Ok(self.branch_head.borrow().clone());
            }
            Err(coco_mem::StoreError::BranchNotFound {
                name: name.to_owned(),
            })
        }

        fn delete_branch(&self, _name: &str) -> coco_mem::StoreResult<()> {
            Err(coco_mem::StoreError::StoreReadOnly {
                path: PathBuf::from("branch advance test store"),
            })
        }

        fn set_branch_head(
            &self,
            _name: &str,
            _expected_old_head: &str,
            _new_head: &str,
        ) -> coco_mem::StoreResult<()> {
            Err(coco_mem::StoreError::StoreReadOnly {
                path: PathBuf::from("branch advance test store"),
            })
        }
    }

    impl SessionStore for BranchAdvanceDuringWalkStore {
        fn list_session_states(&self) -> coco_mem::StoreResult<HashMap<String, SessionState>> {
            Ok(HashMap::from([("main".to_owned(), SessionState::Active)]))
        }

        fn get_session_state(&self, name: &str) -> coco_mem::StoreResult<SessionState> {
            if name == "main" {
                return Ok(SessionState::Active);
            }
            Err(coco_mem::StoreError::BranchNotFound {
                name: name.to_owned(),
            })
        }

        fn set_session_state(
            &self,
            _name: &str,
            _expected: Option<&SessionState>,
            _next: SessionState,
        ) -> coco_mem::StoreResult<SessionState> {
            Err(coco_mem::StoreError::StoreReadOnly {
                path: PathBuf::from("branch advance test store"),
            })
        }

        fn rebase_session(
            &self,
            _name: &str,
            _patch: &SessionAnchorPatch,
        ) -> coco_mem::StoreResult<String> {
            Err(coco_mem::StoreError::StoreReadOnly {
                path: PathBuf::from("branch advance test store"),
            })
        }

        fn handoff_session(
            &self,
            _name: &str,
            _patch: &SessionAnchorPatch,
            _prompt: &str,
        ) -> coco_mem::StoreResult<String> {
            Err(coco_mem::StoreError::StoreReadOnly {
                path: PathBuf::from("branch advance test store"),
            })
        }
    }

    fn temp_store_path() -> PathBuf {
        static TEMP_STORE_COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let process_id = std::process::id();
        let counter = TEMP_STORE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "coco-console-snapshot-{process_id}-{nanos}-{counter}"
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn materialization_source_snapshot_captures_branch_heads_before_node_walk() {
        let store = BranchAdvanceDuringWalkStore::new();

        let snapshot = MaterializationSourceSnapshot::from_store(
            &store,
            &[("main".to_owned(), SessionState::Active)],
        )
        .unwrap();

        assert_eq!(store.get_branch_head("main").unwrap(), store.new_head_id);
        assert_eq!(snapshot.get_branch_head("main").unwrap(), store.old_head.id);
        assert_eq!(snapshot.ancestry("main").unwrap()[0].id, store.old_head.id);
    }

    #[test]
    fn failed_batch_seed_clears_committed_facts_and_restores_empty_meta() {
        let path = temp_store_path();
        let _writer = PersistentStore::open(&path).unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(&path).unwrap();

        let store = snapshots.clone();
        let (restored_version, row_count) = snapshots
            .with_connection_for_tests(move |connection| {
                store.run_bool_write_transaction(connection, |this, connection| {
                    this.put_empty_materialization_in_transaction(connection, 7, GraphMode::All)
                })?;
                let previous_meta =
                    store.latest_materialization_row_in_connection(connection, GraphMode::All)?;
                store.run_write_transaction(connection, |this, connection| {
                    this.delete_materialization_meta(connection, GraphMode::All)?;
                    let lane = GraphViewportLane {
                        key: "main".to_owned(),
                        label: "main".to_owned(),
                        y: crate::layout::GRAPH_TOP_Y,
                    };
                    let node = GraphViewportNode {
                        key: "node:test:0:0".to_owned(),
                        id: "test".to_owned(),
                        node_target: "test".to_owned(),
                        short_id: "test".to_owned(),
                        kind: "text".to_owned(),
                        summary: "test".to_owned(),
                        labels: Vec::new(),
                        x: GRAPH_LEFT_X,
                        y: lane.y,
                    };
                    this.insert_node_location(
                        connection,
                        NodeLocationInsert {
                            mode: GraphMode::All,
                            node: &node,
                            lane: &lane,
                            bounds: node_bounds(&node),
                        },
                    )?;
                    Ok(())
                })?;

                store.restore_empty_materialization_after_failed_batch_seed(
                    connection,
                    GraphMode::All,
                    previous_meta,
                )?;
                let restored = store
                    .latest_materialization_row_in_connection(connection, GraphMode::All)?
                    .expect("empty materialization meta should be restored");
                let rows =
                    store.materialized_node_rows_in_connection(connection, GraphMode::All)?;
                Ok((restored.source_version, rows.len()))
            })
            .unwrap();

        assert_eq!(restored_version, 7);
        assert_eq!(row_count, 0);

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn direct_materialized_node_lookups_ignore_rows_without_meta() {
        let path = temp_store_path();
        let _writer = PersistentStore::open(&path).unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(&path).unwrap();

        let store = snapshots.clone();
        snapshots
            .with_connection_for_tests(move |connection| {
                store.run_write_transaction(connection, |this, connection| {
                    this.delete_materialization_meta(connection, GraphMode::All)?;
                    let lane = GraphViewportLane {
                        key: "main".to_owned(),
                        label: "main".to_owned(),
                        y: crate::layout::GRAPH_TOP_Y,
                    };
                    let node = GraphViewportNode {
                        key: "node:test:0:0".to_owned(),
                        id: "test".to_owned(),
                        node_target: "test".to_owned(),
                        short_id: "test".to_owned(),
                        kind: "text".to_owned(),
                        summary: "test".to_owned(),
                        labels: vec!["main".to_owned()],
                        x: GRAPH_LEFT_X,
                        y: lane.y,
                    };
                    this.insert_node_location(
                        connection,
                        NodeLocationInsert {
                            mode: GraphMode::All,
                            node: &node,
                            lane: &lane,
                            bounds: node_bounds(&node),
                        },
                    )?;
                    Ok(())
                })
            })
            .unwrap();

        let reference = snapshots
            .materialized_node_reference(GraphMode::All, "test")
            .unwrap();
        let points = snapshots
            .materialized_node_points(GraphMode::All, &BTreeSet::from(["test".to_owned()]))
            .unwrap();
        assert!(reference.is_none());
        assert!(points.is_empty());

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn latest_viewport_reads_meta_and_rows_from_one_snapshot() {
        let path = temp_store_path();
        let _writer = PersistentStore::open(&path).unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(&path).unwrap();

        let store = snapshots.clone();
        snapshots
            .with_connection_for_tests(move |connection| {
                store.run_bool_write_transaction(connection, |this, connection| {
                    this.put_empty_materialization_in_transaction(connection, 7, GraphMode::All)
                })
            })
            .unwrap();

        let reader = snapshots.clone();
        let writer = snapshots.clone();
        let response = snapshots
            .with_connection_for_tests(move |connection| {
                reader.run_read_transaction(connection, |this, connection| {
                    let meta = this
                        .latest_materialization_row_in_connection(connection, GraphMode::All)?
                        .expect("empty materialization meta should exist");

                    let writer_store = writer.clone();
                    writer.with_connection_for_tests(move |writer_connection| {
                        writer_store.run_write_transaction(
                            writer_connection,
                            |this, writer_connection| {
                                this.delete_materialization_meta(
                                    writer_connection,
                                    GraphMode::All,
                                )?;
                                let lane = GraphViewportLane {
                                    key: "main".to_owned(),
                                    label: "main".to_owned(),
                                    y: crate::layout::GRAPH_TOP_Y,
                                };
                                let node = GraphViewportNode {
                                    key: "node:test:0:0".to_owned(),
                                    id: "test".to_owned(),
                                    node_target: "test".to_owned(),
                                    short_id: "test".to_owned(),
                                    kind: "text".to_owned(),
                                    summary: "test".to_owned(),
                                    labels: vec!["main".to_owned()],
                                    x: GRAPH_LEFT_X,
                                    y: lane.y,
                                };
                                this.insert_node_location(
                                    writer_connection,
                                    NodeLocationInsert {
                                        mode: GraphMode::All,
                                        node: &node,
                                        lane: &lane,
                                        bounds: node_bounds(&node),
                                    },
                                )?;
                                Ok(())
                            },
                        )
                    })?;

                    this.viewport_from_row(
                        connection,
                        GraphMode::All,
                        meta,
                        GraphViewportRequest::default(),
                    )
                })
            })
            .unwrap()
            .expect("viewport should be available");

        assert_eq!(response.version, 7);
        assert!(response.nodes.is_empty());

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn meta_gated_materialized_node_lookups_read_from_one_snapshot() {
        let path = temp_store_path();
        let _writer = PersistentStore::open(&path).unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(&path).unwrap();

        let store = snapshots.clone();
        snapshots
            .with_connection_for_tests(move |connection| {
                store.run_bool_write_transaction(connection, |this, connection| {
                    this.put_empty_materialization_in_transaction(connection, 7, GraphMode::All)
                })
            })
            .unwrap();

        let reader = snapshots.clone();
        let writer = snapshots.clone();
        let (reference, points) = snapshots
            .with_connection_for_tests(move |connection| {
                reader.run_read_transaction(connection, |this, connection| {
                    assert!(
                        this.latest_materialization_row_in_connection(connection, GraphMode::All)?
                            .is_some()
                    );

                    let writer_store = writer.clone();
                    writer.with_connection_for_tests(move |writer_connection| {
                        writer_store.run_write_transaction(
                            writer_connection,
                            |this, writer_connection| {
                                this.delete_materialization_meta(
                                    writer_connection,
                                    GraphMode::All,
                                )?;
                                let lane = GraphViewportLane {
                                    key: "main".to_owned(),
                                    label: "main".to_owned(),
                                    y: crate::layout::GRAPH_TOP_Y,
                                };
                                let node = GraphViewportNode {
                                    key: "node:test:0:0".to_owned(),
                                    id: "test".to_owned(),
                                    node_target: "test".to_owned(),
                                    short_id: "test".to_owned(),
                                    kind: "text".to_owned(),
                                    summary: "test".to_owned(),
                                    labels: vec!["main".to_owned()],
                                    x: GRAPH_LEFT_X,
                                    y: lane.y,
                                };
                                this.insert_node_location(
                                    writer_connection,
                                    NodeLocationInsert {
                                        mode: GraphMode::All,
                                        node: &node,
                                        lane: &lane,
                                        bounds: node_bounds(&node),
                                    },
                                )?;
                                Ok(())
                            },
                        )
                    })?;

                    let reference = this.materialized_node_reference_in_connection(
                        connection,
                        GraphMode::All,
                        "test",
                    )?;
                    let points = this.materialized_node_points_in_connection(
                        connection,
                        GraphMode::All,
                        &BTreeSet::from(["test".to_owned()]),
                    )?;
                    Ok((reference, points))
                })
            })
            .unwrap();

        assert!(reference.is_none());
        assert!(points.is_empty());

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn visible_skill_invocation_linear_subtrees_handles_deep_chain() {
        let store = MemoryStore::new();
        let source_id = store.root_id();
        let depth = 20_000;
        let mut node_ids = Vec::with_capacity(depth);
        let mut parent = source_id.clone();
        for index in 0..depth {
            parent = store
                .append(NewNode {
                    parent,
                    role: Role::User,
                    metadata: None,
                    kind: Kind::Text(format!("node {index}")),
                })
                .unwrap();
            node_ids.push(parent.clone());
        }
        let nodes = node_ids
            .iter()
            .map(|node_id| store.get_node(node_id).unwrap())
            .collect();

        let subtrees = visible_skill_invocation_linear_subtrees(&source_id, nodes).unwrap();
        let expected_last = node_ids.last().unwrap();

        assert_eq!(subtrees.len(), 1);
        assert_eq!(subtrees[0].len(), depth);
        assert_eq!(
            subtrees[0].first().map(|node| node.id.as_str()),
            node_ids.first().map(String::as_str)
        );
        assert_eq!(
            subtrees[0].last().map(|node| node.id.as_str()),
            Some(expected_last.as_str())
        );
    }
}
