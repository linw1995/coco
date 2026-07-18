use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use coco_mem::{Kind, Node, SessionState};
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Integer, Nullable, Text};
use serde::Deserialize;
use snafu::prelude::*;

use super::frontier::{
    AdaptiveFrontier, AdaptiveFrontierError, FrontierConfig, FrontierMetrics, ReplaceMinOutcome,
};
use super::incremental_layout::{LayoutPoint, StableEdgeKind, StableLayoutConfig, route_edge};
use super::incremental_store::{FrontierNode, SqliteFrontierStore};
use super::snapshot_store::{
    ConsoleGraphSnapshotStore, INCREMENTAL_BUILD_LEASE_HEARTBEAT_INTERVAL, IncrementalBuildLease,
    SnapshotDatabase,
};
use crate::graph::{GraphMode, graph_branch_label, graph_node_from_node};
use crate::layout::{GraphLayoutEdge, GraphLayoutEdgeKind, GraphLayoutNode, edge_key};

const DEFAULT_FRONTIER_LOW_WATERMARK: usize = 4_096;
const DEFAULT_FRONTIER_HIGH_WATERMARK: usize = 8_192;
const DEFAULT_GRAPH_BUFFER_LIMIT: usize = 256;
const DEFAULT_CHILD_PAGE_SIZE: usize = 128;

// This mirrors VisibleGraphCollector while retaining the branch provenance needed to resolve
// each visible anchor's parents against the union of only the scopes that contain that anchor.
const ANCHOR_SCOPE_CTE: &str = "WITH RECURSIVE provider_scope( \
    branch_name, node_id, traversal_kind \
) AS (\
    SELECT name, head_id, 'graph' FROM console_graph_source_branches \
    UNION \
    SELECT scope.branch_name, nodes.parent_id, 'graph' \
    FROM provider_scope AS scope \
    INNER JOIN console_graph_source_nodes AS nodes ON nodes.node_id = scope.node_id \
    WHERE nodes.parent_id <> '' \
      AND NOT ( \
          COALESCE( \
              json_type(nodes.node_json, '$.kind.Anchor.payload.Session'), \
              '' \
          ) = 'object' \
          AND ( \
              COALESCE(json_type( \
                  nodes.node_json, \
                  '$.kind.Anchor.payload.Session.active_skill' \
              ), 'null') = 'null' \
              OR COALESCE(json_type( \
                  nodes.node_json, \
                  '$.kind.Anchor.payload.Session.active_skill.handoff' \
              ), 'null') <> 'null' \
          ) \
      ) \
    UNION \
    SELECT scope.branch_name, relations.parent_id, 'graph' \
    FROM provider_scope AS scope \
    INNER JOIN console_graph_source_nodes AS nodes ON nodes.node_id = scope.node_id \
    INNER JOIN console_graph_source_node_relations AS relations \
        ON relations.child_id = scope.node_id \
       AND relations.parent_id <> nodes.parent_id \
    UNION \
    SELECT scope.branch_name, relations.child_id, 'skill_subtree' \
    FROM provider_scope AS scope \
    INNER JOIN console_graph_source_nodes AS nodes ON nodes.node_id = scope.node_id \
    INNER JOIN console_graph_source_node_relations AS relations \
        ON relations.parent_id = scope.node_id \
    INNER JOIN console_graph_source_nodes AS children \
        ON children.node_id = relations.child_id \
    WHERE scope.traversal_kind = 'skill_subtree' \
       OR ( \
           json_type(nodes.node_json, '$.kind.ToolUse') = 'array' \
           AND json_type( \
               children.node_json, \
               '$.kind.Anchor.payload.SkillInvocation' \
           ) = 'object' \
       ) \
)";

const ANCHOR_EDGE_CTE: &str = "WITH RECURSIVE \
params(generation) AS (SELECT ?), \
visible_anchors(target_id) AS ( \
    SELECT DISTINCT scopes.node_id \
    FROM console_graph_anchor_scopes AS scopes \
    INNER JOIN params ON params.generation = scopes.generation \
    INNER JOIN console_graph_source_nodes AS nodes ON nodes.node_id = scopes.node_id \
    WHERE json_type(nodes.node_json, '$.kind.Anchor') = 'object' \
), \
raw_edges(target_id, raw_parent_id, edge_kind, edge_order) AS ( \
    SELECT anchors.target_id, nodes.parent_id, 'primary', 0 \
    FROM visible_anchors AS anchors \
    INNER JOIN console_graph_source_nodes AS nodes ON nodes.node_id = anchors.target_id \
    WHERE nodes.parent_id <> '' \
    UNION ALL \
    SELECT anchors.target_id, \
           json_extract(parents.value, '$.node_id'), \
           CASE json_extract(parents.value, '$.kind') \
               WHEN 'shadow' THEN 'shadow' \
               ELSE 'merge' \
           END, \
           CAST(parents.key AS INTEGER) + 1 \
    FROM visible_anchors AS anchors \
    INNER JOIN console_graph_source_nodes AS nodes ON nodes.node_id = anchors.target_id \
    INNER JOIN json_each(nodes.node_json, '$.kind.Anchor.merge_parents') AS parents \
), \
resolved(target_id, current_id, edge_kind, edge_order, depth) AS ( \
    SELECT raw.target_id, raw.raw_parent_id, raw.edge_kind, raw.edge_order, 0 \
    FROM raw_edges AS raw \
    INNER JOIN params \
    WHERE raw.raw_parent_id <> '' \
      AND EXISTS ( \
          SELECT 1 \
          FROM console_graph_anchor_scopes AS target_scope \
          INNER JOIN console_graph_anchor_scopes AS parent_scope \
              ON parent_scope.generation = target_scope.generation \
             AND parent_scope.branch_name = target_scope.branch_name \
          WHERE target_scope.generation = params.generation \
            AND target_scope.node_id = raw.target_id \
            AND parent_scope.node_id = raw.raw_parent_id \
      ) \
    UNION ALL \
    SELECT resolved.target_id, nodes.parent_id, resolved.edge_kind, \
           resolved.edge_order, resolved.depth + 1 \
    FROM resolved \
    INNER JOIN console_graph_source_nodes AS nodes ON nodes.node_id = resolved.current_id \
    INNER JOIN params \
    WHERE json_type(nodes.node_json, '$.kind.Anchor') IS NULL \
      AND nodes.parent_id <> '' \
      AND EXISTS ( \
          SELECT 1 \
          FROM console_graph_anchor_scopes AS target_scope \
          INNER JOIN console_graph_anchor_scopes AS parent_scope \
              ON parent_scope.generation = target_scope.generation \
             AND parent_scope.branch_name = target_scope.branch_name \
          WHERE target_scope.generation = params.generation \
            AND target_scope.node_id = resolved.target_id \
            AND parent_scope.node_id = nodes.parent_id \
      ) \
), \
candidates(target_id, source_id, edge_kind, edge_order, precedence) AS ( \
    SELECT resolved.target_id, resolved.current_id, resolved.edge_kind, \
           resolved.edge_order, \
           ROW_NUMBER() OVER ( \
               PARTITION BY resolved.target_id, resolved.current_id \
               ORDER BY resolved.edge_order, resolved.depth \
           ) \
    FROM resolved \
    INNER JOIN console_graph_source_nodes AS nodes ON nodes.node_id = resolved.current_id \
    WHERE json_type(nodes.node_json, '$.kind.Anchor') = 'object' \
      AND resolved.current_id <> resolved.target_id \
)";

#[derive(Debug, Clone, Copy)]
pub(crate) struct IncrementalBuildConfig {
    pub frontier_low_watermark: usize,
    pub frontier_high_watermark: usize,
    pub graph_buffer_limit: usize,
    pub child_page_size: usize,
}

impl Default for IncrementalBuildConfig {
    fn default() -> Self {
        Self {
            frontier_low_watermark: DEFAULT_FRONTIER_LOW_WATERMARK,
            frontier_high_watermark: DEFAULT_FRONTIER_HIGH_WATERMARK,
            graph_buffer_limit: DEFAULT_GRAPH_BUFFER_LIMIT,
            child_page_size: DEFAULT_CHILD_PAGE_SIZE,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct IncrementalBuildStats {
    pub processed_nodes: usize,
    pub all_nodes: usize,
    pub anchors_nodes: usize,
    pub reused_baseline: bool,
    pub frontier: FrontierMetrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IncrementalBuildKind {
    Full,
    Append,
}

#[derive(Debug)]
struct BufferedModeItems {
    mode: GraphMode,
    nodes: Vec<GraphLayoutNode>,
    edges: Vec<GraphLayoutEdge>,
}

impl BufferedModeItems {
    fn new(mode: GraphMode) -> Self {
        Self {
            mode,
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.nodes.len().saturating_add(self.edges.len())
    }

    fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.edges.is_empty()
    }
}

#[derive(Debug)]
struct GraphBuffer {
    anchors: BufferedModeItems,
    all: BufferedModeItems,
    limit: usize,
}

impl GraphBuffer {
    fn new(limit: usize) -> Self {
        Self {
            anchors: BufferedModeItems::new(GraphMode::Anchors),
            all: BufferedModeItems::new(GraphMode::All),
            limit: limit.max(1),
        }
    }

    fn len(&self) -> usize {
        self.anchors.len().saturating_add(self.all.len())
    }

    fn should_flush(&self) -> bool {
        self.len() >= self.limit
    }
}

#[derive(Debug)]
struct IncrementalWorkStore {
    database: SnapshotDatabase,
    path: PathBuf,
    run_id: i64,
    owner_id: String,
    baseline_generation: i64,
    source_version: u64,
    root_id: String,
    child_page_size: usize,
    layout: StableLayoutConfig,
    kind: IncrementalBuildKind,
}

#[derive(Debug, QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(Debug, QueryableByName)]
struct SourceNodeRow {
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Text)]
    node_json: String,
}

#[derive(Debug, QueryableByName)]
struct WorkNodeRow {
    #[diesel(sql_type = BigInt)]
    created_at_ns: i64,
    #[diesel(sql_type = Integer)]
    remaining_parents: i32,
    #[diesel(sql_type = Integer)]
    processed: i32,
}

#[derive(Debug, QueryableByName)]
struct ProjectionWorkNodeRow {
    #[diesel(sql_type = Integer)]
    remaining_parents: i32,
    #[diesel(sql_type = Integer)]
    processed: i32,
    #[diesel(sql_type = Nullable<Text>)]
    anchor_edges_json: Option<String>,
}

#[derive(Debug, Clone, QueryableByName)]
struct SlotRow {
    #[diesel(sql_type = Integer)]
    rank: i32,
    #[diesel(sql_type = Integer)]
    row_index: i32,
    #[diesel(sql_type = Integer)]
    x: i32,
    #[diesel(sql_type = Integer)]
    y: i32,
}

#[derive(Debug, QueryableByName)]
struct IntegerRow {
    #[diesel(sql_type = Integer)]
    value: i32,
}

#[derive(Debug, QueryableByName)]
struct BranchRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    head_id: String,
    #[diesel(sql_type = Text)]
    state_json: String,
}

#[derive(Debug, QueryableByName)]
struct ProjectedBranchRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    head_id: String,
    #[diesel(sql_type = Text)]
    state_json: String,
    #[diesel(sql_type = Nullable<Text>)]
    anchor_id: Option<String>,
}

#[derive(Debug, QueryableByName)]
struct PortRow {
    #[diesel(sql_type = Integer)]
    source_slot: i32,
    #[diesel(sql_type = Integer)]
    target_slot: i32,
}

#[derive(Debug, Clone)]
struct ProjectionEdge {
    source_id: String,
    kind: StableEdgeKind,
}

#[derive(Debug, Deserialize)]
struct StoredProjectionEdge {
    source_id: String,
    edge_kind: String,
}

#[derive(Debug)]
struct ReadyChildPage {
    ready: Vec<FrontierNode>,
    next_cursor: Option<String>,
}

type IncrementalFrontier = AdaptiveFrontier<FrontierNode, SqliteFrontierStore>;

#[derive(Debug)]
struct TraversalCounters {
    processed_nodes: usize,
    all_nodes: usize,
    anchors_nodes: usize,
}

#[derive(Debug)]
struct IncrementalBuildCompletion {
    active_nodes: usize,
    target_nodes: usize,
    counters: TraversalCounters,
    frontier: FrontierMetrics,
}

pub(crate) async fn build_incremental_generation(
    snapshots: &ConsoleGraphSnapshotStore,
    root_id: &str,
    baseline_generation: i64,
    lease: &IncrementalBuildLease,
    source_version: u64,
) -> crate::Result<IncrementalBuildStats> {
    build_incremental_generation_with_config(
        snapshots,
        root_id,
        baseline_generation,
        lease,
        source_version,
        IncrementalBuildConfig::default(),
    )
    .await
}

async fn build_incremental_generation_with_config(
    snapshots: &ConsoleGraphSnapshotStore,
    root_id: &str,
    baseline_generation: i64,
    lease: &IncrementalBuildLease,
    source_version: u64,
    config: IncrementalBuildConfig,
) -> crate::Result<IncrementalBuildStats> {
    let generation = lease.generation();
    validate_incremental_build_config(config)?;
    require_build_lease(snapshots, lease).await?;
    let (store, mut frontier) = initialize_incremental_build(
        snapshots,
        root_id,
        baseline_generation,
        lease,
        source_version,
        config,
    )
    .await?;

    let active_nodes = store.active_node_count().await?;
    if active_nodes == 0 {
        return finish_empty_incremental_build(
            snapshots,
            generation,
            source_version,
            store.kind,
            frontier.metrics(),
        )
        .await;
    }

    let target_nodes = store.target_node_count().await?;
    seed_incremental_frontier(snapshots, lease, &store, &mut frontier).await?;
    let counters = traverse_incremental_frontier(
        snapshots,
        lease,
        &store,
        &mut frontier,
        generation,
        config.graph_buffer_limit,
    )
    .await?;
    finalize_incremental_build(
        snapshots,
        &store,
        generation,
        source_version,
        IncrementalBuildCompletion {
            active_nodes,
            target_nodes,
            counters,
            frontier: frontier.metrics(),
        },
    )
    .await
}

fn validate_incremental_build_config(config: IncrementalBuildConfig) -> crate::Result<()> {
    ensure!(
        config.graph_buffer_limit > 0
            && config.child_page_size > 0
            && config.child_page_size <= config.frontier_high_watermark,
        crate::error::InvalidGraphSnapshotStoreValueSnafu {
            column: "incremental_build_config",
            value: format!(
                "graph_buffer_limit={} child_page_size={} frontier_high_watermark={}",
                config.graph_buffer_limit, config.child_page_size, config.frontier_high_watermark
            ),
        }
    );
    Ok(())
}

async fn initialize_incremental_build(
    snapshots: &ConsoleGraphSnapshotStore,
    root_id: &str,
    baseline_generation: i64,
    lease: &IncrementalBuildLease,
    source_version: u64,
    config: IncrementalBuildConfig,
) -> crate::Result<(IncrementalWorkStore, IncrementalFrontier)> {
    let generation = lease.generation();
    let mut store = IncrementalWorkStore::new(
        snapshots.database(),
        root_id.to_owned(),
        baseline_generation,
        generation,
        lease.owner_id().to_owned(),
        source_version,
        config.child_page_size,
    );
    store.begin().await?;
    let frontier_store = SqliteFrontierStore::new(store.database.clone(), generation);
    let frontier = AdaptiveFrontier::open(
        frontier_store,
        FrontierConfig::new(
            config.frontier_low_watermark,
            config.frontier_high_watermark,
        ),
    )
    .await
    .map_err(frontier_error)?;
    Ok((store, frontier))
}

async fn finish_empty_incremental_build(
    snapshots: &ConsoleGraphSnapshotStore,
    generation: i64,
    source_version: u64,
    kind: IncrementalBuildKind,
    frontier: FrontierMetrics,
) -> crate::Result<IncrementalBuildStats> {
    snapshots
        .finish_incremental_mode(generation, source_version, GraphMode::Anchors)
        .await?;
    snapshots
        .finish_incremental_mode(generation, source_version, GraphMode::All)
        .await?;
    snapshots
        .publish_incremental_ports(generation, generation)
        .await?;
    Ok(IncrementalBuildStats {
        processed_nodes: 0,
        all_nodes: 0,
        anchors_nodes: 0,
        reused_baseline: kind == IncrementalBuildKind::Append,
        frontier,
    })
}

async fn seed_incremental_frontier(
    snapshots: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
    store: &IncrementalWorkStore,
    frontier: &mut IncrementalFrontier,
) -> crate::Result<()> {
    match store.kind {
        IncrementalBuildKind::Full => seed_full_frontier(store, frontier).await,
        IncrementalBuildKind::Append => {
            seed_append_frontier(snapshots, lease, store, frontier).await
        }
    }
}

async fn seed_full_frontier(
    store: &IncrementalWorkStore,
    frontier: &mut IncrementalFrontier,
) -> crate::Result<()> {
    let root = store.load_source_node(&store.root_id).await?;
    let root_item = FrontierNode {
        created_at_ns: saturating_i64(root.created_at.as_nanosecond()),
        node_id: root.id.clone(),
    };
    store.insert_root(&root_item).await?;
    frontier
        .push_batch([root_item])
        .await
        .map_err(frontier_error)?;
    Ok(())
}

async fn seed_append_frontier(
    snapshots: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
    store: &IncrementalWorkStore,
    frontier: &mut IncrementalFrontier,
) -> crate::Result<()> {
    let mut seed_cursor = String::new();
    loop {
        let page = store.append_seed_page(&seed_cursor).await?;
        if page.is_empty() {
            return Ok(());
        }
        seed_cursor.clone_from(
            &page
                .last()
                .expect("non-empty seed page must have a last node")
                .node_id,
        );
        store.insert_append_seeds(&page).await?;
        frontier.push_batch(page).await.map_err(frontier_error)?;
        require_build_lease(snapshots, lease).await?;
        tokio::task::yield_now().await;
    }
}

async fn traverse_incremental_frontier(
    snapshots: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
    store: &IncrementalWorkStore,
    frontier: &mut IncrementalFrontier,
    generation: i64,
    graph_buffer_limit: usize,
) -> crate::Result<TraversalCounters> {
    let mut buffer = GraphBuffer::new(graph_buffer_limit);
    let mut counters = TraversalCounters {
        processed_nodes: 0,
        all_nodes: store.mode_node_count(GraphMode::All).await?,
        anchors_nodes: store.mode_node_count(GraphMode::Anchors).await?,
    };
    while let Some(cursor) = frontier.peek_min().await.map_err(frontier_error)? {
        let (all_added, anchors_added) =
            process_frontier_cursor(snapshots, lease, store, frontier, &mut buffer, &cursor)
                .await?;
        counters.processed_nodes = counters.processed_nodes.saturating_add(1);
        counters.all_nodes = counters.all_nodes.saturating_add(usize::from(all_added));
        counters.anchors_nodes = counters
            .anchors_nodes
            .saturating_add(usize::from(anchors_added));
        maintain_incremental_traversal(
            snapshots,
            lease,
            generation,
            &mut buffer,
            counters.processed_nodes,
        )
        .await?;
    }
    flush_graph_buffer(snapshots, generation, &mut buffer).await?;
    require_build_lease(snapshots, lease).await?;
    Ok(counters)
}

async fn process_frontier_cursor(
    snapshots: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
    store: &IncrementalWorkStore,
    frontier: &mut IncrementalFrontier,
    buffer: &mut GraphBuffer,
    cursor: &FrontierNode,
) -> crate::Result<(bool, bool)> {
    let node = store.load_source_node(&cursor.node_id).await?;
    let added = store.process_node(&node, snapshots, buffer).await?;
    let first_child_page = store.discover_ready_child_page(&node.id, "").await?;
    store.mark_processed(&node.id).await?;
    replace_frontier_minimum(frontier, cursor, first_child_page.ready).await?;
    push_remaining_child_pages(
        snapshots,
        lease,
        store,
        frontier,
        &node.id,
        first_child_page.next_cursor,
    )
    .await?;
    Ok(added)
}

async fn replace_frontier_minimum(
    frontier: &mut IncrementalFrontier,
    cursor: &FrontierNode,
    ready: Vec<FrontierNode>,
) -> crate::Result<()> {
    match frontier
        .replace_min(cursor, ready)
        .await
        .map_err(frontier_error)?
    {
        ReplaceMinOutcome::Applied { .. } => Ok(()),
        ReplaceMinOutcome::StaleMinimum { current } => {
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "frontier_minimum",
                value: format!(
                    "expected {:?}, current {:?}",
                    cursor.node_id,
                    current.map(|item| item.node_id)
                ),
            }
            .fail()
        }
    }
}

async fn push_remaining_child_pages(
    snapshots: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
    store: &IncrementalWorkStore,
    frontier: &mut IncrementalFrontier,
    node_id: &str,
    mut child_cursor: Option<String>,
) -> crate::Result<()> {
    let mut child_pages = 0usize;
    let mut lease_renewed_at = tokio::time::Instant::now();
    while let Some(cursor) = child_cursor {
        let page = store.discover_ready_child_page(node_id, &cursor).await?;
        frontier
            .push_batch(page.ready)
            .await
            .map_err(frontier_error)?;
        child_cursor = page.next_cursor;
        child_pages = child_pages.saturating_add(1);
        if should_renew_child_page_lease(child_pages, lease_renewed_at) {
            require_build_lease(snapshots, lease).await?;
            lease_renewed_at = tokio::time::Instant::now();
        }
        tokio::task::yield_now().await;
    }
    Ok(())
}

fn should_renew_child_page_lease(child_pages: usize, renewed_at: tokio::time::Instant) -> bool {
    child_pages.is_multiple_of(128)
        || renewed_at.elapsed() >= INCREMENTAL_BUILD_LEASE_HEARTBEAT_INTERVAL
}

async fn maintain_incremental_traversal(
    snapshots: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
    generation: i64,
    buffer: &mut GraphBuffer,
    processed_nodes: usize,
) -> crate::Result<()> {
    if buffer.should_flush() {
        flush_graph_buffer(snapshots, generation, buffer).await?;
    }
    if processed_nodes.is_multiple_of(1_024) {
        require_build_lease(snapshots, lease).await?;
        tokio::task::yield_now().await;
    }
    Ok(())
}

async fn finalize_incremental_build(
    snapshots: &ConsoleGraphSnapshotStore,
    store: &IncrementalWorkStore,
    generation: i64,
    source_version: u64,
    completion: IncrementalBuildCompletion,
) -> crate::Result<IncrementalBuildStats> {
    let IncrementalBuildCompletion {
        active_nodes,
        target_nodes,
        counters,
        frontier,
    } = completion;
    let remaining = store.unprocessed_node_count().await?;
    validate_incremental_completion(&counters, target_nodes, active_nodes, remaining)?;
    store.apply_branch_labels(generation).await?;
    snapshots
        .publish_incremental_ports(generation, generation)
        .await?;
    snapshots
        .finish_incremental_mode(generation, source_version, GraphMode::Anchors)
        .await?;
    snapshots
        .finish_incremental_mode(generation, source_version, GraphMode::All)
        .await?;
    log_incremental_build(source_version, generation, &counters, frontier);
    Ok(IncrementalBuildStats {
        processed_nodes: counters.processed_nodes,
        all_nodes: counters.all_nodes,
        anchors_nodes: counters.anchors_nodes,
        reused_baseline: store.kind == IncrementalBuildKind::Append,
        frontier,
    })
}

fn validate_incremental_completion(
    counters: &TraversalCounters,
    target_nodes: usize,
    active_nodes: usize,
    remaining: usize,
) -> crate::Result<()> {
    ensure!(
        remaining == 0 && counters.processed_nodes == target_nodes,
        crate::error::InvalidGraphSnapshotStoreValueSnafu {
            column: "incremental_graph",
            value: format!(
                "processed={} target={target_nodes} active={active_nodes} remaining={remaining}",
                counters.processed_nodes
            ),
        }
    );
    Ok(())
}

fn log_incremental_build(
    source_version: u64,
    generation: i64,
    counters: &TraversalCounters,
    frontier: FrontierMetrics,
) {
    tracing::info!(
        source_version,
        generation,
        processed_nodes = counters.processed_nodes,
        all_nodes = counters.all_nodes,
        anchors_nodes = counters.anchors_nodes,
        frontier_hot_to_spilled = frontier.hot_to_spilled,
        frontier_spilled_to_hot = frontier.spilled_to_hot,
        frontier_max_hot_len = frontier.max_hot_len,
        "console graph incremental generation built",
    );
}

async fn flush_graph_buffer(
    snapshots: &ConsoleGraphSnapshotStore,
    generation: i64,
    buffer: &mut GraphBuffer,
) -> crate::Result<()> {
    for items in [&mut buffer.anchors, &mut buffer.all] {
        if items.is_empty() {
            continue;
        }
        snapshots
            .write_incremental_batch(
                generation,
                items.mode,
                std::mem::take(&mut items.nodes),
                std::mem::take(&mut items.edges),
            )
            .await?;
    }
    Ok(())
}

async fn push_buffered_node(
    snapshots: &ConsoleGraphSnapshotStore,
    generation: i64,
    buffer: &mut GraphBuffer,
    mode: GraphMode,
    node: GraphLayoutNode,
) -> crate::Result<()> {
    match mode {
        GraphMode::Anchors => buffer.anchors.nodes.push(node),
        GraphMode::All => buffer.all.nodes.push(node),
    }
    if buffer.should_flush() {
        flush_graph_buffer(snapshots, generation, buffer).await?;
    }
    Ok(())
}

async fn push_buffered_edge(
    snapshots: &ConsoleGraphSnapshotStore,
    generation: i64,
    buffer: &mut GraphBuffer,
    mode: GraphMode,
    edge: GraphLayoutEdge,
) -> crate::Result<()> {
    match mode {
        GraphMode::Anchors => buffer.anchors.edges.push(edge),
        GraphMode::All => buffer.all.edges.push(edge),
    }
    if buffer.should_flush() {
        flush_graph_buffer(snapshots, generation, buffer).await?;
    }
    Ok(())
}

fn frontier_error(error: AdaptiveFrontierError<crate::Error>) -> crate::Error {
    crate::Error::IncrementalFrontier {
        source: Box::new(error),
    }
}

async fn require_build_lease(
    snapshots: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
) -> crate::Result<()> {
    let renewed = snapshots.renew_incremental_build_lease(lease).await?;
    ensure!(
        renewed,
        crate::error::InvalidGraphSnapshotStoreValueSnafu {
            column: "incremental_build_lease",
            value: format!("generation {} is no longer owned", lease.generation()),
        }
    );
    Ok(())
}

fn saturating_i64(value: i128) -> i64 {
    value.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

impl IncrementalWorkStore {
    fn new(
        database: SnapshotDatabase,
        root_id: String,
        baseline_generation: i64,
        run_id: i64,
        owner_id: String,
        source_version: u64,
        child_page_size: usize,
    ) -> Self {
        let path = database.path().to_owned();
        Self {
            database,
            path,
            run_id,
            owner_id,
            baseline_generation,
            source_version,
            root_id,
            child_page_size,
            layout: StableLayoutConfig::default(),
            kind: IncrementalBuildKind::Full,
        }
    }

    async fn begin(&mut self) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let baseline_generation = self.baseline_generation;
        let source_version = self.source_version.min(i64::MAX as u64) as i64;
        let kind = self
            .database
            .with_connection(move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let owned = diesel::sql_query(
                            "SELECT COUNT(*) AS count FROM console_graph_build_runs \
                             WHERE run_id = ? AND source_version = ? \
                               AND owner_id = ? AND status = 'building'",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<BigInt, _>(source_version)
                        .bind::<Text, _>(&owner_id)
                        .get_result::<CountRow>(connection)?
                        .count
                            == 1;
                        if !owned {
                            return Err(diesel::result::Error::NotFound);
                        }
                        for table in [
                            "console_graph_build_nodes",
                            "console_graph_build_anchor_edges",
                            "console_graph_build_frontier",
                            "console_graph_build_rank_slots",
                            "console_graph_build_edge_ports",
                        ] {
                            diesel::sql_query(format!("DELETE FROM {table} WHERE run_id = ?"))
                                .bind::<BigInt, _>(run_id)
                                .execute(connection)?;
                        }
                        diesel::sql_query(
                            "DELETE FROM console_graph_anchor_scopes WHERE generation = ?",
                        )
                        .bind::<BigInt, _>(run_id)
                        .execute(connection)?;
                        diesel::sql_query(format!(
                            "{ANCHOR_SCOPE_CTE} \
                             INSERT OR IGNORE INTO console_graph_anchor_scopes \
                                 (generation, branch_name, node_id) \
                             SELECT ?, scope.branch_name, scope.node_id \
                             FROM provider_scope AS scope \
                             INNER JOIN console_graph_source_nodes AS nodes \
                                 ON nodes.node_id = scope.node_id \
                             WHERE nodes.parent_id <> ''"
                        ))
                        .bind::<BigInt, _>(run_id)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_anchor_scope_manifests \
                                 (generation, scope_count) \
                             SELECT ?, COUNT(*) \
                             FROM console_graph_anchor_scopes \
                             WHERE generation = ? \
                             ON CONFLICT(generation) DO UPDATE SET \
                                 scope_count = excluded.scope_count",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<BigInt, _>(run_id)
                        .execute(connection)?;
                        diesel::sql_query(format!(
                            "{ANCHOR_EDGE_CTE} \
                             INSERT INTO console_graph_build_anchor_edges \
                                 (run_id, target_id, source_id, edge_kind, edge_order) \
                             SELECT params.generation, candidates.target_id, \
                                    candidates.source_id, candidates.edge_kind, \
                                    candidates.edge_order \
                             FROM candidates \
                             INNER JOIN params \
                             WHERE candidates.precedence = 1"
                        ))
                        .bind::<BigInt, _>(run_id)
                        .execute(connection)?;
                        let complete_modes = diesel::sql_query(
                            "SELECT COUNT(DISTINCT mode) AS count \
                             FROM console_graph_materializations \
                             WHERE generation = ? AND mode IN ('anchors', 'all')",
                        )
                        .bind::<BigInt, _>(baseline_generation)
                        .get_result::<CountRow>(connection)?
                        .count;
                        let baseline_scope_complete = diesel::sql_query(
                            "SELECT CASE \
                                 WHEN EXISTS ( \
                                     SELECT 1 \
                                     FROM console_graph_anchor_scope_manifests AS manifest \
                                     WHERE manifest.generation = ? \
                                       AND manifest.scope_count = ( \
                                           SELECT COUNT(*) \
                                           FROM console_graph_anchor_scopes AS scopes \
                                           WHERE scopes.generation = manifest.generation \
                                       ) \
                                 ) THEN 1 \
                                 WHEN NOT EXISTS ( \
                                     SELECT 1 \
                                     FROM console_graph_node_locations \
                                     WHERE generation = ? AND mode IN ('anchors', 'all') \
                                 ) THEN 1 \
                                 ELSE 0 \
                             END AS count",
                        )
                        .bind::<BigInt, _>(baseline_generation)
                        .bind::<BigInt, _>(baseline_generation)
                        .get_result::<CountRow>(connection)?
                        .count
                            == 1;
                        let removed_nodes = diesel::sql_query(
                            "SELECT COUNT(*) AS count \
                             FROM console_graph_node_locations AS baseline \
                             WHERE baseline.generation = ? AND baseline.mode = 'all' \
                               AND NOT EXISTS ( \
                                   SELECT 1 \
                                   FROM console_graph_source_branch_nodes AS branch_nodes \
                                   INNER JOIN console_graph_source_branches AS branches \
                                       ON branches.name = branch_nodes.branch_name \
                                      AND branches.contribution_generation = \
                                          branch_nodes.contribution_generation \
                                   WHERE branch_nodes.node_id = baseline.node_id \
                               )",
                        )
                        .bind::<BigInt, _>(baseline_generation)
                        .get_result::<CountRow>(connection)?
                        .count;
                        let anchor_scope_changes = diesel::sql_query(
                            "SELECT ( \
                                 SELECT COUNT(*) \
                                 FROM console_graph_anchor_scopes AS baseline \
                                 WHERE baseline.generation = ? \
                                   AND NOT EXISTS ( \
                                       SELECT 1 \
                                       FROM console_graph_anchor_scopes AS current \
                                       WHERE current.generation = ? \
                                         AND current.branch_name = baseline.branch_name \
                                         AND current.node_id = baseline.node_id \
                                   ) \
                             ) + ( \
                                 SELECT COUNT(*) \
                                 FROM console_graph_anchor_scopes AS current \
                                 WHERE current.generation = ? \
                                   AND EXISTS ( \
                                       SELECT 1 \
                                       FROM console_graph_node_locations AS baseline_all \
                                       WHERE baseline_all.generation = ? \
                                         AND baseline_all.mode = 'all' \
                                         AND baseline_all.node_id = current.node_id \
                                   ) \
                                   AND NOT EXISTS ( \
                                       SELECT 1 \
                                       FROM console_graph_anchor_scopes AS baseline \
                                       WHERE baseline.generation = ? \
                                         AND baseline.branch_name = current.branch_name \
                                         AND baseline.node_id = current.node_id \
                                   ) \
                             ) AS count",
                        )
                        .bind::<BigInt, _>(baseline_generation)
                        .bind::<BigInt, _>(run_id)
                        .bind::<BigInt, _>(run_id)
                        .bind::<BigInt, _>(baseline_generation)
                        .bind::<BigInt, _>(baseline_generation)
                        .get_result::<CountRow>(connection)?
                        .count;
                        let missing_ports = diesel::sql_query(
                            "SELECT COUNT(*) AS count \
                             FROM console_graph_edge_routes AS routes \
                             WHERE routes.generation = ? \
                               AND NOT EXISTS ( \
                                   SELECT 1 FROM console_graph_edge_ports AS ports \
                                   WHERE ports.generation = routes.generation \
                                     AND ports.mode = routes.mode \
                                     AND ports.edge_key = routes.edge_key \
                               )",
                        )
                        .bind::<BigInt, _>(baseline_generation)
                        .get_result::<CountRow>(connection)?
                        .count;
                        let kind = if complete_modes == 2
                            && baseline_scope_complete
                            && removed_nodes == 0
                            && anchor_scope_changes == 0
                            && missing_ports == 0
                        {
                            IncrementalBuildKind::Append
                        } else {
                            IncrementalBuildKind::Full
                        };
                        diesel::sql_query(
                            "UPDATE console_graph_branch_build_state \
                             SET inflight_head_id = NULL, build_generation = NULL \
                             WHERE build_generation = ?",
                        )
                        .bind::<BigInt, _>(run_id)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_build_rank_slots \
                             (run_id, mode, rank, row, node_id, x, y, active) \
                             SELECT ?, mode, rank, sort_order, node_id, x, y, 0 \
                             FROM console_graph_node_locations \
                             WHERE generation = ?",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<BigInt, _>(baseline_generation)
                        .execute(connection)?;

                        if kind == IncrementalBuildKind::Append {
                            diesel::sql_query(
                                "INSERT INTO console_graph_node_locations ( \
                                     generation, mode, node_id, node_key, node_target, short_id, \
                                     node_kind, summary, labels_json, rank, sort_order, x, y, \
                                     created_at, created_at_ns, min_x, min_y, max_x, max_y \
                                 ) \
                                 SELECT ?, mode, node_id, node_key, node_target, short_id, \
                                        node_kind, summary, '[]', rank, sort_order, x, y, \
                                        created_at, created_at_ns, min_x, min_y, max_x, max_y \
                                 FROM console_graph_node_locations WHERE generation = ?",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<BigInt, _>(baseline_generation)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_edge_routes ( \
                                     generation, mode, edge_key, edge_kind, source_id, target_id, \
                                     source_x, source_y, control_1_x, control_1_y, control_2_x, \
                                     control_2_y, target_x, target_y, min_x, min_y, max_x, max_y \
                                 ) \
                                 SELECT ?, mode, edge_key, edge_kind, source_id, target_id, \
                                        source_x, source_y, control_1_x, control_1_y, control_2_x, \
                                        control_2_y, target_x, target_y, min_x, min_y, max_x, max_y \
                                 FROM console_graph_edge_routes WHERE generation = ?",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<BigInt, _>(baseline_generation)
                            .execute(connection)?;
                            diesel::sql_query(
                                "UPDATE console_graph_build_rank_slots SET active = 1 \
                                 WHERE run_id = ?",
                            )
                            .bind::<BigInt, _>(run_id)
                            .execute(connection)?;
                        }
                        diesel::sql_query(
                            "INSERT INTO console_graph_build_edge_ports \
                             (run_id, mode, edge_key, source_id, target_id, \
                              source_slot, target_slot, active) \
                             SELECT ?, mode, edge_key, source_id, target_id, \
                                    source_slot, target_slot, 0 \
                             FROM console_graph_edge_ports \
                             WHERE generation = ?",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<BigInt, _>(baseline_generation)
                        .execute(connection)?;
                        if kind == IncrementalBuildKind::Append {
                            diesel::sql_query(
                                "UPDATE console_graph_build_edge_ports SET active = 1 \
                                 WHERE run_id = ?",
                            )
                            .bind::<BigInt, _>(run_id)
                            .execute(connection)?;
                        }

                        let branches = diesel::sql_query(
                            "SELECT name, head_id, state_json \
                             FROM console_graph_source_branches ORDER BY name",
                        )
                        .load::<BranchRow>(connection)?;
                        diesel::sql_query(
                            "DELETE FROM console_graph_materialization_branches \
                             WHERE generation = ?",
                        )
                        .bind::<BigInt, _>(run_id)
                        .execute(connection)?;
                        for branch in branches {
                            diesel::sql_query(
                                "INSERT INTO console_graph_materialization_branches \
                                 (generation, name, head_id, state_json) \
                                 VALUES (?, ?, ?, ?)",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&branch.name)
                            .bind::<Text, _>(&branch.head_id)
                            .bind::<Text, _>(&branch.state_json)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_branch_build_state \
                                 (branch_name, desired_head_id, inflight_head_id, \
                                  published_head_id, build_generation) \
                                 VALUES (?, ?, ?, NULL, ?) \
                                 ON CONFLICT(branch_name) DO UPDATE SET \
                                     desired_head_id = excluded.desired_head_id, \
                                     inflight_head_id = excluded.inflight_head_id, \
                                     build_generation = excluded.build_generation",
                            )
                            .bind::<Text, _>(&branch.name)
                            .bind::<Text, _>(&branch.head_id)
                            .bind::<Text, _>(&branch.head_id)
                            .bind::<BigInt, _>(run_id)
                            .execute(connection)?;
                        }
                        Ok(kind)
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        self.kind = kind;
        Ok(())
    }

    async fn active_node_count(&self) -> crate::Result<usize> {
        let path = self.path.clone();
        let count = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(DISTINCT branch_nodes.node_id) AS count \
                     FROM console_graph_source_branch_nodes AS branch_nodes \
                     INNER JOIN console_graph_source_branches AS branches \
                         ON branches.name = branch_nodes.branch_name \
                        AND branches.contribution_generation = \
                            branch_nodes.contribution_generation",
                )
                .get_result::<CountRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?
            .count;
        Ok(count.max(0) as usize)
    }

    async fn target_node_count(&self) -> crate::Result<usize> {
        if self.kind == IncrementalBuildKind::Full {
            return self.active_node_count().await;
        }
        let path = self.path.clone();
        let root_id = self.root_id.clone();
        let baseline_generation = self.baseline_generation;
        let count = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(DISTINCT nodes.node_id) AS count \
                     FROM console_graph_source_nodes AS nodes \
                     INNER JOIN console_graph_source_branch_nodes AS branch_nodes \
                         ON branch_nodes.node_id = nodes.node_id \
                     INNER JOIN console_graph_source_branches AS branches \
                         ON branches.name = branch_nodes.branch_name \
                        AND branches.contribution_generation = \
                            branch_nodes.contribution_generation \
                     WHERE nodes.node_id <> ? \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_node_locations AS baseline \
                           WHERE baseline.generation = ? AND baseline.mode = 'all' \
                             AND baseline.node_id = nodes.node_id \
                       )",
                )
                .bind::<Text, _>(&root_id)
                .bind::<BigInt, _>(baseline_generation)
                .get_result::<CountRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?
            .count;
        Ok(count.max(0) as usize)
    }

    async fn mode_node_count(&self, mode: GraphMode) -> crate::Result<usize> {
        let path = self.path.clone();
        let generation = self.run_id;
        let mode = mode.as_query_value().to_owned();
        let count = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count FROM console_graph_node_locations \
                     WHERE generation = ? AND mode = ?",
                )
                .bind::<BigInt, _>(generation)
                .bind::<Text, _>(mode)
                .get_result::<CountRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?
            .count;
        Ok(count.max(0) as usize)
    }

    async fn append_seed_page(&self, cursor: &str) -> crate::Result<Vec<FrontierNode>> {
        debug_assert_eq!(self.kind, IncrementalBuildKind::Append);
        let path = self.path.clone();
        let cursor = cursor.to_owned();
        let root_id = self.root_id.clone();
        let baseline_generation = self.baseline_generation;
        let limit = self.child_page_size.min(i64::MAX as usize) as i64;
        let rows = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT nodes.node_id, nodes.node_json \
                     FROM console_graph_source_nodes AS nodes \
                     WHERE nodes.node_id > ? AND nodes.node_id <> ? \
                       AND EXISTS ( \
                           SELECT 1 \
                           FROM console_graph_source_branch_nodes AS branch_nodes \
                           INNER JOIN console_graph_source_branches AS branches \
                               ON branches.name = branch_nodes.branch_name \
                              AND branches.contribution_generation = \
                                  branch_nodes.contribution_generation \
                           WHERE branch_nodes.node_id = nodes.node_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM console_graph_node_locations AS baseline \
                           WHERE baseline.generation = ? AND baseline.mode = 'all' \
                             AND baseline.node_id = nodes.node_id \
                       ) \
                       AND NOT EXISTS ( \
                           SELECT 1 \
                           FROM console_graph_source_node_relations AS relations \
                           WHERE relations.child_id = nodes.node_id \
                             AND relations.parent_id <> ? \
                             AND EXISTS ( \
                                 SELECT 1 \
                                 FROM console_graph_source_branch_nodes AS parent_membership \
                                 INNER JOIN console_graph_source_branches AS parent_branch \
                                     ON parent_branch.name = parent_membership.branch_name \
                                    AND parent_branch.contribution_generation = \
                                        parent_membership.contribution_generation \
                                 WHERE parent_membership.node_id = relations.parent_id \
                             ) \
                             AND NOT EXISTS ( \
                                 SELECT 1 \
                                 FROM console_graph_node_locations AS baseline_parent \
                                 WHERE baseline_parent.generation = ? \
                                   AND baseline_parent.mode = 'all' \
                                   AND baseline_parent.node_id = relations.parent_id \
                             ) \
                       ) \
                     ORDER BY nodes.node_id LIMIT ?",
                )
                .bind::<Text, _>(cursor)
                .bind::<Text, _>(&root_id)
                .bind::<BigInt, _>(baseline_generation)
                .bind::<Text, _>(&root_id)
                .bind::<BigInt, _>(baseline_generation)
                .bind::<BigInt, _>(limit)
                .load::<SourceNodeRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        rows.into_iter()
            .map(|row| {
                let node = serde_json::from_str::<Node>(&row.node_json).context(
                    crate::error::ParseGraphSnapshotStoreValueSnafu {
                        column: "node_json",
                    },
                )?;
                Ok(FrontierNode {
                    created_at_ns: saturating_i64(node.created_at.as_nanosecond()),
                    node_id: row.node_id,
                })
            })
            .collect()
    }

    async fn insert_append_seeds(&self, seeds: &[FrontierNode]) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let seeds = seeds.to_vec();
        self.database
            .with_connection(move |connection| {
                connection
                    .transaction::<_, diesel::result::Error, _>(|connection| {
                        for seed in seeds {
                            diesel::sql_query(
                                "INSERT OR IGNORE INTO console_graph_build_nodes \
                                 (run_id, node_id, created_at_ns, remaining_parents, processed) \
                                 VALUES (?, ?, ?, 0, 0)",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(seed.node_id)
                            .bind::<BigInt, _>(seed.created_at_ns)
                            .execute(connection)?;
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn unprocessed_node_count(&self) -> crate::Result<usize> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let count = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count FROM console_graph_build_nodes \
                     WHERE run_id = ? AND processed = 0",
                )
                .bind::<BigInt, _>(run_id)
                .get_result::<CountRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?
            .count;
        Ok(count.max(0) as usize)
    }

    async fn load_source_node(&self, node_id: &str) -> crate::Result<Node> {
        let path = self.path.clone();
        let node_id = node_id.to_owned();
        let query_node_id = node_id.clone();
        let row = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT node_id, node_json FROM console_graph_source_nodes \
                     WHERE node_id = ?",
                )
                .bind::<Text, _>(&query_node_id)
                .get_result::<SourceNodeRow>(connection)
                .optional()
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?
            .with_context(|| crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "node_id",
                value: node_id.to_owned(),
            })?;
        serde_json::from_str(&row.node_json).context(
            crate::error::ParseGraphSnapshotStoreValueSnafu {
                column: "node_json",
            },
        )
    }

    async fn insert_root(&self, root: &FrontierNode) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let root = root.clone();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "INSERT INTO console_graph_build_nodes \
                     (run_id, node_id, created_at_ns, remaining_parents, processed) \
                     VALUES (?, ?, ?, 0, 0)",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(root.node_id)
                .bind::<BigInt, _>(root.created_at_ns)
                .execute(connection)
                .map(|_| ())
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn mark_processed(&self, node_id: &str) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let node_id = node_id.to_owned();
        let query_node_id = node_id.clone();
        let updated = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_build_nodes SET processed = 1 \
                     WHERE run_id = ? AND node_id = ? \
                       AND processed = 0 AND remaining_parents = 0",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(&query_node_id)
                .execute(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        ensure!(
            updated == 1,
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "processed_node",
                value: node_id,
            }
        );
        Ok(())
    }

    async fn work_node(&self, node_id: &str) -> crate::Result<Option<WorkNodeRow>> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let node_id = node_id.to_owned();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT created_at_ns, remaining_parents, processed \
                     FROM console_graph_build_nodes WHERE run_id = ? AND node_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(node_id)
                .get_result::<WorkNodeRow>(connection)
                .optional()
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn projection_work_node(
        &self,
        node_id: &str,
    ) -> crate::Result<Option<ProjectionWorkNodeRow>> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let node_id = node_id.to_owned();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT build.remaining_parents, build.processed, \
                            CASE WHEN EXISTS ( \
                                SELECT 1 FROM console_graph_anchor_scopes AS scopes \
                                WHERE scopes.generation = ? \
                                  AND scopes.node_id = build.node_id \
                            ) THEN ( \
                                SELECT COALESCE( \
                                    json_group_array(json_object( \
                                        'source_id', desired.source_id, \
                                        'edge_kind', desired.edge_kind \
                                    )), \
                                    '[]' \
                                ) \
                                FROM ( \
                                    SELECT source_id, edge_kind \
                                    FROM console_graph_build_anchor_edges \
                                    WHERE run_id = ? AND target_id = build.node_id \
                                    ORDER BY edge_order, source_id \
                                ) AS desired \
                            ) END AS anchor_edges_json \
                     FROM console_graph_build_nodes AS build \
                     WHERE build.run_id = ? AND build.node_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<BigInt, _>(run_id)
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(node_id)
                .get_result::<ProjectionWorkNodeRow>(connection)
                .optional()
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn discover_ready_child_page(
        &self,
        parent_id: &str,
        cursor: &str,
    ) -> crate::Result<ReadyChildPage> {
        let page = self.active_child_page(parent_id, cursor).await?;
        if page.is_empty() {
            return Ok(ReadyChildPage {
                ready: Vec::new(),
                next_cursor: None,
            });
        }
        let next_cursor = (page.len() == self.child_page_size).then(|| {
            page.last()
                .expect("non-empty child page must have a last node")
                .node_id
                .clone()
        });
        let children = page
            .iter()
            .map(|row| {
                let node = serde_json::from_str::<Node>(&row.node_json).context(
                    crate::error::ParseGraphSnapshotStoreValueSnafu {
                        column: "node_json",
                    },
                )?;
                Ok(FrontierNode {
                    created_at_ns: saturating_i64(node.created_at.as_nanosecond()),
                    node_id: row.node_id.clone(),
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let ready = self.commit_child_page(parent_id, &children).await?;
        Ok(ReadyChildPage { ready, next_cursor })
    }

    async fn active_child_page(
        &self,
        parent_id: &str,
        cursor: &str,
    ) -> crate::Result<Vec<SourceNodeRow>> {
        let path = self.path.clone();
        let parent_id = parent_id.to_owned();
        let cursor = cursor.to_owned();
        let limit = self.child_page_size.min(i64::MAX as usize) as i64;
        let baseline_generation = self.baseline_generation;
        let append = self.kind == IncrementalBuildKind::Append;
        self.database
            .with_connection(move |connection| {
                let base_query =
                    "SELECT relations.child_id AS node_id, nodes.node_json AS node_json \
                     FROM console_graph_source_node_relations AS relations \
                     INNER JOIN console_graph_source_nodes AS nodes \
                         ON nodes.node_id = relations.child_id \
                     WHERE relations.parent_id = ? \
                       AND relations.child_id > ? \
                       AND EXISTS ( \
                           SELECT 1 \
                           FROM console_graph_source_branch_nodes AS branch_nodes \
                           INNER JOIN console_graph_source_branches AS branches \
                               ON branches.name = branch_nodes.branch_name \
                              AND branches.contribution_generation = \
                                  branch_nodes.contribution_generation \
                           WHERE branch_nodes.node_id = relations.child_id \
                       )";
                if append {
                    diesel::sql_query(format!(
                        "{base_query} \
                         AND NOT EXISTS ( \
                             SELECT 1 FROM console_graph_node_locations AS baseline \
                             WHERE baseline.generation = ? AND baseline.mode = 'all' \
                               AND baseline.node_id = relations.child_id \
                         ) \
                         ORDER BY relations.child_id LIMIT ?"
                    ))
                    .bind::<Text, _>(parent_id)
                    .bind::<Text, _>(cursor)
                    .bind::<BigInt, _>(baseline_generation)
                    .bind::<BigInt, _>(limit)
                    .load::<SourceNodeRow>(connection)
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
                } else {
                    diesel::sql_query(format!(
                        "{base_query} ORDER BY relations.child_id LIMIT ?"
                    ))
                    .bind::<Text, _>(parent_id)
                    .bind::<Text, _>(cursor)
                    .bind::<BigInt, _>(limit)
                    .load::<SourceNodeRow>(connection)
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
                }
            })
            .await
    }

    async fn commit_child_page(
        &self,
        parent_id: &str,
        children: &[FrontierNode],
    ) -> crate::Result<Vec<FrontierNode>> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let parent_id = parent_id.to_owned();
        let children = children.to_vec();
        let baseline_generation = self.baseline_generation;
        let root_id = self.root_id.clone();
        let append = self.kind == IncrementalBuildKind::Append;
        self.database
            .with_connection(move |connection| {
                connection
                    .transaction::<_, diesel::result::Error, _>(|connection| {
                        let mut ready = Vec::new();
                        for child in &children {
                            let insert_query = "INSERT OR IGNORE INTO console_graph_build_nodes \
                                 (run_id, node_id, created_at_ns, remaining_parents, processed) \
                                 SELECT ?, ?, ?, COUNT(DISTINCT relations.parent_id), 0 \
                                 FROM console_graph_source_node_relations AS relations \
                                 WHERE relations.child_id = ? \
                                   AND EXISTS ( \
                                       SELECT 1 \
                                       FROM console_graph_source_branch_nodes AS branch_nodes \
                                       INNER JOIN console_graph_source_branches AS branches \
                                           ON branches.name = branch_nodes.branch_name \
                                          AND branches.contribution_generation = \
                                              branch_nodes.contribution_generation \
                                       WHERE branch_nodes.node_id = relations.parent_id \
                                   )";
                            if append {
                                diesel::sql_query(format!(
                                    "{insert_query} \
                                     AND relations.parent_id <> ? \
                                     AND NOT EXISTS ( \
                                         SELECT 1 \
                                         FROM console_graph_node_locations AS baseline_parent \
                                         WHERE baseline_parent.generation = ? \
                                           AND baseline_parent.mode = 'all' \
                                           AND baseline_parent.node_id = relations.parent_id \
                                     )"
                                ))
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&child.node_id)
                                .bind::<BigInt, _>(child.created_at_ns)
                                .bind::<Text, _>(&child.node_id)
                                .bind::<Text, _>(&root_id)
                                .bind::<BigInt, _>(baseline_generation)
                                .execute(connection)?;
                            } else {
                                diesel::sql_query(insert_query)
                                    .bind::<BigInt, _>(run_id)
                                    .bind::<Text, _>(&child.node_id)
                                    .bind::<BigInt, _>(child.created_at_ns)
                                    .bind::<Text, _>(&child.node_id)
                                    .execute(connection)?;
                            }
                            let updated = diesel::sql_query(
                                "UPDATE console_graph_build_nodes \
                                 SET remaining_parents = remaining_parents - 1 \
                                 WHERE run_id = ? AND node_id = ? \
                                   AND processed = 0 AND remaining_parents > 0",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&child.node_id)
                            .execute(connection)?;
                            if updated != 1 {
                                continue;
                            }
                            let state = diesel::sql_query(
                                "SELECT created_at_ns, remaining_parents, processed \
                                 FROM console_graph_build_nodes \
                                 WHERE run_id = ? AND node_id = ?",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&child.node_id)
                            .get_result::<WorkNodeRow>(connection)?;
                            if state.remaining_parents == 0 && state.processed == 0 {
                                ready.push(FrontierNode {
                                    created_at_ns: state.created_at_ns,
                                    node_id: child.node_id.clone(),
                                });
                            }
                        }
                        let relation_exists = diesel::sql_query(
                            "SELECT COUNT(*) AS count \
                             FROM console_graph_source_node_relations \
                             WHERE parent_id = ?",
                        )
                        .bind::<Text, _>(&parent_id)
                        .get_result::<CountRow>(connection)?;
                        debug_assert!(relation_exists.count >= children.len() as i64);
                        Ok(ready)
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn process_node(
        &self,
        node: &Node,
        snapshots: &ConsoleGraphSnapshotStore,
        buffer: &mut GraphBuffer,
    ) -> crate::Result<(bool, bool)> {
        let state = self
            .projection_work_node(&node.id)
            .await?
            .with_context(|| crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "work_node",
                value: node.id.clone(),
            })?;
        ensure!(
            state.remaining_parents == 0 && state.processed == 0,
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "ready_node",
                value: format!(
                    "{} remaining={} processed={}",
                    node.id, state.remaining_parents, state.processed
                ),
            }
        );
        if node.is_root() {
            return Ok((false, false));
        }

        let all_edges = self.all_edges(node).await?;
        self.buffer_mode_node(node, GraphMode::All, all_edges, snapshots, buffer)
            .await?;

        let anchors_added = if matches!(node.kind, Kind::Anchor(_))
            && let Some(edges_json) = state.anchor_edges_json
        {
            let anchor_edges = stored_projection_edges(&edges_json)?;
            self.buffer_mode_node(node, GraphMode::Anchors, anchor_edges, snapshots, buffer)
                .await?;
            true
        } else {
            false
        };
        Ok((true, anchors_added))
    }

    async fn all_edges(&self, node: &Node) -> crate::Result<Vec<ProjectionEdge>> {
        let mut edges = Vec::new();
        self.push_all_edge(&mut edges, &node.parent, StableEdgeKind::Primary)
            .await?;
        if let Kind::Anchor(anchor) = &node.kind {
            for merge_parent in anchor.merge_parents() {
                let kind = if merge_parent.is_shadow() {
                    StableEdgeKind::Shadow
                } else {
                    StableEdgeKind::Merge
                };
                self.push_all_edge(&mut edges, merge_parent.node_id(), kind)
                    .await?;
            }
        }
        edges.retain(|edge| edge.source_id != node.id);
        Ok(edges)
    }

    async fn push_all_edge(
        &self,
        edges: &mut Vec<ProjectionEdge>,
        raw_parent_id: &str,
        kind: StableEdgeKind,
    ) -> crate::Result<()> {
        if raw_parent_id.is_empty() || raw_parent_id == self.root_id {
            return Ok(());
        }
        let processed = self
            .work_node(raw_parent_id)
            .await?
            .is_some_and(|row| row.processed == 1);
        let reused = self.kind == IncrementalBuildKind::Append
            && self.slot(GraphMode::All, raw_parent_id).await?.is_some();
        let source_id = (processed || reused).then(|| raw_parent_id.to_owned());
        let Some(source_id) = source_id else {
            return Ok(());
        };
        if edges.iter().any(|edge| edge.source_id == source_id) {
            return Ok(());
        }
        edges.push(ProjectionEdge { source_id, kind });
        Ok(())
    }

    async fn buffer_mode_node(
        &self,
        node: &Node,
        mode: GraphMode,
        projection_edges: Vec<ProjectionEdge>,
        snapshots: &ConsoleGraphSnapshotStore,
        buffer: &mut GraphBuffer,
    ) -> crate::Result<()> {
        let parent_ids = projection_edges
            .iter()
            .map(|edge| edge.source_id.clone())
            .collect::<Vec<_>>();
        let placement = self.place_node(mode, &node.id, &parent_ids).await?;
        let graph_node = graph_node_from_node(node.clone(), Vec::new(), Vec::new());
        push_buffered_node(
            snapshots,
            self.run_id,
            buffer,
            mode,
            GraphLayoutNode {
                node_id: graph_node.id.clone(),
                node_target: crate::graph::node_target_id(&graph_node.id),
                short_id: graph_node.short_id,
                kind: graph_node.kind,
                summary: graph_node.summary,
                labels: graph_node.labels,
                created_at: graph_node.created_at,
                created_at_ns: graph_node.created_at_ns,
                rank: placement.rank.max(0) as usize,
                order: placement.row_index.max(0) as usize,
                point: crate::api::Point {
                    x: placement.x,
                    y: placement.y,
                },
            },
        )
        .await?;

        for edge in projection_edges {
            let source = self.slot(mode, &edge.source_id).await?.with_context(|| {
                crate::error::InvalidGraphSnapshotStoreValueSnafu {
                    column: "edge_source",
                    value: edge.source_id.clone(),
                }
            })?;
            let layout_kind = graph_layout_edge_kind(edge.kind);
            let edge_key = edge_key(layout_kind, &edge.source_id, &node.id);
            let ports = self
                .assign_ports(mode, &edge_key, &edge.source_id, &node.id)
                .await?;
            let route = route_edge(
                LayoutPoint {
                    x: source.x,
                    y: source.y,
                },
                LayoutPoint {
                    x: placement.x,
                    y: placement.y,
                },
                super::incremental_layout::EndpointPortSlots {
                    source: ports.source_slot.max(0) as usize,
                    target: ports.target_slot.max(0) as usize,
                },
                self.layout,
            );
            push_buffered_edge(
                snapshots,
                self.run_id,
                buffer,
                mode,
                GraphLayoutEdge {
                    source_node_id: edge.source_id,
                    target_node_id: node.id.clone(),
                    kind: layout_kind,
                    route: crate::api::GraphBezierRoute {
                        source: crate::api::Point {
                            x: route.source.x,
                            y: route.source.y,
                        },
                        control_1: crate::api::Point {
                            x: route.control_1.x,
                            y: route.control_1.y,
                        },
                        control_2: crate::api::Point {
                            x: route.control_2.x,
                            y: route.control_2.y,
                        },
                        target: crate::api::Point {
                            x: route.target.x,
                            y: route.target.y,
                        },
                    },
                },
            )
            .await?;
        }
        Ok(())
    }

    async fn place_node(
        &self,
        mode: GraphMode,
        node_id: &str,
        parent_ids: &[String],
    ) -> crate::Result<SlotRow> {
        let mut parent_slots = Vec::with_capacity(parent_ids.len());
        for parent_id in parent_ids.iter().collect::<BTreeSet<_>>() {
            parent_slots.push(self.slot(mode, parent_id).await?.with_context(|| {
                crate::error::InvalidGraphSnapshotStoreValueSnafu {
                    column: "layout_parent",
                    value: parent_id.clone(),
                }
            })?);
        }
        let required_rank = parent_slots
            .iter()
            .map(|slot| slot.rank.saturating_add(1))
            .max()
            .unwrap_or(0);
        if let Some(existing) = self.slot(mode, node_id).await? {
            if existing.rank >= required_rank {
                self.activate_slot(mode, node_id).await?;
                return Ok(existing);
            }
            self.retire_slot(mode, node_id, &existing).await?;
        }

        let desired_row = median_i32(parent_slots.iter().map(|slot| slot.row_index));
        let row = self
            .nearest_free_row(mode, required_rank, desired_row)
            .await?;
        let placement = SlotRow {
            rank: required_rank,
            row_index: row,
            x: layout_coordinate(self.layout.padding, required_rank, self.layout.rank_step),
            y: layout_coordinate(self.layout.padding, row, self.layout.row_step),
        };
        self.insert_slot(mode, node_id, &placement).await?;
        Ok(placement)
    }

    async fn slot(&self, mode: GraphMode, node_id: &str) -> crate::Result<Option<SlotRow>> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let mode = mode.as_query_value().to_owned();
        let node_id = node_id.to_owned();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT rank, row AS row_index, x, y \
                     FROM console_graph_build_rank_slots \
                     WHERE run_id = ? AND mode = ? AND node_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(mode)
                .bind::<Text, _>(node_id)
                .get_result::<SlotRow>(connection)
                .optional()
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn activate_slot(&self, mode: GraphMode, node_id: &str) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let mode = mode.as_query_value().to_owned();
        let node_id = node_id.to_owned();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_build_rank_slots SET active = 1 \
                     WHERE run_id = ? AND mode = ? AND node_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(mode)
                .bind::<Text, _>(node_id)
                .execute(connection)
                .map(|_| ())
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn retire_slot(
        &self,
        mode: GraphMode,
        node_id: &str,
        slot: &SlotRow,
    ) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let mode = mode.as_query_value().to_owned();
        let node_id = node_id.to_owned();
        let tombstone = format!(
            "tombstone:{run_id}:{}:{}:{}:{}",
            mode, slot.rank, slot.row_index, node_id
        );
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_build_rank_slots \
                     SET node_id = ?, active = 0 \
                     WHERE run_id = ? AND mode = ? AND node_id = ?",
                )
                .bind::<Text, _>(tombstone)
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(mode)
                .bind::<Text, _>(node_id)
                .execute(connection)
                .map(|_| ())
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn nearest_free_row(
        &self,
        mode: GraphMode,
        rank: i32,
        _desired: i32,
    ) -> crate::Result<i32> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let mode = mode.as_query_value().to_owned();
        let row = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COALESCE(MAX(row) + 1, 0) AS value \
                     FROM console_graph_build_rank_slots \
                     WHERE run_id = ? AND mode = ? AND rank = ?",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(&mode)
                .bind::<Integer, _>(rank)
                .get_result::<IntegerRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        Ok(row.value.max(0))
    }

    async fn insert_slot(
        &self,
        mode: GraphMode,
        node_id: &str,
        slot: &SlotRow,
    ) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let mode = mode.as_query_value().to_owned();
        let node_id = node_id.to_owned();
        let slot = slot.clone();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "INSERT INTO console_graph_build_rank_slots \
                     (run_id, mode, rank, row, node_id, x, y, active) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, 1)",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(mode)
                .bind::<Integer, _>(slot.rank)
                .bind::<Integer, _>(slot.row_index)
                .bind::<Text, _>(node_id)
                .bind::<Integer, _>(slot.x)
                .bind::<Integer, _>(slot.y)
                .execute(connection)
                .map(|_| ())
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn assign_ports(
        &self,
        mode: GraphMode,
        edge_key: &str,
        source_id: &str,
        target_id: &str,
    ) -> crate::Result<PortRow> {
        if let Some(ports) = self.port_assignment(mode, edge_key).await? {
            self.activate_port_assignment(mode, edge_key).await?;
            return Ok(ports);
        }
        let source_slot = self.next_port_slot(mode, source_id, true).await?;
        let target_slot = self.next_port_slot(mode, target_id, false).await?;
        let ports = PortRow {
            source_slot,
            target_slot,
        };
        self.insert_port_assignment(mode, edge_key, source_id, target_id, &ports)
            .await?;
        Ok(ports)
    }

    async fn port_assignment(
        &self,
        mode: GraphMode,
        edge_key: &str,
    ) -> crate::Result<Option<PortRow>> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let mode = mode.as_query_value().to_owned();
        let edge_key = edge_key.to_owned();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT source_slot, target_slot \
                     FROM console_graph_build_edge_ports \
                     WHERE run_id = ? AND mode = ? AND edge_key = ?",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(mode)
                .bind::<Text, _>(edge_key)
                .get_result::<PortRow>(connection)
                .optional()
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn activate_port_assignment(&self, mode: GraphMode, edge_key: &str) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let mode = mode.as_query_value().to_owned();
        let edge_key = edge_key.to_owned();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_build_edge_ports SET active = 1 \
                     WHERE run_id = ? AND mode = ? AND edge_key = ?",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(mode)
                .bind::<Text, _>(edge_key)
                .execute(connection)
                .map(|_| ())
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn next_port_slot(
        &self,
        mode: GraphMode,
        node_id: &str,
        source: bool,
    ) -> crate::Result<i32> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let mode = mode.as_query_value().to_owned();
        let node_id = node_id.to_owned();
        let (id_column, slot_column) = if source {
            ("source_id", "source_slot")
        } else {
            ("target_id", "target_slot")
        };
        let query = format!(
            "SELECT COALESCE(MAX({slot_column}), -1) + 1 AS value \
             FROM console_graph_build_edge_ports \
             WHERE run_id = ? AND mode = ? AND {id_column} = ?"
        );
        let row = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(query)
                    .bind::<BigInt, _>(run_id)
                    .bind::<Text, _>(mode)
                    .bind::<Text, _>(node_id)
                    .get_result::<IntegerRow>(connection)
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        Ok(row.value.max(0))
    }

    async fn insert_port_assignment(
        &self,
        mode: GraphMode,
        edge_key: &str,
        source_id: &str,
        target_id: &str,
        ports: &PortRow,
    ) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let mode = mode.as_query_value().to_owned();
        let edge_key = edge_key.to_owned();
        let source_id = source_id.to_owned();
        let target_id = target_id.to_owned();
        let source_slot = ports.source_slot;
        let target_slot = ports.target_slot;
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "INSERT INTO console_graph_build_edge_ports \
                     (run_id, mode, edge_key, source_id, target_id, \
                      source_slot, target_slot, active) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, 1)",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(mode)
                .bind::<Text, _>(edge_key)
                .bind::<Text, _>(source_id)
                .bind::<Text, _>(target_id)
                .bind::<Integer, _>(source_slot)
                .bind::<Integer, _>(target_slot)
                .execute(connection)
                .map(|_| ())
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn apply_branch_labels(&self, generation: i64) -> crate::Result<()> {
        let branches = self.projected_branches().await?;
        let mut labels = BTreeMap::<(GraphMode, String), Vec<String>>::new();
        for branch in branches {
            let state = serde_json::from_str::<SessionState>(&branch.state_json).context(
                crate::error::ParseGraphSnapshotStoreValueSnafu {
                    column: "state_json",
                },
            )?;
            let label = graph_branch_label(&branch.name, &state);
            let processed_head = self
                .work_node(&branch.head_id)
                .await?
                .is_some_and(|row| row.processed == 1);
            let reused_head = self.kind == IncrementalBuildKind::Append
                && self.slot(GraphMode::All, &branch.head_id).await?.is_some();
            if branch.head_id != self.root_id && (processed_head || reused_head) {
                labels
                    .entry((GraphMode::All, branch.head_id.clone()))
                    .or_default()
                    .push(label.clone());
            }
            if let Some(anchor_id) = branch.anchor_id {
                labels
                    .entry((GraphMode::Anchors, anchor_id))
                    .or_default()
                    .push(label);
            }
        }
        for ((mode, node_id), mut node_labels) in labels {
            node_labels.sort();
            node_labels.dedup();
            let labels_json = serde_json::to_string(&node_labels).context(
                crate::error::SerializeGraphSnapshotStoreValueSnafu {
                    column: "labels_json",
                },
            )?;
            self.update_node_labels(generation, mode, &node_id, &labels_json)
                .await?;
        }
        Ok(())
    }

    async fn projected_branches(&self) -> crate::Result<Vec<ProjectedBranchRow>> {
        let path = self.path.clone();
        let generation = self.run_id;
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "WITH RECURSIVE branch_ancestry( \
                         branch_name, node_id, depth \
                     ) AS ( \
                         SELECT branches.name, branches.head_id, 0 \
                         FROM console_graph_source_branches AS branches \
                         WHERE EXISTS ( \
                             SELECT 1 FROM console_graph_anchor_scopes AS scopes \
                             WHERE scopes.generation = ? \
                               AND scopes.branch_name = branches.name \
                               AND scopes.node_id = branches.head_id \
                         ) \
                         UNION ALL \
                         SELECT ancestry.branch_name, nodes.parent_id, ancestry.depth + 1 \
                         FROM branch_ancestry AS ancestry \
                         INNER JOIN console_graph_source_nodes AS nodes \
                             ON nodes.node_id = ancestry.node_id \
                         WHERE json_type(nodes.node_json, '$.kind.Anchor') IS NULL \
                           AND nodes.parent_id <> '' \
                           AND EXISTS ( \
                               SELECT 1 FROM console_graph_anchor_scopes AS scopes \
                               WHERE scopes.generation = ? \
                                 AND scopes.branch_name = ancestry.branch_name \
                                 AND scopes.node_id = nodes.parent_id \
                           ) \
                     ) \
                     SELECT branches.name, branches.head_id, branches.state_json, ( \
                         SELECT ancestry.node_id \
                         FROM branch_ancestry AS ancestry \
                         INNER JOIN console_graph_source_nodes AS nodes \
                             ON nodes.node_id = ancestry.node_id \
                         WHERE ancestry.branch_name = branches.name \
                           AND json_type(nodes.node_json, '$.kind.Anchor') = 'object' \
                         ORDER BY ancestry.depth \
                         LIMIT 1 \
                     ) AS anchor_id \
                     FROM console_graph_source_branches AS branches \
                     ORDER BY branches.name",
                )
                .bind::<BigInt, _>(generation)
                .bind::<BigInt, _>(generation)
                .load::<ProjectedBranchRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn update_node_labels(
        &self,
        generation: i64,
        mode: GraphMode,
        node_id: &str,
        labels_json: &str,
    ) -> crate::Result<()> {
        let path = self.path.clone();
        let mode = mode.as_query_value().to_owned();
        let node_id = node_id.to_owned();
        let labels_json = labels_json.to_owned();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_node_locations SET labels_json = ? \
                     WHERE generation = ? AND mode = ? AND node_id = ?",
                )
                .bind::<Text, _>(labels_json)
                .bind::<BigInt, _>(generation)
                .bind::<Text, _>(mode)
                .bind::<Text, _>(node_id)
                .execute(connection)
                .map(|_| ())
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }
}

fn median_i32(values: impl IntoIterator<Item = i32>) -> i32 {
    let mut values = values.into_iter().collect::<Vec<_>>();
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[values.len() / 2]
}

fn layout_coordinate(padding: i32, index: i32, step: i32) -> i32 {
    padding.saturating_add(index.max(0).saturating_mul(step))
}

fn graph_layout_edge_kind(kind: StableEdgeKind) -> GraphLayoutEdgeKind {
    match kind {
        StableEdgeKind::Primary => GraphLayoutEdgeKind::Primary,
        StableEdgeKind::Merge => GraphLayoutEdgeKind::Merge,
        StableEdgeKind::Shadow => GraphLayoutEdgeKind::Shadow,
    }
}

fn stored_projection_edges(value: &str) -> crate::Result<Vec<ProjectionEdge>> {
    serde_json::from_str::<Vec<StoredProjectionEdge>>(value)
        .context(crate::error::ParseGraphSnapshotStoreValueSnafu {
            column: "anchor_edges_json",
        })?
        .into_iter()
        .map(|edge| {
            Ok(ProjectionEdge {
                source_id: edge.source_id,
                kind: stored_edge_kind(&edge.edge_kind)?,
            })
        })
        .collect()
}

fn stored_edge_kind(value: &str) -> crate::Result<StableEdgeKind> {
    match value {
        "primary" => Ok(StableEdgeKind::Primary),
        "merge" => Ok(StableEdgeKind::Merge),
        "shadow" => Ok(StableEdgeKind::Shadow),
        _ => crate::error::InvalidGraphSnapshotStoreValueSnafu {
            column: "edge_kind",
            value: value.to_owned(),
        }
        .fail(),
    }
}

#[cfg(test)]
mod tests {
    use coco_mem::{
        Anchor, BranchStore, Kind, MergeParent, NewNode, NodeStore, PromptAnchor, Role,
        SessionAnchor, SessionRole, SkillInvocationAnchor, SkillInvocationMode,
        SkillRuntimeContext, SqliteGraphStore, SqliteStore, ToolUse,
    };
    use serde_json::json;

    use super::*;
    use crate::api::{GraphViewportEdgeKind, GraphViewportResponse};
    use crate::graph::{GraphEdgeKind, GraphSnapshot, build_graph_snapshot_with_mode};
    use crate::host::api::GraphViewportRequest;
    use crate::host::source_cache::PersistentGraphIndex;

    #[derive(Debug, PartialEq, Eq)]
    struct AnchorGraphShape {
        nodes: BTreeMap<String, Vec<String>>,
        edges: BTreeSet<(String, String, String)>,
    }

    #[derive(Debug)]
    struct HandoffSkillFixture {
        invocation: String,
        handoff_session: String,
        external_prompt: String,
        merge_child: String,
    }

    #[tokio::test]
    async fn sqlite_frontier_spills_and_returns_to_memory_during_streaming_build() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let mut child_ids = Vec::new();
        for index in 0..6 {
            let child_id = writer
                .append(NewNode {
                    parent: root.clone(),
                    role: Role::User,
                    metadata: None,
                    kind: Kind::Text(format!("child {index}")),
                })
                .await
                .unwrap();
            writer
                .fork(&format!("branch-{index}"), &child_id)
                .await
                .unwrap();
            child_ids.push(child_id);
        }

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let records = source.graph_branches().await.unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index.reconcile_full_refresh(&records).await.unwrap();
        index.refresh_records(&source, records).await.unwrap();

        let lease = snapshots
            .acquire_incremental_build_lease(9)
            .await
            .unwrap()
            .unwrap();
        let stats = build_incremental_generation_with_config(
            &snapshots,
            &root,
            snapshots.active_generation().await.unwrap(),
            &lease,
            9,
            IncrementalBuildConfig {
                frontier_low_watermark: 1,
                frontier_high_watermark: 2,
                graph_buffer_limit: 2,
                child_page_size: 2,
            },
        )
        .await
        .unwrap();

        assert_eq!(stats.processed_nodes, child_ids.len() + 1);
        assert_eq!(stats.all_nodes, child_ids.len());
        assert_eq!(stats.anchors_nodes, 0);
        assert_eq!(stats.frontier.hot_to_spilled, 1);
        assert_eq!(stats.frontier.spilled_to_hot, 1);
        assert!(stats.frontier.max_hot_len <= 2);
        assert!(
            snapshots
                .latest_viewport(GraphMode::All, GraphViewportRequest::default())
                .await
                .unwrap()
                .is_none()
        );

        snapshots
            .publish_incremental_generation(&lease, 9)
            .await
            .unwrap();
        let all = snapshots
            .latest_viewport(GraphMode::All, GraphViewportRequest::default())
            .await
            .unwrap()
            .unwrap();
        let anchors = snapshots
            .latest_viewport(GraphMode::Anchors, GraphViewportRequest::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(all.nodes.len(), child_ids.len());
        assert!(all.nodes.iter().all(|node| node.labels.len() == 1));
        assert!(anchors.nodes.is_empty());
    }

    #[tokio::test]
    async fn streaming_projection_preserves_unique_nodes_labels_and_typed_edges() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let left_anchor = append_prompt(&writer, &root, Vec::new(), "left").await;
        let left_hidden = append_text(&writer, &left_anchor, "left hidden").await;
        let right_anchor = append_prompt(&writer, &root, Vec::new(), "right").await;
        let right_hidden = append_text(&writer, &right_anchor, "right hidden").await;
        let merged = append_prompt(
            &writer,
            &left_hidden,
            vec![MergeParent::merge(right_hidden.clone())],
            "merged",
        )
        .await;
        writer.fork("main", &merged).await.unwrap();
        writer.fork("draft", &merged).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let records = source.graph_branches().await.unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index.reconcile_full_refresh(&records).await.unwrap();
        index.refresh_records(&source, records).await.unwrap();

        let lease = snapshots
            .acquire_incremental_build_lease(12)
            .await
            .unwrap()
            .unwrap();
        build_incremental_generation_with_config(
            &snapshots,
            &root,
            snapshots.active_generation().await.unwrap(),
            &lease,
            12,
            IncrementalBuildConfig {
                frontier_low_watermark: 1,
                frontier_high_watermark: 3,
                graph_buffer_limit: 2,
                child_page_size: 2,
            },
        )
        .await
        .unwrap();
        snapshots
            .publish_incremental_generation(&lease, 12)
            .await
            .unwrap();

        let all = snapshots
            .latest_viewport(GraphMode::All, GraphViewportRequest::default())
            .await
            .unwrap()
            .unwrap();
        let anchors = snapshots
            .latest_viewport(GraphMode::Anchors, GraphViewportRequest::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(all.nodes.len(), 5);
        assert_eq!(anchors.nodes.len(), 3);
        assert_eq!(
            all.nodes
                .iter()
                .find(|node| node.id == merged)
                .unwrap()
                .labels,
            vec!["draft".to_owned(), "main".to_owned()]
        );
        assert_eq!(
            anchors
                .nodes
                .iter()
                .find(|node| node.id == merged)
                .unwrap()
                .labels,
            vec!["draft".to_owned(), "main".to_owned()]
        );
        assert!(anchors.edges.iter().any(|edge| {
            edge.key == format!("edge:primary_parent:{left_anchor}:{merged}")
                && edge.source_id == left_anchor
                && edge.target_id == merged
                && edge.kind == crate::api::GraphViewportEdgeKind::Primary
        }));
        assert!(anchors.edges.iter().any(|edge| {
            edge.key == format!("edge:merge_parent:{right_anchor}:{merged}")
                && edge.source_id == right_anchor
                && edge.target_id == merged
                && edge.kind == crate::api::GraphViewportEdgeKind::Merge
        }));
        assert!(
            all.nodes
                .iter()
                .all(|node| node.key == format!("node:{}", node.id))
        );
    }

    #[tokio::test]
    async fn full_build_matches_handoff_skill_subtree_with_merge_child() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let session = append_session(&writer, &root, "main session").await;
        let tool_use = append_tool_use(&writer, &session).await;
        writer.fork("main", &tool_use).await.unwrap();
        let fixture = append_handoff_skill_fixture(&writer, &root, &tool_use).await;

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        refresh_source_index(&source, &mut index).await;

        let stats = build_and_publish(&snapshots, &root, 1).await;
        assert!(!stats.reused_baseline);

        assert_handoff_skill_projection(&writer, &snapshots, 1, &fixture).await;
    }

    #[tokio::test]
    async fn append_matches_handoff_skill_subtree_with_merge_child() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let session = append_session(&writer, &root, "main session").await;
        let tool_use = append_tool_use(&writer, &session).await;
        writer.fork("main", &tool_use).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        refresh_source_index(&source, &mut index).await;
        build_and_publish(&snapshots, &root, 1).await;

        let fixture = append_handoff_skill_fixture(&writer, &root, &tool_use).await;
        refresh_source_index(&source, &mut index).await;

        let stats = build_and_publish(&snapshots, &root, 2).await;
        assert!(stats.reused_baseline);

        assert_handoff_skill_projection(&writer, &snapshots, 2, &fixture).await;
    }

    #[tokio::test]
    async fn context_rollover_rebuild_matches_full_anchor_scope() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let old_session = append_session(&writer, &root, "old session").await;
        let old_anchor = append_prompt(&writer, &old_session, Vec::new(), "old prompt").await;
        let old_head = append_text(&writer, &old_anchor, "old response").await;
        writer.fork("main", &old_head).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        refresh_source_index(&source, &mut index).await;

        let baseline = build_and_publish(&snapshots, &root, 1).await;
        assert!(!baseline.reused_baseline);

        let new_session = append_session(&writer, &old_head, "new session").await;
        let new_anchor = append_prompt(&writer, &new_session, Vec::new(), "new prompt").await;
        let new_head = append_text(&writer, &new_anchor, "new response").await;
        writer
            .set_branch_head("main", &old_head, &new_head)
            .await
            .unwrap();
        refresh_source_index(&source, &mut index).await;

        let rollover = build_and_publish(&snapshots, &root, 2).await;
        assert!(!rollover.reused_baseline);

        let full = build_graph_snapshot_with_mode(&writer, 2, GraphMode::Anchors)
            .await
            .unwrap();
        let persisted = persisted_anchors(&snapshots).await;
        assert_eq!(snapshot_shape(&full), viewport_shape(&persisted));
        assert_eq!(
            persisted
                .nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([new_session.as_str(), new_anchor.as_str()])
        );
        assert!(persisted.nodes.iter().all(|node| node.id != old_session));
        assert!(persisted.nodes.iter().all(|node| node.id != old_anchor));
    }

    #[tokio::test]
    async fn missing_active_scope_provenance_rebuilds_new_context() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let old_session = append_session(&writer, &root, "old session").await;
        let old_anchor = append_prompt(&writer, &old_session, Vec::new(), "old prompt").await;
        let old_head = append_text(&writer, &old_anchor, "old response").await;
        writer.fork("main", &old_head).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        refresh_source_index(&source, &mut index).await;
        build_and_publish(&snapshots, &root, 1).await;

        let active_generation = snapshots.active_generation().await.unwrap();
        let deleted_scopes = snapshots
            .with_connection(move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let deleted = diesel::sql_query(
                            "DELETE FROM console_graph_anchor_scopes WHERE generation = ?",
                        )
                        .bind::<BigInt, _>(active_generation)
                        .execute(connection)?;
                        diesel::sql_query(
                            "DELETE FROM console_graph_anchor_scope_manifests \
                             WHERE generation = ?",
                        )
                        .bind::<BigInt, _>(active_generation)
                        .execute(connection)?;
                        Ok(deleted)
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("missing-anchor-scope-provenance"),
                    })
            })
            .await
            .unwrap();
        assert!(deleted_scopes > 0);

        let new_session = append_session(&writer, &old_head, "new session").await;
        let new_anchor = append_prompt(&writer, &new_session, Vec::new(), "new prompt").await;
        let new_head = append_text(&writer, &new_anchor, "new response").await;
        writer
            .set_branch_head("main", &old_head, &new_head)
            .await
            .unwrap();
        refresh_source_index(&source, &mut index).await;

        let rollover = build_and_publish(&snapshots, &root, 2).await;
        assert!(!rollover.reused_baseline);

        let full = build_graph_snapshot_with_mode(&writer, 2, GraphMode::Anchors)
            .await
            .unwrap();
        let persisted = persisted_anchors(&snapshots).await;
        assert_eq!(snapshot_shape(&full), viewport_shape(&persisted));
        assert_eq!(
            persisted
                .nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([new_session.as_str(), new_anchor.as_str()])
        );
        assert!(persisted.nodes.iter().all(|node| node.id != old_session));
        assert!(persisted.nodes.iter().all(|node| node.id != old_anchor));
    }

    #[tokio::test]
    async fn empty_baseline_without_scope_manifest_can_append() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        refresh_source_index(&source, &mut index).await;
        let baseline = build_and_publish(&snapshots, &root, 1).await;
        assert!(!baseline.reused_baseline);
        assert!(persisted_anchors(&snapshots).await.nodes.is_empty());

        let active_generation = snapshots.active_generation().await.unwrap();
        snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "DELETE FROM console_graph_anchor_scope_manifests WHERE generation = ?",
                )
                .bind::<BigInt, _>(active_generation)
                .execute(connection)
                .map(|_| ())
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("missing-empty-anchor-scope-manifest"),
                })
            })
            .await
            .unwrap();

        let next = build_and_publish(&snapshots, &root, 2).await;
        assert!(next.reused_baseline);
        assert!(persisted_anchors(&snapshots).await.nodes.is_empty());
    }

    #[tokio::test]
    async fn provenance_change_rebuilds_union_without_cross_context_edge() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let old_session = append_session(&writer, &root, "old session").await;
        let old_anchor = append_prompt(&writer, &old_session, Vec::new(), "old prompt").await;
        let old_head = append_text(&writer, &old_anchor, "old response").await;
        writer.fork("legacy", &old_head).await.unwrap();
        writer.fork("main", &old_head).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        refresh_source_index(&source, &mut index).await;
        build_and_publish(&snapshots, &root, 1).await;

        let new_session = append_session(&writer, &old_head, "new session").await;
        let new_anchor = append_prompt(&writer, &new_session, Vec::new(), "new prompt").await;
        let new_head = append_text(&writer, &new_anchor, "new response").await;
        writer
            .set_branch_head("main", &old_head, &new_head)
            .await
            .unwrap();
        refresh_source_index(&source, &mut index).await;

        let rebuilt = build_and_publish(&snapshots, &root, 2).await;
        assert!(!rebuilt.reused_baseline);

        let full = build_graph_snapshot_with_mode(&writer, 2, GraphMode::Anchors)
            .await
            .unwrap();
        let persisted = persisted_anchors(&snapshots).await;
        assert_eq!(snapshot_shape(&full), viewport_shape(&persisted));
        assert_eq!(
            persisted
                .nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                old_session.as_str(),
                old_anchor.as_str(),
                new_session.as_str(),
                new_anchor.as_str(),
            ])
        );
        assert!(!persisted.edges.iter().any(|edge| {
            edge.source_id == old_anchor
                && edge.target_id == new_session
                && edge.kind == GraphViewportEdgeKind::Primary
        }));
        assert_eq!(
            persisted
                .nodes
                .iter()
                .find(|node| node.id == old_anchor)
                .unwrap()
                .labels,
            vec!["legacy".to_owned()]
        );
        assert_eq!(
            persisted
                .nodes
                .iter()
                .find(|node| node.id == new_anchor)
                .unwrap()
                .labels,
            vec!["main".to_owned()]
        );
    }

    #[tokio::test]
    async fn missing_ports_rebuild_before_fast_forward_reuses_geometry() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let baseline_parent = append_text(&writer, &root, "baseline").await;
        let published_head = append_text(&writer, &baseline_parent, "published").await;
        writer.fork("main", &published_head).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        let records = source.graph_branches().await.unwrap();
        index.reconcile_full_refresh(&records).await.unwrap();
        index.refresh_records(&source, records).await.unwrap();

        let first_lease = snapshots
            .acquire_incremental_build_lease(1)
            .await
            .unwrap()
            .unwrap();
        let first = build_incremental_generation_with_config(
            &snapshots,
            &root,
            snapshots.active_generation().await.unwrap(),
            &first_lease,
            1,
            IncrementalBuildConfig {
                frontier_low_watermark: 1,
                frontier_high_watermark: 4,
                graph_buffer_limit: 2,
                child_page_size: 2,
            },
        )
        .await
        .unwrap();
        assert!(!first.reused_baseline);
        snapshots
            .publish_incremental_generation(&first_lease, 1)
            .await
            .unwrap();
        let first_view = snapshots
            .latest_viewport(GraphMode::All, GraphViewportRequest::default())
            .await
            .unwrap()
            .unwrap();
        let published_node = first_view
            .nodes
            .iter()
            .find(|node| node.id == published_head)
            .unwrap();
        let published_point = (published_node.x, published_node.y);

        let upgraded_generation = snapshots.active_generation().await.unwrap();
        snapshots
            .with_connection(move |connection| {
                diesel::sql_query("DELETE FROM console_graph_edge_ports WHERE generation = ?")
                    .bind::<BigInt, _>(upgraded_generation)
                    .execute(connection)
                    .map(|_| ())
                    .context(crate::error::QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("missing-baseline-ports"),
                    })
            })
            .await
            .unwrap();
        let recovery_lease = snapshots
            .acquire_incremental_build_lease(2)
            .await
            .unwrap()
            .unwrap();
        let recovery = build_incremental_generation_with_config(
            &snapshots,
            &root,
            upgraded_generation,
            &recovery_lease,
            2,
            IncrementalBuildConfig {
                frontier_low_watermark: 1,
                frontier_high_watermark: 4,
                graph_buffer_limit: 2,
                child_page_size: 2,
            },
        )
        .await
        .unwrap();
        assert!(!recovery.reused_baseline);
        assert_eq!(recovery.processed_nodes, 3);
        snapshots
            .publish_incremental_generation(&recovery_lease, 2)
            .await
            .unwrap();

        let suffix_one = append_text(&writer, &published_head, "suffix one").await;
        let suffix_two = append_text(&writer, &suffix_one, "suffix two").await;
        writer
            .set_branch_head("main", &published_head, &suffix_two)
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();

        let second_lease = snapshots
            .acquire_incremental_build_lease(3)
            .await
            .unwrap()
            .unwrap();
        let second = build_incremental_generation_with_config(
            &snapshots,
            &root,
            snapshots.active_generation().await.unwrap(),
            &second_lease,
            3,
            IncrementalBuildConfig {
                frontier_low_watermark: 1,
                frontier_high_watermark: 4,
                graph_buffer_limit: 2,
                child_page_size: 2,
            },
        )
        .await
        .unwrap();
        assert!(second.reused_baseline);
        assert_eq!(second.processed_nodes, 2);
        snapshots
            .publish_incremental_generation(&second_lease, 3)
            .await
            .unwrap();
        let second_view = snapshots
            .latest_viewport(GraphMode::All, GraphViewportRequest::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second_view.nodes.len(), 4);
        assert_eq!(
            {
                let node = second_view
                    .nodes
                    .iter()
                    .find(|node| node.id == published_head)
                    .unwrap();
                (node.x, node.y)
            },
            published_point
        );

        writer
            .set_branch_head("main", &suffix_two, &published_head)
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let rewind_lease = snapshots
            .acquire_incremental_build_lease(4)
            .await
            .unwrap()
            .unwrap();
        let rewind = build_incremental_generation_with_config(
            &snapshots,
            &root,
            snapshots.active_generation().await.unwrap(),
            &rewind_lease,
            4,
            IncrementalBuildConfig {
                frontier_low_watermark: 1,
                frontier_high_watermark: 4,
                graph_buffer_limit: 2,
                child_page_size: 2,
            },
        )
        .await
        .unwrap();
        assert!(!rewind.reused_baseline);
        assert_eq!(rewind.processed_nodes, 3);
    }

    async fn append_text(store: &SqliteStore, parent: &str, content: &str) -> String {
        store
            .append(NewNode {
                parent: parent.to_owned(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text(content.to_owned()),
            })
            .await
            .unwrap()
    }

    async fn append_tool_use(store: &SqliteStore, parent: &str) -> String {
        store
            .append(NewNode {
                parent: parent.to_owned(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-call".to_owned(),
                    name: "exec_command".to_owned(),
                    input: json!({"cmd": "coco skill run test-skill"}),
                }),
            })
            .await
            .unwrap()
    }

    async fn append_session(store: &SqliteStore, parent: &str, prompt: &str) -> String {
        store
            .append(NewNode {
                parent: parent.to_owned(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(
                    Vec::new(),
                    SessionAnchor {
                        role: SessionRole::Orchestrator,
                        provider_profile: None,
                        provider: Some("test".to_owned()),
                        model: "test-model".to_owned(),
                        tools: Vec::new(),
                        system_prompt: "system".to_owned(),
                        prompt: prompt.to_owned(),
                        temperature: None,
                        max_tokens: None,
                        additional_params: None,
                        enable_coco_shim: false,
                        active_skill: None,
                    },
                )),
            })
            .await
            .unwrap()
    }

    async fn append_handoff_session(store: &SqliteStore, parent: &str) -> String {
        store
            .append(NewNode {
                parent: parent.to_owned(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(
                    Vec::new(),
                    SessionAnchor {
                        role: SessionRole::Runner,
                        provider_profile: None,
                        provider: Some("test".to_owned()),
                        model: "test-model".to_owned(),
                        tools: Vec::new(),
                        system_prompt: "skill system".to_owned(),
                        prompt: "handoff prompt".to_owned(),
                        temperature: None,
                        max_tokens: None,
                        additional_params: None,
                        enable_coco_shim: false,
                        active_skill: Some(SkillRuntimeContext {
                            name: "test-skill".to_owned(),
                            handoff: Some("handoff prompt".to_owned()),
                        }),
                    },
                )),
            })
            .await
            .unwrap()
    }

    async fn append_handoff_skill_fixture(
        store: &SqliteStore,
        root: &str,
        tool_use: &str,
    ) -> HandoffSkillFixture {
        let invocation = store
            .append(NewNode {
                parent: tool_use.to_owned(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_invocation(
                    Vec::new(),
                    SkillInvocationAnchor {
                        skill_name: "test-skill".to_owned(),
                        mode: SkillInvocationMode::Handoff {
                            prompt: "handoff prompt".to_owned(),
                        },
                    },
                )),
            })
            .await
            .unwrap();
        let handoff_session = append_handoff_session(store, &invocation).await;
        let external_session = append_session(store, root, "external session").await;
        let external_prompt =
            append_prompt(store, &external_session, Vec::new(), "external prompt").await;
        let merge_child = append_prompt(
            store,
            &external_prompt,
            vec![MergeParent::merge(handoff_session.clone())],
            "merge child",
        )
        .await;
        HandoffSkillFixture {
            invocation,
            handoff_session,
            external_prompt,
            merge_child,
        }
    }

    async fn append_prompt(
        store: &SqliteStore,
        parent: &str,
        merge_parents: Vec<MergeParent>,
        prompt: &str,
    ) -> String {
        store
            .append(NewNode {
                parent: parent.to_owned(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    merge_parents,
                    PromptAnchor {
                        prompt: prompt.to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap()
    }

    async fn refresh_source_index(source: &SqliteGraphStore, index: &mut PersistentGraphIndex) {
        let records = source.graph_branches().await.unwrap();
        index.reconcile_full_refresh(&records).await.unwrap();
        index.refresh_records(source, records).await.unwrap();
    }

    async fn build_and_publish(
        snapshots: &ConsoleGraphSnapshotStore,
        root_id: &str,
        source_version: u64,
    ) -> IncrementalBuildStats {
        let lease = snapshots
            .acquire_incremental_build_lease(source_version)
            .await
            .unwrap()
            .unwrap();
        let stats = build_incremental_generation(
            snapshots,
            root_id,
            snapshots.active_generation().await.unwrap(),
            &lease,
            source_version,
        )
        .await
        .unwrap();
        snapshots
            .publish_incremental_generation(&lease, source_version)
            .await
            .unwrap();
        stats
    }

    async fn persisted_anchors(snapshots: &ConsoleGraphSnapshotStore) -> GraphViewportResponse {
        snapshots
            .latest_viewport(GraphMode::Anchors, GraphViewportRequest::default())
            .await
            .unwrap()
            .unwrap()
    }

    async fn assert_handoff_skill_projection(
        writer: &SqliteStore,
        snapshots: &ConsoleGraphSnapshotStore,
        version: u64,
        fixture: &HandoffSkillFixture,
    ) {
        let full = build_graph_snapshot_with_mode(writer, version, GraphMode::Anchors)
            .await
            .unwrap();
        let persisted = persisted_anchors(snapshots).await;
        let full_shape = snapshot_shape(&full);
        let persisted_shape = viewport_shape(&persisted);
        assert_eq!(full_shape, persisted_shape);
        assert!(persisted_shape.edges.contains(&(
            fixture.invocation.clone(),
            fixture.handoff_session.clone(),
            "primary_parent".to_owned(),
        )));
        assert!(persisted_shape.edges.contains(&(
            fixture.external_prompt.clone(),
            fixture.merge_child.clone(),
            "primary_parent".to_owned(),
        )));
        assert!(persisted_shape.edges.contains(&(
            fixture.handoff_session.clone(),
            fixture.merge_child.clone(),
            "merge_parent".to_owned(),
        )));
    }

    fn snapshot_shape(snapshot: &GraphSnapshot) -> AnchorGraphShape {
        AnchorGraphShape {
            nodes: snapshot
                .nodes
                .iter()
                .map(|node| (node.id.clone(), node.labels.clone()))
                .collect(),
            edges: snapshot
                .edges
                .iter()
                .map(|edge| {
                    (
                        edge.source.clone(),
                        edge.target.clone(),
                        snapshot_edge_kind(edge.kind).to_owned(),
                    )
                })
                .collect(),
        }
    }

    fn viewport_shape(viewport: &GraphViewportResponse) -> AnchorGraphShape {
        AnchorGraphShape {
            nodes: viewport
                .nodes
                .iter()
                .map(|node| (node.id.clone(), node.labels.clone()))
                .collect(),
            edges: viewport
                .edges
                .iter()
                .map(|edge| {
                    (
                        edge.source_id.clone(),
                        edge.target_id.clone(),
                        edge.kind.key_part().to_owned(),
                    )
                })
                .collect(),
        }
    }

    fn snapshot_edge_kind(kind: GraphEdgeKind) -> &'static str {
        match kind {
            GraphEdgeKind::Primary => "primary_parent",
            GraphEdgeKind::Merge => "merge_parent",
            GraphEdgeKind::Shadow => "shadow_parent",
        }
    }
}
