use std::path::PathBuf;

use coco_mem::{Kind, Node, SessionState};
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Integer, Nullable, Text};
use snafu::prelude::*;

use super::frontier::{
    AdaptiveFrontier, AdaptiveFrontierError, FrontierConfig, FrontierMetrics, FrontierMode,
    ReplaceMinOutcome,
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
const PARENT_REQUIREMENT_PAGE_SIZE: i64 = 128;
const BUILD_PROGRESS_NODE_INTERVAL: usize = 10_000;
const BRANCH_CHANGE_RESET_BATCH_SIZE: i64 = 128;
const DELTA_REMOVAL_UPPER_BOUND_SQL: &str = "SELECT COALESCE(MAX(refresh.refresh_id), -1) AS value \
     FROM console_graph_build_changed_branches AS changed \
     INNER JOIN console_graph_build_runs AS run ON run.run_id = changed.run_id \
     INNER JOIN console_graph_source_refresh_runs AS refresh \
         ON refresh.branch_name = changed.branch_name \
        AND refresh.target_contribution_generation = \
            changed.baseline_contribution_generation \
        AND refresh.status = 'published' \
        AND refresh.published_source_revision <= run.dag_baseline_source_revision \
     WHERE changed.run_id = ? AND changed.branch_name = ?";
const DELTA_REMOVAL_CANDIDATE_SQL: &str = "SELECT membership.contribution_generation AS refresh_id, \
            membership.node_id, \
            CASE WHEN EXISTS ( \
                SELECT 1 \
                FROM console_graph_build_changed_branches AS changed \
                INNER JOIN console_graph_build_runs AS run \
                    ON run.run_id = changed.run_id \
                INNER JOIN console_graph_source_refresh_runs AS refresh \
                    ON refresh.refresh_id = membership.contribution_generation \
                   AND refresh.branch_name = membership.branch_name \
                   AND refresh.target_contribution_generation = \
                       changed.baseline_contribution_generation \
                   AND refresh.status = 'published' \
                   AND refresh.published_source_revision <= \
                       run.dag_baseline_source_revision \
                WHERE changed.run_id = ? \
                  AND changed.branch_name = membership.branch_name \
            ) THEN 1 ELSE 0 END AS eligible \
     FROM console_graph_source_branch_nodes AS membership \
     WHERE membership.branch_name = ? \
       AND (membership.contribution_generation, membership.node_id) > (?, ?) \
       AND membership.contribution_generation <= ? \
     ORDER BY membership.contribution_generation, membership.node_id \
     LIMIT ?";

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
    pub classification_reason: BuildClassificationReason,
    pub frontier: FrontierMetrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BuildClassificationReason {
    Unclassified,
    SourceJournalDeltaCompatible,
    BaselineDeltaCapabilityUnavailable,
    SourceJournalGap,
    SourceAppendBaseMismatch,
    SourceChangeJournalInvalid,
    SourceMetadataBaselineMismatch,
    BaselineAnchorScopeManifestUnavailable,
}

impl BuildClassificationReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unclassified => "unclassified",
            Self::SourceJournalDeltaCompatible => "source_journal_delta_compatible",
            Self::BaselineDeltaCapabilityUnavailable => "baseline_delta_capability_unavailable",
            Self::SourceJournalGap => "source_change_journal_gap",
            Self::SourceAppendBaseMismatch => "source_append_base_mismatch",
            Self::SourceChangeJournalInvalid => "source_change_journal_invalid",
            Self::SourceMetadataBaselineMismatch => "source_metadata_baseline_mismatch",
            Self::BaselineAnchorScopeManifestUnavailable => {
                "baseline_anchor_scope_manifest_unavailable"
            }
        }
    }
}

fn stored_build_classification_reason(value: &str) -> Result<BuildClassificationReason, ()> {
    match value {
        "unclassified" => Ok(BuildClassificationReason::Unclassified),
        "source_journal_delta_compatible" => {
            Ok(BuildClassificationReason::SourceJournalDeltaCompatible)
        }
        "baseline_delta_capability_unavailable" => {
            Ok(BuildClassificationReason::BaselineDeltaCapabilityUnavailable)
        }
        "source_change_journal_gap" => Ok(BuildClassificationReason::SourceJournalGap),
        "source_append_base_mismatch" => Ok(BuildClassificationReason::SourceAppendBaseMismatch),
        "source_change_journal_invalid" => {
            Ok(BuildClassificationReason::SourceChangeJournalInvalid)
        }
        "source_metadata_baseline_mismatch" => {
            Ok(BuildClassificationReason::SourceMetadataBaselineMismatch)
        }
        "baseline_anchor_scope_manifest_unavailable" => {
            Ok(BuildClassificationReason::BaselineAnchorScopeManifestUnavailable)
        }
        _ => Err(()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IncrementalBuildKind {
    Full,
    Append,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchChangeCompatibility {
    Compatible,
    PendingReset,
    RequiresFull(BuildClassificationReason),
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
    lease_epoch: i64,
    baseline_generation: i64,
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
struct CandidateSourceNodeRow {
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Text)]
    node_json: String,
    #[diesel(sql_type = Integer)]
    eligible: i32,
}

#[derive(Debug)]
struct InspectedSourceNodePage {
    nodes: Vec<SourceNodeRow>,
    inspected_cursor: Option<String>,
}

#[derive(Debug)]
struct InspectedFrontierPage {
    nodes: Vec<FrontierNode>,
    inspected_cursor: Option<String>,
}

#[derive(Debug, QueryableByName)]
struct ReadyNodeRow {
    #[diesel(sql_type = BigInt)]
    created_at_ns: i64,
    #[diesel(sql_type = Text)]
    node_id: String,
}

#[derive(Debug, QueryableByName)]
struct WorkNodeRow {
    #[diesel(sql_type = Integer)]
    processed: i32,
    #[diesel(sql_type = Integer)]
    projection_complete: i32,
}

#[derive(Debug, QueryableByName)]
struct ParentRequirementStateRow {
    #[diesel(sql_type = Text)]
    parent_requirement_cursor: String,
    #[diesel(sql_type = Integer)]
    parent_requirements_complete: i32,
}

#[derive(Debug, QueryableByName)]
struct ProjectionWorkNodeRow {
    #[diesel(sql_type = Integer)]
    remaining_parents: i32,
    #[diesel(sql_type = Integer)]
    processed: i32,
    #[diesel(sql_type = Integer)]
    projection_complete: i32,
    #[diesel(sql_type = Text)]
    projection_phase: String,
    #[diesel(sql_type = BigInt)]
    projection_raw_cursor: i64,
    #[diesel(sql_type = Integer)]
    projection_edge_order_cursor: i32,
    #[diesel(sql_type = Text)]
    projection_edge_source_cursor: String,
    #[diesel(sql_type = Integer)]
    projection_required_rank: i32,
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
    #[diesel(sql_type = BigInt)]
    contribution_generation: i64,
}

#[derive(Debug, QueryableByName)]
struct ProjectedBranchRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    head_id: String,
    #[diesel(sql_type = Text)]
    state_json: String,
}

#[derive(Debug, QueryableByName)]
struct BranchLabelResolutionRow {
    #[diesel(sql_type = Text)]
    current_node_id: String,
    #[diesel(sql_type = Nullable<Text>)]
    anchor_id: Option<String>,
    #[diesel(sql_type = Text)]
    phase: String,
}

#[derive(Debug, QueryableByName)]
struct BranchAncestryNodeRow {
    #[diesel(sql_type = Text)]
    parent_id: String,
    #[diesel(sql_type = Integer)]
    is_anchor: i32,
}

#[derive(Debug, QueryableByName)]
struct PortRow {
    #[diesel(sql_type = Integer)]
    source_slot: i32,
    #[diesel(sql_type = Integer)]
    target_slot: i32,
}

#[derive(Debug, Clone, QueryableByName)]
struct ProjectionEdgeRow {
    #[diesel(sql_type = Text)]
    source_id: String,
    #[diesel(sql_type = Text)]
    edge_kind: String,
    #[diesel(sql_type = Integer)]
    edge_order: i32,
}

#[derive(Debug, QueryableByName)]
struct AnchorProjectionStateRow {
    #[diesel(sql_type = BigInt)]
    raw_cursor: i64,
    #[diesel(sql_type = Integer)]
    raw_complete: i32,
    #[diesel(sql_type = Integer)]
    resolution_complete: i32,
}

#[derive(Debug, Clone, QueryableByName)]
struct AnchorRawEdgeRow {
    #[diesel(sql_type = Integer)]
    edge_order: i32,
    #[diesel(sql_type = Text)]
    edge_kind: String,
    #[diesel(sql_type = Text)]
    current_ancestor_id: String,
    #[diesel(sql_type = Integer)]
    ancestor_depth: i32,
}

#[derive(Debug, QueryableByName)]
struct AnchorAncestorRow {
    #[diesel(sql_type = Text)]
    parent_id: String,
    #[diesel(sql_type = Integer)]
    is_anchor: i32,
}

#[derive(Debug, QueryableByName)]
struct DagRunRow {
    #[diesel(sql_type = Integer)]
    dag_initialized: i32,
    #[diesel(sql_type = Nullable<BigInt>)]
    dag_baseline_generation: Option<i64>,
    #[diesel(sql_type = Nullable<Text>)]
    dag_root_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    dag_build_kind: Option<String>,
    #[diesel(sql_type = Text)]
    dag_init_phase: String,
    #[diesel(sql_type = BigInt)]
    dag_init_row_cursor: i64,
    #[diesel(sql_type = Text)]
    dag_init_text_cursor: String,
    #[diesel(sql_type = BigInt)]
    dag_init_counter: i64,
    #[diesel(sql_type = BigInt)]
    dag_source_revision: i64,
}

#[derive(Debug, QueryableByName)]
struct ManifestBranchRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    head_id: String,
    #[diesel(sql_type = BigInt)]
    contribution_generation: i64,
}

#[derive(Debug, QueryableByName)]
struct FrozenManifestBranchRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Nullable<BigInt>)]
    contribution_generation: Option<i64>,
    #[diesel(sql_type = Nullable<Text>)]
    head_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    state_json: Option<String>,
    #[diesel(sql_type = Integer)]
    removed: i32,
}

#[derive(Debug, QueryableByName)]
struct SourceBranchNameRow {
    #[diesel(sql_type = Text)]
    branch_name: String,
    #[diesel(sql_type = BigInt)]
    first_source_revision: i64,
}

#[derive(Debug, QueryableByName)]
struct ManifestRefreshRow {
    #[diesel(sql_type = Text)]
    branch_name: String,
    #[diesel(sql_type = BigInt)]
    refresh_id: i64,
    #[diesel(sql_type = BigInt)]
    target_contribution_generation: i64,
    #[diesel(sql_type = BigInt)]
    published_source_revision: i64,
}

#[derive(Debug, QueryableByName)]
struct BuildKindStateRow {
    #[diesel(sql_type = Text)]
    phase: String,
    #[diesel(sql_type = Text)]
    branch_cursor: String,
    #[diesel(sql_type = BigInt)]
    revision_cursor: i64,
}

#[derive(Debug, QueryableByName)]
struct BaselinePublicationRow {
    #[diesel(sql_type = BigInt)]
    source_revision: i64,
    #[diesel(sql_type = BigInt)]
    publication_epoch: i64,
}

#[derive(Debug, QueryableByName)]
struct BuildRevisionRangeRow {
    #[diesel(sql_type = BigInt)]
    baseline_revision: i64,
    #[diesel(sql_type = BigInt)]
    build_revision: i64,
}

#[derive(Debug, QueryableByName)]
struct BranchDeltaSeedRow {
    #[diesel(sql_type = Text)]
    branch_name: String,
    #[diesel(sql_type = BigInt)]
    published_source_revision: i64,
    #[diesel(sql_type = BigInt)]
    refresh_id: i64,
    #[diesel(sql_type = Text)]
    seed_cursor: String,
}

#[derive(Debug, QueryableByName)]
struct BranchRemovalSeedRow {
    #[diesel(sql_type = Text)]
    branch_name: String,
    #[diesel(sql_type = Text)]
    removal_cursor: String,
    #[diesel(sql_type = BigInt)]
    removal_refresh_id_cursor: i64,
    #[diesel(sql_type = BigInt)]
    removal_refresh_id_upper_bound: i64,
    #[diesel(sql_type = Integer)]
    removal_bound_frozen: i32,
    #[diesel(sql_type = Integer)]
    removal_complete: i32,
}

#[derive(Debug, QueryableByName)]
struct BranchRemovalCandidateRow {
    #[diesel(sql_type = BigInt)]
    refresh_id: i64,
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Integer)]
    eligible: i32,
}

#[derive(Debug, QueryableByName)]
struct BranchScopeRemovalSeedRow {
    #[diesel(sql_type = Text)]
    branch_name: String,
    #[diesel(sql_type = Text)]
    scope_removal_cursor: String,
    #[diesel(sql_type = Integer)]
    scope_reuses_baseline: i32,
}

#[derive(Debug, QueryableByName)]
struct BranchChangeJournalRow {
    #[diesel(sql_type = BigInt)]
    source_revision: i64,
    #[diesel(sql_type = Text)]
    branch_name: String,
    #[diesel(sql_type = Text)]
    change_kind: String,
    #[diesel(sql_type = Nullable<BigInt>)]
    refresh_id: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    base_contribution_generation: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    target_contribution_generation: Option<i64>,
    #[diesel(sql_type = Nullable<Text>)]
    head_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    state_json: Option<String>,
}

#[derive(Debug, QueryableByName)]
struct StagedBranchChangeRow {
    #[diesel(sql_type = Text)]
    change_kind: String,
}

#[derive(Debug, QueryableByName)]
struct ScopeQueueRow {
    #[diesel(sql_type = Text)]
    branch_name: String,
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Text)]
    traversal_kind: String,
    #[diesel(sql_type = Integer)]
    node_expanded: i32,
    #[diesel(sql_type = Text)]
    parent_cursor: String,
    #[diesel(sql_type = Text)]
    child_cursor: String,
}

#[derive(Debug, QueryableByName)]
struct BigIntRow {
    #[diesel(sql_type = BigInt)]
    value: i64,
}

#[derive(Debug, QueryableByName)]
struct TextValueRow {
    #[diesel(sql_type = Text)]
    value: String,
}

#[derive(Debug, QueryableByName)]
struct EligibleTextValueRow {
    #[diesel(sql_type = Text)]
    value: String,
    #[diesel(sql_type = Integer)]
    eligible: i32,
}

#[derive(Debug)]
struct ProjectedBranchCandidate {
    inspected_cursor: String,
    branch: Option<ProjectedBranchRow>,
}

#[derive(Debug, QueryableByName)]
struct SeedStateRow {
    #[diesel(sql_type = Text)]
    dag_seed_cursor: String,
    #[diesel(sql_type = Integer)]
    dag_seed_complete: i32,
}

#[derive(Debug, QueryableByName)]
struct ParentExpansionRow {
    #[diesel(sql_type = Text)]
    next_child_id: String,
    #[diesel(sql_type = Integer)]
    complete: i32,
}

#[derive(Debug, QueryableByName)]
struct FinalizeStateRow {
    #[diesel(sql_type = Text)]
    dag_finalize_phase: String,
    #[diesel(sql_type = Text)]
    dag_finalize_mode: String,
    #[diesel(sql_type = Text)]
    dag_finalize_cursor: String,
}

#[derive(Debug, QueryableByName)]
struct PortCopyRow {
    #[diesel(sql_type = Text)]
    mode: String,
    #[diesel(sql_type = Text)]
    edge_key: String,
    #[diesel(sql_type = Text)]
    source_id: String,
    #[diesel(sql_type = Text)]
    target_id: String,
    #[diesel(sql_type = Integer)]
    source_slot: i32,
    #[diesel(sql_type = Integer)]
    target_slot: i32,
}

type IncrementalFrontier = AdaptiveFrontier<FrontierNode, SqliteFrontierStore>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DagProgress {
    discovered_nodes: usize,
    processed_nodes: usize,
}

#[derive(Debug, QueryableByName)]
struct DagProgressRow {
    #[diesel(sql_type = BigInt)]
    discovered_nodes: i64,
    #[diesel(sql_type = BigInt)]
    processed_nodes: i64,
}

#[derive(Debug, Clone, Copy)]
struct OutputNodeCounts {
    all_nodes: usize,
    anchors_nodes: usize,
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
    validate_incremental_build_config(config)?;
    require_build_lease(snapshots, lease).await?;
    let (store, mut frontier) =
        initialize_incremental_build(snapshots, root_id, baseline_generation, lease, config)
            .await?;

    if store.has_active_nodes().await? {
        seed_incremental_frontier(snapshots, lease, &store, &mut frontier).await?;
        traverse_incremental_frontier(
            snapshots,
            lease,
            &store,
            &mut frontier,
            config.graph_buffer_limit,
            source_version,
        )
        .await?;
    }
    finalize_incremental_build(snapshots, &store, lease, source_version, frontier.metrics()).await
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
    config: IncrementalBuildConfig,
) -> crate::Result<(IncrementalWorkStore, IncrementalFrontier)> {
    let generation = lease.generation();
    let mut store = IncrementalWorkStore::new(
        snapshots.database(),
        root_id.to_owned(),
        baseline_generation,
        generation,
        lease.owner_id().to_owned(),
        lease.lease_epoch(),
        config.child_page_size,
    );
    store.begin().await?;
    let frontier_store = SqliteFrontierStore::new_leased(
        store.database.clone(),
        generation,
        lease.owner_id().to_owned(),
        lease.lease_epoch(),
    );
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

async fn seed_incremental_frontier(
    snapshots: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
    store: &IncrementalWorkStore,
    frontier: &mut IncrementalFrontier,
) -> crate::Result<()> {
    match store.kind {
        IncrementalBuildKind::Full => seed_full_frontier(lease, store, frontier).await,
        IncrementalBuildKind::Append => {
            seed_append_frontier(snapshots, lease, store, frontier).await
        }
    }
}

async fn seed_full_frontier(
    lease: &IncrementalBuildLease,
    store: &IncrementalWorkStore,
    frontier: &mut IncrementalFrontier,
) -> crate::Result<()> {
    if store.seed_state().await?.dag_seed_complete == 1 {
        return enqueue_ready_nodes(lease, store, frontier).await;
    }
    let root = store.load_source_node(&store.root_id).await?;
    let root_item = FrontierNode {
        created_at_ns: saturating_i64(root.created_at.as_nanosecond()),
        node_id: root.id.clone(),
    };
    store.insert_root(&root_item).await?;
    store.initialize_parent_requirements(&root_item).await?;
    let previous_mode = frontier.mode();
    frontier
        .push_batch([root_item])
        .await
        .map_err(frontier_error)?;
    log_frontier_storage_transition(lease, previous_mode, frontier);
    store.mark_seed_complete("").await?;
    Ok(())
}

async fn seed_append_frontier(
    snapshots: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
    store: &IncrementalWorkStore,
    frontier: &mut IncrementalFrontier,
) -> crate::Result<()> {
    let seed_state = store.seed_state().await?;
    if seed_state.dag_seed_complete == 1 {
        return enqueue_ready_nodes(lease, store, frontier).await;
    }
    let mut seed_cursor = seed_state.dag_seed_cursor;
    loop {
        let page = store.append_seed_page(&seed_cursor).await?;
        let Some(next_cursor) = page.inspected_cursor else {
            store.mark_seed_complete(&seed_cursor).await?;
            return Ok(());
        };
        store.insert_append_seeds(&page.nodes).await?;
        for seed in &page.nodes {
            store.initialize_parent_requirements(seed).await?;
        }
        let previous_mode = frontier.mode();
        frontier
            .push_batch(page.nodes)
            .await
            .map_err(frontier_error)?;
        log_frontier_storage_transition(lease, previous_mode, frontier);
        seed_cursor = next_cursor;
        store.mark_seed_progress(&seed_cursor).await?;
        require_build_lease(snapshots, lease).await?;
        tokio::task::yield_now().await;
    }
}

async fn traverse_incremental_frontier(
    snapshots: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
    store: &IncrementalWorkStore,
    frontier: &mut IncrementalFrontier,
    graph_buffer_limit: usize,
    source_version: u64,
) -> crate::Result<()> {
    let mut buffer = GraphBuffer::new(graph_buffer_limit);
    let mut processed_nodes = store.dag_progress().await?.processed_nodes;
    enqueue_ready_nodes(lease, store, frontier).await?;
    while let Some(cursor) = frontier.peek_min().await.map_err(frontier_error)? {
        process_frontier_cursor(snapshots, lease, store, frontier, &mut buffer, &cursor).await?;
        processed_nodes = processed_nodes.saturating_add(1);
        maintain_incremental_traversal(
            snapshots,
            lease,
            store,
            frontier,
            &mut buffer,
            source_version,
            processed_nodes,
        )
        .await?;
    }
    flush_graph_buffer(snapshots, lease, &mut buffer).await?;
    require_build_lease(snapshots, lease).await?;
    Ok(())
}

async fn process_frontier_cursor(
    snapshots: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
    store: &IncrementalWorkStore,
    frontier: &mut IncrementalFrontier,
    buffer: &mut GraphBuffer,
    cursor: &FrontierNode,
) -> crate::Result<()> {
    let node = store.load_source_node(&cursor.node_id).await?;
    let mut projection_steps = 0usize;
    loop {
        let state = store.work_node(&node.id).await?.with_context(|| {
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "work_node",
                value: node.id.clone(),
            }
        })?;
        if state.projection_complete == 1 {
            break;
        }
        store.process_node(&node, snapshots, lease, buffer).await?;
        projection_steps = projection_steps.saturating_add(1);
        if projection_steps.is_multiple_of(DEFAULT_CHILD_PAGE_SIZE) {
            require_build_lease(snapshots, lease).await?;
        }
        tokio::task::yield_now().await;
    }
    expand_all_child_pages(snapshots, lease, store, &node.id).await?;
    complete_frontier_minimum(lease, frontier, cursor).await?;
    enqueue_ready_nodes(lease, store, frontier).await?;
    Ok(())
}

async fn complete_frontier_minimum(
    lease: &IncrementalBuildLease,
    frontier: &mut IncrementalFrontier,
    cursor: &FrontierNode,
) -> crate::Result<()> {
    let previous_mode = frontier.mode();
    let outcome = frontier
        .complete_min(cursor)
        .await
        .map_err(frontier_error)?;
    log_frontier_storage_transition(lease, previous_mode, frontier);
    match outcome {
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

async fn expand_all_child_pages(
    snapshots: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
    store: &IncrementalWorkStore,
    node_id: &str,
) -> crate::Result<()> {
    let mut expansion = store.parent_expansion(node_id).await?;
    let mut child_pages = 0usize;
    let mut lease_renewed_at = tokio::time::Instant::now();
    while expansion.complete == 0 {
        store
            .discover_ready_child_page(node_id, &expansion.next_child_id)
            .await?;
        expansion = store.parent_expansion(node_id).await?;
        child_pages = child_pages.saturating_add(1);
        if should_renew_child_page_lease(child_pages, lease_renewed_at) {
            require_build_lease(snapshots, lease).await?;
            lease_renewed_at = tokio::time::Instant::now();
        }
        tokio::task::yield_now().await;
    }
    Ok(())
}

async fn enqueue_ready_nodes(
    lease: &IncrementalBuildLease,
    store: &IncrementalWorkStore,
    frontier: &mut IncrementalFrontier,
) -> crate::Result<()> {
    loop {
        let ready = store.unqueued_ready_node_page().await?;
        if ready.is_empty() {
            return Ok(());
        }
        let previous_mode = frontier.mode();
        frontier.push_batch(ready).await.map_err(frontier_error)?;
        log_frontier_storage_transition(lease, previous_mode, frontier);
    }
}

fn log_frontier_storage_transition(
    lease: &IncrementalBuildLease,
    previous_mode: FrontierMode,
    frontier: &IncrementalFrontier,
) {
    let frontier_storage_mode = frontier.mode();
    if previous_mode == frontier_storage_mode {
        return;
    }
    tracing::info!(
        rebuild_output_scope = "anchors_and_all",
        build_run_id = lease.generation(),
        build_lease_epoch = lease.lease_epoch(),
        build_frozen_source_revision = lease.frozen_source_revision(),
        previous_frontier_storage_mode = frontier_mode_value(previous_mode),
        frontier_storage_mode = frontier_mode_value(frontier_storage_mode),
        frontier_pending_node_count = frontier.len(),
        frontier_in_memory_node_count = frontier.hot_len(),
        "console graph frontier storage mode changed",
    );
}

fn frontier_mode_value(mode: FrontierMode) -> &'static str {
    match mode {
        FrontierMode::HotAll => "in_memory",
        FrontierMode::Spilled => "external_sqlite",
    }
}

fn should_renew_child_page_lease(child_pages: usize, renewed_at: tokio::time::Instant) -> bool {
    child_pages.is_multiple_of(128)
        || renewed_at.elapsed() >= INCREMENTAL_BUILD_LEASE_HEARTBEAT_INTERVAL
}

async fn maintain_incremental_traversal(
    snapshots: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
    store: &IncrementalWorkStore,
    frontier: &IncrementalFrontier,
    buffer: &mut GraphBuffer,
    process_local_invalidation_version: u64,
    processed_nodes: usize,
) -> crate::Result<()> {
    if buffer.should_flush() {
        flush_graph_buffer(snapshots, lease, buffer).await?;
    }
    if processed_nodes.is_multiple_of(1_024) {
        require_build_lease(snapshots, lease).await?;
        tokio::task::yield_now().await;
    }
    if processed_nodes > 0 && processed_nodes.is_multiple_of(BUILD_PROGRESS_NODE_INTERVAL) {
        let progress = store.dag_progress().await?;
        let metrics = frontier.metrics();
        tracing::info!(
            rebuild_output_scope = "anchors_and_all",
            process_local_invalidation_version,
            build_run_id = lease.generation(),
            build_lease_epoch = lease.lease_epoch(),
            build_frozen_source_revision = lease.frozen_source_revision(),
            run_completed_node_count = progress.processed_nodes,
            run_discovered_node_count = progress.discovered_nodes,
            frontier_pending_node_count = frontier.len(),
            frontier_in_memory_node_count = frontier.hot_len(),
            lease_session_frontier_hot_to_spilled_count = metrics.hot_to_spilled,
            lease_session_frontier_spilled_to_hot_count = metrics.spilled_to_hot,
            "console graph incremental build progress",
        );
    }
    Ok(())
}

async fn finalize_incremental_build(
    snapshots: &ConsoleGraphSnapshotStore,
    store: &IncrementalWorkStore,
    lease: &IncrementalBuildLease,
    source_version: u64,
    frontier: FrontierMetrics,
) -> crate::Result<IncrementalBuildStats> {
    let progress = store.dag_progress().await?;
    validate_incremental_completion(progress, store.has_unprocessed_nodes().await?)?;
    loop {
        let state = store.finalize_state().await?;
        match state.dag_finalize_phase.as_str() {
            "traversal" => store.begin_port_finalization().await?,
            "ports" => {
                store.copy_port_batch(&state).await?;
                tokio::task::yield_now().await;
            }
            "labels" => {
                store.apply_branch_label_batch(&state).await?;
                tokio::task::yield_now().await;
            }
            "anchors" => {
                snapshots
                    .finish_incremental_mode(lease, source_version, GraphMode::Anchors)
                    .await?;
                store.checkpoint_finalize_phase("anchors", "all").await?;
            }
            "all" => {
                snapshots
                    .finish_incremental_mode(lease, source_version, GraphMode::All)
                    .await?;
                store.checkpoint_finalize_phase("all", "ready").await?;
            }
            "ready" => break,
            value => {
                return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                    column: "dag_finalize_phase",
                    value: value.to_owned(),
                }
                .fail();
            }
        }
    }
    let output_counts = OutputNodeCounts {
        all_nodes: store.completed_shell_node_count(GraphMode::All).await?,
        anchors_nodes: store.completed_shell_node_count(GraphMode::Anchors).await?,
    };
    let classification_reason = store.build_classification_reason().await?;
    log_incremental_build(
        source_version,
        lease,
        store.kind,
        progress,
        output_counts,
        classification_reason,
        frontier,
    );
    Ok(IncrementalBuildStats {
        processed_nodes: progress.processed_nodes,
        all_nodes: output_counts.all_nodes,
        anchors_nodes: output_counts.anchors_nodes,
        reused_baseline: store.kind == IncrementalBuildKind::Append,
        classification_reason,
        frontier,
    })
}

fn validate_incremental_completion(
    progress: DagProgress,
    has_unprocessed_nodes: bool,
) -> crate::Result<()> {
    ensure!(
        !has_unprocessed_nodes && progress.processed_nodes == progress.discovered_nodes,
        crate::error::InvalidGraphSnapshotStoreValueSnafu {
            column: "incremental_graph",
            value: format!(
                "processed={} discovered={} has_unprocessed_nodes={has_unprocessed_nodes}",
                progress.processed_nodes, progress.discovered_nodes
            ),
        }
    );
    Ok(())
}

fn log_incremental_build(
    process_local_invalidation_version: u64,
    lease: &IncrementalBuildLease,
    build_kind: IncrementalBuildKind,
    progress: DagProgress,
    output_counts: OutputNodeCounts,
    classification_reason: BuildClassificationReason,
    frontier: FrontierMetrics,
) {
    tracing::info!(
        rebuild_output_scope = "anchors_and_all",
        process_local_invalidation_version,
        build_run_id = lease.generation(),
        build_lease_epoch = lease.lease_epoch(),
        build_frozen_source_revision = lease.frozen_source_revision(),
        graph_build_kind = build_kind_value(build_kind),
        graph_build_classification_reason = classification_reason.as_str(),
        run_completed_node_count = progress.processed_nodes,
        run_discovered_node_count = progress.discovered_nodes,
        staged_all_node_count = output_counts.all_nodes,
        staged_anchors_node_count = output_counts.anchors_nodes,
        lease_session_frontier_hot_to_spilled_count = frontier.hot_to_spilled,
        lease_session_frontier_spilled_to_hot_count = frontier.spilled_to_hot,
        lease_session_frontier_max_in_memory_node_count = frontier.max_hot_len,
        lease_session_frontier_candidate_node_count = frontier.requested_pushes,
        lease_session_frontier_distinct_candidate_node_count = frontier.distinct_pushes,
        lease_session_frontier_newly_enqueued_node_count = frontier.inserted_pushes,
        lease_session_frontier_repeated_within_batch_node_count =
            frontier.repeated_within_batch_pushes,
        lease_session_frontier_already_seen_node_count = frontier.already_seen_pushes,
        "console graph build output staged",
    );
}

async fn flush_graph_buffer(
    snapshots: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
    buffer: &mut GraphBuffer,
) -> crate::Result<()> {
    for items in [&mut buffer.anchors, &mut buffer.all] {
        if items.is_empty() {
            continue;
        }
        snapshots
            .write_incremental_batch(
                lease,
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
    lease: &IncrementalBuildLease,
    buffer: &mut GraphBuffer,
    mode: GraphMode,
    node: GraphLayoutNode,
) -> crate::Result<()> {
    match mode {
        GraphMode::Anchors => buffer.anchors.nodes.push(node),
        GraphMode::All => buffer.all.nodes.push(node),
    }
    if buffer.should_flush() {
        flush_graph_buffer(snapshots, lease, buffer).await?;
    }
    Ok(())
}

async fn push_buffered_edge(
    snapshots: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
    buffer: &mut GraphBuffer,
    mode: GraphMode,
    edge: GraphLayoutEdge,
) -> crate::Result<()> {
    match mode {
        GraphMode::Anchors => buffer.anchors.edges.push(edge),
        GraphMode::All => buffer.all.edges.push(edge),
    }
    if buffer.should_flush() {
        flush_graph_buffer(snapshots, lease, buffer).await?;
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

fn build_kind_value(kind: IncrementalBuildKind) -> &'static str {
    match kind {
        IncrementalBuildKind::Full => "full",
        IncrementalBuildKind::Append => "append",
    }
}

fn stored_build_kind(value: Option<&str>) -> Result<IncrementalBuildKind, ()> {
    match value {
        Some("full") => Ok(IncrementalBuildKind::Full),
        Some("append") => Ok(IncrementalBuildKind::Append),
        _ => Err(()),
    }
}

fn stored_node_count(column: &'static str, value: i64) -> crate::Result<usize> {
    usize::try_from(value).map_err(|_| crate::Error::InvalidGraphSnapshotStoreValue {
        column,
        value: value.to_string(),
    })
}

fn require_build_fence(
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
    if !owned {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(())
}

fn record_discovered_nodes(
    connection: &mut SqliteConnection,
    run_id: i64,
    owner_id: &str,
    lease_epoch: i64,
    inserted: usize,
) -> QueryResult<()> {
    if inserted == 0 {
        return Ok(());
    }
    let updated = diesel::sql_query(
        "UPDATE console_graph_build_runs \
         SET dag_discovered_node_count = dag_discovered_node_count + ? \
         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? AND status = 'building'",
    )
    .bind::<BigInt, _>(i64::try_from(inserted).unwrap_or(i64::MAX))
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(owner_id)
    .bind::<BigInt, _>(lease_epoch)
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(())
}

fn advance_dag_init_phase(
    connection: &mut SqliteConnection,
    run_id: i64,
    owner_id: &str,
    lease_epoch: i64,
    current_phase: &str,
    next_phase: &str,
) -> QueryResult<()> {
    let updated = diesel::sql_query(
        "UPDATE console_graph_build_runs \
         SET dag_init_phase = ?, dag_init_row_cursor = 0, \
             dag_init_text_cursor = '', dag_init_text_cursor_secondary = '', \
             dag_init_counter = 0 \
         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
           AND status = 'building' AND dag_init_phase = ?",
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

fn checkpoint_build_kind(
    connection: &mut SqliteConnection,
    run_id: i64,
    owner_id: &str,
    lease_epoch: i64,
    current_phase: &str,
    kind: IncrementalBuildKind,
    classification_reason: BuildClassificationReason,
) -> QueryResult<()> {
    let next_phase = match kind {
        IncrementalBuildKind::Full => "full_reset",
        IncrementalBuildKind::Append => "delta_remove",
    };
    let updated = diesel::sql_query(
        "UPDATE console_graph_build_runs \
         SET dag_build_kind = ?, dag_build_classification_reason = ?, dag_init_phase = ?, \
             dag_init_row_cursor = 0, dag_init_text_cursor = '', \
             dag_init_text_cursor_secondary = '', dag_init_counter = 0 \
         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
           AND status = 'building' AND dag_init_phase = ?",
    )
    .bind::<Text, _>(build_kind_value(kind))
    .bind::<Text, _>(classification_reason.as_str())
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

fn journal_page_is_contiguous(
    baseline_revision: i64,
    saved_revision_cursor: i64,
    revisions: impl IntoIterator<Item = i64>,
) -> bool {
    let mut previous_revision = if saved_revision_cursor == 0 {
        baseline_revision
    } else {
        saved_revision_cursor
    };
    let mut saw_revision = false;
    for revision in revisions {
        if revision == previous_revision && (saved_revision_cursor != 0 || saw_revision) {
            saw_revision = true;
            continue;
        }
        let Some(expected_revision) = previous_revision.checked_add(1) else {
            return false;
        };
        if revision != expected_revision {
            return false;
        }
        previous_revision = revision;
        saw_revision = true;
    }
    true
}

fn journal_cursor_covers_revision_range(
    baseline_revision: i64,
    build_revision: i64,
    saved_revision_cursor: i64,
) -> bool {
    build_revision >= baseline_revision
        && if saved_revision_cursor == 0 {
            build_revision == baseline_revision
        } else {
            saved_revision_cursor == build_revision
        }
}

fn reset_staged_branch_change(
    connection: &mut SqliteConnection,
    run_id: i64,
    branch_name: &str,
) -> QueryResult<bool> {
    for table in [
        "console_graph_build_branch_deltas",
        "console_graph_build_source_refresh_manifest",
    ] {
        let deleted = diesel::sql_query(format!(
            "DELETE FROM {table} WHERE rowid IN ( \
                 SELECT rowid FROM {table} \
                 WHERE run_id = ? AND branch_name = ? LIMIT ? \
             )"
        ))
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(branch_name)
        .bind::<BigInt, _>(BRANCH_CHANGE_RESET_BATCH_SIZE)
        .execute(connection)?;
        if deleted > 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn stage_branch_journal_change(
    connection: &mut SqliteConnection,
    run_id: i64,
    baseline_generation: i64,
    change: &BranchChangeJournalRow,
) -> QueryResult<BranchChangeCompatibility> {
    let baseline = diesel::sql_query(
        "SELECT name, head_id, contribution_generation \
         FROM console_graph_materialization_branches \
         WHERE generation = ? AND name = ?",
    )
    .bind::<BigInt, _>(baseline_generation)
    .bind::<Text, _>(&change.branch_name)
    .get_result::<ManifestBranchRow>(connection)
    .optional()?;
    let staged = diesel::sql_query(
        "SELECT change_kind \
         FROM console_graph_build_changed_branches \
         WHERE run_id = ? AND branch_name = ?",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&change.branch_name)
    .get_result::<StagedBranchChangeRow>(connection)
    .optional()?;
    let staged_manifest = diesel::sql_query(
        "SELECT branch_name AS name, head_id, contribution_generation \
         FROM console_graph_build_source_manifest \
         WHERE run_id = ? AND branch_name = ?",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&change.branch_name)
    .get_result::<ManifestBranchRow>(connection)
    .optional()?;
    let current_generation = if staged
        .as_ref()
        .is_some_and(|staged| staged.change_kind == "delete")
    {
        None
    } else {
        staged_manifest
            .as_ref()
            .map(|branch| branch.contribution_generation)
            .or_else(|| {
                baseline
                    .as_ref()
                    .map(|branch| branch.contribution_generation)
            })
    };

    if change.change_kind == "delete" {
        if change.base_contribution_generation != current_generation {
            return Ok(BranchChangeCompatibility::RequiresFull(
                BuildClassificationReason::SourceChangeJournalInvalid,
            ));
        }
        if !reset_staged_branch_change(connection, run_id, &change.branch_name)? {
            return Ok(BranchChangeCompatibility::PendingReset);
        }
        diesel::sql_query(
            "DELETE FROM console_graph_build_source_manifest \
             WHERE run_id = ? AND branch_name = ?",
        )
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&change.branch_name)
        .execute(connection)?;
        if baseline.is_none() {
            diesel::sql_query(
                "DELETE FROM console_graph_build_changed_branches \
                 WHERE run_id = ? AND branch_name = ?",
            )
            .bind::<BigInt, _>(run_id)
            .bind::<Text, _>(&change.branch_name)
            .execute(connection)?;
            return Ok(BranchChangeCompatibility::Compatible);
        }
        diesel::sql_query(
            "INSERT INTO console_graph_build_changed_branches ( \
                 run_id, branch_name, baseline_contribution_generation, \
                 target_contribution_generation, change_kind, removal_complete, \
                 scope_removal_complete \
             ) VALUES (?, ?, ?, NULL, 'delete', 0, 0) \
             ON CONFLICT(run_id, branch_name) DO UPDATE SET \
                 target_contribution_generation = NULL, change_kind = 'delete', \
                 removal_cursor = '', removal_refresh_id_cursor = -1, \
                 removal_refresh_id_upper_bound = -1, removal_bound_frozen = 0, \
                 removal_complete = 0, \
                 scope_removal_cursor = '', scope_removal_complete = 0, \
                 scope_reuses_baseline = 0",
        )
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&change.branch_name)
        .bind::<BigInt, _>(
            baseline
                .as_ref()
                .expect("baseline branch checked above")
                .contribution_generation,
        )
        .execute(connection)?;
        return Ok(BranchChangeCompatibility::Compatible);
    }

    let Some(target_generation) = change.target_contribution_generation else {
        return Ok(BranchChangeCompatibility::RequiresFull(
            BuildClassificationReason::SourceChangeJournalInvalid,
        ));
    };
    let Some(head_id) = change.head_id.as_deref() else {
        return Ok(BranchChangeCompatibility::RequiresFull(
            BuildClassificationReason::SourceChangeJournalInvalid,
        ));
    };
    let Some(state_json) = change.state_json.as_deref() else {
        return Ok(BranchChangeCompatibility::RequiresFull(
            BuildClassificationReason::SourceChangeJournalInvalid,
        ));
    };

    let (delta_refresh, staged_change_kind) = match change.change_kind.as_str() {
        "replace" => {
            let Some(refresh_id) = change.refresh_id else {
                return Ok(BranchChangeCompatibility::RequiresFull(
                    BuildClassificationReason::SourceChangeJournalInvalid,
                ));
            };
            let valid_full_refresh = diesel::sql_query(
                "SELECT CASE WHEN EXISTS ( \
                     SELECT 1 FROM console_graph_source_refresh_runs \
                     WHERE refresh_id = ? AND branch_name = ? \
                       AND target_contribution_generation = ? \
                       AND refresh_kind = 'full' AND status = 'published' \
                       AND published_source_revision = ? \
                 ) THEN 1 ELSE 0 END AS value",
            )
            .bind::<BigInt, _>(refresh_id)
            .bind::<Text, _>(&change.branch_name)
            .bind::<BigInt, _>(target_generation)
            .bind::<BigInt, _>(change.source_revision)
            .get_result::<IntegerRow>(connection)?
            .value
                == 1;
            if !valid_full_refresh {
                return Ok(BranchChangeCompatibility::RequiresFull(
                    BuildClassificationReason::SourceChangeJournalInvalid,
                ));
            }
            if !reset_staged_branch_change(connection, run_id, &change.branch_name)? {
                return Ok(BranchChangeCompatibility::PendingReset);
            }
            (
                Some((refresh_id, "full")),
                if baseline.is_some() {
                    "replace"
                } else {
                    "added"
                },
            )
        }
        "append" => {
            if change.base_contribution_generation != current_generation {
                return Ok(BranchChangeCompatibility::RequiresFull(
                    BuildClassificationReason::SourceAppendBaseMismatch,
                ));
            }
            let Some(refresh_id) = change.refresh_id else {
                return Ok(BranchChangeCompatibility::RequiresFull(
                    BuildClassificationReason::SourceChangeJournalInvalid,
                ));
            };
            let valid_append_refresh = diesel::sql_query(
                "SELECT CASE WHEN EXISTS ( \
                     SELECT 1 FROM console_graph_source_refresh_runs \
                     WHERE refresh_id = ? AND branch_name = ? \
                       AND target_contribution_generation = ? \
                       AND refresh_kind = 'append' AND status = 'published' \
                       AND published_source_revision = ? \
                 ) THEN 1 ELSE 0 END AS value",
            )
            .bind::<BigInt, _>(refresh_id)
            .bind::<Text, _>(&change.branch_name)
            .bind::<BigInt, _>(target_generation)
            .bind::<BigInt, _>(change.source_revision)
            .get_result::<IntegerRow>(connection)?
            .value
                == 1;
            if !valid_append_refresh {
                return Ok(BranchChangeCompatibility::RequiresFull(
                    BuildClassificationReason::SourceChangeJournalInvalid,
                ));
            }
            let staged_change_kind = staged
                .as_ref()
                .map(|staged| staged.change_kind.as_str())
                .filter(|kind| matches!(*kind, "added" | "replace"))
                .unwrap_or("append");
            (Some((refresh_id, "append")), staged_change_kind)
        }
        "metadata" => {
            let Some(expected_generation) = current_generation else {
                return Ok(BranchChangeCompatibility::RequiresFull(
                    BuildClassificationReason::SourceMetadataBaselineMismatch,
                ));
            };
            let staged_head = diesel::sql_query(
                "SELECT head_id AS value FROM console_graph_build_source_manifest \
                 WHERE run_id = ? AND branch_name = ?",
            )
            .bind::<BigInt, _>(run_id)
            .bind::<Text, _>(&change.branch_name)
            .get_result::<TextValueRow>(connection)
            .optional()?
            .map(|row| row.value);
            let expected_head = staged_head
                .as_deref()
                .or_else(|| baseline.as_ref().map(|branch| branch.head_id.as_str()));
            if expected_head != Some(head_id) || target_generation != expected_generation {
                return Ok(BranchChangeCompatibility::RequiresFull(
                    BuildClassificationReason::SourceMetadataBaselineMismatch,
                ));
            }
            let staged_change_kind = staged
                .as_ref()
                .map(|staged| staged.change_kind.as_str())
                .filter(|kind| matches!(*kind, "added" | "append" | "replace"))
                .unwrap_or("metadata");
            (None, staged_change_kind)
        }
        _ => {
            return Ok(BranchChangeCompatibility::RequiresFull(
                BuildClassificationReason::SourceChangeJournalInvalid,
            ));
        }
    };

    diesel::sql_query(
        "INSERT INTO console_graph_build_source_manifest ( \
             run_id, branch_name, contribution_generation, head_id, state_json \
         ) VALUES (?, ?, ?, ?, ?) \
         ON CONFLICT(run_id, branch_name) DO UPDATE SET \
             contribution_generation = excluded.contribution_generation, \
             head_id = excluded.head_id, state_json = excluded.state_json",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&change.branch_name)
    .bind::<BigInt, _>(target_generation)
    .bind::<Text, _>(head_id)
    .bind::<Text, _>(state_json)
    .execute(connection)?;
    let remove_baseline_scope =
        baseline.is_some() && matches!(staged_change_kind, "append" | "replace");
    diesel::sql_query(
        "INSERT INTO console_graph_build_changed_branches ( \
             run_id, branch_name, baseline_contribution_generation, \
             target_contribution_generation, change_kind, removal_complete, \
             scope_removal_complete \
         ) VALUES (?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(run_id, branch_name) DO UPDATE SET \
             target_contribution_generation = excluded.target_contribution_generation, \
             change_kind = excluded.change_kind, \
             removal_cursor = '', removal_refresh_id_cursor = -1, \
             removal_refresh_id_upper_bound = -1, removal_bound_frozen = 0, \
             removal_complete = excluded.removal_complete, \
             scope_removal_cursor = '', \
             scope_removal_complete = excluded.scope_removal_complete, \
             scope_reuses_baseline = 0",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&change.branch_name)
    .bind::<Nullable<BigInt>, _>(
        baseline
            .as_ref()
            .map(|branch| branch.contribution_generation),
    )
    .bind::<BigInt, _>(target_generation)
    .bind::<Text, _>(staged_change_kind)
    .bind::<Integer, _>(i32::from(staged_change_kind != "replace"))
    .bind::<Integer, _>(i32::from(!remove_baseline_scope))
    .execute(connection)?;
    if let Some((refresh_id, refresh_kind)) = delta_refresh {
        diesel::sql_query(
            "INSERT OR IGNORE INTO console_graph_build_source_refresh_manifest ( \
                 run_id, branch_name, refresh_id \
             ) VALUES (?, ?, ?)",
        )
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&change.branch_name)
        .bind::<BigInt, _>(refresh_id)
        .execute(connection)?;
        diesel::sql_query(
            "INSERT OR IGNORE INTO console_graph_build_branch_deltas ( \
                 run_id, branch_name, published_source_revision, refresh_id, refresh_kind \
             ) VALUES (?, ?, ?, ?, ?)",
        )
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&change.branch_name)
        .bind::<BigInt, _>(change.source_revision)
        .bind::<BigInt, _>(refresh_id)
        .bind::<Text, _>(refresh_kind)
        .execute(connection)?;
    }
    Ok(BranchChangeCompatibility::Compatible)
}
fn expand_scope_item(
    connection: &mut SqliteConnection,
    run_id: i64,
    owner_id: &str,
    lease_epoch: i64,
    item: &ScopeQueueRow,
) -> QueryResult<()> {
    let baseline_scope_reached = diesel::sql_query(
        "SELECT CASE WHEN run.dag_build_kind = 'append' AND NOT EXISTS ( \
             SELECT 1 FROM console_graph_build_changed_branches AS changed \
             WHERE changed.run_id = run.run_id AND changed.branch_name = ? \
               AND changed.change_kind = 'replace' \
         ) AND EXISTS ( \
             SELECT 1 FROM console_graph_anchor_scopes AS baseline \
             WHERE baseline.generation = run.dag_baseline_generation \
               AND baseline.branch_name = ? AND baseline.node_id = ? \
         ) THEN 1 ELSE 0 END AS value \
         FROM console_graph_build_runs AS run WHERE run.run_id = ?",
    )
    .bind::<Text, _>(&item.branch_name)
    .bind::<Text, _>(&item.branch_name)
    .bind::<Text, _>(&item.node_id)
    .bind::<BigInt, _>(run_id)
    .get_result::<IntegerRow>(connection)?
    .value
        == 1;
    if baseline_scope_reached {
        diesel::sql_query(
            "UPDATE console_graph_build_changed_branches \
             SET scope_reuses_baseline = 1 \
             WHERE run_id = ? AND branch_name = ? \
               AND scope_removal_complete = 0",
        )
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&item.branch_name)
        .execute(connection)?;
        if item.node_expanded == 0 {
            let updated = diesel::sql_query(
                "UPDATE console_graph_build_scope_queue SET node_expanded = 1 \
                 WHERE run_id = ? AND branch_name = ? AND node_id = ? \
                   AND traversal_kind = ? AND processed = 0 AND node_expanded = 0",
            )
            .bind::<BigInt, _>(run_id)
            .bind::<Text, _>(&item.branch_name)
            .bind::<Text, _>(&item.node_id)
            .bind::<Text, _>(&item.traversal_kind)
            .execute(connection)?;
            if updated != 1 {
                return Err(diesel::result::Error::NotFound);
            }
            return Ok(());
        }
    }
    if item.node_expanded == 0 {
        let inserted_scope = diesel::sql_query(
            "INSERT OR IGNORE INTO console_graph_anchor_scopes \
                 (generation, branch_name, node_id) \
             SELECT ?, ?, nodes.node_id \
             FROM console_graph_source_nodes AS nodes \
             INNER JOIN console_graph_build_source_manifest AS manifest \
                 ON manifest.run_id = ? AND manifest.branch_name = ? \
             INNER JOIN console_graph_build_runs AS run \
                 ON run.run_id = manifest.run_id \
             LEFT JOIN console_graph_build_effective_source_branch_nodes AS membership \
                 ON membership.run_id = manifest.run_id \
                AND membership.branch_name = manifest.branch_name \
                AND membership.node_id = nodes.node_id \
             LEFT JOIN console_graph_anchor_scopes AS baseline_scope \
                 ON run.dag_build_kind = 'append' \
                AND NOT EXISTS ( \
                    SELECT 1 FROM console_graph_build_changed_branches AS changed \
                    WHERE changed.run_id = run.run_id \
                      AND changed.branch_name = manifest.branch_name \
                      AND changed.change_kind = 'replace' \
                ) \
                AND baseline_scope.generation = run.dag_baseline_generation \
                AND baseline_scope.branch_name = manifest.branch_name \
                AND baseline_scope.node_id = nodes.node_id \
             WHERE nodes.node_id = ? AND nodes.parent_id <> '' \
               AND (membership.node_id IS NOT NULL \
                    OR baseline_scope.node_id IS NOT NULL)",
        )
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&item.branch_name)
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&item.branch_name)
        .bind::<Text, _>(&item.node_id)
        .execute(connection)?;
        if inserted_scope > 0 {
            let updated = diesel::sql_query(
                "UPDATE console_graph_build_runs \
                 SET dag_scope_count = dag_scope_count + ? \
                 WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                   AND status = 'building'",
            )
            .bind::<BigInt, _>(i64::try_from(inserted_scope).unwrap_or(i64::MAX))
            .bind::<BigInt, _>(run_id)
            .bind::<Text, _>(owner_id)
            .bind::<BigInt, _>(lease_epoch)
            .execute(connection)?;
            if updated != 1 {
                return Err(diesel::result::Error::NotFound);
            }
        }
        diesel::sql_query(
            "INSERT OR IGNORE INTO console_graph_build_scope_queue \
                 (run_id, branch_name, node_id, traversal_kind, processed) \
             SELECT ?, ?, nodes.parent_id, 'graph', 0 \
             FROM console_graph_source_nodes AS nodes \
             INNER JOIN console_graph_build_source_manifest AS manifest \
                 ON manifest.run_id = ? AND manifest.branch_name = ? \
             INNER JOIN console_graph_build_runs AS run \
                 ON run.run_id = manifest.run_id \
             LEFT JOIN console_graph_build_effective_source_branch_nodes AS membership \
                 ON membership.run_id = manifest.run_id \
                AND membership.branch_name = manifest.branch_name \
                AND membership.node_id = nodes.parent_id \
             LEFT JOIN console_graph_anchor_scopes AS baseline_scope \
                 ON run.dag_build_kind = 'append' \
                AND NOT EXISTS ( \
                    SELECT 1 FROM console_graph_build_changed_branches AS changed \
                    WHERE changed.run_id = run.run_id \
                      AND changed.branch_name = manifest.branch_name \
                      AND changed.change_kind = 'replace' \
                ) \
                AND baseline_scope.generation = run.dag_baseline_generation \
                AND baseline_scope.branch_name = manifest.branch_name \
                AND baseline_scope.node_id = nodes.parent_id \
             WHERE nodes.node_id = ? AND nodes.parent_id <> '' \
               AND (membership.node_id IS NOT NULL \
                    OR baseline_scope.node_id IS NOT NULL) \
               AND NOT ( \
                   COALESCE(json_type( \
                       nodes.node_json, \
                       '$.kind.Anchor.payload.Session' \
                   ), '') = 'object' \
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
               )",
        )
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&item.branch_name)
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&item.branch_name)
        .bind::<Text, _>(&item.node_id)
        .execute(connection)?;
        let parents = diesel::sql_query(
            "SELECT relations.parent_id AS value, \
                    CASE WHEN EXISTS ( \
                        SELECT 1 \
                        FROM console_graph_build_effective_source_branch_nodes AS membership \
                        WHERE membership.run_id = ? AND membership.branch_name = ? \
                          AND membership.node_id = relations.parent_id \
                    ) OR EXISTS ( \
                        SELECT 1 FROM console_graph_build_runs AS run \
                        WHERE run.run_id = ? AND run.dag_build_kind = 'append' \
                          AND NOT EXISTS ( \
                              SELECT 1 \
                              FROM console_graph_build_changed_branches AS changed \
                              WHERE changed.run_id = run.run_id \
                                AND changed.branch_name = ? \
                                AND changed.change_kind = 'replace' \
                          ) AND EXISTS ( \
                              SELECT 1 FROM console_graph_anchor_scopes AS baseline \
                              WHERE baseline.generation = run.dag_baseline_generation \
                                AND baseline.branch_name = ? \
                                AND baseline.node_id = relations.parent_id \
                          ) \
                    ) THEN 1 ELSE 0 END AS eligible \
             FROM console_graph_source_node_relations AS relations \
             WHERE relations.child_id = ? AND relations.parent_id > ? \
               AND relations.parent_id <> ( \
                   SELECT parent_id FROM console_graph_source_nodes WHERE node_id = ? \
               ) \
             ORDER BY relations.parent_id LIMIT 128",
        )
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&item.branch_name)
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&item.branch_name)
        .bind::<Text, _>(&item.branch_name)
        .bind::<Text, _>(&item.node_id)
        .bind::<Text, _>(&item.parent_cursor)
        .bind::<Text, _>(&item.node_id)
        .load::<EligibleTextValueRow>(connection)?;
        for parent in parents.iter().filter(|parent| parent.eligible == 1) {
            diesel::sql_query(
                "INSERT OR IGNORE INTO console_graph_build_scope_queue \
                     (run_id, branch_name, node_id, traversal_kind, processed) \
                 VALUES (?, ?, ?, 'graph', 0)",
            )
            .bind::<BigInt, _>(run_id)
            .bind::<Text, _>(&item.branch_name)
            .bind::<Text, _>(&parent.value)
            .execute(connection)?;
        }
        if let Some(next_parent_cursor) = parents.last().map(|parent| parent.value.as_str()) {
            let updated = diesel::sql_query(
                "UPDATE console_graph_build_scope_queue SET parent_cursor = ? \
                 WHERE run_id = ? AND branch_name = ? AND node_id = ? \
                   AND traversal_kind = ? AND processed = 0 AND node_expanded = 0 \
                   AND parent_cursor = ?",
            )
            .bind::<Text, _>(next_parent_cursor)
            .bind::<BigInt, _>(run_id)
            .bind::<Text, _>(&item.branch_name)
            .bind::<Text, _>(&item.node_id)
            .bind::<Text, _>(&item.traversal_kind)
            .bind::<Text, _>(&item.parent_cursor)
            .execute(connection)?;
            if updated != 1 {
                return Err(diesel::result::Error::NotFound);
            }
            return Ok(());
        }
        let updated = diesel::sql_query(
            "UPDATE console_graph_build_scope_queue SET node_expanded = 1 \
             WHERE run_id = ? AND branch_name = ? AND node_id = ? \
               AND traversal_kind = ? AND processed = 0 AND node_expanded = 0 \
               AND parent_cursor = ?",
        )
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&item.branch_name)
        .bind::<Text, _>(&item.node_id)
        .bind::<Text, _>(&item.traversal_kind)
        .bind::<Text, _>(&item.parent_cursor)
        .execute(connection)?;
        if updated != 1 {
            return Err(diesel::result::Error::NotFound);
        }
    }

    let children = diesel::sql_query(
        "SELECT relations.child_id AS value, \
                CASE WHEN (EXISTS ( \
                    SELECT 1 \
                    FROM console_graph_build_effective_source_branch_nodes AS membership \
                    WHERE membership.run_id = ? AND membership.branch_name = ? \
                      AND membership.node_id = relations.child_id \
                ) OR EXISTS ( \
                    SELECT 1 FROM console_graph_build_runs AS run \
                    WHERE run.run_id = ? AND run.dag_build_kind = 'append' \
                      AND NOT EXISTS ( \
                          SELECT 1 \
                          FROM console_graph_build_changed_branches AS changed \
                          WHERE changed.run_id = run.run_id \
                            AND changed.branch_name = ? \
                            AND changed.change_kind = 'replace' \
                      ) AND EXISTS ( \
                          SELECT 1 FROM console_graph_anchor_scopes AS baseline \
                          WHERE baseline.generation = run.dag_baseline_generation \
                            AND baseline.branch_name = ? \
                            AND baseline.node_id = relations.child_id \
                      ) \
                )) AND (? = 'skill_subtree' OR ( \
                    json_type(( \
                        SELECT node_json FROM console_graph_source_nodes \
                        WHERE node_id = ? \
                    ), '$.kind.ToolUse') = 'array' \
                    AND json_type(( \
                        SELECT node_json FROM console_graph_source_nodes \
                        WHERE node_id = relations.child_id \
                    ), '$.kind.Anchor.payload.SkillInvocation') = 'object' \
                )) THEN 1 ELSE 0 END AS eligible \
         FROM console_graph_source_node_relations AS relations \
         WHERE relations.parent_id = ? AND relations.child_id > ? \
         ORDER BY relations.child_id LIMIT 128",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&item.branch_name)
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&item.branch_name)
    .bind::<Text, _>(&item.branch_name)
    .bind::<Text, _>(&item.traversal_kind)
    .bind::<Text, _>(&item.node_id)
    .bind::<Text, _>(&item.node_id)
    .bind::<Text, _>(&item.child_cursor)
    .load::<EligibleTextValueRow>(connection)?;
    for child in children.iter().filter(|child| child.eligible == 1) {
        diesel::sql_query(
            "INSERT OR IGNORE INTO console_graph_build_scope_queue \
                 (run_id, branch_name, node_id, traversal_kind, processed) \
             VALUES (?, ?, ?, 'skill_subtree', 0)",
        )
        .bind::<BigInt, _>(run_id)
        .bind::<Text, _>(&item.branch_name)
        .bind::<Text, _>(&child.value)
        .execute(connection)?;
    }
    let next_cursor = children
        .last()
        .map(|child| child.value.as_str())
        .unwrap_or(item.child_cursor.as_str());
    let updated = diesel::sql_query(
        "UPDATE console_graph_build_scope_queue \
         SET child_cursor = ?, processed = ? \
         WHERE run_id = ? AND branch_name = ? AND node_id = ? \
           AND traversal_kind = ? AND processed = 0 AND child_cursor = ?",
    )
    .bind::<Text, _>(next_cursor)
    .bind::<Integer, _>(i32::from(children.is_empty()))
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(&item.branch_name)
    .bind::<Text, _>(&item.node_id)
    .bind::<Text, _>(&item.traversal_kind)
    .bind::<Text, _>(&item.child_cursor)
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(())
}

impl IncrementalWorkStore {
    fn new(
        database: SnapshotDatabase,
        root_id: String,
        baseline_generation: i64,
        run_id: i64,
        owner_id: String,
        lease_epoch: i64,
        child_page_size: usize,
    ) -> Self {
        let path = database.path().to_owned();
        Self {
            database,
            path,
            run_id,
            owner_id,
            lease_epoch,
            baseline_generation,
            root_id,
            child_page_size,
            layout: StableLayoutConfig::default(),
            kind: IncrementalBuildKind::Full,
        }
    }

    async fn begin(&mut self) -> crate::Result<()> {
        loop {
            let state = self.dag_run_state().await?;
            if state.dag_initialized == 1 {
                ensure!(
                    state.dag_baseline_generation == Some(self.baseline_generation)
                        && state.dag_root_id.as_deref() == Some(self.root_id.as_str()),
                    crate::error::InvalidGraphSnapshotStoreValueSnafu {
                        column: "dag_build_identity",
                        value: format!("run_id={}", self.run_id),
                    }
                );
                self.kind = stored_build_kind(state.dag_build_kind.as_deref()).map_err(|()| {
                    crate::Error::InvalidGraphSnapshotStoreValue {
                        column: "dag_build_kind",
                        value: state.dag_build_kind.unwrap_or_default(),
                    }
                })?;
                return Ok(());
            }

            match state.dag_init_phase.as_str() {
                "new"
                | "full_reset"
                | "manifest_refresh_reset"
                | "manifest_branch_reset"
                | "manifest_copy"
                | "manifest_refresh_copy"
                | "manifest_reset"
                | "manifest_seed" => {
                    self.initialize_source_manifest(&state).await?;
                    tokio::task::yield_now().await;
                }
                "scope" => {
                    if self.process_scope_batch().await? > 0 {
                        tokio::task::yield_now().await;
                    }
                }
                phase if phase.starts_with("kind") => {
                    self.choose_build_kind().await?;
                    tokio::task::yield_now().await;
                }
                "delta_remove" => {
                    self.populate_delta_removal_batch().await?;
                    tokio::task::yield_now().await;
                }
                "delta_scope_remove" => {
                    self.populate_delta_scope_removal_batch().await?;
                    tokio::task::yield_now().await;
                }
                "delta_seed" => {
                    self.populate_delta_node_batch().await?;
                    tokio::task::yield_now().await;
                }
                "rank_slots" | "nodes" | "edges" | "ports" => {
                    self.advance_layout_initialization(&state.dag_init_phase)
                        .await?;
                    tokio::task::yield_now().await;
                }
                "branches" => {
                    self.initialize_branch_metadata(&state.dag_init_text_cursor)
                        .await?;
                    tokio::task::yield_now().await;
                }
                value => {
                    return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                        column: "dag_init_phase",
                        value: value.to_owned(),
                    }
                    .fail();
                }
            }
        }
    }

    async fn dag_run_state(&self) -> crate::Result<DagRunRow> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT dag_initialized, dag_baseline_generation, dag_root_id, \
                            dag_build_kind, dag_init_phase, dag_init_row_cursor, \
                            dag_init_text_cursor, dag_init_counter, dag_source_revision \
                     FROM console_graph_build_runs \
                     WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                       AND status = 'building'",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(owner_id)
                .bind::<BigInt, _>(lease_epoch)
                .get_result::<DagRunRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn initialize_source_manifest(&self, state: &DagRunRow) -> crate::Result<()> {
        const MANIFEST_BATCH_SIZE: i64 = 128;
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let baseline_generation = self.baseline_generation;
        let root_id = self.root_id.clone();
        let phase = state.dag_init_phase.clone();
        let cursor = state.dag_init_text_cursor.clone();
        let counter = state.dag_init_counter;
        let source_revision = state.dag_source_revision;
        let row_cursor = state.dag_init_row_cursor;
        let append = state.dag_build_kind.as_deref() == Some("append");
        self.database
            .with_write_connection("snapshot DAG build source manifest", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        match phase.as_str() {
                            "new" => {
                                let updated = diesel::sql_query(
                                    "UPDATE console_graph_build_runs \
                                     SET dag_baseline_generation = ?, dag_root_id = ?, \
                                         dag_init_phase = 'kind', \
                                         dag_init_text_cursor = '', \
                                         dag_init_text_cursor_secondary = '', \
                                         dag_init_counter = 0, dag_scope_count = 0 \
                                     WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                                       AND status = 'building' AND dag_init_phase = 'new' \
                                       AND dag_source_revision = ?",
                                )
                                .bind::<BigInt, _>(baseline_generation)
                                .bind::<Text, _>(root_id)
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&owner_id)
                                .bind::<BigInt, _>(lease_epoch)
                                .bind::<BigInt, _>(source_revision)
                                .execute(connection)?;
                                if updated != 1 {
                                    return Err(diesel::result::Error::NotFound);
                                }
                            }
                            "full_reset" => {
                                for table in [
                                    "console_graph_build_branch_deltas",
                                    "console_graph_build_anchor_node_tombstones",
                                    "console_graph_build_scope_tombstones",
                                    "console_graph_build_changed_branches",
                                    "console_graph_build_node_tombstones",
                                    "console_graph_build_delta_nodes",
                                ] {
                                    let deleted = diesel::sql_query(format!(
                                        "DELETE FROM {table} WHERE rowid IN ( \
                                             SELECT rowid FROM {table} \
                                             WHERE run_id = ? LIMIT ? \
                                         )"
                                    ))
                                    .bind::<BigInt, _>(run_id)
                                    .bind::<BigInt, _>(MANIFEST_BATCH_SIZE)
                                    .execute(connection)?;
                                    if deleted > 0 {
                                        return Ok(());
                                    }
                                }
                                advance_dag_init_phase(
                                    connection,
                                    run_id,
                                    &owner_id,
                                    lease_epoch,
                                    "full_reset",
                                    "manifest_refresh_reset",
                                )?;
                            }
                            "manifest_refresh_reset" => {
                                let deleted = diesel::sql_query(
                                    "DELETE FROM console_graph_build_source_refresh_manifest \
                                     WHERE rowid IN ( \
                                         SELECT rowid \
                                         FROM console_graph_build_source_refresh_manifest \
                                         WHERE run_id = ? ORDER BY rowid LIMIT ? \
                                     )",
                                )
                                .bind::<BigInt, _>(run_id)
                                .bind::<BigInt, _>(MANIFEST_BATCH_SIZE)
                                .execute(connection)?;
                                if deleted == 0 {
                                    advance_dag_init_phase(
                                        connection,
                                        run_id,
                                        &owner_id,
                                        lease_epoch,
                                        "manifest_refresh_reset",
                                        "manifest_branch_reset",
                                    )?;
                                }
                            }
                            "manifest_branch_reset" => {
                                let deleted = diesel::sql_query(
                                    "DELETE FROM console_graph_build_source_manifest \
                                     WHERE rowid IN ( \
                                         SELECT rowid FROM console_graph_build_source_manifest \
                                         WHERE run_id = ? ORDER BY rowid LIMIT ? \
                                     )",
                                )
                                .bind::<BigInt, _>(run_id)
                                .bind::<BigInt, _>(MANIFEST_BATCH_SIZE)
                                .execute(connection)?;
                                if deleted == 0 {
                                    advance_dag_init_phase(
                                        connection,
                                        run_id,
                                        &owner_id,
                                        lease_epoch,
                                        "manifest_branch_reset",
                                        "manifest_copy",
                                    )?;
                                }
                            }
                            "manifest_copy" => {
                                let names = if counter == 0 {
                                    diesel::sql_query(
                                        "SELECT branch_name, first_source_revision \
                                         FROM console_graph_source_branch_names \
                                         WHERE first_source_revision <= ? \
                                         ORDER BY first_source_revision, branch_name LIMIT ?",
                                    )
                                    .bind::<BigInt, _>(source_revision)
                                    .bind::<BigInt, _>(MANIFEST_BATCH_SIZE)
                                    .load::<SourceBranchNameRow>(connection)?
                                } else {
                                    diesel::sql_query(
                                        "SELECT branch_name, first_source_revision \
                                         FROM console_graph_source_branch_names \
                                         WHERE first_source_revision <= ? \
                                           AND (first_source_revision, branch_name) > (?, ?) \
                                         ORDER BY first_source_revision, branch_name LIMIT ?",
                                    )
                                    .bind::<BigInt, _>(source_revision)
                                    .bind::<BigInt, _>(row_cursor)
                                    .bind::<Text, _>(&cursor)
                                    .bind::<BigInt, _>(MANIFEST_BATCH_SIZE)
                                    .load::<SourceBranchNameRow>(connection)?
                                };
                                if names.is_empty() {
                                    advance_dag_init_phase(
                                        connection,
                                        run_id,
                                        &owner_id,
                                        lease_epoch,
                                        "manifest_copy",
                                        "manifest_refresh_copy",
                                    )?;
                                } else {
                                    let last =
                                        names.last().expect("non-empty manifest branch name batch");
                                    let next_cursor = last.branch_name.clone();
                                    let next_revision_cursor = last.first_source_revision;
                                    let batch_len = i64::try_from(names.len()).unwrap_or(i64::MAX);
                                    for name in names {
                                        let branch = diesel::sql_query(
                                            "SELECT branch_name AS name, \
                                                    contribution_generation, head_id, \
                                                    state_json, removed \
                                             FROM console_graph_source_branch_history \
                                             WHERE branch_name = ? AND source_revision <= ? \
                                             ORDER BY source_revision DESC LIMIT 1",
                                        )
                                        .bind::<Text, _>(&name.branch_name)
                                        .bind::<BigInt, _>(source_revision)
                                        .get_result::<FrozenManifestBranchRow>(connection)?;
                                        if branch.removed != 0 {
                                            if branch.contribution_generation.is_some()
                                                || branch.head_id.is_some()
                                                || branch.state_json.is_some()
                                            {
                                                return Err(
                                                    diesel::result::Error::RollbackTransaction,
                                                );
                                            }
                                            continue;
                                        }
                                        let Some(contribution_generation) =
                                            branch.contribution_generation
                                        else {
                                            return Err(diesel::result::Error::RollbackTransaction);
                                        };
                                        let Some(head_id) = branch.head_id else {
                                            return Err(diesel::result::Error::RollbackTransaction);
                                        };
                                        let Some(state_json) = branch.state_json else {
                                            return Err(diesel::result::Error::RollbackTransaction);
                                        };
                                        diesel::sql_query(
                                            "INSERT OR IGNORE INTO \
                                                 console_graph_build_source_manifest ( \
                                                 run_id, branch_name, contribution_generation, \
                                                 head_id, state_json \
                                             ) VALUES (?, ?, ?, ?, ?)",
                                        )
                                        .bind::<BigInt, _>(run_id)
                                        .bind::<Text, _>(&branch.name)
                                        .bind::<BigInt, _>(contribution_generation)
                                        .bind::<Text, _>(head_id)
                                        .bind::<Text, _>(state_json)
                                        .execute(connection)?;
                                    }
                                    let updated = diesel::sql_query(
                                        "UPDATE console_graph_build_runs \
                                         SET dag_init_text_cursor = ?, dag_init_row_cursor = ?, \
                                             dag_init_counter = dag_init_counter + ? \
                                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                                           AND status = 'building' \
                                           AND dag_init_phase = 'manifest_copy' \
                                           AND dag_init_text_cursor = ? \
                                           AND dag_init_row_cursor = ? \
                                           AND dag_init_counter = ?",
                                    )
                                    .bind::<Text, _>(next_cursor)
                                    .bind::<BigInt, _>(next_revision_cursor)
                                    .bind::<BigInt, _>(batch_len)
                                    .bind::<BigInt, _>(run_id)
                                    .bind::<Text, _>(&owner_id)
                                    .bind::<BigInt, _>(lease_epoch)
                                    .bind::<Text, _>(&cursor)
                                    .bind::<BigInt, _>(row_cursor)
                                    .bind::<BigInt, _>(counter)
                                    .execute(connection)?;
                                    if updated != 1 {
                                        return Err(diesel::result::Error::NotFound);
                                    }
                                }
                            }
                            "manifest_refresh_copy" => {
                                let refreshes = if counter == 0 {
                                    diesel::sql_query(
                                        "SELECT branch_name, refresh_id, \
                                                target_contribution_generation, \
                                                published_source_revision \
                                         FROM console_graph_source_refresh_runs \
                                         WHERE status = 'published' \
                                           AND published_source_revision <= ? \
                                         ORDER BY published_source_revision, refresh_id LIMIT ?",
                                    )
                                    .bind::<BigInt, _>(source_revision)
                                    .bind::<BigInt, _>(MANIFEST_BATCH_SIZE)
                                    .load::<ManifestRefreshRow>(connection)?
                                } else {
                                    let refresh_id_cursor = cursor
                                        .parse::<i64>()
                                        .map_err(|_| diesel::result::Error::RollbackTransaction)?;
                                    diesel::sql_query(
                                        "SELECT branch_name, refresh_id, \
                                                target_contribution_generation, \
                                                published_source_revision \
                                         FROM console_graph_source_refresh_runs \
                                         WHERE status = 'published' \
                                           AND published_source_revision <= ? \
                                           AND (published_source_revision, refresh_id) > (?, ?) \
                                         ORDER BY published_source_revision, refresh_id LIMIT ?",
                                    )
                                    .bind::<BigInt, _>(source_revision)
                                    .bind::<BigInt, _>(row_cursor)
                                    .bind::<BigInt, _>(refresh_id_cursor)
                                    .bind::<BigInt, _>(MANIFEST_BATCH_SIZE)
                                    .load::<ManifestRefreshRow>(connection)?
                                };
                                if refreshes.is_empty() {
                                    advance_dag_init_phase(
                                        connection,
                                        run_id,
                                        &owner_id,
                                        lease_epoch,
                                        "manifest_refresh_copy",
                                        "manifest_seed",
                                    )?;
                                } else {
                                    for refresh in &refreshes {
                                        let included = diesel::sql_query(
                                            "SELECT 1 AS count \
                                             FROM console_graph_build_source_manifest \
                                             WHERE run_id = ? AND branch_name = ? \
                                               AND contribution_generation = ? LIMIT 1",
                                        )
                                        .bind::<BigInt, _>(run_id)
                                        .bind::<Text, _>(&refresh.branch_name)
                                        .bind::<BigInt, _>(refresh.target_contribution_generation)
                                        .get_result::<CountRow>(connection)
                                        .optional()?
                                        .is_some();
                                        if included {
                                            diesel::sql_query(
                                                "INSERT OR IGNORE INTO \
                                                     console_graph_build_source_refresh_manifest ( \
                                                     run_id, branch_name, refresh_id \
                                                 ) VALUES (?, ?, ?)",
                                            )
                                            .bind::<BigInt, _>(run_id)
                                            .bind::<Text, _>(&refresh.branch_name)
                                            .bind::<BigInt, _>(refresh.refresh_id)
                                            .execute(connection)?;
                                        }
                                    }
                                    let last =
                                        refreshes.last().expect("non-empty manifest refresh batch");
                                    let updated = diesel::sql_query(
                                        "UPDATE console_graph_build_runs \
                                         SET dag_init_text_cursor = ?, dag_init_row_cursor = ?, \
                                             dag_init_counter = dag_init_counter + ? \
                                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                                           AND status = 'building' \
                                           AND dag_init_phase = 'manifest_refresh_copy' \
                                           AND dag_init_text_cursor = ? \
                                           AND dag_init_row_cursor = ? \
                                           AND dag_init_counter = ?",
                                    )
                                    .bind::<Text, _>(last.refresh_id.to_string())
                                    .bind::<BigInt, _>(last.published_source_revision)
                                    .bind::<BigInt, _>(
                                        i64::try_from(refreshes.len()).unwrap_or(i64::MAX),
                                    )
                                    .bind::<BigInt, _>(run_id)
                                    .bind::<Text, _>(&owner_id)
                                    .bind::<BigInt, _>(lease_epoch)
                                    .bind::<Text, _>(&cursor)
                                    .bind::<BigInt, _>(row_cursor)
                                    .bind::<BigInt, _>(counter)
                                    .execute(connection)?;
                                    if updated != 1 {
                                        return Err(diesel::result::Error::NotFound);
                                    }
                                }
                            }
                            "manifest_reset" => {
                                advance_dag_init_phase(
                                    connection,
                                    run_id,
                                    &owner_id,
                                    lease_epoch,
                                    "manifest_reset",
                                    "manifest_refresh_reset",
                                )?;
                            }
                            "manifest_seed" => {
                                let (branches, inspected_cursor, inspected_count) = if append {
                                    let names = if counter == 0 {
                                        diesel::sql_query(
                                            "SELECT branch_name AS value \
                                             FROM console_graph_build_changed_branches \
                                             WHERE run_id = ? \
                                             ORDER BY branch_name LIMIT ?",
                                        )
                                        .bind::<BigInt, _>(run_id)
                                        .bind::<BigInt, _>(MANIFEST_BATCH_SIZE)
                                        .load::<TextValueRow>(connection)?
                                    } else {
                                        diesel::sql_query(
                                            "SELECT branch_name AS value \
                                             FROM console_graph_build_changed_branches \
                                             WHERE run_id = ? AND branch_name > ? \
                                             ORDER BY branch_name LIMIT ?",
                                        )
                                        .bind::<BigInt, _>(run_id)
                                        .bind::<Text, _>(&cursor)
                                        .bind::<BigInt, _>(MANIFEST_BATCH_SIZE)
                                        .load::<TextValueRow>(connection)?
                                    };
                                    let inspected_cursor =
                                        names.last().map(|row| row.value.clone());
                                    let inspected_count = names.len();
                                    let mut branches = Vec::new();
                                    for name in names {
                                        if let Some(branch) = diesel::sql_query(
                                            "SELECT branch_name AS name, head_id, \
                                                    contribution_generation \
                                             FROM console_graph_build_source_manifest \
                                             WHERE run_id = ? AND branch_name = ?",
                                        )
                                        .bind::<BigInt, _>(run_id)
                                        .bind::<Text, _>(&name.value)
                                        .get_result::<ManifestBranchRow>(connection)
                                        .optional()?
                                        {
                                            branches.push(branch);
                                        }
                                    }
                                    (branches, inspected_cursor, inspected_count)
                                } else {
                                    let branches = if counter == 0 {
                                        diesel::sql_query(
                                            "SELECT branch_name AS name, head_id, \
                                                    contribution_generation \
                                             FROM console_graph_build_source_manifest \
                                             WHERE run_id = ? \
                                             ORDER BY branch_name LIMIT ?",
                                        )
                                        .bind::<BigInt, _>(run_id)
                                        .bind::<BigInt, _>(MANIFEST_BATCH_SIZE)
                                        .load::<ManifestBranchRow>(connection)?
                                    } else {
                                        diesel::sql_query(
                                            "SELECT branch_name AS name, head_id, \
                                                    contribution_generation \
                                             FROM console_graph_build_source_manifest \
                                             WHERE run_id = ? AND branch_name > ? \
                                             ORDER BY branch_name LIMIT ?",
                                        )
                                        .bind::<BigInt, _>(run_id)
                                        .bind::<Text, _>(&cursor)
                                        .bind::<BigInt, _>(MANIFEST_BATCH_SIZE)
                                        .load::<ManifestBranchRow>(connection)?
                                    };
                                    let inspected_cursor =
                                        branches.last().map(|branch| branch.name.clone());
                                    let inspected_count = branches.len();
                                    (branches, inspected_cursor, inspected_count)
                                };
                                let Some(next_cursor) = inspected_cursor else {
                                    advance_dag_init_phase(
                                        connection,
                                        run_id,
                                        &owner_id,
                                        lease_epoch,
                                        "manifest_seed",
                                        "scope",
                                    )?;
                                    return Ok(());
                                };
                                let batch_len = i64::try_from(inspected_count).unwrap_or(i64::MAX);
                                for branch in branches {
                                    diesel::sql_query(
                                        "INSERT OR IGNORE INTO \
                                                 console_graph_build_scope_queue ( \
                                                 run_id, branch_name, node_id, \
                                                 traversal_kind, processed \
                                             ) VALUES (?, ?, ?, 'graph', 0)",
                                    )
                                    .bind::<BigInt, _>(run_id)
                                    .bind::<Text, _>(branch.name)
                                    .bind::<Text, _>(branch.head_id)
                                    .execute(connection)?;
                                }
                                let updated = diesel::sql_query(
                                    "UPDATE console_graph_build_runs \
                                         SET dag_init_text_cursor = ?, \
                                             dag_init_counter = dag_init_counter + ? \
                                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                                           AND status = 'building' \
                                           AND dag_init_phase = 'manifest_seed' \
                                           AND dag_init_text_cursor = ? \
                                           AND dag_init_counter = ?",
                                )
                                .bind::<Text, _>(next_cursor)
                                .bind::<BigInt, _>(batch_len)
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&owner_id)
                                .bind::<BigInt, _>(lease_epoch)
                                .bind::<Text, _>(&cursor)
                                .bind::<BigInt, _>(counter)
                                .execute(connection)?;
                                if updated != 1 {
                                    return Err(diesel::result::Error::NotFound);
                                }
                            }
                            _ => return Err(diesel::result::Error::NotFound),
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn populate_delta_removal_batch(&self) -> crate::Result<()> {
        const REMOVAL_BATCH_SIZE: i64 = 128;
        let path = self.path.clone();
        let run_id = self.run_id;
        let baseline_generation = self.baseline_generation;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        self.database
            .with_write_connection("capture graph delta removal batch", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let removal = diesel::sql_query(
                            "SELECT branch_name, removal_cursor, removal_refresh_id_cursor, \
                                    removal_refresh_id_upper_bound, removal_bound_frozen, \
                                    removal_complete \
                             FROM console_graph_build_changed_branches \
                             WHERE run_id = ? AND removal_complete = 0 \
                             ORDER BY branch_name LIMIT 1",
                        )
                        .bind::<BigInt, _>(run_id)
                        .get_result::<BranchRemovalSeedRow>(connection)
                        .optional()?;
                        let Some(removal) = removal else {
                            return advance_dag_init_phase(
                                connection,
                                run_id,
                                &owner_id,
                                lease_epoch,
                                "delta_remove",
                                "delta_seed",
                            );
                        };

                        if removal.removal_complete == 0 {
                            if removal.removal_bound_frozen == 0 {
                                let upper_bound = diesel::sql_query(DELTA_REMOVAL_UPPER_BOUND_SQL)
                                    .bind::<BigInt, _>(run_id)
                                    .bind::<Text, _>(&removal.branch_name)
                                    .get_result::<BigIntRow>(connection)?
                                    .value;
                                let updated = diesel::sql_query(
                                    "UPDATE console_graph_build_changed_branches \
                                     SET removal_refresh_id_upper_bound = ?, \
                                         removal_bound_frozen = 1 \
                                     WHERE run_id = ? AND branch_name = ? \
                                       AND removal_complete = 0 \
                                       AND removal_bound_frozen = 0 \
                                       AND removal_cursor = ? \
                                       AND removal_refresh_id_cursor = ? \
                                       AND removal_refresh_id_upper_bound = ?",
                                )
                                .bind::<BigInt, _>(upper_bound)
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&removal.branch_name)
                                .bind::<Text, _>(&removal.removal_cursor)
                                .bind::<BigInt, _>(removal.removal_refresh_id_cursor)
                                .bind::<BigInt, _>(removal.removal_refresh_id_upper_bound)
                                .execute(connection)?;
                                if updated != 1 {
                                    return Err(diesel::result::Error::NotFound);
                                }
                                return Ok(());
                            }
                            let candidates = diesel::sql_query(DELTA_REMOVAL_CANDIDATE_SQL)
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&removal.branch_name)
                                .bind::<BigInt, _>(removal.removal_refresh_id_cursor)
                                .bind::<Text, _>(&removal.removal_cursor)
                                .bind::<BigInt, _>(removal.removal_refresh_id_upper_bound)
                                .bind::<BigInt, _>(REMOVAL_BATCH_SIZE)
                                .load::<BranchRemovalCandidateRow>(connection)?;
                            if candidates.is_empty() {
                                let updated = diesel::sql_query(
                                    "UPDATE console_graph_build_changed_branches \
                                     SET removal_complete = 1 \
                                     WHERE run_id = ? AND branch_name = ? \
                                       AND removal_complete = 0 AND removal_cursor = ? \
                                       AND removal_refresh_id_cursor = ? \
                                       AND removal_bound_frozen = 1 \
                                       AND removal_refresh_id_upper_bound = ?",
                                )
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&removal.branch_name)
                                .bind::<Text, _>(&removal.removal_cursor)
                                .bind::<BigInt, _>(removal.removal_refresh_id_cursor)
                                .bind::<BigInt, _>(removal.removal_refresh_id_upper_bound)
                                .execute(connection)?;
                                if updated != 1 {
                                    return Err(diesel::result::Error::NotFound);
                                }
                                return Ok(());
                            }
                            for candidate in candidates
                                .iter()
                                .filter(|candidate| candidate.eligible == 1)
                            {
                                diesel::sql_query(
                                    "INSERT OR IGNORE INTO \
                                         console_graph_build_node_tombstones (run_id, node_id) \
                                     SELECT ?, ? \
                                     WHERE EXISTS ( \
                                         SELECT 1 FROM console_graph_node_locations \
                                         WHERE generation = ? AND mode = 'all' \
                                           AND node_id = ? \
                                     ) AND NOT EXISTS ( \
                                         SELECT 1 \
                                         FROM console_graph_build_effective_source_branch_nodes \
                                         WHERE run_id = ? AND node_id = ? \
                                     )",
                                )
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&candidate.node_id)
                                .bind::<BigInt, _>(baseline_generation)
                                .bind::<Text, _>(&candidate.node_id)
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&candidate.node_id)
                                .execute(connection)?;
                            }
                            let next = candidates.last().expect("non-empty removal candidate page");
                            let updated = diesel::sql_query(
                                "UPDATE console_graph_build_changed_branches \
                                 SET removal_cursor = ?, removal_refresh_id_cursor = ? \
                                 WHERE run_id = ? AND branch_name = ? \
                                   AND removal_complete = 0 AND removal_cursor = ? \
                                   AND removal_refresh_id_cursor = ? \
                                   AND removal_bound_frozen = 1 \
                                   AND removal_refresh_id_upper_bound = ?",
                            )
                            .bind::<Text, _>(&next.node_id)
                            .bind::<BigInt, _>(next.refresh_id)
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&removal.branch_name)
                            .bind::<Text, _>(&removal.removal_cursor)
                            .bind::<BigInt, _>(removal.removal_refresh_id_cursor)
                            .bind::<BigInt, _>(removal.removal_refresh_id_upper_bound)
                            .execute(connection)?;
                            if updated != 1 {
                                return Err(diesel::result::Error::NotFound);
                            }
                            return Ok(());
                        }

                        Err(diesel::result::Error::NotFound)
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn populate_delta_scope_removal_batch(&self) -> crate::Result<()> {
        const REMOVAL_BATCH_SIZE: i64 = 128;
        let path = self.path.clone();
        let run_id = self.run_id;
        let baseline_generation = self.baseline_generation;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        self.database
            .with_write_connection(
                "capture graph delta scope removal batch",
                move |connection| {
                    connection
                        .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                            require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                            let removal = diesel::sql_query(
                                "SELECT branch_name, scope_removal_cursor, scope_reuses_baseline \
                             FROM console_graph_build_changed_branches \
                             WHERE run_id = ? AND scope_removal_complete = 0 \
                             ORDER BY branch_name LIMIT 1",
                            )
                            .bind::<BigInt, _>(run_id)
                            .get_result::<BranchScopeRemovalSeedRow>(connection)
                            .optional()?;
                            let Some(removal) = removal else {
                                diesel::sql_query(
                                    "INSERT INTO console_graph_anchor_scope_manifests \
                                     (generation, scope_count) \
                                 SELECT run.run_id, \
                                        MAX(0, run.dag_scope_count - run.dag_init_counter) \
                                 FROM console_graph_build_runs AS run \
                                 WHERE run.run_id = ? AND run.owner_id = ? \
                                   AND run.lease_epoch = ? AND run.status = 'building' \
                                   AND run.dag_init_phase = 'delta_scope_remove' \
                                 ON CONFLICT(generation) DO UPDATE SET \
                                     scope_count = excluded.scope_count",
                                )
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&owner_id)
                                .bind::<BigInt, _>(lease_epoch)
                                .execute(connection)?;
                                return advance_dag_init_phase(
                                    connection,
                                    run_id,
                                    &owner_id,
                                    lease_epoch,
                                    "delta_scope_remove",
                                    "rank_slots",
                                );
                            };

                            if removal.scope_reuses_baseline == 1 {
                                let updated = diesel::sql_query(
                                    "UPDATE console_graph_build_changed_branches \
                                 SET scope_removal_complete = 1 \
                                 WHERE run_id = ? AND branch_name = ? \
                                   AND scope_removal_complete = 0 \
                                   AND scope_reuses_baseline = 1",
                                )
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&removal.branch_name)
                                .execute(connection)?;
                                if updated != 1 {
                                    return Err(diesel::result::Error::NotFound);
                                }
                                return Ok(());
                            }

                            let scopes = diesel::sql_query(
                                "SELECT node_id AS value FROM console_graph_anchor_scopes \
                             WHERE generation = ? AND branch_name = ? AND node_id > ? \
                             ORDER BY node_id LIMIT ?",
                            )
                            .bind::<BigInt, _>(baseline_generation)
                            .bind::<Text, _>(&removal.branch_name)
                            .bind::<Text, _>(&removal.scope_removal_cursor)
                            .bind::<BigInt, _>(REMOVAL_BATCH_SIZE)
                            .load::<TextValueRow>(connection)?;
                            if scopes.is_empty() {
                                let updated = diesel::sql_query(
                                    "UPDATE console_graph_build_changed_branches \
                                 SET scope_removal_complete = 1 \
                                 WHERE run_id = ? AND branch_name = ? \
                                   AND scope_removal_complete = 0 \
                                   AND scope_removal_cursor = ?",
                                )
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&removal.branch_name)
                                .bind::<Text, _>(&removal.scope_removal_cursor)
                                .execute(connection)?;
                                if updated != 1 {
                                    return Err(diesel::result::Error::NotFound);
                                }
                                return Ok(());
                            }
                            let mut inserted_scope_tombstones = 0_i64;
                            for scope in &scopes {
                                inserted_scope_tombstones += diesel::sql_query(
                                    "INSERT OR IGNORE INTO console_graph_build_scope_tombstones \
                                     (run_id, branch_name, node_id) VALUES (?, ?, ?)",
                                )
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&removal.branch_name)
                                .bind::<Text, _>(&scope.value)
                                .execute(connection)?
                                    as i64;
                                diesel::sql_query(
                                    "INSERT OR IGNORE INTO \
                                     console_graph_build_anchor_node_tombstones \
                                     (run_id, node_id) \
                                 SELECT ?, ? \
                                 WHERE NOT EXISTS ( \
                                     SELECT 1 FROM console_graph_anchor_scopes \
                                     WHERE generation = ? AND node_id = ? \
                                 ) AND NOT EXISTS ( \
                                     SELECT 1 \
                                     FROM console_graph_anchor_scopes AS baseline \
                                     WHERE baseline.generation = ? \
                                       AND baseline.node_id = ? \
                                       AND NOT EXISTS ( \
                                           SELECT 1 \
                                           FROM console_graph_build_scope_tombstones AS removed \
                                           WHERE removed.run_id = ? \
                                             AND removed.branch_name = \
                                                 baseline.branch_name \
                                             AND removed.node_id = baseline.node_id \
                                       ) \
                                 )",
                                )
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&scope.value)
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&scope.value)
                                .bind::<BigInt, _>(baseline_generation)
                                .bind::<Text, _>(&scope.value)
                                .bind::<BigInt, _>(run_id)
                                .execute(connection)?;
                            }
                            let counted = diesel::sql_query(
                                "UPDATE console_graph_build_runs \
                                 SET dag_init_counter = dag_init_counter + ? \
                                 WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                                   AND status = 'building' \
                                   AND dag_init_phase = 'delta_scope_remove'",
                            )
                            .bind::<BigInt, _>(inserted_scope_tombstones)
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&owner_id)
                            .bind::<BigInt, _>(lease_epoch)
                            .execute(connection)?;
                            if counted != 1 {
                                return Err(diesel::result::Error::NotFound);
                            }
                            let next_cursor =
                                &scopes.last().expect("non-empty removal scope page").value;
                            let updated = diesel::sql_query(
                                "UPDATE console_graph_build_changed_branches \
                             SET scope_removal_cursor = ? \
                             WHERE run_id = ? AND branch_name = ? \
                               AND scope_removal_complete = 0 \
                               AND scope_removal_cursor = ?",
                            )
                            .bind::<Text, _>(next_cursor)
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&removal.branch_name)
                            .bind::<Text, _>(&removal.scope_removal_cursor)
                            .execute(connection)?;
                            if updated != 1 {
                                return Err(diesel::result::Error::NotFound);
                            }
                            Ok(())
                        })
                        .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
                },
            )
            .await
    }

    async fn populate_delta_node_batch(&self) -> crate::Result<()> {
        const DELTA_BATCH_SIZE: i64 = 128;
        let path = self.path.clone();
        let run_id = self.run_id;
        let baseline_generation = self.baseline_generation;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        self.database
            .with_write_connection("capture graph delta node batch", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let delta = diesel::sql_query(
                            "SELECT branch_name, published_source_revision, refresh_id, \
                                    seed_cursor \
                             FROM console_graph_build_branch_deltas \
                             WHERE run_id = ? AND seed_complete = 0 \
                             ORDER BY branch_name, published_source_revision, refresh_id \
                             LIMIT 1",
                        )
                        .bind::<BigInt, _>(run_id)
                        .get_result::<BranchDeltaSeedRow>(connection)
                        .optional()?;
                        let Some(delta) = delta else {
                            return advance_dag_init_phase(
                                connection,
                                run_id,
                                &owner_id,
                                lease_epoch,
                                "delta_seed",
                                "manifest_seed",
                            );
                        };
                        let node_ids = diesel::sql_query(
                            "SELECT node_id AS value \
                             FROM console_graph_source_branch_nodes \
                             WHERE branch_name = ? AND contribution_generation = ? \
                               AND node_id > ? ORDER BY node_id LIMIT ?",
                        )
                        .bind::<Text, _>(&delta.branch_name)
                        .bind::<BigInt, _>(delta.refresh_id)
                        .bind::<Text, _>(&delta.seed_cursor)
                        .bind::<BigInt, _>(DELTA_BATCH_SIZE)
                        .load::<TextValueRow>(connection)?;
                        if node_ids.is_empty() {
                            let updated = diesel::sql_query(
                                "UPDATE console_graph_build_branch_deltas \
                                 SET seed_complete = 1 \
                                 WHERE run_id = ? AND branch_name = ? \
                                   AND published_source_revision = ? AND refresh_id = ? \
                                   AND seed_complete = 0 AND seed_cursor = ?",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&delta.branch_name)
                            .bind::<BigInt, _>(delta.published_source_revision)
                            .bind::<BigInt, _>(delta.refresh_id)
                            .bind::<Text, _>(&delta.seed_cursor)
                            .execute(connection)?;
                            if updated != 1 {
                                return Err(diesel::result::Error::NotFound);
                            }
                            return Ok(());
                        }
                        for node_id in &node_ids {
                            diesel::sql_query(
                                "INSERT OR IGNORE INTO console_graph_build_delta_nodes \
                                     (run_id, node_id) \
                                 SELECT ?, ? \
                                 WHERE NOT EXISTS ( \
                                     SELECT 1 FROM console_graph_node_locations \
                                     WHERE generation = ? AND mode = 'all' AND node_id = ? \
                                 )",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&node_id.value)
                            .bind::<BigInt, _>(baseline_generation)
                            .bind::<Text, _>(&node_id.value)
                            .execute(connection)?;
                        }
                        let next_cursor = &node_ids
                            .last()
                            .expect("non-empty graph delta node page")
                            .value;
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_build_branch_deltas SET seed_cursor = ? \
                             WHERE run_id = ? AND branch_name = ? \
                               AND published_source_revision = ? AND refresh_id = ? \
                               AND seed_complete = 0 AND seed_cursor = ?",
                        )
                        .bind::<Text, _>(next_cursor)
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&delta.branch_name)
                        .bind::<BigInt, _>(delta.published_source_revision)
                        .bind::<BigInt, _>(delta.refresh_id)
                        .bind::<Text, _>(&delta.seed_cursor)
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn process_scope_batch(&self) -> crate::Result<usize> {
        const SCOPE_BATCH_SIZE: i64 = 16;
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        self.database
            .with_write_connection("expand DAG anchor scope batch", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let batch = diesel::sql_query(
                            "SELECT branch_name, node_id, traversal_kind, \
                                    node_expanded, parent_cursor, child_cursor \
                             FROM console_graph_build_scope_queue \
                             WHERE run_id = ? AND processed = 0 \
                             ORDER BY branch_name, node_id, traversal_kind LIMIT ?",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<BigInt, _>(SCOPE_BATCH_SIZE)
                        .load::<ScopeQueueRow>(connection)?;
                        if batch.is_empty() {
                            advance_dag_init_phase(
                                connection,
                                run_id,
                                &owner_id,
                                lease_epoch,
                                "scope",
                                "delta_scope_remove",
                            )?;
                            return Ok(0);
                        }
                        for item in &batch {
                            expand_scope_item(connection, run_id, &owner_id, lease_epoch, item)?;
                        }
                        Ok(batch.len())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn choose_build_kind(&mut self) -> crate::Result<()> {
        const JOURNAL_BATCH_SIZE: i64 = 1;
        let path = self.path.clone();
        let run_id = self.run_id;
        let baseline_generation = self.baseline_generation;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let decided_kind = self
            .database
            .with_write_connection("classify incremental graph build", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let state = diesel::sql_query(
                            "SELECT dag_init_phase AS phase, \
                                    dag_init_text_cursor AS branch_cursor, \
                                    dag_init_row_cursor AS revision_cursor \
                             FROM console_graph_build_runs WHERE run_id = ?",
                        )
                        .bind::<BigInt, _>(run_id)
                        .get_result::<BuildKindStateRow>(connection)?;
                        let choose_full = |
                            connection: &mut SqliteConnection,
                            reason: BuildClassificationReason,
                        | {
                            checkpoint_build_kind(
                                connection,
                                run_id,
                                &owner_id,
                                lease_epoch,
                                &state.phase,
                                IncrementalBuildKind::Full,
                                reason,
                            )?;
                            Ok(Some(IncrementalBuildKind::Full))
                        };

                        match state.phase.as_str() {
                            "kind" => {
                                let baseline_publication = diesel::sql_query(
                                    "SELECT revision.source_revision, capability.publication_epoch \
                                     FROM console_graph_generation_state AS active \
                                     INNER JOIN console_graph_generation_delta_capabilities \
                                         AS capability \
                                         ON capability.generation = active.active_generation \
                                        AND capability.delta_compatible = 1 \
                                     INNER JOIN console_graph_generation_source_revisions AS revision \
                                         ON revision.generation = active.active_generation \
                                     WHERE active.id = 1 AND active.active_generation = ? \
                                       AND EXISTS ( \
                                           SELECT 1 FROM console_graph_materializations \
                                           WHERE generation = active.active_generation \
                                             AND mode = 'anchors' \
                                       ) \
                                       AND EXISTS ( \
                                           SELECT 1 FROM console_graph_materializations \
                                           WHERE generation = active.active_generation \
                                             AND mode = 'all' \
                                       )",
                                )
                                .bind::<BigInt, _>(baseline_generation)
                                .get_result::<BaselinePublicationRow>(connection)
                                .optional()?;
                                let Some(baseline_publication) = baseline_publication else {
                                    return choose_full(
                                        connection,
                                        BuildClassificationReason::BaselineDeltaCapabilityUnavailable,
                                    );
                                };
                                let updated = diesel::sql_query(
                                    "UPDATE console_graph_build_runs \
                                     SET dag_baseline_source_revision = ?, \
                                         dag_baseline_publication_epoch = ?, \
                                         dag_init_phase = 'kind_journal', \
                                         dag_init_row_cursor = 0, dag_init_text_cursor = '', \
                                         dag_init_text_cursor_secondary = '', dag_init_counter = 0 \
                                     WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                                       AND status = 'building' AND dag_init_phase = 'kind'",
                                )
                                .bind::<BigInt, _>(baseline_publication.source_revision)
                                .bind::<BigInt, _>(baseline_publication.publication_epoch)
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&owner_id)
                                .bind::<BigInt, _>(lease_epoch)
                                .execute(connection)?;
                                if updated != 1 {
                                    return Err(diesel::result::Error::NotFound);
                                }
                                Ok(None)
                            }
                            "kind_journal" => {
                                let revisions = diesel::sql_query(
                                    "SELECT dag_baseline_source_revision AS baseline_revision, \
                                            dag_source_revision AS build_revision \
                                     FROM console_graph_build_runs WHERE run_id = ?",
                                )
                                .bind::<BigInt, _>(run_id)
                                .get_result::<BuildRevisionRangeRow>(connection)?;
                                let changes = diesel::sql_query(
                                    "SELECT source_revision, branch_name, change_kind, refresh_id, \
                                            base_contribution_generation, \
                                            target_contribution_generation, head_id, state_json \
                                     FROM console_graph_source_branch_change_journal \
                                     WHERE source_revision > ? AND source_revision <= ? \
                                       AND (source_revision > ? OR ( \
                                           source_revision = ? AND branch_name > ? \
                                       )) \
                                     ORDER BY source_revision, branch_name LIMIT ?",
                                )
                                .bind::<BigInt, _>(revisions.baseline_revision)
                                .bind::<BigInt, _>(revisions.build_revision)
                                .bind::<BigInt, _>(state.revision_cursor)
                                .bind::<BigInt, _>(state.revision_cursor)
                                .bind::<Text, _>(&state.branch_cursor)
                                .bind::<BigInt, _>(JOURNAL_BATCH_SIZE)
                                .load::<BranchChangeJournalRow>(connection)?;
                                if changes.is_empty() {
                                    if !journal_cursor_covers_revision_range(
                                        revisions.baseline_revision,
                                        revisions.build_revision,
                                        state.revision_cursor,
                                    ) {
                                        return choose_full(
                                            connection,
                                            BuildClassificationReason::SourceJournalGap,
                                        );
                                    }
                                    let baseline_scope_count = diesel::sql_query(
                                        "SELECT scope_count AS value \
                                         FROM console_graph_anchor_scope_manifests \
                                         WHERE generation = ?",
                                    )
                                    .bind::<BigInt, _>(baseline_generation)
                                    .get_result::<BigIntRow>(connection)
                                    .optional()?;
                                    let baseline_scope_count = if let Some(scope_count) =
                                        baseline_scope_count
                                    {
                                        scope_count.value
                                    } else {
                                        let baseline_is_empty = diesel::sql_query(
                                            "SELECT CASE WHEN NOT EXISTS ( \
                                                 SELECT 1 \
                                                 FROM console_graph_node_locations \
                                                 WHERE generation = ? AND mode = 'anchors' \
                                                 LIMIT 1 \
                                             ) THEN 1 ELSE 0 END AS value",
                                        )
                                        .bind::<BigInt, _>(baseline_generation)
                                        .get_result::<IntegerRow>(connection)?
                                        .value
                                            == 1;
                                        if !baseline_is_empty {
                                            return choose_full(
                                                connection,
                                                BuildClassificationReason::BaselineAnchorScopeManifestUnavailable,
                                            );
                                        }
                                        0
                                    };
                                    let updated = diesel::sql_query(
                                        "UPDATE console_graph_build_runs SET dag_scope_count = ? \
                                         WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                                           AND status = 'building' \
                                           AND dag_init_phase = 'kind_journal'",
                                    )
                                    .bind::<BigInt, _>(baseline_scope_count)
                                    .bind::<BigInt, _>(run_id)
                                    .bind::<Text, _>(&owner_id)
                                    .bind::<BigInt, _>(lease_epoch)
                                    .execute(connection)?;
                                    if updated != 1 {
                                        return Err(diesel::result::Error::NotFound);
                                    }
                                    checkpoint_build_kind(
                                        connection,
                                        run_id,
                                        &owner_id,
                                        lease_epoch,
                                        &state.phase,
                                        IncrementalBuildKind::Append,
                                        BuildClassificationReason::SourceJournalDeltaCompatible,
                                    )?;
                                    return Ok(Some(IncrementalBuildKind::Append));
                                }

                                if !journal_page_is_contiguous(
                                    revisions.baseline_revision,
                                    state.revision_cursor,
                                    changes.iter().map(|change| change.source_revision),
                                ) {
                                    return choose_full(
                                        connection,
                                        BuildClassificationReason::SourceJournalGap,
                                    );
                                }

                                for change in &changes {
                                    match stage_branch_journal_change(
                                        connection,
                                        run_id,
                                        baseline_generation,
                                        change,
                                    )? {
                                        BranchChangeCompatibility::Compatible => {}
                                        BranchChangeCompatibility::PendingReset => return Ok(None),
                                        BranchChangeCompatibility::RequiresFull(reason) => {
                                            return choose_full(connection, reason);
                                        }
                                    }
                                }
                                let last = changes
                                    .last()
                                    .expect("non-empty source branch journal page");
                                let updated = diesel::sql_query(
                                    "UPDATE console_graph_build_runs \
                                     SET dag_init_row_cursor = ?, dag_init_text_cursor = ?, \
                                         dag_init_counter = dag_init_counter + ? \
                                     WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                                       AND status = 'building' \
                                       AND dag_init_phase = 'kind_journal' \
                                       AND dag_init_row_cursor = ? \
                                       AND dag_init_text_cursor = ?",
                                )
                                .bind::<BigInt, _>(last.source_revision)
                                .bind::<Text, _>(&last.branch_name)
                                .bind::<BigInt, _>(
                                    i64::try_from(changes.len()).unwrap_or(i64::MAX),
                                )
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&owner_id)
                                .bind::<BigInt, _>(lease_epoch)
                                .bind::<BigInt, _>(state.revision_cursor)
                                .bind::<Text, _>(&state.branch_cursor)
                                .execute(connection)?;
                                if updated != 1 {
                                    return Err(diesel::result::Error::NotFound);
                                }
                                Ok(None)
                            }
                            _ => Err(diesel::result::Error::NotFound),
                        }
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        if let Some(kind) = decided_kind {
            self.kind = kind;
        }
        Ok(())
    }
    async fn advance_layout_initialization(&self, phase: &str) -> crate::Result<()> {
        let next_phase = match phase {
            "rank_slots" => "nodes",
            "nodes" => "edges",
            "edges" => "ports",
            "ports" => "branches",
            _ => {
                return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                    column: "dag_init_phase",
                    value: phase.to_owned(),
                }
                .fail();
            }
        };
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let phase = phase.to_owned();
        self.database
            .with_write_connection("advance DAG layout initialization", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        advance_dag_init_phase(
                            connection,
                            run_id,
                            &owner_id,
                            lease_epoch,
                            &phase,
                            next_phase,
                        )
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }
    async fn initialize_branch_metadata(&self, cursor: &str) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let cursor = cursor.to_owned();
        let append = self.kind == IncrementalBuildKind::Append;
        self.database
            .with_write_connection("stage DAG branch metadata batch", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let (branches, inspected_cursor) = if append {
                            let names = diesel::sql_query(
                                "SELECT branch_name AS value \
                                 FROM console_graph_build_changed_branches \
                                 WHERE run_id = ? AND branch_name > ? \
                                 ORDER BY branch_name LIMIT 128",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&cursor)
                            .load::<TextValueRow>(connection)?;
                            let inspected_cursor = names.last().map(|row| row.value.clone());
                            let mut branches = Vec::new();
                            for name in names {
                                if let Some(branch) = diesel::sql_query(
                                    "SELECT branch_name AS name, head_id, state_json, \
                                            contribution_generation \
                                     FROM console_graph_build_source_manifest \
                                     WHERE run_id = ? AND branch_name = ?",
                                )
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&name.value)
                                .get_result::<BranchRow>(connection)
                                .optional()?
                                {
                                    branches.push(branch);
                                }
                            }
                            (branches, inspected_cursor)
                        } else {
                            let branches = diesel::sql_query(
                                "SELECT branch_name AS name, head_id, state_json, \
                                        contribution_generation \
                                 FROM console_graph_build_source_manifest \
                                 WHERE run_id = ? AND branch_name > ? \
                                 ORDER BY branch_name LIMIT 128",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&cursor)
                            .load::<BranchRow>(connection)?;
                            let inspected_cursor =
                                branches.last().map(|branch| branch.name.clone());
                            (branches, inspected_cursor)
                        };
                        let Some(next_cursor) = inspected_cursor else {
                            let updated = diesel::sql_query(
                                "UPDATE console_graph_build_runs \
                                 SET dag_initialized = 1, dag_init_phase = 'complete', \
                                     dag_init_text_cursor = '' \
                                 WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                                   AND status = 'building' AND dag_init_phase = 'branches' \
                                   AND dag_init_text_cursor = ?",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&owner_id)
                            .bind::<BigInt, _>(lease_epoch)
                            .bind::<Text, _>(&cursor)
                            .execute(connection)?;
                            if updated != 1 {
                                return Err(diesel::result::Error::NotFound);
                            }
                            return Ok(());
                        };
                        for branch in branches {
                            diesel::sql_query(
                                "INSERT INTO console_graph_materialization_branches \
                                     (generation, name, head_id, state_json, \
                                      contribution_generation) \
                                 VALUES (?, ?, ?, ?, ?) \
                                 ON CONFLICT(generation, name) DO UPDATE SET \
                                     head_id = excluded.head_id, \
                                     state_json = excluded.state_json, \
                                     contribution_generation = \
                                         excluded.contribution_generation",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&branch.name)
                            .bind::<Text, _>(&branch.head_id)
                            .bind::<Text, _>(&branch.state_json)
                            .bind::<BigInt, _>(branch.contribution_generation)
                            .execute(connection)?;
                        }
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_build_runs SET dag_init_text_cursor = ? \
                             WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                               AND status = 'building' AND dag_init_phase = 'branches' \
                               AND dag_init_text_cursor = ?",
                        )
                        .bind::<Text, _>(next_cursor)
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<Text, _>(&cursor)
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn has_active_nodes(&self) -> crate::Result<bool> {
        let path = self.path.clone();
        let run_id = self.run_id;
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT EXISTS ( \
                         SELECT 1 \
                         FROM console_graph_build_effective_source_branch_nodes \
                         WHERE run_id = ? \
                         LIMIT 1 \
                     ) AS value",
                )
                .bind::<BigInt, _>(run_id)
                .get_result::<IntegerRow>(connection)
                .map(|row| row.value == 1)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn dag_progress(&self) -> crate::Result<DagProgress> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let row = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT dag_discovered_node_count AS discovered_nodes, \
                            dag_processed_node_count AS processed_nodes \
                     FROM console_graph_build_runs WHERE run_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .get_result::<DagProgressRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        Ok(DagProgress {
            discovered_nodes: stored_node_count("dag_discovered_node_count", row.discovered_nodes)?,
            processed_nodes: stored_node_count("dag_processed_node_count", row.processed_nodes)?,
        })
    }

    async fn build_classification_reason(&self) -> crate::Result<BuildClassificationReason> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let row = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT dag_build_classification_reason AS value \
                     FROM console_graph_build_runs WHERE run_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .get_result::<TextValueRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        stored_build_classification_reason(&row.value).map_err(|()| {
            crate::Error::InvalidGraphSnapshotStoreValue {
                column: "dag_build_classification_reason",
                value: row.value,
            }
        })
    }

    async fn completed_shell_node_count(&self, mode: GraphMode) -> crate::Result<usize> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let mode = mode.as_query_value().to_owned();
        let row = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT node_count AS value \
                     FROM console_graph_build_shell_projections \
                     WHERE run_id = ? AND mode = ? AND phase = 'complete'",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(mode)
                .get_result::<BigIntRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        stored_node_count(
            "console_graph_build_shell_projections.node_count",
            row.value,
        )
    }

    async fn append_seed_page(&self, cursor: &str) -> crate::Result<InspectedFrontierPage> {
        debug_assert_eq!(self.kind, IncrementalBuildKind::Append);
        let path = self.path.clone();
        let cursor = cursor.to_owned();
        let root_id = self.root_id.clone();
        let run_id = self.run_id;
        let limit = self.child_page_size.min(i64::MAX as usize) as i64;
        let rows = self
            .database
            .with_connection(move |connection| {
                let result = (|| -> QueryResult<_> {
                    let candidates = diesel::sql_query(
                        "SELECT delta.node_id AS value, \
                            CASE WHEN delta.node_id <> ? AND NOT EXISTS ( \
                                SELECT 1 \
                                FROM console_graph_source_node_relations AS relation \
                                INNER JOIN console_graph_build_delta_nodes AS delta_parent \
                                    ON delta_parent.run_id = delta.run_id \
                                   AND delta_parent.node_id = relation.parent_id \
                                WHERE relation.child_id = delta.node_id \
                                  AND relation.parent_id <> ? \
                            ) THEN 1 ELSE 0 END AS eligible \
                     FROM console_graph_build_delta_nodes AS delta \
                     WHERE delta.run_id = ? AND delta.node_id > ? \
                     ORDER BY delta.node_id LIMIT ?",
                    )
                    .bind::<Text, _>(&root_id)
                    .bind::<Text, _>(&root_id)
                    .bind::<BigInt, _>(run_id)
                    .bind::<Text, _>(cursor)
                    .bind::<BigInt, _>(limit)
                    .load::<EligibleTextValueRow>(connection)?;
                    let mut rows = Vec::new();
                    for candidate in candidates
                        .iter()
                        .filter(|candidate| candidate.eligible == 1)
                    {
                        rows.push(
                            diesel::sql_query(
                                "SELECT node_id, node_json \
                                 FROM console_graph_source_nodes WHERE node_id = ?",
                            )
                            .bind::<Text, _>(&candidate.value)
                            .get_result::<SourceNodeRow>(connection)?,
                        );
                    }
                    Ok((candidates, rows))
                })();
                result.context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        let inspected_cursor = rows.0.last().map(|row| row.value.clone());
        let nodes = rows
            .1
            .into_iter()
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
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(InspectedFrontierPage {
            nodes,
            inspected_cursor,
        })
    }
    async fn insert_append_seeds(&self, seeds: &[FrontierNode]) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let seeds = seeds.to_vec();
        self.database
            .with_write_connection("insert DAG append seeds", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let mut inserted = 0usize;
                        for seed in seeds {
                            let affected = diesel::sql_query(
                                "INSERT OR IGNORE INTO console_graph_build_nodes \
                                 (run_id, node_id, created_at_ns, remaining_parents, processed) \
                                 VALUES (?, ?, ?, 0, 0)",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(seed.node_id)
                            .bind::<BigInt, _>(seed.created_at_ns)
                            .execute(connection)?;
                            inserted = inserted.saturating_add(affected);
                        }
                        record_discovered_nodes(
                            connection,
                            run_id,
                            &owner_id,
                            lease_epoch,
                            inserted,
                        )?;
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn seed_state(&self) -> crate::Result<SeedStateRow> {
        let path = self.path.clone();
        let run_id = self.run_id;
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT dag_seed_cursor, dag_seed_complete \
                     FROM console_graph_build_runs WHERE run_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .get_result::<SeedStateRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn mark_seed_progress(&self, cursor: &str) -> crate::Result<()> {
        self.write_seed_state(cursor, false).await
    }

    async fn mark_seed_complete(&self, cursor: &str) -> crate::Result<()> {
        self.write_seed_state(cursor, true).await
    }

    async fn write_seed_state(&self, cursor: &str, complete: bool) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let cursor = cursor.to_owned();
        self.database
            .with_write_connection("checkpoint DAG frontier seed", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        diesel::sql_query(
                            "UPDATE console_graph_build_runs \
                             SET dag_seed_cursor = ?, dag_seed_complete = ? \
                             WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                               AND status = 'building'",
                        )
                        .bind::<Text, _>(cursor)
                        .bind::<Integer, _>(i32::from(complete))
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .execute(connection)
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .map(|_| ())
    }

    async fn unqueued_ready_node_page(&self) -> crate::Result<Vec<FrontierNode>> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let limit = self.child_page_size.min(i64::MAX as usize) as i64;
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT build.created_at_ns, build.node_id \
                     FROM console_graph_build_nodes AS build \
                     WHERE build.run_id = ? AND build.processed = 0 \
                       AND build.parent_requirements_complete = 1 \
                       AND build.remaining_parents = 0 \
                       AND build.frontier_enqueued = 0 \
                       AND NOT EXISTS ( \
                           SELECT 1 \
                           FROM console_graph_build_parent_satisfactions AS satisfaction \
                           INNER JOIN console_graph_build_nodes AS parent \
                               ON parent.run_id = satisfaction.run_id \
                              AND parent.node_id = satisfaction.parent_id \
                           WHERE satisfaction.run_id = build.run_id \
                             AND satisfaction.child_id = build.node_id \
                             AND parent.processed = 0 \
                       ) \
                     ORDER BY build.created_at_ns, build.node_id LIMIT ?",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<BigInt, _>(limit)
                .load::<ReadyNodeRow>(connection)
                .map(|rows| {
                    rows.into_iter()
                        .map(|row| FrontierNode::new(row.created_at_ns, row.node_id))
                        .collect()
                })
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn parent_expansion(&self, parent_id: &str) -> crate::Result<ParentExpansionRow> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let parent_id = parent_id.to_owned();
        let query_parent_id = parent_id.clone();
        self.database
            .with_write_connection("open DAG parent expansion", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        diesel::sql_query(
                            "INSERT OR IGNORE INTO console_graph_build_parent_expansions \
                             (run_id, parent_id, next_child_id, complete) \
                             VALUES (?, ?, '', 0)",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&parent_id)
                        .execute(connection)?;
                        diesel::sql_query(
                            "SELECT next_child_id, complete \
                             FROM console_graph_build_parent_expansions \
                             WHERE run_id = ? AND parent_id = ?",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&query_parent_id)
                        .get_result::<ParentExpansionRow>(connection)
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn has_unprocessed_nodes(&self) -> crate::Result<bool> {
        let path = self.path.clone();
        let run_id = self.run_id;
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT EXISTS ( \
                         SELECT 1 FROM console_graph_build_nodes \
                         WHERE run_id = ? AND processed = 0 \
                         LIMIT 1 \
                     ) AS value",
                )
                .bind::<BigInt, _>(run_id)
                .get_result::<IntegerRow>(connection)
                .map(|row| row.value == 1)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
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
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let root = root.clone();
        self.database
            .with_write_connection("insert DAG root seed", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let inserted = diesel::sql_query(
                            "INSERT OR IGNORE INTO console_graph_build_nodes \
                             (run_id, node_id, created_at_ns, remaining_parents, processed) \
                             VALUES (?, ?, ?, 0, 0)",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(root.node_id)
                        .bind::<BigInt, _>(root.created_at_ns)
                        .execute(connection)?;
                        record_discovered_nodes(
                            connection,
                            run_id,
                            &owner_id,
                            lease_epoch,
                            inserted,
                        )
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn work_node(&self, node_id: &str) -> crate::Result<Option<WorkNodeRow>> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let node_id = node_id.to_owned();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT processed, projection_complete \
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
                            build.projection_complete, build.projection_phase, \
                            build.projection_raw_cursor, \
                            build.projection_edge_order_cursor, \
                            build.projection_edge_source_cursor, \
                            build.projection_required_rank \
                     FROM console_graph_build_nodes AS build \
                     WHERE build.run_id = ? AND build.node_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(node_id)
                .get_result::<ProjectionWorkNodeRow>(connection)
                .optional()
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn discover_ready_child_page(&self, parent_id: &str, cursor: &str) -> crate::Result<()> {
        let page = self.active_child_page(parent_id, cursor).await?;
        let Some(next_child_id) = page.inspected_cursor else {
            self.commit_child_page(parent_id, cursor, cursor, &[], true)
                .await?;
            return Ok(());
        };
        let children = page
            .nodes
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
        for child in &children {
            self.initialize_parent_requirements(child).await?;
        }
        self.commit_child_page(parent_id, cursor, &next_child_id, &children, false)
            .await?;
        Ok(())
    }

    async fn initialize_parent_requirements(&self, child: &FrontierNode) -> crate::Result<()> {
        loop {
            if self.advance_parent_requirement_page(child).await? {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    async fn advance_parent_requirement_page(&self, child: &FrontierNode) -> crate::Result<bool> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let child = child.clone();
        let root_id = self.root_id.clone();
        let append = self.kind == IncrementalBuildKind::Append;
        self.database
            .with_write_connection(
                "checkpoint DAG parent requirement page",
                move |connection| {
                    connection
                        .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                            require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                            let inserted = diesel::sql_query(
                                "INSERT OR IGNORE INTO console_graph_build_nodes \
                                 (run_id, node_id, created_at_ns, remaining_parents, processed) \
                             VALUES (?, ?, ?, 0, 0)",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&child.node_id)
                            .bind::<BigInt, _>(child.created_at_ns)
                            .execute(connection)?;
                            record_discovered_nodes(
                                connection,
                                run_id,
                                &owner_id,
                                lease_epoch,
                                inserted,
                            )?;
                            let state = diesel::sql_query(
                                "SELECT parent_requirement_cursor, parent_requirements_complete \
                             FROM console_graph_build_nodes \
                             WHERE run_id = ? AND node_id = ?",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&child.node_id)
                            .get_result::<ParentRequirementStateRow>(connection)?;
                            if state.parent_requirements_complete == 1 {
                                return Ok(true);
                            }

                            let parents = if append {
                                diesel::sql_query(
                                    "SELECT relations.parent_id AS value, \
                                            CASE WHEN relations.parent_id <> ? AND EXISTS ( \
                                                SELECT 1 \
                                                FROM console_graph_build_delta_nodes AS delta \
                                                WHERE delta.run_id = ? \
                                                  AND delta.node_id = relations.parent_id \
                                            ) THEN 1 ELSE 0 END AS eligible \
                                     FROM console_graph_source_node_relations AS relations \
                                     WHERE relations.child_id = ? \
                                       AND relations.parent_id > ? \
                                     ORDER BY relations.parent_id LIMIT ?",
                                )
                                .bind::<Text, _>(&root_id)
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&child.node_id)
                                .bind::<Text, _>(&state.parent_requirement_cursor)
                                .bind::<BigInt, _>(PARENT_REQUIREMENT_PAGE_SIZE)
                                .load::<EligibleTextValueRow>(connection)?
                            } else {
                                diesel::sql_query(
                                    "SELECT relations.parent_id AS value, \
                                            CASE WHEN EXISTS ( \
                                                SELECT 1 \
                                                FROM \
                                                    console_graph_build_effective_source_branch_nodes \
                                                        AS membership \
                                                WHERE membership.run_id = ? \
                                                  AND membership.node_id = relations.parent_id \
                                            ) THEN 1 ELSE 0 END AS eligible \
                                     FROM console_graph_source_node_relations AS relations \
                                     WHERE relations.child_id = ? \
                                       AND relations.parent_id > ? \
                                     ORDER BY relations.parent_id LIMIT ?",
                                )
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&child.node_id)
                                .bind::<Text, _>(&state.parent_requirement_cursor)
                                .bind::<BigInt, _>(PARENT_REQUIREMENT_PAGE_SIZE)
                                .load::<EligibleTextValueRow>(connection)?
                            };
                            let Some(next_cursor) =
                                parents.last().map(|parent| parent.value.as_str())
                            else {
                                let updated = diesel::sql_query(
                                    "UPDATE console_graph_build_nodes \
                                 SET parent_requirements_complete = 1 \
                                 WHERE run_id = ? AND node_id = ? AND processed = 0 \
                                   AND parent_requirements_complete = 0 \
                                   AND parent_requirement_cursor = ?",
                                )
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&child.node_id)
                                .bind::<Text, _>(&state.parent_requirement_cursor)
                                .execute(connection)?;
                                if updated != 1 {
                                    return Err(diesel::result::Error::NotFound);
                                }
                                return Ok(true);
                            };
                            let updated = diesel::sql_query(
                                "UPDATE console_graph_build_nodes \
                             SET parent_requirement_cursor = ?, \
                                 remaining_parents = remaining_parents + ? \
                             WHERE run_id = ? AND node_id = ? AND processed = 0 \
                               AND parent_requirements_complete = 0 \
                               AND parent_requirement_cursor = ?",
                            )
                            .bind::<Text, _>(next_cursor)
                            .bind::<Integer, _>(
                                i32::try_from(
                                    parents.iter().filter(|parent| parent.eligible == 1).count(),
                                )
                                    .expect("parent requirement page must fit in SQLite INTEGER"),
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&child.node_id)
                            .bind::<Text, _>(&state.parent_requirement_cursor)
                            .execute(connection)?;
                            if updated != 1 {
                                return Err(diesel::result::Error::NotFound);
                            }
                            Ok(false)
                        })
                        .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
                },
            )
            .await
    }

    async fn active_child_page(
        &self,
        parent_id: &str,
        cursor: &str,
    ) -> crate::Result<InspectedSourceNodePage> {
        let path = self.path.clone();
        let parent_id = parent_id.to_owned();
        let cursor = cursor.to_owned();
        let limit = self.child_page_size.min(i64::MAX as usize) as i64;
        let run_id = self.run_id;
        let append = self.kind == IncrementalBuildKind::Append;
        let rows = self
            .database
            .with_connection(move |connection| {
                if append {
                    diesel::sql_query(
                        "SELECT relations.child_id AS node_id, nodes.node_json AS node_json, \
                                CASE WHEN EXISTS ( \
                                    SELECT 1 FROM console_graph_build_delta_nodes AS delta \
                                    WHERE delta.run_id = ? \
                                      AND delta.node_id = relations.child_id \
                                ) THEN 1 ELSE 0 END AS eligible \
                         FROM console_graph_source_node_relations AS relations \
                         INNER JOIN console_graph_source_nodes AS nodes \
                             ON nodes.node_id = relations.child_id \
                         WHERE relations.parent_id = ? AND relations.child_id > ? \
                         ORDER BY relations.child_id LIMIT ?",
                    )
                    .bind::<BigInt, _>(run_id)
                    .bind::<Text, _>(parent_id)
                    .bind::<Text, _>(cursor)
                    .bind::<BigInt, _>(limit)
                    .load::<CandidateSourceNodeRow>(connection)
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
                } else {
                    diesel::sql_query(
                        "SELECT relations.child_id AS node_id, nodes.node_json AS node_json, \
                                CASE WHEN EXISTS ( \
                                    SELECT 1 \
                                    FROM console_graph_build_effective_source_branch_nodes \
                                        AS branch_nodes \
                                    WHERE branch_nodes.run_id = ? \
                                      AND branch_nodes.node_id = relations.child_id \
                                ) THEN 1 ELSE 0 END AS eligible \
                         FROM console_graph_source_node_relations AS relations \
                         INNER JOIN console_graph_source_nodes AS nodes \
                             ON nodes.node_id = relations.child_id \
                         WHERE relations.parent_id = ? AND relations.child_id > ? \
                         ORDER BY relations.child_id LIMIT ?",
                    )
                    .bind::<BigInt, _>(run_id)
                    .bind::<Text, _>(parent_id)
                    .bind::<Text, _>(cursor)
                    .bind::<BigInt, _>(limit)
                    .load::<CandidateSourceNodeRow>(connection)
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
                }
            })
            .await?;
        let inspected_cursor = rows.last().map(|row| row.node_id.clone());
        let nodes = rows
            .into_iter()
            .filter(|row| row.eligible == 1)
            .map(|row| SourceNodeRow {
                node_id: row.node_id,
                node_json: row.node_json,
            })
            .collect();
        Ok(InspectedSourceNodePage {
            nodes,
            inspected_cursor,
        })
    }

    async fn commit_child_page(
        &self,
        parent_id: &str,
        expected_cursor: &str,
        next_cursor: &str,
        children: &[FrontierNode],
        complete: bool,
    ) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let parent_id = parent_id.to_owned();
        let expected_cursor = expected_cursor.to_owned();
        let next_cursor = next_cursor.to_owned();
        let children = children.to_vec();
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        self.database
            .with_write_connection("checkpoint DAG child page", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let current = diesel::sql_query(
                            "SELECT next_child_id, complete \
                             FROM console_graph_build_parent_expansions \
                             WHERE run_id = ? AND parent_id = ?",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&parent_id)
                        .get_result::<ParentExpansionRow>(connection)?;
                        if current.complete == 1 || current.next_child_id != expected_cursor {
                            return Ok(());
                        }
                        for child in &children {
                            let satisfied = diesel::sql_query(
                                "INSERT OR IGNORE INTO \
                                     console_graph_build_parent_satisfactions \
                                     (run_id, parent_id, child_id) VALUES (?, ?, ?)",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&parent_id)
                            .bind::<Text, _>(&child.node_id)
                            .execute(connection)?;
                            if satisfied == 0 {
                                continue;
                            }
                            let updated = diesel::sql_query(
                                "UPDATE console_graph_build_nodes \
                                 SET remaining_parents = remaining_parents - 1 \
                                 WHERE run_id = ? AND node_id = ? \
                                   AND processed = 0 AND remaining_parents > 0 \
                                   AND parent_requirements_complete = 1",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&child.node_id)
                            .execute(connection)?;
                            if updated != 1 {
                                return Err(diesel::result::Error::NotFound);
                            }
                        }
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_build_parent_expansions \
                             SET next_child_id = ?, complete = ? \
                             WHERE run_id = ? AND parent_id = ? \
                               AND next_child_id = ? AND complete = 0",
                        )
                        .bind::<Text, _>(&next_cursor)
                        .bind::<Integer, _>(i32::from(complete))
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&parent_id)
                        .bind::<Text, _>(&expected_cursor)
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn process_node(
        &self,
        node: &Node,
        snapshots: &ConsoleGraphSnapshotStore,
        lease: &IncrementalBuildLease,
        buffer: &mut GraphBuffer,
    ) -> crate::Result<()> {
        let state = self
            .projection_work_node(&node.id)
            .await?
            .with_context(|| crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "work_node",
                value: node.id.clone(),
            })?;
        ensure!(
            state.remaining_parents == 0 && state.processed == 0 && state.projection_complete == 0,
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "ready_node",
                value: format!(
                    "{} remaining={} processed={} projection_complete={}",
                    node.id, state.remaining_parents, state.processed, state.projection_complete
                ),
            }
        );
        if node.is_root() {
            self.complete_node_projection(&node.id, &state.projection_phase)
                .await?;
            return Ok(());
        }

        match state.projection_phase.as_str() {
            "all_prepare" => {
                let page = self.raw_projection_edge_page(node, state.projection_raw_cursor);
                if page.is_empty() {
                    self.advance_node_projection_phase(&node.id, "all_prepare", "all_rank")
                        .await?;
                } else {
                    self.commit_all_projection_edge_page(
                        &node.id,
                        state.projection_raw_cursor,
                        &page,
                    )
                    .await?;
                }
            }
            "all_rank" => {
                let page = self
                    .projection_edge_page(
                        GraphMode::All,
                        &node.id,
                        state.projection_edge_order_cursor,
                        &state.projection_edge_source_cursor,
                    )
                    .await?;
                if page.is_empty() {
                    self.advance_node_projection_phase(&node.id, "all_rank", "all_node")
                        .await?;
                } else {
                    let required_rank = self
                        .projection_page_required_rank(GraphMode::All, &page)
                        .await?;
                    self.checkpoint_projection_rank_page(
                        &node.id,
                        "all_rank",
                        state.projection_edge_order_cursor,
                        &state.projection_edge_source_cursor,
                        required_rank,
                        &page,
                    )
                    .await?;
                }
            }
            "all_node" => {
                self.buffer_projection_node(
                    node,
                    GraphMode::All,
                    state.projection_required_rank,
                    snapshots,
                    lease,
                    buffer,
                )
                .await?;
                flush_graph_buffer(snapshots, lease, buffer).await?;
                self.advance_node_projection_phase(&node.id, "all_node", "all_edges")
                    .await?;
            }
            "all_edges" => {
                let page = self
                    .projection_edge_page(
                        GraphMode::All,
                        &node.id,
                        state.projection_edge_order_cursor,
                        &state.projection_edge_source_cursor,
                    )
                    .await?;
                if page.is_empty() {
                    if matches!(node.kind, Kind::Anchor(_))
                        && self.is_visible_anchor(&node.id).await?
                    {
                        self.advance_node_projection_phase(&node.id, "all_edges", "anchor_prepare")
                            .await?;
                    } else {
                        self.complete_node_projection(&node.id, "all_edges").await?;
                    }
                } else {
                    self.buffer_projection_edge_page(
                        node,
                        GraphMode::All,
                        &page,
                        snapshots,
                        lease,
                        buffer,
                    )
                    .await?;
                    flush_graph_buffer(snapshots, lease, buffer).await?;
                    self.checkpoint_projection_edge_page(
                        &node.id,
                        "all_edges",
                        state.projection_edge_order_cursor,
                        &state.projection_edge_source_cursor,
                        &page,
                    )
                    .await?;
                }
            }
            "anchor_prepare" => {
                if self.advance_anchor_resolution(node).await? {
                    self.advance_node_projection_phase(&node.id, "anchor_prepare", "anchor_rank")
                        .await?;
                }
            }
            "anchor_rank" => {
                let page = self
                    .projection_edge_page(
                        GraphMode::Anchors,
                        &node.id,
                        state.projection_edge_order_cursor,
                        &state.projection_edge_source_cursor,
                    )
                    .await?;
                if page.is_empty() {
                    self.advance_node_projection_phase(&node.id, "anchor_rank", "anchor_node")
                        .await?;
                } else {
                    let required_rank = self
                        .projection_page_required_rank(GraphMode::Anchors, &page)
                        .await?;
                    self.checkpoint_projection_rank_page(
                        &node.id,
                        "anchor_rank",
                        state.projection_edge_order_cursor,
                        &state.projection_edge_source_cursor,
                        required_rank,
                        &page,
                    )
                    .await?;
                }
            }
            "anchor_node" => {
                self.buffer_projection_node(
                    node,
                    GraphMode::Anchors,
                    state.projection_required_rank,
                    snapshots,
                    lease,
                    buffer,
                )
                .await?;
                flush_graph_buffer(snapshots, lease, buffer).await?;
                self.advance_node_projection_phase(&node.id, "anchor_node", "anchor_edges")
                    .await?;
            }
            "anchor_edges" => {
                let page = self
                    .projection_edge_page(
                        GraphMode::Anchors,
                        &node.id,
                        state.projection_edge_order_cursor,
                        &state.projection_edge_source_cursor,
                    )
                    .await?;
                if page.is_empty() {
                    self.complete_node_projection(&node.id, "anchor_edges")
                        .await?;
                } else {
                    self.buffer_projection_edge_page(
                        node,
                        GraphMode::Anchors,
                        &page,
                        snapshots,
                        lease,
                        buffer,
                    )
                    .await?;
                    flush_graph_buffer(snapshots, lease, buffer).await?;
                    self.checkpoint_projection_edge_page(
                        &node.id,
                        "anchor_edges",
                        state.projection_edge_order_cursor,
                        &state.projection_edge_source_cursor,
                        &page,
                    )
                    .await?;
                }
            }
            "complete" if state.projection_complete == 1 => {}
            phase => {
                return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                    column: "projection_phase",
                    value: format!("{}:{phase}", node.id),
                }
                .fail();
            }
        }
        Ok(())
    }

    fn raw_projection_edge_page(&self, node: &Node, cursor: i64) -> Vec<ProjectionEdgeRow> {
        let merge_parents = match &node.kind {
            Kind::Anchor(anchor) => anchor.merge_parents(),
            _ => &[],
        };
        let total = merge_parents.len().saturating_add(1);
        let start = usize::try_from(cursor.saturating_add(1)).unwrap_or(usize::MAX);
        let end = start
            .saturating_add(self.child_page_size.min(DEFAULT_CHILD_PAGE_SIZE))
            .min(total);
        (start..end)
            .map(|edge_order| {
                if edge_order == 0 {
                    ProjectionEdgeRow {
                        source_id: node.parent.clone(),
                        edge_kind: "primary".to_owned(),
                        edge_order: 0,
                    }
                } else {
                    let parent = &merge_parents[edge_order - 1];
                    ProjectionEdgeRow {
                        source_id: parent.node_id().to_owned(),
                        edge_kind: if parent.is_shadow() {
                            "shadow".to_owned()
                        } else {
                            "merge".to_owned()
                        },
                        edge_order: i32::try_from(edge_order).unwrap_or(i32::MAX),
                    }
                }
            })
            .collect()
    }

    async fn commit_all_projection_edge_page(
        &self,
        target_id: &str,
        expected_cursor: i64,
        page: &[ProjectionEdgeRow],
    ) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let root_id = self.root_id.clone();
        let baseline_generation = self.baseline_generation;
        let append = self.kind == IncrementalBuildKind::Append;
        let target_id = target_id.to_owned();
        let page = page.to_vec();
        let next_cursor = i64::from(
            page.last()
                .expect("non-empty projection edge page must have a cursor")
                .edge_order,
        );
        self.database
            .with_write_connection("checkpoint all-mode raw parent page", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let current = diesel::sql_query(
                            "SELECT COUNT(*) AS count FROM console_graph_build_nodes \
                             WHERE run_id = ? AND node_id = ? \
                               AND projection_complete = 0 \
                               AND projection_phase = 'all_prepare' \
                               AND projection_raw_cursor = ?",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&target_id)
                        .bind::<BigInt, _>(expected_cursor)
                        .get_result::<CountRow>(connection)?
                        .count;
                        if current != 1 {
                            return Ok(());
                        }
                        for edge in page {
                            if edge.source_id.is_empty()
                                || edge.source_id == root_id
                                || edge.source_id == target_id
                            {
                                continue;
                            }
                            let eligible = diesel::sql_query(
                                "SELECT CASE WHEN EXISTS ( \
                                     SELECT 1 FROM console_graph_build_nodes \
                                     WHERE run_id = ? AND node_id = ? AND processed = 1 \
                                 ) OR (? = 1 AND EXISTS ( \
                                     SELECT 1 FROM console_graph_node_locations \
                                     WHERE generation = ? AND mode = 'all' AND node_id = ? \
                                 )) THEN 1 ELSE 0 END AS value",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&edge.source_id)
                            .bind::<Integer, _>(i32::from(append))
                            .bind::<BigInt, _>(baseline_generation)
                            .bind::<Text, _>(&edge.source_id)
                            .get_result::<IntegerRow>(connection)?
                            .value;
                            if eligible == 0 {
                                continue;
                            }
                            diesel::sql_query(
                                "INSERT OR IGNORE INTO console_graph_build_projection_edges \
                                     (run_id, mode, target_id, source_id, edge_kind, edge_order) \
                                 VALUES (?, 'all', ?, ?, ?, ?)",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&target_id)
                            .bind::<Text, _>(edge.source_id)
                            .bind::<Text, _>(edge.edge_kind)
                            .bind::<Integer, _>(edge.edge_order)
                            .execute(connection)?;
                        }
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_build_nodes \
                             SET projection_raw_cursor = ? \
                             WHERE run_id = ? AND node_id = ? \
                               AND projection_complete = 0 \
                               AND projection_phase = 'all_prepare' \
                               AND projection_raw_cursor = ?",
                        )
                        .bind::<BigInt, _>(next_cursor)
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&target_id)
                        .bind::<BigInt, _>(expected_cursor)
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn projection_edge_page(
        &self,
        mode: GraphMode,
        target_id: &str,
        edge_order_cursor: i32,
        source_cursor: &str,
    ) -> crate::Result<Vec<ProjectionEdgeRow>> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let target_id = target_id.to_owned();
        let source_cursor = source_cursor.to_owned();
        let limit = self.child_page_size.min(DEFAULT_CHILD_PAGE_SIZE) as i64;
        self.database
            .with_connection(move |connection| {
                let query = match mode {
                    GraphMode::All => {
                        "SELECT source_id, edge_kind, edge_order \
                         FROM console_graph_build_projection_edges \
                         WHERE run_id = ? AND mode = 'all' AND target_id = ? \
                           AND (edge_order > ? OR (edge_order = ? AND source_id > ?)) \
                         ORDER BY edge_order, source_id LIMIT ?"
                    }
                    GraphMode::Anchors => {
                        "SELECT source_id, edge_kind, edge_order \
                         FROM console_graph_build_anchor_edges \
                         WHERE run_id = ? AND target_id = ? \
                           AND (edge_order > ? OR (edge_order = ? AND source_id > ?)) \
                         ORDER BY edge_order, source_id LIMIT ?"
                    }
                };
                diesel::sql_query(query)
                    .bind::<BigInt, _>(run_id)
                    .bind::<Text, _>(target_id)
                    .bind::<Integer, _>(edge_order_cursor)
                    .bind::<Integer, _>(edge_order_cursor)
                    .bind::<Text, _>(source_cursor)
                    .bind::<BigInt, _>(limit)
                    .load::<ProjectionEdgeRow>(connection)
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn is_visible_anchor(&self, target_id: &str) -> crate::Result<bool> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let baseline_generation = self.baseline_generation;
        let append = self.kind == IncrementalBuildKind::Append;
        let target_id = target_id.to_owned();
        let visible = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT CASE WHEN EXISTS ( \
                         SELECT 1 FROM console_graph_anchor_scopes \
                         WHERE generation = ? AND node_id = ? \
                     ) OR (? = 1 AND EXISTS ( \
                         SELECT 1 FROM console_graph_anchor_scopes \
                         WHERE generation = ? AND node_id = ? \
                     )) THEN 1 ELSE 0 END AS value",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(&target_id)
                .bind::<Integer, _>(i32::from(append))
                .bind::<BigInt, _>(baseline_generation)
                .bind::<Text, _>(&target_id)
                .get_result::<IntegerRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await?
            .value;
        Ok(visible == 1)
    }

    async fn projection_page_required_rank(
        &self,
        mode: GraphMode,
        page: &[ProjectionEdgeRow],
    ) -> crate::Result<i32> {
        let mut required_rank = 0;
        for edge in page {
            let slot = self.slot(mode, &edge.source_id).await?.with_context(|| {
                crate::error::InvalidGraphSnapshotStoreValueSnafu {
                    column: "layout_parent",
                    value: format!("{}:{}", mode.as_query_value(), edge.source_id),
                }
            })?;
            required_rank = required_rank.max(slot.rank.saturating_add(1));
        }
        Ok(required_rank)
    }

    async fn advance_node_projection_phase(
        &self,
        node_id: &str,
        expected_phase: &str,
        next_phase: &str,
    ) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let node_id = node_id.to_owned();
        let expected_phase = expected_phase.to_owned();
        let next_phase = next_phase.to_owned();
        self.database
            .with_write_connection("advance node projection phase", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_build_nodes \
                             SET projection_phase = ?, projection_raw_cursor = -1, \
                                 projection_edge_order_cursor = -1, \
                                 projection_edge_source_cursor = '', \
                                 projection_required_rank = CASE \
                                     WHEN ? IN ('all_rank', 'anchor_rank') THEN 0 \
                                     ELSE projection_required_rank END \
                             WHERE run_id = ? AND node_id = ? \
                               AND projection_complete = 0 AND projection_phase = ?",
                        )
                        .bind::<Text, _>(&next_phase)
                        .bind::<Text, _>(&next_phase)
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(node_id)
                        .bind::<Text, _>(expected_phase)
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn checkpoint_projection_rank_page(
        &self,
        node_id: &str,
        phase: &str,
        expected_order: i32,
        expected_source: &str,
        page_required_rank: i32,
        page: &[ProjectionEdgeRow],
    ) -> crate::Result<()> {
        let last = page
            .last()
            .expect("non-empty projection rank page must have a last row");
        let next_order = last.edge_order;
        let next_source = last.source_id.clone();
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let node_id = node_id.to_owned();
        let phase = phase.to_owned();
        let expected_source = expected_source.to_owned();
        self.database
            .with_write_connection("checkpoint node projection rank page", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_build_nodes \
                             SET projection_required_rank = \
                                     MAX(projection_required_rank, ?), \
                                 projection_edge_order_cursor = ?, \
                                 projection_edge_source_cursor = ? \
                             WHERE run_id = ? AND node_id = ? \
                               AND projection_complete = 0 AND projection_phase = ? \
                               AND projection_edge_order_cursor = ? \
                               AND projection_edge_source_cursor = ?",
                        )
                        .bind::<Integer, _>(page_required_rank)
                        .bind::<Integer, _>(next_order)
                        .bind::<Text, _>(next_source)
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(node_id)
                        .bind::<Text, _>(phase)
                        .bind::<Integer, _>(expected_order)
                        .bind::<Text, _>(expected_source)
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn checkpoint_projection_edge_page(
        &self,
        node_id: &str,
        phase: &str,
        expected_order: i32,
        expected_source: &str,
        page: &[ProjectionEdgeRow],
    ) -> crate::Result<()> {
        let last = page
            .last()
            .expect("non-empty projection edge page must have a last row");
        let next_order = last.edge_order;
        let next_source = last.source_id.clone();
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let node_id = node_id.to_owned();
        let phase = phase.to_owned();
        let expected_source = expected_source.to_owned();
        self.database
            .with_write_connection("checkpoint node projection edge page", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_build_nodes \
                             SET projection_edge_order_cursor = ?, \
                                 projection_edge_source_cursor = ? \
                             WHERE run_id = ? AND node_id = ? \
                               AND projection_complete = 0 AND projection_phase = ? \
                               AND projection_edge_order_cursor = ? \
                               AND projection_edge_source_cursor = ?",
                        )
                        .bind::<Integer, _>(next_order)
                        .bind::<Text, _>(next_source)
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(node_id)
                        .bind::<Text, _>(phase)
                        .bind::<Integer, _>(expected_order)
                        .bind::<Text, _>(expected_source)
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn complete_node_projection(
        &self,
        node_id: &str,
        expected_phase: &str,
    ) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let node_id = node_id.to_owned();
        let expected_phase = expected_phase.to_owned();
        self.database
            .with_write_connection("complete node projection", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_build_nodes \
                             SET projection_phase = 'complete', projection_complete = 1 \
                             WHERE run_id = ? AND node_id = ? \
                               AND processed = 0 AND remaining_parents = 0 \
                               AND projection_complete = 0 AND projection_phase = ?",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(node_id)
                        .bind::<Text, _>(expected_phase)
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn advance_anchor_resolution(&self, node: &Node) -> crate::Result<bool> {
        let target_id = node.id.as_str();
        let state = self.anchor_projection_state(target_id).await?;
        if state.raw_complete == 0 {
            let page = self.raw_projection_edge_page(node, state.raw_cursor);
            self.commit_anchor_raw_edge_page(target_id, state.raw_cursor, &page)
                .await?;
            return Ok(false);
        }
        if state.resolution_complete == 1 {
            return Ok(true);
        }
        let page = self.anchor_resolution_page(target_id).await?;
        if page.is_empty() {
            self.complete_anchor_resolution(target_id).await?;
            return Ok(true);
        }
        self.commit_anchor_resolution_page(target_id, &page).await?;
        Ok(false)
    }

    async fn anchor_projection_state(
        &self,
        target_id: &str,
    ) -> crate::Result<AnchorProjectionStateRow> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let target_id = target_id.to_owned();
        self.database
            .with_write_connection("open anchor projection state", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        diesel::sql_query(
                            "INSERT OR IGNORE INTO console_graph_build_anchor_projection_state \
                                 (run_id, target_id) VALUES (?, ?)",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&target_id)
                        .execute(connection)?;
                        diesel::sql_query(
                            "SELECT raw_cursor, raw_complete, resolution_complete \
                             FROM console_graph_build_anchor_projection_state \
                             WHERE run_id = ? AND target_id = ?",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&target_id)
                        .get_result::<AnchorProjectionStateRow>(connection)
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn commit_anchor_raw_edge_page(
        &self,
        target_id: &str,
        expected_cursor: i64,
        page: &[ProjectionEdgeRow],
    ) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let target_id = target_id.to_owned();
        let page = page.to_vec();
        self.database
            .with_write_connection("checkpoint anchor raw parent page", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let current = diesel::sql_query(
                            "SELECT raw_cursor, raw_complete, resolution_complete \
                             FROM console_graph_build_anchor_projection_state \
                             WHERE run_id = ? AND target_id = ?",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&target_id)
                        .get_result::<AnchorProjectionStateRow>(connection)?;
                        if current.raw_complete == 1 || current.raw_cursor != expected_cursor {
                            return Ok(());
                        }
                        for edge in &page {
                            if edge.source_id.is_empty() {
                                continue;
                            }
                            diesel::sql_query(
                                "INSERT OR IGNORE INTO console_graph_build_anchor_raw_edges ( \
                                     run_id, target_id, edge_order, raw_parent_id, edge_kind, \
                                     current_ancestor_id, ancestor_depth, complete \
                                 ) VALUES (?, ?, ?, ?, ?, ?, 0, 0)",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&target_id)
                            .bind::<Integer, _>(edge.edge_order)
                            .bind::<Text, _>(&edge.source_id)
                            .bind::<Text, _>(&edge.edge_kind)
                            .bind::<Text, _>(&edge.source_id)
                            .execute(connection)?;
                        }
                        if page.is_empty() {
                            diesel::sql_query(
                                "UPDATE console_graph_build_anchor_projection_state \
                                 SET raw_complete = 1 \
                                 WHERE run_id = ? AND target_id = ? \
                                   AND raw_cursor = ? AND raw_complete = 0",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&target_id)
                            .bind::<BigInt, _>(expected_cursor)
                            .execute(connection)?;
                        } else {
                            let next_cursor = i64::from(
                                page.last()
                                    .expect("non-empty anchor raw page must have a cursor")
                                    .edge_order,
                            );
                            diesel::sql_query(
                                "UPDATE console_graph_build_anchor_projection_state \
                                 SET raw_cursor = ? \
                                 WHERE run_id = ? AND target_id = ? \
                                   AND raw_cursor = ? AND raw_complete = 0",
                            )
                            .bind::<BigInt, _>(next_cursor)
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&target_id)
                            .bind::<BigInt, _>(expected_cursor)
                            .execute(connection)?;
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn anchor_resolution_page(
        &self,
        target_id: &str,
    ) -> crate::Result<Vec<AnchorRawEdgeRow>> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let target_id = target_id.to_owned();
        let limit = self.child_page_size.min(DEFAULT_CHILD_PAGE_SIZE) as i64;
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT edge_order, edge_kind, \
                            current_ancestor_id, ancestor_depth \
                     FROM console_graph_build_anchor_raw_edges \
                     WHERE run_id = ? AND target_id = ? AND complete = 0 \
                     ORDER BY edge_order LIMIT ?",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(target_id)
                .bind::<BigInt, _>(limit)
                .load::<AnchorRawEdgeRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn commit_anchor_resolution_page(
        &self,
        target_id: &str,
        page: &[AnchorRawEdgeRow],
    ) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let target_id = target_id.to_owned();
        let page = page.to_vec();
        self.database
            .with_write_connection("checkpoint anchor ancestor page", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        for edge in page {
                            let first_visit = diesel::sql_query(
                                "INSERT OR IGNORE INTO \
                                     console_graph_build_anchor_ancestor_visits ( \
                                     run_id, target_id, edge_order, ancestor_id \
                                 ) VALUES (?, ?, ?, ?)",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&target_id)
                            .bind::<Integer, _>(edge.edge_order)
                            .bind::<Text, _>(&edge.current_ancestor_id)
                            .execute(connection)?;
                            if first_visit == 0 {
                                mark_anchor_raw_edge_complete(
                                    connection, run_id, &target_id, &edge,
                                )?;
                                continue;
                            }
                            let ancestor = diesel::sql_query(
                                "SELECT nodes.parent_id, \
                                        CASE WHEN json_type( \
                                            nodes.node_json, '$.kind.Anchor' \
                                        ) = 'object' THEN 1 ELSE 0 END AS is_anchor \
                                 FROM console_graph_source_nodes AS nodes \
                                 WHERE nodes.node_id = ? AND EXISTS ( \
                                     SELECT 1 \
                                     FROM console_graph_build_runs AS run \
                                     INNER JOIN console_graph_anchor_scopes AS target_scope \
                                       ON target_scope.generation = run.run_id \
                                     WHERE run.run_id = ? \
                                       AND target_scope.node_id = ? \
                                       AND (EXISTS ( \
                                           SELECT 1 \
                                           FROM console_graph_anchor_scopes AS ancestor_scope \
                                           WHERE ancestor_scope.generation = run.run_id \
                                             AND ancestor_scope.branch_name = \
                                                 target_scope.branch_name \
                                             AND ancestor_scope.node_id = nodes.node_id \
                                       ) OR EXISTS ( \
                                           SELECT 1 \
                                           FROM console_graph_build_changed_branches AS changed \
                                           INNER JOIN console_graph_anchor_scopes AS baseline_scope \
                                             ON baseline_scope.generation = \
                                                 run.dag_baseline_generation \
                                            AND baseline_scope.branch_name = \
                                                changed.branch_name \
                                            AND baseline_scope.node_id = nodes.node_id \
                                           WHERE changed.run_id = run.run_id \
                                             AND changed.branch_name = \
                                                 target_scope.branch_name \
                                             AND changed.scope_reuses_baseline = 1 \
                                       )) \
                                 )",
                            )
                            .bind::<Text, _>(&edge.current_ancestor_id)
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&target_id)
                            .get_result::<AnchorAncestorRow>(connection)
                            .optional()?;
                            let Some(ancestor) = ancestor else {
                                mark_anchor_raw_edge_complete(
                                    connection, run_id, &target_id, &edge,
                                )?;
                                continue;
                            };
                            if ancestor.is_anchor == 1 {
                                if edge.current_ancestor_id != target_id {
                                    diesel::sql_query(
                                        "INSERT INTO console_graph_build_anchor_edges ( \
                                             run_id, target_id, source_id, edge_kind, \
                                             edge_order, ancestor_depth \
                                         ) VALUES (?, ?, ?, ?, ?, ?) \
                                         ON CONFLICT(run_id, target_id, source_id) DO UPDATE SET \
                                             edge_kind = excluded.edge_kind, \
                                             edge_order = excluded.edge_order, \
                                             ancestor_depth = excluded.ancestor_depth \
                                         WHERE excluded.edge_order < edge_order \
                                            OR (excluded.edge_order = edge_order \
                                                AND excluded.ancestor_depth < ancestor_depth)",
                                    )
                                    .bind::<BigInt, _>(run_id)
                                    .bind::<Text, _>(&target_id)
                                    .bind::<Text, _>(&edge.current_ancestor_id)
                                    .bind::<Text, _>(&edge.edge_kind)
                                    .bind::<Integer, _>(edge.edge_order)
                                    .bind::<Integer, _>(edge.ancestor_depth)
                                    .execute(connection)?;
                                }
                                mark_anchor_raw_edge_complete(
                                    connection, run_id, &target_id, &edge,
                                )?;
                            } else if ancestor.parent_id.is_empty() {
                                mark_anchor_raw_edge_complete(
                                    connection, run_id, &target_id, &edge,
                                )?;
                            } else {
                                diesel::sql_query(
                                    "UPDATE console_graph_build_anchor_raw_edges \
                                     SET current_ancestor_id = ?, \
                                         ancestor_depth = ancestor_depth + 1 \
                                     WHERE run_id = ? AND target_id = ? AND edge_order = ? \
                                       AND complete = 0 AND current_ancestor_id = ? \
                                       AND ancestor_depth = ?",
                                )
                                .bind::<Text, _>(ancestor.parent_id)
                                .bind::<BigInt, _>(run_id)
                                .bind::<Text, _>(&target_id)
                                .bind::<Integer, _>(edge.edge_order)
                                .bind::<Text, _>(edge.current_ancestor_id)
                                .bind::<Integer, _>(edge.ancestor_depth)
                                .execute(connection)?;
                            }
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn complete_anchor_resolution(&self, target_id: &str) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let target_id = target_id.to_owned();
        self.database
            .with_write_connection("complete anchor ancestor resolution", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_build_anchor_projection_state \
                             SET resolution_complete = 1 \
                             WHERE run_id = ? AND target_id = ? AND raw_complete = 1 \
                               AND resolution_complete = 0 AND NOT EXISTS ( \
                                   SELECT 1 FROM console_graph_build_anchor_raw_edges \
                                   WHERE run_id = ? AND target_id = ? AND complete = 0 \
                               )",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&target_id)
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&target_id)
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn buffer_projection_node(
        &self,
        node: &Node,
        mode: GraphMode,
        required_rank: i32,
        snapshots: &ConsoleGraphSnapshotStore,
        lease: &IncrementalBuildLease,
        buffer: &mut GraphBuffer,
    ) -> crate::Result<()> {
        let placement = self
            .place_node_at_rank(mode, &node.id, required_rank)
            .await?;
        let graph_node = graph_node_from_node(node.clone(), Vec::new(), Vec::new());
        push_buffered_node(
            snapshots,
            lease,
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
        Ok(())
    }

    async fn buffer_projection_edge_page(
        &self,
        node: &Node,
        mode: GraphMode,
        page: &[ProjectionEdgeRow],
        snapshots: &ConsoleGraphSnapshotStore,
        lease: &IncrementalBuildLease,
        buffer: &mut GraphBuffer,
    ) -> crate::Result<()> {
        let target = self.slot(mode, &node.id).await?.with_context(|| {
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "edge_target",
                value: node.id.clone(),
            }
        })?;
        for edge in page {
            let source = self.slot(mode, &edge.source_id).await?.with_context(|| {
                crate::error::InvalidGraphSnapshotStoreValueSnafu {
                    column: "edge_source",
                    value: edge.source_id.clone(),
                }
            })?;
            let layout_kind = graph_layout_edge_kind(stored_edge_kind(&edge.edge_kind)?);
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
                    x: target.x,
                    y: target.y,
                },
                super::incremental_layout::EndpointPortSlots {
                    source: ports.source_slot.max(0) as usize,
                    target: ports.target_slot.max(0) as usize,
                },
                self.layout,
            );
            push_buffered_edge(
                snapshots,
                lease,
                buffer,
                mode,
                GraphLayoutEdge {
                    source_node_id: edge.source_id.clone(),
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

    async fn place_node_at_rank(
        &self,
        mode: GraphMode,
        node_id: &str,
        required_rank: i32,
    ) -> crate::Result<SlotRow> {
        if let Some(existing) = self.slot(mode, node_id).await? {
            if existing.rank >= required_rank {
                self.activate_slot(mode, node_id).await?;
                return Ok(existing);
            }
            self.retire_slot(mode, node_id, &existing).await?;
        }

        let row = self.nearest_free_row(mode, required_rank, 0).await?;
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
        let baseline_generation = self.baseline_generation;
        let mode = mode.as_query_value().to_owned();
        let node_id = node_id.to_owned();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT rank, row_index, x, y FROM ( \
                         SELECT rank, row AS row_index, x, y, 0 AS source_order \
                         FROM console_graph_build_rank_slots \
                         WHERE run_id = ? AND mode = ? AND node_id = ? \
                         UNION ALL \
                         SELECT rank, sort_order AS row_index, x, y, 1 AS source_order \
                         FROM console_graph_node_locations AS baseline \
                         WHERE baseline.generation = ? AND baseline.mode = ? \
                           AND baseline.node_id = ? \
                           AND NOT EXISTS ( \
                               SELECT 1 FROM console_graph_build_rank_slots AS staged \
                               WHERE staged.run_id = ? AND staged.mode = baseline.mode \
                                 AND staged.node_id = baseline.node_id \
                           ) \
                     ) ORDER BY source_order LIMIT 1",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(&mode)
                .bind::<Text, _>(&node_id)
                .bind::<BigInt, _>(baseline_generation)
                .bind::<Text, _>(&mode)
                .bind::<Text, _>(&node_id)
                .bind::<BigInt, _>(run_id)
                .get_result::<SlotRow>(connection)
                .optional()
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn activate_slot(&self, mode: GraphMode, node_id: &str) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let baseline_generation = self.baseline_generation;
        let mode = mode.as_query_value().to_owned();
        let node_id = node_id.to_owned();
        self.database
            .with_write_connection("activate DAG layout slot", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_build_rank_slots ( \
                                 run_id, mode, rank, row, node_id, x, y, active \
                             ) \
                             SELECT ?, mode, rank, sort_order, node_id, x, y, 1 \
                             FROM console_graph_node_locations \
                             WHERE generation = ? AND mode = ? AND node_id = ? \
                             ON CONFLICT(run_id, mode, node_id) DO UPDATE SET active = 1",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<BigInt, _>(baseline_generation)
                        .bind::<Text, _>(&mode)
                        .bind::<Text, _>(&node_id)
                        .execute(connection)
                        .map(|_| ())
                    })
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
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let mode = mode.as_query_value().to_owned();
        let node_id = node_id.to_owned();
        let tombstone = format!(
            "tombstone:{run_id}:{}:{}:{}:{}",
            mode, slot.rank, slot.row_index, node_id
        );
        self.database
            .with_write_connection("retire DAG layout slot", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        diesel::sql_query(
                            "UPDATE console_graph_build_rank_slots \
                             SET node_id = ?, active = 0 \
                             WHERE run_id = ? AND mode = ? AND node_id = ?",
                        )
                        .bind::<Text, _>(tombstone)
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&mode)
                        .bind::<Text, _>(&node_id)
                        .execute(connection)
                        .map(|_| ())
                    })
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
        let baseline_generation = self.baseline_generation;
        let mode = mode.as_query_value().to_owned();
        let row = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT MAX( \
                         COALESCE(( \
                             SELECT MAX(row) FROM console_graph_build_rank_slots \
                             WHERE run_id = ? AND mode = ? AND rank = ? \
                         ), -1), \
                         COALESCE(( \
                             SELECT MAX(sort_order) FROM console_graph_node_locations \
                             WHERE generation = ? AND mode = ? AND rank = ? \
                         ), -1) \
                     ) + 1 AS value",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(&mode)
                .bind::<Integer, _>(rank)
                .bind::<BigInt, _>(baseline_generation)
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
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let mode = mode.as_query_value().to_owned();
        let node_id = node_id.to_owned();
        let slot = slot.clone();
        self.database
            .with_write_connection("insert DAG layout slot", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
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
                    })
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
        let baseline_generation = self.baseline_generation;
        let mode = mode.as_query_value().to_owned();
        let edge_key = edge_key.to_owned();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT source_slot, target_slot FROM ( \
                         SELECT source_slot, target_slot, 0 AS source_order \
                         FROM console_graph_build_edge_ports \
                         WHERE run_id = ? AND mode = ? AND edge_key = ? \
                         UNION ALL \
                         SELECT source_slot, target_slot, 1 AS source_order \
                         FROM console_graph_edge_ports AS baseline \
                         WHERE baseline.generation = ? AND baseline.mode = ? \
                           AND baseline.edge_key = ? \
                           AND NOT EXISTS ( \
                               SELECT 1 FROM console_graph_build_edge_ports AS staged \
                               WHERE staged.run_id = ? AND staged.mode = baseline.mode \
                                 AND staged.edge_key = baseline.edge_key \
                           ) \
                     ) ORDER BY source_order LIMIT 1",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(&mode)
                .bind::<Text, _>(&edge_key)
                .bind::<BigInt, _>(baseline_generation)
                .bind::<Text, _>(&mode)
                .bind::<Text, _>(&edge_key)
                .bind::<BigInt, _>(run_id)
                .get_result::<PortRow>(connection)
                .optional()
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn activate_port_assignment(&self, mode: GraphMode, edge_key: &str) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let baseline_generation = self.baseline_generation;
        let mode = mode.as_query_value().to_owned();
        let edge_key = edge_key.to_owned();
        self.database
            .with_write_connection("activate DAG edge ports", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_build_edge_ports ( \
                                 run_id, mode, edge_key, source_id, target_id, \
                                 source_slot, target_slot, active \
                             ) \
                             SELECT ?, mode, edge_key, source_id, target_id, \
                                    source_slot, target_slot, 1 \
                             FROM console_graph_edge_ports \
                             WHERE generation = ? AND mode = ? AND edge_key = ? \
                             ON CONFLICT(run_id, mode, edge_key) DO UPDATE SET active = 1",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<BigInt, _>(baseline_generation)
                        .bind::<Text, _>(mode)
                        .bind::<Text, _>(edge_key)
                        .execute(connection)
                        .map(|_| ())
                    })
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
        let baseline_generation = self.baseline_generation;
        let mode = mode.as_query_value().to_owned();
        let node_id = node_id.to_owned();
        let (id_column, slot_column) = if source {
            ("source_id", "source_slot")
        } else {
            ("target_id", "target_slot")
        };
        let query = format!(
            "SELECT MAX( \
                 COALESCE(( \
                     SELECT MAX({slot_column}) FROM console_graph_build_edge_ports \
                     WHERE run_id = ? AND mode = ? AND {id_column} = ? \
                 ), -1), \
                 COALESCE(( \
                     SELECT MAX({slot_column}) FROM console_graph_edge_ports \
                     WHERE generation = ? AND mode = ? AND {id_column} = ? \
                 ), -1) \
             ) + 1 AS value"
        );
        let row = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(query)
                    .bind::<BigInt, _>(run_id)
                    .bind::<Text, _>(&mode)
                    .bind::<Text, _>(&node_id)
                    .bind::<BigInt, _>(baseline_generation)
                    .bind::<Text, _>(&mode)
                    .bind::<Text, _>(&node_id)
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
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let mode = mode.as_query_value().to_owned();
        let edge_key = edge_key.to_owned();
        let source_id = source_id.to_owned();
        let target_id = target_id.to_owned();
        let source_slot = ports.source_slot;
        let target_slot = ports.target_slot;
        self.database
            .with_write_connection("insert DAG edge ports", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
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
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn finalize_state(&self) -> crate::Result<FinalizeStateRow> {
        let path = self.path.clone();
        let run_id = self.run_id;
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT dag_finalize_phase, dag_finalize_mode, dag_finalize_cursor \
                     FROM console_graph_build_runs WHERE run_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .get_result::<FinalizeStateRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn begin_port_finalization(&self) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        self.database
            .with_write_connection("begin DAG port finalization", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_build_runs \
                             SET dag_finalize_phase = 'ports', dag_finalize_mode = '', \
                                 dag_finalize_cursor = '' \
                             WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                               AND status = 'building' \
                               AND dag_finalize_phase = 'traversal'",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn copy_port_batch(&self, state: &FinalizeStateRow) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let cursor_mode = state.dag_finalize_mode.clone();
        let cursor_key = state.dag_finalize_cursor.clone();
        self.database
            .with_write_connection("copy DAG port finalization batch", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let rows = diesel::sql_query(
                            "SELECT mode, edge_key, source_id, target_id, \
                                    source_slot, target_slot \
                             FROM console_graph_build_edge_ports \
                             WHERE run_id = ? AND active = 1 \
                               AND (mode > ? OR (mode = ? AND edge_key > ?)) \
                             ORDER BY mode, edge_key LIMIT 128",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&cursor_mode)
                        .bind::<Text, _>(&cursor_mode)
                        .bind::<Text, _>(&cursor_key)
                        .load::<PortCopyRow>(connection)?;
                        if rows.is_empty() {
                            let updated = diesel::sql_query(
                                "UPDATE console_graph_build_runs \
                                 SET dag_finalize_phase = 'labels', \
                                     dag_finalize_mode = '', dag_finalize_cursor = '' \
                                 WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                                   AND status = 'building' \
                                   AND dag_finalize_phase = 'ports' \
                                   AND dag_finalize_mode = ? \
                                   AND dag_finalize_cursor = ?",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&owner_id)
                            .bind::<BigInt, _>(lease_epoch)
                            .bind::<Text, _>(&cursor_mode)
                            .bind::<Text, _>(&cursor_key)
                            .execute(connection)?;
                            if updated != 1 {
                                return Err(diesel::result::Error::NotFound);
                            }
                            return Ok(());
                        }
                        for row in &rows {
                            diesel::sql_query(
                                "INSERT INTO console_graph_edge_ports ( \
                                     generation, mode, edge_key, source_id, target_id, \
                                     source_slot, target_slot \
                                 ) VALUES (?, ?, ?, ?, ?, ?, ?) \
                                 ON CONFLICT(generation, mode, edge_key) DO UPDATE SET \
                                     source_id = excluded.source_id, \
                                     target_id = excluded.target_id, \
                                     source_slot = excluded.source_slot, \
                                     target_slot = excluded.target_slot",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&row.mode)
                            .bind::<Text, _>(&row.edge_key)
                            .bind::<Text, _>(&row.source_id)
                            .bind::<Text, _>(&row.target_id)
                            .bind::<Integer, _>(row.source_slot)
                            .bind::<Integer, _>(row.target_slot)
                            .execute(connection)?;
                        }
                        let last = rows
                            .last()
                            .expect("non-empty port batch must have a last row");
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_build_runs \
                             SET dag_finalize_mode = ?, dag_finalize_cursor = ? \
                             WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                               AND status = 'building' AND dag_finalize_phase = 'ports' \
                               AND dag_finalize_mode = ? AND dag_finalize_cursor = ?",
                        )
                        .bind::<Text, _>(&last.mode)
                        .bind::<Text, _>(&last.edge_key)
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<Text, _>(&cursor_mode)
                        .bind::<Text, _>(&cursor_key)
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn checkpoint_finalize_phase(
        &self,
        current_phase: &str,
        next_phase: &str,
    ) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let current_phase = current_phase.to_owned();
        let next_phase = next_phase.to_owned();
        self.database
            .with_write_connection("checkpoint DAG finalization phase", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_build_runs SET dag_finalize_phase = ? \
                             WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                               AND status = 'building' AND dag_finalize_phase = ?",
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
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn apply_branch_label_batch(&self, state: &FinalizeStateRow) -> crate::Result<()> {
        ensure!(
            state.dag_finalize_mode.is_empty(),
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "dag_finalize_mode",
                value: state.dag_finalize_mode.clone(),
            }
        );
        let Some(candidate) = self
            .next_projected_branch(&state.dag_finalize_cursor)
            .await?
        else {
            return self
                .checkpoint_label_progress(&state.dag_finalize_cursor, None)
                .await;
        };
        let Some(branch) = candidate.branch else {
            return self
                .checkpoint_label_progress(
                    &state.dag_finalize_cursor,
                    Some(&candidate.inspected_cursor),
                )
                .await;
        };
        let resolution = self.ensure_branch_label_resolution(&branch).await?;
        if resolution.phase == "ancestry" {
            self.advance_branch_label_resolution(&branch.name, &resolution)
                .await?;
            return Ok(());
        }
        ensure!(
            resolution.phase == "ready",
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "branch_label_resolution_phase",
                value: resolution.phase,
            }
        );

        let branch_state = serde_json::from_str::<SessionState>(&branch.state_json).context(
            crate::error::ParseGraphSnapshotStoreValueSnafu {
                column: "state_json",
            },
        )?;
        let label = graph_branch_label(&branch.name, &branch_state);
        let processed_head = self
            .work_node(&branch.head_id)
            .await?
            .is_some_and(|row| row.processed == 1);
        let reused_head = self.kind == IncrementalBuildKind::Append
            && self.slot(GraphMode::All, &branch.head_id).await?.is_some();
        if branch.head_id != self.root_id && (processed_head || reused_head) {
            self.stage_branch_label_assignment(
                &branch.name,
                GraphMode::All,
                &branch.head_id,
                &label,
            )
            .await?;
        }
        if let Some(anchor_id) = resolution.anchor_id {
            self.stage_branch_label_assignment(
                &branch.name,
                GraphMode::Anchors,
                &anchor_id,
                &label,
            )
            .await?;
        }
        self.checkpoint_label_progress(
            &state.dag_finalize_cursor,
            Some(&candidate.inspected_cursor),
        )
        .await
    }

    async fn next_projected_branch(
        &self,
        cursor: &str,
    ) -> crate::Result<Option<ProjectedBranchCandidate>> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let cursor = cursor.to_owned();
        let append = self.kind == IncrementalBuildKind::Append;
        self.database
            .with_connection(move |connection| {
                let result = (|| -> QueryResult<_> {
                    if append {
                        let name = diesel::sql_query(
                            "SELECT branch_name AS value \
                             FROM console_graph_build_changed_branches \
                             WHERE run_id = ? AND branch_name > ? \
                             ORDER BY branch_name LIMIT 1",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&cursor)
                        .get_result::<TextValueRow>(connection)
                        .optional()?;
                        let Some(name) = name else {
                            return Ok(None);
                        };
                        let branch = diesel::sql_query(
                            "SELECT branch_name AS name, head_id, state_json \
                             FROM console_graph_build_source_manifest \
                             WHERE run_id = ? AND branch_name = ?",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&name.value)
                        .get_result::<ProjectedBranchRow>(connection)
                        .optional()?;
                        Ok(Some(ProjectedBranchCandidate {
                            inspected_cursor: name.value,
                            branch,
                        }))
                    } else {
                        let branch = diesel::sql_query(
                            "SELECT branch_name AS name, head_id, state_json \
                             FROM console_graph_build_source_manifest \
                             WHERE run_id = ? AND branch_name > ? \
                             ORDER BY branch_name LIMIT 1",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&cursor)
                        .get_result::<ProjectedBranchRow>(connection)
                        .optional()?;
                        Ok(branch.map(|branch| ProjectedBranchCandidate {
                            inspected_cursor: branch.name.clone(),
                            branch: Some(branch),
                        }))
                    }
                })();
                result.context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn ensure_branch_label_resolution(
        &self,
        branch: &ProjectedBranchRow,
    ) -> crate::Result<BranchLabelResolutionRow> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let baseline_generation = self.baseline_generation;
        let append = self.kind == IncrementalBuildKind::Append;
        let branch_name = branch.name.clone();
        let head_id = branch.head_id.clone();
        self.database
            .with_write_connection(
                "initialize DAG branch label resolution",
                move |connection| {
                    connection
                        .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                            require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                            diesel::sql_query(
                            "INSERT OR IGNORE INTO console_graph_build_branch_label_resolutions ( \
                                 run_id, branch_name, current_node_id, phase \
                             ) VALUES (?, ?, ?, CASE WHEN EXISTS ( \
                                 SELECT 1 FROM console_graph_anchor_scopes \
                                 WHERE generation = ? AND branch_name = ? AND node_id = ? \
                             ) OR (? = 1 AND NOT EXISTS ( \
                                 SELECT 1 FROM console_graph_build_changed_branches \
                                 WHERE run_id = ? AND branch_name = ? \
                                   AND change_kind = 'replace' \
                             ) AND EXISTS ( \
                                 SELECT 1 FROM console_graph_anchor_scopes \
                                 WHERE generation = ? AND branch_name = ? AND node_id = ? \
                             )) THEN 'ancestry' ELSE 'ready' END)",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&branch_name)
                        .bind::<Text, _>(&head_id)
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&branch_name)
                        .bind::<Text, _>(&head_id)
                        .bind::<Integer, _>(i32::from(append))
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&branch_name)
                        .bind::<BigInt, _>(baseline_generation)
                        .bind::<Text, _>(&branch_name)
                        .bind::<Text, _>(&head_id)
                        .execute(connection)?;
                            diesel::sql_query(
                                "SELECT current_node_id, anchor_id, phase \
                             FROM console_graph_build_branch_label_resolutions \
                             WHERE run_id = ? AND branch_name = ?",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&branch_name)
                            .get_result::<BranchLabelResolutionRow>(connection)
                        })
                        .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
                },
            )
            .await
    }

    async fn advance_branch_label_resolution(
        &self,
        branch_name: &str,
        state: &BranchLabelResolutionRow,
    ) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let baseline_generation = self.baseline_generation;
        let append = self.kind == IncrementalBuildKind::Append;
        let branch_name = branch_name.to_owned();
        let current_node_id = state.current_node_id.clone();
        self.database
            .with_write_connection("advance DAG branch label ancestry", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let first_visit = diesel::sql_query(
                            "INSERT OR IGNORE INTO console_graph_build_branch_label_visited \
                                 (run_id, branch_name, node_id) VALUES (?, ?, ?)",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&branch_name)
                        .bind::<Text, _>(&current_node_id)
                        .execute(connection)?
                            == 1;
                        if !first_visit {
                            return checkpoint_branch_label_ancestry(
                                connection,
                                run_id,
                                &branch_name,
                                &current_node_id,
                                "",
                                None,
                                "ready",
                            );
                        }

                        let node = diesel::sql_query(
                            "SELECT parent_id, \
                                    CASE WHEN json_type(node_json, '$.kind.Anchor') = 'object' \
                                         THEN 1 ELSE 0 END AS is_anchor \
                             FROM console_graph_source_nodes WHERE node_id = ?",
                        )
                        .bind::<Text, _>(&current_node_id)
                        .get_result::<BranchAncestryNodeRow>(connection)?;
                        if node.is_anchor == 1 {
                            return checkpoint_branch_label_ancestry(
                                connection,
                                run_id,
                                &branch_name,
                                &current_node_id,
                                &current_node_id,
                                Some(&current_node_id),
                                "ready",
                            );
                        }

                        let parent_is_in_scope = !node.parent_id.is_empty()
                            && diesel::sql_query(
                                "SELECT CASE WHEN EXISTS ( \
                                     SELECT 1 FROM console_graph_anchor_scopes \
                                     WHERE generation = ? AND branch_name = ? AND node_id = ? \
                                 ) OR (? = 1 AND NOT EXISTS ( \
                                     SELECT 1 FROM console_graph_build_changed_branches \
                                     WHERE run_id = ? AND branch_name = ? \
                                       AND change_kind = 'replace' \
                                 ) AND EXISTS ( \
                                     SELECT 1 FROM console_graph_anchor_scopes \
                                     WHERE generation = ? AND branch_name = ? AND node_id = ? \
                                 )) THEN 1 ELSE 0 END AS value",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&branch_name)
                            .bind::<Text, _>(&node.parent_id)
                            .bind::<Integer, _>(i32::from(append))
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&branch_name)
                            .bind::<BigInt, _>(baseline_generation)
                            .bind::<Text, _>(&branch_name)
                            .bind::<Text, _>(&node.parent_id)
                            .get_result::<IntegerRow>(connection)?
                            .value
                                == 1;
                        checkpoint_branch_label_ancestry(
                            connection,
                            run_id,
                            &branch_name,
                            &current_node_id,
                            if parent_is_in_scope {
                                &node.parent_id
                            } else {
                                ""
                            },
                            None,
                            if parent_is_in_scope {
                                "ancestry"
                            } else {
                                "ready"
                            },
                        )
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn stage_branch_label_assignment(
        &self,
        branch_name: &str,
        mode: GraphMode,
        node_id: &str,
        label: &str,
    ) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let branch_name = branch_name.to_owned();
        let mode = mode.as_query_value().to_owned();
        let node_id = node_id.to_owned();
        let label = label.to_owned();
        self.database
            .with_write_connection("stage graph branch label assignment", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_branch_label_assignments ( \
                                 generation, branch_name, mode, node_id, label \
                             ) VALUES (?, ?, ?, ?, ?) \
                             ON CONFLICT(generation, branch_name, mode) DO UPDATE SET \
                                 node_id = excluded.node_id, label = excluded.label",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(branch_name)
                        .bind::<Text, _>(&mode)
                        .bind::<Text, _>(&node_id)
                        .bind::<Text, _>(label)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT OR IGNORE INTO \
                                 console_graph_build_label_affected_nodes \
                                 (run_id, mode, node_id) VALUES (?, ?, ?)",
                        )
                        .bind::<BigInt, _>(run_id)
                        .bind::<Text, _>(&mode)
                        .bind::<Text, _>(&node_id)
                        .execute(connection)
                        .map(|_| ())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn checkpoint_label_progress(
        &self,
        expected_cursor: &str,
        next_cursor: Option<&str>,
    ) -> crate::Result<()> {
        let path = self.path.clone();
        let run_id = self.run_id;
        let owner_id = self.owner_id.clone();
        let lease_epoch = self.lease_epoch;
        let expected_cursor = expected_cursor.to_owned();
        let next_cursor = next_cursor.map(str::to_owned);
        self.database
            .with_write_connection("checkpoint DAG branch label batch", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_build_fence(connection, run_id, &owner_id, lease_epoch)?;
                        let updated = if let Some(next_cursor) = next_cursor {
                            diesel::sql_query(
                                "UPDATE console_graph_build_runs \
                                 SET dag_finalize_cursor = ? \
                                 WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                                   AND status = 'building' \
                                   AND dag_finalize_phase = 'labels' \
                                   AND dag_finalize_mode = '' \
                                   AND dag_finalize_cursor = ?",
                            )
                            .bind::<Text, _>(next_cursor)
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&owner_id)
                            .bind::<BigInt, _>(lease_epoch)
                            .bind::<Text, _>(&expected_cursor)
                            .execute(connection)?
                        } else {
                            diesel::sql_query(
                                "UPDATE console_graph_build_runs \
                                 SET dag_finalize_phase = 'anchors', \
                                     dag_finalize_cursor = '' \
                                 WHERE run_id = ? AND owner_id = ? AND lease_epoch = ? \
                                   AND status = 'building' \
                                   AND dag_finalize_phase = 'labels' \
                                   AND dag_finalize_mode = '' \
                                   AND dag_finalize_cursor = ?",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<Text, _>(&owner_id)
                            .bind::<BigInt, _>(lease_epoch)
                            .bind::<Text, _>(&expected_cursor)
                            .execute(connection)?
                        };
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }
}

fn checkpoint_branch_label_ancestry(
    connection: &mut SqliteConnection,
    run_id: i64,
    branch_name: &str,
    expected_current_node_id: &str,
    next_current_node_id: &str,
    anchor_id: Option<&str>,
    next_phase: &str,
) -> QueryResult<()> {
    let updated = diesel::sql_query(
        "UPDATE console_graph_build_branch_label_resolutions \
         SET current_node_id = ?, anchor_id = ?, phase = ? \
         WHERE run_id = ? AND branch_name = ? AND phase = 'ancestry' \
           AND current_node_id = ?",
    )
    .bind::<Text, _>(next_current_node_id)
    .bind::<Nullable<Text>, _>(anchor_id)
    .bind::<Text, _>(next_phase)
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(branch_name)
    .bind::<Text, _>(expected_current_node_id)
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(())
}

fn mark_anchor_raw_edge_complete(
    connection: &mut SqliteConnection,
    run_id: i64,
    target_id: &str,
    edge: &AnchorRawEdgeRow,
) -> QueryResult<()> {
    let updated = diesel::sql_query(
        "UPDATE console_graph_build_anchor_raw_edges SET complete = 1 \
         WHERE run_id = ? AND target_id = ? AND edge_order = ? AND complete = 0 \
           AND current_ancestor_id = ? AND ancestor_depth = ?",
    )
    .bind::<BigInt, _>(run_id)
    .bind::<Text, _>(target_id)
    .bind::<Integer, _>(edge.edge_order)
    .bind::<Text, _>(&edge.current_ancestor_id)
    .bind::<Integer, _>(edge.ancestor_depth)
    .execute(connection)?;
    if updated != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    Ok(())
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
    use std::collections::{BTreeMap, BTreeSet};

    use coco_mem::{
        Anchor, BranchStore, Kind, MergeParent, NewNode, NodeStore, PromptAnchor, Role,
        SessionAnchor, SessionRole, SkillInvocationAnchor, SkillInvocationMode,
        SkillRuntimeContext, SqliteGraphStore, SqliteStore, ToolUse,
    };
    use diesel::connection::SimpleConnection;
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

    #[derive(Debug, PartialEq, Eq, QueryableByName)]
    struct ParentRequirementCheckpoint {
        #[diesel(sql_type = Integer)]
        remaining_parents: i32,
        #[diesel(sql_type = Text)]
        parent_requirement_cursor: String,
        #[diesel(sql_type = Integer)]
        parent_requirements_complete: i32,
        #[diesel(sql_type = BigInt)]
        discovered_node_count: i64,
    }

    #[derive(Debug, PartialEq, Eq, QueryableByName)]
    struct ParentRequirementTransactionCheckpoint {
        #[diesel(sql_type = BigInt)]
        work_node_count: i64,
        #[diesel(sql_type = BigInt)]
        discovered_node_count: i64,
    }

    #[derive(Debug, PartialEq, Eq, QueryableByName)]
    struct ChildPageTransactionCheckpoint {
        #[diesel(sql_type = Integer)]
        remaining_parents: i32,
        #[diesel(sql_type = BigInt)]
        satisfaction_count: i64,
        #[diesel(sql_type = Text)]
        next_child_id: String,
        #[diesel(sql_type = Integer)]
        complete: i32,
    }

    #[derive(Debug, PartialEq, Eq, QueryableByName)]
    struct ScopeRemovalCheckpoint {
        #[diesel(sql_type = Text)]
        scope_removal_cursor: String,
        #[diesel(sql_type = Integer)]
        scope_removal_complete: i32,
        #[diesel(sql_type = BigInt)]
        tombstone_count: i64,
    }

    #[derive(Debug, PartialEq, Eq, QueryableByName)]
    struct DeltaRemovalCheckpoint {
        #[diesel(sql_type = Text)]
        removal_cursor: String,
        #[diesel(sql_type = BigInt)]
        removal_refresh_id_cursor: i64,
        #[diesel(sql_type = BigInt)]
        removal_refresh_id_upper_bound: i64,
        #[diesel(sql_type = Integer)]
        removal_bound_frozen: i32,
        #[diesel(sql_type = Integer)]
        removal_complete: i32,
        #[diesel(sql_type = BigInt)]
        tombstone_count: i64,
    }

    #[derive(Debug, QueryableByName)]
    struct DeltaStagingCounts {
        #[diesel(sql_type = BigInt)]
        materialized_nodes: i64,
        #[diesel(sql_type = BigInt)]
        materialized_edges: i64,
        #[diesel(sql_type = BigInt)]
        rank_slots: i64,
        #[diesel(sql_type = BigInt)]
        edge_ports: i64,
        #[diesel(sql_type = BigInt)]
        work_nodes: i64,
        #[diesel(sql_type = BigInt)]
        source_branches: i64,
        #[diesel(sql_type = BigInt)]
        source_refreshes: i64,
        #[diesel(sql_type = BigInt)]
        delta_nodes: i64,
    }

    #[derive(Debug, PartialEq, Eq, QueryableByName)]
    struct TickParityRow {
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

    #[derive(Debug, PartialEq, Eq, QueryableByName)]
    struct DiamondDedupCheckpoint {
        #[diesel(sql_type = BigInt)]
        discovered_nodes: i64,
        #[diesel(sql_type = BigInt)]
        processed_nodes: i64,
        #[diesel(sql_type = BigInt)]
        child_work_rows: i64,
        #[diesel(sql_type = BigInt)]
        child_seen_rows: i64,
        #[diesel(sql_type = BigInt)]
        child_pending_rows: i64,
        #[diesel(sql_type = Integer)]
        child_processed: i32,
        #[diesel(sql_type = Integer)]
        child_frontier_enqueued: i32,
    }

    #[derive(Debug, QueryableByName)]
    struct QueryPlanDetailRow {
        #[diesel(sql_type = Text)]
        detail: String,
    }

    #[derive(Debug, QueryableByName)]
    struct CountAndValueRow {
        #[diesel(sql_type = BigInt)]
        count: i64,
        #[diesel(sql_type = BigInt)]
        value: i64,
    }

    #[derive(Debug, QueryableByName)]
    struct TestManifestBranchRow {
        #[diesel(sql_type = Text)]
        name: String,
        #[diesel(sql_type = Text)]
        head_id: String,
        #[diesel(sql_type = Text)]
        state_json: String,
        #[diesel(sql_type = BigInt)]
        contribution_generation: i64,
    }

    #[derive(Debug, PartialEq, Eq, QueryableByName)]
    struct JournalResetCheckpoint {
        #[diesel(sql_type = BigInt)]
        revision_cursor: i64,
        #[diesel(sql_type = Text)]
        branch_cursor: String,
        #[diesel(sql_type = BigInt)]
        processed_changes: i64,
        #[diesel(sql_type = BigInt)]
        branch_deltas: i64,
        #[diesel(sql_type = BigInt)]
        refreshes: i64,
    }

    async fn journal_reset_checkpoint(
        snapshots: &ConsoleGraphSnapshotStore,
        run_id: i64,
    ) -> JournalResetCheckpoint {
        snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT dag_init_row_cursor AS revision_cursor, \
                            dag_init_text_cursor AS branch_cursor, \
                            dag_init_counter AS processed_changes, \
                            (SELECT COUNT(*) \
                             FROM console_graph_build_branch_deltas AS delta \
                             WHERE delta.run_id = run.run_id) AS branch_deltas, \
                            (SELECT COUNT(*) \
                             FROM console_graph_build_source_refresh_manifest AS refresh \
                             WHERE refresh.run_id = run.run_id) AS refreshes \
                     FROM console_graph_build_runs AS run WHERE run_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .get_result::<JournalResetCheckpoint>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("journal-reset-checkpoint"),
                })
            })
            .await
            .unwrap()
    }

    async fn build_refresh_manifest_count(
        snapshots: &ConsoleGraphSnapshotStore,
        run_id: i64,
    ) -> i64 {
        snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count \
                     FROM console_graph_build_source_refresh_manifest WHERE run_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .get_result::<CountRow>(connection)
                .map(|row| row.count)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("build-refresh-manifest-count"),
                })
            })
            .await
            .unwrap()
    }

    async fn delta_removal_checkpoint(
        snapshots: &ConsoleGraphSnapshotStore,
        run_id: i64,
        branch_name: &str,
    ) -> DeltaRemovalCheckpoint {
        let branch_name = branch_name.to_owned();
        snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT changed.removal_cursor, \
                            changed.removal_refresh_id_cursor, \
                            changed.removal_refresh_id_upper_bound, \
                            changed.removal_bound_frozen, changed.removal_complete, \
                            (SELECT COUNT(*) \
                             FROM console_graph_build_node_tombstones AS removed \
                             WHERE removed.run_id = changed.run_id) AS tombstone_count \
                     FROM console_graph_build_changed_branches AS changed \
                     WHERE changed.run_id = ? AND changed.branch_name = ?",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(branch_name)
                .get_result::<DeltaRemovalCheckpoint>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("delta-removal-checkpoint"),
                })
            })
            .await
            .unwrap()
    }

    #[test]
    fn source_change_journal_rejects_missing_middle_revision() {
        assert!(!journal_page_is_contiguous(40, 0, [41, 43]));
        assert!(!journal_page_is_contiguous(40, 41, [43]));
        assert!(journal_page_is_contiguous(40, 0, [41, 41, 42]));
        assert!(journal_page_is_contiguous(40, 41, [41, 42]));
    }

    #[test]
    fn source_change_journal_rejects_missing_tail_revision() {
        assert!(!journal_cursor_covers_revision_range(40, 43, 42));
        assert!(!journal_cursor_covers_revision_range(40, 43, 0));
        assert!(journal_cursor_covers_revision_range(40, 40, 0));
        assert!(journal_cursor_covers_revision_range(40, 43, 43));
    }

    #[tokio::test]
    async fn same_branch_journal_reset_is_bounded_and_resumes_across_pages() {
        const STAGED_ROW_COUNT: i64 = 300;
        const SOURCE_VERSION: u64 = 2;

        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let head = append_text(&writer, &root, "journal reset branch").await;
        let branch_name = "journal-reset";
        writer.fork(branch_name, &head).await.unwrap();

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

        let baseline_generation = snapshots.active_generation().await.unwrap();
        let branch_name_for_seed = branch_name.to_owned();
        let baseline_revision = snapshots
            .with_connection(move |connection| {
                (|| -> QueryResult<_> {
                    let baseline_revision = diesel::sql_query(
                        "SELECT source_revision AS value \
                         FROM console_graph_generation_source_revisions \
                         WHERE generation = ?",
                    )
                    .bind::<BigInt, _>(baseline_generation)
                    .get_result::<BigIntRow>(connection)?
                    .value;
                    let branch = diesel::sql_query(
                        "SELECT name, head_id, state_json, contribution_generation \
                         FROM console_graph_materialization_branches \
                         WHERE generation = ? AND name = ?",
                    )
                    .bind::<BigInt, _>(baseline_generation)
                    .bind::<Text, _>(&branch_name_for_seed)
                    .get_result::<TestManifestBranchRow>(connection)?;
                    for offset in 1_i64..=3 {
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_change_journal ( \
                                 source_revision, target_invalidation_incarnation, \
                                 target_invalidation_version, branch_name, change_kind, \
                                 refresh_id, base_contribution_generation, \
                                 target_contribution_generation, head_id, state_json \
                             ) VALUES (?, 'journal-reset', ?, ?, 'metadata', NULL, \
                                 NULL, ?, ?, ?)",
                        )
                        .bind::<BigInt, _>(baseline_revision + offset)
                        .bind::<BigInt, _>(offset)
                        .bind::<Text, _>(&branch.name)
                        .bind::<BigInt, _>(branch.contribution_generation)
                        .bind::<Text, _>(&branch.head_id)
                        .bind::<Text, _>(&branch.state_json)
                        .execute(connection)?;
                    }
                    diesel::sql_query(
                        "INSERT INTO console_graph_source_branch_change_journal ( \
                             source_revision, target_invalidation_incarnation, \
                             target_invalidation_version, branch_name, change_kind, \
                             refresh_id, base_contribution_generation, \
                             target_contribution_generation, head_id, state_json \
                         ) VALUES (?, 'journal-reset', 4, ?, 'delete', NULL, ?, \
                             NULL, NULL, NULL)",
                    )
                    .bind::<BigInt, _>(baseline_revision + 4)
                    .bind::<Text, _>(&branch.name)
                    .bind::<BigInt, _>(branch.contribution_generation)
                    .execute(connection)?;
                    diesel::sql_query(
                        "UPDATE console_graph_source_identity SET revision = ? WHERE id = 1",
                    )
                    .bind::<BigInt, _>(baseline_revision + 4)
                    .execute(connection)?;
                    Ok(baseline_revision)
                })()
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("journal-reset-fixture"),
                })
            })
            .await
            .unwrap();

        let lease = snapshots
            .acquire_incremental_build_lease(SOURCE_VERSION)
            .await
            .unwrap()
            .unwrap();
        let run_id = lease.generation();
        let mut store = IncrementalWorkStore::new(
            snapshots.database(),
            root.clone(),
            baseline_generation,
            run_id,
            lease.owner_id().to_owned(),
            lease.lease_epoch(),
            DEFAULT_CHILD_PAGE_SIZE,
        );
        let state = store.dag_run_state().await.unwrap();
        assert_eq!(state.dag_init_phase, "new");
        store.initialize_source_manifest(&state).await.unwrap();
        store.choose_build_kind().await.unwrap();
        for offset in 1_i64..=3 {
            store.choose_build_kind().await.unwrap();
            let checkpoint = journal_reset_checkpoint(&snapshots, run_id).await;
            assert_eq!(checkpoint.revision_cursor, baseline_revision + offset);
            assert_eq!(checkpoint.branch_cursor, branch_name);
            assert_eq!(checkpoint.processed_changes, offset);
        }

        let branch_name_for_rows = branch_name.to_owned();
        snapshots
            .with_connection(move |connection| {
                (|| -> QueryResult<()> {
                    diesel::sql_query(
                        "WITH RECURSIVE ids(value) AS ( \
                             SELECT 1 UNION ALL SELECT value + 1 FROM ids WHERE value < ? \
                         ) \
                         INSERT INTO console_graph_build_branch_deltas ( \
                             run_id, branch_name, published_source_revision, refresh_id, \
                             refresh_kind \
                         ) SELECT ?, ?, 1000000 + value, 2000000 + value, 'append' FROM ids",
                    )
                    .bind::<BigInt, _>(STAGED_ROW_COUNT)
                    .bind::<BigInt, _>(run_id)
                    .bind::<Text, _>(&branch_name_for_rows)
                    .execute(connection)?;
                    diesel::sql_query(
                        "WITH RECURSIVE ids(value) AS ( \
                             SELECT 1 UNION ALL SELECT value + 1 FROM ids WHERE value < ? \
                         ) \
                         INSERT INTO console_graph_build_source_refresh_manifest ( \
                             run_id, branch_name, refresh_id \
                         ) SELECT ?, ?, 2000000 + value FROM ids",
                    )
                    .bind::<BigInt, _>(STAGED_ROW_COUNT)
                    .bind::<BigInt, _>(run_id)
                    .bind::<Text, _>(&branch_name_for_rows)
                    .execute(connection)?;
                    Ok(())
                })()
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("journal-reset-staged-rows"),
                })
            })
            .await
            .unwrap();
        let staged = journal_reset_checkpoint(&snapshots, run_id).await;
        assert_eq!(staged.branch_deltas, STAGED_ROW_COUNT);
        assert_eq!(staged.refreshes, STAGED_ROW_COUNT);

        store.choose_build_kind().await.unwrap();
        let first_reset = journal_reset_checkpoint(&snapshots, run_id).await;
        assert_eq!(
            first_reset.branch_deltas,
            STAGED_ROW_COUNT - BRANCH_CHANGE_RESET_BATCH_SIZE
        );
        assert_eq!(first_reset.refreshes, STAGED_ROW_COUNT);
        assert_eq!(first_reset.revision_cursor, baseline_revision + 3);

        assert!(snapshots.pause_incremental_build(&lease).await.unwrap());
        drop(store);
        let resumed_lease = snapshots
            .acquire_incremental_build_lease(SOURCE_VERSION)
            .await
            .unwrap()
            .unwrap();
        assert!(resumed_lease.is_resumed());
        assert_eq!(resumed_lease.generation(), run_id);
        let mut resumed = IncrementalWorkStore::new(
            snapshots.database(),
            root,
            baseline_generation,
            run_id,
            resumed_lease.owner_id().to_owned(),
            resumed_lease.lease_epoch(),
            DEFAULT_CHILD_PAGE_SIZE,
        );

        for _ in 0..16 {
            let before = journal_reset_checkpoint(&snapshots, run_id).await;
            if before.revision_cursor == baseline_revision + 4 {
                break;
            }
            resumed.choose_build_kind().await.unwrap();
            let after = journal_reset_checkpoint(&snapshots, run_id).await;
            let removed =
                before.branch_deltas + before.refreshes - after.branch_deltas - after.refreshes;
            if after.revision_cursor == before.revision_cursor {
                assert!(removed > 0);
                assert!(removed <= BRANCH_CHANGE_RESET_BATCH_SIZE);
            } else {
                assert_eq!(removed, 0);
                assert_eq!(after.revision_cursor, baseline_revision + 4);
            }
        }

        let completed = journal_reset_checkpoint(&snapshots, run_id).await;
        assert_eq!(completed.revision_cursor, baseline_revision + 4);
        assert_eq!(completed.branch_cursor, branch_name);
        assert_eq!(completed.processed_changes, 4);
        assert_eq!(completed.branch_deltas, 0);
        assert_eq!(completed.refreshes, 0);
        let branch_name_for_result = branch_name.to_owned();
        snapshots
            .with_connection(move |connection| {
                (|| -> QueryResult<()> {
                    let change_kind = diesel::sql_query(
                        "SELECT change_kind AS value \
                         FROM console_graph_build_changed_branches \
                         WHERE run_id = ? AND branch_name = ?",
                    )
                    .bind::<BigInt, _>(run_id)
                    .bind::<Text, _>(&branch_name_for_result)
                    .get_result::<TextValueRow>(connection)?
                    .value;
                    assert_eq!(change_kind, "delete");
                    let manifest_rows = diesel::sql_query(
                        "SELECT COUNT(*) AS value \
                         FROM console_graph_build_source_manifest \
                         WHERE run_id = ? AND branch_name = ?",
                    )
                    .bind::<BigInt, _>(run_id)
                    .bind::<Text, _>(&branch_name_for_result)
                    .get_result::<BigIntRow>(connection)?
                    .value;
                    assert_eq!(manifest_rows, 0);
                    Ok(())
                })()
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("journal-reset-result"),
                })
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn layout_slot_max_queries_use_composite_index_searches() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let (rank_plan, source_port_plan, target_port_plan) = snapshots
            .with_connection(|connection| {
                (|| -> QueryResult<_> {
                    let rank_plan = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                     SELECT MAX( \
                         COALESCE(( \
                             SELECT MAX(row) FROM console_graph_build_rank_slots \
                             WHERE run_id = ? AND mode = ? AND rank = ? \
                         ), -1), \
                         COALESCE(( \
                             SELECT MAX(sort_order) FROM console_graph_node_locations \
                             WHERE generation = ? AND mode = ? AND rank = ? \
                         ), -1) \
                     ) + 1 AS value",
                    )
                    .bind::<BigInt, _>(1_i64)
                    .bind::<Text, _>("all")
                    .bind::<Integer, _>(1_i32)
                    .bind::<BigInt, _>(0_i64)
                    .bind::<Text, _>("all")
                    .bind::<Integer, _>(1_i32)
                    .load::<QueryPlanDetailRow>(connection)?;
                    let source_port_plan = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                     SELECT MAX( \
                         COALESCE(( \
                             SELECT MAX(source_slot) \
                             FROM console_graph_build_edge_ports \
                             WHERE run_id = ? AND mode = ? AND source_id = ? \
                         ), -1), \
                         COALESCE(( \
                             SELECT MAX(source_slot) FROM console_graph_edge_ports \
                             WHERE generation = ? AND mode = ? AND source_id = ? \
                         ), -1) \
                     ) + 1 AS value",
                    )
                    .bind::<BigInt, _>(1_i64)
                    .bind::<Text, _>("all")
                    .bind::<Text, _>("node")
                    .bind::<BigInt, _>(0_i64)
                    .bind::<Text, _>("all")
                    .bind::<Text, _>("node")
                    .load::<QueryPlanDetailRow>(connection)?;
                    let target_port_plan = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                     SELECT MAX( \
                         COALESCE(( \
                             SELECT MAX(target_slot) \
                             FROM console_graph_build_edge_ports \
                             WHERE run_id = ? AND mode = ? AND target_id = ? \
                         ), -1), \
                         COALESCE(( \
                             SELECT MAX(target_slot) FROM console_graph_edge_ports \
                             WHERE generation = ? AND mode = ? AND target_id = ? \
                         ), -1) \
                     ) + 1 AS value",
                    )
                    .bind::<BigInt, _>(1_i64)
                    .bind::<Text, _>("all")
                    .bind::<Text, _>("node")
                    .bind::<BigInt, _>(0_i64)
                    .bind::<Text, _>("all")
                    .bind::<Text, _>("node")
                    .load::<QueryPlanDetailRow>(connection)?;
                    Ok((rank_plan, source_port_plan, target_port_plan))
                })()
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("layout-slot-max-query-plan"),
                })
            })
            .await
            .unwrap();

        let assert_index_search = |plan: &[QueryPlanDetailRow], table: &str| {
            let detail = plan
                .iter()
                .find(|row| row.detail.contains(table))
                .unwrap_or_else(|| panic!("missing query-plan row for {table}: {plan:?}"));
            assert!(detail.detail.contains("SEARCH"), "{detail:?}");
            assert!(detail.detail.contains("INDEX"), "{detail:?}");
            assert!(!detail.detail.contains("SCAN"), "{detail:?}");
        };
        assert_index_search(&rank_plan, "console_graph_build_rank_slots");
        assert_index_search(&rank_plan, "console_graph_node_locations");
        assert_index_search(&source_port_plan, "console_graph_build_edge_ports");
        assert_index_search(&source_port_plan, "console_graph_edge_ports");
        assert_index_search(&target_port_plan, "console_graph_build_edge_ports");
        assert_index_search(&target_port_plan, "console_graph_edge_ports");
    }

    #[tokio::test]
    async fn source_manifest_keeps_the_revision_frozen_by_lease_acquisition() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let lease = snapshots
            .acquire_incremental_build_lease(1)
            .await
            .unwrap()
            .unwrap();
        let frozen_source_revision = lease.frozen_source_revision();
        snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_source_identity SET revision = ? WHERE id = 1",
                )
                .bind::<BigInt, _>(frozen_source_revision + 1)
                .execute(connection)
                .map(|_| ())
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("lease-frozen-source-revision"),
                })
            })
            .await
            .unwrap();

        let store = IncrementalWorkStore::new(
            snapshots.database(),
            writer.root_id(),
            snapshots.active_generation().await.unwrap(),
            lease.generation(),
            lease.owner_id().to_owned(),
            lease.lease_epoch(),
            DEFAULT_CHILD_PAGE_SIZE,
        );
        let state = store.dag_run_state().await.unwrap();
        assert_eq!(state.dag_init_phase, "new");
        assert_eq!(state.dag_source_revision, frozen_source_revision);
        store.initialize_source_manifest(&state).await.unwrap();
        let initialized = store.dag_run_state().await.unwrap();
        assert_eq!(initialized.dag_init_phase, "kind");
        assert_eq!(initialized.dag_source_revision, frozen_source_revision);
    }

    #[tokio::test]
    async fn frozen_source_manifest_queries_use_bounded_keyset_indexes() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let plans = snapshots
            .with_connection(|connection| {
                (|| -> QueryResult<_> {
                    let names = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                         SELECT branch_name, first_source_revision \
                         FROM console_graph_source_branch_names \
                         WHERE first_source_revision <= ? \
                           AND (first_source_revision, branch_name) > (?, ?) \
                         ORDER BY first_source_revision, branch_name LIMIT 128",
                    )
                    .bind::<BigInt, _>(500_i64)
                    .bind::<BigInt, _>(100_i64)
                    .bind::<Text, _>("branch")
                    .load::<QueryPlanDetailRow>(connection)?;
                    let history = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                         SELECT branch_name, contribution_generation, head_id, \
                                state_json, removed \
                         FROM console_graph_source_branch_history \
                         WHERE branch_name = ? AND source_revision <= ? \
                         ORDER BY source_revision DESC LIMIT 1",
                    )
                    .bind::<Text, _>("branch")
                    .bind::<BigInt, _>(500_i64)
                    .load::<QueryPlanDetailRow>(connection)?;
                    let refreshes = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                         SELECT branch_name, refresh_id, target_contribution_generation, \
                                published_source_revision \
                         FROM console_graph_source_refresh_runs \
                         WHERE status = 'published' \
                           AND published_source_revision <= ? \
                           AND (published_source_revision, refresh_id) > (?, ?) \
                         ORDER BY published_source_revision, refresh_id LIMIT 128",
                    )
                    .bind::<BigInt, _>(500_i64)
                    .bind::<BigInt, _>(100_i64)
                    .bind::<BigInt, _>(1_i64)
                    .load::<QueryPlanDetailRow>(connection)?;
                    Ok((names, history, refreshes))
                })()
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("frozen-manifest-query-plans"),
                })
            })
            .await
            .unwrap();

        for (plan, index) in [
            (
                plans.0.as_slice(),
                "console_graph_source_branch_names_revision_idx",
            ),
            (
                plans.1.as_slice(),
                "sqlite_autoindex_console_graph_source_branch_history_1",
            ),
            (
                plans.2.as_slice(),
                "console_graph_source_refresh_runs_manifest_idx",
            ),
        ] {
            assert!(
                plan.iter().any(|row| row.detail.contains(index)),
                "missing {index}: {plan:?}",
            );
            assert!(
                plan.iter()
                    .all(|row| !row.detail.contains("USE TEMP B-TREE")),
                "temporary sort for {index}: {plan:?}",
            );
        }
        let names_detail = plans
            .0
            .iter()
            .find(|row| row.detail.contains("console_graph_source_branch_names"))
            .expect("missing branch-name query plan");
        assert!(names_detail.detail.contains("SEARCH"), "{names_detail:?}");
        assert!(!names_detail.detail.contains("SCAN"), "{names_detail:?}");
        assert!(
            names_detail
                .detail
                .contains("(first_source_revision,branch_name)>(?,?)"),
            "branch-name cursor did not become an index range: {names_detail:?}",
        );
    }

    #[tokio::test]
    async fn incremental_candidate_pages_use_bounded_keyset_indexes() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let plans = snapshots
            .with_connection(|connection| {
                (|| -> QueryResult<_> {
                    let manifest = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                         SELECT branch_name, head_id, contribution_generation \
                         FROM console_graph_build_source_manifest \
                         WHERE run_id = ? AND branch_name > ? \
                         ORDER BY branch_name LIMIT 128",
                    )
                    .bind::<BigInt, _>(1_i64)
                    .bind::<Text, _>("branch")
                    .load::<QueryPlanDetailRow>(connection)?;
                    let changed = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                         SELECT branch_name \
                         FROM console_graph_build_changed_branches \
                         WHERE run_id = ? AND branch_name > ? \
                         ORDER BY branch_name LIMIT 128",
                    )
                    .bind::<BigInt, _>(1_i64)
                    .bind::<Text, _>("branch")
                    .load::<QueryPlanDetailRow>(connection)?;
                    let delta = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                         SELECT node_id FROM console_graph_build_delta_nodes \
                         WHERE run_id = ? AND node_id > ? \
                         ORDER BY node_id LIMIT 128",
                    )
                    .bind::<BigInt, _>(1_i64)
                    .bind::<Text, _>("node")
                    .load::<QueryPlanDetailRow>(connection)?;
                    let parents = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                         SELECT parent_id FROM console_graph_source_node_relations \
                         WHERE child_id = ? AND parent_id > ? \
                         ORDER BY parent_id LIMIT 128",
                    )
                    .bind::<Text, _>("child")
                    .bind::<Text, _>("parent")
                    .load::<QueryPlanDetailRow>(connection)?;
                    let children = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                         SELECT child_id FROM console_graph_source_node_relations \
                         WHERE parent_id = ? AND child_id > ? \
                         ORDER BY child_id LIMIT 128",
                    )
                    .bind::<Text, _>("parent")
                    .bind::<Text, _>("child")
                    .load::<QueryPlanDetailRow>(connection)?;
                    let removal = diesel::sql_query(format!(
                        "EXPLAIN QUERY PLAN {DELTA_REMOVAL_CANDIDATE_SQL}"
                    ))
                    .bind::<BigInt, _>(10_i64)
                    .bind::<Text, _>("branch")
                    .bind::<BigInt, _>(2_i64)
                    .bind::<Text, _>("node")
                    .bind::<BigInt, _>(20_i64)
                    .bind::<BigInt, _>(128_i64)
                    .load::<QueryPlanDetailRow>(connection)?;
                    let removal_bound = diesel::sql_query(format!(
                        "EXPLAIN QUERY PLAN {DELTA_REMOVAL_UPPER_BOUND_SQL}"
                    ))
                    .bind::<BigInt, _>(10_i64)
                    .bind::<Text, _>("branch")
                    .load::<QueryPlanDetailRow>(connection)?;
                    Ok((
                        manifest,
                        changed,
                        delta,
                        parents,
                        children,
                        removal,
                        removal_bound,
                    ))
                })()
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("incremental-candidate-query-plans"),
                })
            })
            .await
            .unwrap();

        for (plan, index) in [
            (
                plans.0.as_slice(),
                "sqlite_autoindex_console_graph_build_source_manifest_1",
            ),
            (
                plans.1.as_slice(),
                "sqlite_autoindex_console_graph_build_changed_branches_1",
            ),
            (
                plans.2.as_slice(),
                "console_graph_build_delta_nodes_order_idx",
            ),
            (
                plans.3.as_slice(),
                "console_graph_source_node_relations_child_idx",
            ),
            (
                plans.4.as_slice(),
                "sqlite_autoindex_console_graph_source_node_relations_1",
            ),
            (
                plans.5.as_slice(),
                "sqlite_autoindex_console_graph_source_branch_nodes_1",
            ),
            (
                plans.6.as_slice(),
                "console_graph_source_refresh_runs_manifest_idx",
            ),
        ] {
            assert!(
                plan.iter().any(|row| row.detail.contains(index)),
                "missing {index}: {plan:?}",
            );
            assert!(
                plan.iter()
                    .all(|row| !row.detail.contains("USE TEMP B-TREE")),
                "temporary sort for {index}: {plan:?}",
            );
        }
        let removal_membership = plans
            .5
            .iter()
            .find(|row| row.detail.contains("console_graph_source_branch_nodes"))
            .expect("missing delta-removal membership plan");
        assert!(
            removal_membership.detail.contains("SEARCH"),
            "delta-removal membership is not a range search: {removal_membership:?}",
        );
        assert!(
            removal_membership
                .detail
                .contains("(contribution_generation,node_id)>(?,?)"),
            "delta-removal raw cursor is not a tuple range: {removal_membership:?}",
        );
        assert!(
            plans.5.iter().any(|row| {
                row.detail
                    .contains("sqlite_autoindex_console_graph_build_changed_branches_1")
                    && row.detail.contains("SEARCH")
            }),
            "delta-removal eligibility does not use the frozen branch key: {:?}",
            plans.5,
        );
        assert!(
            plans.5.iter().any(|row| {
                row.detail.contains("console_graph_source_refresh_runs")
                    && row.detail.contains("SEARCH")
            }),
            "delta-removal eligibility does not search frozen baseline refreshes: {:?}",
            plans.5,
        );
    }

    #[tokio::test]
    async fn source_manifest_capture_resumes_at_a_frozen_revision_during_new_mutations() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let root_for_seed = root.clone();
        snapshots
            .with_connection(move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        for index in 0..129 {
                            let branch_name = format!("branch-{index:03}");
                            let refresh_id = 10_000 + index;
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_branch_names ( \
                                     branch_name, first_source_revision \
                                 ) VALUES (?, 1)",
                            )
                            .bind::<Text, _>(&branch_name)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_branch_history ( \
                                     branch_name, source_revision, contribution_generation, \
                                     head_id, state_json, removed \
                                 ) VALUES (?, 1, ?, ?, '{}', 0)",
                            )
                            .bind::<Text, _>(&branch_name)
                            .bind::<BigInt, _>(refresh_id)
                            .bind::<Text, _>(&root_for_seed)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_refresh_runs ( \
                                     refresh_id, branch_name, target_head_id, target_state_json, \
                                     status, target_contribution_generation, \
                                     published_source_revision \
                                 ) VALUES (?, ?, ?, '{}', 'published', ?, 1)",
                            )
                            .bind::<BigInt, _>(refresh_id)
                            .bind::<Text, _>(&branch_name)
                            .bind::<Text, _>(&root_for_seed)
                            .bind::<BigInt, _>(refresh_id)
                            .execute(connection)?;
                        }
                        diesel::sql_query(
                            "UPDATE console_graph_source_identity SET revision = 1 WHERE id = 1",
                        )
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("manifest-pagination-fixture"),
                    })
            })
            .await
            .unwrap();
        let lease = snapshots
            .acquire_incremental_build_lease(1)
            .await
            .unwrap()
            .unwrap();
        let mut store = IncrementalWorkStore::new(
            snapshots.database(),
            root.clone(),
            snapshots.active_generation().await.unwrap(),
            lease.generation(),
            lease.owner_id().to_owned(),
            lease.lease_epoch(),
            128,
        );
        for _ in 0..24 {
            let state = store.dag_run_state().await.unwrap();
            if state.dag_init_phase == "manifest_copy" && state.dag_init_counter == 128 {
                break;
            }
            match state.dag_init_phase.as_str() {
                "new"
                | "full_reset"
                | "manifest_refresh_reset"
                | "manifest_branch_reset"
                | "manifest_copy" => {
                    store.initialize_source_manifest(&state).await.unwrap();
                }
                phase if phase.starts_with("kind") => {
                    store.choose_build_kind().await.unwrap();
                }
                phase => panic!("unexpected source manifest phase {phase}"),
            }
        }
        let checkpoint = store.dag_run_state().await.unwrap();
        assert_eq!(checkpoint.dag_init_phase, "manifest_copy");
        assert_eq!(checkpoint.dag_init_counter, 128);
        assert_eq!(checkpoint.dag_source_revision, 1);
        assert!(snapshots.pause_incremental_build(&lease).await.unwrap());
        drop(store);
        drop(snapshots);

        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        snapshots
            .with_connection(|connection| {
                (|| -> QueryResult<()> {
                    diesel::sql_query(
                        "INSERT INTO console_graph_source_branch_names ( \
                             branch_name, first_source_revision \
                         ) VALUES ('future-branch', 2)",
                    )
                    .execute(connection)?;
                    diesel::sql_query(
                        "INSERT INTO console_graph_source_branch_history ( \
                             branch_name, source_revision, contribution_generation, \
                             head_id, state_json, removed \
                         ) VALUES ('future-branch', 2, 20000, 'future', '{}', 0)",
                    )
                    .execute(connection)?;
                    diesel::sql_query(
                        "INSERT INTO console_graph_source_branch_history ( \
                             branch_name, source_revision, contribution_generation, \
                             head_id, state_json, removed \
                         ) VALUES ('branch-128', 2, 20001, 'future', '{}', 0)",
                    )
                    .execute(connection)?;
                    for revision in 3_i64..=302 {
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_history ( \
                                 branch_name, source_revision, contribution_generation, \
                                 head_id, state_json, removed \
                             ) VALUES ('branch-000', ?, 30000, 'future', '{}', 0)",
                        )
                        .bind::<BigInt, _>(revision)
                        .execute(connection)?;
                    }
                    for index in 0_i64..300 {
                        let branch_name = format!("future-{index:03}");
                        let revision = 303 + index;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_names ( \
                                 branch_name, first_source_revision \
                             ) VALUES (?, ?)",
                        )
                        .bind::<Text, _>(&branch_name)
                        .bind::<BigInt, _>(revision)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_history ( \
                                 branch_name, source_revision, contribution_generation, \
                                 head_id, state_json, removed \
                             ) VALUES (?, ?, 40000, 'future', '{}', 0)",
                        )
                        .bind::<Text, _>(&branch_name)
                        .bind::<BigInt, _>(revision)
                        .execute(connection)?;
                    }
                    diesel::sql_query(
                        "UPDATE console_graph_source_identity SET revision = 602 WHERE id = 1",
                    )
                    .execute(connection)?;
                    Ok(())
                })()
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("manifest-source-revision-change"),
                })
            })
            .await
            .unwrap();
        let resumed_lease = snapshots.acquire_incremental_build_lease(2).await.unwrap();
        let resumed_lease = resumed_lease.expect("paused manifest build should resume");
        assert!(resumed_lease.is_resumed());
        assert_eq!(resumed_lease.generation(), lease.generation());
        let resumed = IncrementalWorkStore::new(
            snapshots.database(),
            root,
            snapshots.active_generation().await.unwrap(),
            resumed_lease.generation(),
            resumed_lease.owner_id().to_owned(),
            resumed_lease.lease_epoch(),
            128,
        );
        for _ in 0..16 {
            let state = resumed.dag_run_state().await.unwrap();
            if state.dag_init_phase == "manifest_seed" {
                break;
            }
            resumed.initialize_source_manifest(&state).await.unwrap();
        }
        let completed = resumed.dag_run_state().await.unwrap();
        assert_eq!(completed.dag_init_phase, "manifest_seed");
        assert_eq!(completed.dag_source_revision, 1);
        let run_id = resumed_lease.generation();
        let counts = snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT \
                         (SELECT COUNT(*) \
                          FROM console_graph_build_source_manifest \
                          WHERE run_id = ?) AS count, \
                         (SELECT COUNT(*) \
                          FROM console_graph_build_source_refresh_manifest \
                          WHERE run_id = ?) AS value",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<BigInt, _>(run_id)
                .get_result::<CountAndValueRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("frozen-manifest-count"),
                })
            })
            .await
            .unwrap();
        assert_eq!(counts.count, 129);
        assert_eq!(counts.value, 129);
    }

    #[tokio::test]
    async fn high_cardinality_refresh_manifest_reset_and_copy_resume_in_bounded_pages() {
        const REFRESH_COUNT: i64 = 300;

        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let root_for_seed = root.clone();
        snapshots
            .with_connection(move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_names ( \
                                 branch_name, first_source_revision \
                             ) VALUES ('many-refreshes', 1)",
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_history ( \
                                 branch_name, source_revision, contribution_generation, \
                                 head_id, state_json, removed \
                             ) VALUES ('many-refreshes', 1, 42, ?, '{}', 0)",
                        )
                        .bind::<Text, _>(&root_for_seed)
                        .execute(connection)?;
                        for refresh_id in 1_i64..=REFRESH_COUNT {
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_refresh_runs ( \
                                     refresh_id, branch_name, target_head_id, target_state_json, \
                                     status, target_contribution_generation, \
                                     published_source_revision \
                                 ) VALUES (?, 'many-refreshes', ?, '{}', \
                                           'published', 42, 1)",
                            )
                            .bind::<BigInt, _>(refresh_id)
                            .bind::<Text, _>(&root_for_seed)
                            .execute(connection)?;
                        }
                        diesel::sql_query(
                            "UPDATE console_graph_source_identity SET revision = 1 WHERE id = 1",
                        )
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("high-cardinality-refresh-source"),
                    })
            })
            .await
            .unwrap();
        let lease = snapshots
            .acquire_incremental_build_lease(1)
            .await
            .unwrap()
            .unwrap();
        let run_id = lease.generation();
        snapshots
            .with_connection(move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        for refresh_id in 1_i64..=REFRESH_COUNT {
                            diesel::sql_query(
                                "INSERT INTO console_graph_build_source_refresh_manifest ( \
                                     run_id, branch_name, refresh_id \
                                 ) VALUES (?, 'many-refreshes', ?)",
                            )
                            .bind::<BigInt, _>(run_id)
                            .bind::<BigInt, _>(refresh_id)
                            .execute(connection)?;
                        }
                        diesel::sql_query(
                            "UPDATE console_graph_build_runs \
                             SET dag_baseline_generation = 0, dag_root_id = ?, \
                                 dag_build_kind = 'full', \
                                 dag_init_phase = 'manifest_refresh_reset' \
                             WHERE run_id = ?",
                        )
                        .bind::<Text, _>(&root)
                        .bind::<BigInt, _>(run_id)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(crate::error::QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("high-cardinality-refresh-build"),
                    })
            })
            .await
            .unwrap();
        let store = IncrementalWorkStore::new(
            snapshots.database(),
            writer.root_id(),
            0,
            run_id,
            lease.owner_id().to_owned(),
            lease.lease_epoch(),
            128,
        );
        let state = store.dag_run_state().await.unwrap();
        store.initialize_source_manifest(&state).await.unwrap();
        assert_eq!(build_refresh_manifest_count(&snapshots, run_id).await, 172);
        assert!(snapshots.pause_incremental_build(&lease).await.unwrap());
        drop(store);
        drop(snapshots);

        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let resumed_lease = snapshots
            .acquire_incremental_build_lease(1)
            .await
            .unwrap()
            .unwrap();
        assert!(resumed_lease.is_resumed());
        let resumed = IncrementalWorkStore::new(
            snapshots.database(),
            writer.root_id(),
            0,
            run_id,
            resumed_lease.owner_id().to_owned(),
            resumed_lease.lease_epoch(),
            128,
        );
        for _ in 0..24 {
            let state = resumed.dag_run_state().await.unwrap();
            if state.dag_init_phase == "manifest_seed" {
                break;
            }
            let before = build_refresh_manifest_count(&snapshots, run_id).await;
            let phase = state.dag_init_phase.clone();
            resumed.initialize_source_manifest(&state).await.unwrap();
            let after = build_refresh_manifest_count(&snapshots, run_id).await;
            match phase.as_str() {
                "manifest_refresh_reset" => {
                    assert!(after <= before && before - after <= 128)
                }
                "manifest_refresh_copy" => {
                    assert!(after >= before && after - before <= 128)
                }
                _ => assert_eq!(before, after),
            }
        }
        assert_eq!(
            resumed.dag_run_state().await.unwrap().dag_init_phase,
            "manifest_seed"
        );
        assert_eq!(
            build_refresh_manifest_count(&snapshots, run_id).await,
            REFRESH_COUNT
        );
    }

    #[test]
    fn build_kind_journal_cursor_resumes_at_the_next_revision() {
        let first_page = (41..=168).collect::<Vec<_>>();
        assert!(journal_page_is_contiguous(
            40,
            0,
            first_page.iter().copied()
        ));
        let saved_revision_cursor = *first_page.last().unwrap();
        assert!(journal_page_is_contiguous(
            40,
            saved_revision_cursor,
            [169, 170]
        ));
        assert!(journal_cursor_covers_revision_range(40, 170, 170));
    }

    #[tokio::test]
    async fn high_fan_in_parent_requirements_resume_from_persisted_cursor() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let mut parents = Vec::new();
        for index in 0..129 {
            parents.push(append_text(&writer, &root, &format!("parent {index}")).await);
        }
        let child = append_prompt(
            &writer,
            &parents[0],
            parents[1..]
                .iter()
                .cloned()
                .map(MergeParent::merge)
                .collect(),
            "wide merge",
        )
        .await;
        writer.fork("main", &child).await.unwrap();

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
        let sparse_child = child.clone();
        snapshots
            .with_connection(move |connection| {
                (|| -> QueryResult<()> {
                    for index in 0..128 {
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_node_relations \
                                 (parent_id, child_id) VALUES (?, ?)",
                        )
                        .bind::<Text, _>(format!("!inactive-{index:03}"))
                        .bind::<Text, _>(&sparse_child)
                        .execute(connection)?;
                    }
                    Ok(())
                })()
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("sparse-parent-requirement-fixture"),
                })
            })
            .await
            .unwrap();

        let lease = snapshots
            .acquire_incremental_build_lease(31)
            .await
            .unwrap()
            .unwrap();
        let baseline_generation = snapshots.active_generation().await.unwrap();
        let mut store = IncrementalWorkStore::new(
            snapshots.database(),
            root.clone(),
            baseline_generation,
            lease.generation(),
            lease.owner_id().to_owned(),
            lease.lease_epoch(),
            128,
        );
        store.begin().await.unwrap();
        let child_node = store.load_source_node(&child).await.unwrap();
        let child_item = FrontierNode::new(
            saturating_i64(child_node.created_at.as_nanosecond()),
            child.clone(),
        );

        assert!(
            !store
                .advance_parent_requirement_page(&child_item)
                .await
                .unwrap()
        );
        let first = parent_requirement_checkpoint(&snapshots, lease.generation(), &child).await;
        assert_eq!(first.remaining_parents, 0);
        assert_eq!(first.parent_requirements_complete, 0);
        assert_eq!(first.discovered_node_count, 1);
        assert!(!first.parent_requirement_cursor.is_empty());

        assert!(snapshots.pause_incremental_build(&lease).await.unwrap());
        assert!(
            store
                .advance_parent_requirement_page(&child_item)
                .await
                .is_err()
        );
        let resumed_lease = snapshots
            .acquire_incremental_build_lease(31)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resumed_lease.generation(), lease.generation());
        assert!(resumed_lease.lease_epoch() > lease.lease_epoch());
        let mut resumed = IncrementalWorkStore::new(
            snapshots.database(),
            root,
            baseline_generation,
            resumed_lease.generation(),
            resumed_lease.owner_id().to_owned(),
            resumed_lease.lease_epoch(),
            128,
        );
        resumed.begin().await.unwrap();
        while !resumed
            .advance_parent_requirement_page(&child_item)
            .await
            .unwrap()
        {}

        let completed =
            parent_requirement_checkpoint(&snapshots, resumed_lease.generation(), &child).await;
        assert_eq!(completed.remaining_parents, 129);
        assert_eq!(completed.parent_requirements_complete, 1);
        assert!(
            resumed
                .advance_parent_requirement_page(&child_item)
                .await
                .unwrap()
        );
        let repeated =
            parent_requirement_checkpoint(&snapshots, resumed_lease.generation(), &child).await;
        assert_eq!(repeated, completed);
    }

    #[tokio::test]
    async fn parent_requirement_failure_before_checkpoint_is_reentrant() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let parent = append_text(&writer, &root, "parent").await;
        let child = append_text(&writer, &parent, "child").await;
        writer.fork("main", &child).await.unwrap();

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

        let lease = snapshots
            .acquire_incremental_build_lease(37)
            .await
            .unwrap()
            .unwrap();
        let mut store = IncrementalWorkStore::new(
            snapshots.database(),
            root,
            snapshots.active_generation().await.unwrap(),
            lease.generation(),
            lease.owner_id().to_owned(),
            lease.lease_epoch(),
            128,
        );
        store.begin().await.unwrap();
        let child_node = store.load_source_node(&child).await.unwrap();
        let child_item = FrontierNode::new(
            saturating_i64(child_node.created_at.as_nanosecond()),
            child.clone(),
        );

        snapshots
            .with_connection(|connection| {
                connection
                    .batch_execute(
                        "CREATE TRIGGER inject_parent_requirement_checkpoint_failure \
                         BEFORE UPDATE OF parent_requirement_cursor \
                         ON console_graph_build_nodes \
                         BEGIN SELECT RAISE(ABORT, 'injected checkpoint failure'); END;",
                    )
                    .context(crate::error::QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("parent-requirement-failure-injection"),
                    })
            })
            .await
            .unwrap();

        assert!(
            store
                .advance_parent_requirement_page(&child_item)
                .await
                .is_err()
        );
        assert_eq!(
            parent_requirement_transaction_checkpoint(&snapshots, lease.generation(), &child).await,
            ParentRequirementTransactionCheckpoint {
                work_node_count: 0,
                discovered_node_count: 0,
            }
        );

        snapshots
            .with_connection(|connection| {
                connection
                    .batch_execute("DROP TRIGGER inject_parent_requirement_checkpoint_failure")
                    .context(crate::error::QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("parent-requirement-failure-injection"),
                    })
            })
            .await
            .unwrap();

        assert!(
            !store
                .advance_parent_requirement_page(&child_item)
                .await
                .unwrap()
        );
        assert!(
            store
                .advance_parent_requirement_page(&child_item)
                .await
                .unwrap()
        );
        let committed = parent_requirement_checkpoint(&snapshots, lease.generation(), &child).await;
        assert_eq!(committed.remaining_parents, 1);
        assert_eq!(committed.parent_requirements_complete, 1);
        assert_eq!(committed.discovered_node_count, 1);
        assert!(
            store
                .advance_parent_requirement_page(&child_item)
                .await
                .unwrap()
        );
        assert_eq!(
            parent_requirement_checkpoint(&snapshots, lease.generation(), &child).await,
            committed
        );
    }

    #[tokio::test]
    async fn child_page_failure_before_cursor_commit_is_reentrant() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let parent = append_text(&writer, &root, "parent").await;
        let child = append_text(&writer, &parent, "child").await;
        writer.fork("main", &child).await.unwrap();

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

        let lease = snapshots
            .acquire_incremental_build_lease(38)
            .await
            .unwrap()
            .unwrap();
        let mut store = IncrementalWorkStore::new(
            snapshots.database(),
            root,
            snapshots.active_generation().await.unwrap(),
            lease.generation(),
            lease.owner_id().to_owned(),
            lease.lease_epoch(),
            128,
        );
        store.begin().await.unwrap();
        let child_node = store.load_source_node(&child).await.unwrap();
        let child_item = FrontierNode::new(
            saturating_i64(child_node.created_at.as_nanosecond()),
            child.clone(),
        );
        while !store
            .advance_parent_requirement_page(&child_item)
            .await
            .unwrap()
        {}
        store.parent_expansion(&parent).await.unwrap();

        snapshots
            .with_connection(|connection| {
                connection
                    .batch_execute(
                        "CREATE TRIGGER inject_child_page_cursor_failure \
                         BEFORE UPDATE OF next_child_id \
                         ON console_graph_build_parent_expansions \
                         BEGIN SELECT RAISE(ABORT, 'injected batch commit failure'); END;",
                    )
                    .context(crate::error::QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("child-page-failure-injection"),
                    })
            })
            .await
            .unwrap();

        assert!(store.discover_ready_child_page(&parent, "").await.is_err());
        assert_eq!(
            child_page_transaction_checkpoint(&snapshots, lease.generation(), &parent, &child,)
                .await,
            ChildPageTransactionCheckpoint {
                remaining_parents: 1,
                satisfaction_count: 0,
                next_child_id: String::new(),
                complete: 0,
            }
        );

        snapshots
            .with_connection(|connection| {
                connection
                    .batch_execute("DROP TRIGGER inject_child_page_cursor_failure")
                    .context(crate::error::QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("child-page-failure-injection"),
                    })
            })
            .await
            .unwrap();

        store.discover_ready_child_page(&parent, "").await.unwrap();
        let non_empty_page =
            child_page_transaction_checkpoint(&snapshots, lease.generation(), &parent, &child)
                .await;
        assert_eq!(non_empty_page.remaining_parents, 0);
        assert_eq!(non_empty_page.satisfaction_count, 1);
        assert_eq!(non_empty_page.next_child_id, child);
        assert_eq!(non_empty_page.complete, 0);
        store
            .discover_ready_child_page(&parent, &child_item.node_id)
            .await
            .unwrap();
        let committed =
            child_page_transaction_checkpoint(&snapshots, lease.generation(), &parent, &child)
                .await;
        assert_eq!(committed.remaining_parents, 0);
        assert_eq!(committed.satisfaction_count, 1);
        assert_eq!(committed.next_child_id, child);
        assert_eq!(committed.complete, 1);
        store.discover_ready_child_page(&parent, "").await.unwrap();
        assert_eq!(
            child_page_transaction_checkpoint(
                &snapshots,
                lease.generation(),
                &parent,
                &child_item.node_id,
            )
            .await,
            committed
        );
    }

    #[tokio::test]
    async fn restart_does_not_enqueue_child_before_expanded_parent_is_completed() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let parent = append_text(&writer, &root, "parent").await;
        let child = append_text(&writer, &parent, "child").await;
        writer.fork("main", &child).await.unwrap();

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

        let source_version = 36;
        let config = IncrementalBuildConfig {
            frontier_low_watermark: 1,
            frontier_high_watermark: 4,
            graph_buffer_limit: 2,
            child_page_size: 2,
        };
        let lease = snapshots
            .acquire_incremental_build_lease(source_version)
            .await
            .unwrap()
            .unwrap();
        let baseline_generation = snapshots.active_generation().await.unwrap();
        let (store, mut frontier) =
            initialize_incremental_build(&snapshots, &root, baseline_generation, &lease, config)
                .await
                .unwrap();
        seed_incremental_frontier(&snapshots, &lease, &store, &mut frontier)
            .await
            .unwrap();

        let root_cursor = frontier.peek_min().await.unwrap().unwrap();
        let mut buffer = GraphBuffer::new(config.graph_buffer_limit);
        process_frontier_cursor(
            &snapshots,
            &lease,
            &store,
            &mut frontier,
            &mut buffer,
            &root_cursor,
        )
        .await
        .unwrap();
        let parent_cursor = frontier.peek_min().await.unwrap().unwrap();
        assert_eq!(parent_cursor.node_id, parent);

        let parent_node = store.load_source_node(&parent).await.unwrap();
        loop {
            let state = store.work_node(&parent).await.unwrap().unwrap();
            if state.projection_complete == 1 {
                break;
            }
            store
                .process_node(&parent_node, &snapshots, &lease, &mut buffer)
                .await
                .unwrap();
        }
        expand_all_child_pages(&snapshots, &lease, &store, &parent)
            .await
            .unwrap();
        assert!(store.unqueued_ready_node_page().await.unwrap().is_empty());

        let child_for_update = child.clone();
        let run_id = lease.generation();
        snapshots
            .database()
            .with_write_connection(
                "make resumed child sort before its parent",
                move |connection| {
                    diesel::sql_query(
                        "UPDATE console_graph_build_nodes SET created_at_ns = ? \
                     WHERE run_id = ? AND node_id = ?",
                    )
                    .bind::<BigInt, _>(parent_cursor.created_at_ns.saturating_sub(1))
                    .bind::<BigInt, _>(run_id)
                    .bind::<Text, _>(child_for_update)
                    .execute(connection)
                    .map(|_| ())
                    .context(crate::error::QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("ready-child-order-fixture"),
                    })
                },
            )
            .await
            .unwrap();

        assert!(snapshots.pause_incremental_build(&lease).await.unwrap());
        drop(frontier);
        drop(store);
        let resumed_lease = snapshots
            .acquire_incremental_build_lease(source_version)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resumed_lease.generation(), lease.generation());
        let (resumed_store, mut resumed_frontier) = initialize_incremental_build(
            &snapshots,
            &root,
            baseline_generation,
            &resumed_lease,
            config,
        )
        .await
        .unwrap();
        seed_incremental_frontier(
            &snapshots,
            &resumed_lease,
            &resumed_store,
            &mut resumed_frontier,
        )
        .await
        .unwrap();
        let resumed_parent = resumed_frontier.peek_min().await.unwrap().unwrap();
        assert_eq!(resumed_parent.node_id, parent);

        let mut resumed_buffer = GraphBuffer::new(config.graph_buffer_limit);
        process_frontier_cursor(
            &snapshots,
            &resumed_lease,
            &resumed_store,
            &mut resumed_frontier,
            &mut resumed_buffer,
            &resumed_parent,
        )
        .await
        .unwrap();
        let child_cursor = resumed_frontier.peek_min().await.unwrap().unwrap();
        assert_eq!(child_cursor.node_id, child);
        process_frontier_cursor(
            &snapshots,
            &resumed_lease,
            &resumed_store,
            &mut resumed_frontier,
            &mut resumed_buffer,
            &child_cursor,
        )
        .await
        .unwrap();

        let parent_for_query = parent.clone();
        let child_for_query = child.clone();
        let edge_count = snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count FROM console_graph_edge_routes \
                     WHERE generation = ? AND mode = 'all' \
                       AND source_id = ? AND target_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(parent_for_query)
                .bind::<Text, _>(child_for_query)
                .get_result::<CountRow>(connection)
                .map(|row| row.count)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("resumed-parent-edge-check"),
                })
            })
            .await
            .unwrap();
        assert_eq!(edge_count, 1);
    }

    #[tokio::test]
    async fn dag_progress_counters_resume_and_include_late_discovery() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let first = append_text(&writer, &root, "first").await;
        let second = append_text(&writer, &first, "second").await;
        writer.fork("main", &second).await.unwrap();

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

        let source_version = 37;
        let config = IncrementalBuildConfig {
            frontier_low_watermark: 1,
            frontier_high_watermark: 4,
            graph_buffer_limit: 2,
            child_page_size: 2,
        };
        let lease = snapshots
            .acquire_incremental_build_lease(source_version)
            .await
            .unwrap()
            .unwrap();
        let baseline_generation = snapshots.active_generation().await.unwrap();
        let (store, mut frontier) =
            initialize_incremental_build(&snapshots, &root, baseline_generation, &lease, config)
                .await
                .unwrap();
        seed_incremental_frontier(&snapshots, &lease, &store, &mut frontier)
            .await
            .unwrap();
        assert_eq!(
            store.dag_progress().await.unwrap(),
            DagProgress {
                discovered_nodes: 1,
                processed_nodes: 0,
            }
        );

        let cursor = frontier.peek_min().await.unwrap().unwrap();
        assert_eq!(cursor.node_id, root);
        let mut buffer = GraphBuffer::new(config.graph_buffer_limit);
        process_frontier_cursor(
            &snapshots,
            &lease,
            &store,
            &mut frontier,
            &mut buffer,
            &cursor,
        )
        .await
        .unwrap();
        assert_eq!(
            store.dag_progress().await.unwrap(),
            DagProgress {
                discovered_nodes: 2,
                processed_nodes: 1,
            }
        );

        assert!(snapshots.pause_incremental_build(&lease).await.unwrap());
        drop(frontier);
        drop(store);
        let resumed_lease = snapshots
            .acquire_incremental_build_lease(source_version)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resumed_lease.generation(), lease.generation());
        assert!(resumed_lease.lease_epoch() > lease.lease_epoch());

        let stats = build_incremental_generation_with_config(
            &snapshots,
            &root,
            baseline_generation,
            &resumed_lease,
            source_version,
            config,
        )
        .await
        .unwrap();
        assert_eq!(stats.processed_nodes, 3);

        let mut resumed_store = IncrementalWorkStore::new(
            snapshots.database(),
            root,
            baseline_generation,
            resumed_lease.generation(),
            resumed_lease.owner_id().to_owned(),
            resumed_lease.lease_epoch(),
            config.child_page_size,
        );
        resumed_store.begin().await.unwrap();
        assert_eq!(
            resumed_store.dag_progress().await.unwrap(),
            DagProgress {
                discovered_nodes: 3,
                processed_nodes: 3,
            }
        );
        assert!(!resumed_store.has_unprocessed_nodes().await.unwrap());
        assert_eq!(
            stats.all_nodes,
            resumed_store
                .completed_shell_node_count(GraphMode::All)
                .await
                .unwrap()
        );
        assert_eq!(
            stats.anchors_nodes,
            resumed_store
                .completed_shell_node_count(GraphMode::Anchors)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn wide_anchor_projection_resumes_after_raw_parent_checkpoint() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let mut parents = Vec::new();
        for index in 0..130 {
            parents
                .push(append_prompt(&writer, &root, Vec::new(), &format!("parent {index}")).await);
        }
        let target = append_prompt(
            &writer,
            &parents[0],
            parents[1..]
                .iter()
                .cloned()
                .map(MergeParent::merge)
                .collect(),
            "wide anchor",
        )
        .await;
        writer.fork("main", &target).await.unwrap();

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

        let source_version = 41;
        let config = IncrementalBuildConfig {
            frontier_low_watermark: 64,
            frontier_high_watermark: 128,
            graph_buffer_limit: 16,
            child_page_size: 128,
        };
        let lease = snapshots
            .acquire_incremental_build_lease(source_version)
            .await
            .unwrap()
            .unwrap();
        let baseline_generation = snapshots.active_generation().await.unwrap();
        let (store, mut frontier) =
            initialize_incremental_build(&snapshots, &root, baseline_generation, &lease, config)
                .await
                .unwrap();
        seed_incremental_frontier(&snapshots, &lease, &store, &mut frontier)
            .await
            .unwrap();
        let mut buffer = GraphBuffer::new(config.graph_buffer_limit);
        loop {
            let cursor = frontier.peek_min().await.unwrap().unwrap();
            if cursor.node_id == target {
                break;
            }
            process_frontier_cursor(
                &snapshots,
                &lease,
                &store,
                &mut frontier,
                &mut buffer,
                &cursor,
            )
            .await
            .unwrap();
        }

        let target_node = store.load_source_node(&target).await.unwrap();
        loop {
            let state = store.projection_work_node(&target).await.unwrap().unwrap();
            if state.projection_phase == "anchor_prepare" {
                store
                    .process_node(&target_node, &snapshots, &lease, &mut buffer)
                    .await
                    .unwrap();
                break;
            }
            store
                .process_node(&target_node, &snapshots, &lease, &mut buffer)
                .await
                .unwrap();
        }
        let checkpoint = store.anchor_projection_state(&target).await.unwrap();
        assert_eq!(checkpoint.raw_cursor, 127);
        assert_eq!(checkpoint.raw_complete, 0);

        assert!(snapshots.pause_incremental_build(&lease).await.unwrap());
        assert!(store.anchor_projection_state(&target).await.is_err());
        let resumed_lease = snapshots
            .acquire_incremental_build_lease(source_version)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resumed_lease.generation(), lease.generation());
        assert!(resumed_lease.lease_epoch() > lease.lease_epoch());

        let stats = build_incremental_generation_with_config(
            &snapshots,
            &root,
            baseline_generation,
            &resumed_lease,
            source_version,
            config,
        )
        .await
        .unwrap();
        assert_eq!(stats.processed_nodes, parents.len() + 2);

        let run_id = resumed_lease.generation();
        let target_id = target.clone();
        let resolved_edges = snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count \
                     FROM console_graph_build_anchor_edges \
                     WHERE run_id = ? AND target_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(target_id)
                .get_result::<CountRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("wide-anchor-projection"),
                })
            })
            .await
            .unwrap()
            .count;
        assert_eq!(resolved_edges, 130);
    }

    #[tokio::test]
    async fn diamond_dag_child_is_lifetime_deduplicated_across_spill_and_reopen() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let left = append_text(&writer, &root, "left").await;
        let right = append_text(&writer, &root, "right").await;
        let extra_a = append_text(&writer, &root, "extra-a").await;
        let extra_b = append_text(&writer, &root, "extra-b").await;
        let child = append_prompt(
            &writer,
            &left,
            vec![MergeParent::merge(right.clone())],
            "diamond-child",
        )
        .await;
        writer.fork("main", &child).await.unwrap();
        writer.fork("extra-a", &extra_a).await.unwrap();
        writer.fork("extra-b", &extra_b).await.unwrap();

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

        let source_version = 44;
        let config = IncrementalBuildConfig {
            frontier_low_watermark: 1,
            frontier_high_watermark: 2,
            graph_buffer_limit: 1,
            child_page_size: 2,
        };
        let lease = snapshots
            .acquire_incremental_build_lease(source_version)
            .await
            .unwrap()
            .unwrap();
        let baseline_generation = snapshots.active_generation().await.unwrap();
        let (store, mut frontier) =
            initialize_incremental_build(&snapshots, &root, baseline_generation, &lease, config)
                .await
                .unwrap();
        seed_incremental_frontier(&snapshots, &lease, &store, &mut frontier)
            .await
            .unwrap();
        let root_cursor = frontier.peek_min().await.unwrap().unwrap();
        assert_eq!(root_cursor.node_id, root);
        let mut buffer = GraphBuffer::new(config.graph_buffer_limit);
        process_frontier_cursor(
            &snapshots,
            &lease,
            &store,
            &mut frontier,
            &mut buffer,
            &root_cursor,
        )
        .await
        .unwrap();
        assert_eq!(frontier.mode(), FrontierMode::Spilled);
        assert_eq!(frontier.metrics().hot_to_spilled, 1);

        assert!(snapshots.pause_incremental_build(&lease).await.unwrap());
        drop(frontier);
        drop(store);
        let resumed_lease = snapshots
            .acquire_incremental_build_lease(source_version)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resumed_lease.generation(), lease.generation());
        assert!(resumed_lease.lease_epoch() > lease.lease_epoch());

        let stats = build_incremental_generation_with_config(
            &snapshots,
            &root,
            baseline_generation,
            &resumed_lease,
            source_version,
            config,
        )
        .await
        .unwrap();
        let distinct_nodes = [
            root.as_str(),
            left.as_str(),
            right.as_str(),
            extra_a.as_str(),
            extra_b.as_str(),
            child.as_str(),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>()
        .len();
        assert_eq!(stats.processed_nodes, distinct_nodes);
        assert_eq!(stats.frontier.spilled_to_hot, 1);

        let run_id = resumed_lease.generation();
        let child_id = child.clone();
        let checkpoint = snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT run.dag_discovered_node_count AS discovered_nodes, \
                            run.dag_processed_node_count AS processed_nodes, \
                            (SELECT COUNT(*) FROM console_graph_build_nodes AS work \
                             WHERE work.run_id = run.run_id AND work.node_id = ?) \
                                AS child_work_rows, \
                            (SELECT COUNT(*) FROM console_graph_build_frontier AS seen \
                             WHERE seen.run_id = run.run_id AND seen.node_id = ?) \
                                AS child_seen_rows, \
                            (SELECT COUNT(*) FROM console_graph_build_frontier AS pending \
                             WHERE pending.run_id = run.run_id AND pending.node_id = ? \
                               AND pending.pending = 1) AS child_pending_rows, \
                            work.processed AS child_processed, \
                            work.frontier_enqueued AS child_frontier_enqueued \
                     FROM console_graph_build_runs AS run \
                     INNER JOIN console_graph_build_nodes AS work \
                         ON work.run_id = run.run_id AND work.node_id = ? \
                     WHERE run.run_id = ?",
                )
                .bind::<Text, _>(&child_id)
                .bind::<Text, _>(&child_id)
                .bind::<Text, _>(&child_id)
                .bind::<Text, _>(&child_id)
                .bind::<BigInt, _>(run_id)
                .get_result::<DiamondDedupCheckpoint>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("diamond-dag-lifetime-dedup"),
                })
            })
            .await
            .unwrap();
        assert_eq!(checkpoint.discovered_nodes, distinct_nodes as i64);
        assert_eq!(checkpoint.processed_nodes, distinct_nodes as i64);
        assert_eq!(checkpoint.child_work_rows, 1);
        assert_eq!(checkpoint.child_seen_rows, 1);
        assert_eq!(checkpoint.child_pending_rows, 0);
        assert_eq!(checkpoint.child_processed, 1);
        assert_eq!(checkpoint.child_frontier_enqueued, 1);
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
    async fn newly_published_branch_is_applied_as_additive_delta() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let main_head = append_text(&writer, &root, "main").await;
        writer.fork("main", &main_head).await.unwrap();

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

        let feature_head = append_text(&writer, &root, "feature").await;
        writer.fork("feature", &feature_head).await.unwrap();
        refresh_source_index(&source, &mut index).await;

        let delta = build_and_publish(&snapshots, &root, 2).await;
        assert!(delta.reused_baseline);

        let all = snapshots
            .latest_viewport(GraphMode::All, GraphViewportRequest::default())
            .await
            .unwrap()
            .unwrap();
        let feature = all
            .nodes
            .iter()
            .find(|node| node.id == feature_head)
            .unwrap();
        assert_eq!(feature.labels, vec!["feature".to_owned()]);
        assert!(all.nodes.iter().any(|node| node.id == main_head));
    }

    #[tokio::test]
    async fn deleted_branch_is_applied_as_tombstone_delta() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let main_head = append_text(&writer, &root, "main").await;
        writer.fork("main", &main_head).await.unwrap();
        let stale_session = append_session(&writer, &root, "stale session").await;
        let stale_anchor = append_prompt(&writer, &stale_session, Vec::new(), "stale").await;
        let stale_head = append_text(&writer, &stale_anchor, "stale response").await;
        writer.fork("stale", &stale_head).await.unwrap();

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

        writer.delete_branch("stale").await.unwrap();
        refresh_source_index(&source, &mut index).await;
        let delta = build_and_publish(&snapshots, &root, 2).await;
        assert!(delta.reused_baseline);
        assert_eq!(delta.processed_nodes, 0);

        for mode in [GraphMode::Anchors, GraphMode::All] {
            let expected = build_graph_snapshot_with_mode(&writer, 2, mode)
                .await
                .unwrap();
            let actual = snapshots
                .latest_viewport(mode, GraphViewportRequest::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(snapshot_shape(&expected), viewport_shape(&actual));
            assert!(actual.nodes.iter().all(|node| {
                node.id != stale_session && node.id != stale_anchor && node.id != stale_head
            }));
        }

        let active_generation = snapshots.active_generation().await.unwrap();
        let stale_branch_count = snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count \
                     FROM console_graph_materialization_branches \
                     WHERE generation = ? AND name = 'stale'",
                )
                .bind::<BigInt, _>(active_generation)
                .get_result::<CountRow>(connection)
                .map(|row| row.count)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("deleted-branch-delta"),
                })
            })
            .await
            .unwrap();
        assert_eq!(stale_branch_count, 0);
    }

    #[tokio::test]
    async fn delta_removal_reopen_uses_frozen_source_membership() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let main_head = append_text(&writer, &root, "main").await;
        writer.fork("main", &main_head).await.unwrap();
        let stale_session = append_session(&writer, &root, "stale session").await;
        let stale_head = append_text(&writer, &stale_session, "stale response").await;
        writer.fork("stale", &stale_head).await.unwrap();

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

        writer.delete_branch("stale").await.unwrap();
        refresh_source_index(&source, &mut index).await;
        let baseline_generation = snapshots.active_generation().await.unwrap();
        let lease = snapshots
            .acquire_incremental_build_lease(2)
            .await
            .unwrap()
            .unwrap();
        let run_id = lease.generation();
        let mut store = IncrementalWorkStore::new(
            snapshots.database(),
            root.clone(),
            baseline_generation,
            run_id,
            lease.owner_id().to_owned(),
            lease.lease_epoch(),
            DEFAULT_CHILD_PAGE_SIZE,
        );
        for _ in 0..10_000 {
            let state = store.dag_run_state().await.unwrap();
            if state.dag_init_phase == "delta_remove" {
                break;
            }
            match state.dag_init_phase.as_str() {
                "new"
                | "full_reset"
                | "manifest_refresh_reset"
                | "manifest_branch_reset"
                | "manifest_copy"
                | "manifest_refresh_copy"
                | "manifest_reset"
                | "manifest_seed" => store.initialize_source_manifest(&state).await.unwrap(),
                phase if phase.starts_with("kind") => store.choose_build_kind().await.unwrap(),
                "scope" => {
                    store.process_scope_batch().await.unwrap();
                }
                phase => panic!("unexpected pre-removal phase {phase}"),
            }
        }
        assert_eq!(
            store.dag_run_state().await.unwrap().dag_init_phase,
            "delta_remove"
        );

        store.populate_delta_removal_batch().await.unwrap();
        let frozen = delta_removal_checkpoint(&snapshots, run_id, "stale").await;
        assert_eq!(frozen.removal_bound_frozen, 1);
        assert_eq!(frozen.removal_complete, 0);

        writer.fork("future", &stale_head).await.unwrap();
        refresh_source_index(&source, &mut index).await;
        let current_stale_head = stale_head.clone();
        let current_membership = snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT EXISTS ( \
                         SELECT 1 FROM console_graph_source_current_branch_nodes \
                         WHERE branch_name = 'future' AND node_id = ? \
                     ) AS value",
                )
                .bind::<Text, _>(current_stale_head)
                .get_result::<IntegerRow>(connection)
                .map(|row| row.value)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("future-source-membership"),
                })
            })
            .await
            .unwrap();
        assert_eq!(current_membership, 1);

        assert!(snapshots.pause_incremental_build(&lease).await.unwrap());
        assert!(store.populate_delta_removal_batch().await.is_err());
        drop(store);
        drop(index);
        drop(source);
        drop(snapshots);

        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let resumed = snapshots
            .acquire_incremental_build_lease(2)
            .await
            .unwrap()
            .unwrap();
        assert!(resumed.is_resumed());
        assert_eq!(resumed.generation(), run_id);
        let resumed_store = IncrementalWorkStore::new(
            snapshots.database(),
            root.clone(),
            baseline_generation,
            run_id,
            resumed.owner_id().to_owned(),
            resumed.lease_epoch(),
            DEFAULT_CHILD_PAGE_SIZE,
        );
        for _ in 0..100 {
            resumed_store.populate_delta_removal_batch().await.unwrap();
            if delta_removal_checkpoint(&snapshots, run_id, "stale")
                .await
                .removal_complete
                == 1
            {
                break;
            }
        }
        assert_eq!(
            delta_removal_checkpoint(&snapshots, run_id, "stale")
                .await
                .removal_complete,
            1
        );
        let tombstoned_stale_head = stale_head.clone();
        let tombstone_exists = snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT EXISTS ( \
                         SELECT 1 FROM console_graph_build_node_tombstones \
                         WHERE run_id = ? AND node_id = ? \
                     ) AS value",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(tombstoned_stale_head)
                .get_result::<IntegerRow>(connection)
                .map(|row| row.value)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("frozen-source-membership-tombstone"),
                })
            })
            .await
            .unwrap();
        assert_eq!(tombstone_exists, 1);
    }

    #[tokio::test]
    async fn delta_removal_reopens_with_a_frozen_raw_upper_bound() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let main_head = append_text(&writer, &root, "main").await;
        writer.fork("main", &main_head).await.unwrap();
        let stale_session = append_session(&writer, &root, "stale session").await;
        let mut stale_head = stale_session;
        for index in 0..130 {
            let prompt = append_prompt(
                &writer,
                &stale_head,
                Vec::new(),
                &format!("stale prompt {index}"),
            )
            .await;
            stale_head = append_text(&writer, &prompt, &format!("stale response {index}")).await;
        }
        writer.fork("stale", &stale_head).await.unwrap();

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

        writer.delete_branch("stale").await.unwrap();
        refresh_source_index(&source, &mut index).await;
        let baseline_generation = snapshots.active_generation().await.unwrap();
        let lease = snapshots
            .acquire_incremental_build_lease(2)
            .await
            .unwrap()
            .unwrap();
        let run_id = lease.generation();
        let mut store = IncrementalWorkStore::new(
            snapshots.database(),
            root.clone(),
            baseline_generation,
            run_id,
            lease.owner_id().to_owned(),
            lease.lease_epoch(),
            DEFAULT_CHILD_PAGE_SIZE,
        );
        for _ in 0..10_000 {
            let state = store.dag_run_state().await.unwrap();
            if state.dag_init_phase == "delta_remove" {
                break;
            }
            match state.dag_init_phase.as_str() {
                "new"
                | "full_reset"
                | "manifest_refresh_reset"
                | "manifest_branch_reset"
                | "manifest_copy"
                | "manifest_refresh_copy"
                | "manifest_reset"
                | "manifest_seed" => store.initialize_source_manifest(&state).await.unwrap(),
                phase if phase.starts_with("kind") => store.choose_build_kind().await.unwrap(),
                "scope" => {
                    store.process_scope_batch().await.unwrap();
                }
                phase => panic!("unexpected pre-removal phase {phase}"),
            }
        }
        assert_eq!(
            store.dag_run_state().await.unwrap().dag_init_phase,
            "delta_remove"
        );

        store.populate_delta_removal_batch().await.unwrap();
        let frozen = delta_removal_checkpoint(&snapshots, run_id, "stale").await;
        assert_eq!(frozen.removal_bound_frozen, 1);
        assert!(frozen.removal_refresh_id_upper_bound >= 0);
        assert_eq!(frozen.removal_refresh_id_cursor, -1);
        assert_eq!(frozen.removal_cursor, "");
        assert_eq!(frozen.removal_complete, 0);

        store.populate_delta_removal_batch().await.unwrap();
        let first_page = delta_removal_checkpoint(&snapshots, run_id, "stale").await;
        assert_eq!(
            first_page.removal_refresh_id_upper_bound,
            frozen.removal_refresh_id_upper_bound
        );
        assert_eq!(first_page.removal_bound_frozen, 1);
        assert_eq!(first_page.removal_complete, 0);
        assert!(!first_page.removal_cursor.is_empty());
        assert!(first_page.tombstone_count > 0);

        let future_refresh_id = frozen.removal_refresh_id_upper_bound + 10_000;
        let future_root = root.clone();
        snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "INSERT INTO console_graph_source_branch_nodes ( \
                         branch_name, contribution_generation, node_id \
                     ) VALUES ('stale', ?, ?)",
                )
                .bind::<BigInt, _>(future_refresh_id)
                .bind::<Text, _>(future_root)
                .execute(connection)
                .map(|_| ())
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("future-delta-removal-membership"),
                })
            })
            .await
            .unwrap();

        assert!(snapshots.pause_incremental_build(&lease).await.unwrap());
        assert!(store.populate_delta_removal_batch().await.is_err());
        assert_eq!(
            delta_removal_checkpoint(&snapshots, run_id, "stale").await,
            first_page
        );
        drop(store);
        drop(index);
        drop(source);
        drop(snapshots);

        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let resumed = snapshots
            .acquire_incremental_build_lease(2)
            .await
            .unwrap()
            .unwrap();
        assert!(resumed.is_resumed());
        assert_eq!(resumed.generation(), run_id);
        assert!(resumed.lease_epoch() > lease.lease_epoch());
        let resumed_store = IncrementalWorkStore::new(
            snapshots.database(),
            root.clone(),
            baseline_generation,
            run_id,
            resumed.owner_id().to_owned(),
            resumed.lease_epoch(),
            DEFAULT_CHILD_PAGE_SIZE,
        );
        let mut appended_future_rows = 1_i64;
        for offset in 1_i64..=8 {
            resumed_store.populate_delta_removal_batch().await.unwrap();
            let checkpoint = delta_removal_checkpoint(&snapshots, run_id, "stale").await;
            if checkpoint.removal_complete == 1 {
                break;
            }
            let future_root = root.clone();
            snapshots
                .with_connection(move |connection| {
                    diesel::sql_query(
                        "INSERT INTO console_graph_source_branch_nodes ( \
                             branch_name, contribution_generation, node_id \
                         ) VALUES ('stale', ?, ?)",
                    )
                    .bind::<BigInt, _>(future_refresh_id + offset)
                    .bind::<Text, _>(future_root)
                    .execute(connection)
                    .map(|_| ())
                    .context(crate::error::QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("continuous-future-delta-removal-membership"),
                    })
                })
                .await
                .unwrap();
            appended_future_rows += 1;
        }
        let completed = delta_removal_checkpoint(&snapshots, run_id, "stale").await;
        assert_eq!(completed.removal_complete, 1);
        assert_eq!(completed.removal_bound_frozen, 1);
        assert_eq!(
            completed.removal_refresh_id_upper_bound,
            frozen.removal_refresh_id_upper_bound
        );
        assert!(
            completed.removal_refresh_id_cursor <= completed.removal_refresh_id_upper_bound,
            "future memberships advanced the frozen raw cursor: {completed:?}",
        );
        let future_count = snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count \
                     FROM console_graph_source_branch_nodes \
                     WHERE branch_name = 'stale' AND contribution_generation > ?",
                )
                .bind::<BigInt, _>(completed.removal_refresh_id_upper_bound)
                .get_result::<CountRow>(connection)
                .map(|row| row.count)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("future-delta-removal-membership-count"),
                })
            })
            .await
            .unwrap();
        assert_eq!(future_count, appended_future_rows);

        drop(resumed_store);
        let stats =
            build_incremental_generation(&snapshots, &root, baseline_generation, &resumed, 2)
                .await
                .unwrap();
        assert!(stats.reused_baseline);
        snapshots
            .publish_incremental_generation(&resumed, 2)
            .await
            .unwrap();
        for mode in [GraphMode::Anchors, GraphMode::All] {
            let expected = build_graph_snapshot_with_mode(&writer, 2, mode)
                .await
                .unwrap();
            let actual = snapshots
                .latest_viewport(mode, GraphViewportRequest::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(snapshot_shape(&expected), viewport_shape(&actual));
        }
    }

    #[tokio::test]
    async fn deleted_branch_scope_removal_resumes_after_bounded_page() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let main_head = append_text(&writer, &root, "main").await;
        writer.fork("main", &main_head).await.unwrap();
        let stale_session = append_session(&writer, &root, "stale session").await;
        let mut stale_head = stale_session;
        for index in 0..130 {
            let prompt = append_prompt(
                &writer,
                &stale_head,
                Vec::new(),
                &format!("stale prompt {index}"),
            )
            .await;
            stale_head = append_text(&writer, &prompt, &format!("stale response {index}")).await;
        }
        writer.fork("stale", &stale_head).await.unwrap();

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

        writer.delete_branch("stale").await.unwrap();
        refresh_source_index(&source, &mut index).await;
        let baseline_generation = snapshots.active_generation().await.unwrap();
        let lease = snapshots
            .acquire_incremental_build_lease(2)
            .await
            .unwrap()
            .unwrap();
        let mut store = IncrementalWorkStore::new(
            snapshots.database(),
            root.clone(),
            baseline_generation,
            lease.generation(),
            lease.owner_id().to_owned(),
            lease.lease_epoch(),
            DEFAULT_CHILD_PAGE_SIZE,
        );
        for _ in 0..10_000 {
            let state = store.dag_run_state().await.unwrap();
            if state.dag_init_phase == "delta_scope_remove" {
                break;
            }
            match state.dag_init_phase.as_str() {
                "new"
                | "full_reset"
                | "manifest_refresh_reset"
                | "manifest_branch_reset"
                | "manifest_copy"
                | "manifest_refresh_copy"
                | "manifest_reset"
                | "manifest_seed" => store.initialize_source_manifest(&state).await.unwrap(),
                phase if phase.starts_with("kind") => store.choose_build_kind().await.unwrap(),
                "delta_remove" => store.populate_delta_removal_batch().await.unwrap(),
                "delta_seed" => store.populate_delta_node_batch().await.unwrap(),
                "scope" => {
                    store.process_scope_batch().await.unwrap();
                }
                phase => panic!("unexpected pre-removal phase {phase}"),
            }
        }
        assert_eq!(
            store.dag_run_state().await.unwrap().dag_init_phase,
            "delta_scope_remove"
        );
        store.populate_delta_scope_removal_batch().await.unwrap();

        let run_id = lease.generation();
        let checkpoint = snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT changed.scope_removal_cursor, \
                            changed.scope_removal_complete, \
                            (SELECT COUNT(*) \
                             FROM console_graph_build_scope_tombstones AS removed \
                             WHERE removed.run_id = changed.run_id \
                               AND removed.branch_name = changed.branch_name) \
                                AS tombstone_count \
                     FROM console_graph_build_changed_branches AS changed \
                     WHERE changed.run_id = ? AND changed.branch_name = 'stale'",
                )
                .bind::<BigInt, _>(run_id)
                .get_result::<ScopeRemovalCheckpoint>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("scope-removal-checkpoint"),
                })
            })
            .await
            .unwrap();
        assert_eq!(checkpoint.scope_removal_complete, 0);
        assert!(!checkpoint.scope_removal_cursor.is_empty());
        assert_eq!(checkpoint.tombstone_count, 128);

        assert!(snapshots.pause_incremental_build(&lease).await.unwrap());
        drop(store);
        let resumed = snapshots
            .acquire_incremental_build_lease(2)
            .await
            .unwrap()
            .unwrap();
        assert!(resumed.is_resumed());
        assert_eq!(resumed.generation(), lease.generation());
        let stats =
            build_incremental_generation(&snapshots, &root, baseline_generation, &resumed, 2)
                .await
                .unwrap();
        assert!(stats.reused_baseline);
        snapshots
            .publish_incremental_generation(&resumed, 2)
            .await
            .unwrap();

        let expected = build_graph_snapshot_with_mode(&writer, 2, GraphMode::Anchors)
            .await
            .unwrap();
        let actual = snapshots
            .latest_viewport(GraphMode::Anchors, GraphViewportRequest::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot_shape(&expected), viewport_shape(&actual));
    }

    #[tokio::test]
    async fn single_node_append_stages_delta_not_large_baseline() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let mut head = root.clone();
        for index in 0..260 {
            head = append_text(&writer, &head, &format!("baseline-{index}")).await;
        }
        writer.fork("main", &head).await.unwrap();

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

        let appended = append_text(&writer, &head, "delta").await;
        writer
            .set_branch_head("main", &head, &appended)
            .await
            .unwrap();
        refresh_source_index(&source, &mut index).await;

        let lease = snapshots
            .acquire_incremental_build_lease(2)
            .await
            .unwrap()
            .unwrap();
        let stats = build_incremental_generation(
            &snapshots,
            &root,
            snapshots.active_generation().await.unwrap(),
            &lease,
            2,
        )
        .await
        .unwrap();
        assert!(stats.reused_baseline);
        assert_eq!(stats.processed_nodes, 1);

        let run_id = lease.generation();
        let staging = snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT \
                         (SELECT COUNT(*) FROM console_graph_node_locations \
                          WHERE generation = ?) AS materialized_nodes, \
                         (SELECT COUNT(*) FROM console_graph_edge_routes \
                          WHERE generation = ?) AS materialized_edges, \
                         (SELECT COUNT(*) FROM console_graph_build_rank_slots \
                          WHERE run_id = ?) AS rank_slots, \
                         (SELECT COUNT(*) FROM console_graph_build_edge_ports \
                          WHERE run_id = ?) AS edge_ports, \
                         (SELECT COUNT(*) FROM console_graph_build_nodes \
                          WHERE run_id = ?) AS work_nodes, \
                         (SELECT COUNT(*) FROM console_graph_build_source_manifest \
                          WHERE run_id = ?) AS source_branches, \
                         (SELECT COUNT(*) FROM console_graph_build_source_refresh_manifest \
                          WHERE run_id = ?) AS source_refreshes, \
                         (SELECT COUNT(*) FROM console_graph_build_delta_nodes \
                          WHERE run_id = ?) AS delta_nodes",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<BigInt, _>(run_id)
                .bind::<BigInt, _>(run_id)
                .bind::<BigInt, _>(run_id)
                .bind::<BigInt, _>(run_id)
                .bind::<BigInt, _>(run_id)
                .bind::<BigInt, _>(run_id)
                .bind::<BigInt, _>(run_id)
                .get_result::<DeltaStagingCounts>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("delta-staging-counts"),
                })
            })
            .await
            .unwrap();
        assert!(staging.materialized_nodes <= 2);
        assert!(staging.materialized_edges <= 2);
        assert!(staging.rank_slots <= 2);
        assert!(staging.edge_ports <= 2);
        assert_eq!(staging.work_nodes, 1);
        assert_eq!(staging.source_branches, 1);
        assert_eq!(staging.source_refreshes, 1);
        assert_eq!(staging.delta_nodes, 1);

        snapshots
            .publish_incremental_generation(&lease, 2)
            .await
            .unwrap();
        let active_generation = snapshots.active_generation().await.unwrap();
        let (actual_ticks, full_projection_ticks) = snapshots
            .with_connection(move |connection| {
                (|| -> QueryResult<_> {
                    let actual = diesel::sql_query(
                        "SELECT sample_index, node_id, node_target, x, y, \
                                created_at, created_at_ns \
                         FROM console_graph_materialization_time_ticks \
                         WHERE generation = ? AND mode = 'all' \
                         ORDER BY sample_index",
                    )
                    .bind::<BigInt, _>(active_generation)
                    .load::<TickParityRow>(connection)?;
                    let expected = diesel::sql_query(
                        "WITH ordered_nodes AS ( \
                             SELECT node_id, node_target, x, y, created_at, created_at_ns, \
                                    ROW_NUMBER() OVER ( \
                                        ORDER BY created_at_ns, node_id \
                                    ) AS row_number, \
                                    COUNT(*) OVER () AS node_count \
                             FROM console_graph_node_locations \
                             WHERE generation = ? AND mode = 'all' \
                         ) \
                         SELECT CAST( \
                                    CASE WHEN node_count <= 256 THEN row_number - 1 \
                                         ELSE (row_number - 1) * 255 / (node_count - 1) \
                                    END AS INTEGER \
                                ) AS sample_index, \
                                node_id, node_target, x, y, created_at, created_at_ns \
                         FROM ordered_nodes \
                         WHERE node_count <= 256 OR row_number = 1 OR \
                               ((row_number - 1) * 255 / (node_count - 1)) > \
                               ((row_number - 2) * 255 / (node_count - 1)) \
                         ORDER BY row_number LIMIT 256",
                    )
                    .bind::<BigInt, _>(active_generation)
                    .load::<TickParityRow>(connection)?;
                    Ok((actual, expected))
                })()
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("append-tick-parity"),
                })
            })
            .await
            .unwrap();
        assert_eq!(actual_ticks, full_projection_ticks);
    }

    #[tokio::test]
    async fn context_rollover_delta_matches_full_anchor_scope() {
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
        assert!(rollover.reused_baseline);

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
    async fn provenance_change_delta_preserves_union_without_cross_context_edge() {
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
        assert!(rebuilt.reused_baseline);

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
        assert!(rewind.reused_baseline);
        assert_eq!(rewind.processed_nodes, 0);
        snapshots
            .publish_incremental_generation(&rewind_lease, 4)
            .await
            .unwrap();
        let rewind_view = snapshots
            .latest_viewport(GraphMode::All, GraphViewportRequest::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rewind_view.nodes.len(), 2);
        assert!(
            rewind_view
                .nodes
                .iter()
                .all(|node| { node.id != suffix_one && node.id != suffix_two })
        );
        assert_eq!(
            {
                let node = rewind_view
                    .nodes
                    .iter()
                    .find(|node| node.id == published_head)
                    .unwrap();
                (node.x, node.y)
            },
            published_point
        );
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

    async fn parent_requirement_checkpoint(
        snapshots: &ConsoleGraphSnapshotStore,
        run_id: i64,
        node_id: &str,
    ) -> ParentRequirementCheckpoint {
        let node_id = node_id.to_owned();
        snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT nodes.remaining_parents, nodes.parent_requirement_cursor, \
                            nodes.parent_requirements_complete, \
                            runs.dag_discovered_node_count AS discovered_node_count \
                     FROM console_graph_build_nodes AS nodes \
                     INNER JOIN console_graph_build_runs AS runs ON runs.run_id = nodes.run_id \
                     WHERE nodes.run_id = ? AND nodes.node_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(node_id)
                .get_result::<ParentRequirementCheckpoint>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("parent-requirement-checkpoint"),
                })
            })
            .await
            .unwrap()
    }

    async fn parent_requirement_transaction_checkpoint(
        snapshots: &ConsoleGraphSnapshotStore,
        run_id: i64,
        node_id: &str,
    ) -> ParentRequirementTransactionCheckpoint {
        let node_id = node_id.to_owned();
        snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT ( \
                         SELECT COUNT(*) FROM console_graph_build_nodes \
                         WHERE run_id = ? AND node_id = ? \
                     ) AS work_node_count, \
                     dag_discovered_node_count AS discovered_node_count \
                     FROM console_graph_build_runs WHERE run_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(node_id)
                .bind::<BigInt, _>(run_id)
                .get_result::<ParentRequirementTransactionCheckpoint>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("parent-requirement-transaction-checkpoint"),
                })
            })
            .await
            .unwrap()
    }

    async fn child_page_transaction_checkpoint(
        snapshots: &ConsoleGraphSnapshotStore,
        run_id: i64,
        parent_id: &str,
        child_id: &str,
    ) -> ChildPageTransactionCheckpoint {
        let parent_id = parent_id.to_owned();
        let child_id = child_id.to_owned();
        snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT nodes.remaining_parents, ( \
                         SELECT COUNT(*) \
                         FROM console_graph_build_parent_satisfactions AS satisfactions \
                         WHERE satisfactions.run_id = nodes.run_id \
                           AND satisfactions.parent_id = ? \
                           AND satisfactions.child_id = nodes.node_id \
                     ) AS satisfaction_count, expansions.next_child_id, expansions.complete \
                     FROM console_graph_build_nodes AS nodes \
                     INNER JOIN console_graph_build_parent_expansions AS expansions \
                         ON expansions.run_id = nodes.run_id \
                        AND expansions.parent_id = ? \
                     WHERE nodes.run_id = ? AND nodes.node_id = ?",
                )
                .bind::<Text, _>(&parent_id)
                .bind::<Text, _>(&parent_id)
                .bind::<BigInt, _>(run_id)
                .bind::<Text, _>(child_id)
                .get_result::<ChildPageTransactionCheckpoint>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("child-page-transaction-checkpoint"),
                })
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
