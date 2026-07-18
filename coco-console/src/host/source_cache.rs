use std::collections::{BTreeSet, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use coco_mem::{
    BranchAppendSessionState, BranchStore, GRAPH_READ_BATCH_SIZE, GraphBranchPageCursor,
    GraphBranchRecord, GraphChildPageCursor, GraphMutationBranchChangeKind,
    GraphMutationBranchChangePageCursor, GraphMutationDirtyParentPageCursor, Kind, NewNode,
    NewNodeContent, Node, NodeStore, SessionAnchorPatch, SessionState, SessionStore,
    SqliteGraphStore, StoreError, StoreResult,
};
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Integer, Nullable, Text};
use snafu::prelude::*;

use super::publisher::ConsoleInvalidationBatch;
use super::snapshot_store::{ConsoleGraphSnapshotStore, SnapshotDatabase};
use crate::error::{
    ParseGraphSnapshotStoreValueSnafu, QueryGraphSnapshotStoreSnafu,
    SerializeGraphSnapshotStoreValueSnafu,
};
use crate::schema::{
    console_graph_source_branch_nodes, console_graph_source_branches,
    console_graph_source_node_relations, console_graph_source_nodes,
    console_graph_source_refresh_queue, console_graph_source_state,
};

const SOURCE_CACHE_BATCH_SIZE: usize = GRAPH_READ_BATCH_SIZE;
const SOURCE_REFRESH_CLEANUP_CANDIDATE_BATCH_SIZE: usize = SOURCE_CACHE_BATCH_SIZE;
const SOURCE_REFRESH_CLEANUP_DELETE_BATCH_SIZE: usize = SOURCE_CACHE_BATCH_SIZE;
const SOURCE_REFRESH_CLEANUP_DEPENDENT_TABLES: [(&str, &str); 4] = [
    ("console_graph_source_refresh_queue", "refresh_id"),
    ("console_graph_source_refresh_dirty_seeds", "refresh_id"),
    (
        "console_graph_source_child_rechecks",
        "contribution_generation",
    ),
    (
        "console_graph_source_branch_nodes",
        "contribution_generation",
    ),
];
const SOURCE_CHILD_RECHECK_PAGE_SIZE: usize = 128;
const SOURCE_DYNAMIC_BRANCH_CANDIDATE_PAGE_SIZE: usize = 128;
const SOURCE_PARENT_PAGE_SIZE: usize = 128;
const SOURCE_WORK_SUPERSEDE_BATCH_SIZE: usize = 128;
const SOURCE_DIRTY_SEED_LIMIT: usize = 1;
const SOURCE_LEASE_DURATION_MS: i64 = 30_000;
const TARGETED_DYNAMIC_BRANCH_LIMIT: usize = GRAPH_READ_BATCH_SIZE;
const TRAVERSAL_GRAPH: &str = "graph";
const TRAVERSAL_SKILL_SUBTREE: &str = "skill_subtree";
const DURABLE_MUTATION_INCARNATION: &str = "graph-mutation-journal-v1";
const DURABLE_FULL_INCARNATION: &str = "graph-mutation-full-v1";

const DYNAMIC_SCAN_RAW_UPPER_SQL: &str = "SELECT refresh.refresh_id AS contribution_generation \
     FROM console_graph_source_refresh_runs AS refresh \
          INDEXED BY console_graph_source_refresh_runs_published_raw_upper_idx \
     WHERE refresh.status = 'published' \
       AND refresh.published_source_revision <= ? \
     ORDER BY refresh.refresh_id DESC LIMIT 1";

const DYNAMIC_BRANCH_RAW_PAGE_SQL: &str = "SELECT recheck.branch_name, \
            recheck.contribution_generation AS refresh_id, \
            recheck.traversal_kind, \
            CASE WHEN EXISTS ( \
                SELECT 1 \
                FROM console_graph_source_refresh_runs AS refresh \
                INNER JOIN console_graph_source_branch_history AS history \
                    ON history.branch_name = refresh.branch_name \
                   AND history.contribution_generation = \
                       refresh.target_contribution_generation \
                   AND history.removed = 0 \
                WHERE refresh.refresh_id = recheck.contribution_generation \
                  AND refresh.branch_name = recheck.branch_name \
                  AND refresh.status = 'published' \
                  AND refresh.published_source_revision <= ? \
                  AND history.source_revision = ( \
                      SELECT candidate.source_revision \
                      FROM console_graph_source_branch_history AS candidate \
                      WHERE candidate.branch_name = recheck.branch_name \
                        AND candidate.source_revision <= ? \
                      ORDER BY candidate.source_revision DESC LIMIT 1 \
                  ) \
            ) THEN 1 ELSE 0 END AS eligible \
     FROM console_graph_source_child_rechecks AS recheck \
          INDEXED BY console_graph_source_child_rechecks_node_raw_idx \
     WHERE recheck.node_id = ? \
       AND recheck.contribution_generation <= ? \
     ORDER BY recheck.branch_name, recheck.contribution_generation, \
              recheck.traversal_kind \
     LIMIT ?";

const DYNAMIC_BRANCH_RAW_PAGE_AFTER_SQL: &str = "SELECT recheck.branch_name, \
            recheck.contribution_generation AS refresh_id, \
            recheck.traversal_kind, \
            CASE WHEN EXISTS ( \
                SELECT 1 \
                FROM console_graph_source_refresh_runs AS refresh \
                INNER JOIN console_graph_source_branch_history AS history \
                    ON history.branch_name = refresh.branch_name \
                   AND history.contribution_generation = \
                       refresh.target_contribution_generation \
                   AND history.removed = 0 \
                WHERE refresh.refresh_id = recheck.contribution_generation \
                  AND refresh.branch_name = recheck.branch_name \
                  AND refresh.status = 'published' \
                  AND refresh.published_source_revision <= ? \
                  AND history.source_revision = ( \
                      SELECT candidate.source_revision \
                      FROM console_graph_source_branch_history AS candidate \
                      WHERE candidate.branch_name = recheck.branch_name \
                        AND candidate.source_revision <= ? \
                      ORDER BY candidate.source_revision DESC LIMIT 1 \
                  ) \
            ) THEN 1 ELSE 0 END AS eligible \
     FROM console_graph_source_child_rechecks AS recheck \
          INDEXED BY console_graph_source_child_rechecks_node_raw_idx \
     WHERE recheck.node_id = ? \
       AND recheck.contribution_generation <= ? \
       AND (recheck.branch_name, recheck.contribution_generation, \
            recheck.traversal_kind) > (?, ?, ?) \
     ORDER BY recheck.branch_name, recheck.contribution_generation, \
              recheck.traversal_kind \
     LIMIT ?";

const DYNAMIC_ORIGIN_RAW_PAGE_SQL: &str = "SELECT recheck.node_id, recheck.traversal_kind, \
            recheck.contribution_generation AS refresh_id, \
            CASE WHEN EXISTS ( \
                SELECT 1 \
                FROM console_graph_source_refresh_runs AS refresh \
                INNER JOIN console_graph_source_branch_history AS history \
                    ON history.branch_name = refresh.branch_name \
                   AND history.contribution_generation = \
                       refresh.target_contribution_generation \
                   AND history.removed = 0 \
                WHERE refresh.refresh_id = recheck.contribution_generation \
                  AND refresh.branch_name = recheck.branch_name \
                  AND refresh.status = 'published' \
                  AND refresh.published_source_revision <= ? \
                  AND history.source_revision = ( \
                      SELECT candidate.source_revision \
                      FROM console_graph_source_branch_history AS candidate \
                      WHERE candidate.branch_name = recheck.branch_name \
                        AND candidate.source_revision <= ? \
                      ORDER BY candidate.source_revision DESC LIMIT 1 \
                  ) \
            ) THEN 1 ELSE 0 END AS eligible \
     FROM console_graph_source_child_rechecks AS recheck \
          INDEXED BY console_graph_source_child_rechecks_branch_order_idx \
     WHERE recheck.branch_name = ? \
       AND recheck.contribution_generation <= ? \
     ORDER BY recheck.node_id, recheck.traversal_kind, \
              recheck.contribution_generation \
     LIMIT ?";

const DYNAMIC_ORIGIN_RAW_PAGE_AFTER_SQL: &str = "SELECT recheck.node_id, recheck.traversal_kind, \
            recheck.contribution_generation AS refresh_id, \
            CASE WHEN EXISTS ( \
                SELECT 1 \
                FROM console_graph_source_refresh_runs AS refresh \
                INNER JOIN console_graph_source_branch_history AS history \
                    ON history.branch_name = refresh.branch_name \
                   AND history.contribution_generation = \
                       refresh.target_contribution_generation \
                   AND history.removed = 0 \
                WHERE refresh.refresh_id = recheck.contribution_generation \
                  AND refresh.branch_name = recheck.branch_name \
                  AND refresh.status = 'published' \
                  AND refresh.published_source_revision <= ? \
                  AND history.source_revision = ( \
                      SELECT candidate.source_revision \
                      FROM console_graph_source_branch_history AS candidate \
                      WHERE candidate.branch_name = recheck.branch_name \
                        AND candidate.source_revision <= ? \
                      ORDER BY candidate.source_revision DESC LIMIT 1 \
                  ) \
            ) THEN 1 ELSE 0 END AS eligible \
     FROM console_graph_source_child_rechecks AS recheck \
          INDEXED BY console_graph_source_child_rechecks_branch_order_idx \
     WHERE recheck.branch_name = ? \
       AND recheck.contribution_generation <= ? \
       AND (recheck.node_id, recheck.traversal_kind, \
            recheck.contribution_generation) > (?, ?, ?) \
     ORDER BY recheck.node_id, recheck.traversal_kind, \
              recheck.contribution_generation \
     LIMIT ?";

const DYNAMIC_DIRTY_SCAN_FOR_EVENT_SQL: &str = "SELECT scan_id AS contribution_generation \
     FROM console_graph_source_dynamic_branch_scans \
          INDEXED BY console_graph_source_dynamic_branch_scans_mutation_idx \
     WHERE scan_kind = 'dirty_parent' AND mutation_revision = ? \
     ORDER BY scan_id LIMIT 1";

const DISCARDING_MUTATION_EVENT_FIRST_SQL: &str = "SELECT revision AS contribution_generation \
     FROM console_graph_source_mutation_event_runs \
          INDEXED BY console_graph_source_mutation_event_runs_cleanup_idx \
     WHERE phase = 'discarding' ORDER BY revision LIMIT 1";

const DISCARDING_MUTATION_EVENT_THROUGH_SQL: &str = "SELECT revision AS contribution_generation \
     FROM console_graph_source_mutation_event_runs \
          INDEXED BY console_graph_source_mutation_event_runs_cleanup_idx \
     WHERE phase = 'discarding' AND revision <= ? ORDER BY revision LIMIT 1";

const MUTATION_EVENT_CLEANUP_FIRST_SQL: &str = "SELECT revision AS contribution_generation \
     FROM console_graph_source_mutation_event_runs \
     ORDER BY revision LIMIT 1";

const MUTATION_EVENT_CLEANUP_THROUGH_SQL: &str = "SELECT revision AS contribution_generation \
     FROM console_graph_source_mutation_event_runs \
     WHERE revision <= ? ORDER BY revision LIMIT 1";

const SOURCE_REFRESH_CLEANUP_CANDIDATE_PAGE_SQL: &str = "SELECT refresh_id, status \
     FROM console_graph_source_refresh_runs \
     WHERE refresh_id > ? AND refresh_id <= ? \
     ORDER BY refresh_id LIMIT ?";

const SOURCE_REFRESH_CLEANUP_PROTECTED_SQL: &str = "SELECT CASE WHEN \
         EXISTS ( \
             SELECT 1 \
             FROM console_graph_source_refresh_runs AS refresh \
             INNER JOIN console_graph_source_branch_publications AS publication \
                 ON publication.branch_name = refresh.branch_name \
                AND publication.target_contribution_generation = \
                    refresh.target_contribution_generation \
             WHERE refresh.refresh_id = ? \
               AND refresh.status = 'published' \
               AND refresh.published_source_revision <= publication.source_revision \
         ) OR EXISTS ( \
             SELECT 1 \
             FROM console_graph_source_refresh_runs AS candidate \
             INNER JOIN console_graph_source_refresh_runs AS dependent \
                 ON dependent.target_contribution_generation = \
                    candidate.target_contribution_generation \
             WHERE candidate.refresh_id = ? AND dependent.status = 'building' \
         ) OR EXISTS ( \
             SELECT 1 \
             FROM console_graph_build_source_refresh_manifest AS manifest \
             INNER JOIN console_graph_build_runs AS build \
                 ON build.run_id = manifest.run_id \
             WHERE manifest.refresh_id = ? \
               AND build.status IN ('building', 'paused') \
         ) OR EXISTS ( \
             SELECT 1 \
             FROM console_graph_source_branch_change_journal AS change \
             WHERE change.refresh_id = ? \
               AND change.source_revision > COALESCE( \
                   (SELECT revision.source_revision \
                    FROM console_graph_generation_state AS state \
                    INNER JOIN console_graph_generation_source_revisions AS revision \
                        ON revision.generation = state.active_generation \
                    WHERE state.id = 1), \
                   -1 \
               ) \
         ) OR EXISTS ( \
             SELECT 1 \
             FROM console_graph_source_refresh_runs AS refresh \
             INNER JOIN console_graph_build_runs AS build \
                 ON build.status IN ('building', 'paused') \
             INNER JOIN console_graph_source_branch_history AS history \
                 ON history.branch_name = refresh.branch_name \
                AND history.source_revision = ( \
                    SELECT MAX(candidate.source_revision) \
                    FROM console_graph_source_branch_history AS candidate \
                    WHERE candidate.branch_name = refresh.branch_name \
                      AND candidate.source_revision <= build.dag_source_revision \
                ) \
                AND history.removed = 0 \
                AND history.contribution_generation = \
                    refresh.target_contribution_generation \
             WHERE refresh.refresh_id = ? \
         ) OR EXISTS ( \
             SELECT 1 \
             FROM console_graph_source_refresh_runs AS refresh \
             INNER JOIN console_graph_source_dynamic_branch_scans AS scan \
                 ON scan.status = 'building' \
                AND refresh.refresh_id <= scan.raw_refresh_id_upper_bound \
                AND refresh.published_source_revision <= scan.source_revision \
             INNER JOIN console_graph_source_branch_history AS history \
                 ON history.branch_name = refresh.branch_name \
                AND history.source_revision = ( \
                    SELECT MAX(candidate.source_revision) \
                    FROM console_graph_source_branch_history AS candidate \
                    WHERE candidate.branch_name = refresh.branch_name \
                      AND candidate.source_revision <= scan.source_revision \
                ) \
                AND history.removed = 0 \
                AND history.contribution_generation = \
                    refresh.target_contribution_generation \
             WHERE refresh.refresh_id = ? \
               AND (scan.scan_kind = 'dirty_parent' \
                    OR scan.lease_expires_at_ms > ?) \
         ) OR EXISTS ( \
             SELECT 1 \
             FROM console_graph_source_refresh_runs AS refresh \
             INNER JOIN console_graph_build_source_manifest AS manifest \
                 ON manifest.contribution_generation = \
                    refresh.target_contribution_generation \
             INNER JOIN console_graph_build_runs AS build \
                 ON build.run_id = manifest.run_id \
             WHERE refresh.refresh_id = ? \
               AND build.status IN ('building', 'paused') \
         ) OR EXISTS ( \
             SELECT 1 \
             FROM console_graph_source_refresh_runs AS refresh \
             INNER JOIN console_graph_materialization_branches AS materialized \
                 ON materialized.contribution_generation = \
                    refresh.target_contribution_generation \
             WHERE refresh.refresh_id = ? \
         ) THEN 1 ELSE 0 END AS value";

fn source_refresh_cleanup_delete_sql(table: &str, refresh_column: &str) -> String {
    format!(
        "DELETE FROM {table} WHERE rowid IN ( \
             SELECT rowid FROM {table} \
             WHERE {refresh_column} = ? LIMIT ? \
         )"
    )
}

#[derive(Debug)]
pub(crate) struct PersistentGraphIndex {
    root_id: String,
    database: SnapshotDatabase,
    path: PathBuf,
    owner_id: String,
    #[cfg(test)]
    refresh_count: usize,
    #[cfg(test)]
    branch_refresh_count: usize,
    #[cfg(test)]
    traversed_node_count: usize,
    #[cfg(test)]
    branch_refresh_history: Vec<String>,
    #[cfg(test)]
    fail_next_branch_refresh: Option<String>,
    #[cfg(test)]
    targeted_dynamic_branch_limit: usize,
    #[cfg(test)]
    full_refresh_branch_page_size: NonZeroUsize,
    #[cfg(test)]
    full_refresh_source_page_count: usize,
    #[cfg(test)]
    fail_next_child_recheck_page_checkpoint: bool,
    #[cfg(test)]
    child_recheck_page_cursors: Vec<Option<GraphChildPageCursor>>,
}

#[derive(Clone, Debug)]
pub(crate) struct PersistentGraphStore {
    root_id: String,
    database: SnapshotDatabase,
    path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum TraversalKind {
    Graph,
    SkillSubtree,
}

#[derive(Debug)]
enum DynamicBranchScope {
    Targeted(BTreeSet<String>),
    FullRefresh,
}

#[derive(Debug, PartialEq, Eq)]
enum DynamicBranchResultStep {
    Page(Vec<String>),
    CleanupPending,
    Complete,
}

impl TraversalKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Graph => TRAVERSAL_GRAPH,
            Self::SkillSubtree => TRAVERSAL_SKILL_SUBTREE,
        }
    }

    fn parse(value: &str) -> crate::Result<Self> {
        match value {
            TRAVERSAL_GRAPH => Ok(Self::Graph),
            TRAVERSAL_SKILL_SUBTREE => Ok(Self::SkillSubtree),
            _ => crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "traversal_kind",
                value: value.to_owned(),
            }
            .fail(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct QueueItem {
    node_id: String,
    traversal: TraversalKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DirtySeed {
    node_id: String,
    child_high_watermark: Option<GraphChildPageCursor>,
}

#[derive(Clone, Copy, Debug)]
struct SourceInvalidation<'a> {
    incarnation: &'a str,
    version: i64,
    relation_revision: i64,
}

#[derive(Debug)]
struct RefreshWorkItem {
    item: QueueItem,
    node_committed: bool,
    force_child_scan: bool,
    child_cursor: Option<GraphChildPageCursor>,
    parent_cursor_offset: i64,
    parent_traversal_complete: bool,
    child_scan_required: bool,
    child_high_watermark_frozen: bool,
    child_high_watermark: Option<GraphChildPageCursor>,
}

#[derive(Debug)]
struct RefreshRun {
    refresh_id: i64,
    target_contribution_generation: i64,
    base_generation: Option<i64>,
    owner_id: String,
    lease_epoch: i64,
    target_invalidation_version: i64,
    target_invalidation_incarnation: String,
    target_invalidation_kind: String,
    relation_revision: i64,
    resumed: bool,
}

#[derive(Debug)]
struct PublishedBranch {
    head_id: String,
    state_json: String,
    contribution_generation: i64,
    source_revision: i64,
}

struct RefreshStart<'a> {
    record: &'a GraphBranchRecord,
    base_generation: Option<i64>,
    invalidation: SourceInvalidation<'a>,
    target_invalidation_kind: &'a str,
    previous: Option<&'a PublishedBranch>,
    initial_work: &'a [(QueueItem, bool)],
    dirty_seeds: &'a [DirtySeed],
}

#[derive(Debug, QueryableByName)]
struct PublishedBranchRow {
    #[diesel(sql_type = Text)]
    head_id: String,
    #[diesel(sql_type = Text)]
    state_json: String,
    #[diesel(sql_type = BigInt)]
    contribution_generation: i64,
    #[diesel(sql_type = BigInt)]
    source_revision: i64,
}

#[derive(Debug)]
struct PersistedSourceNode {
    node_id: String,
    parent_id: String,
    node_json: String,
}

#[derive(Debug, QueryableByName)]
struct NodeIdRow {
    #[diesel(sql_type = Text)]
    node_id: String,
}

#[derive(Debug, QueryableByName)]
struct GenerationRow {
    #[diesel(sql_type = BigInt)]
    contribution_generation: i64,
}

#[derive(Debug, QueryableByName)]
struct SourceRefreshCleanupStateRow {
    #[diesel(sql_type = Nullable<BigInt>)]
    upper_bound_refresh_id: Option<i64>,
    #[diesel(sql_type = BigInt)]
    raw_refresh_id_cursor: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    active_refresh_id: Option<i64>,
}

#[derive(Debug, QueryableByName)]
struct SourceRefreshCleanupCandidateRow {
    #[diesel(sql_type = BigInt)]
    refresh_id: i64,
    #[diesel(sql_type = Text)]
    status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceRefreshCleanupStep {
    Continue,
    RoundComplete,
}

#[derive(Debug, QueryableByName)]
struct NodeJsonRow {
    #[diesel(sql_type = Text)]
    node_json: String,
}

#[derive(Debug, QueryableByName)]
struct ChildRecheckRow {
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Text)]
    traversal_kind: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ChildRecheckRawCursor {
    item: QueueItem,
    refresh_id: i64,
}

#[derive(Debug, QueryableByName)]
struct ChildRecheckRawRow {
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Text)]
    traversal_kind: String,
    #[diesel(sql_type = BigInt)]
    refresh_id: i64,
    #[diesel(sql_type = Integer)]
    eligible: i32,
}

#[derive(Debug)]
struct ChildRecheckRawPage {
    rows: Vec<(ChildRecheckRawCursor, bool)>,
    complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DynamicBranchRawCursor {
    branch_name: String,
    refresh_id: i64,
    traversal: TraversalKind,
}

#[derive(Debug, QueryableByName)]
struct DynamicBranchRawRow {
    #[diesel(sql_type = Text)]
    branch_name: String,
    #[diesel(sql_type = BigInt)]
    refresh_id: i64,
    #[diesel(sql_type = Text)]
    traversal_kind: String,
    #[diesel(sql_type = Integer)]
    eligible: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DynamicOriginRawCursor {
    node_id: String,
    traversal: TraversalKind,
    refresh_id: i64,
}

#[derive(Debug, QueryableByName)]
struct DynamicOriginRawRow {
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Text)]
    traversal_kind: String,
    #[diesel(sql_type = BigInt)]
    refresh_id: i64,
    #[diesel(sql_type = Integer)]
    eligible: i32,
}

#[derive(Clone, Debug)]
struct DynamicBranchScan {
    scan_id: i64,
    scan_kind: String,
    request_key: String,
    source_revision: i64,
    raw_refresh_id_upper_bound: i64,
    status: String,
    origin_branch_cursor: Option<String>,
    active_origin_branch_name: Option<String>,
    origin_raw_cursor: Option<DynamicOriginRawCursor>,
    completed_origin_node_id: Option<String>,
    active_origin_node_id: Option<String>,
    candidate_raw_cursor: Option<DynamicBranchRawCursor>,
    result_count: i64,
    exceeded_limit: bool,
    owner_id: String,
    lease_epoch: i64,
}

#[derive(Debug, QueryableByName)]
struct DynamicBranchScanRow {
    #[diesel(sql_type = BigInt)]
    scan_id: i64,
    #[diesel(sql_type = Text)]
    scan_kind: String,
    #[diesel(sql_type = Text)]
    request_key: String,
    #[diesel(sql_type = BigInt)]
    source_revision: i64,
    #[diesel(sql_type = BigInt)]
    raw_refresh_id_upper_bound: i64,
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Nullable<Text>)]
    origin_branch_cursor: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    active_origin_branch_name: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    origin_raw_node_cursor: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    origin_raw_traversal_cursor: Option<String>,
    #[diesel(sql_type = Nullable<BigInt>)]
    origin_raw_refresh_id_cursor: Option<i64>,
    #[diesel(sql_type = Nullable<Text>)]
    completed_origin_node_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    active_origin_node_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    candidate_raw_branch_cursor: Option<String>,
    #[diesel(sql_type = Nullable<BigInt>)]
    candidate_raw_refresh_id_cursor: Option<i64>,
    #[diesel(sql_type = Nullable<Text>)]
    candidate_raw_traversal_cursor: Option<String>,
    #[diesel(sql_type = BigInt)]
    result_count: i64,
    #[diesel(sql_type = Integer)]
    exceeded_limit: i32,
    #[diesel(sql_type = Text)]
    owner_id: String,
    #[diesel(sql_type = BigInt)]
    lease_epoch: i64,
    #[diesel(sql_type = BigInt)]
    lease_expires_at_ms: i64,
}

#[derive(Debug, QueryableByName)]
struct BranchRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    state_json: String,
}

#[derive(Debug, QueryableByName)]
struct BranchNameRow {
    #[diesel(sql_type = Text)]
    name: String,
}

#[derive(Debug, QueryableByName)]
struct DeletedBranchRow {
    #[diesel(sql_type = Text)]
    head_id: String,
    #[diesel(sql_type = Text)]
    state_json: String,
    #[diesel(sql_type = BigInt)]
    contribution_generation: i64,
}

#[derive(Debug, QueryableByName)]
struct RefreshRunRow {
    #[diesel(sql_type = BigInt)]
    refresh_id: i64,
    #[diesel(sql_type = BigInt)]
    target_contribution_generation: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    base_generation: Option<i64>,
    #[diesel(sql_type = BigInt)]
    lease_epoch: i64,
    #[diesel(sql_type = BigInt)]
    lease_expires_at_ms: i64,
    #[diesel(sql_type = BigInt)]
    target_invalidation_version: i64,
    #[diesel(sql_type = Text)]
    target_invalidation_incarnation: String,
    #[diesel(sql_type = Text)]
    target_invalidation_kind: String,
    #[diesel(sql_type = BigInt)]
    relation_revision: i64,
}

#[derive(Debug, QueryableByName)]
struct RefreshExpectedBranchRow {
    #[diesel(sql_type = Nullable<Text>)]
    expected_branch_head_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    expected_branch_state_json: Option<String>,
    #[diesel(sql_type = Nullable<BigInt>)]
    expected_branch_contribution_generation: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    expected_branch_source_revision: Option<i64>,
    #[diesel(sql_type = Integer)]
    expected_branch_absent: i32,
}

#[derive(Debug, QueryableByName)]
struct InvalidationReceiptRow {
    #[diesel(sql_type = BigInt)]
    receipt_id: i64,
    #[diesel(sql_type = Text)]
    target_head_id: String,
    #[diesel(sql_type = Text)]
    target_state_json: String,
    #[diesel(sql_type = Text)]
    invalidation_kind: String,
    #[diesel(sql_type = BigInt)]
    source_revision: i64,
    #[diesel(sql_type = BigInt)]
    relation_revision: i64,
}

#[derive(Debug, QueryableByName)]
struct InvalidationReceiptSeedRow {
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Nullable<BigInt>)]
    child_high_watermark_relation_revision: Option<i64>,
    #[diesel(sql_type = Nullable<Text>)]
    child_high_watermark_node_id: Option<String>,
}

#[derive(Debug, QueryableByName)]
struct ReceiptIdRow {
    #[diesel(sql_type = BigInt)]
    receipt_id: i64,
}

#[derive(Debug, QueryableByName)]
struct ActiveRefreshRecordRow {
    #[diesel(sql_type = Text)]
    target_head_id: String,
    #[diesel(sql_type = Text)]
    target_state_json: String,
}

#[derive(Debug, QueryableByName)]
struct RefreshWorkRow {
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Text)]
    traversal_kind: String,
    #[diesel(sql_type = Integer)]
    node_committed: i32,
    #[diesel(sql_type = Integer)]
    force_child_scan: i32,
    #[diesel(sql_type = Nullable<BigInt>)]
    child_cursor_relation_revision: Option<i64>,
    #[diesel(sql_type = Nullable<Text>)]
    child_cursor_node_id: Option<String>,
    #[diesel(sql_type = BigInt)]
    parent_cursor_offset: i64,
    #[diesel(sql_type = Integer)]
    parent_traversal_complete: i32,
    #[diesel(sql_type = Integer)]
    child_scan_required: i32,
    #[diesel(sql_type = Integer)]
    child_high_watermark_frozen: i32,
    #[diesel(sql_type = Nullable<BigInt>)]
    child_high_watermark_relation_revision: Option<i64>,
    #[diesel(sql_type = Nullable<Text>)]
    child_high_watermark_node_id: Option<String>,
}

#[derive(Debug, QueryableByName)]
struct FlagRow {
    #[diesel(sql_type = Integer)]
    value: i32,
}

#[derive(Debug, QueryableByName)]
struct NullableNodeIdRow {
    #[diesel(sql_type = Nullable<Text>)]
    node_id: Option<String>,
}

#[derive(Debug, QueryableByName)]
struct OrphanGcStateRow {
    #[diesel(sql_type = Nullable<Text>)]
    scan_cursor: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    scan_upper_bound: Option<String>,
}

#[derive(Debug, QueryableByName)]
struct RelationIngestStateRow {
    #[diesel(sql_type = BigInt)]
    relation_cursor_offset: i64,
    #[diesel(sql_type = Integer)]
    relation_ingest_complete: i32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SourceWorkSupersedeCounts {
    boundaries: usize,
    sweeps: usize,
    refreshes: usize,
}

impl SourceWorkSupersedeCounts {
    fn accumulate(&mut self, batch: Self) {
        self.boundaries = self.boundaries.saturating_add(batch.boundaries);
        self.sweeps = self.sweeps.saturating_add(batch.sweeps);
        self.refreshes = self.refreshes.saturating_add(batch.refreshes);
    }

    fn is_empty(self) -> bool {
        self.boundaries == 0 && self.sweeps == 0 && self.refreshes == 0
    }
}

#[derive(Debug, QueryableByName)]
struct SourceSweepRunRow {
    #[diesel(sql_type = Text)]
    target_invalidation_incarnation: String,
    #[diesel(sql_type = BigInt)]
    target_invalidation_version: i64,
    #[diesel(sql_type = BigInt)]
    relation_revision: i64,
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Text)]
    phase: String,
    #[diesel(sql_type = Nullable<Text>)]
    source_upper_bound: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    page_cursor: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    branch_recheck_node_cursor: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    branch_recheck_traversal_cursor: Option<String>,
    #[diesel(sql_type = Text)]
    owner_id: String,
    #[diesel(sql_type = BigInt)]
    lease_epoch: i64,
    #[diesel(sql_type = BigInt)]
    lease_expires_at_ms: i64,
}

#[derive(Debug)]
struct SourceSweepRun {
    target_invalidation_incarnation: String,
    target_invalidation_version: i64,
    relation_revision: i64,
    phase: String,
    source_upper_bound: Option<String>,
    page_cursor: Option<String>,
    owner_id: String,
    lease_epoch: i64,
}

#[derive(Debug, QueryableByName)]
struct DurableFullBoundaryRow {
    #[diesel(sql_type = BigInt)]
    relation_revision: i64,
    #[diesel(sql_type = Text)]
    requested_scope: String,
}

#[derive(Debug, QueryableByName)]
struct PendingInvalidationBoundaryRow {
    #[diesel(sql_type = Text)]
    target_invalidation_incarnation: String,
    #[diesel(sql_type = BigInt)]
    target_invalidation_version: i64,
    #[diesel(sql_type = BigInt)]
    relation_revision: i64,
}

#[derive(Debug, QueryableByName)]
struct SourceSweepRecheckStateRow {
    #[diesel(sql_type = Nullable<Text>)]
    branch_recheck_node_cursor: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    branch_recheck_traversal_cursor: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    branch_recheck_raw_node_cursor: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    branch_recheck_raw_traversal_cursor: Option<String>,
    #[diesel(sql_type = Nullable<BigInt>)]
    branch_recheck_raw_refresh_id_cursor: Option<i64>,
    #[diesel(sql_type = Nullable<Text>)]
    branch_recheck_active_node_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    branch_recheck_active_traversal_kind: Option<String>,
    #[diesel(sql_type = Nullable<BigInt>)]
    branch_recheck_child_cursor_relation_revision: Option<i64>,
    #[diesel(sql_type = Nullable<Text>)]
    branch_recheck_child_cursor_node_id: Option<String>,
    #[diesel(sql_type = BigInt)]
    lease_epoch: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SourceSweepRecheckState {
    completed_item: Option<QueueItem>,
    raw_cursor: Option<ChildRecheckRawCursor>,
    active_item: Option<QueueItem>,
    child_cursor: Option<GraphChildPageCursor>,
    lease_epoch: Option<i64>,
}

#[derive(Debug, QueryableByName)]
struct MutationJournalStateRow {
    #[diesel(sql_type = BigInt)]
    consumed_revision: i64,
}

#[derive(Debug, QueryableByName)]
struct MutationEventRunRow {
    #[diesel(sql_type = Text)]
    phase: String,
    #[diesel(sql_type = Nullable<Text>)]
    branch_cursor: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    dirty_parent_cursor: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    active_dirty_parent_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    peer_branch_cursor: Option<String>,
}

fn require_source_refresh_fence(
    connection: &mut SqliteConnection,
    refresh_id: i64,
    owner_id: &str,
    lease_epoch: i64,
) -> QueryResult<()> {
    let renewed = diesel::sql_query(
        "UPDATE console_graph_source_refresh_runs SET lease_expires_at_ms = ? \
         WHERE refresh_id = ? AND owner_id = ? AND lease_epoch = ? \
           AND status = 'building'",
    )
    .bind::<BigInt, _>(source_lease_deadline_ms())
    .bind::<BigInt, _>(refresh_id)
    .bind::<Text, _>(owner_id)
    .bind::<BigInt, _>(lease_epoch)
    .execute(connection)?;
    if renewed == 1 {
        Ok(())
    } else {
        Err(diesel::result::Error::NotFound)
    }
}

fn advance_source_revision(connection: &mut SqliteConnection) -> QueryResult<i64> {
    let advanced = diesel::sql_query(
        "UPDATE console_graph_source_identity SET revision = revision + 1 WHERE id = 1",
    )
    .execute(connection)?;
    if advanced != 1 {
        return Err(diesel::result::Error::NotFound);
    }
    diesel::sql_query(
        "SELECT revision AS contribution_generation \
         FROM console_graph_source_identity WHERE id = 1",
    )
    .get_result::<GenerationRow>(connection)
    .map(|row| row.contribution_generation)
}

fn current_source_revision(connection: &mut SqliteConnection) -> QueryResult<i64> {
    diesel::sql_query(
        "SELECT revision AS contribution_generation \
         FROM console_graph_source_identity WHERE id = 1",
    )
    .get_result::<GenerationRow>(connection)
    .map(|row| row.contribution_generation)
}

fn dynamic_scan_fence_lost(error: &crate::Error) -> bool {
    matches!(
        error,
        crate::Error::QueryGraphSnapshotStore {
            source: diesel::result::Error::NotFound,
            ..
        }
    )
}

fn load_dynamic_branch_raw_candidates(
    connection: &mut SqliteConnection,
    node_id: &str,
    source_revision: i64,
    raw_refresh_id_upper_bound: i64,
    after: Option<&DynamicBranchRawCursor>,
) -> QueryResult<Vec<DynamicBranchRawRow>> {
    if let Some(after) = after {
        diesel::sql_query(DYNAMIC_BRANCH_RAW_PAGE_AFTER_SQL)
            .bind::<BigInt, _>(source_revision)
            .bind::<BigInt, _>(source_revision)
            .bind::<Text, _>(node_id)
            .bind::<BigInt, _>(raw_refresh_id_upper_bound)
            .bind::<Text, _>(&after.branch_name)
            .bind::<BigInt, _>(after.refresh_id)
            .bind::<Text, _>(after.traversal.as_str())
            .bind::<BigInt, _>(SOURCE_DYNAMIC_BRANCH_CANDIDATE_PAGE_SIZE as i64)
            .load(connection)
    } else {
        diesel::sql_query(DYNAMIC_BRANCH_RAW_PAGE_SQL)
            .bind::<BigInt, _>(source_revision)
            .bind::<BigInt, _>(source_revision)
            .bind::<Text, _>(node_id)
            .bind::<BigInt, _>(raw_refresh_id_upper_bound)
            .bind::<BigInt, _>(SOURCE_DYNAMIC_BRANCH_CANDIDATE_PAGE_SIZE as i64)
            .load(connection)
    }
}

fn load_dynamic_origin_raw_candidates(
    connection: &mut SqliteConnection,
    branch_name: &str,
    source_revision: i64,
    raw_refresh_id_upper_bound: i64,
    after: Option<&DynamicOriginRawCursor>,
) -> QueryResult<Vec<DynamicOriginRawRow>> {
    if let Some(after) = after {
        diesel::sql_query(DYNAMIC_ORIGIN_RAW_PAGE_AFTER_SQL)
            .bind::<BigInt, _>(source_revision)
            .bind::<BigInt, _>(source_revision)
            .bind::<Text, _>(branch_name)
            .bind::<BigInt, _>(raw_refresh_id_upper_bound)
            .bind::<Text, _>(&after.node_id)
            .bind::<Text, _>(after.traversal.as_str())
            .bind::<BigInt, _>(after.refresh_id)
            .bind::<BigInt, _>(SOURCE_DYNAMIC_BRANCH_CANDIDATE_PAGE_SIZE as i64)
            .load(connection)
    } else {
        diesel::sql_query(DYNAMIC_ORIGIN_RAW_PAGE_SQL)
            .bind::<BigInt, _>(source_revision)
            .bind::<BigInt, _>(source_revision)
            .bind::<Text, _>(branch_name)
            .bind::<BigInt, _>(raw_refresh_id_upper_bound)
            .bind::<BigInt, _>(SOURCE_DYNAMIC_BRANCH_CANDIDATE_PAGE_SIZE as i64)
            .load(connection)
    }
}

fn insert_source_branch_history(
    connection: &mut SqliteConnection,
    branch_name: &str,
    source_revision: i64,
    contribution_generation: Option<i64>,
    head_id: Option<&str>,
    state_json: Option<&str>,
) -> QueryResult<()> {
    diesel::sql_query(
        "INSERT OR IGNORE INTO console_graph_source_branch_names ( \
             branch_name, first_source_revision \
         ) VALUES (?, ?)",
    )
    .bind::<Text, _>(branch_name)
    .bind::<BigInt, _>(source_revision)
    .execute(connection)?;
    diesel::sql_query(
        "INSERT INTO console_graph_source_branch_history ( \
             branch_name, source_revision, contribution_generation, \
             head_id, state_json, removed \
         ) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind::<Text, _>(branch_name)
    .bind::<BigInt, _>(source_revision)
    .bind::<Nullable<BigInt>, _>(contribution_generation)
    .bind::<Nullable<Text>, _>(head_id)
    .bind::<Nullable<Text>, _>(state_json)
    .bind::<Integer, _>(i32::from(contribution_generation.is_none()))
    .execute(connection)
    .map(|_| ())
}

#[allow(clippy::too_many_arguments)]
fn insert_source_invalidation_receipt(
    connection: &mut SqliteConnection,
    target_invalidation_incarnation: &str,
    target_invalidation_version: i64,
    branch_name: &str,
    source_revision: i64,
    relation_revision: i64,
    refresh_id: Option<i64>,
    invalidation_kind: &str,
    target_head_id: &str,
    target_state_json: &str,
    dirty_seeds: &[DirtySeed],
) -> QueryResult<()> {
    diesel::sql_query(
        "INSERT INTO console_graph_source_invalidation_receipts ( \
             target_invalidation_incarnation, target_invalidation_version, branch_name, \
             source_revision, relation_revision, refresh_id, invalidation_kind, \
             target_head_id, target_state_json \
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind::<Text, _>(target_invalidation_incarnation)
    .bind::<BigInt, _>(target_invalidation_version)
    .bind::<Text, _>(branch_name)
    .bind::<BigInt, _>(source_revision)
    .bind::<BigInt, _>(relation_revision)
    .bind::<Nullable<BigInt>, _>(refresh_id)
    .bind::<Text, _>(invalidation_kind)
    .bind::<Text, _>(target_head_id)
    .bind::<Text, _>(target_state_json)
    .execute(connection)?;
    let receipt_id = diesel::sql_query("SELECT last_insert_rowid() AS receipt_id")
        .get_result::<ReceiptIdRow>(connection)?
        .receipt_id;
    for seed in dirty_seeds {
        diesel::sql_query(
            "INSERT INTO console_graph_source_invalidation_receipt_seeds ( \
                 receipt_id, node_id, child_high_watermark_relation_revision, \
                 child_high_watermark_node_id \
             ) VALUES (?, ?, ?, ?)",
        )
        .bind::<BigInt, _>(receipt_id)
        .bind::<Text, _>(&seed.node_id)
        .bind::<Nullable<BigInt>, _>(
            seed.child_high_watermark
                .as_ref()
                .map(|watermark| watermark.relation_revision),
        )
        .bind::<Nullable<Text>, _>(
            seed.child_high_watermark
                .as_ref()
                .map(|watermark| watermark.node_id.as_str()),
        )
        .execute(connection)?;
    }
    Ok(())
}

fn new_source_refresh_owner_id() -> String {
    static NEXT_OWNER_ID: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_OWNER_ID.fetch_add(1, Ordering::Relaxed);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "source-refresh-{}-{timestamp}-{sequence}",
        std::process::id()
    )
}

fn source_time_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

fn source_lease_deadline_ms() -> i64 {
    source_time_ms().saturating_add(SOURCE_LEASE_DURATION_MS)
}

fn source_node_is_gc_protected(
    connection: &mut SqliteConnection,
    node_id: &str,
) -> QueryResult<bool> {
    diesel::sql_query(
        "SELECT CASE WHEN \
             EXISTS ( \
                 SELECT 1 FROM console_graph_source_current_branch_nodes \
                 WHERE node_id = ? \
             ) OR EXISTS ( \
                 SELECT 1 \
                 FROM console_graph_source_branch_nodes AS membership \
                 INNER JOIN console_graph_source_refresh_runs AS refresh \
                     ON refresh.branch_name = membership.branch_name \
                    AND refresh.refresh_id = \
                        membership.contribution_generation \
                 WHERE membership.node_id = ? \
                   AND refresh.status = 'building' \
             ) OR EXISTS ( \
                 SELECT 1 \
                 FROM console_graph_build_effective_source_branch_nodes AS membership \
                 INNER JOIN console_graph_build_runs AS build \
                     ON build.run_id = membership.run_id \
                 WHERE membership.node_id = ? \
                   AND build.status IN ('building', 'paused') \
             ) OR EXISTS ( \
                 SELECT 1 \
                 FROM console_graph_source_branch_nodes AS membership \
                 INNER JOIN console_graph_source_refresh_runs AS refresh \
                     ON refresh.refresh_id = membership.contribution_generation \
                    AND refresh.branch_name = membership.branch_name \
                 INNER JOIN console_graph_source_branch_change_journal AS change \
                     ON change.refresh_id = refresh.refresh_id \
                    AND change.branch_name = refresh.branch_name \
                 WHERE membership.node_id = ? \
                   AND change.source_revision > COALESCE( \
                       (SELECT revision.source_revision \
                        FROM console_graph_generation_state AS state \
                        INNER JOIN console_graph_generation_source_revisions AS revision \
                            ON revision.generation = state.active_generation \
                        WHERE state.id = 1), \
                       -1 \
                   ) \
             ) OR EXISTS ( \
                 SELECT 1 \
                 FROM console_graph_source_branch_nodes AS membership \
                 INNER JOIN console_graph_source_refresh_runs AS refresh \
                     ON refresh.refresh_id = membership.contribution_generation \
                    AND refresh.branch_name = membership.branch_name \
                 INNER JOIN console_graph_build_runs AS build \
                     ON build.status IN ('building', 'paused') \
                 INNER JOIN console_graph_source_branch_history AS history \
                     ON history.branch_name = refresh.branch_name \
                    AND history.source_revision = ( \
                        SELECT MAX(candidate.source_revision) \
                        FROM console_graph_source_branch_history AS candidate \
                        WHERE candidate.branch_name = refresh.branch_name \
                          AND candidate.source_revision <= build.dag_source_revision \
                    ) \
                    AND history.removed = 0 \
                    AND history.contribution_generation = \
                        refresh.target_contribution_generation \
                 WHERE membership.node_id = ? \
             ) OR EXISTS ( \
                 SELECT 1 \
                 FROM console_graph_source_branch_nodes AS membership \
                 INNER JOIN console_graph_source_refresh_runs AS refresh \
                     ON refresh.refresh_id = membership.contribution_generation \
                    AND refresh.branch_name = membership.branch_name \
                 INNER JOIN console_graph_materialization_branches AS materialized \
                     ON materialized.name = membership.branch_name \
                    AND materialized.contribution_generation = \
                        refresh.target_contribution_generation \
                 WHERE membership.node_id = ? \
             ) OR EXISTS ( \
                 SELECT 1 \
                 FROM console_graph_source_refresh_queue AS queue \
                 INNER JOIN console_graph_source_refresh_runs AS refresh \
                     ON refresh.refresh_id = queue.refresh_id \
                    AND refresh.branch_name = queue.branch_name \
                 WHERE queue.node_id = ? \
                   AND refresh.status = 'building' \
             ) OR EXISTS ( \
                 SELECT 1 \
                 FROM console_graph_source_child_rechecks AS recheck \
                 INNER JOIN console_graph_source_refresh_runs AS refresh \
                     ON refresh.refresh_id = \
                        recheck.contribution_generation \
                    AND refresh.branch_name = recheck.branch_name \
                 WHERE recheck.node_id = ? \
                   AND refresh.status = 'building' \
             ) OR EXISTS ( \
                 SELECT 1 \
                 FROM console_graph_source_child_rechecks AS recheck \
                      INDEXED BY console_graph_source_child_rechecks_node_raw_idx \
                 INNER JOIN console_graph_source_refresh_runs AS refresh \
                     ON refresh.refresh_id = recheck.contribution_generation \
                    AND refresh.branch_name = recheck.branch_name \
                 INNER JOIN console_graph_source_dynamic_branch_scans AS scan \
                     ON scan.status = 'building' \
                    AND refresh.refresh_id <= scan.raw_refresh_id_upper_bound \
                    AND refresh.published_source_revision <= scan.source_revision \
                 INNER JOIN console_graph_source_branch_history AS history \
                     ON history.branch_name = refresh.branch_name \
                    AND history.source_revision = ( \
                        SELECT MAX(candidate.source_revision) \
                        FROM console_graph_source_branch_history AS candidate \
                        WHERE candidate.branch_name = refresh.branch_name \
                          AND candidate.source_revision <= scan.source_revision \
                    ) \
                    AND history.removed = 0 \
                    AND history.contribution_generation = \
                        refresh.target_contribution_generation \
                 WHERE recheck.node_id = ? \
                   AND (scan.scan_kind = 'dirty_parent' \
                        OR scan.lease_expires_at_ms > ?) \
             ) THEN 1 ELSE 0 END AS value",
    )
    .bind::<Text, _>(node_id)
    .bind::<Text, _>(node_id)
    .bind::<Text, _>(node_id)
    .bind::<Text, _>(node_id)
    .bind::<Text, _>(node_id)
    .bind::<Text, _>(node_id)
    .bind::<Text, _>(node_id)
    .bind::<Text, _>(node_id)
    .bind::<Text, _>(node_id)
    .bind::<BigInt, _>(source_time_ms())
    .get_result::<FlagRow>(connection)
    .map(|row| row.value != 0)
}

impl PersistentGraphIndex {
    pub(crate) async fn open(
        snapshots: &ConsoleGraphSnapshotStore,
        root_id: String,
    ) -> crate::Result<Self> {
        let database = snapshots.database();
        let path = database.path().to_owned();
        Ok(Self {
            root_id,
            database,
            path,
            owner_id: new_source_refresh_owner_id(),
            #[cfg(test)]
            refresh_count: 0,
            #[cfg(test)]
            branch_refresh_count: 0,
            #[cfg(test)]
            traversed_node_count: 0,
            #[cfg(test)]
            branch_refresh_history: Vec::new(),
            #[cfg(test)]
            fail_next_branch_refresh: None,
            #[cfg(test)]
            targeted_dynamic_branch_limit: TARGETED_DYNAMIC_BRANCH_LIMIT,
            #[cfg(test)]
            full_refresh_branch_page_size: NonZeroUsize::new(GRAPH_READ_BATCH_SIZE)
                .expect("graph read batch size should be non-zero"),
            #[cfg(test)]
            full_refresh_source_page_count: 0,
            #[cfg(test)]
            fail_next_child_recheck_page_checkpoint: false,
            #[cfg(test)]
            child_recheck_page_cursors: Vec::new(),
        })
    }

    #[cfg(test)]
    pub(crate) async fn is_empty(&self) -> crate::Result<bool> {
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                console_graph_source_branches::table
                    .count()
                    .get_result::<i64>(connection)
                    .map(|count| count == 0)
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    pub(crate) async fn start_refresh(&mut self) -> crate::Result<()> {
        #[cfg(test)]
        {
            self.refresh_count += 1;
        }
        self.cleanup_stale_refreshes_bounded().await
    }

    async fn promote_invalidation_boundary_to_full(
        &self,
        incarnation: &str,
        version: i64,
        relation_revision: i64,
    ) -> crate::Result<()> {
        let incarnation = incarnation.to_owned();
        let path = self.path.clone();
        self.database
            .with_write_connection("promote source invalidation boundary", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_source_invalidation_boundaries \
                     SET requested_scope = 'full', \
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE target_invalidation_incarnation = ? \
                       AND target_invalidation_version = ? AND relation_revision = ? \
                       AND status = 'building'",
                )
                .bind::<Text, _>(incarnation)
                .bind::<BigInt, _>(version)
                .bind::<BigInt, _>(relation_revision)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn complete_invalidation_boundary(
        &self,
        incarnation: &str,
        version: i64,
        relation_revision: i64,
    ) -> crate::Result<i64> {
        let incarnation = incarnation.to_owned();
        let path = self.path.clone();
        self.database
            .with_write_connection("complete source invalidation boundary", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let source_revision = current_source_revision(connection)?;
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_source_invalidation_boundaries \
                             SET status = 'completed', source_revision = ?, \
                                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE target_invalidation_incarnation = ? \
                               AND target_invalidation_version = ? AND relation_revision = ? \
                               AND status = 'building'",
                        )
                        .bind::<BigInt, _>(source_revision)
                        .bind::<Text, _>(&incarnation)
                        .bind::<BigInt, _>(version)
                        .bind::<BigInt, _>(relation_revision)
                        .execute(connection)?;
                        if updated != 1 {
                            let persisted = diesel::sql_query(
                                "SELECT source_revision AS contribution_generation \
                                 FROM console_graph_source_invalidation_boundaries \
                                 WHERE target_invalidation_incarnation = ? \
                                   AND target_invalidation_version = ? \
                                   AND relation_revision = ? AND status = 'completed'",
                            )
                            .bind::<Text, _>(&incarnation)
                            .bind::<BigInt, _>(version)
                            .bind::<BigInt, _>(relation_revision)
                            .get_result::<GenerationRow>(connection)?;
                            return Ok(persisted.contribution_generation);
                        }
                        Ok(source_revision)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn reconcile_full_invalidation_boundary(
        &mut self,
        store: &SqliteGraphStore,
        incarnation: &str,
        version: i64,
        relation_revision: i64,
        reason: &str,
    ) -> crate::Result<()> {
        self.promote_invalidation_boundary_to_full(incarnation, version, relation_revision)
            .await?;
        tracing::info!(
            source_mutation_revision = relation_revision,
            source_full_reconciliation_reason = reason,
            "console graph source full reconciliation scheduled",
        );
        self.refresh_all_branches_bounded_event(store, incarnation, version, relation_revision)
            .await?;
        let source_revision = self
            .complete_invalidation_boundary(incarnation, version, relation_revision)
            .await?;
        tracing::info!(
            source_mutation_revision = relation_revision,
            published_source_revision = source_revision,
            source_invalidation_scope = "full",
            "console graph source invalidation completed",
        );
        Ok(())
    }

    async fn recover_incomplete_invalidation_boundaries(
        &mut self,
        store: &SqliteGraphStore,
        current_incarnation: &str,
        current_version: i64,
    ) -> crate::Result<()> {
        loop {
            let current_incarnation = current_incarnation.to_owned();
            let path = self.path.clone();
            let pending = self
                .database
                .with_connection(move |connection| {
                    diesel::sql_query(
                        "SELECT target_invalidation_incarnation, \
                                target_invalidation_version, relation_revision \
                         FROM console_graph_source_invalidation_boundaries \
                         WHERE status = 'building' \
                           AND NOT (target_invalidation_incarnation = ? \
                                    AND target_invalidation_version = ?) \
                         ORDER BY relation_revision, target_invalidation_incarnation, \
                                  target_invalidation_version LIMIT 1",
                    )
                    .bind::<Text, _>(current_incarnation)
                    .bind::<BigInt, _>(current_version)
                    .get_result::<PendingInvalidationBoundaryRow>(connection)
                    .optional()
                    .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
            let Some(pending) = pending else {
                return Ok(());
            };
            self.reconcile_full_invalidation_boundary(
                store,
                &pending.target_invalidation_incarnation,
                pending.target_invalidation_version,
                pending.relation_revision,
                "recovering_incomplete_invalidation",
            )
            .await?;
        }
    }

    pub(crate) async fn refresh_invalidation_batch(
        &mut self,
        store: &SqliteGraphStore,
        batch: &ConsoleInvalidationBatch,
    ) -> crate::Result<()> {
        let revision_bounds = store
            .graph_mutation_revision_bounds()
            .await
            .context(crate::error::StoreSnafu)?;
        let current_revision = revision_bounds.current_revision;
        self.supersede_source_work_outside_revision_bounds(
            revision_bounds.baseline_revision,
            current_revision,
        )
        .await?;
        self.resume_building_source_sweeps(store).await?;
        self.recover_incomplete_invalidation_boundaries(
            store,
            DURABLE_MUTATION_INCARNATION,
            current_revision,
        )
        .await?;

        let consumed_revision = self.consumed_graph_mutation_revision().await?;
        let source_cache_uninitialized = !self.durable_source_initialized().await?;
        if consumed_revision > current_revision {
            return self
                .reconcile_durable_full_and_reset_cursor(
                    store,
                    current_revision,
                    "mutation_journal_cursor_ahead",
                )
                .await;
        }
        if consumed_revision < revision_bounds.baseline_revision || source_cache_uninitialized {
            let reason = if consumed_revision < revision_bounds.baseline_revision {
                "mutation_journal_baseline_gap"
            } else {
                "source_cache_uninitialized"
            };
            return self
                .reconcile_durable_full_and_advance(store, current_revision, reason)
                .await;
        }

        if batch.full {
            return self
                .reconcile_durable_full_and_advance(store, current_revision, "full_wakeup")
                .await;
        }

        self.consume_durable_mutation_journal(store, current_revision)
            .await
    }

    pub(crate) async fn consumed_graph_mutation_revision(&self) -> crate::Result<i64> {
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT consumed_revision \
                     FROM console_graph_source_mutation_journal_state WHERE id = 1",
                )
                .get_result::<MutationJournalStateRow>(connection)
                .map(|row| row.consumed_revision)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn durable_source_initialized(&self) -> crate::Result<bool> {
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT initialized AS value \
                     FROM console_graph_source_mutation_journal_state WHERE id = 1",
                )
                .get_result::<FlagRow>(connection)
                .map(|row| row.value != 0)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn ensure_full_boundary_at_revision(&self, mutation_revision: i64) -> crate::Result<i64> {
        let boundary_version = mutation_revision.saturating_add(1);
        let path = self.path.clone();
        self.database
            .with_write_connection("begin durable source full boundary", move |connection| {
                diesel::sql_query(
                    "INSERT OR IGNORE INTO console_graph_source_invalidation_boundaries ( \
                         target_invalidation_incarnation, target_invalidation_version, \
                         relation_revision, requested_scope, status, source_revision, \
                         changed_branch_count, dirty_parent_count \
                     ) VALUES (?, ?, ?, 'full', 'building', NULL, 0, 0)",
                )
                .bind::<Text, _>(DURABLE_FULL_INCARNATION)
                .bind::<BigInt, _>(boundary_version)
                .bind::<BigInt, _>(mutation_revision)
                .execute(connection)
                .context(QueryGraphSnapshotStoreSnafu { path: path.clone() })?;
                let boundary = diesel::sql_query(
                    "SELECT relation_revision, requested_scope \
                     FROM console_graph_source_invalidation_boundaries \
                     WHERE target_invalidation_incarnation = ? \
                       AND target_invalidation_version = ?",
                )
                .bind::<Text, _>(DURABLE_FULL_INCARNATION)
                .bind::<BigInt, _>(boundary_version)
                .get_result::<DurableFullBoundaryRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })?;
                if boundary.relation_revision != mutation_revision
                    || boundary.requested_scope != "full"
                {
                    return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                        column: "durable_full_source_boundary",
                        value: format!(
                            "revision={}/{} scope={}",
                            boundary.relation_revision, mutation_revision, boundary.requested_scope,
                        ),
                    }
                    .fail();
                }
                Ok(boundary_version)
            })
            .await
    }

    async fn reconcile_durable_full_snapshot(
        &mut self,
        store: &SqliteGraphStore,
        mutation_revision: i64,
        reason: &str,
    ) -> crate::Result<()> {
        let boundary_version = self
            .ensure_full_boundary_at_revision(mutation_revision)
            .await?;
        self.reconcile_full_invalidation_boundary(
            store,
            DURABLE_FULL_INCARNATION,
            boundary_version,
            mutation_revision,
            reason,
        )
        .await
    }

    async fn reconcile_durable_full_and_advance(
        &mut self,
        store: &SqliteGraphStore,
        mutation_revision: i64,
        reason: &str,
    ) -> crate::Result<()> {
        self.reconcile_durable_full_snapshot(store, mutation_revision, reason)
            .await?;
        self.supersede_source_work_before_revision(mutation_revision)
            .await?;
        self.complete_durable_mutation_through(mutation_revision)
            .await
    }

    async fn reconcile_durable_full_and_reset_cursor(
        &mut self,
        store: &SqliteGraphStore,
        mutation_revision: i64,
        reason: &str,
    ) -> crate::Result<()> {
        self.reconcile_durable_full_snapshot(store, mutation_revision, reason)
            .await?;
        self.supersede_source_work_before_revision(mutation_revision)
            .await?;
        self.delete_mutation_event_runs_bounded(None).await?;
        let path = self.path.clone();
        self.database
            .with_write_connection("reset durable source mutation cursor", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_source_mutation_journal_state \
                     SET consumed_revision = ?, initialized = 1 WHERE id = 1",
                )
                .bind::<BigInt, _>(mutation_revision)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn supersede_source_work_outside_revision_bounds(
        &self,
        baseline_revision: i64,
        current_revision: i64,
    ) -> crate::Result<()> {
        let superseded = self
            .supersede_source_work_bounded(baseline_revision, Some(current_revision))
            .await?;
        if !superseded.is_empty() {
            tracing::info!(
                baseline_source_mutation_revision = baseline_revision,
                source_mutation_revision = current_revision,
                superseded_source_boundary_count = superseded.boundaries,
                superseded_source_sweep_count = superseded.sweeps,
                superseded_source_refresh_count = superseded.refreshes,
                "unserviceable console graph source work superseded",
            );
        }
        Ok(())
    }

    async fn supersede_source_work_before_revision(
        &self,
        mutation_revision: i64,
    ) -> crate::Result<()> {
        let superseded = self
            .supersede_source_work_bounded(mutation_revision, None)
            .await?;
        if !superseded.is_empty() {
            tracing::info!(
                source_mutation_revision = mutation_revision,
                superseded_source_boundary_count = superseded.boundaries,
                superseded_source_sweep_count = superseded.sweeps,
                superseded_source_refresh_count = superseded.refreshes,
                "obsolete console graph source work superseded",
            );
        }
        Ok(())
    }

    async fn supersede_source_work_bounded(
        &self,
        minimum_revision: i64,
        maximum_revision: Option<i64>,
    ) -> crate::Result<SourceWorkSupersedeCounts> {
        let mut total = SourceWorkSupersedeCounts::default();
        loop {
            let batch = self
                .supersede_source_work_batch(minimum_revision, maximum_revision)
                .await?;
            if batch.is_empty() {
                return Ok(total);
            }
            total.accumulate(batch);
            tokio::task::yield_now().await;
        }
    }

    async fn supersede_source_work_batch(
        &self,
        minimum_revision: i64,
        maximum_revision: Option<i64>,
    ) -> crate::Result<SourceWorkSupersedeCounts> {
        let path = self.path.clone();
        self.database
            .with_write_connection("supersede source work batch", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let source_revision = current_source_revision(connection)?;
                        let boundaries = diesel::sql_query(
                            "UPDATE console_graph_source_invalidation_boundaries \
                             SET status = 'completed', requested_scope = 'full', \
                                 source_revision = ?, \
                                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE rowid IN ( \
                                 SELECT rowid \
                                 FROM console_graph_source_invalidation_boundaries \
                                 WHERE status = 'building' \
                                   AND (relation_revision < ? OR ( \
                                       ? IS NOT NULL AND relation_revision > ? \
                                   )) \
                                 ORDER BY relation_revision, \
                                          target_invalidation_incarnation, \
                                          target_invalidation_version \
                                 LIMIT ? \
                             )",
                        )
                        .bind::<BigInt, _>(source_revision)
                        .bind::<BigInt, _>(minimum_revision)
                        .bind::<Nullable<BigInt>, _>(maximum_revision)
                        .bind::<Nullable<BigInt>, _>(maximum_revision)
                        .bind::<BigInt, _>(SOURCE_WORK_SUPERSEDE_BATCH_SIZE as i64)
                        .execute(connection)?;
                        let sweeps = diesel::sql_query(
                            "UPDATE console_graph_source_sweep_runs \
                             SET status = 'completed', phase = 'reconcile', page_cursor = NULL, \
                                 branch_recheck_node_cursor = NULL, \
                                 branch_recheck_traversal_cursor = NULL, \
                                 branch_recheck_raw_node_cursor = NULL, \
                                 branch_recheck_raw_traversal_cursor = NULL, \
                                 branch_recheck_raw_refresh_id_cursor = NULL, \
                                 branch_recheck_active_node_id = NULL, \
                                 branch_recheck_active_traversal_kind = NULL, \
                                 branch_recheck_child_cursor_relation_revision = NULL, \
                                 branch_recheck_child_cursor_node_id = NULL, owner_id = '', \
                                 lease_expires_at_ms = 0, \
                                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE rowid IN ( \
                                 SELECT rowid FROM console_graph_source_sweep_runs \
                                 WHERE status = 'building' \
                                   AND (relation_revision < ? OR ( \
                                       ? IS NOT NULL AND relation_revision > ? \
                                   )) \
                                 ORDER BY relation_revision, \
                                          target_invalidation_incarnation, \
                                          target_invalidation_version \
                                 LIMIT ? \
                             )",
                        )
                        .bind::<BigInt, _>(minimum_revision)
                        .bind::<Nullable<BigInt>, _>(maximum_revision)
                        .bind::<Nullable<BigInt>, _>(maximum_revision)
                        .bind::<BigInt, _>(SOURCE_WORK_SUPERSEDE_BATCH_SIZE as i64)
                        .execute(connection)?;
                        let refreshes = diesel::sql_query(
                            "UPDATE console_graph_source_refresh_runs \
                             SET status = 'superseded', owner_id = '', lease_expires_at_ms = 0, \
                                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE rowid IN ( \
                                 SELECT rowid FROM console_graph_source_refresh_runs \
                                 WHERE status = 'building' \
                                   AND (relation_revision < ? OR ( \
                                       ? IS NOT NULL AND relation_revision > ? \
                                   )) \
                                 ORDER BY relation_revision, refresh_id \
                                 LIMIT ? \
                             )",
                        )
                        .bind::<BigInt, _>(minimum_revision)
                        .bind::<Nullable<BigInt>, _>(maximum_revision)
                        .bind::<Nullable<BigInt>, _>(maximum_revision)
                        .bind::<BigInt, _>(SOURCE_WORK_SUPERSEDE_BATCH_SIZE as i64)
                        .execute(connection)?;
                        Ok(SourceWorkSupersedeCounts {
                            boundaries,
                            sweeps,
                            refreshes,
                        })
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn complete_durable_mutation_through(&self, mutation_revision: i64) -> crate::Result<()> {
        self.delete_mutation_event_runs_bounded(Some(mutation_revision))
            .await?;
        let path = self.path.clone();
        self.database
            .with_write_connection(
                "advance durable source mutation cursor",
                move |connection| {
                    connection
                        .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                            diesel::sql_query(
                                "UPDATE console_graph_source_mutation_journal_state \
                             SET consumed_revision = MAX(consumed_revision, ?), initialized = 1 \
                             WHERE id = 1",
                            )
                            .bind::<BigInt, _>(mutation_revision)
                            .execute(connection)?;
                            Ok(())
                        })
                        .context(QueryGraphSnapshotStoreSnafu { path })
                },
            )
            .await
    }

    async fn delete_mutation_event_runs_bounded(
        &self,
        maximum_revision: Option<i64>,
    ) -> crate::Result<()> {
        loop {
            let deleted = self
                .delete_mutation_event_run_batch(maximum_revision)
                .await?;
            if deleted == 0 {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    async fn delete_mutation_event_run_batch(
        &self,
        maximum_revision: Option<i64>,
    ) -> crate::Result<usize> {
        let path = self.path.clone();
        self.database
            .with_write_connection(
                "delete source mutation event run batch",
                move |connection| {
                    connection
                        .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                            let discarding_revision =
                                if let Some(maximum_revision) = maximum_revision {
                                    diesel::sql_query(DISCARDING_MUTATION_EVENT_THROUGH_SQL)
                                        .bind::<BigInt, _>(maximum_revision)
                                        .get_result::<GenerationRow>(connection)
                                        .optional()?
                                } else {
                                    diesel::sql_query(DISCARDING_MUTATION_EVENT_FIRST_SQL)
                                        .get_result::<GenerationRow>(connection)
                                        .optional()?
                                }
                                .map(|row| row.contribution_generation);
                            if let Some(mutation_revision) = discarding_revision {
                                let scan_id = diesel::sql_query(DYNAMIC_DIRTY_SCAN_FOR_EVENT_SQL)
                                    .bind::<BigInt, _>(mutation_revision)
                                    .get_result::<GenerationRow>(connection)
                                    .optional()?
                                    .map(|row| row.contribution_generation);
                                let Some(scan_id) = scan_id else {
                                    let deleted = diesel::sql_query(
                                        "DELETE FROM console_graph_source_mutation_event_runs \
                                         WHERE revision = ? AND phase = 'discarding' \
                                           AND NOT EXISTS ( \
                                               SELECT 1 \
                                               FROM \
                                                   console_graph_source_dynamic_branch_scans \
                                               WHERE scan_kind = 'dirty_parent' \
                                                 AND mutation_revision = ? LIMIT 1 \
                                           )",
                                    )
                                    .bind::<BigInt, _>(mutation_revision)
                                    .bind::<BigInt, _>(mutation_revision)
                                    .execute(connection)?;
                                    if deleted != 1 {
                                        return Err(diesel::result::Error::NotFound);
                                    }
                                    return Ok(deleted);
                                };
                                let fenced = diesel::sql_query(
                                    "UPDATE console_graph_source_dynamic_branch_scans \
                                     SET status = 'discarding', owner_id = '', \
                                         lease_epoch = CASE \
                                             WHEN lease_epoch = 9223372036854775807 THEN 0 \
                                             ELSE lease_epoch + 1 \
                                         END, \
                                         lease_expires_at_ms = 0, \
                                         updated_at = \
                                             strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                                     WHERE scan_id = ? AND scan_kind = 'dirty_parent' \
                                       AND mutation_revision = ? \
                                       AND status IN ('building', 'completed')",
                                )
                                .bind::<BigInt, _>(scan_id)
                                .bind::<BigInt, _>(mutation_revision)
                                .execute(connection)?;
                                if fenced == 1 {
                                    return Ok(fenced);
                                }

                                let mut remaining = SOURCE_CACHE_BATCH_SIZE;
                                let deleted_results = diesel::sql_query(
                                    "DELETE FROM \
                                         console_graph_source_dynamic_branch_scan_results \
                                     WHERE rowid IN ( \
                                         SELECT rowid \
                                         FROM \
                                             console_graph_source_dynamic_branch_scan_results \
                                         WHERE scan_id = ? \
                                         ORDER BY branch_name LIMIT ? \
                                     )",
                                )
                                .bind::<BigInt, _>(scan_id)
                                .bind::<BigInt, _>(i64::try_from(remaining).unwrap_or(i64::MAX))
                                .execute(connection)?;
                                remaining = remaining.saturating_sub(deleted_results);
                                let deleted_origins = if remaining == 0 {
                                    0
                                } else {
                                    diesel::sql_query(
                                        "DELETE FROM \
                                             console_graph_source_dynamic_branch_scan_origins \
                                         WHERE rowid IN ( \
                                             SELECT rowid \
                                             FROM \
                                                 console_graph_source_dynamic_branch_scan_origins \
                                             WHERE scan_id = ? \
                                             ORDER BY branch_name LIMIT ? \
                                         )",
                                    )
                                    .bind::<BigInt, _>(scan_id)
                                    .bind::<BigInt, _>(i64::try_from(remaining).unwrap_or(i64::MAX))
                                    .execute(connection)?
                                };
                                let deleted_children =
                                    deleted_results.saturating_add(deleted_origins);
                                if deleted_children > 0 {
                                    return Ok(deleted_children);
                                }
                                let deleted_scan = diesel::sql_query(
                                    "DELETE FROM console_graph_source_dynamic_branch_scans \
                                     WHERE scan_id = ? AND scan_kind = 'dirty_parent' \
                                       AND mutation_revision = ? AND status = 'discarding' \
                                       AND NOT EXISTS ( \
                                           SELECT 1 \
                                           FROM \
                                               console_graph_source_dynamic_branch_scan_results \
                                           WHERE scan_id = ? LIMIT 1 \
                                       ) \
                                       AND NOT EXISTS ( \
                                           SELECT 1 \
                                           FROM \
                                               console_graph_source_dynamic_branch_scan_origins \
                                           WHERE scan_id = ? LIMIT 1 \
                                       )",
                                )
                                .bind::<BigInt, _>(scan_id)
                                .bind::<BigInt, _>(mutation_revision)
                                .bind::<BigInt, _>(scan_id)
                                .bind::<BigInt, _>(scan_id)
                                .execute(connection)?;
                                if deleted_scan != 1 {
                                    return Err(diesel::result::Error::NotFound);
                                }
                                return Ok(deleted_scan);
                            }

                            let cleanup_revision = if let Some(maximum_revision) = maximum_revision
                            {
                                diesel::sql_query(MUTATION_EVENT_CLEANUP_THROUGH_SQL)
                                    .bind::<BigInt, _>(maximum_revision)
                                    .get_result::<GenerationRow>(connection)
                                    .optional()?
                            } else {
                                diesel::sql_query(MUTATION_EVENT_CLEANUP_FIRST_SQL)
                                    .get_result::<GenerationRow>(connection)
                                    .optional()?
                            }
                            .map(|row| row.contribution_generation);
                            let Some(cleanup_revision) = cleanup_revision else {
                                return Ok(0);
                            };
                            let tombstoned = diesel::sql_query(
                                "UPDATE console_graph_source_mutation_event_runs \
                                 SET phase = 'discarding', branch_cursor = NULL, \
                                     dirty_parent_cursor = NULL, \
                                     active_dirty_parent_id = NULL, peer_branch_cursor = NULL, \
                                     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                                 WHERE revision = ? \
                                   AND phase IN ('branch_changes', 'dirty_parents')",
                            )
                            .bind::<BigInt, _>(cleanup_revision)
                            .execute(connection)?;
                            if tombstoned != 1 {
                                return Err(diesel::result::Error::NotFound);
                            }
                            let scan_id = diesel::sql_query(DYNAMIC_DIRTY_SCAN_FOR_EVENT_SQL)
                                .bind::<BigInt, _>(cleanup_revision)
                                .get_result::<GenerationRow>(connection)
                                .optional()?
                                .map(|row| row.contribution_generation);
                            if let Some(scan_id) = scan_id {
                                let fenced = diesel::sql_query(
                                    "UPDATE console_graph_source_dynamic_branch_scans \
                                     SET status = 'discarding', owner_id = '', \
                                         lease_epoch = CASE \
                                             WHEN lease_epoch = 9223372036854775807 THEN 0 \
                                             ELSE lease_epoch + 1 \
                                         END, \
                                         lease_expires_at_ms = 0, \
                                         updated_at = \
                                             strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                                     WHERE scan_id = ? AND scan_kind = 'dirty_parent' \
                                       AND mutation_revision = ?",
                                )
                                .bind::<BigInt, _>(scan_id)
                                .bind::<BigInt, _>(cleanup_revision)
                                .execute(connection)?;
                                if fenced != 1 {
                                    return Err(diesel::result::Error::NotFound);
                                }
                            }
                            Ok(tombstoned)
                        })
                        .context(QueryGraphSnapshotStoreSnafu { path })
                },
            )
            .await
    }

    async fn discarding_mutation_event_revision(&self) -> crate::Result<Option<i64>> {
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(DISCARDING_MUTATION_EVENT_FIRST_SQL)
                    .get_result::<GenerationRow>(connection)
                    .optional()
                    .map(|row| row.map(|row| row.contribution_generation))
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn consume_durable_mutation_journal(
        &mut self,
        store: &SqliteGraphStore,
        target_mutation_revision: i64,
    ) -> crate::Result<()> {
        let page_size = NonZeroUsize::new(1).expect("durable event page size should be non-zero");
        loop {
            if let Some(discarding_revision) = self.discarding_mutation_event_revision().await? {
                self.complete_durable_mutation_through(discarding_revision)
                    .await?;
                continue;
            }
            let consumed_revision = self.consumed_graph_mutation_revision().await?;
            if consumed_revision >= target_mutation_revision {
                return Ok(());
            }
            let page = store
                .graph_mutation_events_page(consumed_revision, page_size)
                .await
                .context(crate::error::StoreSnafu)?;
            let Some(event) = page
                .events
                .into_iter()
                .find(|event| event.revision <= target_mutation_revision)
            else {
                self.reconcile_durable_full_and_advance(
                    store,
                    target_mutation_revision,
                    "durable_mutation_journal_gap",
                )
                .await?;
                return Ok(());
            };
            if event.revision != consumed_revision.saturating_add(1) {
                self.reconcile_durable_full_and_advance(
                    store,
                    target_mutation_revision,
                    "durable_mutation_journal_non_contiguous",
                )
                .await?;
                return Ok(());
            }
            self.consume_durable_mutation_event(store, event.revision)
                .await?;
        }
    }

    async fn load_or_begin_mutation_event_run(
        &self,
        mutation_revision: i64,
    ) -> crate::Result<Option<(MutationEventRunRow, bool)>> {
        if self.consumed_graph_mutation_revision().await? >= mutation_revision {
            return Ok(None);
        }
        let path = self.path.clone();
        self.database
            .with_write_connection("begin durable source mutation event", move |connection| {
                let inserted = diesel::sql_query(
                    "INSERT OR IGNORE INTO console_graph_source_mutation_event_runs ( \
                         revision, phase \
                     ) VALUES (?, 'branch_changes')",
                )
                .bind::<BigInt, _>(mutation_revision)
                .execute(connection)
                .context(QueryGraphSnapshotStoreSnafu { path: path.clone() })?;
                diesel::sql_query(
                    "SELECT phase, branch_cursor, dirty_parent_cursor, \
                            active_dirty_parent_id, peer_branch_cursor \
                     FROM console_graph_source_mutation_event_runs WHERE revision = ?",
                )
                .bind::<BigInt, _>(mutation_revision)
                .get_result::<MutationEventRunRow>(connection)
                .map(|run| Some((run, inserted == 0)))
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn mutation_event_run(
        &self,
        mutation_revision: i64,
    ) -> crate::Result<Option<MutationEventRunRow>> {
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT phase, branch_cursor, dirty_parent_cursor, \
                            active_dirty_parent_id, peer_branch_cursor \
                     FROM console_graph_source_mutation_event_runs WHERE revision = ?",
                )
                .bind::<BigInt, _>(mutation_revision)
                .get_result::<MutationEventRunRow>(connection)
                .optional()
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn checkpoint_mutation_branch_cursor(
        &self,
        mutation_revision: i64,
        expected: Option<&str>,
        next: &str,
    ) -> crate::Result<()> {
        self.checkpoint_mutation_text_cursor(
            "checkpoint durable source branch change",
            mutation_revision,
            "branch_cursor",
            expected,
            next,
            "phase = 'branch_changes'",
        )
        .await
    }

    async fn checkpoint_mutation_text_cursor(
        &self,
        operation: &'static str,
        mutation_revision: i64,
        column: &'static str,
        expected: Option<&str>,
        next: &str,
        predicate: &'static str,
    ) -> crate::Result<()> {
        let expected = expected.map(str::to_owned);
        let next = next.to_owned();
        let path = self.path.clone();
        let updated = self
            .database
            .with_write_connection(operation, move |connection| {
                diesel::sql_query(format!(
                    "UPDATE console_graph_source_mutation_event_runs \
                     SET {column} = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE revision = ? AND {predicate} \
                       AND (({column} IS NULL AND ? IS NULL) OR {column} = ?)"
                ))
                .bind::<Text, _>(next)
                .bind::<BigInt, _>(mutation_revision)
                .bind::<Nullable<Text>, _>(expected.as_deref())
                .bind::<Nullable<Text>, _>(expected.as_deref())
                .execute(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        self.ensure_mutation_event_cursor_updated(mutation_revision, operation, updated)
    }

    async fn transition_mutation_event_to_dirty_parents(
        &self,
        mutation_revision: i64,
    ) -> crate::Result<()> {
        let path = self.path.clone();
        let updated = self
            .database
            .with_write_connection("advance durable source mutation phase", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_source_mutation_event_runs \
                     SET phase = 'dirty_parents', \
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE revision = ? AND phase = 'branch_changes'",
                )
                .bind::<BigInt, _>(mutation_revision)
                .execute(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        self.ensure_mutation_event_cursor_updated(
            mutation_revision,
            "advance durable source mutation phase",
            updated,
        )?;
        tracing::info!(
            source_mutation_revision = mutation_revision,
            previous_source_mutation_event_phase = "branch_changes",
            source_mutation_event_phase = "dirty_parents",
            "console graph source mutation event phase changed",
        );
        Ok(())
    }

    async fn activate_mutation_dirty_parent(
        &self,
        mutation_revision: i64,
        expected_cursor: Option<&str>,
        parent_id: &str,
    ) -> crate::Result<()> {
        let expected_cursor = expected_cursor.map(str::to_owned);
        let parent_id = parent_id.to_owned();
        let path = self.path.clone();
        let updated = self
            .database
            .with_write_connection("activate durable source dirty parent", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_source_mutation_event_runs \
                     SET active_dirty_parent_id = ?, peer_branch_cursor = NULL, \
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE revision = ? AND phase = 'dirty_parents' \
                       AND active_dirty_parent_id IS NULL \
                       AND ((dirty_parent_cursor IS NULL AND ? IS NULL) \
                            OR dirty_parent_cursor = ?)",
                )
                .bind::<Text, _>(parent_id)
                .bind::<BigInt, _>(mutation_revision)
                .bind::<Nullable<Text>, _>(expected_cursor.as_deref())
                .bind::<Nullable<Text>, _>(expected_cursor.as_deref())
                .execute(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        self.ensure_mutation_event_cursor_updated(
            mutation_revision,
            "activate durable source dirty parent",
            updated,
        )
    }

    async fn checkpoint_mutation_peer_branch(
        &self,
        mutation_revision: i64,
        parent_id: &str,
        expected: Option<&str>,
        next: &str,
    ) -> crate::Result<()> {
        let parent_id = parent_id.to_owned();
        let expected = expected.map(str::to_owned);
        let next = next.to_owned();
        let path = self.path.clone();
        let updated = self
            .database
            .with_write_connection("checkpoint durable source peer branch", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_source_mutation_event_runs \
                     SET peer_branch_cursor = ?, \
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE revision = ? AND phase = 'dirty_parents' \
                       AND active_dirty_parent_id = ? \
                       AND ((peer_branch_cursor IS NULL AND ? IS NULL) \
                            OR peer_branch_cursor = ?)",
                )
                .bind::<Text, _>(next)
                .bind::<BigInt, _>(mutation_revision)
                .bind::<Text, _>(parent_id)
                .bind::<Nullable<Text>, _>(expected.as_deref())
                .bind::<Nullable<Text>, _>(expected.as_deref())
                .execute(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        self.ensure_mutation_event_cursor_updated(
            mutation_revision,
            "checkpoint durable source peer branch",
            updated,
        )
    }

    async fn complete_mutation_dirty_parent(
        &self,
        mutation_revision: i64,
        parent_id: &str,
    ) -> crate::Result<()> {
        let parent_id = parent_id.to_owned();
        let path = self.path.clone();
        let updated = self
            .database
            .with_write_connection("complete durable source dirty parent", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_source_mutation_event_runs \
                     SET dirty_parent_cursor = ?, active_dirty_parent_id = NULL, \
                         peer_branch_cursor = NULL, \
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE revision = ? AND phase = 'dirty_parents' \
                       AND active_dirty_parent_id = ?",
                )
                .bind::<Text, _>(&parent_id)
                .bind::<BigInt, _>(mutation_revision)
                .bind::<Text, _>(&parent_id)
                .execute(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        self.ensure_mutation_event_cursor_updated(
            mutation_revision,
            "complete durable source dirty parent",
            updated,
        )
    }

    fn ensure_mutation_event_cursor_updated(
        &self,
        mutation_revision: i64,
        operation: &str,
        updated: usize,
    ) -> crate::Result<()> {
        if updated == 1 {
            return Ok(());
        }
        crate::error::SourceRefreshBusySnafu {
            resource: format!(
                "durable source mutation event {mutation_revision} during {operation}"
            ),
        }
        .fail()
    }

    async fn consume_durable_mutation_event(
        &mut self,
        store: &SqliteGraphStore,
        mutation_revision: i64,
    ) -> crate::Result<()> {
        let Some((mut run, resumed_source_mutation_event)) = self
            .load_or_begin_mutation_event_run(mutation_revision)
            .await?
        else {
            return Ok(());
        };
        if run.phase == "discarding" {
            self.complete_durable_mutation_through(mutation_revision)
                .await?;
            return Ok(());
        }
        tracing::info!(
            source_mutation_revision = mutation_revision,
            source_mutation_event_phase = %run.phase,
            resumed_source_mutation_event,
            "console graph source mutation event started",
        );
        let page_size = NonZeroUsize::new(SOURCE_CACHE_BATCH_SIZE)
            .expect("source mutation page size should be non-zero");
        while run.phase == "branch_changes" {
            let cursor = run
                .branch_cursor
                .as_ref()
                .map(|name| GraphMutationBranchChangePageCursor { name: name.clone() });
            let page = store
                .graph_mutation_branch_changes_page(mutation_revision, cursor.as_ref(), page_size)
                .await
                .context(crate::error::StoreSnafu)?;
            if page.changes.is_empty() {
                self.transition_mutation_event_to_dirty_parents(mutation_revision)
                    .await?;
                run = self
                    .mutation_event_run(mutation_revision)
                    .await?
                    .expect("active source mutation event should remain persisted");
                continue;
            }
            for change in page.changes {
                match change.kind {
                    GraphMutationBranchChangeKind::Upserted => {
                        let head_id = change.head_id.with_context(|| {
                            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                                column: "graph_mutation_branch_head_id",
                                value: change.name.clone(),
                            }
                        })?;
                        let state = change.state.with_context(|| {
                            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                                column: "graph_mutation_branch_state",
                                value: change.name.clone(),
                            }
                        })?;
                        self.refresh_branch_event(
                            store,
                            GraphBranchRecord {
                                name: change.name.clone(),
                                head_id,
                                state,
                            },
                            SourceInvalidation {
                                incarnation: DURABLE_MUTATION_INCARNATION,
                                version: mutation_revision,
                                relation_revision: mutation_revision,
                            },
                            &BTreeSet::new(),
                            false,
                        )
                        .await?;
                    }
                    GraphMutationBranchChangeKind::Removed => {
                        self.remove_branch_event(
                            &change.name,
                            DURABLE_MUTATION_INCARNATION,
                            mutation_revision,
                            mutation_revision,
                        )
                        .await?;
                    }
                }
                self.checkpoint_mutation_branch_cursor(
                    mutation_revision,
                    run.branch_cursor.as_deref(),
                    &change.name,
                )
                .await?;
                run.branch_cursor = Some(change.name);
            }
            if page.complete {
                self.transition_mutation_event_to_dirty_parents(mutation_revision)
                    .await?;
            }
            run = self
                .mutation_event_run(mutation_revision)
                .await?
                .expect("active source mutation event should remain persisted");
        }

        loop {
            if run.active_dirty_parent_id.is_none() {
                let cursor = run.dirty_parent_cursor.as_ref().map(|parent_id| {
                    GraphMutationDirtyParentPageCursor {
                        parent_id: parent_id.clone(),
                    }
                });
                let one = NonZeroUsize::new(1).expect("dirty parent page size should be non-zero");
                let page = store
                    .graph_mutation_dirty_parents_page(mutation_revision, cursor.as_ref(), one)
                    .await
                    .context(crate::error::StoreSnafu)?;
                let Some(parent_id) = page.parent_ids.into_iter().next() else {
                    self.complete_durable_mutation_through(mutation_revision)
                        .await?;
                    tracing::info!(
                        source_mutation_revision = mutation_revision,
                        "console graph source mutation event completed",
                    );
                    return Ok(());
                };
                self.activate_mutation_dirty_parent(
                    mutation_revision,
                    run.dirty_parent_cursor.as_deref(),
                    &parent_id,
                )
                .await?;
                run = self
                    .mutation_event_run(mutation_revision)
                    .await?
                    .expect("active source mutation event should remain persisted");
                continue;
            }

            let parent_id = run
                .active_dirty_parent_id
                .clone()
                .expect("active dirty parent should be present");
            let peers = self
                .dynamic_peer_branch_page(
                    mutation_revision,
                    &parent_id,
                    run.peer_branch_cursor.as_deref(),
                )
                .await?;
            if peers.is_empty() {
                self.complete_mutation_dirty_parent(mutation_revision, &parent_id)
                    .await?;
                run = self
                    .mutation_event_run(mutation_revision)
                    .await?
                    .expect("active source mutation event should remain persisted");
                continue;
            }
            let records = store
                .graph_branches_at_revision_by_names(mutation_revision, &peers)
                .await
                .context(crate::error::StoreSnafu)?
                .into_iter()
                .map(|record| (record.name.clone(), record))
                .collect::<HashMap<_, _>>();
            let mut expected_peer_cursor = run.peer_branch_cursor.clone();
            for peer in peers {
                if let Some(record) = records.get(&peer) {
                    self.refresh_branch_event(
                        store,
                        record.clone(),
                        SourceInvalidation {
                            incarnation: DURABLE_MUTATION_INCARNATION,
                            version: mutation_revision,
                            relation_revision: mutation_revision,
                        },
                        &BTreeSet::from([parent_id.clone()]),
                        false,
                    )
                    .await?;
                }
                self.checkpoint_mutation_peer_branch(
                    mutation_revision,
                    &parent_id,
                    expected_peer_cursor.as_deref(),
                    &peer,
                )
                .await?;
                expected_peer_cursor = Some(peer);
            }
            run = self
                .mutation_event_run(mutation_revision)
                .await?
                .expect("active source mutation event should remain persisted");
        }
    }

    async fn dynamic_peer_branch_page(
        &mut self,
        mutation_revision: i64,
        dirty_node_id: &str,
        after: Option<&str>,
    ) -> crate::Result<Vec<String>> {
        let request_key = serde_json::to_string(&(mutation_revision, dirty_node_id)).context(
            SerializeGraphSnapshotStoreValueSnafu {
                column: "dynamic_peer_scan_request_key",
            },
        )?;
        let mut scan = self
            .begin_or_claim_dynamic_branch_scan(
                "dirty_parent",
                request_key,
                Some(mutation_revision),
                Some(dirty_node_id.to_owned()),
                None,
                Vec::new(),
            )
            .await?;
        while scan.status == "building" {
            self.advance_dynamic_peer_scan(&mut scan, dirty_node_id)
                .await?;
            tokio::task::yield_now().await;
        }
        self.dynamic_branch_scan_result_page(&scan, after).await
    }

    #[cfg(test)]
    async fn dynamic_branch_scan_source_revision(&self) -> crate::Result<i64> {
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                current_source_revision(connection).context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn begin_or_claim_dynamic_branch_scan(
        &self,
        scan_kind: &'static str,
        request_key: String,
        mutation_revision: Option<i64>,
        dirty_node_id: Option<String>,
        targeted_limit: Option<usize>,
        origins: Vec<String>,
    ) -> crate::Result<DynamicBranchScan> {
        let targeted_limit = targeted_limit.map(|limit| i64::try_from(limit).unwrap_or(i64::MAX));
        let owner_id = self.owner_id.clone();
        let update_owner_id = owner_id.clone();
        let update_request_key = request_key.clone();
        let path = self.path.clone();
        let row = self
            .database
            .with_write_connection("claim dynamic branch scan", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let now_ms = source_time_ms();
                        let lease_expires_at_ms = source_lease_deadline_ms();
                        if scan_kind == "dirty_parent" {
                            let mutation_revision =
                                mutation_revision.ok_or(diesel::result::Error::NotFound)?;
                            let event_active = diesel::sql_query(
                                "SELECT CASE WHEN EXISTS ( \
                                     SELECT 1 \
                                     FROM console_graph_source_mutation_event_runs \
                                     WHERE revision = ? AND phase = 'dirty_parents' \
                                 ) THEN 1 ELSE 0 END AS value",
                            )
                            .bind::<BigInt, _>(mutation_revision)
                            .get_result::<FlagRow>(connection)?
                            .value
                                != 0;
                            if !event_active {
                                return Err(diesel::result::Error::NotFound);
                            }
                        }
                        let captured_source_revision = current_source_revision(connection)?;
                        // The revision and upper bound share this write transaction, so every
                        // published refresh is visible at the captured current revision. The
                        // descending refresh-id index therefore makes LIMIT 1 the exact bound.
                        let raw_refresh_id_upper_bound =
                            diesel::sql_query(DYNAMIC_SCAN_RAW_UPPER_SQL)
                                .bind::<BigInt, _>(captured_source_revision)
                                .get_result::<GenerationRow>(connection)
                                .optional()?
                                .map_or(-1, |row| row.contribution_generation);
                        let inserted = diesel::sql_query(
                            "INSERT OR IGNORE INTO console_graph_source_dynamic_branch_scans ( \
                                 scan_kind, request_key, mutation_revision, source_revision, \
                                 raw_refresh_id_upper_bound, dirty_node_id, targeted_limit, \
                                 status \
                             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'building')",
                        )
                        .bind::<Text, _>(scan_kind)
                        .bind::<Text, _>(&update_request_key)
                        .bind::<Nullable<BigInt>, _>(mutation_revision)
                        .bind::<BigInt, _>(captured_source_revision)
                        .bind::<BigInt, _>(raw_refresh_id_upper_bound)
                        .bind::<Nullable<Text>, _>(dirty_node_id.as_deref())
                        .bind::<Nullable<BigInt>, _>(targeted_limit)
                        .execute(connection)?;
                        let existing = diesel::sql_query(
                            "SELECT scan_id, scan_kind, request_key, source_revision, \
                                    raw_refresh_id_upper_bound, status, \
                                    origin_branch_cursor, active_origin_branch_name, \
                                    origin_raw_node_cursor, origin_raw_traversal_cursor, \
                                    origin_raw_refresh_id_cursor, completed_origin_node_id, \
                                    active_origin_node_id, candidate_raw_branch_cursor, \
                                    candidate_raw_refresh_id_cursor, \
                                    candidate_raw_traversal_cursor, result_count, \
                                    exceeded_limit, owner_id, lease_epoch, lease_expires_at_ms \
                             FROM console_graph_source_dynamic_branch_scans \
                             WHERE scan_kind = ? AND request_key = ? \
                               AND mutation_revision IS ?",
                        )
                        .bind::<Text, _>(scan_kind)
                        .bind::<Text, _>(&update_request_key)
                        .bind::<Nullable<BigInt>, _>(mutation_revision)
                        .get_result::<DynamicBranchScanRow>(connection)?;
                        let lease_epoch = existing.lease_epoch.saturating_add(1);
                        let claimed = diesel::sql_query(
                            "UPDATE console_graph_source_dynamic_branch_scans \
                             SET owner_id = ?, lease_epoch = ?, lease_expires_at_ms = ?, \
                                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE scan_id = ? AND lease_epoch = ? \
                               AND source_revision = ? \
                               AND raw_refresh_id_upper_bound = ? \
                               AND status IN ('building', 'completed') \
                               AND (owner_id = '' OR owner_id = ? \
                                    OR lease_expires_at_ms <= ?)",
                        )
                        .bind::<Text, _>(&update_owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<BigInt, _>(lease_expires_at_ms)
                        .bind::<BigInt, _>(existing.scan_id)
                        .bind::<BigInt, _>(existing.lease_epoch)
                        .bind::<BigInt, _>(existing.source_revision)
                        .bind::<BigInt, _>(existing.raw_refresh_id_upper_bound)
                        .bind::<Text, _>(&update_owner_id)
                        .bind::<BigInt, _>(now_ms)
                        .execute(connection)?;
                        if claimed != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        let stale_affected = scan_kind == "affected"
                            && inserted == 0
                            && (existing.source_revision != captured_source_revision
                                || existing.lease_expires_at_ms <= now_ms);
                        if stale_affected {
                            let completed = diesel::sql_query(
                                "UPDATE console_graph_source_dynamic_branch_scans \
                                 SET status = 'completed', exceeded_limit = 1, \
                                     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                                 WHERE scan_id = ? AND scan_kind = 'affected' \
                                   AND owner_id = ? AND lease_epoch = ? \
                                   AND source_revision = ? \
                                   AND raw_refresh_id_upper_bound = ?",
                            )
                            .bind::<BigInt, _>(existing.scan_id)
                            .bind::<Text, _>(&update_owner_id)
                            .bind::<BigInt, _>(lease_epoch)
                            .bind::<BigInt, _>(existing.source_revision)
                            .bind::<BigInt, _>(existing.raw_refresh_id_upper_bound)
                            .execute(connection)?;
                            if completed != 1 {
                                return Err(diesel::result::Error::NotFound);
                            }
                        }
                        for origin in &origins {
                            diesel::sql_query(
                                "INSERT OR IGNORE INTO \
                                     console_graph_source_dynamic_branch_scan_origins ( \
                                         scan_id, branch_name \
                                     ) VALUES (?, ?)",
                            )
                            .bind::<BigInt, _>(existing.scan_id)
                            .bind::<Text, _>(origin)
                            .execute(connection)?;
                        }
                        diesel::sql_query(
                            "SELECT scan_id, scan_kind, request_key, source_revision, \
                                    raw_refresh_id_upper_bound, status, \
                                    origin_branch_cursor, active_origin_branch_name, \
                                    origin_raw_node_cursor, origin_raw_traversal_cursor, \
                                    origin_raw_refresh_id_cursor, completed_origin_node_id, \
                                    active_origin_node_id, candidate_raw_branch_cursor, \
                                    candidate_raw_refresh_id_cursor, \
                                    candidate_raw_traversal_cursor, result_count, \
                                    exceeded_limit, owner_id, lease_epoch, lease_expires_at_ms \
                             FROM console_graph_source_dynamic_branch_scans \
                             WHERE scan_id = ?",
                        )
                        .bind::<BigInt, _>(existing.scan_id)
                        .get_result::<DynamicBranchScanRow>(connection)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        dynamic_branch_scan_from_row(row)
    }

    async fn advance_dynamic_peer_scan(
        &self,
        scan: &mut DynamicBranchScan,
        dirty_node_id: &str,
    ) -> crate::Result<()> {
        ensure!(
            scan.scan_kind == "dirty_parent",
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "dynamic_branch_scan_kind",
                value: scan.scan_kind.clone(),
            }
        );
        let scan_id = scan.scan_id;
        let owner_id = scan.owner_id.clone();
        let lease_epoch = scan.lease_epoch;
        let frozen_source_revision = scan.source_revision;
        let raw_refresh_id_upper_bound = scan.raw_refresh_id_upper_bound;
        let dirty_node_id = dirty_node_id.to_owned();
        let expected_cursor = scan.candidate_raw_cursor.clone();
        let expected_branch = expected_cursor
            .as_ref()
            .map(|cursor| cursor.branch_name.clone());
        let expected_refresh_id = expected_cursor.as_ref().map(|cursor| cursor.refresh_id);
        let expected_traversal = expected_cursor
            .as_ref()
            .map(|cursor| cursor.traversal.as_str().to_owned());
        let path = self.path.clone();
        let (next_cursor, result_count, completed) = self
            .database
            .with_write_connection("advance dynamic peer scan", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let rows = load_dynamic_branch_raw_candidates(
                            connection,
                            &dirty_node_id,
                            frozen_source_revision,
                            raw_refresh_id_upper_bound,
                            expected_cursor.as_ref(),
                        )?;
                        let mut result_count = 0_i64;
                        for row in &rows {
                            if row.eligible != 0 {
                                result_count += diesel::sql_query(
                                    "INSERT OR IGNORE INTO \
                                         console_graph_source_dynamic_branch_scan_results ( \
                                             scan_id, branch_name \
                                         ) VALUES (?, ?)",
                                )
                                .bind::<BigInt, _>(scan_id)
                                .bind::<Text, _>(&row.branch_name)
                                .execute(connection)?
                                    as i64;
                            }
                        }
                        let next_cursor = rows.last().map(|row| {
                            (
                                row.branch_name.clone(),
                                row.refresh_id,
                                row.traversal_kind.clone(),
                            )
                        });
                        let completed = rows.len() < SOURCE_DYNAMIC_BRANCH_CANDIDATE_PAGE_SIZE;
                        let next_branch = next_cursor.as_ref().map(|cursor| cursor.0.as_str());
                        let next_refresh_id = next_cursor.as_ref().map(|cursor| cursor.1);
                        let next_traversal = next_cursor.as_ref().map(|cursor| cursor.2.as_str());
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_source_dynamic_branch_scans \
                             SET candidate_raw_branch_cursor = COALESCE(?, \
                                     candidate_raw_branch_cursor), \
                                 candidate_raw_refresh_id_cursor = COALESCE(?, \
                                     candidate_raw_refresh_id_cursor), \
                                 candidate_raw_traversal_cursor = COALESCE(?, \
                                     candidate_raw_traversal_cursor), \
                                 result_count = result_count + ?, status = ?, \
                                 lease_expires_at_ms = ?, \
                                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE scan_id = ? AND scan_kind = 'dirty_parent' \
                               AND status = 'building' AND owner_id = ? \
                               AND lease_epoch = ? AND source_revision = ? \
                               AND raw_refresh_id_upper_bound = ? \
                               AND EXISTS ( \
                                   SELECT 1 \
                                   FROM console_graph_source_mutation_event_runs AS event \
                                   WHERE event.revision = \
                                       console_graph_source_dynamic_branch_scans.mutation_revision \
                                     AND event.phase = 'dirty_parents' \
                               ) \
                               AND candidate_raw_branch_cursor IS ? \
                               AND candidate_raw_refresh_id_cursor IS ? \
                               AND candidate_raw_traversal_cursor IS ?",
                        )
                        .bind::<Nullable<Text>, _>(next_branch)
                        .bind::<Nullable<BigInt>, _>(next_refresh_id)
                        .bind::<Nullable<Text>, _>(next_traversal)
                        .bind::<BigInt, _>(result_count)
                        .bind::<Text, _>(if completed { "completed" } else { "building" })
                        .bind::<BigInt, _>(source_lease_deadline_ms())
                        .bind::<BigInt, _>(scan_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<BigInt, _>(frozen_source_revision)
                        .bind::<BigInt, _>(raw_refresh_id_upper_bound)
                        .bind::<Nullable<Text>, _>(expected_branch.as_deref())
                        .bind::<Nullable<BigInt>, _>(expected_refresh_id)
                        .bind::<Nullable<Text>, _>(expected_traversal.as_deref())
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok((next_cursor, result_count, completed))
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        if let Some((branch_name, refresh_id, traversal)) = next_cursor {
            scan.candidate_raw_cursor = Some(DynamicBranchRawCursor {
                branch_name,
                refresh_id,
                traversal: TraversalKind::parse(&traversal)?,
            });
        }
        scan.result_count = scan.result_count.saturating_add(result_count);
        if completed {
            scan.status = "completed".to_owned();
        }
        Ok(())
    }

    async fn dynamic_branch_scan_result_page(
        &self,
        scan: &DynamicBranchScan,
        after: Option<&str>,
    ) -> crate::Result<Vec<String>> {
        loop {
            match self.dynamic_branch_scan_result_step(scan, after).await? {
                DynamicBranchResultStep::Page(rows) => return Ok(rows),
                DynamicBranchResultStep::Complete => return Ok(Vec::new()),
                DynamicBranchResultStep::CleanupPending => tokio::task::yield_now().await,
            }
        }
    }

    async fn dynamic_branch_scan_result_step(
        &self,
        scan: &DynamicBranchScan,
        after: Option<&str>,
    ) -> crate::Result<DynamicBranchResultStep> {
        let scan_id = scan.scan_id;
        let owner_id = scan.owner_id.clone();
        let lease_epoch = scan.lease_epoch;
        let frozen_source_revision = scan.source_revision;
        let raw_refresh_id_upper_bound = scan.raw_refresh_id_upper_bound;
        let after = after.map(str::to_owned);
        let path = self.path.clone();
        self.database
            .with_write_connection("read dynamic branch scan result page", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let renewed = diesel::sql_query(
                            "UPDATE console_graph_source_dynamic_branch_scans \
                             SET lease_expires_at_ms = ?, \
                                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE scan_id = ? AND status = 'completed' \
                               AND owner_id = ? AND lease_epoch = ? \
                               AND source_revision = ? \
                               AND raw_refresh_id_upper_bound = ? \
                               AND EXISTS ( \
                                   SELECT 1 \
                                   FROM console_graph_source_mutation_event_runs AS event \
                                   WHERE event.revision = \
                                       console_graph_source_dynamic_branch_scans.mutation_revision \
                                     AND event.phase = 'dirty_parents' \
                               )",
                        )
                        .bind::<BigInt, _>(source_lease_deadline_ms())
                        .bind::<BigInt, _>(scan_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<BigInt, _>(frozen_source_revision)
                        .bind::<BigInt, _>(raw_refresh_id_upper_bound)
                        .execute(connection)?;
                        if renewed != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        if let Some(after) = &after {
                            diesel::sql_query(
                                "DELETE FROM \
                                     console_graph_source_dynamic_branch_scan_results \
                                 WHERE rowid IN ( \
                                     SELECT rowid \
                                     FROM console_graph_source_dynamic_branch_scan_results \
                                     WHERE scan_id = ? AND branch_name <= ? \
                                     ORDER BY branch_name LIMIT ? \
                                 )",
                            )
                            .bind::<BigInt, _>(scan_id)
                            .bind::<Text, _>(after)
                            .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                            .execute(connection)?;
                        }
                        let rows = if let Some(after) = &after {
                            diesel::sql_query(
                                "SELECT branch_name AS name \
                                 FROM console_graph_source_dynamic_branch_scan_results \
                                 WHERE scan_id = ? AND branch_name > ? \
                                 ORDER BY branch_name LIMIT ?",
                            )
                            .bind::<BigInt, _>(scan_id)
                            .bind::<Text, _>(after)
                            .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                            .load::<BranchNameRow>(connection)?
                        } else {
                            diesel::sql_query(
                                "SELECT branch_name AS name \
                                 FROM console_graph_source_dynamic_branch_scan_results \
                                 WHERE scan_id = ? ORDER BY branch_name LIMIT ?",
                            )
                            .bind::<BigInt, _>(scan_id)
                            .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                            .load::<BranchNameRow>(connection)?
                        };
                        if !rows.is_empty() {
                            return Ok(DynamicBranchResultStep::Page(
                                rows.into_iter().map(|row| row.name).collect(),
                            ));
                        }
                        let cleanup_pending = diesel::sql_query(
                            "SELECT CASE WHEN EXISTS ( \
                                 SELECT 1 \
                                 FROM console_graph_source_dynamic_branch_scan_results \
                                 WHERE scan_id = ? LIMIT 1 \
                             ) THEN 1 ELSE 0 END AS value",
                        )
                        .bind::<BigInt, _>(scan_id)
                        .get_result::<FlagRow>(connection)?
                        .value
                            != 0;
                        if cleanup_pending {
                            return Ok(DynamicBranchResultStep::CleanupPending);
                        }
                        let deleted = diesel::sql_query(
                            "DELETE FROM console_graph_source_dynamic_branch_scans \
                             WHERE scan_id = ? AND status = 'completed' \
                               AND owner_id = ? AND lease_epoch = ? \
                               AND source_revision = ? \
                               AND raw_refresh_id_upper_bound = ?",
                        )
                        .bind::<BigInt, _>(scan_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<BigInt, _>(frozen_source_revision)
                        .bind::<BigInt, _>(raw_refresh_id_upper_bound)
                        .execute(connection)?;
                        if deleted != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(DynamicBranchResultStep::Complete)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn refresh_all_branches_bounded_event(
        &mut self,
        store: &SqliteGraphStore,
        target_invalidation_incarnation: &str,
        target_invalidation_version: i64,
        relation_revision: i64,
    ) -> crate::Result<()> {
        let Some(sweep) = self
            .begin_or_claim_source_sweep(
                target_invalidation_incarnation,
                target_invalidation_version,
                relation_revision,
            )
            .await?
        else {
            return Ok(());
        };
        self.execute_source_sweep(store, sweep).await
    }

    async fn resume_building_source_sweeps(
        &mut self,
        store: &SqliteGraphStore,
    ) -> crate::Result<()> {
        loop {
            let path = self.path.clone();
            let pending = self
                .database
                .with_connection(move |connection| {
                    diesel::sql_query(
                        "SELECT target_invalidation_incarnation, \
                                target_invalidation_version, relation_revision, status, phase, \
                                source_upper_bound, page_cursor, \
                                branch_recheck_node_cursor, branch_recheck_traversal_cursor, \
                                owner_id, lease_epoch, \
                                lease_expires_at_ms \
                         FROM console_graph_source_sweep_runs \
                         WHERE status = 'building' \
                         ORDER BY relation_revision, target_invalidation_incarnation, \
                                  target_invalidation_version LIMIT 1",
                    )
                    .get_result::<SourceSweepRunRow>(connection)
                    .optional()
                    .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
            let Some(pending) = pending else {
                return Ok(());
            };
            let Some(sweep) = self.claim_existing_source_sweep(pending).await? else {
                continue;
            };
            self.execute_source_sweep(store, sweep).await?;
        }
    }

    async fn begin_or_claim_source_sweep(
        &self,
        target_invalidation_incarnation: &str,
        target_invalidation_version: i64,
        relation_revision: i64,
    ) -> crate::Result<Option<SourceSweepRun>> {
        let incarnation = target_invalidation_incarnation.to_owned();
        let path = self.path.clone();
        let existing = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT target_invalidation_incarnation, \
                            target_invalidation_version, relation_revision, status, phase, \
                            source_upper_bound, page_cursor, \
                            branch_recheck_node_cursor, branch_recheck_traversal_cursor, \
                            owner_id, lease_epoch, \
                            lease_expires_at_ms \
                     FROM console_graph_source_sweep_runs \
                     WHERE target_invalidation_incarnation = ? \
                       AND target_invalidation_version = ?",
                )
                .bind::<Text, _>(incarnation)
                .bind::<BigInt, _>(target_invalidation_version)
                .get_result::<SourceSweepRunRow>(connection)
                .optional()
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        if let Some(existing) = existing {
            if existing.status == "completed" {
                return Ok(None);
            }
            if existing.relation_revision != relation_revision {
                return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                    column: "source_sweep_relation_revision",
                    value: format!("{}/{}", existing.relation_revision, relation_revision),
                }
                .fail();
            }
            return self.claim_existing_source_sweep(existing).await;
        }

        let incarnation = target_invalidation_incarnation.to_owned();
        let insert_incarnation = incarnation.clone();
        let path = self.path.clone();
        self.database
            .with_write_connection("begin source branch sweep", move |connection| {
                diesel::sql_query(
                    "INSERT OR IGNORE INTO console_graph_source_sweep_runs ( \
                         target_invalidation_incarnation, target_invalidation_version, \
                         relation_revision, status, phase, source_upper_bound, page_cursor, \
                         owner_id, lease_epoch, \
                         lease_expires_at_ms \
                     ) VALUES (?, ?, ?, 'building', 'enumerate', ?, NULL, '', 0, 0)",
                )
                .bind::<Text, _>(&insert_incarnation)
                .bind::<BigInt, _>(target_invalidation_version)
                .bind::<BigInt, _>(relation_revision)
                .bind::<Nullable<Text>, _>(None::<&str>)
                .execute(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })?;
                Ok(())
            })
            .await?;
        let path = self.path.clone();
        let row = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT target_invalidation_incarnation, \
                            target_invalidation_version, relation_revision, status, phase, \
                            source_upper_bound, page_cursor, \
                            branch_recheck_node_cursor, branch_recheck_traversal_cursor, \
                            owner_id, lease_epoch, \
                            lease_expires_at_ms \
                     FROM console_graph_source_sweep_runs \
                     WHERE target_invalidation_incarnation = ? \
                       AND target_invalidation_version = ?",
                )
                .bind::<Text, _>(incarnation)
                .bind::<BigInt, _>(target_invalidation_version)
                .get_result::<SourceSweepRunRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        self.claim_existing_source_sweep(row).await
    }

    async fn claim_existing_source_sweep(
        &self,
        row: SourceSweepRunRow,
    ) -> crate::Result<Option<SourceSweepRun>> {
        if row.status == "completed" {
            return Ok(None);
        }
        let owner_id = self.owner_id.clone();
        let update_owner_id = owner_id.clone();
        let lease_epoch = row.lease_epoch.saturating_add(1);
        let incarnation = row.target_invalidation_incarnation.clone();
        let version = row.target_invalidation_version;
        let now_ms = source_time_ms();
        let lease_expires_at_ms = source_lease_deadline_ms();
        let path = self.path.clone();
        let claimed = self
            .database
            .with_write_connection("claim source branch sweep", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_source_sweep_runs \
                     SET owner_id = ?, lease_epoch = ?, lease_expires_at_ms = ?, \
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE target_invalidation_incarnation = ? \
                       AND target_invalidation_version = ? \
                       AND status = 'building' AND lease_epoch = ? \
                       AND (owner_id = '' OR owner_id = ? OR lease_expires_at_ms <= ?)",
                )
                .bind::<Text, _>(&update_owner_id)
                .bind::<BigInt, _>(lease_epoch)
                .bind::<BigInt, _>(lease_expires_at_ms)
                .bind::<Text, _>(&incarnation)
                .bind::<BigInt, _>(version)
                .bind::<BigInt, _>(row.lease_epoch)
                .bind::<Text, _>(&update_owner_id)
                .bind::<BigInt, _>(now_ms)
                .execute(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        if claimed != 1 {
            return crate::error::SourceRefreshBusySnafu {
                resource: format!(
                    "source sweep {}/{}",
                    row.target_invalidation_incarnation, row.target_invalidation_version
                ),
            }
            .fail();
        }
        Ok(Some(SourceSweepRun {
            target_invalidation_incarnation: row.target_invalidation_incarnation,
            target_invalidation_version: row.target_invalidation_version,
            relation_revision: row.relation_revision,
            phase: row.phase,
            source_upper_bound: row.source_upper_bound,
            page_cursor: row.page_cursor,
            owner_id,
            lease_epoch,
        }))
    }

    async fn execute_source_sweep(
        &mut self,
        store: &SqliteGraphStore,
        mut sweep: SourceSweepRun,
    ) -> crate::Result<()> {
        let page_size = self.full_refresh_branch_page_size();
        if sweep.phase == "enumerate" {
            loop {
                let cursor = sweep
                    .page_cursor
                    .as_ref()
                    .map(|name| GraphBranchPageCursor { name: name.clone() });
                let page = store
                    .graph_branches_at_revision_page(
                        sweep.relation_revision,
                        cursor.as_ref(),
                        None,
                        page_size,
                    )
                    .await
                    .context(crate::error::StoreSnafu)?;
                let complete = page.complete;
                let next_scan_cursor = page.next_cursor.map(|cursor| cursor.name);
                if page.branches.is_empty() {
                    let Some(next_scan_cursor) = next_scan_cursor else {
                        break;
                    };
                    self.checkpoint_source_sweep(
                        &mut sweep,
                        "enumerate",
                        Some(next_scan_cursor),
                        false,
                    )
                    .await?;
                    tokio::task::yield_now().await;
                    continue;
                }
                for record in page.branches {
                    let name = record.name.clone();
                    self.refresh_branch_event(
                        store,
                        record,
                        SourceInvalidation {
                            incarnation: &sweep.target_invalidation_incarnation,
                            version: sweep.target_invalidation_version,
                            relation_revision: sweep.relation_revision,
                        },
                        &BTreeSet::new(),
                        true,
                    )
                    .await?;
                    self.checkpoint_source_sweep(&mut sweep, "enumerate", Some(name), false)
                        .await?;
                }
                if let Some(next_scan_cursor) = next_scan_cursor
                    && sweep.page_cursor.as_deref() != Some(next_scan_cursor.as_str())
                {
                    self.checkpoint_source_sweep(
                        &mut sweep,
                        "enumerate",
                        Some(next_scan_cursor),
                        false,
                    )
                    .await?;
                }
                if complete {
                    break;
                }
                tokio::task::yield_now().await;
            }
            self.checkpoint_source_sweep(&mut sweep, "reconcile", None, false)
                .await?;
        }

        if sweep.phase != "reconcile" {
            return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "source_sweep_phase",
                value: sweep.phase,
            }
            .fail();
        }
        loop {
            let names = self
                .tracked_branch_name_page(sweep.page_cursor.as_deref(), page_size)
                .await?;
            if names.is_empty() {
                break;
            }
            let complete = names.len() < page_size.get();
            let current = store
                .graph_branches_at_revision_by_names(sweep.relation_revision, &names)
                .await
                .context(crate::error::StoreSnafu)?
                .into_iter()
                .map(|record| record.name)
                .collect::<HashSet<_>>();
            for name in names {
                if !current.contains(&name) {
                    self.remove_branch_event(
                        &name,
                        &sweep.target_invalidation_incarnation,
                        sweep.target_invalidation_version,
                        sweep.relation_revision,
                    )
                    .await?;
                }
                self.checkpoint_source_sweep(&mut sweep, "reconcile", Some(name), false)
                    .await?;
            }
            if complete {
                break;
            }
            tokio::task::yield_now().await;
        }
        let final_cursor = sweep.page_cursor.clone();
        self.checkpoint_source_sweep(&mut sweep, "reconcile", final_cursor, true)
            .await
    }

    async fn checkpoint_source_sweep(
        &self,
        sweep: &mut SourceSweepRun,
        next_phase: &str,
        next_cursor: Option<String>,
        complete: bool,
    ) -> crate::Result<()> {
        let incarnation = sweep.target_invalidation_incarnation.clone();
        let version = sweep.target_invalidation_version;
        let owner_id = sweep.owner_id.clone();
        let lease_epoch = sweep.lease_epoch;
        let expected_phase = sweep.phase.clone();
        let expected_cursor = sweep.page_cursor.clone();
        let next_phase = next_phase.to_owned();
        let update_phase = next_phase.clone();
        let update_cursor = next_cursor.clone();
        let path = self.path.clone();
        let updated = self
            .database
            .with_write_connection("checkpoint source branch sweep", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_source_sweep_runs \
                     SET status = ?, phase = ?, page_cursor = ?, \
                         branch_recheck_node_cursor = NULL, \
                         branch_recheck_traversal_cursor = NULL, \
                         branch_recheck_raw_node_cursor = NULL, \
                         branch_recheck_raw_traversal_cursor = NULL, \
                         branch_recheck_raw_refresh_id_cursor = NULL, \
                         branch_recheck_active_node_id = NULL, \
                         branch_recheck_active_traversal_kind = NULL, \
                         branch_recheck_child_cursor_relation_revision = NULL, \
                         branch_recheck_child_cursor_node_id = NULL, \
                         lease_expires_at_ms = ?, \
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE target_invalidation_incarnation = ? \
                       AND target_invalidation_version = ? \
                       AND status = 'building' AND owner_id = ? AND lease_epoch = ? \
                       AND phase = ? \
                       AND ((page_cursor IS NULL AND ? IS NULL) OR page_cursor = ?)",
                )
                .bind::<Text, _>(if complete { "completed" } else { "building" })
                .bind::<Text, _>(&update_phase)
                .bind::<Nullable<Text>, _>(update_cursor.as_deref())
                .bind::<BigInt, _>(source_lease_deadline_ms())
                .bind::<Text, _>(&incarnation)
                .bind::<BigInt, _>(version)
                .bind::<Text, _>(&owner_id)
                .bind::<BigInt, _>(lease_epoch)
                .bind::<Text, _>(&expected_phase)
                .bind::<Nullable<Text>, _>(expected_cursor.as_deref())
                .bind::<Nullable<Text>, _>(expected_cursor.as_deref())
                .execute(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        if updated != 1 {
            return Err(diesel::result::Error::NotFound).context(QueryGraphSnapshotStoreSnafu {
                path: self.path.clone(),
            });
        }
        sweep.phase = next_phase;
        sweep.page_cursor = next_cursor;
        Ok(())
    }

    async fn renew_source_sweep_lease(
        &self,
        target_invalidation_incarnation: &str,
        target_invalidation_version: i64,
    ) -> crate::Result<()> {
        if target_invalidation_incarnation.is_empty() || target_invalidation_version <= 0 {
            return Ok(());
        }
        let incarnation = target_invalidation_incarnation.to_owned();
        let owner_id = self.owner_id.clone();
        let path = self.path.clone();
        self.database
            .with_write_connection("renew source branch sweep lease", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_source_sweep_runs SET lease_expires_at_ms = ?, \
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE target_invalidation_incarnation = ? \
                       AND target_invalidation_version = ? \
                       AND status = 'building' AND owner_id = ?",
                )
                .bind::<BigInt, _>(source_lease_deadline_ms())
                .bind::<Text, _>(incarnation)
                .bind::<BigInt, _>(target_invalidation_version)
                .bind::<Text, _>(owner_id)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn source_sweep_recheck_state(
        &self,
        target_invalidation_incarnation: &str,
        target_invalidation_version: i64,
    ) -> crate::Result<SourceSweepRecheckState> {
        if target_invalidation_incarnation.is_empty() || target_invalidation_version <= 0 {
            return Ok(SourceSweepRecheckState::default());
        }
        let incarnation = target_invalidation_incarnation.to_owned();
        let owner_id = self.owner_id.clone();
        let path = self.path.clone();
        let row = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT branch_recheck_node_cursor, branch_recheck_traversal_cursor, \
                            branch_recheck_raw_node_cursor, \
                            branch_recheck_raw_traversal_cursor, \
                            branch_recheck_raw_refresh_id_cursor, \
                            branch_recheck_active_node_id, \
                            branch_recheck_active_traversal_kind, \
                            branch_recheck_child_cursor_relation_revision, \
                            branch_recheck_child_cursor_node_id, lease_epoch \
                     FROM console_graph_source_sweep_runs \
                     WHERE target_invalidation_incarnation = ? \
                       AND target_invalidation_version = ? AND status = 'building' \
                       AND phase = 'enumerate' AND owner_id = ?",
                )
                .bind::<Text, _>(incarnation)
                .bind::<BigInt, _>(target_invalidation_version)
                .bind::<Text, _>(owner_id)
                .get_result::<SourceSweepRecheckStateRow>(connection)
                .optional()
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        let Some(row) = row else {
            return Ok(SourceSweepRecheckState::default());
        };
        let completed_item = optional_queue_item(
            row.branch_recheck_node_cursor,
            row.branch_recheck_traversal_cursor,
            "source_sweep_recheck_cursor",
        )?;
        let raw_cursor = optional_child_recheck_raw_cursor(
            row.branch_recheck_raw_node_cursor,
            row.branch_recheck_raw_traversal_cursor,
            row.branch_recheck_raw_refresh_id_cursor,
            "source_sweep_recheck_raw_cursor",
        )?;
        let active_item = optional_queue_item(
            row.branch_recheck_active_node_id,
            row.branch_recheck_active_traversal_kind,
            "source_sweep_active_recheck_item",
        )?;
        let child_cursor = optional_child_cursor(
            row.branch_recheck_child_cursor_relation_revision,
            row.branch_recheck_child_cursor_node_id,
            "source_sweep_recheck_child_cursor",
        )?;
        ensure!(
            child_cursor.is_none() || active_item.is_some(),
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "source_sweep_recheck_state",
                value: "child cursor without an active item".to_owned(),
            }
        );
        ensure!(
            active_item.is_none() || raw_cursor.is_some(),
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "source_sweep_recheck_state",
                value: "active item without a raw cursor".to_owned(),
            }
        );
        Ok(SourceSweepRecheckState {
            completed_item,
            raw_cursor,
            active_item,
            child_cursor,
            lease_epoch: Some(row.lease_epoch),
        })
    }

    async fn checkpoint_source_sweep_recheck_state(
        &self,
        target_invalidation_incarnation: &str,
        target_invalidation_version: i64,
        state: &mut SourceSweepRecheckState,
        next: SourceSweepRecheckState,
    ) -> crate::Result<()> {
        if target_invalidation_incarnation.is_empty() || target_invalidation_version <= 0 {
            *state = next;
            return Ok(());
        }
        let Some(lease_epoch) = state.lease_epoch else {
            return crate::error::SourceRefreshBusySnafu {
                resource: format!(
                    "source sweep {target_invalidation_incarnation}/{target_invalidation_version}"
                ),
            }
            .fail();
        };
        ensure!(
            next.lease_epoch == Some(lease_epoch),
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "source_sweep_recheck_lease_epoch",
                value: format!("{:?}/{:?}", state.lease_epoch, next.lease_epoch),
            }
        );
        ensure!(
            next.child_cursor.is_none() || next.active_item.is_some(),
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "source_sweep_recheck_state",
                value: "child cursor without an active item".to_owned(),
            }
        );
        ensure!(
            next.active_item.is_none() || next.raw_cursor.is_some(),
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "source_sweep_recheck_state",
                value: "active item without a raw cursor".to_owned(),
            }
        );
        let incarnation = target_invalidation_incarnation.to_owned();
        let owner_id = self.owner_id.clone();
        let expected_completed_node_id = state
            .completed_item
            .as_ref()
            .map(|item| item.node_id.clone());
        let expected_completed_traversal = state
            .completed_item
            .as_ref()
            .map(|item| item.traversal.as_str().to_owned());
        let expected_active_node_id = state.active_item.as_ref().map(|item| item.node_id.clone());
        let expected_active_traversal = state
            .active_item
            .as_ref()
            .map(|item| item.traversal.as_str().to_owned());
        let expected_raw_node_id = state
            .raw_cursor
            .as_ref()
            .map(|cursor| cursor.item.node_id.clone());
        let expected_raw_traversal = state
            .raw_cursor
            .as_ref()
            .map(|cursor| cursor.item.traversal.as_str().to_owned());
        let expected_raw_refresh_id = state.raw_cursor.as_ref().map(|cursor| cursor.refresh_id);
        let expected_child_relation_revision = state
            .child_cursor
            .as_ref()
            .map(|cursor| cursor.relation_revision);
        let expected_child_node_id = state
            .child_cursor
            .as_ref()
            .map(|cursor| cursor.node_id.clone());
        let next_completed_node_id = next
            .completed_item
            .as_ref()
            .map(|item| item.node_id.clone());
        let next_completed_traversal = next
            .completed_item
            .as_ref()
            .map(|item| item.traversal.as_str().to_owned());
        let next_active_node_id = next.active_item.as_ref().map(|item| item.node_id.clone());
        let next_active_traversal = next
            .active_item
            .as_ref()
            .map(|item| item.traversal.as_str().to_owned());
        let next_raw_node_id = next
            .raw_cursor
            .as_ref()
            .map(|cursor| cursor.item.node_id.clone());
        let next_raw_traversal = next
            .raw_cursor
            .as_ref()
            .map(|cursor| cursor.item.traversal.as_str().to_owned());
        let next_raw_refresh_id = next.raw_cursor.as_ref().map(|cursor| cursor.refresh_id);
        let next_child_relation_revision = next
            .child_cursor
            .as_ref()
            .map(|cursor| cursor.relation_revision);
        let next_child_node_id = next
            .child_cursor
            .as_ref()
            .map(|cursor| cursor.node_id.clone());
        let path = self.path.clone();
        let updated = self
            .database
            .with_write_connection("checkpoint source sweep child recheck", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_source_sweep_runs \
                     SET branch_recheck_node_cursor = ?, \
                         branch_recheck_traversal_cursor = ?, \
                         branch_recheck_raw_node_cursor = ?, \
                         branch_recheck_raw_traversal_cursor = ?, \
                         branch_recheck_raw_refresh_id_cursor = ?, \
                         branch_recheck_active_node_id = ?, \
                         branch_recheck_active_traversal_kind = ?, \
                         branch_recheck_child_cursor_relation_revision = ?, \
                         branch_recheck_child_cursor_node_id = ?, lease_expires_at_ms = ?, \
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE target_invalidation_incarnation = ? \
                       AND target_invalidation_version = ? AND status = 'building' \
                       AND phase = 'enumerate' AND owner_id = ? AND lease_epoch = ? \
                       AND branch_recheck_node_cursor IS ? \
                       AND branch_recheck_traversal_cursor IS ? \
                       AND branch_recheck_raw_node_cursor IS ? \
                       AND branch_recheck_raw_traversal_cursor IS ? \
                       AND branch_recheck_raw_refresh_id_cursor IS ? \
                       AND branch_recheck_active_node_id IS ? \
                       AND branch_recheck_active_traversal_kind IS ? \
                       AND branch_recheck_child_cursor_relation_revision IS ? \
                       AND branch_recheck_child_cursor_node_id IS ?",
                )
                .bind::<Nullable<Text>, _>(next_completed_node_id.as_deref())
                .bind::<Nullable<Text>, _>(next_completed_traversal.as_deref())
                .bind::<Nullable<Text>, _>(next_raw_node_id.as_deref())
                .bind::<Nullable<Text>, _>(next_raw_traversal.as_deref())
                .bind::<Nullable<BigInt>, _>(next_raw_refresh_id)
                .bind::<Nullable<Text>, _>(next_active_node_id.as_deref())
                .bind::<Nullable<Text>, _>(next_active_traversal.as_deref())
                .bind::<Nullable<BigInt>, _>(next_child_relation_revision)
                .bind::<Nullable<Text>, _>(next_child_node_id.as_deref())
                .bind::<BigInt, _>(source_lease_deadline_ms())
                .bind::<Text, _>(&incarnation)
                .bind::<BigInt, _>(target_invalidation_version)
                .bind::<Text, _>(&owner_id)
                .bind::<BigInt, _>(lease_epoch)
                .bind::<Nullable<Text>, _>(expected_completed_node_id.as_deref())
                .bind::<Nullable<Text>, _>(expected_completed_traversal.as_deref())
                .bind::<Nullable<Text>, _>(expected_raw_node_id.as_deref())
                .bind::<Nullable<Text>, _>(expected_raw_traversal.as_deref())
                .bind::<Nullable<BigInt>, _>(expected_raw_refresh_id)
                .bind::<Nullable<Text>, _>(expected_active_node_id.as_deref())
                .bind::<Nullable<Text>, _>(expected_active_traversal.as_deref())
                .bind::<Nullable<BigInt>, _>(expected_child_relation_revision)
                .bind::<Nullable<Text>, _>(expected_child_node_id.as_deref())
                .execute(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        if updated != 1 {
            return crate::error::SourceRefreshBusySnafu {
                resource: format!(
                    "source sweep {target_invalidation_incarnation}/{target_invalidation_version}"
                ),
            }
            .fail();
        }
        *state = next;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn reconcile_full_refresh(
        &mut self,
        records: &[GraphBranchRecord],
    ) -> crate::Result<()> {
        let current_names = records
            .iter()
            .map(|record| record.name.clone())
            .collect::<HashSet<_>>();
        let path = self.path.clone();
        let existing = self
            .database
            .with_connection(move |connection| {
                console_graph_source_branches::table
                    .select(console_graph_source_branches::name)
                    .load::<String>(connection)
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        for name in existing
            .into_iter()
            .filter(|name| !current_names.contains(name))
        {
            self.remove_branch(&name).await?;
        }
        Ok(())
    }

    pub(crate) async fn refresh_records(
        &mut self,
        store: &SqliteGraphStore,
        records: impl IntoIterator<Item = GraphBranchRecord>,
    ) -> crate::Result<()> {
        for record in records {
            self.refresh_branch(store, record).await?;
        }
        Ok(())
    }

    pub(crate) async fn refresh_named_batch(
        &mut self,
        store: &SqliteGraphStore,
        names: &[String],
    ) -> crate::Result<()> {
        let requested = names.iter().cloned().collect::<BTreeSet<_>>();
        if requested.is_empty() {
            return Ok(());
        }
        if self
            .requires_full_refresh_for_unknown_missing_branch(store, &requested)
            .await?
        {
            return self
                .refresh_all_branches_peer_first(store, &requested)
                .await;
        }

        let pre_dependents = match self.branches_sharing_dynamic_parents(&requested).await? {
            DynamicBranchScope::Targeted(branches) => branches,
            DynamicBranchScope::FullRefresh => {
                return self
                    .refresh_all_branches_peer_first(store, &requested)
                    .await;
            }
        };
        let pre_peers = pre_dependents
            .difference(&requested)
            .cloned()
            .collect::<BTreeSet<_>>();
        self.refresh_exact_branch_names(store, &pre_peers).await?;
        self.refresh_exact_branch_names(store, &requested).await?;

        let post_dependents = match self.branches_sharing_dynamic_parents(&requested).await? {
            DynamicBranchScope::Targeted(branches) => branches,
            DynamicBranchScope::FullRefresh => {
                return self
                    .refresh_all_branches_peer_first(store, &requested)
                    .await;
            }
        };
        let post_peers = post_dependents
            .difference(&requested)
            .filter(|name| !pre_peers.contains(*name))
            .cloned()
            .collect::<BTreeSet<_>>();
        self.refresh_exact_branch_names(store, &post_peers).await
    }

    async fn refresh_exact_branch_names(
        &mut self,
        store: &SqliteGraphStore,
        names: &BTreeSet<String>,
    ) -> crate::Result<()> {
        let mut after = None;
        loop {
            let names = branch_name_batch_after(names, after.as_deref());
            if names.is_empty() {
                return Ok(());
            }
            after = names.last().cloned();
            self.refresh_exact_branch_batch(store, &names).await?;
            tokio::task::yield_now().await;
        }
    }

    async fn refresh_exact_branch_batch(
        &mut self,
        store: &SqliteGraphStore,
        names: &[String],
    ) -> crate::Result<()> {
        let records = store
            .graph_branches_by_names(names)
            .await
            .context(crate::error::StoreSnafu)?;
        let found = records
            .iter()
            .map(|record| record.name.clone())
            .collect::<HashSet<_>>();
        for name in names.iter().filter(|name| !found.contains(*name)) {
            self.remove_branch(name).await?;
        }
        self.refresh_records(store, records).await
    }

    async fn refresh_all_branches_peer_first(
        &mut self,
        store: &SqliteGraphStore,
        requested: &BTreeSet<String>,
    ) -> crate::Result<()> {
        let page_size = self.full_refresh_branch_page_size();
        let high_watermark = store
            .graph_branch_name_high_watermark()
            .await
            .context(crate::error::StoreSnafu)?;
        if let Some(high_watermark) = high_watermark {
            let mut cursor = None;
            loop {
                let page = store
                    .graph_branches_page(cursor.as_ref(), &high_watermark, page_size)
                    .await
                    .context(crate::error::StoreSnafu)?;
                let mut records = page.branches;
                if records.is_empty() {
                    break;
                }
                #[cfg(test)]
                {
                    self.full_refresh_source_page_count += 1;
                }
                records.retain(|record| !requested.contains(&record.name));
                self.refresh_records(store, records).await?;
                if page.complete {
                    break;
                }
                cursor = page.next_cursor;
                tokio::task::yield_now().await;
            }
        }

        self.refresh_exact_branch_names(store, requested).await?;
        self.reconcile_full_refresh_bounded(store).await
    }

    pub(crate) async fn refresh_all_branches_bounded(
        &mut self,
        store: &SqliteGraphStore,
    ) -> crate::Result<()> {
        self.refresh_all_branches_peer_first(store, &BTreeSet::new())
            .await
    }

    async fn reconcile_full_refresh_bounded(
        &mut self,
        store: &SqliteGraphStore,
    ) -> crate::Result<()> {
        let page_size = self.full_refresh_branch_page_size();
        let mut after = None;
        loop {
            let names = self
                .tracked_branch_name_page(after.as_deref(), page_size)
                .await?;
            if names.is_empty() {
                return Ok(());
            }
            let complete = names.len() < page_size.get();
            after = names.last().cloned();
            let current = store
                .graph_branches_by_names(&names)
                .await
                .context(crate::error::StoreSnafu)?
                .into_iter()
                .map(|record| record.name)
                .collect::<HashSet<_>>();
            for name in names.iter().filter(|name| !current.contains(*name)) {
                self.remove_branch(name).await?;
            }
            if complete {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    async fn tracked_branch_name_page(
        &self,
        after: Option<&str>,
        page_size: NonZeroUsize,
    ) -> crate::Result<Vec<String>> {
        let after = after.map(str::to_owned);
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                (|| {
                    let published = if let Some(after) = after.as_deref() {
                        diesel::sql_query(
                            "SELECT name FROM console_graph_source_branches \
                             WHERE name > ? ORDER BY name LIMIT ?",
                        )
                        .bind::<Text, _>(after)
                        .bind::<BigInt, _>(page_size.get() as i64)
                        .load::<BranchNameRow>(connection)?
                    } else {
                        diesel::sql_query(
                            "SELECT name FROM console_graph_source_branches \
                             ORDER BY name LIMIT ?",
                        )
                        .bind::<BigInt, _>(page_size.get() as i64)
                        .load::<BranchNameRow>(connection)?
                    };
                    let building = if let Some(after) = after.as_deref() {
                        diesel::sql_query(
                            "SELECT branch_name AS name \
                             FROM console_graph_source_refresh_runs \
                                  INDEXED BY console_graph_source_refresh_runs_active_branch_idx \
                             WHERE status = 'building' AND branch_name > ? \
                             ORDER BY branch_name LIMIT ?",
                        )
                        .bind::<Text, _>(after)
                        .bind::<BigInt, _>(page_size.get() as i64)
                        .load::<BranchNameRow>(connection)?
                    } else {
                        diesel::sql_query(
                            "SELECT branch_name AS name \
                             FROM console_graph_source_refresh_runs \
                                  INDEXED BY console_graph_source_refresh_runs_active_branch_idx \
                             WHERE status = 'building' \
                             ORDER BY branch_name LIMIT ?",
                        )
                        .bind::<BigInt, _>(page_size.get() as i64)
                        .load::<BranchNameRow>(connection)?
                    };
                    Ok(published
                        .into_iter()
                        .chain(building)
                        .map(|row| row.name)
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .take(page_size.get())
                        .collect())
                })()
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn requires_full_refresh_for_unknown_missing_branch(
        &self,
        store: &SqliteGraphStore,
        requested: &BTreeSet<String>,
    ) -> crate::Result<bool> {
        let mut after = None;
        loop {
            let names = branch_name_batch_after(requested, after.as_deref());
            if names.is_empty() {
                return Ok(false);
            }
            after = names.last().cloned();
            let path = self.path.clone();
            let query_names = names.clone();
            let published = self
                .database
                .with_connection(move |connection| {
                    console_graph_source_branches::table
                        .filter(console_graph_source_branches::name.eq_any(query_names))
                        .select(console_graph_source_branches::name)
                        .load::<String>(connection)
                        .map(|names| names.into_iter().collect::<HashSet<_>>())
                        .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
            let current = store
                .graph_branches_by_names(&names)
                .await
                .context(crate::error::StoreSnafu)?
                .into_iter()
                .map(|record| record.name)
                .collect::<HashSet<_>>();
            if names
                .iter()
                .any(|name| !published.contains(name) && !current.contains(name))
            {
                return Ok(true);
            }
            tokio::task::yield_now().await;
        }
    }

    async fn branches_sharing_dynamic_parents(
        &mut self,
        origin_branches: &BTreeSet<String>,
    ) -> crate::Result<DynamicBranchScope> {
        let targeted_limit = self.targeted_dynamic_branch_limit();
        if origin_branches.len() > targeted_limit {
            return Ok(DynamicBranchScope::FullRefresh);
        }
        let origins = origin_branches.iter().cloned().collect::<Vec<_>>();
        let request_key = serde_json::to_string(&(targeted_limit, &origins)).context(
            SerializeGraphSnapshotStoreValueSnafu {
                column: "affected_branch_scan_request_key",
            },
        )?;
        loop {
            let mut scan = self
                .begin_or_claim_dynamic_branch_scan(
                    "affected",
                    request_key.clone(),
                    None,
                    None,
                    Some(targeted_limit),
                    origins.clone(),
                )
                .await?;
            let mut retry_claim = false;
            while scan.status == "building" {
                let advanced = if scan.active_origin_branch_name.is_none() {
                    self.activate_affected_scan_origin(&mut scan).await
                } else if scan.active_origin_node_id.is_some() {
                    self.load_affected_branch_page(&mut scan, targeted_limit)
                        .await
                } else {
                    self.advance_affected_scan_origin(&mut scan).await
                };
                if let Err(error) = advanced {
                    if dynamic_scan_fence_lost(&error) {
                        retry_claim = true;
                        break;
                    }
                    return Err(error);
                }
                tokio::task::yield_now().await;
            }
            if retry_claim {
                continue;
            }
            match self
                .finish_affected_branch_scan(&scan, targeted_limit)
                .await
            {
                Ok(scope) => return Ok(scope),
                Err(error) if dynamic_scan_fence_lost(&error) => continue,
                Err(error) => return Err(error),
            }
        }
    }

    async fn activate_affected_scan_origin(
        &self,
        scan: &mut DynamicBranchScan,
    ) -> crate::Result<()> {
        let scan_id = scan.scan_id;
        let owner_id = scan.owner_id.clone();
        let lease_epoch = scan.lease_epoch;
        let frozen_source_revision = scan.source_revision;
        let raw_refresh_id_upper_bound = scan.raw_refresh_id_upper_bound;
        let expected_origin_cursor = scan.origin_branch_cursor.clone();
        let path = self.path.clone();
        let (active_origin, completed) = self
            .database
            .with_write_connection("activate affected branch scan origin", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let now_ms = source_time_ms();
                        let origin = if let Some(cursor) = &expected_origin_cursor {
                            diesel::sql_query(
                                "SELECT branch_name AS name \
                                 FROM console_graph_source_dynamic_branch_scan_origins \
                                 WHERE scan_id = ? AND branch_name > ? \
                                 ORDER BY branch_name LIMIT 1",
                            )
                            .bind::<BigInt, _>(scan_id)
                            .bind::<Text, _>(cursor)
                            .get_result::<BranchNameRow>(connection)
                            .optional()?
                        } else {
                            diesel::sql_query(
                                "SELECT branch_name AS name \
                                 FROM console_graph_source_dynamic_branch_scan_origins \
                                 WHERE scan_id = ? ORDER BY branch_name LIMIT 1",
                            )
                            .bind::<BigInt, _>(scan_id)
                            .get_result::<BranchNameRow>(connection)
                            .optional()?
                        };
                        let completed = origin.is_none();
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_source_dynamic_branch_scans \
                             SET active_origin_branch_name = ?, status = ?, \
                                 lease_expires_at_ms = ?, \
                                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE scan_id = ? AND scan_kind = 'affected' \
                               AND status = 'building' AND owner_id = ? \
                               AND lease_epoch = ? AND source_revision = ? \
                               AND raw_refresh_id_upper_bound = ? \
                               AND lease_expires_at_ms > ? \
                               AND active_origin_branch_name IS NULL \
                               AND origin_branch_cursor IS ? \
                               AND origin_raw_node_cursor IS NULL \
                               AND active_origin_node_id IS NULL \
                               AND candidate_raw_branch_cursor IS NULL",
                        )
                        .bind::<Nullable<Text>, _>(origin.as_ref().map(|row| row.name.as_str()))
                        .bind::<Text, _>(if completed { "completed" } else { "building" })
                        .bind::<BigInt, _>(source_lease_deadline_ms())
                        .bind::<BigInt, _>(scan_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<BigInt, _>(frozen_source_revision)
                        .bind::<BigInt, _>(raw_refresh_id_upper_bound)
                        .bind::<BigInt, _>(now_ms)
                        .bind::<Nullable<Text>, _>(expected_origin_cursor.as_deref())
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok((origin.map(|row| row.name), completed))
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        scan.active_origin_branch_name = active_origin;
        if completed {
            scan.status = "completed".to_owned();
        }
        Ok(())
    }

    async fn advance_affected_scan_origin(
        &self,
        scan: &mut DynamicBranchScan,
    ) -> crate::Result<()> {
        let scan_id = scan.scan_id;
        let owner_id = scan.owner_id.clone();
        let lease_epoch = scan.lease_epoch;
        let frozen_source_revision = scan.source_revision;
        let raw_refresh_id_upper_bound = scan.raw_refresh_id_upper_bound;
        let origin_branch = scan
            .active_origin_branch_name
            .clone()
            .expect("active affected scan origin should be present");
        let update_origin_branch = origin_branch.clone();
        let expected_raw_cursor = scan.origin_raw_cursor.clone();
        let expected_node_id = expected_raw_cursor
            .as_ref()
            .map(|cursor| cursor.node_id.clone());
        let expected_traversal = expected_raw_cursor
            .as_ref()
            .map(|cursor| cursor.traversal.as_str().to_owned());
        let expected_refresh_id = expected_raw_cursor.as_ref().map(|cursor| cursor.refresh_id);
        let completed_origin_node_id = scan.completed_origin_node_id.clone();
        let path = self.path.clone();
        let (next_raw_cursor, active_node, origin_complete) = self
            .database
            .with_write_connection("advance affected scan origin", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let now_ms = source_time_ms();
                        let rows = load_dynamic_origin_raw_candidates(
                            connection,
                            &update_origin_branch,
                            frozen_source_revision,
                            raw_refresh_id_upper_bound,
                            expected_raw_cursor.as_ref(),
                        )?;
                        let mut selected = None;
                        let mut last = None;
                        for row in &rows {
                            last = Some((
                                row.node_id.clone(),
                                row.traversal_kind.clone(),
                                row.refresh_id,
                            ));
                            if row.eligible != 0
                                && completed_origin_node_id.as_deref() != Some(row.node_id.as_str())
                            {
                                selected = Some(row.node_id.clone());
                                break;
                            }
                        }
                        let origin_complete = selected.is_none()
                            && rows.len() < SOURCE_DYNAMIC_BRANCH_CANDIDATE_PAGE_SIZE;
                        let next_node_id = (!origin_complete)
                            .then(|| last.as_ref().map(|cursor| cursor.0.as_str()))
                            .flatten();
                        let next_traversal = (!origin_complete)
                            .then(|| last.as_ref().map(|cursor| cursor.1.as_str()))
                            .flatten();
                        let next_refresh_id = (!origin_complete)
                            .then(|| last.as_ref().map(|cursor| cursor.2))
                            .flatten();
                        let next_origin_cursor =
                            origin_complete.then_some(update_origin_branch.as_str());
                        let next_active_origin =
                            (!origin_complete).then_some(update_origin_branch.as_str());
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_source_dynamic_branch_scans \
                             SET origin_branch_cursor = COALESCE(?, origin_branch_cursor), \
                                 active_origin_branch_name = ?, \
                                 origin_raw_node_cursor = ?, \
                                 origin_raw_traversal_cursor = ?, \
                                 origin_raw_refresh_id_cursor = ?, \
                                 completed_origin_node_id = \
                                     CASE WHEN ? THEN NULL ELSE completed_origin_node_id END, \
                                 active_origin_node_id = ?, \
                                 candidate_raw_branch_cursor = NULL, \
                                 candidate_raw_refresh_id_cursor = NULL, \
                                 candidate_raw_traversal_cursor = NULL, \
                                 lease_expires_at_ms = ?, \
                                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE scan_id = ? AND scan_kind = 'affected' \
                               AND status = 'building' AND owner_id = ? \
                               AND lease_epoch = ? AND source_revision = ? \
                               AND raw_refresh_id_upper_bound = ? \
                               AND lease_expires_at_ms > ? \
                               AND active_origin_branch_name = ? \
                               AND origin_raw_node_cursor IS ? \
                               AND origin_raw_traversal_cursor IS ? \
                               AND origin_raw_refresh_id_cursor IS ? \
                               AND active_origin_node_id IS NULL",
                        )
                        .bind::<Nullable<Text>, _>(next_origin_cursor)
                        .bind::<Nullable<Text>, _>(next_active_origin)
                        .bind::<Nullable<Text>, _>(next_node_id)
                        .bind::<Nullable<Text>, _>(next_traversal)
                        .bind::<Nullable<BigInt>, _>(next_refresh_id)
                        .bind::<Integer, _>(i32::from(origin_complete))
                        .bind::<Nullable<Text>, _>(selected.as_deref())
                        .bind::<BigInt, _>(source_lease_deadline_ms())
                        .bind::<BigInt, _>(scan_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<BigInt, _>(frozen_source_revision)
                        .bind::<BigInt, _>(raw_refresh_id_upper_bound)
                        .bind::<BigInt, _>(now_ms)
                        .bind::<Text, _>(&update_origin_branch)
                        .bind::<Nullable<Text>, _>(expected_node_id.as_deref())
                        .bind::<Nullable<Text>, _>(expected_traversal.as_deref())
                        .bind::<Nullable<BigInt>, _>(expected_refresh_id)
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok((last, selected, origin_complete))
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        if origin_complete {
            scan.origin_branch_cursor = Some(origin_branch);
            scan.active_origin_branch_name = None;
            scan.origin_raw_cursor = None;
            scan.completed_origin_node_id = None;
        } else if let Some((node_id, traversal, refresh_id)) = next_raw_cursor {
            scan.origin_raw_cursor = Some(DynamicOriginRawCursor {
                node_id,
                traversal: TraversalKind::parse(&traversal)?,
                refresh_id,
            });
        }
        scan.active_origin_node_id = active_node;
        scan.candidate_raw_cursor = None;
        Ok(())
    }

    async fn load_affected_branch_page(
        &self,
        scan: &mut DynamicBranchScan,
        targeted_limit: usize,
    ) -> crate::Result<()> {
        let scan_id = scan.scan_id;
        let owner_id = scan.owner_id.clone();
        let lease_epoch = scan.lease_epoch;
        let frozen_source_revision = scan.source_revision;
        let raw_refresh_id_upper_bound = scan.raw_refresh_id_upper_bound;
        let active_node_id = scan
            .active_origin_node_id
            .clone()
            .expect("active affected scan node should be present");
        let update_active_node_id = active_node_id.clone();
        let existing_result_count = scan.result_count;
        let expected_cursor = scan.candidate_raw_cursor.clone();
        let expected_branch = expected_cursor
            .as_ref()
            .map(|cursor| cursor.branch_name.clone());
        let expected_refresh_id = expected_cursor.as_ref().map(|cursor| cursor.refresh_id);
        let expected_traversal = expected_cursor
            .as_ref()
            .map(|cursor| cursor.traversal.as_str().to_owned());
        let targeted_limit = i64::try_from(targeted_limit).unwrap_or(i64::MAX);
        let path = self.path.clone();
        let (next_cursor, inserted_count, completed, exceeded_limit) = self
            .database
            .with_write_connection("advance affected branch candidates", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let now_ms = source_time_ms();
                        let rows = load_dynamic_branch_raw_candidates(
                            connection,
                            &update_active_node_id,
                            frozen_source_revision,
                            raw_refresh_id_upper_bound,
                            expected_cursor.as_ref(),
                        )?;
                        let mut inserted_count = 0_i64;
                        let mut next_cursor = None;
                        let mut exceeded_limit = false;
                        for row in &rows {
                            next_cursor = Some((
                                row.branch_name.clone(),
                                row.refresh_id,
                                row.traversal_kind.clone(),
                            ));
                            if row.eligible != 0 {
                                inserted_count += diesel::sql_query(
                                    "INSERT OR IGNORE INTO \
                                         console_graph_source_dynamic_branch_scan_results ( \
                                             scan_id, branch_name \
                                         ) VALUES (?, ?)",
                                )
                                .bind::<BigInt, _>(scan_id)
                                .bind::<Text, _>(&row.branch_name)
                                .execute(connection)?
                                    as i64;
                                if existing_result_count.saturating_add(inserted_count)
                                    > targeted_limit
                                {
                                    exceeded_limit = true;
                                    break;
                                }
                            }
                        }
                        let candidate_complete = !exceeded_limit
                            && rows.len() < SOURCE_DYNAMIC_BRANCH_CANDIDATE_PAGE_SIZE;
                        let completed = exceeded_limit || candidate_complete;
                        let next_branch = (!completed)
                            .then(|| next_cursor.as_ref().map(|cursor| cursor.0.as_str()))
                            .flatten();
                        let next_refresh_id = (!completed)
                            .then(|| next_cursor.as_ref().map(|cursor| cursor.1))
                            .flatten();
                        let next_traversal = (!completed)
                            .then(|| next_cursor.as_ref().map(|cursor| cursor.2.as_str()))
                            .flatten();
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_source_dynamic_branch_scans \
                             SET candidate_raw_branch_cursor = ?, \
                                 candidate_raw_refresh_id_cursor = ?, \
                                 candidate_raw_traversal_cursor = ?, \
                                 completed_origin_node_id = \
                                     CASE WHEN ? THEN active_origin_node_id \
                                          ELSE completed_origin_node_id END, \
                                 active_origin_node_id = \
                                     CASE WHEN ? THEN NULL ELSE active_origin_node_id END, \
                                 result_count = result_count + ?, \
                                 exceeded_limit = MAX(exceeded_limit, ?), \
                                 status = CASE WHEN ? THEN 'completed' ELSE status END, \
                                 lease_expires_at_ms = ?, \
                                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE scan_id = ? AND scan_kind = 'affected' \
                               AND status = 'building' AND owner_id = ? \
                               AND lease_epoch = ? AND source_revision = ? \
                               AND raw_refresh_id_upper_bound = ? \
                               AND lease_expires_at_ms > ? \
                               AND active_origin_node_id = ? \
                               AND candidate_raw_branch_cursor IS ? \
                               AND candidate_raw_refresh_id_cursor IS ? \
                               AND candidate_raw_traversal_cursor IS ?",
                        )
                        .bind::<Nullable<Text>, _>(next_branch)
                        .bind::<Nullable<BigInt>, _>(next_refresh_id)
                        .bind::<Nullable<Text>, _>(next_traversal)
                        .bind::<Integer, _>(i32::from(candidate_complete))
                        .bind::<Integer, _>(i32::from(candidate_complete))
                        .bind::<BigInt, _>(inserted_count)
                        .bind::<Integer, _>(i32::from(exceeded_limit))
                        .bind::<Integer, _>(i32::from(exceeded_limit))
                        .bind::<BigInt, _>(source_lease_deadline_ms())
                        .bind::<BigInt, _>(scan_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<BigInt, _>(frozen_source_revision)
                        .bind::<BigInt, _>(raw_refresh_id_upper_bound)
                        .bind::<BigInt, _>(now_ms)
                        .bind::<Text, _>(&update_active_node_id)
                        .bind::<Nullable<Text>, _>(expected_branch.as_deref())
                        .bind::<Nullable<BigInt>, _>(expected_refresh_id)
                        .bind::<Nullable<Text>, _>(expected_traversal.as_deref())
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok((next_cursor, inserted_count, completed, exceeded_limit))
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        scan.result_count = scan.result_count.saturating_add(inserted_count);
        if exceeded_limit {
            scan.exceeded_limit = true;
            scan.status = "completed".to_owned();
        } else if completed {
            scan.completed_origin_node_id = Some(active_node_id);
            scan.active_origin_node_id = None;
            scan.candidate_raw_cursor = None;
        } else if let Some((branch_name, refresh_id, traversal)) = next_cursor {
            scan.candidate_raw_cursor = Some(DynamicBranchRawCursor {
                branch_name,
                refresh_id,
                traversal: TraversalKind::parse(&traversal)?,
            });
        }
        Ok(())
    }

    async fn finish_affected_branch_scan(
        &self,
        scan: &DynamicBranchScan,
        targeted_limit: usize,
    ) -> crate::Result<DynamicBranchScope> {
        if scan.exceeded_limit {
            self.discard_dynamic_branch_scan_bounded(scan).await?;
            return Ok(DynamicBranchScope::FullRefresh);
        }
        let scan_id = scan.scan_id;
        let owner_id = scan.owner_id.clone();
        let lease_epoch = scan.lease_epoch;
        let frozen_source_revision = scan.source_revision;
        let raw_refresh_id_upper_bound = scan.raw_refresh_id_upper_bound;
        let result_limit = i64::try_from(targeted_limit.saturating_add(1)).unwrap_or(i64::MAX);
        let path = self.path.clone();
        let rows = self
            .database
            .with_write_connection("finish affected branch scan", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let now_ms = source_time_ms();
                        let rows = diesel::sql_query(
                            "SELECT branch_name AS name \
                             FROM console_graph_source_dynamic_branch_scan_results \
                             WHERE scan_id = ? ORDER BY branch_name LIMIT ?",
                        )
                        .bind::<BigInt, _>(scan_id)
                        .bind::<BigInt, _>(result_limit)
                        .load::<BranchNameRow>(connection)?;
                        let deleted = diesel::sql_query(
                            "DELETE FROM console_graph_source_dynamic_branch_scans \
                             WHERE scan_id = ? AND status = 'completed' \
                               AND owner_id = ? AND lease_epoch = ? \
                               AND source_revision = ? \
                               AND raw_refresh_id_upper_bound = ? \
                               AND lease_expires_at_ms > ?",
                        )
                        .bind::<BigInt, _>(scan_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<BigInt, _>(frozen_source_revision)
                        .bind::<BigInt, _>(raw_refresh_id_upper_bound)
                        .bind::<BigInt, _>(now_ms)
                        .execute(connection)?;
                        if deleted != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(rows)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        if rows.len() > targeted_limit {
            Ok(DynamicBranchScope::FullRefresh)
        } else {
            Ok(DynamicBranchScope::Targeted(
                rows.into_iter().map(|row| row.name).collect(),
            ))
        }
    }

    async fn discard_dynamic_branch_scan_bounded(
        &self,
        scan: &DynamicBranchScan,
    ) -> crate::Result<()> {
        loop {
            if self.discard_dynamic_branch_scan_step(scan).await? {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    async fn discard_dynamic_branch_scan_step(
        &self,
        scan: &DynamicBranchScan,
    ) -> crate::Result<bool> {
        let scan_id = scan.scan_id;
        let owner_id = scan.owner_id.clone();
        let lease_epoch = scan.lease_epoch;
        let frozen_source_revision = scan.source_revision;
        let raw_refresh_id_upper_bound = scan.raw_refresh_id_upper_bound;
        let path = self.path.clone();
        self.database
            .with_write_connection("discard dynamic branch scan step", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let now_ms = source_time_ms();
                        let renewed = diesel::sql_query(
                            "UPDATE console_graph_source_dynamic_branch_scans \
                             SET lease_expires_at_ms = ?, \
                                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE scan_id = ? AND status = 'completed' \
                               AND owner_id = ? AND lease_epoch = ? \
                               AND source_revision = ? \
                               AND raw_refresh_id_upper_bound = ? \
                               AND lease_expires_at_ms > ?",
                        )
                        .bind::<BigInt, _>(source_lease_deadline_ms())
                        .bind::<BigInt, _>(scan_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<BigInt, _>(frozen_source_revision)
                        .bind::<BigInt, _>(raw_refresh_id_upper_bound)
                        .bind::<BigInt, _>(now_ms)
                        .execute(connection)?;
                        if renewed != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        let deleted_results = diesel::sql_query(
                            "DELETE FROM \
                                 console_graph_source_dynamic_branch_scan_results \
                             WHERE rowid IN ( \
                                 SELECT rowid \
                                 FROM console_graph_source_dynamic_branch_scan_results \
                                 WHERE scan_id = ? \
                                 ORDER BY branch_name LIMIT ? \
                             )",
                        )
                        .bind::<BigInt, _>(scan_id)
                        .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                        .execute(connection)?;
                        if deleted_results > 0 {
                            return Ok(false);
                        }
                        let deleted_origins = diesel::sql_query(
                            "DELETE FROM \
                                 console_graph_source_dynamic_branch_scan_origins \
                             WHERE rowid IN ( \
                                 SELECT rowid \
                                 FROM console_graph_source_dynamic_branch_scan_origins \
                                 WHERE scan_id = ? \
                                 ORDER BY branch_name LIMIT ? \
                             )",
                        )
                        .bind::<BigInt, _>(scan_id)
                        .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                        .execute(connection)?;
                        if deleted_origins > 0 {
                            return Ok(false);
                        }
                        let deleted = diesel::sql_query(
                            "DELETE FROM console_graph_source_dynamic_branch_scans \
                             WHERE scan_id = ? AND status = 'completed' \
                               AND owner_id = ? AND lease_epoch = ? \
                               AND source_revision = ? \
                               AND raw_refresh_id_upper_bound = ? \
                               AND lease_expires_at_ms > ? \
                               AND NOT EXISTS ( \
                                   SELECT 1 \
                                   FROM \
                                       console_graph_source_dynamic_branch_scan_results \
                                   WHERE scan_id = ? LIMIT 1 \
                               ) \
                               AND NOT EXISTS ( \
                                   SELECT 1 \
                                   FROM \
                                       console_graph_source_dynamic_branch_scan_origins \
                                   WHERE scan_id = ? LIMIT 1 \
                               )",
                        )
                        .bind::<BigInt, _>(scan_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<BigInt, _>(frozen_source_revision)
                        .bind::<BigInt, _>(raw_refresh_id_upper_bound)
                        .bind::<BigInt, _>(now_ms)
                        .bind::<BigInt, _>(scan_id)
                        .bind::<BigInt, _>(scan_id)
                        .execute(connection)?;
                        if deleted != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(true)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    fn targeted_dynamic_branch_limit(&self) -> usize {
        #[cfg(test)]
        {
            self.targeted_dynamic_branch_limit
        }
        #[cfg(not(test))]
        {
            TARGETED_DYNAMIC_BRANCH_LIMIT
        }
    }

    fn full_refresh_branch_page_size(&self) -> NonZeroUsize {
        #[cfg(test)]
        {
            self.full_refresh_branch_page_size
        }
        #[cfg(not(test))]
        {
            NonZeroUsize::new(GRAPH_READ_BATCH_SIZE)
                .expect("graph read batch size should be non-zero")
        }
    }

    pub(crate) fn graph_store(&self) -> PersistentGraphStore {
        PersistentGraphStore {
            root_id: self.root_id.clone(),
            database: self.database.clone(),
            path: self.path.clone(),
        }
    }

    async fn published_branch(&self, name: &str) -> crate::Result<Option<PublishedBranch>> {
        let name = name.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT branch.head_id, branch.state_json, \
                            branch.contribution_generation, publication.source_revision \
                     FROM console_graph_source_branches AS branch \
                     INNER JOIN console_graph_source_branch_publications AS publication \
                         ON publication.branch_name = branch.name \
                        AND publication.target_contribution_generation = \
                            branch.contribution_generation \
                     WHERE branch.name = ?",
                )
                .bind::<Text, _>(name)
                .get_result::<PublishedBranchRow>(connection)
                .optional()
                .map(|published| {
                    published.map(|published| PublishedBranch {
                        head_id: published.head_id,
                        state_json: published.state_json,
                        contribution_generation: published.contribution_generation,
                        source_revision: published.source_revision,
                    })
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn update_published_branch_state(
        &self,
        record: &GraphBranchRecord,
        published: &PublishedBranch,
        target_invalidation_incarnation: &str,
        target_invalidation_version: i64,
        invalidation_kind: &str,
        relation_revision: i64,
    ) -> crate::Result<bool> {
        let state_json = serde_json::to_string(&record.state).context(
            SerializeGraphSnapshotStoreValueSnafu {
                column: "state_json",
            },
        )?;
        let name = record.name.clone();
        let head_id = published.head_id.clone();
        let previous_state_json = published.state_json.clone();
        let contribution_generation = published.contribution_generation;
        let expected_source_revision = published.source_revision;
        let target_invalidation_incarnation = target_invalidation_incarnation.to_owned();
        let invalidation_kind = invalidation_kind.to_owned();
        let path = self.path.clone();
        self.database
            .with_write_connection("update published source branch state", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let publication_matches = diesel::sql_query(
                            "SELECT CASE WHEN EXISTS ( \
                                 SELECT 1 FROM console_graph_source_branch_publications \
                                 WHERE branch_name = ? \
                                   AND target_contribution_generation = ? \
                                   AND source_revision = ? \
                             ) THEN 1 ELSE 0 END AS value",
                        )
                        .bind::<Text, _>(&name)
                        .bind::<BigInt, _>(contribution_generation)
                        .bind::<BigInt, _>(expected_source_revision)
                        .get_result::<FlagRow>(connection)?
                        .value
                            != 0;
                        if !publication_matches {
                            return Ok(false);
                        }
                        let updated = diesel::update(
                            console_graph_source_branches::table
                                .filter(console_graph_source_branches::name.eq(&name))
                                .filter(console_graph_source_branches::head_id.eq(&head_id))
                                .filter(
                                    console_graph_source_branches::contribution_generation
                                        .eq(contribution_generation),
                                )
                                .filter(
                                    console_graph_source_branches::state_json
                                        .eq(&previous_state_json),
                                ),
                        )
                        .set(console_graph_source_branches::state_json.eq(&state_json))
                        .execute(connection)?;
                        if updated != 1 {
                            return Ok(false);
                        }
                        let source_revision = advance_source_revision(connection)?;
                        insert_source_branch_history(
                            connection,
                            &name,
                            source_revision,
                            Some(contribution_generation),
                            Some(&head_id),
                            Some(&state_json),
                        )?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_publications ( \
                                 branch_name, target_contribution_generation, source_revision \
                             ) VALUES (?, ?, ?) \
                             ON CONFLICT(branch_name) DO UPDATE SET \
                                 target_contribution_generation = \
                                     excluded.target_contribution_generation, \
                                 source_revision = excluded.source_revision",
                        )
                        .bind::<Text, _>(&name)
                        .bind::<BigInt, _>(contribution_generation)
                        .bind::<BigInt, _>(source_revision)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_change_journal ( \
                                 source_revision, target_invalidation_incarnation, \
                                 target_invalidation_version, \
                                 branch_name, change_kind, \
                                 target_contribution_generation, head_id, state_json \
                             ) VALUES (?, ?, ?, ?, 'metadata', ?, ?, ?)",
                        )
                        .bind::<BigInt, _>(source_revision)
                        .bind::<Text, _>(&target_invalidation_incarnation)
                        .bind::<BigInt, _>(target_invalidation_version)
                        .bind::<Text, _>(&name)
                        .bind::<BigInt, _>(contribution_generation)
                        .bind::<Text, _>(&head_id)
                        .bind::<Text, _>(&state_json)
                        .execute(connection)?;
                        if target_invalidation_version > 0 {
                            insert_source_invalidation_receipt(
                                connection,
                                &target_invalidation_incarnation,
                                target_invalidation_version,
                                &name,
                                source_revision,
                                relation_revision,
                                None,
                                &invalidation_kind,
                                &head_id,
                                &state_json,
                                &[],
                            )?;
                        }
                        Ok(true)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn freeze_dirty_seeds(
        &self,
        store: &SqliteGraphStore,
        dirty_node_ids: &BTreeSet<String>,
        relation_revision: i64,
    ) -> crate::Result<Vec<DirtySeed>> {
        let node_ids = dirty_node_ids
            .iter()
            .filter(|node_id| !node_id.is_empty())
            .take(SOURCE_DIRTY_SEED_LIMIT + 1)
            .collect::<Vec<_>>();
        if node_ids.len() > SOURCE_DIRTY_SEED_LIMIT {
            return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "source_dirty_seed_count",
                value: node_ids.len().to_string(),
            }
            .fail();
        }
        let mut seeds = Vec::with_capacity(node_ids.len());
        for node_id in node_ids {
            let child_high_watermark = store
                .graph_child_high_watermark_at_revision(node_id, relation_revision)
                .await
                .context(crate::error::StoreSnafu)?;
            seeds.push(DirtySeed {
                node_id: node_id.clone(),
                child_high_watermark,
            });
        }
        Ok(seeds)
    }

    async fn source_invalidation_receipt_exists(
        &self,
        target_invalidation_incarnation: &str,
        target_invalidation_version: i64,
        record: &GraphBranchRecord,
        dirty_seeds: &[DirtySeed],
        invalidation_kind: &str,
        relation_revision: i64,
    ) -> crate::Result<Option<i64>> {
        if target_invalidation_incarnation.is_empty() || target_invalidation_version <= 0 {
            return Ok(None);
        }
        let target_state_json = serde_json::to_string(&record.state).context(
            SerializeGraphSnapshotStoreValueSnafu {
                column: "state_json",
            },
        )?;
        let incarnation = target_invalidation_incarnation.to_owned();
        let invalidation_kind = invalidation_kind.to_owned();
        let branch = record.name.clone();
        let path = self.path.clone();
        let receipt = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT receipt_id, target_head_id, target_state_json, invalidation_kind, \
                            source_revision, relation_revision \
                     FROM console_graph_source_invalidation_receipts \
                     WHERE target_invalidation_incarnation = ? \
                       AND target_invalidation_version = ? AND branch_name = ? \
                     ORDER BY receipt_id DESC LIMIT 1",
                )
                .bind::<Text, _>(incarnation)
                .bind::<BigInt, _>(target_invalidation_version)
                .bind::<Text, _>(branch)
                .get_result::<InvalidationReceiptRow>(connection)
                .optional()
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        let Some(receipt) = receipt else {
            return Ok(None);
        };
        if receipt.target_head_id != record.head_id
            || receipt.target_state_json != target_state_json
            || receipt.invalidation_kind != invalidation_kind
            || receipt.relation_revision != relation_revision
        {
            return Ok(None);
        }
        let receipt_id = receipt.receipt_id;
        let path = self.path.clone();
        let persisted = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT node_id, child_high_watermark_relation_revision, \
                            child_high_watermark_node_id \
                     FROM console_graph_source_invalidation_receipt_seeds \
                     WHERE receipt_id = ? ORDER BY node_id LIMIT ?",
                )
                .bind::<BigInt, _>(receipt_id)
                .bind::<BigInt, _>((SOURCE_DIRTY_SEED_LIMIT + 1) as i64)
                .load::<InvalidationReceiptSeedRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        if persisted.len() != dirty_seeds.len() {
            return Ok(None);
        }
        for (persisted, current) in persisted.iter().zip(dirty_seeds) {
            let persisted_watermark = match (
                persisted.child_high_watermark_relation_revision,
                persisted.child_high_watermark_node_id.as_ref(),
            ) {
                (None, None) => None,
                (Some(relation_revision), Some(node_id)) => Some((relation_revision, node_id)),
                _ => return Ok(None),
            };
            let current_watermark = current
                .child_high_watermark
                .as_ref()
                .map(|watermark| (watermark.relation_revision, &watermark.node_id));
            if persisted.node_id != current.node_id || persisted_watermark != current_watermark {
                return Ok(None);
            }
        }
        Ok(Some(receipt.source_revision))
    }

    async fn commit_noop_invalidation_receipt(
        &self,
        target_invalidation_incarnation: &str,
        target_invalidation_version: i64,
        record: &GraphBranchRecord,
        dirty_seeds: &[DirtySeed],
        invalidation_kind: &str,
        relation_revision: i64,
    ) -> crate::Result<()> {
        if target_invalidation_incarnation.is_empty() || target_invalidation_version <= 0 {
            return Ok(());
        }
        let target_state_json = serde_json::to_string(&record.state).context(
            SerializeGraphSnapshotStoreValueSnafu {
                column: "state_json",
            },
        )?;
        let incarnation = target_invalidation_incarnation.to_owned();
        let invalidation_kind = invalidation_kind.to_owned();
        let branch = record.name.clone();
        let head_id = record.head_id.clone();
        let dirty_seeds = dirty_seeds.to_vec();
        let path = self.path.clone();
        self.database
            .with_write_connection("record source invalidation no-op", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let source_revision = current_source_revision(connection)?;
                        insert_source_invalidation_receipt(
                            connection,
                            &incarnation,
                            target_invalidation_version,
                            &branch,
                            source_revision,
                            relation_revision,
                            None,
                            &invalidation_kind,
                            &head_id,
                            &target_state_json,
                            &dirty_seeds,
                        )?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn refresh_branch(
        &mut self,
        store: &SqliteGraphStore,
        record: GraphBranchRecord,
    ) -> crate::Result<()> {
        let relation_revision = store
            .graph_relation_revision()
            .await
            .context(crate::error::StoreSnafu)?;
        self.refresh_branch_event(
            store,
            record,
            SourceInvalidation {
                incarnation: "",
                version: 0,
                relation_revision,
            },
            &BTreeSet::new(),
            true,
        )
        .await
    }

    async fn refresh_branch_event(
        &mut self,
        store: &SqliteGraphStore,
        record: GraphBranchRecord,
        invalidation: SourceInvalidation<'_>,
        dirty_node_ids: &BTreeSet<String>,
        force_full: bool,
    ) -> crate::Result<()> {
        let SourceInvalidation {
            incarnation: target_invalidation_incarnation,
            version: target_invalidation_version,
            relation_revision,
        } = invalidation;
        #[cfg(test)]
        {
            self.branch_refresh_count += 1;
            self.branch_refresh_history.push(record.name.clone());
            if self.fail_next_branch_refresh.as_deref() == Some(record.name.as_str()) {
                self.fail_next_branch_refresh = None;
                return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                    column: "branch_name",
                    value: record.name,
                }
                .fail();
            }
        }
        let dirty_seeds = self
            .freeze_dirty_seeds(store, dirty_node_ids, relation_revision)
            .await?;
        if let Some(active) = self.active_refresh_record(&record.name).await? {
            self.run_refresh(
                store,
                active,
                SourceInvalidation {
                    incarnation: "",
                    version: 0,
                    relation_revision,
                },
                &[],
                false,
            )
            .await?;
        }
        if force_full {
            if let Some(published_source_revision) = self
                .source_invalidation_receipt_exists(
                    target_invalidation_incarnation,
                    target_invalidation_version,
                    &record,
                    &[],
                    "full",
                    relation_revision,
                )
                .await?
            {
                tracing::info!(
                    source_mutation_revision = relation_revision,
                    source_branch_name = %record.name,
                    target_head_id = %record.head_id,
                    published_source_revision,
                    source_publication_outcome = "replayed",
                    source_invalidation_scope = "full",
                    "console graph source contribution published",
                );
                return Ok(());
            }
            loop {
                let published = self.published_branch(&record.name).await?;
                if published
                    .as_ref()
                    .is_none_or(|published| published.head_id != record.head_id)
                {
                    self.run_refresh(
                        store,
                        record.clone(),
                        SourceInvalidation {
                            incarnation: "",
                            version: 0,
                            relation_revision,
                        },
                        &[],
                        false,
                    )
                    .await?;
                    continue;
                }
                let published = published
                    .expect("same-head full reconciliation should have a published branch");
                if let Some(changed) = self
                    .changed_child_recheck(
                        store,
                        &record.name,
                        published.contribution_generation,
                        target_invalidation_incarnation,
                        target_invalidation_version,
                        relation_revision,
                    )
                    .await?
                {
                    let dirty_node_ids = BTreeSet::from([changed.node_id]);
                    let dirty_seeds = self
                        .freeze_dirty_seeds(store, &dirty_node_ids, relation_revision)
                        .await?;
                    self.run_refresh(store, record.clone(), invalidation, &dirty_seeds, false)
                        .await?;
                    continue;
                }
                let target_state_json = serde_json::to_string(&record.state).context(
                    SerializeGraphSnapshotStoreValueSnafu {
                        column: "state_json",
                    },
                )?;
                if published.state_json != target_state_json {
                    if self
                        .update_published_branch_state(
                            &record,
                            &published,
                            target_invalidation_incarnation,
                            target_invalidation_version,
                            "full",
                            relation_revision,
                        )
                        .await?
                    {
                        return Ok(());
                    }
                    continue;
                }
                self.commit_noop_invalidation_receipt(
                    target_invalidation_incarnation,
                    target_invalidation_version,
                    &record,
                    &[],
                    "full",
                    relation_revision,
                )
                .await?;
                return Ok(());
            }
        }
        self.run_refresh(store, record, invalidation, &dirty_seeds, force_full)
            .await
    }

    async fn run_refresh(
        &mut self,
        store: &SqliteGraphStore,
        record: GraphBranchRecord,
        invalidation: SourceInvalidation<'_>,
        dirty_seeds: &[DirtySeed],
        force_full: bool,
    ) -> crate::Result<()> {
        let SourceInvalidation {
            incarnation: target_invalidation_incarnation,
            version: target_invalidation_version,
            relation_revision,
        } = invalidation;
        let invalidation_kind = if force_full { "full" } else { "targeted" };
        if target_invalidation_version > 0
            && let Some(published_source_revision) = self
                .source_invalidation_receipt_exists(
                    target_invalidation_incarnation,
                    target_invalidation_version,
                    &record,
                    dirty_seeds,
                    invalidation_kind,
                    relation_revision,
                )
                .await?
        {
            tracing::info!(
                source_mutation_revision = relation_revision,
                source_branch_name = %record.name,
                target_head_id = %record.head_id,
                published_source_revision,
                source_publication_outcome = "replayed",
                source_invalidation_scope = invalidation_kind,
                "console graph source contribution published",
            );
            return Ok(());
        }
        let target_state_json = serde_json::to_string(&record.state).context(
            SerializeGraphSnapshotStoreValueSnafu {
                column: "state_json",
            },
        )?;
        let (previous, same_head, existing_refresh) = loop {
            let previous = self.published_branch(&record.name).await?;
            let same_head = previous
                .as_ref()
                .is_some_and(|published| published.head_id == record.head_id);
            let existing_refresh = self.matching_refresh(&record).await?;
            if existing_refresh.is_some() || !same_head || force_full {
                break (previous, same_head, existing_refresh);
            }
            let published = previous
                .as_ref()
                .expect("same-head refresh should have a previous branch");
            if dirty_seeds.is_empty() && published.state_json != target_state_json {
                if self
                    .update_published_branch_state(
                        &record,
                        published,
                        target_invalidation_incarnation,
                        target_invalidation_version,
                        "targeted",
                        relation_revision,
                    )
                    .await?
                {
                    return Ok(());
                }
                continue;
            }
            if !dirty_seeds.is_empty() {
                break (previous, same_head, existing_refresh);
            }
            self.commit_noop_invalidation_receipt(
                target_invalidation_incarnation,
                target_invalidation_version,
                &record,
                dirty_seeds,
                "targeted",
                relation_revision,
            )
            .await?;
            return Ok(());
        };
        let mut initial_work = dirty_seeds
            .iter()
            .map(|seed| {
                (
                    QueueItem {
                        node_id: seed.node_id.clone(),
                        traversal: TraversalKind::Graph,
                    },
                    true,
                )
            })
            .collect::<Vec<_>>();
        if !same_head || force_full {
            initial_work.push((
                QueueItem {
                    node_id: record.head_id.clone(),
                    traversal: TraversalKind::Graph,
                },
                force_full,
            ));
        }
        initial_work.sort_by(|left, right| left.0.cmp(&right.0));
        initial_work.dedup_by(|left, right| left.0 == right.0);
        let mut refresh = if let Some(refresh) = existing_refresh {
            refresh
        } else {
            self.begin_or_resume_refresh(RefreshStart {
                record: &record,
                base_generation: (same_head && !force_full).then(|| {
                    previous
                        .as_ref()
                        .expect("same-head refresh should have a previous branch")
                        .contribution_generation
                }),
                invalidation,
                target_invalidation_kind: invalidation_kind,
                previous: previous.as_ref(),
                initial_work: &initial_work,
                dirty_seeds,
            })
            .await?
        };
        let source_invalidation_scope = refresh.target_invalidation_kind.clone();
        tracing::info!(
            source_refresh_id = refresh.refresh_id,
            target_source_contribution_generation = refresh.target_contribution_generation,
            base_contribution_generation = refresh.base_generation.unwrap_or_default(),
            base_contribution_generation_available = refresh.base_generation.is_some(),
            source_refresh_lease_epoch = refresh.lease_epoch,
            source_mutation_revision = refresh.relation_revision,
            source_invalidation_scope = %source_invalidation_scope,
            source_branch_name = %record.name,
            target_head_id = %record.head_id,
            resumed_source_refresh = refresh.resumed,
            "console graph source refresh started",
        );

        tracing::info!(
            source_refresh_id = refresh.refresh_id,
            target_source_contribution_generation = refresh.target_contribution_generation,
            base_contribution_generation = refresh.base_generation.unwrap_or_default(),
            base_contribution_generation_available = refresh.base_generation.is_some(),
            source_refresh_lease_epoch = refresh.lease_epoch,
            source_mutation_revision = refresh.relation_revision,
            source_invalidation_scope = %source_invalidation_scope,
            source_branch_name = %record.name,
            target_head_id = %record.head_id,
            source_refresh_stage = "traverse_source_graph",
            "console graph source refresh stage started",
        );

        let mut refresh_session_batches = 0usize;
        let mut refresh_session_traversed_work_items = 0usize;
        let mut refresh_session_reused_work_items = 0usize;
        let mut refresh_session_child_pages = 0usize;
        let mut refresh_session_parent_pages = 0usize;
        loop {
            let batch = self.load_refresh_batch(refresh.refresh_id).await?;
            if batch.is_empty() {
                break;
            }
            if refresh.base_generation.is_none()
                && previous.as_ref().is_some_and(|previous| {
                    batch.iter().any(|item| {
                        item.item.traversal == TraversalKind::Graph
                            && item.item.node_id == previous.head_id
                    })
                })
            {
                let previous = previous
                    .as_ref()
                    .expect("previous branch should exist when its head is reached");
                self.set_refresh_base(&refresh, previous.contribution_generation)
                    .await?;
                refresh.base_generation = Some(previous.contribution_generation);
                refresh.target_contribution_generation = previous.contribution_generation;
                continue;
            }

            let reusable_work = if let Some(base_generation) = refresh.base_generation {
                self.previous_completed_work_keys(base_generation, &batch)
                    .await?
            } else {
                HashSet::new()
            };
            let reused = batch
                .iter()
                .filter(|work| {
                    !work.node_committed
                        && !work.force_child_scan
                        && reusable_work.contains(&(
                            work.item.node_id.clone(),
                            work.item.traversal.as_str().to_owned(),
                        ))
                })
                .map(|work| work.item.clone())
                .collect::<Vec<_>>();
            self.commit_reused_work(&refresh, &reused).await?;

            let traversal_batch = batch
                .iter()
                .filter(|work| {
                    !work.node_committed
                        && (work.force_child_scan
                            || !reusable_work.contains(&(
                                work.item.node_id.clone(),
                                work.item.traversal.as_str().to_owned(),
                            )))
                })
                .map(|work| work.item.clone())
                .collect::<Vec<_>>();
            let ids = traversal_batch
                .iter()
                .map(|item| item.node_id.clone())
                .collect::<Vec<_>>();
            let nodes = self.load_nodes(store, &ids).await?;
            let mut child_rechecks = Vec::new();
            for item in &traversal_batch {
                let node = required_node(&nodes, "node_id", &item.node_id)?;
                if needs_children(item.traversal, node) {
                    child_rechecks.push(item.clone());
                }
            }
            self.commit_refresh_batch(&refresh, &record.name, &traversal_batch, &child_rechecks)
                .await?;

            let mut enqueued_parents = 0usize;
            let mut enqueued_children = 0usize;
            for work in batch
                .iter()
                .filter(|work| work.node_committed && !work.parent_traversal_complete)
            {
                let node = self
                    .load_nodes(store, std::slice::from_ref(&work.item.node_id))
                    .await?;
                let node = required_node(&node, "node_id", &work.item.node_id)?;
                enqueued_parents += self
                    .process_parent_page(&refresh, &record.name, work, node)
                    .await?;
                refresh_session_parent_pages = refresh_session_parent_pages.saturating_add(1);
            }
            for work in batch.iter().filter(|work| {
                work.node_committed && work.parent_traversal_complete && work.child_scan_required
            }) {
                enqueued_children += self
                    .process_child_page(store, &refresh, &record.name, work)
                    .await?;
                refresh_session_child_pages = refresh_session_child_pages.saturating_add(1);
            }
            refresh_session_batches = refresh_session_batches.saturating_add(1);
            refresh_session_traversed_work_items =
                refresh_session_traversed_work_items.saturating_add(traversal_batch.len());
            refresh_session_reused_work_items =
                refresh_session_reused_work_items.saturating_add(reused.len());
            #[cfg(test)]
            {
                self.traversed_node_count += traversal_batch.len();
            }
            tracing::debug!(
                source_refresh_id = refresh.refresh_id,
                target_source_contribution_generation = refresh.target_contribution_generation,
                base_contribution_generation = refresh.base_generation.unwrap_or_default(),
                base_contribution_generation_available = refresh.base_generation.is_some(),
                source_refresh_lease_epoch = refresh.lease_epoch,
                source_mutation_revision = refresh.relation_revision,
                source_invalidation_scope = %source_invalidation_scope,
                source_branch_name = %record.name,
                target_head_id = %record.head_id,
                source_refresh_stage = "traverse_source_graph",
                batch_loaded_work_item_count = batch.len(),
                batch_traversal_skipped_base_completed_work_item_count = reused.len(),
                batch_enqueued_parent_work_item_count = enqueued_parents,
                batch_enqueued_child_work_item_count = enqueued_children,
                "console graph source refresh work advanced",
            );
            if refresh_session_batches.is_multiple_of(128) {
                tracing::info!(
                    source_refresh_id = refresh.refresh_id,
                    target_source_contribution_generation = refresh.target_contribution_generation,
                    base_contribution_generation = refresh.base_generation.unwrap_or_default(),
                    base_contribution_generation_available = refresh.base_generation.is_some(),
                    source_refresh_lease_epoch = refresh.lease_epoch,
                    source_mutation_revision = refresh.relation_revision,
                    source_invalidation_scope = %source_invalidation_scope,
                    source_branch_name = %record.name,
                    target_head_id = %record.head_id,
                    source_refresh_stage = "traverse_source_graph",
                    refresh_session_committed_batch_count = refresh_session_batches,
                    refresh_session_traversed_work_item_count =
                        refresh_session_traversed_work_items,
                    refresh_session_traversal_skipped_base_completed_work_item_count =
                        refresh_session_reused_work_items,
                    refresh_session_committed_child_page_count = refresh_session_child_pages,
                    refresh_session_committed_parent_page_count = refresh_session_parent_pages,
                    "console graph source refresh progress",
                );
            }
            self.renew_source_sweep_lease(
                &refresh.target_invalidation_incarnation,
                refresh.target_invalidation_version,
            )
            .await?;
            tokio::task::yield_now().await;
        }

        let published_source_revision = self.commit_branch(&refresh, record.clone()).await?;
        tracing::info!(
            source_refresh_id = refresh.refresh_id,
            target_source_contribution_generation = refresh.target_contribution_generation,
            base_contribution_generation = refresh.base_generation.unwrap_or_default(),
            base_contribution_generation_available = refresh.base_generation.is_some(),
            source_refresh_lease_epoch = refresh.lease_epoch,
            source_mutation_revision = refresh.relation_revision,
            source_invalidation_scope = %source_invalidation_scope,
            source_branch_name = %record.name,
            target_head_id = %record.head_id,
            source_refresh_stage = "complete",
            refresh_session_committed_batch_count = refresh_session_batches,
            refresh_session_traversed_work_item_count = refresh_session_traversed_work_items,
            refresh_session_traversal_skipped_base_completed_work_item_count =
                refresh_session_reused_work_items,
            refresh_session_committed_child_page_count = refresh_session_child_pages,
            refresh_session_committed_parent_page_count = refresh_session_parent_pages,
            published_source_revision,
            source_publication_outcome = "committed",
            source_contribution_build_kind = if refresh.base_generation.is_some() {
                "append"
            } else {
                "full"
            },
            "console graph source contribution published",
        );
        self.cleanup_stale_refreshes_bounded().await
    }

    async fn active_refresh_record(
        &self,
        branch: &str,
    ) -> crate::Result<Option<GraphBranchRecord>> {
        let branch_name = branch.to_owned();
        let path = self.path.clone();
        let row = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT target_head_id, target_state_json \
                     FROM console_graph_source_refresh_runs \
                     WHERE branch_name = ? AND status = 'building' \
                     ORDER BY refresh_id LIMIT 1",
                )
                .bind::<Text, _>(branch_name)
                .get_result::<ActiveRefreshRecordRow>(connection)
                .optional()
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        row.map(|row| {
            let state = serde_json::from_str::<SessionState>(&row.target_state_json).context(
                ParseGraphSnapshotStoreValueSnafu {
                    column: "target_state_json",
                },
            )?;
            Ok(GraphBranchRecord {
                name: branch.to_owned(),
                head_id: row.target_head_id,
                state,
            })
        })
        .transpose()
    }

    async fn begin_or_resume_refresh(&self, start: RefreshStart<'_>) -> crate::Result<RefreshRun> {
        let RefreshStart {
            record,
            base_generation,
            invalidation,
            target_invalidation_kind,
            previous,
            initial_work,
            dirty_seeds,
        } = start;
        let SourceInvalidation {
            incarnation: target_invalidation_incarnation,
            version: target_invalidation_version,
            relation_revision,
        } = invalidation;
        let state_json = serde_json::to_string(&record.state).context(
            SerializeGraphSnapshotStoreValueSnafu {
                column: "state_json",
            },
        )?;
        let branch = record.name.clone();
        let head_id = record.head_id.clone();
        let path = self.path.clone();
        let existing = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT refresh_id, target_contribution_generation, base_generation, \
                            lease_epoch, lease_expires_at_ms, target_invalidation_version, \
                            target_invalidation_incarnation, target_invalidation_kind, \
                            relation_revision \
                     FROM console_graph_source_refresh_runs \
                     WHERE branch_name = ? AND target_head_id = ? \
                       AND status = 'building' \
                     ORDER BY refresh_id DESC LIMIT 1",
                )
                .bind::<Text, _>(branch)
                .bind::<Text, _>(head_id)
                .get_result::<RefreshRunRow>(connection)
                .optional()
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        if let Some(existing) = existing {
            return self.claim_refresh(existing, state_json).await;
        }

        let refresh_id = self.allocate_generation().await?;
        let target_contribution_generation = base_generation.unwrap_or(refresh_id);
        let refresh_kind = if base_generation.is_some() {
            "append"
        } else {
            "full"
        };
        let owner_id = self.owner_id.clone();
        let target_invalidation_incarnation = target_invalidation_incarnation.to_owned();
        let target_invalidation_kind = target_invalidation_kind.to_owned();
        let insert_target_invalidation_incarnation = target_invalidation_incarnation.clone();
        let insert_target_invalidation_kind = target_invalidation_kind.clone();
        let expected_branch_head_id = previous.map(|branch| branch.head_id.clone());
        let expected_branch_state_json = previous.map(|branch| branch.state_json.clone());
        let expected_branch_source_revision = previous.map(|branch| branch.source_revision);
        let expected_branch_contribution_generation =
            previous.map(|branch| branch.contribution_generation);
        let expected_branch_absent = previous.is_none();
        let initial_work = initial_work.to_vec();
        let dirty_seeds = dirty_seeds.to_vec();
        let branch = record.name.clone();
        let head_id = record.head_id.clone();
        let insert_owner_id = owner_id.clone();
        let path = self.path.clone();
        let inserted = self.database
            .with_write_connection("begin source refresh", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let inserted = diesel::sql_query(
                            "INSERT OR IGNORE INTO console_graph_source_refresh_runs ( \
                                 refresh_id, branch_name, target_head_id, target_state_json, \
                                 base_generation, target_contribution_generation, refresh_kind, \
                                 status, owner_id, lease_epoch, lease_expires_at_ms, \
                                 target_invalidation_version, \
                                 target_invalidation_incarnation, target_invalidation_kind, \
                                 relation_revision, \
                                 expected_branch_head_id, expected_branch_state_json, \
                                 expected_branch_contribution_generation, \
                                 expected_branch_source_revision, expected_branch_absent \
                             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'building', ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        )
                        .bind::<BigInt, _>(refresh_id)
                        .bind::<Text, _>(&branch)
                        .bind::<Text, _>(&head_id)
                        .bind::<Text, _>(&state_json)
                        .bind::<Nullable<BigInt>, _>(base_generation)
                        .bind::<BigInt, _>(target_contribution_generation)
                        .bind::<Text, _>(refresh_kind)
                        .bind::<Text, _>(&insert_owner_id)
                        .bind::<BigInt, _>(source_lease_deadline_ms())
                        .bind::<BigInt, _>(target_invalidation_version)
                        .bind::<Text, _>(&insert_target_invalidation_incarnation)
                        .bind::<Text, _>(&insert_target_invalidation_kind)
                        .bind::<BigInt, _>(relation_revision)
                        .bind::<Nullable<Text>, _>(expected_branch_head_id.as_deref())
                        .bind::<Nullable<Text>, _>(expected_branch_state_json.as_deref())
                        .bind::<Nullable<BigInt>, _>(expected_branch_contribution_generation)
                        .bind::<Nullable<BigInt>, _>(expected_branch_source_revision)
                        .bind::<Integer, _>(i32::from(expected_branch_absent))
                        .execute(connection)?;
                        if inserted != 1 {
                            return Ok(false);
                        }
                        for (item, force_child_scan) in &initial_work {
                            let dirty_seed = dirty_seeds
                                .iter()
                                .find(|seed| seed.node_id == item.node_id);
                            diesel::insert_into(console_graph_source_refresh_queue::table)
                                .values((
                                    console_graph_source_refresh_queue::refresh_id.eq(refresh_id),
                                    console_graph_source_refresh_queue::branch_name.eq(&branch),
                                    console_graph_source_refresh_queue::node_id.eq(&item.node_id),
                                    console_graph_source_refresh_queue::traversal_kind
                                        .eq(item.traversal.as_str()),
                                    console_graph_source_refresh_queue::processed.eq(0),
                                    console_graph_source_refresh_queue::force_child_scan
                                        .eq(i32::from(*force_child_scan)),
                                    console_graph_source_refresh_queue::child_high_watermark_frozen
                                        .eq(i32::from(dirty_seed.is_some())),
                                    console_graph_source_refresh_queue::child_high_watermark_relation_revision
                                        .eq(dirty_seed.and_then(|seed| {
                                            seed.child_high_watermark
                                                .as_ref()
                                                .map(|watermark| watermark.relation_revision)
                                        })),
                                    console_graph_source_refresh_queue::child_high_watermark_node_id
                                        .eq(dirty_seed.and_then(|seed| {
                                            seed.child_high_watermark
                                                .as_ref()
                                                .map(|watermark| watermark.node_id.as_str())
                                        })),
                                ))
                                .on_conflict((
                                    console_graph_source_refresh_queue::refresh_id,
                                    console_graph_source_refresh_queue::node_id,
                                    console_graph_source_refresh_queue::traversal_kind,
                                ))
                                .do_nothing()
                                .execute(connection)?;
                        }
                        for seed in &dirty_seeds {
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_refresh_dirty_seeds ( \
                                     refresh_id, node_id, child_high_watermark_relation_revision, \
                                     child_high_watermark_node_id \
                                 ) VALUES (?, ?, ?, ?)",
                            )
                            .bind::<BigInt, _>(refresh_id)
                            .bind::<Text, _>(&seed.node_id)
                            .bind::<Nullable<BigInt>, _>(
                                seed.child_high_watermark
                                    .as_ref()
                                    .map(|watermark| watermark.relation_revision),
                            )
                            .bind::<Nullable<Text>, _>(
                                seed.child_high_watermark
                                    .as_ref()
                                    .map(|watermark| watermark.node_id.as_str()),
                            )
                            .execute(connection)?;
                        }
                        Ok(true)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        if !inserted {
            return crate::error::SourceRefreshBusySnafu {
                resource: format!("source branch {}", record.name),
            }
            .fail();
        }
        Ok(RefreshRun {
            refresh_id,
            target_contribution_generation,
            base_generation,
            owner_id,
            lease_epoch: 1,
            target_invalidation_version,
            target_invalidation_incarnation,
            target_invalidation_kind,
            relation_revision,
            resumed: false,
        })
    }

    async fn matching_refresh(
        &self,
        record: &GraphBranchRecord,
    ) -> crate::Result<Option<RefreshRun>> {
        let state_json = serde_json::to_string(&record.state).context(
            SerializeGraphSnapshotStoreValueSnafu {
                column: "state_json",
            },
        )?;
        let branch = record.name.clone();
        let head_id = record.head_id.clone();
        let path = self.path.clone();
        let existing = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT refresh_id, target_contribution_generation, base_generation, \
                            lease_epoch, lease_expires_at_ms, target_invalidation_version, \
                            target_invalidation_incarnation, target_invalidation_kind, \
                            relation_revision \
                     FROM console_graph_source_refresh_runs \
                     WHERE branch_name = ? AND target_head_id = ? \
                       AND status = 'building' \
                     ORDER BY refresh_id DESC LIMIT 1",
                )
                .bind::<Text, _>(branch)
                .bind::<Text, _>(head_id)
                .get_result::<RefreshRunRow>(connection)
                .optional()
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        if let Some(existing) = existing {
            Ok(Some(self.claim_refresh(existing, state_json).await?))
        } else {
            Ok(None)
        }
    }

    async fn claim_refresh(
        &self,
        existing: RefreshRunRow,
        state_json: String,
    ) -> crate::Result<RefreshRun> {
        let owner_id = self.owner_id.clone();
        let update_owner_id = owner_id.clone();
        let lease_epoch = existing.lease_epoch.saturating_add(1);
        let refresh_id = existing.refresh_id;
        let now_ms = source_time_ms();
        let lease_expires_at_ms = source_lease_deadline_ms();
        let path = self.path.clone();
        self.database
            .with_write_connection("update source refresh target state", move |connection| {
                let updated = diesel::sql_query(
                    "UPDATE console_graph_source_refresh_runs \
                     SET target_state_json = ?, owner_id = ?, lease_epoch = ?, \
                         lease_expires_at_ms = ?, \
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE refresh_id = ? AND lease_epoch = ? AND status = 'building' \
                       AND (owner_id = '' OR owner_id = ? OR lease_expires_at_ms <= ?)",
                )
                .bind::<Text, _>(state_json)
                .bind::<Text, _>(&update_owner_id)
                .bind::<BigInt, _>(lease_epoch)
                .bind::<BigInt, _>(lease_expires_at_ms)
                .bind::<BigInt, _>(refresh_id)
                .bind::<BigInt, _>(existing.lease_epoch)
                .bind::<Text, _>(&update_owner_id)
                .bind::<BigInt, _>(now_ms)
                .execute(connection)
                .context(QueryGraphSnapshotStoreSnafu { path: path.clone() })?;
                if updated != 1 {
                    return crate::error::SourceRefreshBusySnafu {
                        resource: format!("source refresh {refresh_id}"),
                    }
                    .fail();
                }
                Ok(RefreshRun {
                    refresh_id,
                    target_contribution_generation: existing.target_contribution_generation,
                    base_generation: existing.base_generation,
                    owner_id,
                    lease_epoch,
                    target_invalidation_version: existing.target_invalidation_version,
                    target_invalidation_incarnation: existing.target_invalidation_incarnation,
                    target_invalidation_kind: existing.target_invalidation_kind,
                    relation_revision: existing.relation_revision,
                    resumed: true,
                })
            })
            .await
    }

    async fn set_refresh_base(
        &self,
        refresh: &RefreshRun,
        base_generation: i64,
    ) -> crate::Result<()> {
        let refresh_id = refresh.refresh_id;
        let owner_id = refresh.owner_id.clone();
        let lease_epoch = refresh.lease_epoch;
        let path = self.path.clone();
        self.database
            .with_write_connection("set source refresh base", move |connection| {
                let updated = diesel::sql_query(
                    "UPDATE console_graph_source_refresh_runs \
                     SET base_generation = ?, target_contribution_generation = ?, \
                         refresh_kind = 'append', \
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                     WHERE refresh_id = ? AND owner_id = ? AND lease_epoch = ? \
                       AND status = 'building' \
                       AND (base_generation IS NULL OR base_generation = ?)",
                )
                .bind::<BigInt, _>(base_generation)
                .bind::<BigInt, _>(base_generation)
                .bind::<BigInt, _>(refresh_id)
                .bind::<Text, _>(&owner_id)
                .bind::<BigInt, _>(lease_epoch)
                .bind::<BigInt, _>(base_generation)
                .execute(connection)
                .context(QueryGraphSnapshotStoreSnafu { path: path.clone() })?;
                if updated != 1 {
                    return Err(diesel::result::Error::NotFound)
                        .context(QueryGraphSnapshotStoreSnafu { path });
                }
                Ok(())
            })
            .await
    }

    async fn previous_completed_work_keys(
        &self,
        previous_generation: i64,
        batch: &[RefreshWorkItem],
    ) -> crate::Result<HashSet<(String, String)>> {
        let node_ids = batch
            .iter()
            .map(|work| work.item.node_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        self.completed_work_keys(previous_generation, node_ids)
            .await
    }

    async fn completed_work_keys(
        &self,
        generation: i64,
        node_ids: Vec<String>,
    ) -> crate::Result<HashSet<(String, String)>> {
        if node_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let node_ids_json = serde_json::to_string(&node_ids)
            .context(SerializeGraphSnapshotStoreValueSnafu { column: "node_ids" })?;
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT node_id, traversal_kind \
                     FROM console_graph_source_current_completed_work \
                     WHERE contribution_generation = ? \
                       AND node_id IN (SELECT value FROM json_each(?))",
                )
                .bind::<BigInt, _>(generation)
                .bind::<Text, _>(node_ids_json)
                .load::<ChildRecheckRow>(connection)
                .map(|rows| {
                    rows.into_iter()
                        .map(|row| (row.node_id, row.traversal_kind))
                        .collect()
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn load_nodes(
        &self,
        store: &SqliteGraphStore,
        ids: &[String],
    ) -> crate::Result<HashMap<String, Node>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let unique_ids = ids.iter().cloned().collect::<BTreeSet<_>>();
        let query_ids = unique_ids.iter().cloned().collect::<Vec<_>>();
        let mut nodes = HashMap::new();
        for batch in query_ids.chunks(SOURCE_CACHE_BATCH_SIZE) {
            let batch = batch.to_vec();
            let path = self.path.clone();
            let cached = self
                .database
                .with_connection(move |connection| {
                    console_graph_source_nodes::table
                        .filter(console_graph_source_nodes::node_id.eq_any(batch))
                        .select(console_graph_source_nodes::node_json)
                        .load::<String>(connection)
                        .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
            for value in cached {
                let node = serde_json::from_str::<Node>(&value).context(
                    ParseGraphSnapshotStoreValueSnafu {
                        column: "node_json",
                    },
                )?;
                nodes.insert(node.id.clone(), node);
            }
        }
        let missing = unique_ids
            .into_iter()
            .filter(|node_id| !nodes.contains_key(node_id))
            .collect::<Vec<_>>();
        for batch in missing.chunks(SOURCE_CACHE_BATCH_SIZE) {
            let loaded = store
                .graph_nodes_by_ids(batch)
                .await
                .context(crate::error::StoreSnafu)?;
            self.persist_nodes(&loaded).await?;
            nodes.extend(loaded.into_iter().map(|node| (node.id.clone(), node)));
            tokio::task::yield_now().await;
        }
        for node in nodes.values() {
            self.ensure_source_relations_ingested(node).await?;
        }
        Ok(nodes)
    }

    async fn changed_child_recheck(
        &mut self,
        store: &SqliteGraphStore,
        branch: &str,
        generation: i64,
        target_invalidation_incarnation: &str,
        target_invalidation_version: i64,
        relation_revision: i64,
    ) -> crate::Result<Option<QueueItem>> {
        let mut state = self
            .source_sweep_recheck_state(
                target_invalidation_incarnation,
                target_invalidation_version,
            )
            .await?;
        if let Some(active_item) = state.active_item.clone()
            && self
                .child_recheck_has_changes(
                    store,
                    generation,
                    target_invalidation_incarnation,
                    target_invalidation_version,
                    relation_revision,
                    &mut state,
                )
                .await?
        {
            return Ok(Some(active_item));
        }
        loop {
            let page = self
                .load_child_recheck_page(branch, generation, state.raw_cursor.as_ref())
                .await?;
            if page.rows.is_empty() {
                return Ok(None);
            }
            for (raw_cursor, eligible) in &page.rows {
                if !eligible
                    || state
                        .completed_item
                        .as_ref()
                        .is_some_and(|item| item == &raw_cursor.item)
                {
                    continue;
                }
                let item = raw_cursor.item.clone();
                let mut active = state.clone();
                active.raw_cursor = Some(raw_cursor.clone());
                active.active_item = Some(item.clone());
                active.child_cursor = None;
                self.checkpoint_source_sweep_recheck_state(
                    target_invalidation_incarnation,
                    target_invalidation_version,
                    &mut state,
                    active,
                )
                .await?;
                if self
                    .child_recheck_has_changes(
                        store,
                        generation,
                        target_invalidation_incarnation,
                        target_invalidation_version,
                        relation_revision,
                        &mut state,
                    )
                    .await?
                {
                    return Ok(Some(item));
                }
            }
            let raw_cursor = page
                .rows
                .last()
                .map(|(cursor, _)| cursor.clone())
                .expect("non-empty child recheck page should have a cursor");
            if state.raw_cursor.as_ref() != Some(&raw_cursor) {
                let mut advanced = state.clone();
                advanced.raw_cursor = Some(raw_cursor);
                self.checkpoint_source_sweep_recheck_state(
                    target_invalidation_incarnation,
                    target_invalidation_version,
                    &mut state,
                    advanced,
                )
                .await?;
            }
            if page.complete {
                return Ok(None);
            }
            tokio::task::yield_now().await;
        }
    }

    async fn load_child_recheck_page(
        &self,
        branch: &str,
        generation: i64,
        after: Option<&ChildRecheckRawCursor>,
    ) -> crate::Result<ChildRecheckRawPage> {
        let branch = branch.to_owned();
        let after = after.map(|cursor| {
            (
                cursor.item.node_id.clone(),
                cursor.item.traversal.as_str().to_owned(),
                cursor.refresh_id,
            )
        });
        let path = self.path.clone();
        let rows = self
            .database
            .with_connection(move |connection| {
                let rows = if let Some((node_id, traversal_kind, refresh_id)) = after {
                    diesel::sql_query(
                        "SELECT recheck.node_id, recheck.traversal_kind, \
                                recheck.contribution_generation AS refresh_id, \
                                CASE WHEN EXISTS ( \
                                    SELECT 1 \
                                    FROM console_graph_source_refresh_runs AS refresh \
                                    INNER JOIN console_graph_source_branches AS branch \
                                        ON branch.name = refresh.branch_name \
                                       AND branch.contribution_generation = \
                                           refresh.target_contribution_generation \
                                    INNER JOIN console_graph_source_branch_publications \
                                               AS publication \
                                        ON publication.branch_name = branch.name \
                                       AND publication.target_contribution_generation = \
                                           branch.contribution_generation \
                                    WHERE refresh.refresh_id = \
                                              recheck.contribution_generation \
                                      AND refresh.branch_name = recheck.branch_name \
                                      AND refresh.target_contribution_generation = ? \
                                      AND refresh.status = 'published' \
                                      AND refresh.published_source_revision <= \
                                          publication.source_revision \
                                ) THEN 1 ELSE 0 END AS eligible \
                         FROM console_graph_source_child_rechecks AS recheck \
                              INDEXED BY console_graph_source_child_rechecks_branch_order_idx \
                         WHERE recheck.branch_name = ? \
                           AND (recheck.node_id, recheck.traversal_kind, \
                                recheck.contribution_generation) > (?, ?, ?) \
                         ORDER BY recheck.node_id, recheck.traversal_kind, \
                                  recheck.contribution_generation \
                         LIMIT ?",
                    )
                    .bind::<BigInt, _>(generation)
                    .bind::<Text, _>(&branch)
                    .bind::<Text, _>(&node_id)
                    .bind::<Text, _>(&traversal_kind)
                    .bind::<BigInt, _>(refresh_id)
                    .bind::<BigInt, _>(SOURCE_CHILD_RECHECK_PAGE_SIZE as i64)
                    .load::<ChildRecheckRawRow>(connection)
                } else {
                    diesel::sql_query(
                        "SELECT recheck.node_id, recheck.traversal_kind, \
                                recheck.contribution_generation AS refresh_id, \
                                CASE WHEN EXISTS ( \
                                    SELECT 1 \
                                    FROM console_graph_source_refresh_runs AS refresh \
                                    INNER JOIN console_graph_source_branches AS branch \
                                        ON branch.name = refresh.branch_name \
                                       AND branch.contribution_generation = \
                                           refresh.target_contribution_generation \
                                    INNER JOIN console_graph_source_branch_publications \
                                               AS publication \
                                        ON publication.branch_name = branch.name \
                                       AND publication.target_contribution_generation = \
                                           branch.contribution_generation \
                                    WHERE refresh.refresh_id = \
                                              recheck.contribution_generation \
                                      AND refresh.branch_name = recheck.branch_name \
                                      AND refresh.target_contribution_generation = ? \
                                      AND refresh.status = 'published' \
                                      AND refresh.published_source_revision <= \
                                          publication.source_revision \
                                ) THEN 1 ELSE 0 END AS eligible \
                         FROM console_graph_source_child_rechecks AS recheck \
                              INDEXED BY console_graph_source_child_rechecks_branch_order_idx \
                         WHERE recheck.branch_name = ? \
                         ORDER BY recheck.node_id, recheck.traversal_kind, \
                                  recheck.contribution_generation \
                         LIMIT ?",
                    )
                    .bind::<BigInt, _>(generation)
                    .bind::<Text, _>(&branch)
                    .bind::<BigInt, _>(SOURCE_CHILD_RECHECK_PAGE_SIZE as i64)
                    .load::<ChildRecheckRawRow>(connection)
                };
                rows.context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        let complete = rows.len() < SOURCE_CHILD_RECHECK_PAGE_SIZE;
        let rows = rows
            .into_iter()
            .map(|row| {
                Ok((
                    ChildRecheckRawCursor {
                        item: QueueItem {
                            node_id: row.node_id,
                            traversal: TraversalKind::parse(&row.traversal_kind)?,
                        },
                        refresh_id: row.refresh_id,
                    },
                    row.eligible != 0,
                ))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(ChildRecheckRawPage { rows, complete })
    }

    async fn child_recheck_has_changes(
        &mut self,
        store: &SqliteGraphStore,
        generation: i64,
        target_invalidation_incarnation: &str,
        target_invalidation_version: i64,
        relation_revision: i64,
        state: &mut SourceSweepRecheckState,
    ) -> crate::Result<bool> {
        let item = state.active_item.clone().with_context(|| {
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "source_sweep_active_recheck_item",
                value: "missing".to_owned(),
            }
        })?;
        loop {
            #[cfg(test)]
            self.child_recheck_page_cursors
                .push(state.child_cursor.clone());
            let (pending, next_cursor, complete) = self
                .load_child_traversal_page(
                    store,
                    &item,
                    state.child_cursor.as_ref(),
                    None,
                    relation_revision,
                )
                .await?;
            let node_ids = pending
                .iter()
                .map(|(node_id, _)| node_id.clone())
                .collect::<Vec<_>>();
            let completed = self.completed_work_keys(generation, node_ids).await?;
            if pending.iter().any(|(node_id, traversal)| {
                !completed.contains(&(node_id.clone(), traversal.as_str().to_owned()))
            }) {
                return Ok(true);
            }
            if complete {
                let mut completed = state.clone();
                completed.completed_item = Some(item.clone());
                completed.active_item = None;
                completed.child_cursor = None;
                self.checkpoint_source_sweep_recheck_state(
                    target_invalidation_incarnation,
                    target_invalidation_version,
                    state,
                    completed,
                )
                .await?;
                return Ok(false);
            }
            let mut checkpoint = state.clone();
            checkpoint.child_cursor =
                Some(next_cursor.expect("incomplete child recheck page should provide a cursor"));
            self.checkpoint_source_sweep_recheck_state(
                target_invalidation_incarnation,
                target_invalidation_version,
                state,
                checkpoint,
            )
            .await?;
            #[cfg(test)]
            if std::mem::take(&mut self.fail_next_child_recheck_page_checkpoint) {
                return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                    column: "source_sweep_recheck_child_cursor",
                    value: "injected interruption".to_owned(),
                }
                .fail();
            }
            tokio::task::yield_now().await;
        }
    }

    async fn load_child_traversal_page(
        &self,
        store: &SqliteGraphStore,
        item: &QueueItem,
        cursor: Option<&GraphChildPageCursor>,
        through: Option<&GraphChildPageCursor>,
        relation_revision: i64,
    ) -> crate::Result<(
        Vec<(String, TraversalKind)>,
        Option<GraphChildPageCursor>,
        bool,
    )> {
        let page_size = NonZeroUsize::new(SOURCE_CACHE_BATCH_SIZE)
            .expect("source cache batch size should be non-zero");
        let page = if let Some(through) = through {
            store
                .graph_child_ids_page_through_at_revision(
                    &item.node_id,
                    cursor,
                    through,
                    relation_revision,
                    page_size,
                )
                .await
        } else {
            store
                .graph_child_ids_page_at_revision(
                    &item.node_id,
                    cursor,
                    relation_revision,
                    page_size,
                )
                .await
        }
        .context(crate::error::StoreSnafu)?;
        let child_nodes = self.load_nodes(store, &page.child_ids).await?;
        let traversals = child_traversals_for_page(item, &page.child_ids, &child_nodes)?;
        Ok((traversals, page.next_cursor, page.complete))
    }

    async fn persist_nodes(&self, nodes: &[Node]) -> crate::Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        for batch in nodes.chunks(SOURCE_CACHE_BATCH_SIZE) {
            let batch = batch
                .iter()
                .map(PersistedSourceNode::try_from)
                .collect::<crate::Result<Vec<_>>>()?;
            let path = self.path.clone();
            self.database
                .with_write_connection("persist source node batch", move |connection| {
                    connection
                        .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                            for node in &batch {
                                diesel::insert_into(console_graph_source_nodes::table)
                                    .values((
                                        console_graph_source_nodes::node_id.eq(&node.node_id),
                                        console_graph_source_nodes::parent_id.eq(&node.parent_id),
                                        console_graph_source_nodes::node_json.eq(&node.node_json),
                                        console_graph_source_nodes::relation_cursor_offset.eq(0),
                                        console_graph_source_nodes::relation_ingest_complete.eq(0),
                                    ))
                                    .on_conflict(console_graph_source_nodes::node_id)
                                    .do_nothing()
                                    .execute(connection)?;
                            }
                            Ok(())
                        })
                        .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
        }
        for node in nodes {
            self.ensure_source_relations_ingested(node).await?;
        }
        Ok(())
    }

    async fn ensure_source_relations_ingested(&self, node: &Node) -> crate::Result<()> {
        loop {
            let node_id = node.id.clone();
            let path = self.path.clone();
            let state = self
                .database
                .with_connection(move |connection| {
                    diesel::sql_query(
                        "SELECT relation_cursor_offset, relation_ingest_complete \
                         FROM console_graph_source_nodes WHERE node_id = ?",
                    )
                    .bind::<Text, _>(node_id)
                    .get_result::<RelationIngestStateRow>(connection)
                    .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
            if state.relation_ingest_complete != 0 {
                return Ok(());
            }
            let (parent_ids, next_offset, complete) =
                graph_parent_id_page(node, state.relation_cursor_offset);
            let expected_offset = state.relation_cursor_offset;
            let node_id = node.id.clone();
            let path = self.path.clone();
            self.database
                .with_write_connection("commit source relation ingest page", move |connection| {
                    connection
                        .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                            let advanced = diesel::sql_query(
                                "UPDATE console_graph_source_nodes \
                                 SET relation_cursor_offset = ?, relation_ingest_complete = ? \
                                 WHERE node_id = ? AND relation_cursor_offset = ? \
                                   AND relation_ingest_complete = 0",
                            )
                            .bind::<BigInt, _>(next_offset)
                            .bind::<Integer, _>(i32::from(complete))
                            .bind::<Text, _>(&node_id)
                            .bind::<BigInt, _>(expected_offset)
                            .execute(connection)?;
                            if advanced != 1 {
                                return Ok(());
                            }
                            for parent_id in &parent_ids {
                                diesel::insert_into(console_graph_source_node_relations::table)
                                    .values((
                                        console_graph_source_node_relations::parent_id
                                            .eq(parent_id),
                                        console_graph_source_node_relations::child_id.eq(&node_id),
                                    ))
                                    .on_conflict((
                                        console_graph_source_node_relations::parent_id,
                                        console_graph_source_node_relations::child_id,
                                    ))
                                    .do_nothing()
                                    .execute(connection)?;
                            }
                            Ok(())
                        })
                        .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
            tokio::task::yield_now().await;
        }
    }

    async fn allocate_generation(&self) -> crate::Result<i64> {
        let path = self.path.clone();
        self.database
            .with_write_connection("allocate source refresh generation", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let generation = console_graph_source_state::table
                            .filter(console_graph_source_state::id.eq(1))
                            .select(console_graph_source_state::next_generation)
                            .first::<i64>(connection)?;
                        diesel::update(
                            console_graph_source_state::table
                                .filter(console_graph_source_state::id.eq(1)),
                        )
                        .set(
                            console_graph_source_state::next_generation
                                .eq(generation.saturating_add(1)),
                        )
                        .execute(connection)?;
                        Ok(generation)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn seed_refresh_queue(
        &self,
        refresh: &RefreshRun,
        branch: &str,
        item: &QueueItem,
        force_child_scan: bool,
    ) -> crate::Result<()> {
        let refresh_id = refresh.refresh_id;
        let owner_id = refresh.owner_id.clone();
        let lease_epoch = refresh.lease_epoch;
        let branch = branch.to_owned();
        let item = item.clone();
        let path = self.path.clone();
        self.database
            .with_write_connection("seed source refresh queue", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_source_refresh_fence(
                            connection,
                            refresh_id,
                            &owner_id,
                            lease_epoch,
                        )?;
                        diesel::insert_into(console_graph_source_refresh_queue::table)
                            .values((
                                console_graph_source_refresh_queue::refresh_id.eq(refresh_id),
                                console_graph_source_refresh_queue::branch_name.eq(branch),
                                console_graph_source_refresh_queue::node_id.eq(&item.node_id),
                                console_graph_source_refresh_queue::traversal_kind
                                    .eq(item.traversal.as_str()),
                                console_graph_source_refresh_queue::processed.eq(0),
                                console_graph_source_refresh_queue::force_child_scan
                                    .eq(i32::from(force_child_scan)),
                            ))
                            .on_conflict((
                                console_graph_source_refresh_queue::refresh_id,
                                console_graph_source_refresh_queue::node_id,
                                console_graph_source_refresh_queue::traversal_kind,
                            ))
                            .do_nothing()
                            .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn load_refresh_batch(&self, refresh_id: i64) -> crate::Result<Vec<RefreshWorkItem>> {
        let path = self.path.clone();
        let rows = self
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT node_id, traversal_kind, node_committed, force_child_scan, \
                            child_cursor_relation_revision, child_cursor_node_id, \
                            parent_cursor_offset, parent_traversal_complete, \
                            child_scan_required, child_high_watermark_frozen, \
                            child_high_watermark_relation_revision, \
                            child_high_watermark_node_id \
                     FROM console_graph_source_refresh_queue \
                     WHERE refresh_id = ? AND processed = 0 \
                     ORDER BY node_committed DESC, parent_traversal_complete, \
                              node_id, traversal_kind LIMIT ?",
                )
                .bind::<BigInt, _>(refresh_id)
                .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                .load::<RefreshWorkRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        rows.into_iter()
            .map(|row| {
                let child_cursor =
                    match (row.child_cursor_relation_revision, row.child_cursor_node_id) {
                        (None, None) => None,
                        (Some(relation_revision), Some(node_id)) => Some(GraphChildPageCursor {
                            relation_revision,
                            node_id,
                        }),
                        (relation_revision, node_id) => {
                            return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                                column: "source_refresh_child_cursor",
                                value: format!("{relation_revision:?}/{node_id:?}"),
                            }
                            .fail();
                        }
                    };
                let child_high_watermark = match (
                    row.child_high_watermark_relation_revision,
                    row.child_high_watermark_node_id,
                ) {
                    (None, None) => None,
                    (Some(relation_revision), Some(node_id)) => Some(GraphChildPageCursor {
                        relation_revision,
                        node_id,
                    }),
                    (relation_revision, node_id) => {
                        return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                            column: "source_refresh_child_high_watermark",
                            value: format!("{relation_revision:?}/{node_id:?}"),
                        }
                        .fail();
                    }
                };
                Ok(RefreshWorkItem {
                    item: QueueItem {
                        node_id: row.node_id,
                        traversal: TraversalKind::parse(&row.traversal_kind)?,
                    },
                    node_committed: row.node_committed != 0,
                    force_child_scan: row.force_child_scan != 0,
                    child_cursor,
                    parent_cursor_offset: row.parent_cursor_offset,
                    parent_traversal_complete: row.parent_traversal_complete != 0,
                    child_scan_required: row.child_scan_required != 0,
                    child_high_watermark_frozen: row.child_high_watermark_frozen != 0,
                    child_high_watermark,
                })
            })
            .collect()
    }

    async fn process_parent_page(
        &self,
        refresh: &RefreshRun,
        branch: &str,
        work: &RefreshWorkItem,
        node: &Node,
    ) -> crate::Result<usize> {
        let (pending, next_offset, complete) =
            graph_parent_traversal_page(node, work.parent_cursor_offset);
        let refresh_id = refresh.refresh_id;
        let owner_id = refresh.owner_id.clone();
        let lease_epoch = refresh.lease_epoch;
        let branch = branch.to_owned();
        let item = work.item.clone();
        let expected_offset = work.parent_cursor_offset;
        let child_scan_required = work.child_scan_required;
        let path = self.path.clone();
        self.database
            .with_write_connection("commit source refresh parent page", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_source_refresh_fence(
                            connection,
                            refresh_id,
                            &owner_id,
                            lease_epoch,
                        )?;
                        let advanced = diesel::sql_query(
                            "UPDATE console_graph_source_refresh_queue \
                             SET parent_cursor_offset = ?, parent_traversal_complete = ?, \
                                 processed = ? \
                             WHERE refresh_id = ? AND node_id = ? AND traversal_kind = ? \
                               AND processed = 0 AND node_committed = 1 \
                               AND parent_traversal_complete = 0 \
                               AND parent_cursor_offset = ?",
                        )
                        .bind::<BigInt, _>(next_offset)
                        .bind::<Integer, _>(i32::from(complete))
                        .bind::<Integer, _>(i32::from(complete && !child_scan_required))
                        .bind::<BigInt, _>(refresh_id)
                        .bind::<Text, _>(&item.node_id)
                        .bind::<Text, _>(item.traversal.as_str())
                        .bind::<BigInt, _>(expected_offset)
                        .execute(connection)?;
                        if advanced != 1 {
                            return Ok(0);
                        }
                        let mut enqueued = 0usize;
                        for (node_id, traversal) in &pending {
                            enqueued +=
                                diesel::insert_into(console_graph_source_refresh_queue::table)
                                    .values((
                                        console_graph_source_refresh_queue::refresh_id
                                            .eq(refresh_id),
                                        console_graph_source_refresh_queue::branch_name.eq(&branch),
                                        console_graph_source_refresh_queue::node_id.eq(node_id),
                                        console_graph_source_refresh_queue::traversal_kind
                                            .eq(traversal.as_str()),
                                        console_graph_source_refresh_queue::processed.eq(0),
                                    ))
                                    .on_conflict((
                                        console_graph_source_refresh_queue::refresh_id,
                                        console_graph_source_refresh_queue::node_id,
                                        console_graph_source_refresh_queue::traversal_kind,
                                    ))
                                    .do_nothing()
                                    .execute(connection)?;
                        }
                        Ok(enqueued)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn process_child_page(
        &self,
        store: &SqliteGraphStore,
        refresh: &RefreshRun,
        branch: &str,
        work: &RefreshWorkItem,
    ) -> crate::Result<usize> {
        let refresh_id = refresh.refresh_id;
        let owner_id = refresh.owner_id.clone();
        let lease_epoch = refresh.lease_epoch;
        if !work.child_high_watermark_frozen {
            let freeze_owner_id = owner_id.clone();
            let high_watermark = store
                .graph_child_high_watermark_at_revision(
                    &work.item.node_id,
                    refresh.relation_revision,
                )
                .await
                .context(crate::error::StoreSnafu)?;
            let item = work.item.clone();
            let high_watermark_relation_revision = high_watermark
                .as_ref()
                .map(|watermark| watermark.relation_revision);
            let high_watermark_node_id = high_watermark
                .as_ref()
                .map(|watermark| watermark.node_id.clone());
            let path = self.path.clone();
            self.database
                .with_write_connection(
                    "freeze source refresh child high watermark",
                    move |connection| {
                        connection
                            .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                                require_source_refresh_fence(
                                    connection,
                                    refresh_id,
                                    &freeze_owner_id,
                                    lease_epoch,
                                )?;
                                diesel::sql_query(
                                    "UPDATE console_graph_source_refresh_queue \
                                     SET child_high_watermark_frozen = 1, \
                                         child_high_watermark_relation_revision = ?, \
                                         child_high_watermark_node_id = ?, \
                                         processed = CASE WHEN ? IS NULL THEN 1 ELSE processed END \
                                     WHERE refresh_id = ? AND node_id = ? \
                                       AND traversal_kind = ? AND processed = 0 \
                                       AND node_committed = 1 \
                                       AND parent_traversal_complete = 1 \
                                       AND child_scan_required = 1 \
                                       AND child_high_watermark_frozen = 0",
                                )
                                .bind::<Nullable<BigInt>, _>(high_watermark_relation_revision)
                                .bind::<Nullable<Text>, _>(high_watermark_node_id.as_deref())
                                .bind::<Nullable<Text>, _>(high_watermark_node_id.as_deref())
                                .bind::<BigInt, _>(refresh_id)
                                .bind::<Text, _>(&item.node_id)
                                .bind::<Text, _>(item.traversal.as_str())
                                .execute(connection)?;
                                Ok(())
                            })
                            .context(QueryGraphSnapshotStoreSnafu { path })
                    },
                )
                .await?;
            return Ok(0);
        }
        let Some(child_high_watermark) = work.child_high_watermark.as_ref() else {
            let empty_owner_id = owner_id.clone();
            let item = work.item.clone();
            let path = self.path.clone();
            self.database
                .with_write_connection("complete empty source child scan", move |connection| {
                    connection
                        .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                            require_source_refresh_fence(
                                connection,
                                refresh_id,
                                &empty_owner_id,
                                lease_epoch,
                            )?;
                            diesel::sql_query(
                                "UPDATE console_graph_source_refresh_queue SET processed = 1 \
                                 WHERE refresh_id = ? AND node_id = ? \
                                   AND traversal_kind = ? AND processed = 0 \
                                   AND node_committed = 1 \
                                   AND parent_traversal_complete = 1 \
                                   AND child_scan_required = 1 \
                                   AND child_high_watermark_frozen = 1 \
                                   AND child_high_watermark_relation_revision IS NULL \
                                   AND child_high_watermark_node_id IS NULL",
                            )
                            .bind::<BigInt, _>(refresh_id)
                            .bind::<Text, _>(&item.node_id)
                            .bind::<Text, _>(item.traversal.as_str())
                            .execute(connection)?;
                            Ok(())
                        })
                        .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
            return Ok(0);
        };
        let (mut pending, next_cursor, complete) = self
            .load_child_traversal_page(
                store,
                &work.item,
                work.child_cursor.as_ref(),
                Some(child_high_watermark),
                refresh.relation_revision,
            )
            .await?;
        if work.force_child_scan
            && let Some(base_generation) = refresh.base_generation
        {
            let node_ids = pending
                .iter()
                .map(|(node_id, _)| node_id.clone())
                .collect::<Vec<_>>();
            let completed = self.completed_work_keys(base_generation, node_ids).await?;
            pending.retain(|(node_id, traversal)| {
                !completed.contains(&(node_id.clone(), traversal.as_str().to_owned()))
            });
        }
        let pending = pending
            .into_iter()
            .filter(|(node_id, _)| !node_id.is_empty())
            .collect::<BTreeSet<_>>();
        let branch = branch.to_owned();
        let item = work.item.clone();
        let expected_cursor_relation_revision = work
            .child_cursor
            .as_ref()
            .map(|cursor| cursor.relation_revision);
        let expected_cursor_node_id = work
            .child_cursor
            .as_ref()
            .map(|cursor| cursor.node_id.clone());
        let cursor_relation_revision = next_cursor.as_ref().map(|cursor| cursor.relation_revision);
        let cursor_node_id = next_cursor.as_ref().map(|cursor| cursor.node_id.clone());
        let path = self.path.clone();
        self.database
            .with_write_connection("commit source refresh child page", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_source_refresh_fence(
                            connection,
                            refresh_id,
                            &owner_id,
                            lease_epoch,
                        )?;
                        let mut enqueued = 0usize;
                        for (node_id, traversal) in &pending {
                            enqueued +=
                                diesel::insert_into(console_graph_source_refresh_queue::table)
                                    .values((
                                        console_graph_source_refresh_queue::refresh_id
                                            .eq(refresh_id),
                                        console_graph_source_refresh_queue::branch_name.eq(&branch),
                                        console_graph_source_refresh_queue::node_id.eq(node_id),
                                        console_graph_source_refresh_queue::traversal_kind
                                            .eq(traversal.as_str()),
                                        console_graph_source_refresh_queue::processed.eq(0),
                                    ))
                                    .on_conflict((
                                        console_graph_source_refresh_queue::refresh_id,
                                        console_graph_source_refresh_queue::node_id,
                                        console_graph_source_refresh_queue::traversal_kind,
                                    ))
                                    .do_nothing()
                                    .execute(connection)?;
                        }
                        diesel::sql_query(
                            "UPDATE console_graph_source_refresh_queue \
                             SET processed = ?, child_cursor_relation_revision = ?, \
                                 child_cursor_node_id = ? \
                             WHERE refresh_id = ? AND node_id = ? \
                               AND traversal_kind = ? AND processed = 0 \
                               AND node_committed = 1 AND parent_traversal_complete = 1 \
                               AND child_scan_required = 1 \
                               AND ((child_cursor_relation_revision IS NULL AND ? IS NULL) \
                                    OR child_cursor_relation_revision = ?) \
                               AND ((child_cursor_node_id IS NULL AND ? IS NULL) \
                                    OR child_cursor_node_id = ?)",
                        )
                        .bind::<Integer, _>(i32::from(complete))
                        .bind::<Nullable<BigInt>, _>(cursor_relation_revision)
                        .bind::<Nullable<Text>, _>(cursor_node_id)
                        .bind::<BigInt, _>(refresh_id)
                        .bind::<Text, _>(&item.node_id)
                        .bind::<Text, _>(item.traversal.as_str())
                        .bind::<Nullable<BigInt>, _>(expected_cursor_relation_revision)
                        .bind::<Nullable<BigInt>, _>(expected_cursor_relation_revision)
                        .bind::<Nullable<Text>, _>(expected_cursor_node_id.clone())
                        .bind::<Nullable<Text>, _>(expected_cursor_node_id)
                        .execute(connection)?;
                        Ok(enqueued)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn commit_reused_work(
        &self,
        refresh: &RefreshRun,
        reused: &[QueueItem],
    ) -> crate::Result<()> {
        if reused.is_empty() {
            return Ok(());
        }
        let refresh_id = refresh.refresh_id;
        let owner_id = refresh.owner_id.clone();
        let lease_epoch = refresh.lease_epoch;
        let reused = reused.to_vec();
        let path = self.path.clone();
        self.database
            .with_write_connection("commit reused source refresh work", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_source_refresh_fence(
                            connection,
                            refresh_id,
                            &owner_id,
                            lease_epoch,
                        )?;
                        for item in &reused {
                            diesel::sql_query(
                                "UPDATE console_graph_source_refresh_queue \
                                 SET node_committed = 1, processed = 1 \
                                 WHERE refresh_id = ? AND node_id = ? \
                                   AND traversal_kind = ? AND processed = 0 \
                                   AND force_child_scan = 0",
                            )
                            .bind::<BigInt, _>(refresh_id)
                            .bind::<Text, _>(&item.node_id)
                            .bind::<Text, _>(item.traversal.as_str())
                            .execute(connection)?;
                        }
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn commit_refresh_batch(
        &self,
        refresh: &RefreshRun,
        branch: &str,
        processed: &[QueueItem],
        child_rechecks: &[QueueItem],
    ) -> crate::Result<()> {
        let refresh_id = refresh.refresh_id;
        let owner_id = refresh.owner_id.clone();
        let lease_epoch = refresh.lease_epoch;
        let branch = branch.to_owned();
        let processed = processed
            .iter()
            .map(|item| (item.node_id.clone(), item.traversal))
            .collect::<BTreeSet<_>>();
        let child_rechecks = child_rechecks
            .iter()
            .filter(|item| !item.node_id.is_empty())
            .map(|item| (item.node_id.clone(), item.traversal))
            .collect::<BTreeSet<_>>();
        let path = self.path.clone();
        self.database
            .with_write_connection("commit source refresh node batch", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_source_refresh_fence(
                            connection,
                            refresh_id,
                            &owner_id,
                            lease_epoch,
                        )?;
                        for (node_id, _) in &processed {
                            diesel::insert_into(console_graph_source_branch_nodes::table)
                                .values((
                                    console_graph_source_branch_nodes::branch_name.eq(&branch),
                                    console_graph_source_branch_nodes::contribution_generation
                                        .eq(refresh_id),
                                    console_graph_source_branch_nodes::node_id.eq(node_id),
                                ))
                                .on_conflict((
                                    console_graph_source_branch_nodes::branch_name,
                                    console_graph_source_branch_nodes::contribution_generation,
                                    console_graph_source_branch_nodes::node_id,
                                ))
                                .do_nothing()
                                .execute(connection)?;
                        }
                        for (node_id, traversal) in &child_rechecks {
                            diesel::sql_query(
                                "INSERT OR IGNORE INTO console_graph_source_child_rechecks ( \
                                     branch_name, contribution_generation, node_id, traversal_kind \
                                 ) VALUES (?, ?, ?, ?)",
                            )
                            .bind::<Text, _>(&branch)
                            .bind::<BigInt, _>(refresh_id)
                            .bind::<Text, _>(node_id)
                            .bind::<Text, _>(traversal.as_str())
                            .execute(connection)?;
                        }
                        for (node_id, traversal) in &processed {
                            let has_children =
                                child_rechecks.contains(&(node_id.clone(), *traversal));
                            diesel::sql_query(
                                "UPDATE console_graph_source_refresh_queue \
                                 SET node_committed = 1, child_scan_required = ? \
                                 WHERE refresh_id = ? AND node_id = ? \
                                   AND traversal_kind = ? AND processed = 0",
                            )
                            .bind::<Integer, _>(i32::from(has_children))
                            .bind::<BigInt, _>(refresh_id)
                            .bind::<Text, _>(node_id)
                            .bind::<Text, _>(traversal.as_str())
                            .execute(connection)?;
                        }
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn commit_branch(
        &self,
        refresh: &RefreshRun,
        record: GraphBranchRecord,
    ) -> crate::Result<i64> {
        let refresh_id = refresh.refresh_id;
        let target_contribution_generation = refresh.target_contribution_generation;
        let owner_id = refresh.owner_id.clone();
        let lease_epoch = refresh.lease_epoch;
        let target_invalidation_version = refresh.target_invalidation_version;
        let target_invalidation_incarnation = refresh.target_invalidation_incarnation.clone();
        let target_invalidation_kind = refresh.target_invalidation_kind.clone();
        let relation_revision = refresh.relation_revision;
        let append = refresh.base_generation.is_some();
        let base_contribution_generation = refresh.base_generation;
        let state_json = serde_json::to_string(&record.state).context(
            SerializeGraphSnapshotStoreValueSnafu {
                column: "state_json",
            },
        )?;
        let path = self.path.clone();
        self.database
            .with_write_connection("publish source refresh", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        require_source_refresh_fence(
                            connection,
                            refresh_id,
                            &owner_id,
                            lease_epoch,
                        )?;
                        let pending = diesel::sql_query(
                            "SELECT CASE WHEN EXISTS ( \
                                 SELECT 1 FROM console_graph_source_refresh_queue \
                                 WHERE refresh_id = ? AND processed = 0 \
                                 LIMIT 1 \
                             ) THEN 1 ELSE 0 END AS value",
                        )
                        .bind::<BigInt, _>(refresh_id)
                        .get_result::<FlagRow>(connection)?
                        .value
                            != 0;
                        if pending {
                            return Err(diesel::result::Error::RollbackTransaction);
                        }
                        let incomplete_relations = diesel::sql_query(
                            "SELECT CASE WHEN EXISTS ( \
                                 SELECT 1 \
                                 FROM console_graph_source_branch_nodes AS membership \
                                 INNER JOIN console_graph_source_nodes AS node \
                                     ON node.node_id = membership.node_id \
                                 WHERE membership.branch_name = ? \
                                   AND membership.contribution_generation = ? \
                                   AND node.relation_ingest_complete = 0 \
                                 LIMIT 1 \
                             ) THEN 1 ELSE 0 END AS value",
                        )
                        .bind::<Text, _>(&record.name)
                        .bind::<BigInt, _>(refresh_id)
                        .get_result::<FlagRow>(connection)?
                        .value
                            != 0;
                        if incomplete_relations {
                            return Err(diesel::result::Error::RollbackTransaction);
                        }
                        let expected = diesel::sql_query(
                            "SELECT expected_branch_head_id, expected_branch_state_json, \
                                    expected_branch_contribution_generation, \
                                    expected_branch_source_revision, expected_branch_absent \
                             FROM console_graph_source_refresh_runs WHERE refresh_id = ?",
                        )
                        .bind::<BigInt, _>(refresh_id)
                        .get_result::<RefreshExpectedBranchRow>(connection)?;
                        if expected.expected_branch_absent != 0 {
                            if expected.expected_branch_head_id.is_some()
                                || expected.expected_branch_state_json.is_some()
                                || expected.expected_branch_contribution_generation.is_some()
                                || expected.expected_branch_source_revision.is_some()
                            {
                                return Err(diesel::result::Error::RollbackTransaction);
                            }
                            let inserted = diesel::insert_into(console_graph_source_branches::table)
                                .values((
                                    console_graph_source_branches::name.eq(&record.name),
                                    console_graph_source_branches::head_id.eq(&record.head_id),
                                    console_graph_source_branches::state_json.eq(&state_json),
                                    console_graph_source_branches::contribution_generation
                                        .eq(target_contribution_generation),
                                ))
                                .on_conflict(console_graph_source_branches::name)
                                .do_nothing()
                                .execute(connection)?;
                            if inserted != 1 {
                                return Err(diesel::result::Error::RollbackTransaction);
                            }
                        } else {
                            let Some(expected_head_id) = expected.expected_branch_head_id else {
                                return Err(diesel::result::Error::RollbackTransaction);
                            };
                            let Some(expected_state_json) = expected.expected_branch_state_json else {
                                return Err(diesel::result::Error::RollbackTransaction);
                            };
                            let Some(expected_contribution_generation) =
                                expected.expected_branch_contribution_generation
                            else {
                                return Err(diesel::result::Error::RollbackTransaction);
                            };
                            let Some(expected_source_revision) =
                                expected.expected_branch_source_revision
                            else {
                                return Err(diesel::result::Error::RollbackTransaction);
                            };
                            let publication_matches = diesel::sql_query(
                                "SELECT CASE WHEN EXISTS ( \
                                     SELECT 1 FROM console_graph_source_branch_publications \
                                     WHERE branch_name = ? \
                                       AND target_contribution_generation = ? \
                                       AND source_revision = ? \
                                 ) THEN 1 ELSE 0 END AS value",
                            )
                            .bind::<Text, _>(&record.name)
                            .bind::<BigInt, _>(expected_contribution_generation)
                            .bind::<BigInt, _>(expected_source_revision)
                            .get_result::<FlagRow>(connection)?
                            .value
                                != 0;
                            if !publication_matches {
                                return Err(diesel::result::Error::RollbackTransaction);
                            }
                            let updated = diesel::update(
                                console_graph_source_branches::table
                                    .filter(console_graph_source_branches::name.eq(&record.name))
                                    .filter(console_graph_source_branches::head_id.eq(expected_head_id))
                                    .filter(
                                        console_graph_source_branches::state_json
                                            .eq(expected_state_json),
                                    )
                                    .filter(
                                        console_graph_source_branches::contribution_generation
                                            .eq(expected_contribution_generation),
                                    ),
                            )
                            .set((
                                console_graph_source_branches::head_id.eq(&record.head_id),
                                console_graph_source_branches::state_json.eq(&state_json),
                                console_graph_source_branches::contribution_generation
                                    .eq(target_contribution_generation),
                            ))
                            .execute(connection)?;
                            if updated != 1 {
                                return Err(diesel::result::Error::RollbackTransaction);
                            }
                        }
                        let source_revision = advance_source_revision(connection)?;
                        insert_source_branch_history(
                            connection,
                            &record.name,
                            source_revision,
                            Some(target_contribution_generation),
                            Some(&record.head_id),
                            Some(&state_json),
                        )?;
                        let published = diesel::sql_query(
                            "UPDATE console_graph_source_refresh_runs \
                             SET status = 'published', \
                                 published_source_revision = ?, \
                                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE refresh_id = ? AND status = 'building' \
                               AND owner_id = ? AND lease_epoch = ? \
                               AND branch_name = ? AND target_head_id = ? \
                               AND target_state_json = ? \
                               AND target_contribution_generation = ?",
                        )
                        .bind::<BigInt, _>(source_revision)
                        .bind::<BigInt, _>(refresh_id)
                        .bind::<Text, _>(&owner_id)
                        .bind::<BigInt, _>(lease_epoch)
                        .bind::<Text, _>(&record.name)
                        .bind::<Text, _>(&record.head_id)
                        .bind::<Text, _>(&state_json)
                        .bind::<BigInt, _>(target_contribution_generation)
                        .execute(connection)?;
                        if published != 1 {
                            return Err(diesel::result::Error::RollbackTransaction);
                        }
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_publications ( \
                                 branch_name, target_contribution_generation, source_revision \
                             ) VALUES (?, ?, ?) \
                             ON CONFLICT(branch_name) DO UPDATE SET \
                                 target_contribution_generation = \
                                     excluded.target_contribution_generation, \
                                 source_revision = excluded.source_revision",
                        )
                        .bind::<Text, _>(&record.name)
                        .bind::<BigInt, _>(target_contribution_generation)
                        .bind::<BigInt, _>(source_revision)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_change_journal ( \
                                 source_revision, target_invalidation_incarnation, \
                                 target_invalidation_version, branch_name, change_kind, refresh_id, \
                                 base_contribution_generation, \
                                 target_contribution_generation, head_id, state_json \
                             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        )
                        .bind::<BigInt, _>(source_revision)
                        .bind::<Text, _>(&target_invalidation_incarnation)
                        .bind::<BigInt, _>(target_invalidation_version)
                        .bind::<Text, _>(&record.name)
                        .bind::<Text, _>(if append { "append" } else { "replace" })
                        .bind::<BigInt, _>(refresh_id)
                        .bind::<Nullable<BigInt>, _>(base_contribution_generation)
                        .bind::<BigInt, _>(target_contribution_generation)
                        .bind::<Text, _>(&record.head_id)
                        .bind::<Text, _>(&state_json)
                        .execute(connection)?;
                        if !target_invalidation_incarnation.is_empty()
                            && target_invalidation_version > 0
                        {
                            let dirty_seeds = diesel::sql_query(
                                "SELECT node_id, child_high_watermark_relation_revision, \
                                        child_high_watermark_node_id \
                                 FROM console_graph_source_refresh_dirty_seeds \
                                 WHERE refresh_id = ? ORDER BY node_id LIMIT ?",
                            )
                            .bind::<BigInt, _>(refresh_id)
                            .bind::<BigInt, _>((SOURCE_DIRTY_SEED_LIMIT + 1) as i64)
                            .load::<InvalidationReceiptSeedRow>(connection)?;
                            if dirty_seeds.len() > SOURCE_DIRTY_SEED_LIMIT {
                                return Err(diesel::result::Error::RollbackTransaction);
                            }
                            let dirty_seeds = dirty_seeds
                            .into_iter()
                            .map(|seed| {
                                let child_high_watermark = match (
                                    seed.child_high_watermark_relation_revision,
                                    seed.child_high_watermark_node_id,
                                ) {
                                    (None, None) => None,
                                    (Some(relation_revision), Some(node_id)) => {
                                        Some(GraphChildPageCursor {
                                            relation_revision,
                                            node_id,
                                        })
                                    }
                                    _ => return Err(diesel::result::Error::RollbackTransaction),
                                };
                                Ok(DirtySeed {
                                    node_id: seed.node_id,
                                    child_high_watermark,
                                })
                            })
                            .collect::<QueryResult<Vec<_>>>()?;
                            insert_source_invalidation_receipt(
                                connection,
                                &target_invalidation_incarnation,
                                target_invalidation_version,
                                &record.name,
                                source_revision,
                                relation_revision,
                                Some(refresh_id),
                                &target_invalidation_kind,
                                &record.head_id,
                                &state_json,
                                &dirty_seeds,
                            )?;
                        }
                        Ok(source_revision)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn remove_branch(&self, name: &str) -> crate::Result<()> {
        self.remove_branch_event(name, "", 0, 0).await
    }

    async fn remove_branch_event(
        &self,
        name: &str,
        target_invalidation_incarnation: &str,
        target_invalidation_version: i64,
        relation_revision: i64,
    ) -> crate::Result<()> {
        let name = name.to_owned();
        let target_invalidation_incarnation = target_invalidation_incarnation.to_owned();
        let path = self.path.clone();
        self.database
            .with_write_connection("remove published source branch", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        if !target_invalidation_incarnation.is_empty()
                            && target_invalidation_version > 0
                        {
                            let replayed = diesel::sql_query(
                                "SELECT CASE WHEN EXISTS ( \
                                     SELECT 1 \
                                     FROM console_graph_source_invalidation_receipts \
                                     WHERE target_invalidation_incarnation = ? \
                                       AND target_invalidation_version = ? \
                                       AND branch_name = ? AND target_head_id = '' \
                                       AND target_state_json = '' \
                                       AND relation_revision = ? \
                                 ) THEN 1 ELSE 0 END AS value",
                            )
                            .bind::<Text, _>(&target_invalidation_incarnation)
                            .bind::<BigInt, _>(target_invalidation_version)
                            .bind::<Text, _>(&name)
                            .bind::<BigInt, _>(relation_revision)
                            .get_result::<FlagRow>(connection)?
                            .value
                                != 0;
                            if replayed {
                                return Ok(());
                            }
                        }
                        diesel::sql_query(
                            "UPDATE console_graph_source_refresh_runs \
                             SET status = 'superseded', \
                                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                             WHERE branch_name = ? AND status = 'building'",
                        )
                        .bind::<Text, _>(&name)
                        .execute(connection)?;
                        let removed = diesel::sql_query(
                            "SELECT head_id, state_json, contribution_generation \
                             FROM console_graph_source_branches WHERE name = ?",
                        )
                        .bind::<Text, _>(&name)
                        .get_result::<DeletedBranchRow>(connection)
                        .optional()?;
                        let deleted = diesel::delete(
                            console_graph_source_branches::table
                                .filter(console_graph_source_branches::name.eq(&name)),
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "DELETE FROM console_graph_source_branch_publications \
                             WHERE branch_name = ?",
                        )
                        .bind::<Text, _>(&name)
                        .execute(connection)?;
                        let source_revision = if let Some(removed) = removed {
                            if deleted != 1 {
                                return Err(diesel::result::Error::RollbackTransaction);
                            }
                            let source_revision = advance_source_revision(connection)?;
                            insert_source_branch_history(
                                connection,
                                &name,
                                source_revision,
                                None,
                                None,
                                None,
                            )?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_branch_change_journal ( \
                                     source_revision, target_invalidation_incarnation, \
                                     target_invalidation_version, branch_name, change_kind, \
                                     base_contribution_generation, head_id, state_json \
                                 ) VALUES (?, ?, ?, ?, 'delete', ?, ?, ?)",
                            )
                            .bind::<BigInt, _>(source_revision)
                            .bind::<Text, _>(&target_invalidation_incarnation)
                            .bind::<BigInt, _>(target_invalidation_version)
                            .bind::<Text, _>(&name)
                            .bind::<BigInt, _>(removed.contribution_generation)
                            .bind::<Text, _>(removed.head_id)
                            .bind::<Text, _>(removed.state_json)
                            .execute(connection)?;
                            source_revision
                        } else {
                            current_source_revision(connection)?
                        };
                        if !target_invalidation_incarnation.is_empty()
                            && target_invalidation_version > 0
                        {
                            insert_source_invalidation_receipt(
                                connection,
                                &target_invalidation_incarnation,
                                target_invalidation_version,
                                &name,
                                source_revision,
                                relation_revision,
                                None,
                                "targeted",
                                "",
                                "",
                                &[],
                            )?;
                        }
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        self.cleanup_stale_refreshes_bounded().await
    }

    async fn cleanup_stale_refreshes_bounded(&self) -> crate::Result<()> {
        loop {
            match self.advance_stale_refresh_cleanup().await? {
                SourceRefreshCleanupStep::Continue => tokio::task::yield_now().await,
                SourceRefreshCleanupStep::RoundComplete => {
                    self.prune_source_change_journal_bounded().await?;
                    return Ok(());
                }
            }
        }
    }

    async fn advance_stale_refresh_cleanup(&self) -> crate::Result<SourceRefreshCleanupStep> {
        let path = self.path.clone();
        self.database
            .with_write_connection("advance stale source refresh cleanup", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let state = diesel::sql_query(
                            "SELECT upper_bound_refresh_id, raw_refresh_id_cursor, \
                                    active_refresh_id \
                             FROM console_graph_source_refresh_cleanup_state WHERE id = 1",
                        )
                        .get_result::<SourceRefreshCleanupStateRow>(connection)?;
                        let upper_bound = if let Some(upper_bound) = state.upper_bound_refresh_id {
                            upper_bound
                        } else {
                            let upper_bound = diesel::sql_query(
                                "SELECT COALESCE(MAX(refresh_id), 0) AS contribution_generation \
                                 FROM console_graph_source_refresh_runs",
                            )
                            .get_result::<GenerationRow>(connection)?
                            .contribution_generation;
                            let updated = diesel::sql_query(
                                "UPDATE console_graph_source_refresh_cleanup_state \
                                 SET upper_bound_refresh_id = ? \
                                 WHERE id = 1 AND upper_bound_refresh_id IS NULL \
                                   AND raw_refresh_id_cursor = ? \
                                   AND active_refresh_id IS NULL",
                            )
                            .bind::<BigInt, _>(upper_bound)
                            .bind::<BigInt, _>(state.raw_refresh_id_cursor)
                            .execute(connection)?;
                            if updated != 1 {
                                return Err(diesel::result::Error::NotFound);
                            }
                            upper_bound
                        };

                        if let Some(active_refresh_id) = state.active_refresh_id {
                            let mut remaining = SOURCE_REFRESH_CLEANUP_DELETE_BATCH_SIZE;
                            let mut deleted = 0usize;
                            for (table, refresh_column) in SOURCE_REFRESH_CLEANUP_DEPENDENT_TABLES {
                                if remaining == 0 {
                                    break;
                                }
                                let affected = diesel::sql_query(
                                    source_refresh_cleanup_delete_sql(table, refresh_column),
                                )
                                .bind::<BigInt, _>(active_refresh_id)
                                .bind::<BigInt, _>(i64::try_from(remaining).unwrap_or(i64::MAX))
                                .execute(connection)?;
                                deleted = deleted.saturating_add(affected);
                                remaining = remaining.saturating_sub(affected);
                            }
                            if deleted == 0 {
                                let updated = diesel::sql_query(
                                    "UPDATE console_graph_source_refresh_cleanup_state \
                                     SET active_refresh_id = NULL \
                                     WHERE id = 1 AND upper_bound_refresh_id = ? \
                                       AND raw_refresh_id_cursor = ? \
                                       AND active_refresh_id = ?",
                                )
                                .bind::<BigInt, _>(upper_bound)
                                .bind::<BigInt, _>(state.raw_refresh_id_cursor)
                                .bind::<BigInt, _>(active_refresh_id)
                                .execute(connection)?;
                                if updated != 1 {
                                    return Err(diesel::result::Error::NotFound);
                                }
                            }
                            return Ok(SourceRefreshCleanupStep::Continue);
                        }

                        let candidates =
                            diesel::sql_query(SOURCE_REFRESH_CLEANUP_CANDIDATE_PAGE_SQL)
                                .bind::<BigInt, _>(state.raw_refresh_id_cursor)
                                .bind::<BigInt, _>(upper_bound)
                                .bind::<BigInt, _>(
                                    i64::try_from(SOURCE_REFRESH_CLEANUP_CANDIDATE_BATCH_SIZE)
                                        .unwrap_or(i64::MAX),
                                )
                                .load::<SourceRefreshCleanupCandidateRow>(connection)?;
                        let Some(last_candidate) = candidates.last() else {
                            let updated = diesel::sql_query(
                                "UPDATE console_graph_source_refresh_cleanup_state \
                                 SET upper_bound_refresh_id = NULL, \
                                     raw_refresh_id_cursor = 0, active_refresh_id = NULL \
                                 WHERE id = 1 AND upper_bound_refresh_id = ? \
                                   AND raw_refresh_id_cursor = ? \
                                   AND active_refresh_id IS NULL",
                            )
                            .bind::<BigInt, _>(upper_bound)
                            .bind::<BigInt, _>(state.raw_refresh_id_cursor)
                            .execute(connection)?;
                            if updated != 1 {
                                return Err(diesel::result::Error::NotFound);
                            }
                            return Ok(SourceRefreshCleanupStep::RoundComplete);
                        };

                        let mut active_refresh_id = None;
                        for candidate in &candidates {
                            if matches!(candidate.status.as_str(), "published" | "superseded")
                                && !Self::source_refresh_is_protected(
                                    connection,
                                    candidate.refresh_id,
                                )?
                            {
                                active_refresh_id = Some(candidate.refresh_id);
                                break;
                            }
                        }
                        if let Some(active_refresh_id) = active_refresh_id {
                            let claimed = diesel::sql_query(
                                "DELETE FROM console_graph_source_refresh_runs \
                                 WHERE refresh_id = ? \
                                   AND status IN ('published', 'superseded')",
                            )
                            .bind::<BigInt, _>(active_refresh_id)
                            .execute(connection)?;
                            if claimed != 1 {
                                return Err(diesel::result::Error::NotFound);
                            }
                        }
                        let next_cursor = active_refresh_id.unwrap_or(last_candidate.refresh_id);
                        let updated = diesel::sql_query(
                            "UPDATE console_graph_source_refresh_cleanup_state \
                             SET raw_refresh_id_cursor = ?, active_refresh_id = ? \
                             WHERE id = 1 AND upper_bound_refresh_id = ? \
                               AND raw_refresh_id_cursor = ? \
                               AND active_refresh_id IS NULL",
                        )
                        .bind::<BigInt, _>(next_cursor)
                        .bind::<Nullable<BigInt>, _>(active_refresh_id)
                        .bind::<BigInt, _>(upper_bound)
                        .bind::<BigInt, _>(state.raw_refresh_id_cursor)
                        .execute(connection)?;
                        if updated != 1 {
                            return Err(diesel::result::Error::NotFound);
                        }
                        Ok(SourceRefreshCleanupStep::Continue)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    fn source_refresh_is_protected(
        connection: &mut SqliteConnection,
        refresh_id: i64,
    ) -> QueryResult<bool> {
        diesel::sql_query(SOURCE_REFRESH_CLEANUP_PROTECTED_SQL)
            .bind::<BigInt, _>(refresh_id)
            .bind::<BigInt, _>(refresh_id)
            .bind::<BigInt, _>(refresh_id)
            .bind::<BigInt, _>(refresh_id)
            .bind::<BigInt, _>(refresh_id)
            .bind::<BigInt, _>(refresh_id)
            .bind::<BigInt, _>(source_time_ms())
            .bind::<BigInt, _>(refresh_id)
            .bind::<BigInt, _>(refresh_id)
            .get_result::<FlagRow>(connection)
            .map(|row| row.value != 0)
    }

    async fn prune_source_event_history(
        &self,
        active_incarnation: &str,
        active_version: i64,
    ) -> crate::Result<()> {
        loop {
            let incarnation = active_incarnation.to_owned();
            let path = self.path.clone();
            let deleted = self
                .database
                .with_write_connection("prune source invalidation history", move |connection| {
                    connection
                        .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                            let receipts = diesel::sql_query(
                                "SELECT receipt.receipt_id \
                                 FROM console_graph_source_invalidation_receipts AS receipt \
                                 WHERE (receipt.target_invalidation_incarnation <> ? \
                                        OR (receipt.target_invalidation_incarnation = ? \
                                            AND receipt.target_invalidation_version < ?)) \
                                   AND NOT EXISTS ( \
                                       SELECT 1 FROM console_graph_source_sweep_runs AS sweep \
                                       WHERE sweep.target_invalidation_incarnation = \
                                                 receipt.target_invalidation_incarnation \
                                         AND sweep.target_invalidation_version = \
                                                 receipt.target_invalidation_version \
                                         AND sweep.status = 'building' \
                                   ) \
                                   AND NOT EXISTS ( \
                                       SELECT 1 \
                                       FROM console_graph_source_invalidation_boundaries AS boundary \
                                       WHERE boundary.target_invalidation_incarnation = \
                                                 receipt.target_invalidation_incarnation \
                                         AND boundary.target_invalidation_version = \
                                                 receipt.target_invalidation_version \
                                         AND boundary.status = 'building' \
                                   ) \
                                 ORDER BY receipt.receipt_id LIMIT ?",
                            )
                            .bind::<Text, _>(&incarnation)
                            .bind::<Text, _>(&incarnation)
                            .bind::<BigInt, _>(active_version)
                            .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                            .load::<ReceiptIdRow>(connection)?;
                            let mut deleted = 0usize;
                            for receipt in receipts {
                                diesel::sql_query(
                                    "DELETE FROM console_graph_source_invalidation_receipt_seeds \
                                     WHERE receipt_id = ?",
                                )
                                .bind::<BigInt, _>(receipt.receipt_id)
                                .execute(connection)?;
                                deleted += diesel::sql_query(
                                    "DELETE FROM console_graph_source_invalidation_receipts \
                                     WHERE receipt_id = ?",
                                )
                                .bind::<BigInt, _>(receipt.receipt_id)
                                .execute(connection)?;
                            }
                            let deleted_sweeps = diesel::sql_query(
                                "DELETE FROM console_graph_source_sweep_runs \
                                 WHERE rowid IN ( \
                                     SELECT rowid FROM console_graph_source_sweep_runs \
                                     WHERE status = 'completed' \
                                       AND (target_invalidation_incarnation <> ? \
                                            OR (target_invalidation_incarnation = ? \
                                                AND target_invalidation_version < ?)) \
                                     ORDER BY updated_at, target_invalidation_incarnation, \
                                              target_invalidation_version LIMIT ? \
                                 )",
                            )
                            .bind::<Text, _>(&incarnation)
                            .bind::<Text, _>(&incarnation)
                            .bind::<BigInt, _>(active_version)
                            .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                            .execute(connection)?;
                            let deleted_boundaries = diesel::sql_query(
                                "DELETE FROM console_graph_source_invalidation_boundaries \
                                 WHERE rowid IN ( \
                                     SELECT rowid \
                                     FROM console_graph_source_invalidation_boundaries \
                                     WHERE status = 'completed' \
                                       AND (target_invalidation_incarnation <> ? \
                                            OR (target_invalidation_incarnation = ? \
                                                AND target_invalidation_version < ?)) \
                                     ORDER BY updated_at, target_invalidation_incarnation, \
                                              target_invalidation_version LIMIT ? \
                                 )",
                            )
                            .bind::<Text, _>(&incarnation)
                            .bind::<Text, _>(&incarnation)
                            .bind::<BigInt, _>(active_version)
                            .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                            .execute(connection)?;
                            Ok(deleted + deleted_sweeps + deleted_boundaries)
                        })
                        .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
            if deleted == 0 {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    async fn prune_source_change_journal_bounded(&self) -> crate::Result<()> {
        loop {
            let path = self.path.clone();
            let deleted = self
                .database
                .with_write_connection("prune source change journal", move |connection| {
                    let now_ms = source_time_ms();
                    let low_watermark = diesel::sql_query(
                        "SELECT COALESCE( \
                             MIN(required.source_revision), \
                             -1 \
                         ) AS contribution_generation \
                         FROM ( \
                             SELECT revision.source_revision \
                             FROM console_graph_generation_state AS state \
                             INNER JOIN console_graph_generation_source_revisions AS revision \
                                 ON revision.generation = state.active_generation \
                             WHERE state.id = 1 \
                             UNION ALL \
                             SELECT COALESCE( \
                                 build.dag_baseline_source_revision, \
                                 (SELECT revision.source_revision \
                                  FROM console_graph_generation_state AS state \
                                  INNER JOIN \
                                      console_graph_generation_source_revisions AS revision \
                                      ON revision.generation = state.active_generation \
                                  WHERE state.id = 1), \
                                 -1 \
                             ) AS source_revision \
                             FROM console_graph_build_runs AS build \
                             WHERE build.status IN ('building', 'paused') \
                             UNION ALL \
                             SELECT MIN(scan.source_revision) AS source_revision \
                             FROM console_graph_source_dynamic_branch_scans AS scan \
                                  INDEXED BY \
                                      console_graph_source_dynamic_branch_scans_retention_idx \
                             WHERE scan.status = 'building' \
                               AND scan.scan_kind = 'dirty_parent' \
                             UNION ALL \
                             SELECT MIN(scan.source_revision) AS source_revision \
                             FROM console_graph_source_dynamic_branch_scans AS scan \
                                  INDEXED BY \
                                      console_graph_source_dynamic_branch_scans_active_retention_idx \
                             WHERE scan.status = 'building' \
                               AND scan.scan_kind = 'affected' \
                               AND scan.lease_expires_at_ms > ? \
                         ) AS required",
                    )
                    .bind::<BigInt, _>(now_ms)
                    .get_result::<GenerationRow>(connection)
                    .context(QueryGraphSnapshotStoreSnafu { path: path.clone() })?
                    .contribution_generation;
                    let deleted = diesel::sql_query(
                        "DELETE FROM console_graph_source_branch_change_journal \
                         WHERE rowid IN ( \
                             SELECT rowid \
                             FROM console_graph_source_branch_change_journal \
                             WHERE source_revision < ? \
                             ORDER BY source_revision, branch_name LIMIT ? \
                         )",
                    )
                    .bind::<BigInt, _>(low_watermark)
                    .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                    .execute(connection)
                    .context(QueryGraphSnapshotStoreSnafu { path: path.clone() })?;
                    if deleted > 0 {
                        return Ok(deleted);
                    }
                    diesel::sql_query(
                        "DELETE FROM console_graph_source_branch_history \
                         WHERE rowid IN ( \
                             SELECT history.rowid \
                             FROM console_graph_source_branch_history AS history \
                             WHERE history.source_revision < ? \
                               AND EXISTS ( \
                                   SELECT 1 \
                                   FROM console_graph_source_branch_history AS replacement \
                                   WHERE replacement.branch_name = history.branch_name \
                                     AND replacement.source_revision > \
                                         history.source_revision \
                                     AND replacement.source_revision <= ? \
                               ) \
                             ORDER BY history.source_revision, history.branch_name LIMIT ? \
                         )",
                    )
                    .bind::<BigInt, _>(low_watermark)
                    .bind::<BigInt, _>(low_watermark)
                    .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
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

    pub(crate) async fn prune_published_orphans(&self) -> crate::Result<()> {
        loop {
            if self.prune_published_orphans_step().await? {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    async fn prune_published_orphans_step(&self) -> crate::Result<bool> {
        let path = self.path.clone();
        self.database
            .with_write_connection("advance source orphan gc", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let queued_node = diesel::sql_query(
                            "SELECT node_id \
                             FROM console_graph_source_orphan_gc_queue \
                             ORDER BY node_id LIMIT 1",
                        )
                        .get_result::<NodeIdRow>(connection)
                        .optional()?
                        .map(|row| row.node_id);
                        if let Some(node_id) = queued_node {
                            if source_node_is_gc_protected(connection, &node_id)? {
                                diesel::sql_query(
                                    "DELETE FROM console_graph_source_orphan_gc_queue \
                                     WHERE node_id = ?",
                                )
                                .bind::<Text, _>(&node_id)
                                .execute(connection)?;
                                return Ok(false);
                            }

                            diesel::sql_query(
                                "DELETE FROM console_graph_source_node_relations \
                                 WHERE rowid IN ( \
                                     SELECT rowid \
                                     FROM console_graph_source_node_relations \
                                     WHERE parent_id = ? \
                                     ORDER BY child_id LIMIT ? \
                                 )",
                            )
                            .bind::<Text, _>(&node_id)
                            .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                            .execute(connection)?;
                            diesel::sql_query(
                                "DELETE FROM console_graph_source_node_relations \
                                 WHERE rowid IN ( \
                                     SELECT rowid \
                                     FROM console_graph_source_node_relations \
                                     WHERE child_id = ? \
                                     ORDER BY parent_id LIMIT ? \
                                 )",
                            )
                            .bind::<Text, _>(&node_id)
                            .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                            .execute(connection)?;
                            diesel::sql_query(
                                "DELETE FROM console_graph_source_branch_nodes \
                                 WHERE rowid IN ( \
                                     SELECT rowid \
                                     FROM console_graph_source_branch_nodes \
                                     WHERE node_id = ? \
                                     ORDER BY branch_name, contribution_generation LIMIT ? \
                                 )",
                            )
                            .bind::<Text, _>(&node_id)
                            .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                            .execute(connection)?;
                            diesel::sql_query(
                                "DELETE FROM console_graph_source_child_rechecks \
                                 WHERE rowid IN ( \
                                     SELECT rowid \
                                     FROM console_graph_source_child_rechecks \
                                     WHERE node_id = ? \
                                     ORDER BY branch_name, contribution_generation, \
                                              traversal_kind LIMIT ? \
                                 )",
                            )
                            .bind::<Text, _>(&node_id)
                            .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                            .execute(connection)?;
                            diesel::sql_query(
                                "DELETE FROM console_graph_source_refresh_queue \
                                 WHERE rowid IN ( \
                                     SELECT rowid \
                                     FROM console_graph_source_refresh_queue \
                                     WHERE node_id = ? \
                                     ORDER BY refresh_id, traversal_kind LIMIT ? \
                                 )",
                            )
                            .bind::<Text, _>(&node_id)
                            .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                            .execute(connection)?;

                            let has_references = diesel::sql_query(
                                "SELECT CASE WHEN \
                                     EXISTS ( \
                                         SELECT 1 \
                                         FROM console_graph_source_node_relations \
                                         WHERE parent_id = ? OR child_id = ? \
                                     ) OR EXISTS ( \
                                         SELECT 1 \
                                         FROM console_graph_source_branch_nodes \
                                         WHERE node_id = ? \
                                     ) OR EXISTS ( \
                                         SELECT 1 \
                                         FROM console_graph_source_child_rechecks \
                                         WHERE node_id = ? \
                                     ) OR EXISTS ( \
                                         SELECT 1 \
                                         FROM console_graph_source_refresh_queue \
                                         WHERE node_id = ? \
                                     ) THEN 1 ELSE 0 END AS value",
                            )
                            .bind::<Text, _>(&node_id)
                            .bind::<Text, _>(&node_id)
                            .bind::<Text, _>(&node_id)
                            .bind::<Text, _>(&node_id)
                            .bind::<Text, _>(&node_id)
                            .get_result::<FlagRow>(connection)?
                            .value
                                != 0;
                            if !has_references {
                                diesel::delete(
                                    console_graph_source_nodes::table
                                        .filter(console_graph_source_nodes::node_id.eq(&node_id)),
                                )
                                .execute(connection)?;
                                diesel::sql_query(
                                    "DELETE FROM console_graph_source_orphan_gc_queue \
                                     WHERE node_id = ?",
                                )
                                .bind::<Text, _>(&node_id)
                                .execute(connection)?;
                            }
                            return Ok(false);
                        }

                        let state = diesel::sql_query(
                            "SELECT scan_cursor, scan_upper_bound \
                             FROM console_graph_source_orphan_gc_state WHERE id = 1",
                        )
                        .get_result::<OrphanGcStateRow>(connection)?;
                        let scan_upper_bound = match state.scan_upper_bound {
                            Some(upper_bound) => upper_bound,
                            None => {
                                let upper_bound = diesel::sql_query(
                                    "SELECT MAX(node_id) AS node_id \
                                     FROM console_graph_source_nodes",
                                )
                                .get_result::<NullableNodeIdRow>(connection)?
                                .node_id;
                                let Some(upper_bound) = upper_bound else {
                                    return Ok(true);
                                };
                                diesel::sql_query(
                                    "UPDATE console_graph_source_orphan_gc_state \
                                     SET scan_cursor = NULL, scan_upper_bound = ? \
                                     WHERE id = 1",
                                )
                                .bind::<Text, _>(&upper_bound)
                                .execute(connection)?;
                                upper_bound
                            }
                        };
                        let scan_cursor = state.scan_cursor;
                        let page = diesel::sql_query(
                            "SELECT node_id \
                             FROM console_graph_source_nodes \
                             WHERE (? IS NULL OR node_id > ?) \
                               AND node_id <= ? \
                             ORDER BY node_id LIMIT ?",
                        )
                        .bind::<Nullable<Text>, _>(scan_cursor.clone())
                        .bind::<Nullable<Text>, _>(scan_cursor)
                        .bind::<Text, _>(&scan_upper_bound)
                        .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                        .load::<NodeIdRow>(connection)?;
                        let Some(last_node_id) = page.last().map(|row| row.node_id.clone()) else {
                            diesel::sql_query(
                                "UPDATE console_graph_source_orphan_gc_state \
                                 SET scan_cursor = NULL, scan_upper_bound = NULL \
                                 WHERE id = 1",
                            )
                            .execute(connection)?;
                            return Ok(true);
                        };
                        for row in page {
                            if !source_node_is_gc_protected(connection, &row.node_id)? {
                                diesel::sql_query(
                                    "INSERT OR IGNORE INTO \
                                         console_graph_source_orphan_gc_queue (node_id) \
                                     VALUES (?)",
                                )
                                .bind::<Text, _>(&row.node_id)
                                .execute(connection)?;
                            }
                        }
                        diesel::sql_query(
                            "UPDATE console_graph_source_orphan_gc_state \
                             SET scan_cursor = ? WHERE id = 1",
                        )
                        .bind::<Text, _>(last_node_id)
                        .execute(connection)?;
                        Ok(false)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    #[cfg(test)]
    pub(crate) fn refresh_count(&self) -> usize {
        self.refresh_count
    }

    #[cfg(test)]
    pub(crate) fn branch_refresh_count(&self) -> usize {
        self.branch_refresh_count
    }

    #[cfg(test)]
    pub(crate) fn branch_refresh_history(&self) -> &[String] {
        &self.branch_refresh_history
    }

    #[cfg(test)]
    pub(crate) fn fail_next_branch_refresh(&mut self, name: impl Into<String>) {
        self.fail_next_branch_refresh = Some(name.into());
    }

    #[cfg(test)]
    pub(crate) fn set_targeted_dynamic_branch_limit(&mut self, limit: usize) {
        self.targeted_dynamic_branch_limit = limit;
    }

    #[cfg(test)]
    pub(crate) fn set_full_refresh_branch_page_size(&mut self, page_size: NonZeroUsize) {
        self.full_refresh_branch_page_size = page_size;
    }

    #[cfg(test)]
    pub(crate) fn full_refresh_source_page_count(&self) -> usize {
        self.full_refresh_source_page_count
    }

    #[cfg(test)]
    fn fail_next_child_recheck_page_checkpoint(&mut self) {
        self.fail_next_child_recheck_page_checkpoint = true;
    }

    #[cfg(test)]
    fn child_recheck_page_cursors(&self) -> &[Option<GraphChildPageCursor>] {
        &self.child_recheck_page_cursors
    }

    #[cfg(test)]
    pub(crate) fn traversed_node_count(&self) -> usize {
        self.traversed_node_count
    }

    #[cfg(test)]
    async fn published_branch_node_ids(&self, name: &str) -> crate::Result<BTreeSet<String>> {
        let published = self.published_branch(name).await?.with_context(|| {
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "branch_name",
                value: name.to_owned(),
            }
        })?;
        let name = name.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT node_id FROM console_graph_source_current_branch_nodes \
                     WHERE branch_name = ? AND contribution_generation = ?",
                )
                .bind::<Text, _>(name)
                .bind::<BigInt, _>(published.contribution_generation)
                .load::<NodeIdRow>(connection)
                .map(|rows| rows.into_iter().map(|row| row.node_id).collect())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    #[cfg(test)]
    pub(crate) async fn node_count(&self) -> crate::Result<usize> {
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                console_graph_source_nodes::table
                    .count()
                    .get_result::<i64>(connection)
                    .map(|count| count.max(0) as usize)
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }
}

fn needs_children(traversal: TraversalKind, node: &Node) -> bool {
    traversal == TraversalKind::SkillSubtree || node.kind.as_tool_uses().is_some()
}

fn optional_queue_item(
    node_id: Option<String>,
    traversal: Option<String>,
    column: &'static str,
) -> crate::Result<Option<QueueItem>> {
    match (node_id, traversal) {
        (None, None) => Ok(None),
        (Some(node_id), Some(traversal)) => Ok(Some(QueueItem {
            node_id,
            traversal: TraversalKind::parse(&traversal)?,
        })),
        (node_id, traversal) => crate::error::InvalidGraphSnapshotStoreValueSnafu {
            column,
            value: format!("{node_id:?}/{traversal:?}"),
        }
        .fail(),
    }
}

fn optional_child_cursor(
    relation_revision: Option<i64>,
    node_id: Option<String>,
    column: &'static str,
) -> crate::Result<Option<GraphChildPageCursor>> {
    match (relation_revision, node_id) {
        (None, None) => Ok(None),
        (Some(relation_revision), Some(node_id)) => Ok(Some(GraphChildPageCursor {
            relation_revision,
            node_id,
        })),
        (relation_revision, node_id) => crate::error::InvalidGraphSnapshotStoreValueSnafu {
            column,
            value: format!("{relation_revision:?}/{node_id:?}"),
        }
        .fail(),
    }
}

fn optional_child_recheck_raw_cursor(
    node_id: Option<String>,
    traversal: Option<String>,
    refresh_id: Option<i64>,
    column: &'static str,
) -> crate::Result<Option<ChildRecheckRawCursor>> {
    match (node_id, traversal, refresh_id) {
        (None, None, None) => Ok(None),
        (Some(node_id), Some(traversal), Some(refresh_id)) => Ok(Some(ChildRecheckRawCursor {
            item: QueueItem {
                node_id,
                traversal: TraversalKind::parse(&traversal)?,
            },
            refresh_id,
        })),
        (node_id, traversal, refresh_id) => crate::error::InvalidGraphSnapshotStoreValueSnafu {
            column,
            value: format!("{node_id:?}/{traversal:?}/{refresh_id:?}"),
        }
        .fail(),
    }
}

fn dynamic_branch_scan_from_row(row: DynamicBranchScanRow) -> crate::Result<DynamicBranchScan> {
    let origin_raw_cursor = match (
        row.origin_raw_node_cursor,
        row.origin_raw_traversal_cursor,
        row.origin_raw_refresh_id_cursor,
    ) {
        (None, None, None) => None,
        (Some(node_id), Some(traversal), Some(refresh_id)) => Some(DynamicOriginRawCursor {
            node_id,
            traversal: TraversalKind::parse(&traversal)?,
            refresh_id,
        }),
        (node_id, traversal, refresh_id) => {
            return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "dynamic_scan_origin_raw_cursor",
                value: format!("{node_id:?}/{traversal:?}/{refresh_id:?}"),
            }
            .fail();
        }
    };
    let candidate_raw_cursor = match (
        row.candidate_raw_branch_cursor,
        row.candidate_raw_refresh_id_cursor,
        row.candidate_raw_traversal_cursor,
    ) {
        (None, None, None) => None,
        (Some(branch_name), Some(refresh_id), Some(traversal)) => Some(DynamicBranchRawCursor {
            branch_name,
            refresh_id,
            traversal: TraversalKind::parse(&traversal)?,
        }),
        (branch_name, refresh_id, traversal) => {
            return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "dynamic_scan_candidate_raw_cursor",
                value: format!("{branch_name:?}/{refresh_id:?}/{traversal:?}"),
            }
            .fail();
        }
    };
    ensure!(
        row.active_origin_node_id.is_none() || row.active_origin_branch_name.is_some(),
        crate::error::InvalidGraphSnapshotStoreValueSnafu {
            column: "dynamic_scan_active_origin",
            value: format!(
                "{:?}/{:?}",
                row.active_origin_branch_name, row.active_origin_node_id
            ),
        }
    );
    Ok(DynamicBranchScan {
        scan_id: row.scan_id,
        scan_kind: row.scan_kind,
        request_key: row.request_key,
        source_revision: row.source_revision,
        raw_refresh_id_upper_bound: row.raw_refresh_id_upper_bound,
        status: row.status,
        origin_branch_cursor: row.origin_branch_cursor,
        active_origin_branch_name: row.active_origin_branch_name,
        origin_raw_cursor,
        completed_origin_node_id: row.completed_origin_node_id,
        active_origin_node_id: row.active_origin_node_id,
        candidate_raw_cursor,
        result_count: row.result_count,
        exceeded_limit: row.exceeded_limit != 0,
        owner_id: row.owner_id,
        lease_epoch: row.lease_epoch,
    })
}

fn branch_name_batch_after(names: &BTreeSet<String>, after: Option<&str>) -> Vec<String> {
    if let Some(after) = after {
        names
            .range((Excluded(after.to_owned()), Unbounded))
            .take(GRAPH_READ_BATCH_SIZE)
            .cloned()
            .collect()
    } else {
        names.iter().take(GRAPH_READ_BATCH_SIZE).cloned().collect()
    }
}

fn required_node<'a>(
    nodes: &'a HashMap<String, Node>,
    column: &'static str,
    node_id: &str,
) -> crate::Result<&'a Node> {
    nodes
        .get(node_id)
        .with_context(|| crate::error::InvalidGraphSnapshotStoreValueSnafu {
            column,
            value: node_id.to_owned(),
        })
}

fn graph_parent_id_page(node: &Node, cursor_offset: i64) -> (Vec<String>, i64, bool) {
    let offset = usize::try_from(cursor_offset.max(0)).unwrap_or(usize::MAX);
    let merge_parents = match &node.kind {
        Kind::Anchor(anchor) => Some(anchor.merge_parents()),
        _ => None,
    };
    let mut page = std::iter::once(node.parent.as_str())
        .filter(|parent_id| !parent_id.is_empty())
        .chain(
            merge_parents
                .into_iter()
                .flatten()
                .map(|parent| parent.node_id()),
        )
        .skip(offset)
        .take(SOURCE_PARENT_PAGE_SIZE.saturating_add(1))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let complete = page.len() <= SOURCE_PARENT_PAGE_SIZE;
    if !complete {
        page.truncate(SOURCE_PARENT_PAGE_SIZE);
    }
    let next_offset = cursor_offset.max(0).saturating_add(page.len() as i64);
    (page, next_offset, complete)
}

fn graph_parent_traversal_page(
    node: &Node,
    cursor_offset: i64,
) -> (Vec<(String, TraversalKind)>, i64, bool) {
    let (parent_ids, next_offset, complete) = graph_parent_id_page(node, cursor_offset);
    (
        parent_ids
            .into_iter()
            .map(|parent_id| (parent_id, TraversalKind::Graph))
            .collect(),
        next_offset,
        complete,
    )
}

fn child_traversals_for_page(
    item: &QueueItem,
    child_ids: &[String],
    child_nodes: &HashMap<String, Node>,
) -> crate::Result<Vec<(String, TraversalKind)>> {
    match item.traversal {
        TraversalKind::SkillSubtree => Ok(child_ids
            .iter()
            .cloned()
            .map(|child_id| (child_id, TraversalKind::SkillSubtree))
            .collect()),
        TraversalKind::Graph => skill_invocation_traversals(child_ids, child_nodes),
    }
}

fn skill_invocation_traversals(
    child_ids: &[String],
    child_nodes: &HashMap<String, Node>,
) -> crate::Result<Vec<(String, TraversalKind)>> {
    let mut traversals = Vec::new();
    for child_id in child_ids {
        let child = required_node(child_nodes, "child_id", child_id)?;
        if matches!(
            &child.kind,
            Kind::Anchor(anchor) if anchor.as_skill_invocation().is_some()
        ) {
            traversals.push((child_id.clone(), TraversalKind::SkillSubtree));
        }
    }
    Ok(traversals)
}

impl TryFrom<&Node> for PersistedSourceNode {
    type Error = crate::Error;

    fn try_from(node: &Node) -> Result<Self, Self::Error> {
        let node_json =
            serde_json::to_string(node).context(SerializeGraphSnapshotStoreValueSnafu {
                column: "node_json",
            })?;
        Ok(Self {
            node_id: node.id.clone(),
            parent_id: node.parent.clone(),
            node_json,
        })
    }
}

impl PersistentGraphStore {
    fn read_only<T>(&self) -> StoreResult<T> {
        Err(StoreError::StoreReadOnly {
            path: self.path.clone(),
        })
    }

    fn map_error(&self, error: crate::Error) -> StoreError {
        StoreError::CorruptedStore {
            path: self.path.clone(),
            message: error.to_string(),
        }
    }

    async fn resolve_node(&self, reference: &str) -> StoreResult<Node> {
        let reference = reference.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                let node_id = console_graph_source_branches::table
                    .filter(console_graph_source_branches::name.eq(&reference))
                    .select(console_graph_source_branches::head_id)
                    .first::<String>(connection)
                    .optional()
                    .context(QueryGraphSnapshotStoreSnafu { path: path.clone() })?
                    .unwrap_or(reference);
                let value = console_graph_source_nodes::table
                    .filter(console_graph_source_nodes::node_id.eq(&node_id))
                    .select(console_graph_source_nodes::node_json)
                    .first::<String>(connection)
                    .optional()
                    .context(QueryGraphSnapshotStoreSnafu { path })?
                    .with_context(|| crate::error::InvalidGraphSnapshotStoreValueSnafu {
                        column: "node_id",
                        value: node_id,
                    })?;
                serde_json::from_str(&value).context(ParseGraphSnapshotStoreValueSnafu {
                    column: "node_json",
                })
            })
            .await
            .map_err(|error| self.map_error(error))
    }
}

#[async_trait]
impl NodeStore for PersistentGraphStore {
    fn root_id(&self) -> String {
        self.root_id.clone()
    }

    async fn append(&self, _node: NewNode) -> StoreResult<String> {
        self.read_only()
    }

    async fn ancestry(&self, head_ref: &str) -> StoreResult<Vec<Node>> {
        let head_ref = head_ref.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                let mut node_id = console_graph_source_branches::table
                    .filter(console_graph_source_branches::name.eq(&head_ref))
                    .select(console_graph_source_branches::head_id)
                    .first::<String>(connection)
                    .optional()
                    .context(QueryGraphSnapshotStoreSnafu { path: path.clone() })?
                    .unwrap_or(head_ref);
                let mut ancestry = Vec::new();
                let mut seen = HashSet::new();
                loop {
                    ensure!(
                        seen.insert(node_id.clone()),
                        crate::error::InvalidGraphSnapshotStoreValueSnafu {
                            column: "node_id",
                            value: format!("cyclic parent chain at {node_id}"),
                        }
                    );
                    let value = console_graph_source_nodes::table
                        .filter(console_graph_source_nodes::node_id.eq(&node_id))
                        .select(console_graph_source_nodes::node_json)
                        .first::<String>(connection)
                        .optional()
                        .context(QueryGraphSnapshotStoreSnafu { path: path.clone() })?
                        .with_context(|| crate::error::InvalidGraphSnapshotStoreValueSnafu {
                            column: "node_id",
                            value: node_id.clone(),
                        })?;
                    let node = serde_json::from_str::<Node>(&value).context(
                        ParseGraphSnapshotStoreValueSnafu {
                            column: "node_json",
                        },
                    )?;
                    let is_root = node.is_root();
                    node_id.clone_from(&node.parent);
                    ancestry.push(node);
                    if is_root {
                        return Ok(ancestry);
                    }
                }
            })
            .await
            .map_err(|error| self.map_error(error))
    }

    async fn log(&self, base_ref: &str, head_ref: &str) -> StoreResult<Vec<Node>> {
        let base = self.resolve_node(base_ref).await?.id;
        let mut ancestry = self.ancestry(head_ref).await?;
        let index = ancestry
            .iter()
            .position(|node| node.id == base)
            .ok_or_else(|| StoreError::RefsNotConnected {
                base_ref: base_ref.to_owned(),
                head_ref: head_ref.to_owned(),
            })?;
        ancestry.truncate(index + 1);
        Ok(ancestry)
    }

    async fn get_node(&self, id: &str) -> StoreResult<Node> {
        self.resolve_node(id).await
    }

    async fn list_children(&self, node_id: &str) -> StoreResult<Vec<Node>> {
        self.resolve_node(node_id).await?;
        let node_id = node_id.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                let rows = diesel::sql_query(
                    "SELECT DISTINCT nodes.node_json \
                     FROM console_graph_source_node_relations AS relations \
                     INNER JOIN console_graph_source_nodes AS nodes \
                         ON nodes.node_id = relations.child_id \
                     INNER JOIN console_graph_source_current_branch_nodes AS branch_nodes \
                         ON branch_nodes.node_id = nodes.node_id \
                     WHERE relations.parent_id = ? \
                     ORDER BY nodes.node_id",
                )
                .bind::<Text, _>(node_id)
                .load::<NodeJsonRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })?;
                rows.into_iter()
                    .map(|row| {
                        serde_json::from_str(&row.node_json).context(
                            ParseGraphSnapshotStoreValueSnafu {
                                column: "node_json",
                            },
                        )
                    })
                    .collect()
            })
            .await
            .map_err(|error| self.map_error(error))
    }
}

#[async_trait]
impl BranchStore for PersistentGraphStore {
    async fn fork(&self, _name: &str, _from_ref: &str) -> StoreResult<String> {
        self.read_only()
    }

    async fn get_branch_head(&self, name: &str) -> StoreResult<String> {
        let name = name.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                console_graph_source_branches::table
                    .filter(console_graph_source_branches::name.eq(&name))
                    .select(console_graph_source_branches::head_id)
                    .first::<String>(connection)
                    .optional()
                    .context(QueryGraphSnapshotStoreSnafu { path })?
                    .with_context(|| crate::error::InvalidGraphSnapshotStoreValueSnafu {
                        column: "branch_name",
                        value: name,
                    })
            })
            .await
            .map_err(|error| self.map_error(error))
    }

    async fn delete_branch(&self, _name: &str) -> StoreResult<()> {
        self.read_only()
    }

    async fn set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _new_head: &str,
    ) -> StoreResult<()> {
        self.read_only()
    }

    async fn append_nodes_and_set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _parent: &str,
        _nodes: Vec<NewNodeContent>,
    ) -> StoreResult<String> {
        self.read_only()
    }

    async fn append_nodes_and_set_branch_head_to(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _parent: &str,
        _new_head: &str,
        _nodes: Vec<NewNodeContent>,
    ) -> StoreResult<String> {
        self.read_only()
    }

    async fn append_nodes_and_set_branch_head_with_session_state(
        &self,
        _update: BranchAppendSessionState,
    ) -> StoreResult<String> {
        self.read_only()
    }
}

#[async_trait]
impl SessionStore for PersistentGraphStore {
    async fn list_session_states(&self) -> StoreResult<HashMap<String, SessionState>> {
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                let rows = diesel::sql_query(
                    "SELECT name, state_json \
                     FROM console_graph_source_branches \
                     ORDER BY name",
                )
                .load::<BranchRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })?;
                rows.into_iter()
                    .map(|row| {
                        let state = serde_json::from_str(&row.state_json).context(
                            ParseGraphSnapshotStoreValueSnafu {
                                column: "state_json",
                            },
                        )?;
                        Ok((row.name, state))
                    })
                    .collect()
            })
            .await
            .map_err(|error| self.map_error(error))
    }

    async fn get_session_state(&self, name: &str) -> StoreResult<SessionState> {
        let name = name.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                let value = console_graph_source_branches::table
                    .filter(console_graph_source_branches::name.eq(&name))
                    .select(console_graph_source_branches::state_json)
                    .first::<String>(connection)
                    .optional()
                    .context(QueryGraphSnapshotStoreSnafu { path })?
                    .with_context(|| crate::error::InvalidGraphSnapshotStoreValueSnafu {
                        column: "branch_name",
                        value: name,
                    })?;
                serde_json::from_str(&value).context(ParseGraphSnapshotStoreValueSnafu {
                    column: "state_json",
                })
            })
            .await
            .map_err(|error| self.map_error(error))
    }

    async fn set_session_state(
        &self,
        _name: &str,
        _expected: Option<&SessionState>,
        _next: SessionState,
    ) -> StoreResult<SessionState> {
        self.read_only()
    }

    async fn rebase_session(
        &self,
        _name: &str,
        _patch: &SessionAnchorPatch,
    ) -> StoreResult<String> {
        self.read_only()
    }

    async fn handoff_session(
        &self,
        _name: &str,
        _patch: &SessionAnchorPatch,
        _prompt: &str,
    ) -> StoreResult<String> {
        self.read_only()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{GraphMode, build_graph_snapshot_with_mode};
    use crate::host::incremental_build::build_incremental_generation;
    use coco_mem::{
        Anchor, Kind, MergeParent, Role, SkillInvocationAnchor, SkillInvocationMode,
        SkillResultAnchor, SqliteStore, ToolUse,
    };
    use diesel::connection::SimpleConnection;

    #[derive(Debug, QueryableByName)]
    struct OrphanGcReferenceCountRow {
        #[diesel(sql_type = BigInt)]
        outgoing_relations: i64,
        #[diesel(sql_type = BigInt)]
        incoming_relations: i64,
        #[diesel(sql_type = BigInt)]
        branch_memberships: i64,
        #[diesel(sql_type = BigInt)]
        child_rechecks: i64,
        #[diesel(sql_type = BigInt)]
        refresh_work_items: i64,
    }

    #[derive(Debug, QueryableByName)]
    struct TestCountRow {
        #[diesel(sql_type = BigInt)]
        count: i64,
    }

    #[derive(Debug, QueryableByName)]
    struct TestDynamicScanFenceRow {
        #[diesel(sql_type = Text)]
        status: String,
        #[diesel(sql_type = BigInt)]
        lease_epoch: i64,
    }

    #[derive(Debug, QueryableByName)]
    struct TestSourceWorkCountRow {
        #[diesel(sql_type = BigInt)]
        boundaries: i64,
        #[diesel(sql_type = BigInt)]
        sweeps: i64,
        #[diesel(sql_type = BigInt)]
        refreshes: i64,
    }

    #[derive(Debug, QueryableByName)]
    struct ExplainQueryPlanRow {
        #[diesel(sql_type = Text)]
        detail: String,
    }

    async fn seed_mutation_event_run(index: &PersistentGraphIndex, revision: i64) {
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed source mutation event run", move |connection| {
                diesel::sql_query(
                    "INSERT OR IGNORE INTO console_graph_source_mutation_event_runs ( \
                         revision, phase \
                     ) VALUES (?, 'dirty_parents')",
                )
                .bind::<BigInt, _>(revision)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
    }

    async fn dynamic_scan_result_count(index: &PersistentGraphIndex, scan_id: i64) -> i64 {
        let path = index.path.clone();
        index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count \
                     FROM console_graph_source_dynamic_branch_scan_results \
                     WHERE scan_id = ?",
                )
                .bind::<BigInt, _>(scan_id)
                .get_result::<TestCountRow>(connection)
                .map(|row| row.count)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap()
    }

    async fn dynamic_scan_count(index: &PersistentGraphIndex, scan_id: i64) -> i64 {
        let path = index.path.clone();
        index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count \
                     FROM console_graph_source_dynamic_branch_scans WHERE scan_id = ?",
                )
                .bind::<BigInt, _>(scan_id)
                .get_result::<TestCountRow>(connection)
                .map(|row| row.count)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap()
    }

    async fn dynamic_scan_fence(
        index: &PersistentGraphIndex,
        scan_id: i64,
    ) -> TestDynamicScanFenceRow {
        let path = index.path.clone();
        index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT status, lease_epoch \
                     FROM console_graph_source_dynamic_branch_scans WHERE scan_id = ?",
                )
                .bind::<BigInt, _>(scan_id)
                .get_result::<TestDynamicScanFenceRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap()
    }

    async fn seed_building_source_work(
        index: &PersistentGraphIndex,
        invalidation_incarnation: &str,
        count: usize,
        first_relation_revision: i64,
        second_relation_revision: i64,
        first_refresh_id: i64,
    ) {
        let invalidation_incarnation = invalidation_incarnation.to_owned();
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed building source work", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "WITH RECURSIVE sequence(offset) AS ( \
                                 SELECT 0 \
                                 UNION ALL \
                                 SELECT offset + 1 FROM sequence WHERE offset + 1 < ? \
                             ) \
                             INSERT INTO console_graph_source_invalidation_boundaries ( \
                                 target_invalidation_incarnation, \
                                 target_invalidation_version, relation_revision, \
                                 requested_scope, status, source_revision, \
                                 changed_branch_count, dirty_parent_count \
                             ) \
                             SELECT ?, offset + 1, \
                                    CASE WHEN offset % 2 = 0 THEN ? ELSE ? END, \
                                    'targeted', 'building', NULL, 0, 0 \
                             FROM sequence",
                        )
                        .bind::<BigInt, _>(count as i64)
                        .bind::<Text, _>(&invalidation_incarnation)
                        .bind::<BigInt, _>(first_relation_revision)
                        .bind::<BigInt, _>(second_relation_revision)
                        .execute(connection)?;
                        diesel::sql_query(
                            "WITH RECURSIVE sequence(offset) AS ( \
                                 SELECT 0 \
                                 UNION ALL \
                                 SELECT offset + 1 FROM sequence WHERE offset + 1 < ? \
                             ) \
                             INSERT INTO console_graph_source_sweep_runs ( \
                                 target_invalidation_incarnation, \
                                 target_invalidation_version, relation_revision, status, \
                                 phase, source_upper_bound, page_cursor, owner_id, \
                                 lease_epoch, lease_expires_at_ms \
                             ) \
                             SELECT ?, offset + 1, \
                                    CASE WHEN offset % 2 = 0 THEN ? ELSE ? END, \
                                    'building', 'enumerate', NULL, NULL, 'test-owner', 1, 0 \
                             FROM sequence",
                        )
                        .bind::<BigInt, _>(count as i64)
                        .bind::<Text, _>(&invalidation_incarnation)
                        .bind::<BigInt, _>(first_relation_revision)
                        .bind::<BigInt, _>(second_relation_revision)
                        .execute(connection)?;
                        diesel::sql_query(
                            "WITH RECURSIVE sequence(offset) AS ( \
                                 SELECT 0 \
                                 UNION ALL \
                                 SELECT offset + 1 FROM sequence WHERE offset + 1 < ? \
                             ) \
                             INSERT INTO console_graph_source_refresh_runs ( \
                                 refresh_id, branch_name, target_head_id, target_state_json, \
                                 status, target_contribution_generation, owner_id, lease_epoch, \
                                 lease_expires_at_ms, target_invalidation_version, \
                                 target_invalidation_incarnation, relation_revision \
                             ) \
                             SELECT ? + offset, printf('%s-refresh-%04d', ?, offset), \
                                    printf('%s-head-%04d', ?, offset), '{}', 'building', \
                                    ? + offset, 'test-owner', 1, 0, offset + 1, ?, \
                                    CASE WHEN offset % 2 = 0 THEN ? ELSE ? END \
                             FROM sequence",
                        )
                        .bind::<BigInt, _>(count as i64)
                        .bind::<BigInt, _>(first_refresh_id)
                        .bind::<Text, _>(&invalidation_incarnation)
                        .bind::<Text, _>(&invalidation_incarnation)
                        .bind::<BigInt, _>(first_refresh_id)
                        .bind::<Text, _>(&invalidation_incarnation)
                        .bind::<BigInt, _>(first_relation_revision)
                        .bind::<BigInt, _>(second_relation_revision)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
    }

    async fn building_source_work_counts(
        index: &PersistentGraphIndex,
        invalidation_incarnation: &str,
    ) -> SourceWorkSupersedeCounts {
        let invalidation_incarnation = invalidation_incarnation.to_owned();
        let path = index.path.clone();
        index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT \
                         (SELECT COUNT(*) \
                          FROM console_graph_source_invalidation_boundaries \
                          WHERE target_invalidation_incarnation = ? \
                            AND status = 'building') AS boundaries, \
                         (SELECT COUNT(*) \
                          FROM console_graph_source_sweep_runs \
                          WHERE target_invalidation_incarnation = ? \
                            AND status = 'building') AS sweeps, \
                         (SELECT COUNT(*) \
                          FROM console_graph_source_refresh_runs \
                          WHERE target_invalidation_incarnation = ? \
                            AND status = 'building') AS refreshes",
                )
                .bind::<Text, _>(&invalidation_incarnation)
                .bind::<Text, _>(&invalidation_incarnation)
                .bind::<Text, _>(&invalidation_incarnation)
                .get_result::<TestSourceWorkCountRow>(connection)
                .map(|row| SourceWorkSupersedeCounts {
                    boundaries: row.boundaries as usize,
                    sweeps: row.sweeps as usize,
                    refreshes: row.refreshes as usize,
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap()
    }

    async fn mutation_event_run_count(
        index: &PersistentGraphIndex,
        first_revision: i64,
        last_revision: i64,
    ) -> i64 {
        let path = index.path.clone();
        index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count \
                     FROM console_graph_source_mutation_event_runs \
                     WHERE revision BETWEEN ? AND ?",
                )
                .bind::<BigInt, _>(first_revision)
                .bind::<BigInt, _>(last_revision)
                .get_result::<TestCountRow>(connection)
                .map(|row| row.count)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap()
    }

    async fn append_text_chain(store: &SqliteStore, parent: &str, count: usize) -> Vec<String> {
        let mut parent = parent.to_owned();
        let mut node_ids = Vec::with_capacity(count);
        for index in 0..count {
            parent = store
                .append(NewNode {
                    parent,
                    role: Role::User,
                    metadata: None,
                    kind: Kind::Text(format!("chain node {index}")),
                })
                .await
                .unwrap();
            node_ids.push(parent.clone());
        }
        node_ids
    }

    fn durable_batch(version: u64, full: bool) -> ConsoleInvalidationBatch {
        ConsoleInvalidationBatch { version, full }
    }

    async fn seed_ineligible_dynamic_rechecks(
        index: &PersistentGraphIndex,
        node_id: &str,
        source_revision: i64,
        first_refresh_id: i64,
        count: usize,
    ) {
        let node_id = node_id.to_owned();
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed ineligible dynamic rechecks", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "WITH RECURSIVE sequence(offset) AS ( \
                                 SELECT 0 \
                                 UNION ALL \
                                 SELECT offset + 1 FROM sequence WHERE offset + 1 < ? \
                             ) \
                             INSERT INTO console_graph_source_refresh_runs ( \
                                 refresh_id, branch_name, target_head_id, target_state_json, \
                                 target_contribution_generation, status, \
                                 published_source_revision \
                             ) \
                             SELECT ? + offset, printf('aa-candidate-%06d', offset), \
                                    'missing-head', '{}', ? + offset, 'published', ? \
                             FROM sequence",
                        )
                        .bind::<BigInt, _>(count as i64)
                        .bind::<BigInt, _>(first_refresh_id)
                        .bind::<BigInt, _>(first_refresh_id)
                        .bind::<BigInt, _>(source_revision)
                        .execute(connection)?;
                        diesel::sql_query(
                            "WITH RECURSIVE sequence(offset) AS ( \
                                 SELECT 0 \
                                 UNION ALL \
                                 SELECT offset + 1 FROM sequence WHERE offset + 1 < ? \
                             ) \
                             INSERT INTO console_graph_source_child_rechecks ( \
                                 branch_name, contribution_generation, node_id, traversal_kind \
                             ) \
                             SELECT printf('aa-candidate-%06d', offset), ? + offset, ?, 'graph' \
                             FROM sequence",
                        )
                        .bind::<BigInt, _>(count as i64)
                        .bind::<BigInt, _>(first_refresh_id)
                        .bind::<Text, _>(&node_id)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
    }

    async fn source_refresh_run_count(index: &PersistentGraphIndex, refresh_id: i64) -> i64 {
        let path = index.path.clone();
        index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count FROM console_graph_source_refresh_runs \
                     WHERE refresh_id = ?",
                )
                .bind::<BigInt, _>(refresh_id)
                .get_result::<TestCountRow>(connection)
                .map(|row| row.count)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap()
    }

    async fn mark_current_source_revision_consumed(index: &PersistentGraphIndex) {
        let path = index.path.clone();
        index
            .database
            .with_write_connection("mark current source revision consumed", move |connection| {
                diesel::sql_query(
                    "INSERT INTO console_graph_generation_source_revisions ( \
                         generation, source_revision \
                     ) \
                     SELECT state.active_generation, identity.revision \
                     FROM console_graph_generation_state AS state \
                     CROSS JOIN console_graph_source_identity AS identity \
                     WHERE state.id = 1 AND identity.id = 1 \
                     ON CONFLICT(generation) DO UPDATE \
                     SET source_revision = excluded.source_revision",
                )
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
    }

    async fn source_refresh_cleanup_state(
        index: &PersistentGraphIndex,
    ) -> (Option<i64>, i64, Option<i64>) {
        let path = index.path.clone();
        index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT upper_bound_refresh_id, raw_refresh_id_cursor, \
                            active_refresh_id \
                     FROM console_graph_source_refresh_cleanup_state WHERE id = 1",
                )
                .get_result::<SourceRefreshCleanupStateRow>(connection)
                .map(|state| {
                    (
                        state.upper_bound_refresh_id,
                        state.raw_refresh_id_cursor,
                        state.active_refresh_id,
                    )
                })
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap()
    }

    async fn source_refresh_queue_count(index: &PersistentGraphIndex, refresh_id: i64) -> i64 {
        let path = index.path.clone();
        index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count FROM console_graph_source_refresh_queue \
                     WHERE refresh_id = ?",
                )
                .bind::<BigInt, _>(refresh_id)
                .get_result::<TestCountRow>(connection)
                .map(|row| row.count)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap()
    }

    async fn source_refresh_dirty_seed_count(index: &PersistentGraphIndex, refresh_id: i64) -> i64 {
        let path = index.path.clone();
        index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count \
                     FROM console_graph_source_refresh_dirty_seeds WHERE refresh_id = ?",
                )
                .bind::<BigInt, _>(refresh_id)
                .get_result::<TestCountRow>(connection)
                .map(|row| row.count)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap()
    }

    async fn source_refresh_run_range_count(
        index: &PersistentGraphIndex,
        first_refresh_id: i64,
        last_refresh_id: i64,
    ) -> i64 {
        let path = index.path.clone();
        index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count FROM console_graph_source_refresh_runs \
                     WHERE refresh_id BETWEEN ? AND ?",
                )
                .bind::<BigInt, _>(first_refresh_id)
                .bind::<BigInt, _>(last_refresh_id)
                .get_result::<TestCountRow>(connection)
                .map(|row| row.count)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap()
    }

    async fn seed_protected_source_refreshes(
        index: &PersistentGraphIndex,
        first_refresh_id: i64,
        count: usize,
    ) {
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed protected source refreshes", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "WITH RECURSIVE sequence(offset) AS ( \
                                 SELECT 0 \
                                 UNION ALL \
                                 SELECT offset + 1 FROM sequence WHERE offset + 1 < ? \
                             ) \
                             INSERT INTO console_graph_source_refresh_runs ( \
                                 refresh_id, branch_name, target_head_id, target_state_json, \
                                 status, target_contribution_generation, \
                                 published_source_revision \
                             ) \
                             SELECT ? + offset, printf('cleanup-protected-%d', offset), \
                                    printf('cleanup-head-%d', offset), '{}', 'published', \
                                    ? + offset, 1 \
                             FROM sequence",
                        )
                        .bind::<BigInt, _>(i64::try_from(count).unwrap_or(i64::MAX))
                        .bind::<BigInt, _>(first_refresh_id)
                        .bind::<BigInt, _>(first_refresh_id)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_publications ( \
                                 branch_name, target_contribution_generation, source_revision \
                             ) \
                             SELECT branch_name, target_contribution_generation, 1 \
                             FROM console_graph_source_refresh_runs \
                             WHERE refresh_id >= ? AND refresh_id < ?",
                        )
                        .bind::<BigInt, _>(first_refresh_id)
                        .bind::<BigInt, _>(
                            first_refresh_id
                                .saturating_add(i64::try_from(count).unwrap_or(i64::MAX)),
                        )
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
    }

    async fn seed_superseded_source_refresh(index: &PersistentGraphIndex, refresh_id: i64) {
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed superseded source refresh", move |connection| {
                diesel::sql_query(
                    "INSERT INTO console_graph_source_refresh_runs ( \
                         refresh_id, branch_name, target_head_id, target_state_json, \
                         status, target_contribution_generation \
                     ) VALUES (?, ?, ?, '{}', 'superseded', ?)",
                )
                .bind::<BigInt, _>(refresh_id)
                .bind::<Text, _>(format!("cleanup-stale-{refresh_id}"))
                .bind::<Text, _>(format!("cleanup-stale-head-{refresh_id}"))
                .bind::<BigInt, _>(refresh_id)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
    }

    async fn source_node_exists(index: &PersistentGraphIndex, node_id: &str) -> bool {
        let path = index.path.clone();
        let node_id = node_id.to_owned();
        index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT CASE WHEN EXISTS ( \
                         SELECT 1 FROM console_graph_source_nodes WHERE node_id = ? \
                     ) THEN 1 ELSE 0 END AS value",
                )
                .bind::<Text, _>(node_id)
                .get_result::<FlagRow>(connection)
                .map(|row| row.value != 0)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap()
    }

    fn graph_queue_item(node_id: &str) -> QueueItem {
        QueueItem {
            node_id: node_id.to_owned(),
            traversal: TraversalKind::Graph,
        }
    }

    #[tokio::test]
    async fn source_work_outside_revision_bounds_is_batched_and_resumes_after_reopen() {
        const ELIGIBLE_INCARNATION: &str = "outside-bounds-eligible";
        const RETAINED_INCARNATION: &str = "outside-bounds-retained";
        const BASELINE_REVISION: i64 = 10;
        const CURRENT_REVISION: i64 = 20;
        const ELIGIBLE_COUNT: usize = SOURCE_WORK_SUPERSEDE_BATCH_SIZE + 17;

        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        seed_building_source_work(
            &index,
            ELIGIBLE_INCARNATION,
            ELIGIBLE_COUNT,
            BASELINE_REVISION - 1,
            CURRENT_REVISION + 1,
            8_100_000,
        )
        .await;
        seed_building_source_work(
            &index,
            RETAINED_INCARNATION,
            2,
            BASELINE_REVISION,
            CURRENT_REVISION,
            8_200_000,
        )
        .await;

        assert_eq!(
            index
                .supersede_source_work_batch(BASELINE_REVISION, Some(CURRENT_REVISION))
                .await
                .unwrap(),
            SourceWorkSupersedeCounts {
                boundaries: SOURCE_WORK_SUPERSEDE_BATCH_SIZE,
                sweeps: SOURCE_WORK_SUPERSEDE_BATCH_SIZE,
                refreshes: SOURCE_WORK_SUPERSEDE_BATCH_SIZE,
            }
        );
        assert_eq!(
            building_source_work_counts(&index, ELIGIBLE_INCARNATION).await,
            SourceWorkSupersedeCounts {
                boundaries: 17,
                sweeps: 17,
                refreshes: 17,
            }
        );
        assert_eq!(
            building_source_work_counts(&index, RETAINED_INCARNATION).await,
            SourceWorkSupersedeCounts {
                boundaries: 2,
                sweeps: 2,
                refreshes: 2,
            }
        );
        drop(index);
        drop(snapshots);

        let reopened_snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let reopened = PersistentGraphIndex::open(&reopened_snapshots, root)
            .await
            .unwrap();
        reopened
            .supersede_source_work_outside_revision_bounds(BASELINE_REVISION, CURRENT_REVISION)
            .await
            .unwrap();
        assert_eq!(
            building_source_work_counts(&reopened, ELIGIBLE_INCARNATION).await,
            SourceWorkSupersedeCounts::default()
        );
        assert_eq!(
            building_source_work_counts(&reopened, RETAINED_INCARNATION).await,
            SourceWorkSupersedeCounts {
                boundaries: 2,
                sweeps: 2,
                refreshes: 2,
            }
        );
    }

    #[tokio::test]
    async fn source_work_before_revision_is_batched_and_resumes_after_reopen() {
        const ELIGIBLE_INCARNATION: &str = "before-revision-eligible";
        const RETAINED_INCARNATION: &str = "before-revision-retained";
        const MUTATION_REVISION: i64 = 20;
        const ELIGIBLE_COUNT: usize = SOURCE_WORK_SUPERSEDE_BATCH_SIZE + 17;

        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        seed_building_source_work(
            &index,
            ELIGIBLE_INCARNATION,
            ELIGIBLE_COUNT,
            MUTATION_REVISION - 2,
            MUTATION_REVISION - 1,
            8_300_000,
        )
        .await;
        seed_building_source_work(
            &index,
            RETAINED_INCARNATION,
            2,
            MUTATION_REVISION,
            MUTATION_REVISION + 1,
            8_400_000,
        )
        .await;

        assert_eq!(
            index
                .supersede_source_work_batch(MUTATION_REVISION, None)
                .await
                .unwrap(),
            SourceWorkSupersedeCounts {
                boundaries: SOURCE_WORK_SUPERSEDE_BATCH_SIZE,
                sweeps: SOURCE_WORK_SUPERSEDE_BATCH_SIZE,
                refreshes: SOURCE_WORK_SUPERSEDE_BATCH_SIZE,
            }
        );
        assert_eq!(
            building_source_work_counts(&index, ELIGIBLE_INCARNATION).await,
            SourceWorkSupersedeCounts {
                boundaries: 17,
                sweeps: 17,
                refreshes: 17,
            }
        );
        assert_eq!(
            building_source_work_counts(&index, RETAINED_INCARNATION).await,
            SourceWorkSupersedeCounts {
                boundaries: 2,
                sweeps: 2,
                refreshes: 2,
            }
        );
        drop(index);
        drop(snapshots);

        let reopened_snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let reopened = PersistentGraphIndex::open(&reopened_snapshots, root)
            .await
            .unwrap();
        reopened
            .supersede_source_work_before_revision(MUTATION_REVISION)
            .await
            .unwrap();
        assert_eq!(
            building_source_work_counts(&reopened, ELIGIBLE_INCARNATION).await,
            SourceWorkSupersedeCounts::default()
        );
        assert_eq!(
            building_source_work_counts(&reopened, RETAINED_INCARNATION).await,
            SourceWorkSupersedeCounts {
                boundaries: 2,
                sweeps: 2,
                refreshes: 2,
            }
        );
    }

    #[tokio::test]
    async fn mutation_event_run_deletion_is_batched_and_resumes_after_reopen() {
        const FIRST_REVISION: i64 = 8_500_000;
        const ELIGIBLE_COUNT: usize = SOURCE_WORK_SUPERSEDE_BATCH_SIZE + 17;
        const LAST_ELIGIBLE_REVISION: i64 = FIRST_REVISION + ELIGIBLE_COUNT as i64 - 1;
        const RETAINED_REVISION: i64 = LAST_ELIGIBLE_REVISION + 1;

        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed source mutation event runs", move |connection| {
                diesel::sql_query(
                    "WITH RECURSIVE sequence(offset) AS ( \
                         SELECT 0 \
                         UNION ALL \
                         SELECT offset + 1 FROM sequence WHERE offset + 1 <= ? \
                     ) \
                     INSERT INTO console_graph_source_mutation_event_runs (revision, phase) \
                     SELECT ? + offset, 'branch_changes' FROM sequence",
                )
                .bind::<BigInt, _>(ELIGIBLE_COUNT as i64)
                .bind::<BigInt, _>(FIRST_REVISION)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        assert_eq!(
            index
                .delete_mutation_event_run_batch(Some(LAST_ELIGIBLE_REVISION))
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            index
                .mutation_event_run(FIRST_REVISION)
                .await
                .unwrap()
                .unwrap()
                .phase,
            "discarding"
        );
        assert_eq!(
            mutation_event_run_count(&index, FIRST_REVISION, LAST_ELIGIBLE_REVISION,).await,
            ELIGIBLE_COUNT as i64
        );
        assert_eq!(
            mutation_event_run_count(&index, RETAINED_REVISION, RETAINED_REVISION).await,
            1
        );
        drop(index);
        drop(snapshots);

        let reopened_snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let reopened = PersistentGraphIndex::open(&reopened_snapshots, root)
            .await
            .unwrap();
        reopened
            .delete_mutation_event_runs_bounded(Some(LAST_ELIGIBLE_REVISION))
            .await
            .unwrap();
        assert_eq!(
            mutation_event_run_count(&reopened, FIRST_REVISION, LAST_ELIGIBLE_REVISION,).await,
            0
        );
        assert_eq!(
            mutation_event_run_count(&reopened, RETAINED_REVISION, RETAINED_REVISION).await,
            1
        );
    }

    #[tokio::test]
    async fn durable_source_applies_delete_and_recreate_as_distinct_revisions() {
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
        index
            .refresh_invalidation_batch(&source, &durable_batch(1, true))
            .await
            .unwrap();

        writer.delete_branch("main").await.unwrap();
        let delete_revision = source.graph_mutation_revision().await.unwrap();
        index
            .refresh_invalidation_batch(&source, &durable_batch(2, false))
            .await
            .unwrap();
        assert!(index.published_branch("main").await.unwrap().is_none());
        assert_eq!(
            index.consumed_graph_mutation_revision().await.unwrap(),
            delete_revision
        );

        writer.fork("main", &root).await.unwrap();
        let recreate_revision = source.graph_mutation_revision().await.unwrap();
        assert_eq!(recreate_revision, delete_revision + 1);
        assert!(
            source
                .graph_branches_at_revision_by_names(delete_revision, &["main".to_owned()])
                .await
                .unwrap()
                .is_empty()
        );
        index
            .refresh_invalidation_batch(&source, &durable_batch(3, false))
            .await
            .unwrap();

        let published = index.published_branch("main").await.unwrap().unwrap();
        assert_eq!(published.head_id, root);
        assert_eq!(
            index.consumed_graph_mutation_revision().await.unwrap(),
            recreate_revision
        );
    }

    #[tokio::test]
    async fn durable_source_full_reconciles_when_the_journal_baseline_passes_its_cursor() {
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
        index
            .refresh_invalidation_batch(&source, &durable_batch(1, false))
            .await
            .unwrap();
        let consumed_before_gap = index.consumed_graph_mutation_revision().await.unwrap();

        let child = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("journal baseline gap".to_owned()),
            })
            .await
            .unwrap();
        writer.set_branch_head("main", &root, &child).await.unwrap();
        let current_revision = source.graph_mutation_revision().await.unwrap();
        assert!(current_revision > consumed_before_gap);
        {
            let mut connection = SqliteConnection::establish(
                writer
                    .database_path()
                    .to_str()
                    .expect("SQLite path is UTF-8"),
            )
            .unwrap();
            diesel::sql_query("PRAGMA foreign_keys = ON")
                .execute(&mut connection)
                .unwrap();
            diesel::sql_query(
                "UPDATE graph_relation_state \
                 SET baseline_revision = current_revision WHERE singleton = 1",
            )
            .execute(&mut connection)
            .unwrap();
            diesel::sql_query("DELETE FROM graph_mutation_events WHERE revision <= ?")
                .bind::<BigInt, _>(current_revision)
                .execute(&mut connection)
                .unwrap();
        }
        let bounds = source.graph_mutation_revision_bounds().await.unwrap();
        assert_eq!(bounds.baseline_revision, current_revision);
        let stale_revision = consumed_before_gap;
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed pre-baseline source work", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_invalidation_boundaries ( \
                                 target_invalidation_incarnation, target_invalidation_version, \
                                 relation_revision, requested_scope, status, source_revision, \
                                 changed_branch_count, dirty_parent_count \
                             ) VALUES ('pre-baseline-test', 1, ?, 'full', 'building', NULL, 0, 0)",
                        )
                        .bind::<BigInt, _>(stale_revision)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_sweep_runs ( \
                                 target_invalidation_incarnation, target_invalidation_version, \
                                 relation_revision, status, phase, source_upper_bound, \
                                 page_cursor, owner_id, lease_epoch, lease_expires_at_ms \
                             ) VALUES ( \
                                 'pre-baseline-test', 1, ?, 'building', 'enumerate', \
                                 NULL, NULL, 'stale-owner', 1, 0 \
                             )",
                        )
                        .bind::<BigInt, _>(stale_revision)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        index
            .refresh_invalidation_batch(&source, &durable_batch(2, false))
            .await
            .unwrap();

        assert_eq!(
            index.consumed_graph_mutation_revision().await.unwrap(),
            current_revision
        );
        assert_eq!(
            index.published_branch_node_ids("main").await.unwrap(),
            BTreeSet::from([root, child])
        );
        index
            .refresh_invalidation_batch(&source, &durable_batch(3, false))
            .await
            .unwrap();
        let path = index.path.clone();
        let has_stale_work = index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT CASE WHEN \
                         EXISTS ( \
                             SELECT 1 FROM console_graph_source_invalidation_boundaries \
                             WHERE target_invalidation_incarnation = 'pre-baseline-test' \
                               AND status = 'building' \
                         ) OR EXISTS ( \
                             SELECT 1 FROM console_graph_source_sweep_runs \
                             WHERE target_invalidation_incarnation = 'pre-baseline-test' \
                               AND status = 'building' \
                         ) THEN 1 ELSE 0 END AS value",
                )
                .get_result::<FlagRow>(connection)
                .map(|row| row.value != 0)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        assert!(!has_stale_work);
    }

    #[tokio::test]
    async fn durable_source_resumes_an_older_full_sweep_before_reconciling_current() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("branch-a", &root).await.unwrap();
        writer.fork("branch-b", &root).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index.fail_next_branch_refresh("branch-b");
        assert!(
            index
                .refresh_invalidation_batch(&source, &durable_batch(1, false))
                .await
                .is_err()
        );

        writer.fork("branch-c", &root).await.unwrap();
        let current_revision = source.graph_mutation_revision().await.unwrap();
        drop(index);
        let path = snapshots.database().path().to_owned();
        snapshots
            .database()
            .with_write_connection("expire interrupted source sweep", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_source_sweep_runs SET lease_expires_at_ms = 0 \
                     WHERE status = 'building'",
                )
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        let mut reopened = PersistentGraphIndex::open(&snapshots, root).await.unwrap();
        reopened
            .refresh_invalidation_batch(&source, &durable_batch(2, false))
            .await
            .unwrap();
        assert_eq!(
            reopened.consumed_graph_mutation_revision().await.unwrap(),
            current_revision
        );
        let actual = build_graph_snapshot_with_mode(&reopened.graph_store(), 7, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 7, GraphMode::All)
            .await
            .unwrap();
        assert_eq!(actual, expected);
        let path = reopened.path.clone();
        let older_work_building = reopened
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT CASE WHEN \
                         EXISTS ( \
                             SELECT 1 FROM console_graph_source_invalidation_boundaries \
                             WHERE relation_revision < ? AND status = 'building' \
                         ) OR EXISTS ( \
                             SELECT 1 FROM console_graph_source_sweep_runs \
                             WHERE relation_revision < ? AND status = 'building' \
                         ) THEN 1 ELSE 0 END AS value",
                )
                .bind::<BigInt, _>(current_revision)
                .bind::<BigInt, _>(current_revision)
                .get_result::<FlagRow>(connection)
                .map(|row| row.value != 0)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        assert!(!older_work_building);
    }

    #[tokio::test]
    async fn durable_source_resumes_multiple_sweeps_in_revision_order() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let older_revision = source.graph_mutation_revision().await.unwrap();
        let child = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("revision ordered source sweep".to_owned()),
            })
            .await
            .unwrap();
        writer.set_branch_head("main", &root, &child).await.unwrap();
        let current_revision = source.graph_mutation_revision().await.unwrap();
        assert!(older_revision < current_revision);

        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root).await.unwrap();
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed revision ordered source sweeps", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "UPDATE console_graph_source_mutation_journal_state \
                             SET consumed_revision = ?, initialized = 1 WHERE id = 1",
                        )
                        .bind::<BigInt, _>(current_revision)
                        .execute(connection)?;
                        for (incarnation, revision, updated_at) in [
                            ("revision-order-old", older_revision, "9999-12-31T23:59:59Z"),
                            ("revision-order-new", current_revision, "2000-01-01T00:00:00Z"),
                        ] {
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_invalidation_boundaries ( \
                                     target_invalidation_incarnation, \
                                     target_invalidation_version, relation_revision, \
                                     requested_scope, status, source_revision, \
                                     changed_branch_count, dirty_parent_count, updated_at \
                                 ) VALUES (?, 1, ?, 'full', 'building', NULL, 0, 0, ?)",
                            )
                            .bind::<Text, _>(incarnation)
                            .bind::<BigInt, _>(revision)
                            .bind::<Text, _>(updated_at)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_sweep_runs ( \
                                     target_invalidation_incarnation, \
                                     target_invalidation_version, relation_revision, status, \
                                     phase, source_upper_bound, page_cursor, owner_id, \
                                     lease_epoch, lease_expires_at_ms, updated_at \
                                 ) VALUES (?, 1, ?, 'building', 'enumerate', NULL, NULL, '', 0, 0, ?)",
                            )
                            .bind::<Text, _>(incarnation)
                            .bind::<BigInt, _>(revision)
                            .bind::<Text, _>(updated_at)
                            .execute(connection)?;
                        }
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        index
            .refresh_invalidation_batch(&source, &durable_batch(1, false))
            .await
            .unwrap();

        assert_eq!(
            index.consumed_graph_mutation_revision().await.unwrap(),
            current_revision
        );
        let actual = build_graph_snapshot_with_mode(&index.graph_store(), 7, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 7, GraphMode::All)
            .await
            .unwrap();
        assert_eq!(actual, expected);
        let path = index.path.clone();
        let seeded_work_building = index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT CASE WHEN \
                         EXISTS ( \
                             SELECT 1 FROM console_graph_source_invalidation_boundaries \
                             WHERE target_invalidation_incarnation LIKE 'revision-order-%' \
                               AND status = 'building' \
                         ) OR EXISTS ( \
                             SELECT 1 FROM console_graph_source_sweep_runs \
                             WHERE target_invalidation_incarnation LIKE 'revision-order-%' \
                               AND status = 'building' \
                         ) THEN 1 ELSE 0 END AS value",
                )
                .get_result::<FlagRow>(connection)
                .map(|row| row.value != 0)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        assert!(!seeded_work_building);
    }

    #[tokio::test]
    async fn durable_source_resets_an_ahead_cursor_and_future_work() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root).await.unwrap();
        index
            .refresh_invalidation_batch(&source, &durable_batch(1, false))
            .await
            .unwrap();
        let current_revision = source.graph_mutation_revision().await.unwrap();
        let future_revision = current_revision + 10;
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed future source cursor", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "UPDATE console_graph_source_mutation_journal_state \
                             SET consumed_revision = ?, initialized = 1 WHERE id = 1",
                        )
                        .bind::<BigInt, _>(future_revision)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_mutation_event_runs \
                                 (revision, phase) VALUES (?, 'branch_changes')",
                        )
                        .bind::<BigInt, _>(future_revision)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_invalidation_boundaries ( \
                                 target_invalidation_incarnation, target_invalidation_version, \
                                 relation_revision, requested_scope, status, source_revision, \
                                 changed_branch_count, dirty_parent_count \
                             ) VALUES ( \
                                 'future-test', 1, ?, 'full', 'building', NULL, 0, 0 \
                             )",
                        )
                        .bind::<BigInt, _>(future_revision)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_sweep_runs ( \
                                 target_invalidation_incarnation, target_invalidation_version, \
                                 relation_revision, status, phase, source_upper_bound, \
                                 page_cursor, owner_id, lease_epoch, lease_expires_at_ms \
                             ) VALUES ( \
                                 'future-test', 1, ?, 'building', 'enumerate', \
                                 NULL, NULL, 'future-owner', 1, 0 \
                             )",
                        )
                        .bind::<BigInt, _>(future_revision)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        index
            .refresh_invalidation_batch(&source, &durable_batch(2, false))
            .await
            .unwrap();

        assert_eq!(
            index.consumed_graph_mutation_revision().await.unwrap(),
            current_revision
        );
        assert!(
            index
                .mutation_event_run(future_revision)
                .await
                .unwrap()
                .is_none()
        );
        let path = index.path.clone();
        let future_work_building = index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT CASE WHEN \
                         EXISTS ( \
                             SELECT 1 FROM console_graph_source_invalidation_boundaries \
                             WHERE target_invalidation_incarnation = 'future-test' \
                               AND status = 'building' \
                         ) OR EXISTS ( \
                             SELECT 1 FROM console_graph_source_sweep_runs \
                             WHERE target_invalidation_incarnation = 'future-test' \
                               AND status = 'building' \
                         ) THEN 1 ELSE 0 END AS value",
                )
                .get_result::<FlagRow>(connection)
                .map(|row| row.value != 0)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        assert!(!future_work_building);
        let actual = build_graph_snapshot_with_mode(&index.graph_store(), 7, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 7, GraphMode::All)
            .await
            .unwrap();
        assert_eq!(actual, expected);
        index
            .refresh_invalidation_batch(&source, &durable_batch(3, false))
            .await
            .unwrap();
        assert_eq!(
            index.consumed_graph_mutation_revision().await.unwrap(),
            current_revision
        );
    }

    #[tokio::test]
    async fn dynamic_peer_scan_reopens_at_a_frozen_revision_and_raw_upper_bound() {
        const MUTATION_REVISION: i64 = 777;
        const INELIGIBLE_COUNT: usize = SOURCE_DYNAMIC_BRANCH_CANDIDATE_PAGE_SIZE * 2;
        const FIRST_FAKE_REFRESH_ID: i64 = 9_000_000;

        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let dynamic_parent = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "frozen-dynamic-peer".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer.fork("zz-origin", &dynamic_parent).await.unwrap();
        writer.fork("zz-peer", &dynamic_parent).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root).await.unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let frozen_source_revision = index.dynamic_branch_scan_source_revision().await.unwrap();
        seed_ineligible_dynamic_rechecks(
            &index,
            &dynamic_parent,
            frozen_source_revision,
            FIRST_FAKE_REFRESH_ID,
            INELIGIBLE_COUNT,
        )
        .await;
        seed_mutation_event_run(&index, MUTATION_REVISION).await;

        let request_key =
            serde_json::to_string(&(MUTATION_REVISION, dynamic_parent.as_str())).unwrap();
        let mut scan = index
            .begin_or_claim_dynamic_branch_scan(
                "dirty_parent",
                request_key,
                Some(MUTATION_REVISION),
                Some(dynamic_parent.clone()),
                None,
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            scan.raw_refresh_id_upper_bound,
            FIRST_FAKE_REFRESH_ID + INELIGIBLE_COUNT as i64 - 1
        );
        index
            .advance_dynamic_peer_scan(&mut scan, &dynamic_parent)
            .await
            .unwrap();
        assert_eq!(scan.status, "building");
        assert_eq!(scan.result_count, 0);

        let scan_id = scan.scan_id;
        let raw_upper_bound = scan.raw_refresh_id_upper_bound;
        let dynamic_parent_for_mutation = dynamic_parent.clone();
        let path = index.path.clone();
        index
            .database
            .with_write_connection(
                "mutate source during dynamic peer scan",
                move |connection| {
                    connection
                        .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                            diesel::sql_query(
                            "UPDATE console_graph_source_identity SET revision = ? WHERE id = 1",
                        )
                        .bind::<BigInt, _>(frozen_source_revision + 1)
                        .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_generation_source_revisions ( \
                                 generation, source_revision \
                             ) \
                             SELECT active_generation, ? \
                             FROM console_graph_generation_state WHERE id = 1 \
                             ON CONFLICT(generation) DO UPDATE \
                             SET source_revision = excluded.source_revision",
                            )
                            .bind::<BigInt, _>(frozen_source_revision + 1)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_branch_history ( \
                                 branch_name, source_revision, contribution_generation, \
                                 head_id, state_json, removed \
                             ) VALUES ('zz-peer', ?, NULL, NULL, NULL, 1)",
                            )
                            .bind::<BigInt, _>(frozen_source_revision + 1)
                            .execute(connection)?;
                            diesel::sql_query(
                                "DELETE FROM console_graph_source_branch_publications \
                             WHERE branch_name = 'zz-peer'",
                            )
                            .execute(connection)?;
                            diesel::sql_query(
                                "DELETE FROM console_graph_source_branches \
                             WHERE name = 'zz-peer'",
                            )
                            .execute(connection)?;
                            diesel::sql_query(
                                "WITH RECURSIVE sequence(offset) AS ( \
                                 SELECT 1 \
                                 UNION ALL \
                                 SELECT offset + 1 FROM sequence WHERE offset < 512 \
                             ) \
                             INSERT INTO console_graph_source_child_rechecks ( \
                                 branch_name, contribution_generation, node_id, traversal_kind \
                             ) \
                             SELECT printf('future-%06d', offset), ? + offset, ?, 'graph' \
                             FROM sequence",
                            )
                            .bind::<BigInt, _>(raw_upper_bound)
                            .bind::<Text, _>(&dynamic_parent_for_mutation)
                            .execute(connection)?;
                            diesel::sql_query(
                                "UPDATE console_graph_source_dynamic_branch_scans \
                             SET lease_expires_at_ms = 0 WHERE scan_id = ?",
                            )
                            .bind::<BigInt, _>(scan_id)
                            .execute(connection)?;
                            Ok(())
                        })
                        .context(QueryGraphSnapshotStoreSnafu { path })
                },
            )
            .await
            .unwrap();
        index.prune_source_change_journal_bounded().await.unwrap();
        let path = index.path.clone();
        let frozen_history_count = index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count \
                     FROM console_graph_source_branch_history \
                     WHERE branch_name = 'zz-peer' AND source_revision = ?",
                )
                .bind::<BigInt, _>(frozen_source_revision)
                .get_result::<TestCountRow>(connection)
                .map(|row| row.count)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        assert_eq!(frozen_history_count, 1);
        drop(index);

        let mut reopened = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let peers = reopened
            .dynamic_peer_branch_page(MUTATION_REVISION, &dynamic_parent, None)
            .await
            .unwrap();
        assert_eq!(peers, ["zz-origin", "zz-peer"]);
        assert!(
            reopened
                .dynamic_peer_branch_page(
                    MUTATION_REVISION,
                    &dynamic_parent,
                    peers.last().map(String::as_str),
                )
                .await
                .unwrap()
                .is_empty()
        );
        reopened
            .prune_source_change_journal_bounded()
            .await
            .unwrap();
        let path = reopened.path.clone();
        let frozen_history_count = reopened
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count \
                     FROM console_graph_source_branch_history \
                     WHERE branch_name = 'zz-peer' AND source_revision = ?",
                )
                .bind::<BigInt, _>(frozen_source_revision)
                .get_result::<TestCountRow>(connection)
                .map(|row| row.count)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        assert_eq!(frozen_history_count, 0);
    }

    #[tokio::test]
    async fn affected_branch_scan_reopens_after_a_zero_hit_candidate_page() {
        const INELIGIBLE_COUNT: usize = SOURCE_DYNAMIC_BRANCH_CANDIDATE_PAGE_SIZE * 2;
        const FIRST_FAKE_REFRESH_ID: i64 = 9_100_000;

        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let dynamic_parent = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "resumable-affected-branch-scan".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer.fork("zz-origin", &dynamic_parent).await.unwrap();
        writer.fork("zz-peer", &dynamic_parent).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root).await.unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let source_revision = index.dynamic_branch_scan_source_revision().await.unwrap();
        seed_ineligible_dynamic_rechecks(
            &index,
            &dynamic_parent,
            source_revision,
            FIRST_FAKE_REFRESH_ID,
            INELIGIBLE_COUNT,
        )
        .await;

        let origins = vec!["zz-origin".to_owned()];
        let targeted_limit = index.targeted_dynamic_branch_limit();
        let request_key = serde_json::to_string(&(targeted_limit, &origins)).unwrap();
        let mut scan = index
            .begin_or_claim_dynamic_branch_scan(
                "affected",
                request_key,
                None,
                None,
                Some(targeted_limit),
                origins,
            )
            .await
            .unwrap();
        index
            .activate_affected_scan_origin(&mut scan)
            .await
            .unwrap();
        index.advance_affected_scan_origin(&mut scan).await.unwrap();
        assert_eq!(
            scan.active_origin_node_id.as_deref(),
            Some(dynamic_parent.as_str())
        );
        index
            .load_affected_branch_page(&mut scan, targeted_limit)
            .await
            .unwrap();
        assert_eq!(scan.status, "building");
        assert_eq!(scan.result_count, 0);
        assert!(scan.candidate_raw_cursor.is_some());

        let scan_id = scan.scan_id;
        let path = index.path.clone();
        index
            .database
            .with_write_connection("release affected branch scan lease", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_source_dynamic_branch_scans \
                     SET owner_id = '' WHERE scan_id = ?",
                )
                .bind::<BigInt, _>(scan_id)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        drop(index);

        let mut reopened = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        match reopened
            .branches_sharing_dynamic_parents(&BTreeSet::from(["zz-origin".to_owned()]))
            .await
            .unwrap()
        {
            DynamicBranchScope::Targeted(branches) => {
                assert_eq!(
                    branches,
                    BTreeSet::from(["zz-origin".to_owned(), "zz-peer".to_owned()])
                );
            }
            DynamicBranchScope::FullRefresh => panic!("bounded peer set should stay targeted"),
        }
    }

    #[tokio::test]
    async fn expired_affected_scan_falls_back_at_the_same_or_a_new_revision() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let origins = vec!["origin".to_owned()];
        let origin_set = origins.iter().cloned().collect::<BTreeSet<_>>();
        let targeted_limit = index.targeted_dynamic_branch_limit();
        let request_key = serde_json::to_string(&(targeted_limit, &origins)).unwrap();
        let same_revision_scan = index
            .begin_or_claim_dynamic_branch_scan(
                "affected",
                request_key.clone(),
                None,
                None,
                Some(targeted_limit),
                origins.clone(),
            )
            .await
            .unwrap();
        let same_revision_scan_id = same_revision_scan.scan_id;
        let path = index.path.clone();
        index
            .database
            .with_write_connection("expire affected scan with results", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "WITH RECURSIVE sequence(offset) AS ( \
                                 SELECT 0 \
                                 UNION ALL \
                                 SELECT offset + 1 FROM sequence WHERE offset + 1 < ? \
                             ) \
                             INSERT INTO \
                                 console_graph_source_dynamic_branch_scan_results ( \
                                     scan_id, branch_name \
                                 ) \
                             SELECT ?, printf('peer-%04d', offset) FROM sequence",
                        )
                        .bind::<BigInt, _>((SOURCE_CACHE_BATCH_SIZE + 1) as i64)
                        .bind::<BigInt, _>(same_revision_scan_id)
                        .execute(connection)?;
                        diesel::sql_query(
                            "UPDATE console_graph_source_dynamic_branch_scans \
                             SET result_count = ?, lease_expires_at_ms = 0 \
                             WHERE scan_id = ?",
                        )
                        .bind::<BigInt, _>((SOURCE_CACHE_BATCH_SIZE + 1) as i64)
                        .bind::<BigInt, _>(same_revision_scan_id)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        assert!(matches!(
            index
                .branches_sharing_dynamic_parents(&origin_set)
                .await
                .unwrap(),
            DynamicBranchScope::FullRefresh
        ));
        assert_eq!(dynamic_scan_count(&index, same_revision_scan_id).await, 0);
        assert_eq!(
            dynamic_scan_result_count(&index, same_revision_scan_id).await,
            0
        );

        let old_revision_scan = index
            .begin_or_claim_dynamic_branch_scan(
                "affected",
                request_key,
                None,
                None,
                Some(targeted_limit),
                origins,
            )
            .await
            .unwrap();
        let old_revision_scan_id = old_revision_scan.scan_id;
        let path = index.path.clone();
        index
            .database
            .with_write_connection("advance revision past affected scan", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        advance_source_revision(connection)?;
                        diesel::sql_query(
                            "UPDATE console_graph_source_dynamic_branch_scans \
                             SET lease_expires_at_ms = 0 WHERE scan_id = ?",
                        )
                        .bind::<BigInt, _>(old_revision_scan_id)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        assert!(matches!(
            index
                .branches_sharing_dynamic_parents(&origin_set)
                .await
                .unwrap(),
            DynamicBranchScope::FullRefresh
        ));
        assert_eq!(dynamic_scan_count(&index, old_revision_scan_id).await, 0);
    }

    #[tokio::test]
    async fn dirty_scan_results_and_event_lifecycle_cleanup_are_bounded_and_reentrant() {
        const MUTATION_REVISION: i64 = 991;
        const ORPHAN_REVISION: i64 = 992;
        const RESULT_COUNT: usize = SOURCE_CACHE_BATCH_SIZE * 3 + 1;

        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        seed_mutation_event_run(&index, MUTATION_REVISION).await;
        let request_key = serde_json::to_string(&(MUTATION_REVISION, "cleanup-parent")).unwrap();
        let scan = index
            .begin_or_claim_dynamic_branch_scan(
                "dirty_parent",
                request_key.clone(),
                Some(MUTATION_REVISION),
                Some("cleanup-parent".to_owned()),
                None,
                Vec::new(),
            )
            .await
            .unwrap();
        let scan_id = scan.scan_id;
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed completed dirty scan results", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "WITH RECURSIVE sequence(offset) AS ( \
                                 SELECT 0 \
                                 UNION ALL \
                                 SELECT offset + 1 FROM sequence WHERE offset + 1 < ? \
                             ) \
                             INSERT INTO \
                                 console_graph_source_dynamic_branch_scan_results ( \
                                     scan_id, branch_name \
                                 ) \
                             SELECT ?, printf('branch-%04d', offset) FROM sequence",
                        )
                        .bind::<BigInt, _>(RESULT_COUNT as i64)
                        .bind::<BigInt, _>(scan_id)
                        .execute(connection)?;
                        diesel::sql_query(
                            "UPDATE console_graph_source_dynamic_branch_scans \
                             SET status = 'completed', result_count = ?, \
                                 lease_expires_at_ms = 0 \
                             WHERE scan_id = ?",
                        )
                        .bind::<BigInt, _>(RESULT_COUNT as i64)
                        .bind::<BigInt, _>(scan_id)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        let reopened = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let reclaimed = reopened
            .begin_or_claim_dynamic_branch_scan(
                "dirty_parent",
                request_key.clone(),
                Some(MUTATION_REVISION),
                Some("cleanup-parent".to_owned()),
                None,
                Vec::new(),
            )
            .await
            .unwrap();
        assert!(
            index
                .dynamic_branch_scan_result_step(&scan, Some("branch-0384"))
                .await
                .is_err()
        );
        assert_eq!(dynamic_scan_result_count(&reopened, scan_id).await, 385);
        assert_eq!(
            reopened
                .dynamic_branch_scan_result_step(&reclaimed, Some("branch-0384"))
                .await
                .unwrap(),
            DynamicBranchResultStep::CleanupPending
        );
        assert_eq!(dynamic_scan_result_count(&reopened, scan_id).await, 257);

        let path = reopened.path.clone();
        reopened
            .database
            .with_write_connection("expire reclaimed dirty scan", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_source_dynamic_branch_scans \
                     SET lease_expires_at_ms = 0 WHERE scan_id = ?",
                )
                .bind::<BigInt, _>(scan_id)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        let resumed = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let resumed_scan = resumed
            .begin_or_claim_dynamic_branch_scan(
                "dirty_parent",
                request_key,
                Some(MUTATION_REVISION),
                Some("cleanup-parent".to_owned()),
                None,
                Vec::new(),
            )
            .await
            .unwrap();
        assert!(
            reopened
                .dynamic_branch_scan_result_step(&reclaimed, Some("branch-0384"))
                .await
                .is_err()
        );
        assert_eq!(
            resumed
                .dynamic_branch_scan_result_step(&resumed_scan, Some("branch-0384"))
                .await
                .unwrap(),
            DynamicBranchResultStep::CleanupPending
        );
        assert_eq!(dynamic_scan_result_count(&resumed, scan_id).await, 129);
        assert_eq!(
            resumed
                .dynamic_branch_scan_result_step(&resumed_scan, Some("branch-0384"))
                .await
                .unwrap(),
            DynamicBranchResultStep::CleanupPending
        );
        assert_eq!(dynamic_scan_result_count(&resumed, scan_id).await, 1);
        assert_eq!(
            resumed
                .dynamic_branch_scan_result_step(&resumed_scan, Some("branch-0384"))
                .await
                .unwrap(),
            DynamicBranchResultStep::Complete
        );
        assert_eq!(dynamic_scan_result_count(&resumed, scan_id).await, 0);
        assert_eq!(dynamic_scan_count(&resumed, scan_id).await, 0);
        resumed
            .delete_mutation_event_runs_bounded(Some(MUTATION_REVISION))
            .await
            .unwrap();

        seed_mutation_event_run(&resumed, ORPHAN_REVISION).await;
        resumed
            .activate_mutation_dirty_parent(ORPHAN_REVISION, None, "orphan-parent")
            .await
            .unwrap();
        let orphan_scan = resumed
            .begin_or_claim_dynamic_branch_scan(
                "dirty_parent",
                serde_json::to_string(&(ORPHAN_REVISION, "orphan-parent")).unwrap(),
                Some(ORPHAN_REVISION),
                Some("orphan-parent".to_owned()),
                None,
                Vec::new(),
            )
            .await
            .unwrap();
        let orphan_scan_id = orphan_scan.scan_id;
        let path = resumed.path.clone();
        resumed
            .database
            .with_write_connection("seed orphan dirty scan results", move |connection| {
                diesel::sql_query(
                    "WITH RECURSIVE sequence(offset) AS ( \
                         SELECT 0 \
                         UNION ALL \
                         SELECT offset + 1 FROM sequence WHERE offset + 1 < ? \
                     ) \
                     INSERT INTO console_graph_source_dynamic_branch_scan_results ( \
                         scan_id, branch_name \
                     ) \
                     SELECT ?, printf('orphan-%04d', offset) FROM sequence",
                )
                .bind::<BigInt, _>(RESULT_COUNT as i64)
                .bind::<BigInt, _>(orphan_scan_id)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        assert_eq!(
            resumed
                .delete_mutation_event_run_batch(Some(ORPHAN_REVISION))
                .await
                .unwrap(),
            1
        );
        let event = resumed
            .mutation_event_run(ORPHAN_REVISION)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.phase, "discarding");
        assert!(event.active_dirty_parent_id.is_none());
        let fence = dynamic_scan_fence(&resumed, orphan_scan_id).await;
        assert_eq!(fence.status, "discarding");
        assert_eq!(fence.lease_epoch, orphan_scan.lease_epoch + 1);

        let mut stale_scan = orphan_scan.clone();
        assert!(
            resumed
                .advance_dynamic_peer_scan(&mut stale_scan, "orphan-parent")
                .await
                .is_err()
        );
        assert!(
            resumed
                .checkpoint_mutation_peer_branch(
                    ORPHAN_REVISION,
                    "orphan-parent",
                    None,
                    "stale-peer",
                )
                .await
                .is_err()
        );
        assert_eq!(
            resumed
                .delete_mutation_event_run_batch(Some(ORPHAN_REVISION))
                .await
                .unwrap(),
            SOURCE_CACHE_BATCH_SIZE
        );
        assert_eq!(
            dynamic_scan_result_count(&resumed, orphan_scan_id).await,
            (RESULT_COUNT - SOURCE_CACHE_BATCH_SIZE) as i64
        );
        drop(resumed);

        let resumed_after_cleanup_crash = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        resumed_after_cleanup_crash
            .complete_durable_mutation_through(ORPHAN_REVISION)
            .await
            .unwrap();
        assert_eq!(
            dynamic_scan_result_count(&resumed_after_cleanup_crash, orphan_scan_id).await,
            0
        );
        assert_eq!(
            dynamic_scan_count(&resumed_after_cleanup_crash, orphan_scan_id).await,
            0
        );
        assert!(
            resumed_after_cleanup_crash
                .mutation_event_run(ORPHAN_REVISION)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            resumed_after_cleanup_crash
                .consumed_graph_mutation_revision()
                .await
                .unwrap(),
            ORPHAN_REVISION
        );
    }

    #[tokio::test]
    async fn durable_source_resumes_an_active_dirty_parent_after_peer_cursor() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "durable-resume-tool-use".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer.fork("branch-a", &tool_use).await.unwrap();
        writer.fork("branch-b", &tool_use).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_invalidation_batch(&source, &durable_batch(1, true))
            .await
            .unwrap();

        let invocation = writer
            .append(NewNode {
                parent: tool_use.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_invocation(
                    Vec::new(),
                    SkillInvocationAnchor {
                        skill_name: "durable-resume-skill".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let mutation_revision = source.graph_mutation_revision().await.unwrap();
        let branch_a = source
            .graph_branches_at_revision_by_names(mutation_revision, &["branch-a".to_owned()])
            .await
            .unwrap()
            .pop()
            .unwrap();
        index
            .refresh_branch_event(
                &source,
                branch_a,
                SourceInvalidation {
                    incarnation: DURABLE_MUTATION_INCARNATION,
                    version: mutation_revision,
                    relation_revision: mutation_revision,
                },
                &BTreeSet::from([tool_use.clone()]),
                false,
            )
            .await
            .unwrap();
        let path = index.path.clone();
        let active_parent = tool_use.clone();
        index
            .database
            .with_write_connection("seed durable source resume cursor", move |connection| {
                diesel::sql_query(
                    "INSERT INTO console_graph_source_mutation_event_runs ( \
                         revision, phase, active_dirty_parent_id, peer_branch_cursor \
                     ) VALUES (?, 'dirty_parents', ?, 'branch-a')",
                )
                .bind::<BigInt, _>(mutation_revision)
                .bind::<Text, _>(active_parent)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        drop(index);

        let mut reopened = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        reopened
            .refresh_invalidation_batch(&source, &durable_batch(2, false))
            .await
            .unwrap();

        assert_eq!(reopened.branch_refresh_history(), &["branch-b".to_owned()]);
        let expected = BTreeSet::from([root, tool_use, invocation]);
        assert_eq!(
            reopened
                .published_branch_node_ids("branch-a")
                .await
                .unwrap(),
            expected
        );
        assert_eq!(
            reopened
                .published_branch_node_ids("branch-b")
                .await
                .unwrap(),
            expected
        );
        assert_eq!(
            reopened.consumed_graph_mutation_revision().await.unwrap(),
            mutation_revision
        );
    }

    async fn begin_test_refresh(
        index: &PersistentGraphIndex,
        source: &SqliteGraphStore,
        record: &GraphBranchRecord,
    ) -> RefreshRun {
        let previous = index.published_branch(&record.name).await.unwrap();
        let relation_revision = source.graph_mutation_revision().await.unwrap();
        index
            .begin_or_resume_refresh(RefreshStart {
                record,
                base_generation: previous
                    .as_ref()
                    .map(|branch| branch.contribution_generation),
                invalidation: SourceInvalidation {
                    incarnation: "",
                    version: 0,
                    relation_revision,
                },
                target_invalidation_kind: "targeted",
                previous: previous.as_ref(),
                initial_work: &[],
                dirty_seeds: &[],
            })
            .await
            .unwrap()
    }

    async fn expire_test_refresh_lease(index: &PersistentGraphIndex, refresh_id: i64) {
        let path = index.path.clone();
        index
            .database
            .with_write_connection("expire source refresh lease for test", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_source_refresh_runs \
                     SET lease_expires_at_ms = 0 WHERE refresh_id = ?",
                )
                .bind::<BigInt, _>(refresh_id)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
    }

    async fn append_skill_subtree(
        store: &SqliteStore,
        tool_use: &str,
        skill_name: &str,
    ) -> (String, String) {
        let invocation = store
            .append(NewNode {
                parent: tool_use.to_owned(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_invocation(
                    Vec::new(),
                    SkillInvocationAnchor {
                        skill_name: skill_name.to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let child = store
            .append(NewNode {
                parent: invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Text(format!("{skill_name} child")),
            })
            .await
            .unwrap();
        (invocation, child)
    }

    #[tokio::test]
    async fn source_cache_survives_index_reopen() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let child = writer
            .append(NewNode {
                parent: root,
                role: Role::User,
                metadata: None,
                kind: Kind::Text("persistent source cache".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &writer.root_id(), &child)
            .await
            .unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let records = source.graph_branches().await.unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        index.reconcile_full_refresh(&records).await.unwrap();
        index.refresh_records(&source, records).await.unwrap();
        drop(index);

        let reopened = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        assert!(!reopened.is_empty().await.unwrap());
        let actual = build_graph_snapshot_with_mode(&reopened.graph_store(), 7, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 7, GraphMode::All)
            .await
            .unwrap();

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn source_cache_refreshes_every_high_fan_out_skill_invocation() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "high-fan-out".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &root, &tool_use)
            .await
            .unwrap();
        let mut invocation_ids = BTreeSet::new();
        for index in 0..GRAPH_READ_BATCH_SIZE + 17 {
            invocation_ids.insert(
                writer
                    .append(NewNode {
                        parent: tool_use.clone(),
                        role: Role::System,
                        metadata: None,
                        kind: Kind::Anchor(Anchor::skill_invocation(
                            Vec::new(),
                            SkillInvocationAnchor {
                                skill_name: format!("fan-out-skill-{index}"),
                                mode: SkillInvocationMode::InheritContext,
                            },
                        )),
                    })
                    .await
                    .unwrap(),
            );
        }

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let actual = build_graph_snapshot_with_mode(&index.graph_store(), 11, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 11, GraphMode::All)
            .await
            .unwrap();
        let actual_ids = actual
            .nodes
            .iter()
            .map(|node| node.id.clone())
            .collect::<BTreeSet<_>>();

        assert!(invocation_ids.is_subset(&actual_ids));
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn source_cache_fast_forward_refresh_traverses_only_the_new_suffix() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let initial = append_text_chain(&writer, &root, SOURCE_CACHE_BATCH_SIZE + 17).await;
        let previous_head = initial.last().unwrap().clone();
        writer.fork("main", &previous_head).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let previous = index.published_branch("main").await.unwrap().unwrap();
        let traversed_before = index.traversed_node_count();

        let suffix = append_text_chain(&writer, &previous_head, 7).await;
        let next_head = suffix.last().unwrap().clone();
        writer
            .set_branch_head("main", &previous_head, &next_head)
            .await
            .unwrap();
        index
            .refresh_named_batch(&source, &["main".to_owned()])
            .await
            .unwrap();

        let current = index.published_branch("main").await.unwrap().unwrap();
        let mut expected_ids = BTreeSet::from([root]);
        expected_ids.extend(initial);
        expected_ids.extend(suffix.clone());
        assert_eq!(
            current.contribution_generation,
            previous.contribution_generation
        );
        assert!(current.source_revision > previous.source_revision);
        assert_eq!(
            index.traversed_node_count() - traversed_before,
            suffix.len()
        );
        assert_eq!(
            index.published_branch_node_ids("main").await.unwrap(),
            expected_ids
        );
        let actual = build_graph_snapshot_with_mode(&index.graph_store(), 13, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 13, GraphMode::All)
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn source_cache_merge_extension_reuses_the_previous_contribution() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let initial = append_text_chain(&writer, &root, 3).await;
        let previous_head = initial.last().unwrap().clone();
        writer.fork("main", &previous_head).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let traversed_before = index.traversed_node_count();
        let other_parent = writer
            .append(NewNode {
                parent: root,
                role: Role::User,
                metadata: None,
                kind: Kind::Text("merge primary side".to_owned()),
            })
            .await
            .unwrap();
        let merge_head = writer
            .append(NewNode {
                parent: other_parent,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    vec![MergeParent::merge(previous_head.clone())],
                    SkillResultAnchor {
                        skill_name: "merge-extension".to_owned(),
                        output: "complete".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &previous_head, &merge_head)
            .await
            .unwrap();

        index
            .refresh_named_batch(&source, &["main".to_owned()])
            .await
            .unwrap();

        assert_eq!(index.traversed_node_count() - traversed_before, 2);
        let actual = build_graph_snapshot_with_mode(&index.graph_store(), 17, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 17, GraphMode::All)
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn source_cache_same_head_refresh_updates_only_branch_state() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("base", &root).await.unwrap();
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
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let previous = index.published_branch("main").await.unwrap().unwrap();
        let traversed_before = index.traversed_node_count();
        let next_state = SessionState::Attached {
            target_branch: "base".to_owned(),
            base_head_id: root,
        };
        writer
            .set_session_state("main", Some(&SessionState::Active), next_state.clone())
            .await
            .unwrap();

        index
            .refresh_named_batch(&source, &["main".to_owned()])
            .await
            .unwrap();

        let current = index.published_branch("main").await.unwrap().unwrap();
        assert_eq!(current.head_id, previous.head_id);
        assert_eq!(
            current.contribution_generation,
            previous.contribution_generation
        );
        assert_eq!(index.traversed_node_count(), traversed_before);
        assert_eq!(
            index.graph_store().get_session_state("main").await.unwrap(),
            next_state
        );
    }

    #[tokio::test]
    async fn state_only_refresh_does_not_overwrite_a_newer_published_generation() {
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
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let published = index.published_branch("main").await.unwrap().unwrap();
        let replacement_generation = published.contribution_generation + 1_000;
        let replacement_state = SessionState::Attached {
            target_branch: "replacement".to_owned(),
            base_head_id: root.clone(),
        };
        let replacement_state_json = serde_json::to_string(&replacement_state).unwrap();
        snapshots
            .database()
            .with_write_connection(
                "replace published source generation for test",
                move |connection| {
                    diesel::sql_query(
                        "UPDATE console_graph_source_branches \
                     SET contribution_generation = ?, state_json = ? WHERE name = 'main'",
                    )
                    .bind::<BigInt, _>(replacement_generation)
                    .bind::<Text, _>(replacement_state_json)
                    .execute(connection)
                    .map(|_| ())
                    .context(QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("replace-published-source-generation"),
                    })
                },
            )
            .await
            .unwrap();
        let stale_record = GraphBranchRecord {
            name: "main".to_owned(),
            head_id: root,
            state: SessionState::Active,
        };

        assert!(
            !index
                .update_published_branch_state(&stale_record, &published, "", 0, "targeted", 0,)
                .await
                .unwrap()
        );
        assert_eq!(
            index.graph_store().get_session_state("main").await.unwrap(),
            replacement_state
        );
    }

    #[tokio::test]
    async fn source_cache_same_head_refresh_discovers_new_skill_subtree() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "same-head-tool-use".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &root, &tool_use)
            .await
            .unwrap();
        for index in 0..SOURCE_CACHE_BATCH_SIZE + 1 {
            writer
                .append(NewNode {
                    parent: tool_use.clone(),
                    role: Role::System,
                    metadata: None,
                    kind: Kind::Text(format!("irrelevant tool child {index}")),
                })
                .await
                .unwrap();
        }
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let previous = index.published_branch("main").await.unwrap().unwrap();

        let (invocation, child) =
            append_skill_subtree(&writer, &tool_use, "same-head-dynamic").await;
        index
            .refresh_named_batch(&source, &["main".to_owned()])
            .await
            .unwrap();

        let current = index.published_branch("main").await.unwrap().unwrap();
        assert_eq!(
            current.contribution_generation,
            previous.contribution_generation
        );
        assert!(current.source_revision > previous.source_revision);
        assert_eq!(
            index.published_branch_node_ids("main").await.unwrap(),
            BTreeSet::from([root, tool_use, invocation, child])
        );
        let actual = build_graph_snapshot_with_mode(&index.graph_store(), 19, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 19, GraphMode::All)
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn source_cache_refreshes_branches_sharing_a_dynamic_skill_parent() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "shared-dynamic-parent".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer.fork("branch-a", &tool_use).await.unwrap();
        writer.fork("branch-b", &tool_use).await.unwrap();
        writer.fork("unrelated", &root).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();

        let refreshes_before = index.branch_refresh_count();
        let (invocation, child) =
            append_skill_subtree(&writer, &tool_use, "shared-dynamic-child").await;
        index
            .refresh_named_batch(&source, &["branch-a".to_owned()])
            .await
            .unwrap();

        let shared_nodes = BTreeSet::from([
            root.clone(),
            tool_use.clone(),
            invocation.clone(),
            child.clone(),
        ]);
        assert_eq!(index.branch_refresh_count() - refreshes_before, 2);
        assert_eq!(
            index.published_branch_node_ids("branch-a").await.unwrap(),
            shared_nodes
        );
        assert_eq!(
            index.published_branch_node_ids("branch-b").await.unwrap(),
            shared_nodes
        );
        assert_eq!(
            index.published_branch_node_ids("unrelated").await.unwrap(),
            BTreeSet::from([root.clone()])
        );

        writer
            .set_branch_head("branch-a", &tool_use, &root)
            .await
            .unwrap();
        let refresh_history_start = index.branch_refresh_history().len();
        index.fail_next_branch_refresh("branch-b");
        assert!(
            index
                .refresh_named_batch(&source, &["branch-a".to_owned()])
                .await
                .is_err()
        );
        assert_eq!(
            &index.branch_refresh_history()[refresh_history_start..],
            &["branch-b".to_owned()]
        );
        assert_eq!(
            index
                .published_branch("branch-a")
                .await
                .unwrap()
                .unwrap()
                .head_id,
            tool_use
        );
        index
            .refresh_named_batch(&source, &["branch-a".to_owned()])
            .await
            .unwrap();
        assert_eq!(
            &index.branch_refresh_history()[refresh_history_start..],
            &[
                "branch-b".to_owned(),
                "branch-b".to_owned(),
                "branch-a".to_owned()
            ]
        );
        index.prune_published_orphans().await.unwrap();
        assert_eq!(
            index.published_branch_node_ids("branch-a").await.unwrap(),
            BTreeSet::from([root.clone()])
        );
        assert_eq!(
            index.published_branch_node_ids("branch-b").await.unwrap(),
            shared_nodes
        );

        writer.delete_branch("branch-a").await.unwrap();
        index
            .refresh_named_batch(&source, &["branch-a".to_owned()])
            .await
            .unwrap();
        index.prune_published_orphans().await.unwrap();
        assert!(index.published_branch("branch-a").await.unwrap().is_none());
        assert_eq!(
            index.published_branch_node_ids("branch-b").await.unwrap(),
            shared_nodes
        );
        assert_eq!(
            index.graph_store().get_node(&child).await.unwrap().id,
            child
        );
    }

    #[tokio::test]
    async fn source_cache_refreshes_post_peers_for_a_new_branch() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "new-branch-dynamic-parent".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer.fork("branch-b", &tool_use).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();

        let (invocation, child) =
            append_skill_subtree(&writer, &tool_use, "new-branch-dynamic-child").await;
        writer.fork("branch-new", &tool_use).await.unwrap();
        let refresh_history_start = index.branch_refresh_history().len();
        index
            .refresh_named_batch(&source, &["branch-new".to_owned()])
            .await
            .unwrap();

        assert_eq!(
            &index.branch_refresh_history()[refresh_history_start..],
            &["branch-new".to_owned(), "branch-b".to_owned()]
        );
        let expected = BTreeSet::from([root, tool_use, invocation, child]);
        assert_eq!(
            index.published_branch_node_ids("branch-new").await.unwrap(),
            expected
        );
        assert_eq!(
            index.published_branch_node_ids("branch-b").await.unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn source_cache_refreshes_peers_before_a_direct_branch_delete() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "direct-delete-dynamic-parent".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer.fork("branch-a", &tool_use).await.unwrap();
        writer.fork("branch-b", &tool_use).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();

        let (invocation, child) =
            append_skill_subtree(&writer, &tool_use, "direct-delete-dynamic-child").await;
        index
            .refresh_named_batch(&source, &["branch-a".to_owned()])
            .await
            .unwrap();
        writer.delete_branch("branch-a").await.unwrap();

        let refresh_history_start = index.branch_refresh_history().len();
        index
            .refresh_named_batch(&source, &["branch-a".to_owned()])
            .await
            .unwrap();
        assert_eq!(
            &index.branch_refresh_history()[refresh_history_start..],
            &["branch-b".to_owned()]
        );

        index.prune_published_orphans().await.unwrap();
        assert!(index.published_branch("branch-a").await.unwrap().is_none());
        assert_eq!(
            index.published_branch_node_ids("branch-b").await.unwrap(),
            BTreeSet::from([root, tool_use, invocation, child.clone()])
        );
        assert_eq!(
            index.graph_store().get_node(&child).await.unwrap().id,
            child
        );
    }

    #[tokio::test]
    async fn full_fallback_pages_branches_above_the_targeted_limit() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "paged-fallback-dynamic-parent".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        for index in 0..5 {
            let name = format!("branch-{index:02}");
            writer.fork(&name, &tool_use).await.unwrap();
        }
        writer.fork("stale", &root).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();

        let (invocation, child) =
            append_skill_subtree(&writer, &tool_use, "paged-fallback-dynamic-child").await;
        writer.delete_branch("stale").await.unwrap();
        index.set_targeted_dynamic_branch_limit(4);
        index.set_full_refresh_branch_page_size(NonZeroUsize::new(2).unwrap());
        let page_count_before = index.full_refresh_source_page_count();
        let refresh_history_start = index.branch_refresh_history().len();

        index
            .refresh_named_batch(&source, &["branch-00".to_owned()])
            .await
            .unwrap();

        assert_eq!(
            index.full_refresh_source_page_count() - page_count_before,
            3
        );
        assert_eq!(
            &index.branch_refresh_history()[refresh_history_start..],
            &[
                "branch-01".to_owned(),
                "branch-02".to_owned(),
                "branch-03".to_owned(),
                "branch-04".to_owned(),
                "branch-00".to_owned(),
            ]
        );
        assert!(index.published_branch("stale").await.unwrap().is_none());
        let expected = BTreeSet::from([root, tool_use, invocation, child]);
        assert_eq!(
            index.published_branch_node_ids("branch-00").await.unwrap(),
            expected
        );
        assert_eq!(
            index.published_branch_node_ids("branch-04").await.unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn unknown_deleted_branch_invalidation_falls_back_to_full_source_refresh() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "fallback-dynamic-parent".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer.fork("survivor", &tool_use).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();

        let (invocation, child) =
            append_skill_subtree(&writer, &tool_use, "fallback-dynamic-child").await;
        index
            .refresh_named_batch(&source, &["never-observed-deleted-branch".to_owned()])
            .await
            .unwrap();

        assert_eq!(
            index.published_branch_node_ids("survivor").await.unwrap(),
            BTreeSet::from([root, tool_use, invocation, child])
        );
    }

    #[tokio::test]
    async fn source_cache_fast_forward_refresh_discovers_new_skill_subtree() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "fast-forward-tool-use".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &root, &tool_use)
            .await
            .unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();

        let suffix = writer
            .append(NewNode {
                parent: tool_use.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("fast-forward suffix".to_owned()),
            })
            .await
            .unwrap();
        let (invocation, child) =
            append_skill_subtree(&writer, &tool_use, "fast-forward-dynamic").await;
        writer
            .set_branch_head("main", &tool_use, &suffix)
            .await
            .unwrap();
        index
            .refresh_named_batch(&source, &["main".to_owned()])
            .await
            .unwrap();

        assert_eq!(
            index.published_branch_node_ids("main").await.unwrap(),
            BTreeSet::from([root, tool_use, suffix, invocation, child])
        );
        let actual = build_graph_snapshot_with_mode(&index.graph_store(), 23, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 23, GraphMode::All)
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn source_cache_rewind_and_diverge_drop_the_previous_suffix() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let initial = append_text_chain(&writer, &root, 3).await;
        let base = initial[0].clone();
        let old_suffix = BTreeSet::from([initial[1].clone(), initial[2].clone()]);
        let previous_head = initial[2].clone();
        writer.fork("diverge", &previous_head).await.unwrap();
        writer.fork("rewind", &previous_head).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let traversed_before = index.traversed_node_count();
        let diverged = writer
            .append(NewNode {
                parent: base.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("diverged".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("diverge", &previous_head, &diverged)
            .await
            .unwrap();
        writer
            .set_branch_head("rewind", &previous_head, &base)
            .await
            .unwrap();

        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();

        assert_eq!(index.traversed_node_count() - traversed_before, 5);
        assert_eq!(
            index.published_branch_node_ids("rewind").await.unwrap(),
            BTreeSet::from([root.clone(), base.clone()])
        );
        assert_eq!(
            index.published_branch_node_ids("diverge").await.unwrap(),
            BTreeSet::from([root, base, diverged])
        );
        let retained = index
            .graph_store()
            .list_children(&writer.root_id())
            .await
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<BTreeSet<_>>();
        assert!(old_suffix.is_disjoint(&retained));
    }

    #[tokio::test]
    async fn source_cache_retains_superseded_facts_until_explicit_prune() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let superseded = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("superseded".to_owned()),
            })
            .await
            .unwrap();
        writer.fork("main", &superseded).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        writer
            .set_branch_head("main", &superseded, &root)
            .await
            .unwrap();

        index
            .refresh_named_batch(&source, &["main".to_owned()])
            .await
            .unwrap();

        assert_eq!(index.node_count().await.unwrap(), 2);
        assert_eq!(
            index.graph_store().get_node(&superseded).await.unwrap().id,
            superseded
        );
        mark_current_source_revision_consumed(&index).await;
        index.start_refresh().await.unwrap();
        index.prune_published_orphans().await.unwrap();
        assert_eq!(index.node_count().await.unwrap(), 1);
        assert!(index.graph_store().get_node(&superseded).await.is_err());
    }

    #[tokio::test]
    async fn stale_refresh_cleanup_advances_across_protected_raw_pages() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let first_refresh_id = 910_000_000_i64;
        let count = SOURCE_REFRESH_CLEANUP_CANDIDATE_BATCH_SIZE * 2 + 17;
        let last_refresh_id = first_refresh_id + i64::try_from(count).unwrap() - 1;
        seed_protected_source_refreshes(&index, first_refresh_id, count).await;

        assert_eq!(
            index.advance_stale_refresh_cleanup().await.unwrap(),
            SourceRefreshCleanupStep::Continue
        );
        assert_eq!(
            source_refresh_cleanup_state(&index).await,
            (
                Some(last_refresh_id),
                first_refresh_id
                    + i64::try_from(SOURCE_REFRESH_CLEANUP_CANDIDATE_BATCH_SIZE).unwrap()
                    - 1,
                None,
            )
        );

        index.cleanup_stale_refreshes_bounded().await.unwrap();

        assert_eq!(
            source_refresh_run_range_count(&index, first_refresh_id, last_refresh_id).await,
            i64::try_from(count).unwrap()
        );
        assert_eq!(source_refresh_cleanup_state(&index).await, (None, 0, None));
    }

    #[tokio::test]
    async fn stale_refresh_cleanup_does_not_extend_a_frozen_round() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let first_refresh_id = 920_000_000_i64;
        let protected_count = SOURCE_REFRESH_CLEANUP_CANDIDATE_BATCH_SIZE * 2 + 1;
        let frozen_upper = first_refresh_id + i64::try_from(protected_count).unwrap() - 1;
        seed_protected_source_refreshes(&index, first_refresh_id, protected_count).await;

        assert_eq!(
            index.advance_stale_refresh_cleanup().await.unwrap(),
            SourceRefreshCleanupStep::Continue
        );
        seed_superseded_source_refresh(&index, frozen_upper + 1).await;
        assert_eq!(
            index.advance_stale_refresh_cleanup().await.unwrap(),
            SourceRefreshCleanupStep::Continue
        );
        assert_eq!(
            source_refresh_cleanup_state(&index).await.0,
            Some(frozen_upper)
        );
        seed_superseded_source_refresh(&index, frozen_upper + 2).await;
        assert_eq!(
            index.advance_stale_refresh_cleanup().await.unwrap(),
            SourceRefreshCleanupStep::Continue
        );
        assert_eq!(
            source_refresh_cleanup_state(&index).await,
            (Some(frozen_upper), frozen_upper, None)
        );
        seed_superseded_source_refresh(&index, frozen_upper + 3).await;

        index.cleanup_stale_refreshes_bounded().await.unwrap();

        assert_eq!(source_refresh_cleanup_state(&index).await, (None, 0, None));
        assert_eq!(
            source_refresh_run_range_count(&index, frozen_upper + 1, frozen_upper + 3).await,
            3
        );

        index.cleanup_stale_refreshes_bounded().await.unwrap();

        assert_eq!(
            source_refresh_run_range_count(&index, frozen_upper + 1, frozen_upper + 3).await,
            0
        );
    }

    #[tokio::test]
    async fn stale_refresh_cleanup_resumes_active_deletion_after_reopen() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let refresh_id = 930_000_000_i64;
        let index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        seed_superseded_source_refresh(&index, refresh_id).await;
        let queue_count = SOURCE_REFRESH_CLEANUP_DELETE_BATCH_SIZE / 2;
        let dirty_seed_count = SOURCE_REFRESH_CLEANUP_DELETE_BATCH_SIZE + 1;
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed partial source cleanup", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "WITH RECURSIVE sequence(value) AS ( \
                                 SELECT 1 \
                                 UNION ALL \
                                 SELECT value + 1 FROM sequence WHERE value < ? \
                             ) \
                             INSERT INTO console_graph_source_refresh_queue ( \
                                 refresh_id, branch_name, node_id, traversal_kind \
                             ) \
                             SELECT ?, 'cleanup-partial', \
                                    printf('cleanup-node-%d', value), 'graph' \
                             FROM sequence",
                        )
                        .bind::<BigInt, _>(i64::try_from(queue_count).unwrap())
                        .bind::<BigInt, _>(refresh_id)
                        .execute(connection)?;
                        diesel::sql_query(
                            "WITH RECURSIVE sequence(value) AS ( \
                                 SELECT 1 \
                                 UNION ALL \
                                 SELECT value + 1 FROM sequence WHERE value < ? \
                             ) \
                             INSERT INTO console_graph_source_refresh_dirty_seeds ( \
                                 refresh_id, node_id \
                             ) \
                             SELECT ?, printf('cleanup-dirty-%d', value) FROM sequence",
                        )
                        .bind::<BigInt, _>(i64::try_from(dirty_seed_count).unwrap())
                        .bind::<BigInt, _>(refresh_id)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        assert_eq!(
            index.advance_stale_refresh_cleanup().await.unwrap(),
            SourceRefreshCleanupStep::Continue
        );
        assert_eq!(
            source_refresh_cleanup_state(&index).await,
            (Some(refresh_id), refresh_id, Some(refresh_id))
        );
        assert_eq!(source_refresh_run_count(&index, refresh_id).await, 0);
        assert_eq!(
            index.advance_stale_refresh_cleanup().await.unwrap(),
            SourceRefreshCleanupStep::Continue
        );
        assert_eq!(source_refresh_queue_count(&index, refresh_id).await, 0);
        assert_eq!(
            source_refresh_dirty_seed_count(&index, refresh_id).await,
            i64::try_from(queue_count + 1).unwrap()
        );
        assert_eq!(
            source_refresh_cleanup_state(&index).await,
            (Some(refresh_id), refresh_id, Some(refresh_id))
        );
        drop(index);

        let reopened = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        assert_eq!(
            source_refresh_cleanup_state(&reopened).await,
            (Some(refresh_id), refresh_id, Some(refresh_id))
        );
        reopened.cleanup_stale_refreshes_bounded().await.unwrap();

        assert_eq!(source_refresh_queue_count(&reopened, refresh_id).await, 0);
        assert_eq!(
            source_refresh_dirty_seed_count(&reopened, refresh_id).await,
            0
        );
        assert_eq!(source_refresh_run_count(&reopened, refresh_id).await, 0);
        assert_eq!(
            source_refresh_cleanup_state(&reopened).await,
            (None, 0, None)
        );
    }

    #[tokio::test]
    async fn stale_refresh_cleanup_claim_cannot_be_reversed_by_late_protection() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let refresh_id = 940_000_000_i64;
        let queue_count = SOURCE_REFRESH_CLEANUP_DELETE_BATCH_SIZE * 2 + 1;
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed cleanup protection race", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_refresh_runs ( \
                                 refresh_id, branch_name, target_head_id, target_state_json, \
                                 status, target_contribution_generation, \
                                 published_source_revision \
                             ) VALUES ( \
                                 ?, 'cleanup-race', 'head', '{}', 'published', ?, 1 \
                             )",
                        )
                        .bind::<BigInt, _>(refresh_id)
                        .bind::<BigInt, _>(refresh_id)
                        .execute(connection)?;
                        diesel::sql_query(
                            "WITH RECURSIVE sequence(value) AS ( \
                                 SELECT 1 \
                                 UNION ALL \
                                 SELECT value + 1 FROM sequence WHERE value < ? \
                             ) \
                             INSERT INTO console_graph_source_refresh_queue ( \
                                 refresh_id, branch_name, node_id, traversal_kind \
                             ) \
                             SELECT ?, 'cleanup-race', \
                                    printf('cleanup-race-node-%d', value), 'graph' \
                             FROM sequence",
                        )
                        .bind::<BigInt, _>(i64::try_from(queue_count).unwrap())
                        .bind::<BigInt, _>(refresh_id)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        index.advance_stale_refresh_cleanup().await.unwrap();
        assert_eq!(
            source_refresh_cleanup_state(&index).await,
            (Some(refresh_id), refresh_id, Some(refresh_id))
        );
        assert_eq!(source_refresh_run_count(&index, refresh_id).await, 0);
        index.advance_stale_refresh_cleanup().await.unwrap();
        assert_eq!(
            source_refresh_queue_count(&index, refresh_id).await,
            i64::try_from(queue_count - SOURCE_REFRESH_CLEANUP_DELETE_BATCH_SIZE).unwrap()
        );
        let path = index.path.clone();
        index
            .database
            .with_write_connection("protect active source cleanup", move |connection| {
                diesel::sql_query(
                    "INSERT INTO console_graph_source_branch_publications ( \
                         branch_name, target_contribution_generation, source_revision \
                     ) VALUES ('cleanup-race', ?, 1)",
                )
                .bind::<BigInt, _>(refresh_id)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        index.cleanup_stale_refreshes_bounded().await.unwrap();

        assert_eq!(source_refresh_run_count(&index, refresh_id).await, 0);
        assert_eq!(source_refresh_queue_count(&index, refresh_id).await, 0);
        assert_eq!(source_refresh_cleanup_state(&index).await, (None, 0, None));
    }

    #[tokio::test]
    async fn unconsumed_journal_refresh_survives_until_active_generation_advances() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let first_refresh_id = 945_000_001_i64;
        let second_refresh_id = 945_000_002_i64;
        let first_contribution = 945_000_101_i64;
        let second_contribution = 945_000_102_i64;
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed unconsumed source journal", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_refresh_runs ( \
                                 refresh_id, branch_name, target_head_id, target_state_json, \
                                 status, target_contribution_generation, \
                                 published_source_revision \
                             ) VALUES \
                                 (?, 'cleanup-journal', 'head-1', '{}', 'published', ?, 1), \
                                 (?, 'cleanup-journal', 'head-2', '{}', 'published', ?, 2)",
                        )
                        .bind::<BigInt, _>(first_refresh_id)
                        .bind::<BigInt, _>(first_contribution)
                        .bind::<BigInt, _>(second_refresh_id)
                        .bind::<BigInt, _>(second_contribution)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branches ( \
                                 name, head_id, state_json, contribution_generation \
                             ) VALUES ('cleanup-journal', 'head-2', '{}', ?)",
                        )
                        .bind::<BigInt, _>(second_contribution)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_publications ( \
                                 branch_name, target_contribution_generation, source_revision \
                             ) VALUES ('cleanup-journal', ?, 2)",
                        )
                        .bind::<BigInt, _>(second_contribution)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_change_journal ( \
                                 source_revision, target_invalidation_incarnation, \
                                 target_invalidation_version, branch_name, change_kind, \
                                 refresh_id, target_contribution_generation, head_id, state_json \
                             ) VALUES \
                                 (1, 'cleanup-journal', 1, 'cleanup-journal', 'append', \
                                  ?, ?, 'head-1', '{}'), \
                                 (2, 'cleanup-journal', 2, 'cleanup-journal', 'append', \
                                  ?, ?, 'head-2', '{}')",
                        )
                        .bind::<BigInt, _>(first_refresh_id)
                        .bind::<BigInt, _>(first_contribution)
                        .bind::<BigInt, _>(second_refresh_id)
                        .bind::<BigInt, _>(second_contribution)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_nodes ( \
                                 node_id, parent_id, node_json \
                             ) VALUES ('cleanup-journal-node', '', '{}')",
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_nodes ( \
                                 branch_name, contribution_generation, node_id \
                             ) VALUES ( \
                                 'cleanup-journal', ?, 'cleanup-journal-node' \
                             )",
                        )
                        .bind::<BigInt, _>(first_refresh_id)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_orphan_gc_queue (node_id) \
                             VALUES ('cleanup-journal-node')",
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "UPDATE console_graph_source_identity SET revision = 2 WHERE id = 1",
                        )
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        index.cleanup_stale_refreshes_bounded().await.unwrap();
        index.prune_published_orphans().await.unwrap();

        assert_eq!(source_refresh_run_count(&index, first_refresh_id).await, 1);
        assert!(source_node_exists(&index, "cleanup-journal-node").await);

        let path = index.path.clone();
        index
            .database
            .with_write_connection("establish active source revision", move |connection| {
                diesel::sql_query(
                    "INSERT INTO console_graph_generation_source_revisions ( \
                         generation, source_revision \
                     ) VALUES (0, 0)",
                )
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        index.cleanup_stale_refreshes_bounded().await.unwrap();
        assert_eq!(source_refresh_run_count(&index, first_refresh_id).await, 1);

        let path = index.path.clone();
        index
            .database
            .with_write_connection("advance active source revision", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_generation_source_revisions \
                     SET source_revision = 2 WHERE generation = 0",
                )
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        index.cleanup_stale_refreshes_bounded().await.unwrap();
        index.prune_published_orphans().await.unwrap();

        assert_eq!(source_refresh_run_count(&index, first_refresh_id).await, 0);
        assert_eq!(source_refresh_run_count(&index, second_refresh_id).await, 1);
        assert!(!source_node_exists(&index, "cleanup-journal-node").await);
    }

    #[tokio::test]
    async fn building_dynamic_scan_protects_its_frozen_refresh() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let refresh_id = 950_000_000_i64;
        let contribution_generation = 950_000_100_i64;
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed dynamic scan source protection", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_refresh_runs ( \
                                 refresh_id, branch_name, target_head_id, target_state_json, \
                                 status, target_contribution_generation, \
                                 published_source_revision \
                             ) VALUES (?, 'cleanup-dynamic', 'old-head', '{}', \
                                       'published', ?, 4)",
                        )
                        .bind::<BigInt, _>(refresh_id)
                        .bind::<BigInt, _>(contribution_generation)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_history ( \
                                 branch_name, source_revision, contribution_generation, \
                                 head_id, state_json, removed \
                             ) VALUES \
                                 ('cleanup-dynamic', 4, ?, 'old-head', '{}', 0), \
                                 ('cleanup-dynamic', 6, ?, 'new-head', '{}', 0)",
                        )
                        .bind::<BigInt, _>(contribution_generation)
                        .bind::<BigInt, _>(contribution_generation + 1)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_nodes ( \
                                 node_id, parent_id, node_json \
                             ) VALUES ('cleanup-dynamic-node', '', '{}')",
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_child_rechecks ( \
                                 branch_name, contribution_generation, node_id, traversal_kind \
                             ) VALUES ( \
                                 'cleanup-dynamic', ?, 'cleanup-dynamic-node', 'graph' \
                             )",
                        )
                        .bind::<BigInt, _>(refresh_id)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_orphan_gc_queue (node_id) \
                             VALUES ('cleanup-dynamic-node')",
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_mutation_event_runs ( \
                                 revision, phase \
                             ) VALUES (5, 'dirty_parents')",
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_dynamic_branch_scans ( \
                                 scan_kind, request_key, mutation_revision, source_revision, \
                                 raw_refresh_id_upper_bound, dirty_node_id, status \
                             ) VALUES ( \
                                 'dirty_parent', 'cleanup-dynamic-scan', 5, 5, ?, \
                                 'cleanup-dynamic-node', 'building' \
                             )",
                        )
                        .bind::<BigInt, _>(refresh_id)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        index.cleanup_stale_refreshes_bounded().await.unwrap();
        index.prune_published_orphans().await.unwrap();

        assert_eq!(source_refresh_run_count(&index, refresh_id).await, 1);
        assert!(source_node_exists(&index, "cleanup-dynamic-node").await);
        let path = index.path.clone();
        let recheck_count = index
            .database
            .with_write_connection(
                "complete dynamic scan source protection",
                move |connection| {
                    (|| -> QueryResult<i64> {
                        let count = diesel::sql_query(
                            "SELECT COUNT(*) AS count \
                         FROM console_graph_source_child_rechecks \
                         WHERE contribution_generation = ?",
                        )
                        .bind::<BigInt, _>(refresh_id)
                        .get_result::<TestCountRow>(connection)?
                        .count;
                        diesel::sql_query(
                            "UPDATE console_graph_source_dynamic_branch_scans \
                         SET status = 'completed' \
                         WHERE request_key = 'cleanup-dynamic-scan'",
                        )
                        .execute(connection)?;
                        Ok(count)
                    })()
                    .context(QueryGraphSnapshotStoreSnafu { path })
                },
            )
            .await
            .unwrap();
        assert_eq!(recheck_count, 1);

        index.cleanup_stale_refreshes_bounded().await.unwrap();
        index.prune_published_orphans().await.unwrap();

        assert_eq!(source_refresh_run_count(&index, refresh_id).await, 0);
        assert!(!source_node_exists(&index, "cleanup-dynamic-node").await);
        let path = index.path.clone();
        let recheck_count = index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count \
                     FROM console_graph_source_child_rechecks \
                     WHERE contribution_generation = ?",
                )
                .bind::<BigInt, _>(refresh_id)
                .get_result::<TestCountRow>(connection)
                .map(|row| row.count)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        assert_eq!(recheck_count, 0);
    }

    #[tokio::test]
    async fn affected_dynamic_scan_protects_refresh_only_while_its_lease_is_active() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let refresh_id = 955_000_000_i64;
        let contribution_generation = 955_000_100_i64;
        let lease_expires_at_ms = source_time_ms().saturating_add(SOURCE_LEASE_DURATION_MS);
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed affected scan source protection", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_refresh_runs ( \
                                 refresh_id, branch_name, target_head_id, target_state_json, \
                                 status, target_contribution_generation, \
                                 published_source_revision \
                             ) VALUES (?, 'cleanup-affected', 'head', '{}', \
                                       'published', ?, 4)",
                        )
                        .bind::<BigInt, _>(refresh_id)
                        .bind::<BigInt, _>(contribution_generation)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_history ( \
                                 branch_name, source_revision, contribution_generation, \
                                 head_id, state_json, removed \
                             ) VALUES ('cleanup-affected', 4, ?, 'head', '{}', 0)",
                        )
                        .bind::<BigInt, _>(contribution_generation)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_dynamic_branch_scans ( \
                                 scan_kind, request_key, source_revision, \
                                 raw_refresh_id_upper_bound, targeted_limit, status, \
                                 lease_expires_at_ms \
                             ) VALUES ( \
                                 'affected', 'cleanup-affected-scan', 5, ?, 1, \
                                 'building', ? \
                             )",
                        )
                        .bind::<BigInt, _>(refresh_id)
                        .bind::<BigInt, _>(lease_expires_at_ms)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        index.cleanup_stale_refreshes_bounded().await.unwrap();
        assert_eq!(source_refresh_run_count(&index, refresh_id).await, 1);

        let path = index.path.clone();
        index
            .database
            .with_write_connection(
                "expire affected scan source protection",
                move |connection| {
                    diesel::sql_query(
                        "UPDATE console_graph_source_dynamic_branch_scans \
                     SET lease_expires_at_ms = 0 \
                     WHERE request_key = 'cleanup-affected-scan'",
                    )
                    .execute(connection)
                    .map(|_| ())
                    .context(QueryGraphSnapshotStoreSnafu { path })
                },
            )
            .await
            .unwrap();

        index.cleanup_stale_refreshes_bounded().await.unwrap();
        assert_eq!(source_refresh_run_count(&index, refresh_id).await, 0);
    }

    #[tokio::test]
    async fn materialized_generation_protects_superseded_source_contribution() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let superseded = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("materialized source".to_owned()),
            })
            .await
            .unwrap();
        writer.fork("main", &superseded).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        let record = source.graph_branches().await.unwrap().pop().unwrap();
        index
            .refresh_records(&source, [record.clone()])
            .await
            .unwrap();
        let published = index.published_branch("main").await.unwrap().unwrap();
        let state_json = serde_json::to_string(&record.state).unwrap();
        let database = index.database.clone();
        let path = index.path.clone();
        database
            .with_write_connection("seed materialized source contribution", move |connection| {
                diesel::sql_query(
                    "INSERT INTO console_graph_materialization_branches ( \
                         generation, name, head_id, state_json, contribution_generation \
                     ) VALUES (900000002, 'main', ?, ?, ?)",
                )
                .bind::<Text, _>(record.head_id)
                .bind::<Text, _>(state_json)
                .bind::<BigInt, _>(published.contribution_generation)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        writer
            .set_branch_head("main", &superseded, &root)
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        index.prune_published_orphans().await.unwrap();
        assert_eq!(
            index.graph_store().get_node(&superseded).await.unwrap().id,
            superseded
        );

        let database = index.database.clone();
        let path = index.path.clone();
        database
            .with_write_connection(
                "release materialized source contribution",
                move |connection| {
                    diesel::sql_query(
                        "DELETE FROM console_graph_materialization_branches \
                     WHERE generation = 900000002",
                    )
                    .execute(connection)
                    .map(|_| ())
                    .context(QueryGraphSnapshotStoreSnafu { path })
                },
            )
            .await
            .unwrap();
        mark_current_source_revision_consumed(&index).await;
        index.start_refresh().await.unwrap();
        index.prune_published_orphans().await.unwrap();
        assert!(index.graph_store().get_node(&superseded).await.is_err());
    }

    #[tokio::test]
    async fn paused_build_manifest_protects_source_contribution_from_gc() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let superseded = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("paused build source".to_owned()),
            })
            .await
            .unwrap();
        writer.fork("main", &superseded).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        let initial_record = source.graph_branches().await.unwrap().pop().unwrap();
        index
            .refresh_records(&source, [initial_record.clone()])
            .await
            .unwrap();
        let published = index.published_branch("main").await.unwrap().unwrap();
        let state_json = serde_json::to_string(&initial_record.state).unwrap();
        let database = index.database.clone();
        let path = index.path.clone();
        database
            .with_write_connection("seed paused source manifest test", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "INSERT INTO console_graph_build_runs ( \
                                 run_id, source_version, status, owner_id, lease_expires_at_ms \
                             ) VALUES (900000001, 1, 'paused', 'source-cache-test', 0)",
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_build_source_manifest ( \
                                 run_id, branch_name, contribution_generation, head_id, state_json \
                             ) VALUES (900000001, 'main', ?, ?, ?)",
                        )
                        .bind::<BigInt, _>(published.contribution_generation)
                        .bind::<Text, _>(&initial_record.head_id)
                        .bind::<Text, _>(&state_json)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_build_source_refresh_manifest ( \
                                 run_id, branch_name, refresh_id \
                             ) \
                             SELECT 900000001, 'main', refresh_id \
                             FROM console_graph_source_refresh_runs \
                             WHERE branch_name = 'main' \
                               AND target_contribution_generation = ? \
                               AND status = 'published'",
                        )
                        .bind::<BigInt, _>(published.contribution_generation)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        writer
            .set_branch_head("main", &superseded, &root)
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        index.prune_published_orphans().await.unwrap();
        assert_eq!(
            index.graph_store().get_node(&superseded).await.unwrap().id,
            superseded
        );

        let database = index.database.clone();
        let path = index.path.clone();
        database
            .with_write_connection("complete paused source manifest test", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_build_runs SET status = 'completed' \
                     WHERE run_id = 900000001",
                )
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        mark_current_source_revision_consumed(&index).await;
        index.start_refresh().await.unwrap();
        index.prune_published_orphans().await.unwrap();
        assert!(index.graph_store().get_node(&superseded).await.is_err());
    }

    #[tokio::test]
    async fn pre_manifest_build_protects_unstaged_journal_refresh_from_gc() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let database = index.database.clone();
        let path = index.path.clone();
        database
            .with_write_connection("seed unstaged journal refresh", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_refresh_runs ( \
                                 refresh_id, branch_name, target_head_id, target_state_json, \
                                 status, target_contribution_generation, \
                                 published_source_revision \
                             ) VALUES (900000010, 'journal', 'head', '{}', \
                                       'published', 900000010, 5)",
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_nodes (node_id, parent_id, node_json) \
                             VALUES ('unstaged-journal-node', '', '{}')",
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_nodes ( \
                                 branch_name, contribution_generation, node_id \
                             ) VALUES ('journal', 900000010, 'unstaged-journal-node')",
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_orphan_gc_queue (node_id) \
                             VALUES ('unstaged-journal-node')",
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_change_journal ( \
                                 source_revision, target_invalidation_incarnation, \
                                 target_invalidation_version, branch_name, change_kind, \
                                 refresh_id, target_contribution_generation, head_id, state_json \
                             ) VALUES (5, 'journal-test', 1, 'journal', 'append', \
                                       900000010, 900000010, 'head', '{}')",
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_build_runs ( \
                                 run_id, source_version, status, owner_id, lease_expires_at_ms, \
                                 dag_source_revision \
                             ) VALUES (900000010, 1, 'paused', 'journal-test', 0, 5)",
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "UPDATE console_graph_source_identity SET revision = 6 WHERE id = 1",
                        )
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        index.start_refresh().await.unwrap();
        index.prune_published_orphans().await.unwrap();
        assert_eq!(source_refresh_run_count(&index, 900000010).await, 1);
        assert!(source_node_exists(&index, "unstaged-journal-node").await);

        let database = index.database.clone();
        let path = index.path.clone();
        database
            .with_write_connection("release unstaged journal refresh", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_build_runs SET status = 'completed' \
                     WHERE run_id = 900000010",
                )
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        mark_current_source_revision_consumed(&index).await;
        index.start_refresh().await.unwrap();
        index.prune_published_orphans().await.unwrap();
        assert_eq!(source_refresh_run_count(&index, 900000010).await, 0);
        assert!(!source_node_exists(&index, "unstaged-journal-node").await);
    }

    #[tokio::test]
    async fn frozen_pre_manifest_history_protects_replaced_contribution_from_gc() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let superseded = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("frozen pre-manifest source".to_owned()),
            })
            .await
            .unwrap();
        writer.fork("main", &superseded).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let published = index.published_branch("main").await.unwrap().unwrap();
        let target_contribution_generation = published.contribution_generation;
        let old_refresh_id = index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT refresh_id AS contribution_generation \
                     FROM console_graph_source_refresh_runs \
                     WHERE branch_name = 'main' \
                       AND target_contribution_generation = ? \
                       AND status = 'published' \
                     ORDER BY refresh_id LIMIT 1",
                )
                .bind::<BigInt, _>(target_contribution_generation)
                .get_result::<GenerationRow>(connection)
                .map(|row| row.contribution_generation)
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("frozen-pre-manifest-refresh"),
                })
            })
            .await
            .unwrap();
        let baseline_generation = snapshots.active_generation().await.unwrap();
        let lease = snapshots
            .acquire_incremental_build_lease(1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.frozen_source_revision(), published.source_revision);

        let frozen_source_revision = published.source_revision;
        index
            .database
            .with_write_connection("isolate frozen history gc protection", move |connection| {
                diesel::sql_query(
                    "DELETE FROM console_graph_source_branch_change_journal \
                     WHERE source_revision <= ?",
                )
                .bind::<BigInt, _>(frozen_source_revision)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("isolate-frozen-history-gc-protection"),
                })
            })
            .await
            .unwrap();

        writer
            .set_branch_head("main", &superseded, &root)
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        index.start_refresh().await.unwrap();
        index.prune_published_orphans().await.unwrap();
        assert_eq!(source_refresh_run_count(&index, old_refresh_id).await, 1);
        assert!(source_node_exists(&index, &superseded).await);

        build_incremental_generation(&snapshots, &root, baseline_generation, &lease, 1)
            .await
            .unwrap();
        let run_id = lease.generation();
        let frozen_manifest_head = snapshots
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT head_id AS node_id FROM console_graph_build_source_manifest \
                     WHERE run_id = ? AND branch_name = 'main'",
                )
                .bind::<BigInt, _>(run_id)
                .get_result::<NodeIdRow>(connection)
                .map(|row| row.node_id)
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("frozen-pre-manifest-head"),
                })
            })
            .await
            .unwrap();
        assert_eq!(frozen_manifest_head, superseded);

        let run_id = lease.generation();
        index
            .database
            .with_write_connection("release frozen pre-manifest build", move |connection| {
                diesel::sql_query(
                    "UPDATE console_graph_build_runs SET status = 'completed' \
                     WHERE run_id = ?",
                )
                .bind::<BigInt, _>(run_id)
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("release-frozen-pre-manifest-build"),
                })
            })
            .await
            .unwrap();
        snapshots.cleanup_obsolete_generations().await.unwrap();
        index.start_refresh().await.unwrap();
        index.prune_published_orphans().await.unwrap();
        assert_eq!(source_refresh_run_count(&index, old_refresh_id).await, 0);
        assert!(!source_node_exists(&index, &superseded).await);
    }

    #[tokio::test]
    async fn reopen_resumes_relation_ingest_for_a_cached_node_batch() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let child = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("cached relation recovery".to_owned()),
            })
            .await
            .unwrap();
        let node = writer.get_node(&child).await.unwrap();
        let node_json = serde_json::to_string(&node).unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        let path = index.path.clone();
        let child_for_seed = child.clone();
        index
            .database
            .with_write_connection(
                "seed interrupted source relation ingest",
                move |connection| {
                    diesel::sql_query(
                        "INSERT INTO console_graph_source_nodes ( \
                         node_id, parent_id, node_json, relation_cursor_offset, \
                         relation_ingest_complete \
                     ) VALUES (?, ?, ?, 0, 0)",
                    )
                    .bind::<Text, _>(child_for_seed)
                    .bind::<Text, _>(root)
                    .bind::<Text, _>(node_json)
                    .execute(connection)
                    .map(|_| ())
                    .context(QueryGraphSnapshotStoreSnafu { path })
                },
            )
            .await
            .unwrap();
        drop(index);

        let reopened = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        reopened
            .load_nodes(&source, std::slice::from_ref(&child))
            .await
            .unwrap();
        let path = reopened.path.clone();
        let child_for_check = child.clone();
        let state = reopened
            .database
            .with_connection(move |connection| {
                (|| -> QueryResult<_> {
                    let ingest = diesel::sql_query(
                        "SELECT relation_cursor_offset, relation_ingest_complete \
                         FROM console_graph_source_nodes WHERE node_id = ?",
                    )
                    .bind::<Text, _>(&child_for_check)
                    .get_result::<RelationIngestStateRow>(connection)?;
                    let relations = diesel::sql_query(
                        "SELECT COUNT(*) AS count FROM console_graph_source_node_relations \
                         WHERE child_id = ?",
                    )
                    .bind::<Text, _>(&child_for_check)
                    .get_result::<TestCountRow>(connection)?;
                    Ok((ingest, relations.count))
                })()
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        assert_eq!(state.0.relation_ingest_complete, 1);
        assert_eq!(state.1, 1);
    }

    #[tokio::test]
    async fn deleting_branch_waits_for_explicit_source_prune() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        assert_eq!(index.node_count().await.unwrap(), 1);

        writer.delete_branch("main").await.unwrap();
        index
            .refresh_named_batch(&source, &["main".to_owned()])
            .await
            .unwrap();

        assert!(index.is_empty().await.unwrap());
        assert_eq!(index.node_count().await.unwrap(), 1);
        mark_current_source_revision_consumed(&index).await;
        index.start_refresh().await.unwrap();
        index.prune_published_orphans().await.unwrap();
        assert_eq!(index.node_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn tracked_branch_name_pages_merge_bounded_sources_without_duplicates() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed tracked branch merge", move |connection| {
                connection
                    .batch_execute(
                        "INSERT INTO console_graph_source_branches ( \
                             name, head_id, state_json, contribution_generation \
                         ) VALUES \
                             ('a', 'head-a', '{}', 1), \
                             ('c', 'head-c', '{}', 2), \
                             ('e', 'head-e', '{}', 3); \
                         INSERT INTO console_graph_source_refresh_runs ( \
                             refresh_id, branch_name, target_head_id, target_state_json, status \
                         ) VALUES \
                             (900000101, 'b', 'head-b', '{}', 'building'), \
                             (900000102, 'c', 'head-c', '{}', 'building'), \
                             (900000103, 'd', 'head-d', '{}', 'building');",
                    )
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        let page_size = NonZeroUsize::new(3).unwrap();
        assert_eq!(
            index
                .tracked_branch_name_page(None, page_size)
                .await
                .unwrap(),
            ["a", "b", "c"]
        );
        assert_eq!(
            index
                .tracked_branch_name_page(Some("c"), page_size)
                .await
                .unwrap(),
            ["d", "e"]
        );
    }

    #[tokio::test]
    async fn source_refresh_bounded_queries_do_not_use_temporary_sorting() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let path = index.path.clone();
        index
            .database
            .with_connection(move |connection| {
                (|| -> QueryResult<()> {
                    let refresh_batch = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                     SELECT node_id, traversal_kind, node_committed, force_child_scan, \
                            child_cursor_relation_revision, child_cursor_node_id, \
                            parent_cursor_offset, parent_traversal_complete, \
                            child_scan_required, child_high_watermark_frozen, \
                            child_high_watermark_relation_revision, \
                            child_high_watermark_node_id \
                     FROM console_graph_source_refresh_queue \
                     WHERE refresh_id = 1 AND processed = 0 \
                     ORDER BY node_committed DESC, parent_traversal_complete, \
                              node_id, traversal_kind LIMIT 128",
                    )
                    .load::<ExplainQueryPlanRow>(connection)?;
                    assert!(refresh_batch.iter().any(|row| {
                        row.detail.contains(
                            "USING COVERING INDEX console_graph_source_refresh_queue_pending_idx",
                        )
                    }));
                    assert!(
                        refresh_batch
                            .iter()
                            .all(|row| !row.detail.contains("USE TEMP B-TREE"))
                    );

                    let published_branches = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                     SELECT name FROM console_graph_source_branches \
                     WHERE name > 'cursor' ORDER BY name LIMIT 128",
                    )
                    .load::<ExplainQueryPlanRow>(connection)?;
                    assert!(
                        published_branches
                            .iter()
                            .all(|row| !row.detail.contains("USE TEMP B-TREE"))
                    );

                    let building_branches = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                     SELECT branch_name AS name \
                     FROM console_graph_source_refresh_runs \
                          INDEXED BY console_graph_source_refresh_runs_active_branch_idx \
                     WHERE status = 'building' AND branch_name > 'cursor' \
                     ORDER BY branch_name LIMIT 128",
                    )
                    .load::<ExplainQueryPlanRow>(connection)?;
                    assert!(
                        building_branches.iter().any(|row| {
                            row.detail
                                .contains("console_graph_source_refresh_runs_active_branch_idx")
                        }),
                        "{building_branches:?}"
                    );
                    assert!(
                        building_branches
                            .iter()
                            .all(|row| !row.detail.contains("USE TEMP B-TREE"))
                    );

                    let child_rechecks = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                     SELECT recheck.node_id, recheck.traversal_kind, \
                            recheck.contribution_generation AS refresh_id, \
                            CASE WHEN EXISTS ( \
                                SELECT 1 \
                                FROM console_graph_source_refresh_runs AS refresh \
                                INNER JOIN console_graph_source_branches AS branch \
                                    ON branch.name = refresh.branch_name \
                                   AND branch.contribution_generation = \
                                       refresh.target_contribution_generation \
                                INNER JOIN console_graph_source_branch_publications \
                                           AS publication \
                                    ON publication.branch_name = branch.name \
                                   AND publication.target_contribution_generation = \
                                       branch.contribution_generation \
                                WHERE refresh.refresh_id = \
                                          recheck.contribution_generation \
                                  AND refresh.branch_name = recheck.branch_name \
                                  AND refresh.target_contribution_generation = 1 \
                                  AND refresh.status = 'published' \
                                  AND refresh.published_source_revision <= \
                                      publication.source_revision \
                            ) THEN 1 ELSE 0 END AS eligible \
                     FROM console_graph_source_child_rechecks AS recheck \
                          INDEXED BY console_graph_source_child_rechecks_branch_order_idx \
                     WHERE recheck.branch_name = 'main' \
                       AND (recheck.node_id, recheck.traversal_kind, \
                            recheck.contribution_generation) > ('cursor', 'graph', 0) \
                     ORDER BY recheck.node_id, recheck.traversal_kind, \
                              recheck.contribution_generation \
                     LIMIT 128",
                    )
                    .load::<ExplainQueryPlanRow>(connection)?;
                    assert!(child_rechecks.iter().any(|row| {
                        row.detail.contains(
                            "USING COVERING INDEX \
                         console_graph_source_child_rechecks_branch_order_idx",
                        )
                    }));
                    assert!(
                        child_rechecks
                            .iter()
                            .all(|row| !row.detail.contains("USE TEMP B-TREE"))
                    );
                    Ok(())
                })()
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn dynamic_branch_scan_production_queries_are_indexed_without_temporary_sorting() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let path = index.path.clone();
        index
            .database
            .with_connection(move |connection| {
                (|| -> QueryResult<()> {
                    let raw_upper = diesel::sql_query(format!(
                        "EXPLAIN QUERY PLAN {DYNAMIC_SCAN_RAW_UPPER_SQL}"
                    ))
                    .bind::<BigInt, _>(1_i64)
                    .load::<ExplainQueryPlanRow>(connection)?;
                    assert!(
                        raw_upper.iter().any(|row| {
                            row.detail.contains(
                                "console_graph_source_refresh_runs_published_raw_upper_idx",
                            )
                        }),
                        "{raw_upper:?}"
                    );
                    assert!(
                        raw_upper
                            .iter()
                            .all(|row| !row.detail.contains("USE TEMP B-TREE")),
                        "{raw_upper:?}"
                    );

                    let dirty_scan_cleanup = diesel::sql_query(format!(
                        "EXPLAIN QUERY PLAN {DYNAMIC_DIRTY_SCAN_FOR_EVENT_SQL}"
                    ))
                    .bind::<BigInt, _>(1_i64)
                    .load::<ExplainQueryPlanRow>(connection)?;
                    assert!(
                        dirty_scan_cleanup.iter().any(|row| {
                            row.detail
                                .contains("console_graph_source_dynamic_branch_scans_mutation_idx")
                        }),
                        "{dirty_scan_cleanup:?}"
                    );
                    assert!(
                        dirty_scan_cleanup
                            .iter()
                            .all(|row| !row.detail.contains("USE TEMP B-TREE")),
                        "{dirty_scan_cleanup:?}"
                    );

                    for discarding_event in [
                        diesel::sql_query(format!(
                            "EXPLAIN QUERY PLAN {DISCARDING_MUTATION_EVENT_FIRST_SQL}"
                        ))
                        .load::<ExplainQueryPlanRow>(connection)?,
                        diesel::sql_query(format!(
                            "EXPLAIN QUERY PLAN {DISCARDING_MUTATION_EVENT_THROUGH_SQL}"
                        ))
                        .bind::<BigInt, _>(1_i64)
                        .load::<ExplainQueryPlanRow>(connection)?,
                    ] {
                        assert!(
                            discarding_event.iter().any(|row| {
                                row.detail.contains(
                                    "console_graph_source_mutation_event_runs_cleanup_idx",
                                )
                            }),
                            "{discarding_event:?}"
                        );
                        assert!(
                            discarding_event
                                .iter()
                                .all(|row| !row.detail.contains("USE TEMP B-TREE")),
                            "{discarding_event:?}"
                        );
                    }

                    for event_cleanup in [
                        diesel::sql_query(format!(
                            "EXPLAIN QUERY PLAN {MUTATION_EVENT_CLEANUP_FIRST_SQL}"
                        ))
                        .load::<ExplainQueryPlanRow>(connection)?,
                        diesel::sql_query(format!(
                            "EXPLAIN QUERY PLAN {MUTATION_EVENT_CLEANUP_THROUGH_SQL}"
                        ))
                        .bind::<BigInt, _>(1_i64)
                        .load::<ExplainQueryPlanRow>(connection)?,
                    ] {
                        assert!(
                            event_cleanup
                                .iter()
                                .all(|row| !row.detail.contains("USE TEMP B-TREE")),
                            "{event_cleanup:?}"
                        );
                    }

                    let first_candidate = diesel::sql_query(format!(
                        "EXPLAIN QUERY PLAN {DYNAMIC_BRANCH_RAW_PAGE_SQL}"
                    ))
                    .bind::<BigInt, _>(1_i64)
                    .bind::<BigInt, _>(1_i64)
                    .bind::<Text, _>("node")
                    .bind::<BigInt, _>(100_i64)
                    .bind::<BigInt, _>(SOURCE_DYNAMIC_BRANCH_CANDIDATE_PAGE_SIZE as i64)
                    .load::<ExplainQueryPlanRow>(connection)?;
                    assert!(
                        first_candidate.iter().any(|row| {
                            row.detail
                                .contains("console_graph_source_child_rechecks_node_raw_idx")
                        }),
                        "{first_candidate:?}"
                    );
                    assert!(
                        first_candidate.iter().all(|row| {
                            !row.detail.contains("USE TEMP B-TREE")
                                && !row.detail.contains("MATERIALIZE")
                        }),
                        "{first_candidate:?}"
                    );

                    let next_candidate = diesel::sql_query(format!(
                        "EXPLAIN QUERY PLAN {DYNAMIC_BRANCH_RAW_PAGE_AFTER_SQL}"
                    ))
                    .bind::<BigInt, _>(1_i64)
                    .bind::<BigInt, _>(1_i64)
                    .bind::<Text, _>("node")
                    .bind::<BigInt, _>(100_i64)
                    .bind::<Text, _>("branch")
                    .bind::<BigInt, _>(1_i64)
                    .bind::<Text, _>("graph")
                    .bind::<BigInt, _>(SOURCE_DYNAMIC_BRANCH_CANDIDATE_PAGE_SIZE as i64)
                    .load::<ExplainQueryPlanRow>(connection)?;
                    assert!(
                        next_candidate.iter().any(|row| {
                            row.detail
                                .contains("console_graph_source_child_rechecks_node_raw_idx")
                        }),
                        "{next_candidate:?}"
                    );
                    assert!(
                        next_candidate.iter().all(|row| {
                            !row.detail.contains("USE TEMP B-TREE")
                                && !row.detail.contains("MATERIALIZE")
                        }),
                        "{next_candidate:?}"
                    );

                    let first_origin = diesel::sql_query(format!(
                        "EXPLAIN QUERY PLAN {DYNAMIC_ORIGIN_RAW_PAGE_SQL}"
                    ))
                    .bind::<BigInt, _>(1_i64)
                    .bind::<BigInt, _>(1_i64)
                    .bind::<Text, _>("branch")
                    .bind::<BigInt, _>(100_i64)
                    .bind::<BigInt, _>(SOURCE_DYNAMIC_BRANCH_CANDIDATE_PAGE_SIZE as i64)
                    .load::<ExplainQueryPlanRow>(connection)?;
                    assert!(
                        first_origin.iter().any(|row| {
                            row.detail
                                .contains("console_graph_source_child_rechecks_branch_order_idx")
                        }),
                        "{first_origin:?}"
                    );
                    assert!(
                        first_origin.iter().all(|row| {
                            !row.detail.contains("USE TEMP B-TREE")
                                && !row.detail.contains("MATERIALIZE")
                        }),
                        "{first_origin:?}"
                    );

                    let next_origin = diesel::sql_query(format!(
                        "EXPLAIN QUERY PLAN {DYNAMIC_ORIGIN_RAW_PAGE_AFTER_SQL}"
                    ))
                    .bind::<BigInt, _>(1_i64)
                    .bind::<BigInt, _>(1_i64)
                    .bind::<Text, _>("branch")
                    .bind::<BigInt, _>(100_i64)
                    .bind::<Text, _>("node")
                    .bind::<Text, _>("graph")
                    .bind::<BigInt, _>(1_i64)
                    .bind::<BigInt, _>(SOURCE_DYNAMIC_BRANCH_CANDIDATE_PAGE_SIZE as i64)
                    .load::<ExplainQueryPlanRow>(connection)?;
                    assert!(
                        next_origin.iter().any(|row| {
                            row.detail
                                .contains("console_graph_source_child_rechecks_branch_order_idx")
                        }),
                        "{next_origin:?}"
                    );
                    assert!(
                        next_origin.iter().all(|row| {
                            !row.detail.contains("USE TEMP B-TREE")
                                && !row.detail.contains("MATERIALIZE")
                        }),
                        "{next_origin:?}"
                    );

                    let dirty_retention = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                         SELECT MIN(scan.source_revision) \
                         FROM console_graph_source_dynamic_branch_scans AS scan \
                              INDEXED BY \
                                  console_graph_source_dynamic_branch_scans_retention_idx \
                         WHERE scan.status = 'building' \
                           AND scan.scan_kind = 'dirty_parent'",
                    )
                    .load::<ExplainQueryPlanRow>(connection)?;
                    assert!(
                        dirty_retention.iter().any(|row| {
                            row.detail
                                .contains("console_graph_source_dynamic_branch_scans_retention_idx")
                        }),
                        "{dirty_retention:?}"
                    );
                    assert!(
                        dirty_retention
                            .iter()
                            .all(|row| !row.detail.contains("USE TEMP B-TREE")),
                        "{dirty_retention:?}"
                    );

                    let active_retention = diesel::sql_query(
                        "EXPLAIN QUERY PLAN \
                         SELECT MIN(scan.source_revision) \
                         FROM console_graph_source_dynamic_branch_scans AS scan \
                              INDEXED BY \
                                  console_graph_source_dynamic_branch_scans_active_retention_idx \
                         WHERE scan.status = 'building' \
                           AND scan.scan_kind = 'affected' \
                           AND scan.lease_expires_at_ms > ?",
                    )
                    .bind::<BigInt, _>(source_time_ms())
                    .load::<ExplainQueryPlanRow>(connection)?;
                    assert!(
                        active_retention.iter().any(|row| {
                            row.detail.contains(
                                "console_graph_source_dynamic_branch_scans_active_retention_idx",
                            )
                        }),
                        "{active_retention:?}"
                    );
                    assert!(
                        active_retention
                            .iter()
                            .all(|row| !row.detail.contains("USE TEMP B-TREE")),
                        "{active_retention:?}"
                    );
                    Ok(())
                })()
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn stale_refresh_cleanup_production_queries_are_indexed_and_bounded() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let path = index.path.clone();
        index
            .database
            .with_connection(move |connection| {
                (|| -> QueryResult<()> {
                    let candidate_plan = diesel::sql_query(format!(
                        "EXPLAIN QUERY PLAN {SOURCE_REFRESH_CLEANUP_CANDIDATE_PAGE_SQL}"
                    ))
                    .bind::<BigInt, _>(0_i64)
                    .bind::<BigInt, _>(i64::MAX)
                    .bind::<BigInt, _>(
                        i64::try_from(SOURCE_REFRESH_CLEANUP_CANDIDATE_BATCH_SIZE).unwrap(),
                    )
                    .load::<ExplainQueryPlanRow>(connection)?;
                    assert!(
                        candidate_plan.iter().any(|row| {
                            row.detail
                                .contains("sqlite_autoindex_console_graph_source_refresh_runs_1")
                                || row.detail.contains("USING INTEGER PRIMARY KEY")
                        }),
                        "{candidate_plan:?}"
                    );
                    assert!(
                        candidate_plan
                            .iter()
                            .all(|row| !row.detail.contains("USE TEMP B-TREE")),
                        "{candidate_plan:?}"
                    );

                    let protection_plan = diesel::sql_query(format!(
                        "EXPLAIN QUERY PLAN {SOURCE_REFRESH_CLEANUP_PROTECTED_SQL}"
                    ))
                    .bind::<BigInt, _>(1_i64)
                    .bind::<BigInt, _>(1_i64)
                    .bind::<BigInt, _>(1_i64)
                    .bind::<BigInt, _>(1_i64)
                    .bind::<BigInt, _>(1_i64)
                    .bind::<BigInt, _>(1_i64)
                    .bind::<BigInt, _>(source_time_ms())
                    .bind::<BigInt, _>(1_i64)
                    .bind::<BigInt, _>(1_i64)
                    .load::<ExplainQueryPlanRow>(connection)?;
                    for expected_index in [
                        "console_graph_source_branch_change_journal_refresh_idx",
                        "console_graph_source_dynamic_branch_scans_protection_idx",
                        "console_graph_materialization_branches_contribution_idx",
                    ] {
                        assert!(
                            protection_plan
                                .iter()
                                .any(|row| row.detail.contains(expected_index)),
                            "missing {expected_index}: {protection_plan:?}"
                        );
                    }
                    assert!(
                        protection_plan
                            .iter()
                            .all(|row| !row.detail.contains("USE TEMP B-TREE")),
                        "{protection_plan:?}"
                    );

                    for ((table, refresh_column), expected_index) in
                        SOURCE_REFRESH_CLEANUP_DEPENDENT_TABLES.into_iter().zip([
                            "sqlite_autoindex_console_graph_source_refresh_queue_1",
                            "sqlite_autoindex_console_graph_source_refresh_dirty_seeds_1",
                            "console_graph_source_child_rechecks_generation_idx",
                            "console_graph_source_branch_nodes_generation_idx",
                        ])
                    {
                        let delete_plan = diesel::sql_query(format!(
                            "EXPLAIN QUERY PLAN {}",
                            source_refresh_cleanup_delete_sql(table, refresh_column)
                        ))
                        .bind::<BigInt, _>(1_i64)
                        .bind::<BigInt, _>(
                            i64::try_from(SOURCE_REFRESH_CLEANUP_DELETE_BATCH_SIZE).unwrap(),
                        )
                        .load::<ExplainQueryPlanRow>(connection)?;
                        assert!(
                            delete_plan
                                .iter()
                                .any(|row| row.detail.contains(expected_index)),
                            "missing {expected_index}: {delete_plan:?}"
                        );
                        assert!(
                            delete_plan
                                .iter()
                                .all(|row| !row.detail.contains("USE TEMP B-TREE")),
                            "{delete_plan:?}"
                        );
                    }
                    Ok(())
                })()
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn reopening_resumes_child_scan_from_persisted_cursor() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "resumable-child-scan".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer.fork("main", &tool_use).await.unwrap();
        for index in 0..SOURCE_CACHE_BATCH_SIZE + 3 {
            writer
                .append(NewNode {
                    parent: tool_use.clone(),
                    role: Role::System,
                    metadata: None,
                    kind: Kind::Anchor(Anchor::skill_invocation(
                        Vec::new(),
                        SkillInvocationAnchor {
                            skill_name: format!("resumable-child-{index}"),
                            mode: SkillInvocationMode::InheritContext,
                        },
                    )),
                })
                .await
                .unwrap();
        }

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let record = source.graph_branches().await.unwrap().pop().unwrap();
        let index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        let refresh = begin_test_refresh(&index, &source, &record).await;
        let tool_use_item = graph_queue_item(&tool_use);
        index
            .seed_refresh_queue(&refresh, "main", &tool_use_item, false)
            .await
            .unwrap();
        let batch = index.load_refresh_batch(refresh.refresh_id).await.unwrap();
        let nodes = index
            .load_nodes(&source, std::slice::from_ref(&tool_use))
            .await
            .unwrap();
        index
            .commit_refresh_batch(
                &refresh,
                "main",
                &[batch[0].item.clone()],
                &[batch[0].item.clone()],
            )
            .await
            .unwrap();
        let batch = index.load_refresh_batch(refresh.refresh_id).await.unwrap();
        let child_work = batch
            .iter()
            .find(|work| work.item.node_id == tool_use)
            .unwrap();
        index
            .process_parent_page(&refresh, "main", child_work, nodes.get(&tool_use).unwrap())
            .await
            .unwrap();
        let batch = index.load_refresh_batch(refresh.refresh_id).await.unwrap();
        let child_work = batch
            .iter()
            .find(|work| work.item.node_id == tool_use)
            .unwrap();
        assert_eq!(
            index
                .process_child_page(&source, &refresh, "main", child_work)
                .await
                .unwrap(),
            0
        );
        let batch = index.load_refresh_batch(refresh.refresh_id).await.unwrap();
        let child_work = batch
            .iter()
            .find(|work| work.item.node_id == tool_use)
            .unwrap();
        assert_eq!(
            index
                .process_child_page(&source, &refresh, "main", child_work)
                .await
                .unwrap(),
            SOURCE_CACHE_BATCH_SIZE
        );
        let cursor = index
            .load_refresh_batch(refresh.refresh_id)
            .await
            .unwrap()
            .into_iter()
            .find(|work| work.item.node_id == tool_use)
            .unwrap()
            .child_cursor
            .unwrap();
        expire_test_refresh_lease(&index, refresh.refresh_id).await;
        drop(index);

        let mut reopened = PersistentGraphIndex::open(&snapshots, root).await.unwrap();
        let resumed = reopened.matching_refresh(&record).await.unwrap().unwrap();
        assert_eq!(resumed.refresh_id, refresh.refresh_id);
        let reopened_cursor = reopened
            .load_refresh_batch(refresh.refresh_id)
            .await
            .unwrap()
            .into_iter()
            .find(|work| work.item.node_id == tool_use)
            .unwrap()
            .child_cursor
            .unwrap();
        assert_eq!(reopened_cursor, cursor);
        reopened.refresh_records(&source, [record]).await.unwrap();
        let actual = build_graph_snapshot_with_mode(&reopened.graph_store(), 97, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 97, GraphMode::All)
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn reopening_resumes_changed_child_recheck_from_persisted_child_cursor() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "resumable-child-recheck".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer.fork("main", &tool_use).await.unwrap();
        for index in 0..SOURCE_CACHE_BATCH_SIZE + 3 {
            writer
                .append(NewNode {
                    parent: tool_use.clone(),
                    role: Role::System,
                    metadata: None,
                    kind: Kind::Anchor(Anchor::skill_invocation(
                        Vec::new(),
                        SkillInvocationAnchor {
                            skill_name: format!("recheck-child-{index}"),
                            mode: SkillInvocationMode::InheritContext,
                        },
                    )),
                })
                .await
                .unwrap();
        }

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let published = index.published_branch("main").await.unwrap().unwrap();

        writer
            .append(NewNode {
                parent: tool_use.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_invocation(
                    Vec::new(),
                    SkillInvocationAnchor {
                        skill_name: "recheck-child-new".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let relation_revision = source.graph_relation_revision().await.unwrap();
        let sweep_incarnation = "changed-child-recheck-resume";
        let sweep_version = 1;
        index
            .begin_or_claim_source_sweep(sweep_incarnation, sweep_version, relation_revision)
            .await
            .unwrap()
            .unwrap();
        index.fail_next_child_recheck_page_checkpoint();
        assert!(
            index
                .changed_child_recheck(
                    &source,
                    "main",
                    published.contribution_generation,
                    sweep_incarnation,
                    sweep_version,
                    relation_revision,
                )
                .await
                .is_err()
        );
        let interrupted_state = index
            .source_sweep_recheck_state(sweep_incarnation, sweep_version)
            .await
            .unwrap();
        assert_eq!(
            interrupted_state.active_item,
            Some(graph_queue_item(&tool_use))
        );
        let persisted_child_cursor = interrupted_state.child_cursor.unwrap();
        assert!(interrupted_state.raw_cursor.is_some());

        let incarnation = sweep_incarnation.to_owned();
        let path = index.path.clone();
        index
            .database
            .with_write_connection(
                "expire interrupted child recheck sweep",
                move |connection| {
                    diesel::sql_query(
                        "UPDATE console_graph_source_sweep_runs \
                     SET lease_expires_at_ms = 0 \
                     WHERE target_invalidation_incarnation = ? \
                       AND target_invalidation_version = ?",
                    )
                    .bind::<Text, _>(incarnation)
                    .bind::<BigInt, _>(sweep_version)
                    .execute(connection)
                    .map(|_| ())
                    .context(QueryGraphSnapshotStoreSnafu { path })
                },
            )
            .await
            .unwrap();
        drop(index);

        let mut reopened = PersistentGraphIndex::open(&snapshots, root).await.unwrap();
        let mut resumed_sweep = reopened
            .begin_or_claim_source_sweep(sweep_incarnation, sweep_version, relation_revision)
            .await
            .unwrap()
            .unwrap();
        let reopened_state = reopened
            .source_sweep_recheck_state(sweep_incarnation, sweep_version)
            .await
            .unwrap();
        assert_eq!(
            reopened_state.child_cursor.as_ref(),
            Some(&persisted_child_cursor)
        );
        assert_eq!(
            reopened
                .changed_child_recheck(
                    &source,
                    "main",
                    published.contribution_generation,
                    sweep_incarnation,
                    sweep_version,
                    relation_revision,
                )
                .await
                .unwrap(),
            Some(graph_queue_item(&tool_use))
        );
        assert_eq!(
            reopened.child_recheck_page_cursors().first(),
            Some(&Some(persisted_child_cursor))
        );

        reopened
            .checkpoint_source_sweep(&mut resumed_sweep, "reconcile", None, false)
            .await
            .unwrap();
        let incarnation = sweep_incarnation.to_owned();
        let path = reopened.path.clone();
        let recheck_state_cleared = reopened
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT CASE WHEN \
                         branch_recheck_node_cursor IS NULL AND \
                         branch_recheck_traversal_cursor IS NULL AND \
                         branch_recheck_raw_node_cursor IS NULL AND \
                         branch_recheck_raw_traversal_cursor IS NULL AND \
                         branch_recheck_raw_refresh_id_cursor IS NULL AND \
                         branch_recheck_active_node_id IS NULL AND \
                         branch_recheck_active_traversal_kind IS NULL AND \
                         branch_recheck_child_cursor_relation_revision IS NULL AND \
                         branch_recheck_child_cursor_node_id IS NULL \
                     THEN 1 ELSE 0 END AS value \
                     FROM console_graph_source_sweep_runs \
                     WHERE target_invalidation_incarnation = ? \
                       AND target_invalidation_version = ?",
                )
                .bind::<Text, _>(incarnation)
                .bind::<BigInt, _>(sweep_version)
                .get_result::<FlagRow>(connection)
                .map(|row| row.value != 0)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        assert!(recheck_state_cleared);
    }

    #[tokio::test]
    async fn superseded_source_refresh_rejects_stale_queue_writes() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        let record = source.graph_branches().await.unwrap().pop().unwrap();
        let refresh = begin_test_refresh(&index, &source, &record).await;
        index.remove_branch("main").await.unwrap();

        assert!(
            index
                .seed_refresh_queue(&refresh, "main", &graph_queue_item(&root), false)
                .await
                .is_err()
        );
        let generation = refresh.refresh_id;
        let queued = snapshots
            .with_connection(move |connection| {
                console_graph_source_refresh_queue::table
                    .filter(console_graph_source_refresh_queue::refresh_id.eq(generation))
                    .count()
                    .get_result::<i64>(connection)
                    .context(QueryGraphSnapshotStoreSnafu {
                        path: PathBuf::from("stale-source-queue-count"),
                    })
            })
            .await
            .unwrap();
        assert_eq!(queued, 0);
    }

    #[tokio::test]
    async fn reuse_completion_is_scoped_to_traversal_kind() {
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
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let published = index.published_branch("main").await.unwrap().unwrap();
        let work = |traversal| RefreshWorkItem {
            item: QueueItem {
                node_id: root.clone(),
                traversal,
            },
            node_committed: false,
            force_child_scan: false,
            child_cursor: None,
            parent_cursor_offset: 0,
            parent_traversal_complete: false,
            child_scan_required: false,
            child_high_watermark_frozen: false,
            child_high_watermark: None,
        };

        let completed = index
            .previous_completed_work_keys(
                published.contribution_generation,
                &[
                    work(TraversalKind::Graph),
                    work(TraversalKind::SkillSubtree),
                ],
            )
            .await
            .unwrap();

        assert!(completed.contains(&(root.clone(), TRAVERSAL_GRAPH.to_owned())));
        assert!(!completed.contains(&(root, TRAVERSAL_SKILL_SUBTREE.to_owned())));
    }

    #[tokio::test]
    async fn reopening_resumes_incomplete_contribution_generation() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let suffix = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("resume source refresh".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &root, &suffix)
            .await
            .unwrap();
        let record = source
            .graph_branches_by_names(&["main".to_owned()])
            .await
            .unwrap()
            .pop()
            .unwrap();
        let refresh = begin_test_refresh(&index, &source, &record).await;
        let suffix_item = graph_queue_item(&suffix);
        index
            .seed_refresh_queue(&refresh, "main", &suffix_item, false)
            .await
            .unwrap();
        let batch = index.load_refresh_batch(refresh.refresh_id).await.unwrap();
        index
            .load_nodes(&source, std::slice::from_ref(&suffix))
            .await
            .unwrap();
        index
            .commit_refresh_batch(&refresh, "main", &[batch[0].item.clone()], &[])
            .await
            .unwrap();
        expire_test_refresh_lease(&index, refresh.refresh_id).await;
        drop(index);

        let mut reopened = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let resumed = reopened.matching_refresh(&record).await.unwrap().unwrap();
        assert_eq!(resumed.refresh_id, refresh.refresh_id);
        reopened.refresh_records(&source, [record]).await.unwrap();
        let published = reopened.published_branch("main").await.unwrap().unwrap();
        assert_eq!(
            published.contribution_generation,
            refresh.target_contribution_generation
        );
        assert_eq!(
            reopened.published_branch_node_ids("main").await.unwrap(),
            BTreeSet::from([root, suffix])
        );
    }

    #[tokio::test]
    async fn newer_head_finishes_active_refresh_before_extending_it() {
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
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let first_head = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("first active target".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &root, &first_head)
            .await
            .unwrap();
        let first_record = source.graph_branches().await.unwrap().pop().unwrap();
        let active = begin_test_refresh(&index, &source, &first_record).await;
        index
            .seed_refresh_queue(&active, "main", &graph_queue_item(&first_head), false)
            .await
            .unwrap();
        expire_test_refresh_lease(&index, active.refresh_id).await;
        drop(index);

        let second_head = writer
            .append(NewNode {
                parent: first_head.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("newer target".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &first_head, &second_head)
            .await
            .unwrap();
        let latest_record = source.graph_branches().await.unwrap().pop().unwrap();
        let mut reopened = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        reopened
            .refresh_records(&source, [latest_record])
            .await
            .unwrap();

        assert_eq!(reopened.traversed_node_count(), 2);
        assert_eq!(
            reopened.published_branch_node_ids("main").await.unwrap(),
            BTreeSet::from([root, first_head, second_head])
        );
    }

    #[tokio::test]
    async fn source_orphan_gc_bounds_high_fanout_batches_and_resumes_after_reopen() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let target = "gc-high-fanout-target".to_owned();
        let reference_count = SOURCE_CACHE_BATCH_SIZE + 1;
        let target_for_seed = target.clone();
        let path = index.path.clone();
        index
            .database
            .with_write_connection("seed high fanout source orphan", move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_nodes ( \
                                 node_id, parent_id, node_json \
                             ) VALUES (?, '', '{}')",
                        )
                        .bind::<Text, _>(&target_for_seed)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branches ( \
                                 name, head_id, state_json, contribution_generation \
                             ) VALUES ('gc-protected', 'gc-protected-child-0000', '{}', 7000000)",
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_refresh_runs ( \
                                 refresh_id, branch_name, target_head_id, target_state_json, \
                                 status, target_contribution_generation, \
                                 published_source_revision \
                             ) VALUES (7000000, 'gc-protected', \
                                 'gc-protected-child-0000', '{}', 'published', 7000000, 1)",
                        )
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_branch_publications ( \
                                 branch_name, target_contribution_generation, source_revision \
                             ) VALUES ('gc-protected', 7000000, 1)",
                        )
                        .execute(connection)?;
                        for index in 0..reference_count {
                            let child_id = format!("gc-protected-child-{index:04}");
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_nodes ( \
                                     node_id, parent_id, node_json \
                                 ) VALUES (?, ?, '{}')",
                            )
                            .bind::<Text, _>(&child_id)
                            .bind::<Text, _>(&target_for_seed)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_branch_nodes ( \
                                     branch_name, contribution_generation, node_id \
                                 ) VALUES ('gc-protected', 7000000, ?)",
                            )
                            .bind::<Text, _>(&child_id)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_node_relations ( \
                                     parent_id, child_id \
                                 ) VALUES (?, ?)",
                            )
                            .bind::<Text, _>(&target_for_seed)
                            .bind::<Text, _>(&child_id)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_node_relations ( \
                                     parent_id, child_id \
                                 ) VALUES (?, ?)",
                            )
                            .bind::<Text, _>(format!("gc-incoming-parent-{index:04}"))
                            .bind::<Text, _>(&target_for_seed)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_branch_nodes ( \
                                     branch_name, contribution_generation, node_id \
                                 ) VALUES ('gc-stale', ?, ?)",
                            )
                            .bind::<BigInt, _>(8_000_000 + index as i64)
                            .bind::<Text, _>(&target_for_seed)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_child_rechecks ( \
                                     branch_name, contribution_generation, node_id, traversal_kind \
                                 ) VALUES ('gc-stale', ?, ?, 'graph')",
                            )
                            .bind::<BigInt, _>(9_000_000 + index as i64)
                            .bind::<Text, _>(&target_for_seed)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_source_refresh_queue ( \
                                     refresh_id, branch_name, node_id, \
                                     traversal_kind \
                                 ) VALUES (?, 'gc-stale', ?, 'graph')",
                            )
                            .bind::<BigInt, _>(10_000_000 + index as i64)
                            .bind::<Text, _>(&target_for_seed)
                            .execute(connection)?;
                        }
                        diesel::sql_query(
                            "INSERT INTO console_graph_source_orphan_gc_queue (node_id) \
                             VALUES (?)",
                        )
                        .bind::<Text, _>(&target_for_seed)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();

        assert!(!index.prune_published_orphans_step().await.unwrap());
        let target_for_counts = target.clone();
        let path = index.path.clone();
        let counts = index
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT \
                         (SELECT COUNT(*) \
                          FROM console_graph_source_node_relations \
                          WHERE parent_id = ?) AS outgoing_relations, \
                         (SELECT COUNT(*) \
                          FROM console_graph_source_node_relations \
                          WHERE child_id = ?) AS incoming_relations, \
                         (SELECT COUNT(*) \
                          FROM console_graph_source_branch_nodes \
                          WHERE node_id = ?) AS branch_memberships, \
                         (SELECT COUNT(*) \
                          FROM console_graph_source_child_rechecks \
                          WHERE node_id = ?) AS child_rechecks, \
                         (SELECT COUNT(*) \
                          FROM console_graph_source_refresh_queue \
                          WHERE node_id = ?) AS refresh_work_items",
                )
                .bind::<Text, _>(&target_for_counts)
                .bind::<Text, _>(&target_for_counts)
                .bind::<Text, _>(&target_for_counts)
                .bind::<Text, _>(&target_for_counts)
                .bind::<Text, _>(&target_for_counts)
                .get_result::<OrphanGcReferenceCountRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        assert_eq!(counts.outgoing_relations, 1);
        assert_eq!(counts.incoming_relations, 1);
        assert_eq!(counts.branch_memberships, 1);
        assert_eq!(counts.child_rechecks, 1);
        assert_eq!(counts.refresh_work_items, 1);
        drop(index);

        let reopened = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        reopened.prune_published_orphans().await.unwrap();
        let target_for_check = target.clone();
        let path = reopened.path.clone();
        let target_exists = reopened
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT EXISTS( \
                         SELECT 1 FROM console_graph_source_nodes WHERE node_id = ? \
                     ) AS value",
                )
                .bind::<Text, _>(&target_for_check)
                .get_result::<FlagRow>(connection)
                .map(|row| row.value != 0)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap();
        assert!(!target_exists);
        assert_eq!(reopened.node_count().await.unwrap(), reference_count);
    }
}
