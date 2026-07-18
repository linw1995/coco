use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::incremental_build::{IncrementalBuildStats, build_incremental_generation};
use super::snapshot_store::{
    ConsoleGraphSnapshotStore, INCREMENTAL_BUILD_LEASE_HEARTBEAT_INTERVAL, IncrementalBuildLease,
    IncrementalBuildLeasePhase, IncrementalBuildProgress,
};
use super::source_cache::{PersistentGraphIndex, PersistentGraphStore};
use crate::api::{GraphViewportDiffResponse, GraphViewportResponse};
use crate::graph::{
    GraphMode, GraphNode, GraphSnapshot, build_graph_snapshot_with_mode_and_progress,
    graph_node_from_node, provider_context_for_node,
};
use crate::host::api::{GraphViewportDiffRequest, GraphViewportRequest};
use crate::host::render::{
    MaterializedGraphShell, MaterializedGraphShellBranch, MaterializedGraphShellTick,
    ProviderContextItem,
};
use crate::layout::{layout_graph_viewport, layout_graph_viewport_diff};
use crate::publisher::{ConsoleInvalidationBatch, ConsolePublisher};
use coco_mem::{NodeStore, SqliteGraphStore, Store};
use serde::Serialize;
use snafu::prelude::*;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleGraphRebuildState {
    Scheduled,
    Building,
    Ready,
    Failed,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ConsoleGraphRebuildStatus {
    pub mode: GraphMode,
    pub source_version: u64,
    pub state: ConsoleGraphRebuildState,
    pub phase: Option<crate::graph::GraphBuildPhase>,
    pub processed: usize,
    pub total: usize,
    pub message: String,
}

#[derive(Clone)]
pub struct ConsoleGraphCache<S> {
    source: ConsoleGraphSource<S>,
    invalidations: ConsolePublisher,
    ready: ConsolePublisher,
    progress: ConsolePublisher,
    snapshots: Option<ConsoleGraphSnapshotStore>,
    persistent_graph_store: Option<SqliteGraphStore>,
    persistent_index: Option<Arc<tokio::sync::Mutex<PersistentGraphIndex>>>,
    last_notified_mutation_revision: Arc<AtomicI64>,
    rebuild_lock: Arc<tokio::sync::Mutex<()>>,
    publish_lock: Arc<Mutex<()>>,
    state: Arc<Mutex<CacheState>>,
}

#[derive(Clone)]
enum ConsoleGraphSource<S> {
    #[allow(dead_code)]
    Store(S),
    PersistentStore(PathBuf),
}

#[derive(Default)]
struct CacheState {
    anchors: Option<CachedGraphSnapshot>,
    all: Option<CachedGraphSnapshot>,
    anchors_rebuild: Option<ConsoleGraphRebuildStatus>,
    all_rebuild: Option<ConsoleGraphRebuildStatus>,
}

struct CachedGraphSnapshot {
    source_version: u64,
    snapshot: Arc<GraphSnapshot>,
}

struct GraphSnapshotSet {
    anchors: GraphSnapshot,
    all: GraphSnapshot,
}

struct GraphRefreshResult {
    snapshots: Option<GraphSnapshotSet>,
}

struct PreparedPersistentRefresh {
    snapshots: Option<GraphSnapshotSet>,
    stats: IncrementalBuildStats,
}

enum ActivePublicationAction {
    Complete,
    Recheck,
    Build,
}

struct TakenInvalidations {
    publisher: ConsolePublisher,
    batch: Option<ConsoleInvalidationBatch>,
}

const BUILD_LEASE_RELEASE_TIMEOUT: Duration = Duration::from_secs(5);
const BUILD_PROGRESS_POLL_INTERVAL: Duration = Duration::from_secs(1);

struct ActiveBuildLease {
    store: ConsoleGraphSnapshotStore,
    lease: Option<IncrementalBuildLease>,
}

impl ActiveBuildLease {
    fn new(store: ConsoleGraphSnapshotStore, lease: IncrementalBuildLease) -> Self {
        Self {
            store,
            lease: Some(lease),
        }
    }

    fn lease(&self) -> &IncrementalBuildLease {
        self.lease
            .as_ref()
            .expect("active build lease should remain available until completion")
    }

    fn complete(mut self) {
        self.lease = None;
    }

    async fn pause(&mut self) {
        let Some(lease) = self.lease.as_ref().cloned() else {
            return;
        };
        if pause_build_lease(&self.store, &lease).await {
            self.lease = None;
        }
    }
}

impl Drop for ActiveBuildLease {
    fn drop(&mut self) {
        let Some(lease) = self.lease.take() else {
            return;
        };
        let store = self.store.clone();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                rebuild_output_scope = "anchors_and_all",
                build_run_id = lease.generation(),
                build_lease_epoch = lease.lease_epoch(),
                build_frozen_source_revision = lease.frozen_source_revision(),
                "could not schedule console graph build lease pause outside a Tokio runtime",
            );
            return;
        };
        runtime.spawn(async move {
            let release = async {
                let mut retry_delay = Duration::from_millis(25);
                loop {
                    if pause_build_lease(&store, &lease).await {
                        return;
                    }
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(Duration::from_millis(500));
                }
            };
            if tokio::time::timeout(BUILD_LEASE_RELEASE_TIMEOUT, release)
                .await
                .is_err()
            {
                tracing::warn!(
                    rebuild_output_scope = "anchors_and_all",
                    build_run_id = lease.generation(),
                    build_lease_epoch = lease.lease_epoch(),
                    build_frozen_source_revision = lease.frozen_source_revision(),
                    "timed out pausing cancelled console graph build lease",
                );
            }
        });
    }
}

async fn pause_build_lease(
    store: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
) -> bool {
    match store.pause_incremental_build(lease).await {
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(
                rebuild_output_scope = "anchors_and_all",
                build_run_id = lease.generation(),
                build_lease_epoch = lease.lease_epoch(),
                build_frozen_source_revision = lease.frozen_source_revision(),
                error = %error,
                "failed to pause console graph build lease",
            );
            return false;
        }
    }
    true
}

async fn maintain_build_lease(
    store: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
    process_local_invalidation_version: u64,
    mut report_progress: impl FnMut(IncrementalBuildProgress),
) -> crate::Result<()> {
    let mut heartbeat = tokio::time::interval(INCREMENTAL_BUILD_LEASE_HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut progress_poll = tokio::time::interval(BUILD_PROGRESS_POLL_INTERVAL);
    progress_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_stage = None;
    let mut last_progress = None;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                match store.renew_incremental_build_lease(lease).await {
                    Ok(true) => {}
                    Ok(false) => {
                        return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                            column: "incremental_build_lease",
                            value: format!(
                                "build run {} lease epoch {} is no longer owned",
                                lease.generation(),
                                lease.lease_epoch(),
                            ),
                        }
                        .fail();
                    }
                    Err(error) => {
                        tracing::warn!(
                            rebuild_output_scope = "anchors_and_all",
                            build_run_id = lease.generation(),
                            build_lease_epoch = lease.lease_epoch(),
                            build_frozen_source_revision = lease.frozen_source_revision(),
                            process_local_invalidation_version,
                            error = %error,
                            "failed to renew console graph build lease",
                        );
                    }
                }
            }
            _ = progress_poll.tick() => {
                match store.incremental_build_progress(lease).await {
                    Ok(progress) => {
                        if last_stage != Some(progress.stage) {
                            tracing::info!(
                                rebuild_output_scope = "anchors_and_all",
                                process_local_invalidation_version,
                                build_run_id = lease.generation(),
                                build_lease_epoch = lease.lease_epoch(),
                                build_frozen_source_revision = lease.frozen_source_revision(),
                                build_stage = progress.stage,
                                build_stage_progress_unit = progress.unit,
                                build_stage_completed_unit_count = progress.completed_units,
                                build_stage_total_unit_count = progress.total_units,
                                build_stage_total_unit_count_available = progress.total_units > 0,
                                build_stage_detail = progress.message,
                                "console graph build stage changed",
                            );
                            last_stage = Some(progress.stage);
                        } else if last_progress != Some(progress) {
                            tracing::debug!(
                                rebuild_output_scope = "anchors_and_all",
                                process_local_invalidation_version,
                                build_run_id = lease.generation(),
                                build_lease_epoch = lease.lease_epoch(),
                                build_frozen_source_revision = lease.frozen_source_revision(),
                                build_stage = progress.stage,
                                build_stage_progress_unit = progress.unit,
                                build_stage_completed_unit_count = progress.completed_units,
                                build_stage_total_unit_count = progress.total_units,
                                build_stage_total_unit_count_available = progress.total_units > 0,
                                build_stage_detail = progress.message,
                                "console graph build stage progress",
                            );
                        }
                        last_progress = Some(progress);
                        report_progress(progress);
                    }
                    Err(error) => {
                        tracing::warn!(
                            rebuild_output_scope = "anchors_and_all",
                            process_local_invalidation_version,
                            build_run_id = lease.generation(),
                            build_lease_epoch = lease.lease_epoch(),
                            build_frozen_source_revision = lease.frozen_source_revision(),
                            error = %error,
                            "failed to read console graph build progress",
                        );
                    }
                }
            }
        }
    }
}

impl TakenInvalidations {
    fn new(publisher: ConsolePublisher, batch: ConsoleInvalidationBatch) -> Self {
        Self {
            publisher,
            batch: Some(batch),
        }
    }

    fn batch(&self) -> &ConsoleInvalidationBatch {
        self.batch
            .as_ref()
            .expect("taken invalidations should remain available until committed")
    }

    fn commit(mut self) {
        self.batch = None;
    }
}

impl Drop for TakenInvalidations {
    fn drop(&mut self) {
        if let Some(batch) = self.batch.take() {
            self.publisher.restore_invalidations(batch);
        }
    }
}

macro_rules! log_rebuild_status {
    ($status:expr) => {{
        let status = &$status;
        debug_assert_ne!(
            status.state,
            ConsoleGraphRebuildState::Failed,
            "shared rebuild failures must be logged once by the refresh worker",
        );
        if status.state != ConsoleGraphRebuildState::Failed
            && should_log_rebuild_status_at_info(status)
        {
            if status.total > 0 {
                tracing::info!(
                    graph_mode = status.mode.as_query_value(),
                    process_local_invalidation_version = status.source_version,
                    build_state = rebuild_state_name(status.state),
                    build_phase = rebuild_phase_name(status.phase),
                    build_phase_progress_unit = rebuild_phase_progress_unit(status.phase),
                    build_phase_completed_unit_count = status.processed,
                    build_phase_total_unit_count = status.total,
                    build_phase_progress_percent = rebuild_progress_percent(status),
                    status_detail = %status.message,
                    "console graph rebuild status",
                );
            } else {
                tracing::info!(
                    graph_mode = status.mode.as_query_value(),
                    process_local_invalidation_version = status.source_version,
                    build_state = rebuild_state_name(status.state),
                    build_phase = rebuild_phase_name(status.phase),
                    status_detail = %status.message,
                    "console graph rebuild status",
                );
            }
        } else if status.state != ConsoleGraphRebuildState::Failed {
            if status.total > 0 {
                tracing::debug!(
                    graph_mode = status.mode.as_query_value(),
                    process_local_invalidation_version = status.source_version,
                    build_state = rebuild_state_name(status.state),
                    build_phase = rebuild_phase_name(status.phase),
                    build_phase_progress_unit = rebuild_phase_progress_unit(status.phase),
                    build_phase_completed_unit_count = status.processed,
                    build_phase_total_unit_count = status.total,
                    build_phase_progress_percent = rebuild_progress_percent(status),
                    status_detail = %status.message,
                    "console graph rebuild progress",
                );
            } else {
                tracing::debug!(
                    graph_mode = status.mode.as_query_value(),
                    process_local_invalidation_version = status.source_version,
                    build_state = rebuild_state_name(status.state),
                    build_phase = rebuild_phase_name(status.phase),
                    status_detail = %status.message,
                    "console graph rebuild progress",
                );
            }
        }
    }};
}

macro_rules! set_rebuild_status {
    ($cache:expr, $status:expr) => {{
        let status = $status;
        if $cache.set_rebuild_status(&status) {
            log_rebuild_status!(status);
        }
    }};
}

impl<S> ConsoleGraphCache<S>
where
    S: Store + Clone + Send + Sync + 'static,
{
    #[allow(dead_code)]
    pub(crate) fn new(store: S, invalidations: ConsolePublisher) -> Self {
        Self {
            source: ConsoleGraphSource::Store(store),
            invalidations,
            ready: ConsolePublisher::new(),
            progress: ConsolePublisher::new(),
            snapshots: None,
            persistent_graph_store: None,
            persistent_index: None,
            last_notified_mutation_revision: Arc::new(AtomicI64::new(0)),
            rebuild_lock: Arc::new(tokio::sync::Mutex::new(())),
            publish_lock: Arc::new(Mutex::new(())),
            state: Arc::new(Mutex::new(CacheState::default())),
        }
    }

    pub async fn new_with_persistent_store_path(
        _store: S,
        invalidations: ConsolePublisher,
        path: PathBuf,
    ) -> crate::Result<Self> {
        let persistent_graph_store = SqliteGraphStore::open_read_only(&path)
            .await
            .context(crate::error::StoreSnafu)?;
        let snapshots = ConsoleGraphSnapshotStore::open(&path).await?;
        let persistent_index =
            PersistentGraphIndex::open(&snapshots, persistent_graph_store.root_id()).await?;
        let persisted_version = latest_persistent_materialization_version(&snapshots).await?;
        let resumable_version = snapshots.latest_resumable_build_version().await?;
        let ready = ConsolePublisher::new();
        if let Some(version) = persisted_version {
            ready.advance_to(version);
        }
        let startup_version = persisted_version
            .map(|version| version.saturating_add(1))
            .unwrap_or(1)
            .max(resumable_version.unwrap_or(0));
        invalidations.notify_durable_change_at_least(startup_version);
        Ok(Self {
            source: ConsoleGraphSource::PersistentStore(path),
            invalidations,
            ready,
            progress: ConsolePublisher::new(),
            snapshots: Some(snapshots),
            persistent_graph_store: Some(persistent_graph_store),
            persistent_index: Some(Arc::new(tokio::sync::Mutex::new(persistent_index))),
            last_notified_mutation_revision: Arc::new(AtomicI64::new(0)),
            rebuild_lock: Arc::new(tokio::sync::Mutex::new(())),
            publish_lock: Arc::new(Mutex::new(())),
            state: Arc::new(Mutex::new(CacheState::default())),
        })
    }

    pub fn current_version(&self) -> u64 {
        self.ready.current_version()
    }

    pub async fn current_viewport_version(&self, mode: GraphMode) -> u64 {
        match self.snapshots.as_ref() {
            Some(snapshots) => snapshots
                .latest_materialization_version(mode)
                .await
                .ok()
                .flatten()
                .unwrap_or(0),
            None => self.current_version(),
        }
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.ready.subscribe()
    }

    pub fn subscribe_progress(&self) -> tokio::sync::watch::Receiver<u64> {
        self.progress.subscribe()
    }

    pub fn rebuild_statuses(&self) -> Vec<ConsoleGraphRebuildStatus> {
        let state = self
            .state
            .lock()
            .expect("console graph cache lock poisoned");
        [state.anchors_rebuild.clone(), state.all_rebuild.clone()]
            .into_iter()
            .flatten()
            .collect()
    }

    pub fn subscribe_invalidations(&self) -> tokio::sync::watch::Receiver<u64> {
        self.invalidations.subscribe()
    }

    pub(crate) async fn poll_durable_graph_mutations(&self) -> crate::Result<bool> {
        let (Some(store), Some(index)) = (&self.persistent_graph_store, &self.persistent_index)
        else {
            return Ok(false);
        };
        let current_revision = store
            .graph_mutation_revision()
            .await
            .context(crate::error::StoreSnafu)?;
        let consumed_revision = index
            .lock()
            .await
            .consumed_graph_mutation_revision()
            .await?;
        if current_revision <= consumed_revision {
            return Ok(false);
        }
        let previously_notified = self
            .last_notified_mutation_revision
            .fetch_max(current_revision, Ordering::SeqCst);
        if current_revision <= previously_notified {
            return Ok(false);
        }
        let process_local_invalidation_version = self.invalidations.notify_durable_change();
        tracing::info!(
            source_mutation_revision = current_revision,
            consumed_source_mutation_revision = consumed_revision,
            process_local_invalidation_version,
            "durable console graph mutation detected",
        );
        Ok(true)
    }

    #[cfg(test)]
    pub(crate) async fn fail_next_branch_refresh(&self, branch: impl Into<String>) {
        self.persistent_index
            .as_ref()
            .expect("persistent graph index should be available")
            .lock()
            .await
            .fail_next_branch_refresh(branch);
    }

    pub async fn rebuild_requested_modes(&self) -> crate::Result<()> {
        let _guard = self.rebuild_lock.lock().await;
        let source_version = self.invalidations.current_version();
        if self.graph_current(source_version).await {
            let invalidations = self.invalidations.take_invalidations();
            if invalidations.version <= source_version {
                self.publish_graph_ready(source_version);
                return Ok(());
            }
            self.invalidations.restore_invalidations(invalidations);
        }
        self.run_refresh_worker().await
    }

    #[cfg(test)]
    pub(crate) async fn snapshot_current(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Arc<GraphSnapshot>> {
        loop {
            self.rebuild_requested_modes().await?;
            let source_version = self.invalidations.current_version();
            if let Some(snapshot) = self.cached_snapshot(mode, source_version) {
                return Ok(snapshot);
            }
            self.fail_if_materialization_failed(mode, source_version)?;
        }
    }

    pub(crate) fn snapshot_current_ready_or_schedule(
        &self,
        mode: GraphMode,
    ) -> Option<Arc<GraphSnapshot>> {
        let source_version = self.invalidations.current_version();
        if let Some(snapshot) = self.cached_snapshot(mode, source_version) {
            return Some(snapshot);
        }
        self.latest_cached_snapshot(mode)
    }

    pub(crate) fn snapshot_current_ready(&self, mode: GraphMode) -> Option<Arc<GraphSnapshot>> {
        let source_version = self.invalidations.current_version();
        self.cached_snapshot(mode, source_version)
    }

    pub fn has_materialized_viewports(&self) -> bool {
        self.snapshots.is_some()
    }

    pub async fn node_detail_current_ready_or_schedule(
        &self,
        mode: GraphMode,
        target: &str,
    ) -> crate::Result<Option<GraphNode>> {
        let Some(snapshots) = &self.snapshots else {
            return Ok(self
                .snapshot_current_ready_or_schedule(mode)
                .and_then(|snapshot| {
                    snapshot
                        .nodes
                        .iter()
                        .find(|node| crate::graph::node_target_id(&node.id) == target)
                        .map(|node| GraphNode {
                            id: node.id.clone(),
                            short_id: node.short_id.clone(),
                            kind: node.kind.clone(),
                            role: node.role.clone(),
                            created_at: node.created_at.clone(),
                            created_at_ns: node.created_at_ns,
                            content: node.content.clone(),
                            summary: node.summary.clone(),
                            labels: node.labels.clone(),
                            provider_context_ids: node.provider_context_ids.clone(),
                        })
                }));
        };
        let reference = snapshots.materialized_node_reference(mode, target).await?;
        let (node_id, labels) = match reference {
            Some(reference) => (reference.node_id, reference.labels),
            None => {
                let Some(node_id) = node_id_from_graph_target(target) else {
                    return Ok(None);
                };
                if snapshots.has_materialization(mode).await?
                    && !self
                        .node_is_in_materialized_provider_context(snapshots, mode, &node_id)
                        .await?
                {
                    return Ok(None);
                }
                (node_id, Vec::new())
            }
        };
        match self.source.clone().get_node(&node_id).await {
            Ok(node) => Ok(Some(graph_node_from_node(node, labels, Vec::new()))),
            Err(_) => Ok(None),
        }
    }

    async fn node_is_in_materialized_provider_context(
        &self,
        snapshots: &ConsoleGraphSnapshotStore,
        mode: GraphMode,
        node_id: &str,
    ) -> crate::Result<bool> {
        let Some(selection) = self
            .source
            .clone()
            .provider_context_for_node(node_id, None)
            .await?
        else {
            return Ok(false);
        };
        let node_ids = selection
            .context
            .nodes
            .iter()
            .map(|node| node.id.clone())
            .collect::<BTreeSet<_>>();
        Ok(!snapshots
            .materialized_node_points(mode, &node_ids)
            .await?
            .is_empty())
    }

    pub(crate) async fn provider_context_current_ready_or_schedule(
        &self,
        mode: GraphMode,
        target: &str,
        context: Option<&str>,
    ) -> crate::Result<Option<Vec<ProviderContextItem>>> {
        let Some(snapshots) = &self.snapshots else {
            return Ok(None);
        };
        let materialized_reference = snapshots.materialized_node_reference(mode, target).await?;
        let target_was_materialized = materialized_reference.is_some();
        let Some(target_node_id) = materialized_reference
            .map(|reference| reference.node_id)
            .or_else(|| node_id_from_graph_target(target))
        else {
            return Ok(None);
        };
        let Some(selection) = self
            .source
            .clone()
            .provider_context_for_node(&target_node_id, context)
            .await?
        else {
            return Ok(target_was_materialized.then(Vec::new));
        };

        let node_ids = selection
            .context
            .nodes
            .iter()
            .map(|node| node.id.clone())
            .collect::<BTreeSet<_>>();
        let points = snapshots.materialized_node_points(mode, &node_ids).await?;
        if points.is_empty() {
            return Ok(None);
        }

        let context_target = selection.context.id;
        let items = selection
            .context
            .nodes
            .into_iter()
            .map(|mut node| {
                let point = points.get(&node.id).copied();
                node.visible = point.is_some();
                ProviderContextItem {
                    context_target: context_target.clone(),
                    selected: node.id == selection.selected_id,
                    node,
                    point,
                }
            })
            .collect();
        Ok(Some(items))
    }

    pub(crate) async fn materialized_shell_current_ready_or_schedule(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Option<MaterializedGraphShell>> {
        let Some(snapshots) = &self.snapshots else {
            return Ok(None);
        };
        let Some(facts) = snapshots.materialized_shell_facts(mode).await? else {
            return Ok(None);
        };
        let version = facts.version;
        let node_count = facts.node_count;
        let edge_count = facts.edge_count;
        let branches = facts
            .branches
            .into_iter()
            .map(|branch| MaterializedGraphShellBranch {
                name: branch.name,
                head_short_id: crate::graph::shorten_id(&branch.head_id),
                state: branch.state,
            })
            .collect();
        let time_ticks = facts
            .nodes
            .into_iter()
            .map(|node| MaterializedGraphShellTick {
                time_ns: node.created_at_ns,
                label: node.created_at,
                node_target: node.node_target,
                point: node.point,
            })
            .collect();
        Ok(Some(MaterializedGraphShell {
            version,
            mode,
            node_count,
            edge_count,
            branches,
            time_ticks,
        }))
    }

    pub(crate) async fn snapshot_after(
        &self,
        mode: GraphMode,
        observed_version: u64,
    ) -> crate::Result<Arc<GraphSnapshot>> {
        self.rebuild_requested_modes().await?;
        loop {
            let mut rx = self.ready.subscribe();
            let mut invalidations = self.invalidations.subscribe();
            if let Some(snapshot) = self.snapshot_current_ready_or_schedule(mode)
                && snapshot.version > observed_version
            {
                return Ok(snapshot);
            }
            tokio::select! {
                changed = rx.changed() => {
                    if changed.is_err() {
                        continue;
                    }
                }
                changed = invalidations.changed() => {
                    if changed.is_err() {
                        continue;
                    }
                    self.rebuild_requested_modes().await?;
                }
            }
        }
    }

    pub async fn viewport_current_ready_or_schedule(
        &self,
        mode: GraphMode,
        request: GraphViewportRequest,
    ) -> Option<GraphViewportResponse> {
        if let Some(snapshots) = &self.snapshots {
            return snapshots
                .latest_viewport(mode, request)
                .await
                .ok()
                .flatten();
        }
        self.snapshot_current_ready_or_schedule(mode)
            .map(|snapshot| layout_graph_viewport(&snapshot, request))
    }

    pub async fn viewport_after(
        &self,
        mode: GraphMode,
        observed_version: u64,
        request: GraphViewportRequest,
    ) -> crate::Result<GraphViewportResponse> {
        if self.snapshots.is_none() {
            let snapshot = self.snapshot_after(mode, observed_version).await?;
            return Ok(layout_graph_viewport(&snapshot, request));
        }
        loop {
            let mut rx = self.ready.subscribe();
            let mut progress = self.progress.subscribe();
            if let Some(response) = self.viewport_current_ready_or_schedule(mode, request).await
                && response.version > observed_version
            {
                return Ok(response);
            }
            self.fail_if_materialization_failed(mode, observed_version)?;
            tokio::select! {
                changed = rx.changed() => {
                    if changed.is_err() {
                        continue;
                    }
                }
                changed = progress.changed() => {
                    if changed.is_err() {
                        continue;
                    }
                }
            }
        }
    }

    pub async fn viewport_diff_current_ready_or_schedule(
        &self,
        mode: GraphMode,
        request: GraphViewportDiffRequest,
    ) -> Option<GraphViewportDiffResponse> {
        if let Some(snapshots) = &self.snapshots {
            return snapshots
                .latest_viewport_diff(mode, request)
                .await
                .ok()
                .flatten();
        }
        self.snapshot_current_ready_or_schedule(mode)
            .map(|snapshot| layout_graph_viewport_diff(&snapshot, request))
    }

    pub async fn viewport_diff_after(
        &self,
        mode: GraphMode,
        observed_version: u64,
        request: GraphViewportDiffRequest,
    ) -> crate::Result<GraphViewportDiffResponse> {
        if self.snapshots.is_none() {
            let snapshot = self.snapshot_after(mode, observed_version).await?;
            return Ok(layout_graph_viewport_diff(&snapshot, request));
        }
        loop {
            let mut rx = self.ready.subscribe();
            let mut progress = self.progress.subscribe();
            if let Some(response) = self
                .viewport_diff_current_ready_or_schedule(mode, request.clone())
                .await
                && response.version > observed_version
            {
                return Ok(response);
            }
            self.fail_if_materialization_failed(mode, observed_version)?;
            tokio::select! {
                changed = rx.changed() => {
                    if changed.is_err() {
                        continue;
                    }
                }
                changed = progress.changed() => {
                    if changed.is_err() {
                        continue;
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub async fn current_snapshot(&self, mode: GraphMode) -> Arc<GraphSnapshot> {
        let snapshot = self
            .snapshot_current(mode)
            .await
            .expect("graph snapshot should build");
        self.wait_for_materialization_current(mode, snapshot.version)
            .await;
        snapshot
    }

    #[cfg(test)]
    async fn wait_for_materialization_current(&self, mode: GraphMode, source_version: u64) {
        if self.snapshots.is_none() {
            return;
        }
        for _ in 0..50 {
            if self.materialization_current(mode, source_version).await {
                return;
            }
            if self.rebuild_statuses().iter().any(|status| {
                status.mode == mode
                    && status.source_version == source_version
                    && status.state == ConsoleGraphRebuildState::Failed
            }) {
                panic!("graph materialization should not fail");
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("graph materialization should become current");
    }

    fn publish_ready_version(&self, source_version: u64) -> u64 {
        let _guard = self
            .publish_lock
            .lock()
            .expect("console graph publish lock poisoned");
        let target = source_version;
        while self.ready.current_version() < target {
            self.ready.mark_changed();
        }
        self.ready.current_version()
    }

    fn cached_snapshot(&self, mode: GraphMode, source_version: u64) -> Option<Arc<GraphSnapshot>> {
        let state = self
            .state
            .lock()
            .expect("console graph cache lock poisoned");
        state
            .slot(mode)
            .as_ref()
            .filter(|cached| cached.source_version == source_version)
            .map(|cached| cached.snapshot.clone())
    }

    fn latest_cached_snapshot(&self, mode: GraphMode) -> Option<Arc<GraphSnapshot>> {
        let state = self
            .state
            .lock()
            .expect("console graph cache lock poisoned");
        state
            .slot(mode)
            .as_ref()
            .map(|cached| cached.snapshot.clone())
    }

    fn store_cached_snapshot(
        &self,
        mode: GraphMode,
        source_version: u64,
        snapshot: Arc<GraphSnapshot>,
    ) {
        let mut state = self
            .state
            .lock()
            .expect("console graph cache lock poisoned");
        *state.slot_mut(mode) = Some(CachedGraphSnapshot {
            source_version,
            snapshot,
        });
    }

    async fn graph_current(&self, source_version: u64) -> bool {
        if self.snapshots.is_some() {
            for mode in [GraphMode::Anchors, GraphMode::All] {
                if !self.materialization_current(mode, source_version).await {
                    return false;
                }
            }
            return true;
        }
        [GraphMode::Anchors, GraphMode::All]
            .into_iter()
            .all(|mode| self.cached_snapshot(mode, source_version).is_some())
    }

    async fn run_refresh_worker(&self) -> crate::Result<()> {
        loop {
            let (source_version, batch) = take_refresh_worker_batch(&self.invalidations, || {});
            for mode in [GraphMode::Anchors, GraphMode::All] {
                set_rebuild_status!(
                    self,
                    rebuild_status(
                        mode,
                        source_version,
                        ConsoleGraphRebuildState::Scheduled,
                        None,
                        0,
                        0,
                        "Graph snapshot scheduled",
                    )
                );
            }
            let invalidations = TakenInvalidations::new(self.invalidations.clone(), batch);
            let result = self
                .refresh_snapshot_set(source_version, invalidations.batch())
                .await;
            match result {
                Ok(result) => {
                    invalidations.commit();
                    if let Some(snapshots) = result.snapshots {
                        self.store_cached_snapshot(
                            GraphMode::Anchors,
                            source_version,
                            Arc::new(snapshots.anchors),
                        );
                        self.store_cached_snapshot(
                            GraphMode::All,
                            source_version,
                            Arc::new(snapshots.all),
                        );
                    }
                    self.publish_graph_ready(source_version);
                }
                Err(error) => {
                    self.record_refresh_worker_failure(source_version, &error)
                        .await;
                    return Err(error);
                }
            }

            if self.invalidations.current_version() <= source_version {
                return Ok(());
            }
        }
    }

    async fn record_refresh_worker_failure(&self, source_version: u64, error: &crate::Error) {
        let status_detail = error.to_string();
        for mode in [GraphMode::Anchors, GraphMode::All] {
            self.set_rebuild_status(&rebuild_status(
                mode,
                source_version,
                ConsoleGraphRebuildState::Failed,
                None,
                0,
                0,
                &status_detail,
            ));
        }
        self.log_refresh_worker_failure(source_version, error).await;
    }

    async fn log_refresh_worker_failure(&self, source_version: u64, error: &crate::Error) {
        let Some(snapshot_store) = self.snapshots.as_ref() else {
            tracing::warn!(
                rebuild_output_scope = "anchors_and_all",
                rebuild_execution_scope = "in_memory_shared",
                process_local_invalidation_version = source_version,
                build_context_available = false,
                error = %error,
                "console graph rebuild failed",
            );
            return;
        };
        match snapshot_store
            .latest_incremental_build_diagnostic_context()
            .await
        {
            Ok(Some(context)) => {
                tracing::warn!(
                    rebuild_output_scope = "anchors_and_all",
                    rebuild_execution_scope = "persistent_shared",
                    process_local_invalidation_version = source_version,
                    build_context_available = true,
                    build_run_id = context.build_run_id,
                    build_lease_epoch = context.build_lease_epoch,
                    build_target_process_local_invalidation_version =
                        context.target_process_local_invalidation_version,
                    build_frozen_source_revision = context.frozen_source_revision,
                    persisted_build_state = %context.persisted_build_state,
                    persisted_build_stage = %context.persisted_build_stage,
                    error = %error,
                    "console graph rebuild failed",
                );
            }
            Ok(None) => {
                tracing::warn!(
                    rebuild_output_scope = "anchors_and_all",
                    rebuild_execution_scope = "persistent_shared",
                    process_local_invalidation_version = source_version,
                    build_context_available = false,
                    error = %error,
                    "console graph rebuild failed",
                );
            }
            Err(context_error) => {
                tracing::warn!(
                    rebuild_output_scope = "anchors_and_all",
                    rebuild_execution_scope = "persistent_shared",
                    process_local_invalidation_version = source_version,
                    build_context_available = false,
                    build_context_lookup_error = %context_error,
                    error = %error,
                    "console graph rebuild failed",
                );
            }
        }
    }

    fn publish_graph_ready(&self, source_version: u64) {
        self.publish_ready_version(source_version);
        for mode in [GraphMode::Anchors, GraphMode::All] {
            set_rebuild_status!(self, ready_rebuild_status(mode, source_version));
        }
    }

    async fn refresh_snapshot_set(
        &self,
        source_version: u64,
        invalidations: &ConsoleInvalidationBatch,
    ) -> crate::Result<GraphRefreshResult> {
        let uses_materialized_store = self.snapshots.is_some();
        for mode in [GraphMode::Anchors, GraphMode::All] {
            set_rebuild_status!(
                self,
                rebuild_status(
                    mode,
                    source_version,
                    ConsoleGraphRebuildState::Building,
                    None,
                    0,
                    0,
                    if uses_materialized_store {
                        "Building graph materialization"
                    } else {
                        "Building graph snapshot"
                    },
                )
            );
        }

        if let (Some(store), Some(index), Some(snapshot_store)) = (
            &self.persistent_graph_store,
            &self.persistent_index,
            &self.snapshots,
        ) {
            return self
                .refresh_persistent_snapshot_set(
                    store,
                    index,
                    snapshot_store,
                    source_version,
                    invalidations,
                )
                .await;
        }

        let anchors = self
            .build_snapshot_for_mode(None, GraphMode::Anchors, source_version)
            .await?;
        let all = self
            .build_snapshot_for_mode(None, GraphMode::All, source_version)
            .await?;
        Ok(GraphRefreshResult {
            snapshots: Some(GraphSnapshotSet { anchors, all }),
        })
    }

    async fn refresh_persistent_snapshot_set(
        &self,
        store: &SqliteGraphStore,
        index: &Arc<tokio::sync::Mutex<PersistentGraphIndex>>,
        snapshot_store: &ConsoleGraphSnapshotStore,
        source_version: u64,
        invalidations: &ConsoleInvalidationBatch,
    ) -> crate::Result<GraphRefreshResult> {
        for mode in [GraphMode::Anchors, GraphMode::All] {
            set_rebuild_status!(
                self,
                rebuild_status(
                    mode,
                    source_version,
                    ConsoleGraphRebuildState::Building,
                    Some(crate::graph::GraphBuildPhase::Branches),
                    0,
                    0,
                    "Refreshing graph source contributions",
                )
            );
        }
        {
            let mut index = index.lock().await;
            index.start_refresh().await?;
            index
                .refresh_invalidation_batch(store, invalidations)
                .await?;
        }
        if let Err(error) = snapshot_store.cleanup_abandoned_generations().await {
            tracing::warn!(
                process_local_invalidation_version = source_version,
                error = %error,
                "failed to clean abandoned console graph build runs",
            );
        }
        let mut prepared_snapshots = None;
        loop {
            match self
                .active_publication_action(snapshot_store, source_version)
                .await?
            {
                ActivePublicationAction::Complete => {
                    return Ok(GraphRefreshResult {
                        snapshots: prepared_snapshots,
                    });
                }
                ActivePublicationAction::Recheck => continue,
                ActivePublicationAction::Build => {}
            }
            let Some(lease) = snapshot_store
                .acquire_incremental_build_lease(source_version)
                .await?
            else {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                continue;
            };
            if lease.phase() == IncrementalBuildLeasePhase::Compacting {
                self.resume_persistent_compaction(snapshot_store, source_version, lease)
                    .await?;
                continue;
            }
            prepared_snapshots = self
                .build_and_publish_persistent_lease(
                    store,
                    index,
                    snapshot_store,
                    source_version,
                    lease,
                )
                .await?;
        }
    }

    async fn active_publication_action(
        &self,
        snapshot_store: &ConsoleGraphSnapshotStore,
        source_version: u64,
    ) -> crate::Result<ActivePublicationAction> {
        let active = snapshot_store.active_publication_state().await?;
        let active_has_both_modes =
            active.anchors_source_version.is_some() && active.all_source_version.is_some();
        if !active_has_both_modes
            || !active.matches_current_source
            || active.overlay_compaction_pending
        {
            return Ok(ActivePublicationAction::Build);
        }
        let active_version_is_current = active
            .anchors_source_version
            .is_some_and(|version| version >= source_version)
            && active
                .all_source_version
                .is_some_and(|version| version >= source_version);
        if active_version_is_current {
            return Ok(ActivePublicationAction::Complete);
        }
        if !snapshot_store
            .advance_current_publication_version(source_version)
            .await?
        {
            return Ok(ActivePublicationAction::Build);
        }
        tracing::info!(
            rebuild_output_scope = "anchors_and_all",
            process_local_invalidation_version = source_version,
            active_graph_generation = active.graph_generation,
            published_source_revision = active.source_revision.unwrap_or_default(),
            "current console graph publication version advanced",
        );
        Ok(ActivePublicationAction::Recheck)
    }

    async fn resume_persistent_compaction(
        &self,
        snapshot_store: &ConsoleGraphSnapshotStore,
        source_version: u64,
        lease: IncrementalBuildLease,
    ) -> crate::Result<()> {
        let target_source_version = lease.target_source_version();
        tracing::info!(
            rebuild_output_scope = "anchors_and_all",
            process_local_invalidation_version = source_version,
            build_target_process_local_invalidation_version = target_source_version,
            build_run_id = lease.generation(),
            build_lease_epoch = lease.lease_epoch(),
            build_frozen_source_revision = lease.frozen_source_revision(),
            "resuming interrupted console graph publication compaction",
        );
        let mut active_lease = ActiveBuildLease::new(snapshot_store.clone(), lease.clone());
        let publication = match snapshot_store
            .publish_incremental_generation(&lease, target_source_version)
            .await
        {
            Ok(publication) => publication,
            Err(error) => {
                active_lease.pause().await;
                return Err(error);
            }
        };
        active_lease.complete();
        tracing::info!(
            rebuild_output_scope = "anchors_and_all",
            process_local_invalidation_version = source_version,
            build_target_process_local_invalidation_version = target_source_version,
            build_run_id = publication.build_run_id,
            build_lease_epoch = lease.lease_epoch(),
            build_frozen_source_revision = lease.frozen_source_revision(),
            published_graph_generation = publication.published_graph_generation,
            graph_publication_epoch = publication.publication_epoch,
            published_source_revision = publication.published_source_revision,
            "interrupted console graph publication compaction completed",
        );
        if let Err(error) = snapshot_store.cleanup_completed_build_work().await {
            tracing::warn!(
                build_run_id = publication.build_run_id,
                error = %error,
                "failed to clean recovered console graph build work",
            );
        }
        Ok(())
    }

    async fn build_and_publish_persistent_lease(
        &self,
        store: &SqliteGraphStore,
        index: &Arc<tokio::sync::Mutex<PersistentGraphIndex>>,
        snapshot_store: &ConsoleGraphSnapshotStore,
        source_version: u64,
        lease: IncrementalBuildLease,
    ) -> crate::Result<Option<GraphSnapshotSet>> {
        let target_source_version = lease.target_source_version();
        tracing::info!(
            rebuild_output_scope = "anchors_and_all",
            process_local_invalidation_version = source_version,
            build_target_process_local_invalidation_version = target_source_version,
            build_run_id = lease.generation(),
            build_lease_epoch = lease.lease_epoch(),
            build_frozen_source_revision = lease.frozen_source_revision(),
            resumed_build_run = lease.is_resumed(),
            "console graph build lease acquired",
        );
        for mode in [GraphMode::Anchors, GraphMode::All] {
            set_rebuild_status!(
                self,
                rebuild_status(
                    mode,
                    source_version,
                    ConsoleGraphRebuildState::Building,
                    Some(crate::graph::GraphBuildPhase::Entries),
                    0,
                    0,
                    if lease.is_resumed() {
                        "Resuming incremental graph build"
                    } else {
                        "Initializing incremental graph build"
                    },
                )
            );
        }

        let mut active_lease = ActiveBuildLease::new(snapshot_store.clone(), lease);
        let lease = active_lease.lease().clone();
        let prepared = {
            let build = self.refresh_persistent_snapshot_set_with_lease(
                store,
                index,
                snapshot_store,
                target_source_version,
                &lease,
            );
            tokio::pin!(build);
            let progress_cache = self.clone();
            let report_progress = move |progress: IncrementalBuildProgress| {
                for mode in [GraphMode::Anchors, GraphMode::All] {
                    let status = rebuild_status(
                        mode,
                        source_version,
                        ConsoleGraphRebuildState::Building,
                        Some(progress.phase),
                        progress.completed_units,
                        progress.total_units,
                        progress.message,
                    );
                    progress_cache.set_rebuild_status(&status);
                }
            };
            let heartbeat =
                maintain_build_lease(snapshot_store, &lease, source_version, report_progress);
            tokio::pin!(heartbeat);
            tokio::select! {
                result = &mut build => result,
                result = &mut heartbeat => {
                    result?;
                    unreachable!("build lease heartbeat only exits after losing the lease")
                }
            }
        };
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                active_lease.pause().await;
                return Err(error);
            }
        };
        let publication = match snapshot_store
            .publish_incremental_generation(&lease, target_source_version)
            .await
        {
            Ok(publication) => publication,
            Err(error) => {
                active_lease.pause().await;
                return Err(error);
            }
        };
        active_lease.complete();

        let build_run_id = publication.build_run_id;
        let published_graph_generation = publication.published_graph_generation;
        let stats = prepared.stats;
        tracing::info!(
            rebuild_output_scope = "anchors_and_all",
            process_local_invalidation_version = source_version,
            build_target_process_local_invalidation_version = target_source_version,
            build_run_id,
            published_graph_generation,
            graph_publication_epoch = publication.publication_epoch,
            published_source_revision = publication.published_source_revision,
            publication_outcome = if publication.replayed {
                "replayed"
            } else {
                "committed"
            },
            build_lease_epoch = lease.lease_epoch(),
            build_frozen_source_revision = lease.frozen_source_revision(),
            run_completed_node_count = stats.processed_nodes,
            published_all_node_count = stats.all_nodes,
            published_anchors_node_count = stats.anchors_nodes,
            graph_build_kind = if stats.reused_baseline {
                "append"
            } else {
                "full"
            },
            graph_build_classification_reason = stats.classification_reason.as_str(),
            lease_session_frontier_hot_to_spilled_count = stats.frontier.hot_to_spilled,
            lease_session_frontier_spilled_to_hot_count = stats.frontier.spilled_to_hot,
            "console graph publication completed",
        );
        self.cleanup_persistent_publication(
            index,
            snapshot_store,
            build_run_id,
            published_graph_generation,
        )
        .await;
        Ok(prepared.snapshots)
    }

    async fn cleanup_persistent_publication(
        &self,
        index: &Arc<tokio::sync::Mutex<PersistentGraphIndex>>,
        snapshot_store: &ConsoleGraphSnapshotStore,
        build_run_id: i64,
        published_graph_generation: i64,
    ) {
        let index = index.lock().await;
        if let Err(error) = index.prune_published_orphans().await {
            tracing::warn!(
                published_graph_generation,
                error = %error,
                "failed to prune published console graph source orphans",
            );
        }
        drop(index);
        if let Err(error) = snapshot_store.cleanup_abandoned_generations().await {
            tracing::warn!(
                published_graph_generation,
                error = %error,
                "failed to clean abandoned console graph build runs",
            );
        }
        if let Err(error) = snapshot_store.cleanup_obsolete_generations().await {
            tracing::warn!(
                published_graph_generation,
                error = %error,
                "failed to clean obsolete console graph generations",
            );
        }
        if let Err(error) = snapshot_store.cleanup_completed_build_work().await {
            tracing::warn!(
                build_run_id,
                error = %error,
                "failed to clean completed console graph build work",
            );
        }
    }

    async fn refresh_persistent_snapshot_set_with_lease(
        &self,
        store: &SqliteGraphStore,
        _index: &Arc<tokio::sync::Mutex<PersistentGraphIndex>>,
        snapshot_store: &ConsoleGraphSnapshotStore,
        source_version: u64,
        lease: &IncrementalBuildLease,
    ) -> crate::Result<PreparedPersistentRefresh> {
        let baseline_generation = snapshot_store.active_generation().await?;
        ensure!(
            snapshot_store.renew_incremental_build_lease(lease).await?,
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "incremental_build_lease",
                value: format!(
                    "build run {} lease epoch {} is no longer owned",
                    lease.generation(),
                    lease.lease_epoch(),
                ),
            }
        );
        let stats = build_incremental_generation(
            snapshot_store,
            &store.root_id(),
            baseline_generation,
            lease,
            source_version,
        )
        .await?;

        #[cfg(test)]
        let final_snapshots = {
            let graph_store = _index.lock().await.graph_store();
            let anchors = self
                .build_snapshot_for_mode(Some(&graph_store), GraphMode::Anchors, source_version)
                .await?;
            let all = self
                .build_snapshot_for_mode(Some(&graph_store), GraphMode::All, source_version)
                .await?;
            Some(GraphSnapshotSet { anchors, all })
        };
        #[cfg(not(test))]
        let final_snapshots = None;
        Ok(PreparedPersistentRefresh {
            snapshots: final_snapshots,
            stats,
        })
    }

    async fn build_snapshot_for_mode(
        &self,
        graph_store: Option<&PersistentGraphStore>,
        mode: GraphMode,
        source_version: u64,
    ) -> crate::Result<GraphSnapshot> {
        let progress_cache = self.clone();
        let progress = move |progress: crate::graph::GraphBuildProgress| {
            set_rebuild_status!(
                progress_cache,
                rebuild_status(
                    mode,
                    source_version,
                    ConsoleGraphRebuildState::Building,
                    Some(progress.phase),
                    progress.processed,
                    progress.total,
                    progress.phase.label(),
                )
            );
        };
        match graph_store {
            Some(store) => {
                build_graph_snapshot_with_mode_and_progress(store, source_version, mode, progress)
                    .await
            }
            None => {
                self.source
                    .clone()
                    .build_snapshot_with_progress(mode, source_version, progress)
                    .await
            }
        }
    }

    async fn materialization_current(&self, mode: GraphMode, source_version: u64) -> bool {
        let Some(snapshots) = self.snapshots.as_ref() else {
            return false;
        };
        snapshots
            .latest_materialization_version(mode)
            .await
            .ok()
            .flatten()
            .is_some_and(|version| version >= source_version)
    }

    fn fail_if_materialization_failed(
        &self,
        mode: GraphMode,
        observed_version: u64,
    ) -> crate::Result<()> {
        let status = {
            let state = self
                .state
                .lock()
                .expect("console graph cache lock poisoned");
            state.rebuild_slot(mode).as_ref().cloned()
        };
        if let Some(status) = status
            && status.state == ConsoleGraphRebuildState::Failed
            && status.source_version >= observed_version
        {
            crate::error::ConsoleGraphRebuildSnafu {
                mode: mode.as_query_value(),
                source_version: status.source_version,
                message: status.message,
            }
            .fail()?;
        }
        Ok(())
    }

    fn set_rebuild_status(&self, status: &ConsoleGraphRebuildStatus) -> bool {
        let mode = status.mode;
        let mut state = self
            .state
            .lock()
            .expect("console graph cache lock poisoned");
        if state.rebuild_slot(mode).as_ref() == Some(status)
            || state
                .rebuild_slot(mode)
                .as_ref()
                .is_some_and(|current| current.source_version > status.source_version)
        {
            return false;
        }
        *state.rebuild_slot_mut(mode) = Some(status.clone());
        drop(state);
        self.progress.mark_changed();
        true
    }
}

fn should_log_rebuild_status_at_info(status: &ConsoleGraphRebuildStatus) -> bool {
    if status.state != ConsoleGraphRebuildState::Building {
        return true;
    }

    status.phase.is_some_and(|_| {
        status.processed == 0
            || status.processed == status.total
            || rebuild_progress_crosses_info_bucket(status)
    })
}

fn rebuild_state_name(state: ConsoleGraphRebuildState) -> &'static str {
    match state {
        ConsoleGraphRebuildState::Scheduled => "scheduled",
        ConsoleGraphRebuildState::Building => "building",
        ConsoleGraphRebuildState::Ready => "ready",
        ConsoleGraphRebuildState::Failed => "failed",
    }
}

fn rebuild_phase_name(phase: Option<crate::graph::GraphBuildPhase>) -> &'static str {
    match phase {
        Some(crate::graph::GraphBuildPhase::Branches) => "branches",
        Some(crate::graph::GraphBuildPhase::ProviderContexts) => "provider_contexts",
        Some(crate::graph::GraphBuildPhase::Entries) => "entries",
        Some(crate::graph::GraphBuildPhase::Snapshot) => "snapshot",
        None => "not_started",
    }
}

fn rebuild_phase_progress_unit(phase: Option<crate::graph::GraphBuildPhase>) -> &'static str {
    match phase {
        Some(crate::graph::GraphBuildPhase::Branches)
        | Some(crate::graph::GraphBuildPhase::ProviderContexts) => "source_branch",
        Some(crate::graph::GraphBuildPhase::Entries) => "graph_node",
        Some(crate::graph::GraphBuildPhase::Snapshot) => "snapshot_step",
        None => "not_applicable",
    }
}

fn rebuild_progress_percent(status: &ConsoleGraphRebuildStatus) -> usize {
    if status.total == 0 {
        return 0;
    }
    status.processed.saturating_mul(100) / status.total
}

fn rebuild_progress_crosses_info_bucket(status: &ConsoleGraphRebuildStatus) -> bool {
    if status.total == 0 || status.processed == 0 {
        return false;
    }
    let previous = status.processed.saturating_sub(1);
    let previous_bucket = previous.saturating_mul(10) / status.total;
    let current_bucket = status.processed.saturating_mul(10) / status.total;
    current_bucket > previous_bucket
}

impl CacheState {
    fn slot(&self, mode: GraphMode) -> &Option<CachedGraphSnapshot> {
        match mode {
            GraphMode::Anchors => &self.anchors,
            GraphMode::All => &self.all,
        }
    }

    fn slot_mut(&mut self, mode: GraphMode) -> &mut Option<CachedGraphSnapshot> {
        match mode {
            GraphMode::Anchors => &mut self.anchors,
            GraphMode::All => &mut self.all,
        }
    }

    fn rebuild_slot(&self, mode: GraphMode) -> &Option<ConsoleGraphRebuildStatus> {
        match mode {
            GraphMode::Anchors => &self.anchors_rebuild,
            GraphMode::All => &self.all_rebuild,
        }
    }

    fn rebuild_slot_mut(&mut self, mode: GraphMode) -> &mut Option<ConsoleGraphRebuildStatus> {
        match mode {
            GraphMode::Anchors => &mut self.anchors_rebuild,
            GraphMode::All => &mut self.all_rebuild,
        }
    }
}

fn take_refresh_worker_batch(
    invalidations: &ConsolePublisher,
    before_take: impl FnOnce(),
) -> (u64, ConsoleInvalidationBatch) {
    before_take();
    let batch = invalidations.take_invalidations();
    (batch.version, batch)
}

fn rebuild_status(
    mode: GraphMode,
    source_version: u64,
    state: ConsoleGraphRebuildState,
    phase: Option<crate::graph::GraphBuildPhase>,
    processed: usize,
    total: usize,
    message: impl Into<String>,
) -> ConsoleGraphRebuildStatus {
    ConsoleGraphRebuildStatus {
        mode,
        source_version,
        state,
        phase,
        processed,
        total,
        message: message.into(),
    }
}

fn ready_rebuild_status(mode: GraphMode, source_version: u64) -> ConsoleGraphRebuildStatus {
    rebuild_status(
        mode,
        source_version,
        ConsoleGraphRebuildState::Ready,
        None,
        1,
        1,
        "Graph ready",
    )
}

async fn latest_persistent_materialization_version(
    snapshots: &ConsoleGraphSnapshotStore,
) -> crate::Result<Option<u64>> {
    let mut latest = None;
    for mode in [GraphMode::Anchors, GraphMode::All] {
        if let Some(version) = snapshots.latest_materialization_version(mode).await? {
            latest = Some(latest.map_or(version, |current: u64| current.max(version)));
        }
    }
    Ok(latest)
}

fn node_id_from_graph_target(target: &str) -> Option<String> {
    target
        .strip_prefix("detail-")
        .filter(|node_id| !node_id.is_empty())
        .map(str::to_owned)
}

async fn open_persistent_graph_store(path: &Path) -> crate::Result<SqliteGraphStore> {
    SqliteGraphStore::open_read_only(path)
        .await
        .context(crate::error::StoreSnafu)
}

impl<S> ConsoleGraphSource<S>
where
    S: Store + Clone + Send + Sync + 'static,
{
    async fn build_snapshot_with_progress<F>(
        self,
        mode: GraphMode,
        version: u64,
        progress: F,
    ) -> crate::Result<GraphSnapshot>
    where
        F: FnMut(crate::graph::GraphBuildProgress),
    {
        match self {
            Self::Store(store) => {
                build_graph_snapshot_with_mode_and_progress(&store, version, mode, progress).await
            }
            Self::PersistentStore(path) => {
                let store = open_persistent_graph_store(&path).await?;
                build_graph_snapshot_with_mode_and_progress(&store, version, mode, progress).await
            }
        }
    }

    async fn get_node(self, node_id: &str) -> crate::Result<coco_mem::Node> {
        match self {
            Self::Store(store) => store
                .get_node(node_id)
                .await
                .context(crate::error::StoreSnafu),
            Self::PersistentStore(path) => {
                let store = open_persistent_graph_store(&path).await?;
                store
                    .get_node(node_id)
                    .await
                    .context(crate::error::StoreSnafu)
            }
        }
    }

    async fn provider_context_for_node(
        self,
        target_node_id: &str,
        context: Option<&str>,
    ) -> crate::Result<Option<crate::graph::GraphProviderContextSelection>> {
        match self {
            Self::Store(store) => provider_context_for_node(&store, target_node_id, context).await,
            Self::PersistentStore(path) => {
                let store = open_persistent_graph_store(&path).await?;
                provider_context_for_node(&store, target_node_id, context).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::graph::build_graph_snapshot_with_mode;
    use coco_mem::{BranchStore, Kind, NewNode, NodeStore, Role, SessionState, SqliteStore};
    use diesel::prelude::*;
    use diesel::sql_types::BigInt;

    #[derive(Clone, Default)]
    struct SharedLogWriter(Arc<Mutex<Vec<u8>>>);

    struct SharedLogBuffer(Arc<Mutex<Vec<u8>>>);

    impl SharedLogWriter {
        fn contents(&self) -> String {
            String::from_utf8(
                self.0
                    .lock()
                    .expect("shared log writer lock poisoned")
                    .clone(),
            )
            .expect("tracing output should be UTF-8")
        }
    }

    impl Write for SharedLogBuffer {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("shared log buffer lock poisoned")
                .extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for SharedLogWriter {
        type Writer = SharedLogBuffer;

        fn make_writer(&'writer self) -> Self::Writer {
            SharedLogBuffer(self.0.clone())
        }
    }

    #[test]
    fn rebuild_status_names_cover_every_public_state() {
        assert_eq!(
            [
                ConsoleGraphRebuildState::Scheduled,
                ConsoleGraphRebuildState::Building,
                ConsoleGraphRebuildState::Ready,
                ConsoleGraphRebuildState::Failed,
            ]
            .map(rebuild_state_name),
            ["scheduled", "building", "ready", "failed"]
        );

        let phases = [
            Some(crate::graph::GraphBuildPhase::Branches),
            Some(crate::graph::GraphBuildPhase::ProviderContexts),
            Some(crate::graph::GraphBuildPhase::Entries),
            Some(crate::graph::GraphBuildPhase::Snapshot),
            None,
        ];
        assert_eq!(
            phases.map(rebuild_phase_name),
            [
                "branches",
                "provider_contexts",
                "entries",
                "snapshot",
                "not_started",
            ]
        );
        assert_eq!(
            phases.map(rebuild_phase_progress_unit),
            [
                "source_branch",
                "source_branch",
                "graph_node",
                "snapshot_step",
                "not_applicable",
            ]
        );
    }

    #[derive(Debug, QueryableByName)]
    struct PublicationRevisionRangeRow {
        #[diesel(sql_type = BigInt)]
        count: i64,
        #[diesel(sql_type = BigInt)]
        minimum_revision: i64,
        #[diesel(sql_type = BigInt)]
        maximum_revision: i64,
    }

    #[test]
    fn dropped_rebuild_restores_taken_invalidations() {
        let publisher = ConsolePublisher::new();
        publisher.mark_changed();
        let batch = publisher.take_invalidations();

        drop(TakenInvalidations::new(publisher.clone(), batch.clone()));

        assert_eq!(publisher.take_invalidations(), batch);
    }

    #[test]
    fn worker_batch_captures_the_published_version() {
        let publisher = ConsolePublisher::new();
        publisher.notify_durable_change();

        let (source_version, batch) = take_refresh_worker_batch(&publisher, || {
            publisher.notify_durable_change();
        });

        assert_eq!(source_version, 2);
        assert_eq!(batch.version, 2);
        assert!(!batch.full);
    }

    #[tokio::test]
    async fn cancelled_build_pauses_and_resumes_lease_promptly() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let store = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let lease = store
            .acquire_incremental_build_lease(1)
            .await
            .unwrap()
            .unwrap();
        let build_run_id = lease.generation();
        let lease_epoch = lease.lease_epoch();
        let frozen_source_revision = lease.frozen_source_revision();
        let context = store
            .latest_incremental_build_diagnostic_context()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(context.build_run_id, build_run_id);
        assert_eq!(context.build_lease_epoch, lease_epoch);
        assert_eq!(context.frozen_source_revision, frozen_source_revision);
        assert_eq!(context.target_process_local_invalidation_version, 1);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let guarded_store = store.clone();
        let build = tokio::spawn(async move {
            let _active_lease = ActiveBuildLease::new(guarded_store, lease);
            started_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        started_rx.await.unwrap();
        assert!(
            store
                .acquire_incremental_build_lease(1)
                .await
                .unwrap()
                .is_none()
        );

        build.abort();
        assert!(build.await.unwrap_err().is_cancelled());
        let replacement = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(lease) = store.acquire_incremental_build_lease(1).await.unwrap() {
                    return lease;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cancelled build lease should be paused before its TTL");
        assert_eq!(replacement.generation(), build_run_id);
        assert_eq!(replacement.lease_epoch(), lease_epoch + 1);
        assert_eq!(replacement.frozen_source_revision(), frozen_source_revision);

        assert!(store.abandon_incremental_build(&replacement).await.unwrap());
        store.cleanup_abandoned_generations().await.unwrap();
    }

    #[tokio::test]
    async fn existing_materialization_publishes_ready_and_consumes_invalidations() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let path = writer.store_path().to_owned();
        writer.fork("main", &writer.root_id()).await.unwrap();
        let first_invalidations = ConsolePublisher::new();
        let second_invalidations = ConsolePublisher::new();
        let first = ConsoleGraphCache::new_with_persistent_store_path(
            writer.clone(),
            first_invalidations,
            path.clone(),
        )
        .await
        .unwrap();
        let second = ConsoleGraphCache::new_with_persistent_store_path(
            writer.clone(),
            second_invalidations.clone(),
            path,
        )
        .await
        .unwrap();
        let mut ready = second.subscribe();

        first.rebuild_requested_modes().await.unwrap();
        second.rebuild_requested_modes().await.unwrap();

        assert_eq!(*ready.borrow_and_update(), 1);
        let statuses = second.rebuild_statuses();
        assert_eq!(statuses.len(), 2);
        assert!(statuses.iter().all(|status| {
            status.source_version == 1 && status.state == ConsoleGraphRebuildState::Ready
        }));
        let pending = second_invalidations.take_invalidations();
        assert_eq!(pending.version, 1);
        assert!(!pending.full);
    }

    #[tokio::test]
    async fn restart_without_source_changes_advances_versioned_viewports_in_place() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let path = writer.store_path().to_owned();
        writer.fork("main", &writer.root_id()).await.unwrap();
        let first = ConsoleGraphCache::new_with_persistent_store_path(
            writer.clone(),
            ConsolePublisher::new(),
            path.clone(),
        )
        .await
        .unwrap();
        first.rebuild_requested_modes().await.unwrap();
        let initial = first
            .viewport_current_ready_or_schedule(GraphMode::All, GraphViewportRequest::default())
            .await
            .unwrap();
        let generation = first
            .snapshots
            .as_ref()
            .unwrap()
            .active_generation()
            .await
            .unwrap();
        drop(first);

        let second = ConsoleGraphCache::new_with_persistent_store_path(
            writer.clone(),
            ConsolePublisher::new(),
            path,
        )
        .await
        .unwrap();
        second.rebuild_requested_modes().await.unwrap();
        let current = tokio::time::timeout(
            Duration::from_secs(2),
            second.viewport_after(
                GraphMode::All,
                initial.version,
                GraphViewportRequest::default(),
            ),
        )
        .await
        .expect("versioned viewport should observe the restart version")
        .unwrap();

        assert!(current.version > initial.version);
        assert_eq!(
            second
                .snapshots
                .as_ref()
                .unwrap()
                .active_generation()
                .await
                .unwrap(),
            generation
        );
        assert_eq!(
            second
                .materialized_shell_current_ready_or_schedule(GraphMode::All)
                .await
                .unwrap()
                .unwrap()
                .version,
            current.version
        );
    }

    #[tokio::test]
    async fn restart_finishes_expired_active_overlay_compaction_before_fast_path() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let path = writer.store_path().to_owned();
        writer.fork("main", &writer.root_id()).await.unwrap();
        let first = ConsoleGraphCache::new_with_persistent_store_path(
            writer.clone(),
            ConsolePublisher::new(),
            path.clone(),
        )
        .await
        .unwrap();
        first.rebuild_requested_modes().await.unwrap();
        let snapshots = first.snapshots.as_ref().unwrap();
        let active = snapshots.active_publication_state().await.unwrap();
        let base_generation = active.graph_generation;
        let source_revision = active.source_revision.unwrap();
        let publication_epoch = active.publication_epoch.unwrap();
        let source_version = active.all_source_version.unwrap() as i64;
        let database = snapshots.database();
        database
            .with_write_connection(
                "seed expired active overlay compaction",
                move |connection| {
                    connection
                        .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                            diesel::sql_query(
                                "INSERT INTO console_graph_build_runs ( \
                                 run_id, source_version, status, owner_id, lease_expires_at_ms, \
                                 lease_epoch, dag_source_revision \
                             ) VALUES (900000020, ?, 'compacting', 'expired-overlay', 0, 1, ?)",
                            )
                            .bind::<BigInt, _>(source_version)
                            .bind::<BigInt, _>(source_revision)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_overlay_runs ( \
                                 run_id, base_generation, baseline_source_revision, \
                                 baseline_publication_epoch, target_source_version, \
                                 target_source_revision, target_publication_epoch, status, phase, \
                                 owner_id, lease_epoch, lease_expires_at_ms \
                             ) VALUES (900000020, ?, ?, ?, ?, ?, ?, 'compacting', 'finalize', \
                                       'expired-overlay', 1, 0)",
                            )
                            .bind::<BigInt, _>(base_generation)
                            .bind::<BigInt, _>(source_revision)
                            .bind::<BigInt, _>(publication_epoch)
                            .bind::<BigInt, _>(source_version)
                            .bind::<BigInt, _>(source_revision)
                            .bind::<BigInt, _>(publication_epoch + 1)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_build_publications ( \
                                 build_run_id, published_graph_generation, publication_epoch, \
                                 build_kind, source_version, source_revision \
                             ) VALUES (900000020, ?, ?, 'append', ?, ?)",
                            )
                            .bind::<BigInt, _>(base_generation)
                            .bind::<BigInt, _>(publication_epoch + 1)
                            .bind::<BigInt, _>(source_version)
                            .bind::<BigInt, _>(source_revision)
                            .execute(connection)?;
                            diesel::sql_query(
                                "INSERT INTO console_graph_materializations ( \
                                 generation, mode, source_version, coordinate_space, \
                                 world_min_x, world_min_y, world_max_x, world_max_y \
                             ) \
                             SELECT 900000020, mode, source_version, coordinate_space, \
                                    world_min_x, world_min_y, world_max_x, world_max_y \
                             FROM console_graph_materializations WHERE generation = ?",
                            )
                            .bind::<BigInt, _>(base_generation)
                            .execute(connection)?;
                            diesel::sql_query(
                                "UPDATE console_graph_generation_state \
                             SET active_overlay_run_id = 900000020 WHERE id = 1",
                            )
                            .execute(connection)?;
                            Ok(())
                        })
                        .context(crate::error::QueryGraphSnapshotStoreSnafu {
                            path: PathBuf::from("expired-active-overlay-test"),
                        })
                },
            )
            .await
            .unwrap();
        drop(first);

        let restarted = ConsoleGraphCache::new_with_persistent_store_path(
            writer.clone(),
            ConsolePublisher::new(),
            path,
        )
        .await
        .unwrap();
        restarted.rebuild_requested_modes().await.unwrap();
        let recovered = restarted
            .snapshots
            .as_ref()
            .unwrap()
            .active_publication_state()
            .await
            .unwrap();
        assert!(!recovered.overlay_compaction_pending);
        assert_eq!(recovered.graph_generation, base_generation);
        assert!(recovered.matches_current_source);
    }

    #[tokio::test]
    async fn newer_source_revision_finishes_frozen_run_before_serial_catchup() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let path = writer.store_path().to_owned();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let first = ConsoleGraphCache::new_with_persistent_store_path(
            writer.clone(),
            ConsolePublisher::new(),
            path.clone(),
        )
        .await
        .unwrap();
        first.rebuild_requested_modes().await.unwrap();
        let snapshots = first.snapshots.as_ref().unwrap();
        let baseline_generation = snapshots.active_generation().await.unwrap();
        let frozen = snapshots
            .acquire_incremental_build_lease(2)
            .await
            .unwrap()
            .unwrap();
        build_incremental_generation(snapshots, &root, baseline_generation, &frozen, 2)
            .await
            .unwrap();
        assert!(snapshots.pause_incremental_build(&frozen).await.unwrap());

        let child = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("newer durable source".to_owned()),
            })
            .await
            .unwrap();
        writer.set_branch_head("main", &root, &child).await.unwrap();
        drop(first);

        let restarted = ConsoleGraphCache::new_with_persistent_store_path(
            writer.clone(),
            ConsolePublisher::new(),
            path,
        )
        .await
        .unwrap();
        restarted.rebuild_requested_modes().await.unwrap();
        let active = restarted
            .snapshots
            .as_ref()
            .unwrap()
            .active_publication_state()
            .await
            .unwrap();
        assert!(active.matches_current_source);

        let database = restarted.snapshots.as_ref().unwrap().database();
        let revisions = database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT COUNT(*) AS count, MIN(source_revision) AS minimum_revision, \
                            MAX(source_revision) AS maximum_revision \
                     FROM console_graph_build_publications WHERE source_version = 2",
                )
                .get_result::<PublicationRevisionRangeRow>(connection)
                .context(crate::error::QueryGraphSnapshotStoreSnafu {
                    path: PathBuf::from("serial-catchup-publications"),
                })
            })
            .await
            .unwrap();
        assert_eq!(revisions.count, 2);
        assert!(revisions.minimum_revision < revisions.maximum_revision);
    }

    #[tokio::test]
    async fn identical_rebuild_status_is_not_republished() {
        let store = SqliteStore::open_temporary().await.unwrap();
        let cache = ConsoleGraphCache::new(store, ConsolePublisher::new());
        let status = ready_rebuild_status(GraphMode::All, 7);

        assert!(cache.set_rebuild_status(&status));
        assert_eq!(cache.progress.current_version(), 1);

        assert!(!cache.set_rebuild_status(&status));
        assert_eq!(cache.progress.current_version(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shared_rebuild_failure_updates_both_modes_and_logs_once() {
        let store = SqliteStore::open_temporary().await.unwrap();
        let cache = ConsoleGraphCache::new(store, ConsolePublisher::new());
        let logs = SharedLogWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_max_level(tracing::Level::WARN)
            .with_writer(logs.clone())
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let guard = tracing::dispatcher::set_default(&dispatch);
        let error = crate::Error::InvalidGraphSnapshotStoreValue {
            column: "test_failure",
            value: "injected".to_owned(),
        };

        cache.record_refresh_worker_failure(7, &error).await;
        drop(guard);

        let statuses = cache.rebuild_statuses();
        assert_eq!(statuses.len(), 2);
        assert!(
            statuses
                .iter()
                .all(|status| status.state == ConsoleGraphRebuildState::Failed)
        );
        let output = logs.contents();
        assert_eq!(
            output
                .matches("\"message\":\"console graph rebuild failed\"")
                .count(),
            1,
            "{output}",
        );
        assert!(
            output.contains("\"rebuild_output_scope\":\"anchors_and_all\""),
            "{output}",
        );
        assert!(
            output.contains("\"rebuild_execution_scope\":\"in_memory_shared\""),
            "{output}",
        );
    }

    #[tokio::test]
    async fn concurrent_modes_share_one_persistent_source_refresh() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let path = writer.store_path().to_owned();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let publisher = ConsolePublisher::new();
        publisher.mark_changed();
        let cache =
            ConsoleGraphCache::new_with_persistent_store_path(writer.clone(), publisher, path)
                .await
                .unwrap();

        let (anchors, all) = tokio::join!(
            cache.snapshot_current(GraphMode::Anchors),
            cache.snapshot_current(GraphMode::All),
        );

        assert_eq!(anchors.unwrap().version, all.unwrap().version);
        assert_eq!(
            cache
                .persistent_index
                .as_ref()
                .unwrap()
                .lock()
                .await
                .refresh_count(),
            1
        );
    }

    #[tokio::test]
    async fn full_refresh_activates_one_generation_after_all_branch_batches() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let path = writer.store_path().to_owned();
        let root = writer.root_id();
        for index in 0..=coco_mem::GRAPH_READ_BATCH_SIZE {
            writer
                .fork(&format!("branch-{index:03}"), &root)
                .await
                .unwrap();
        }
        let publisher = ConsolePublisher::new();
        publisher.mark_changed();
        let cache =
            ConsoleGraphCache::new_with_persistent_store_path(writer.clone(), publisher, path)
                .await
                .unwrap();

        let snapshot = cache.snapshot_current(GraphMode::All).await.unwrap();
        let snapshot_store = cache.snapshots.as_ref().unwrap();

        assert_eq!(snapshot.branches.len(), coco_mem::GRAPH_READ_BATCH_SIZE + 1);
        assert_eq!(snapshot_store.active_generation().await.unwrap(), 1);
        assert_eq!(
            snapshot_store
                .latest_materialization_version(GraphMode::Anchors)
                .await
                .unwrap(),
            Some(snapshot.version)
        );
        assert_eq!(
            snapshot_store
                .latest_materialization_version(GraphMode::All)
                .await
                .unwrap(),
            Some(snapshot.version)
        );
    }

    #[tokio::test]
    async fn materialized_shell_branches_switch_with_incremental_publication() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let path = writer.store_path().to_owned();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let publisher = ConsolePublisher::new();
        publisher.mark_changed();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            writer.clone(),
            publisher.clone(),
            path,
        )
        .await
        .unwrap();

        cache.snapshot_current(GraphMode::All).await.unwrap();
        let snapshots = cache.snapshots.as_ref().unwrap();
        let initial_generation = snapshots.active_generation().await.unwrap();
        let initial_publication_epoch = snapshots
            .active_publication_state()
            .await
            .unwrap()
            .publication_epoch
            .unwrap();
        let initial_shell = cache
            .materialized_shell_current_ready_or_schedule(GraphMode::All)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            initial_shell
                .branches
                .iter()
                .map(|branch| branch.name.as_str())
                .collect::<Vec<_>>(),
            vec!["main"]
        );

        writer.fork("aaa", &root).await.unwrap();
        let shell_while_old_generation_is_active = cache
            .materialized_shell_current_ready_or_schedule(GraphMode::All)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            shell_while_old_generation_is_active
                .branches
                .iter()
                .map(|branch| branch.name.as_str())
                .collect::<Vec<_>>(),
            vec!["main"]
        );
        assert_eq!(
            snapshots.active_generation().await.unwrap(),
            initial_generation
        );

        publisher.mark_changed();
        cache.snapshot_current(GraphMode::All).await.unwrap();
        let activated_publication_epoch = snapshots
            .active_publication_state()
            .await
            .unwrap()
            .publication_epoch
            .unwrap();
        let activated_shell = cache
            .materialized_shell_current_ready_or_schedule(GraphMode::All)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            cache
                .persistent_index
                .as_ref()
                .unwrap()
                .lock()
                .await
                .graph_store()
                .get_branch_head("aaa")
                .await
                .unwrap(),
            root
        );
        assert_eq!(
            snapshots
                .latest_materialization_version(GraphMode::All)
                .await
                .unwrap(),
            Some(publisher.current_version())
        );
        assert!(activated_publication_epoch > initial_publication_epoch);
        assert_eq!(
            activated_shell
                .branches
                .iter()
                .map(|branch| branch.name.as_str())
                .collect::<Vec<_>>(),
            vec!["main", "aaa"]
        );
        assert!(
            activated_shell
                .branches
                .iter()
                .all(|branch| branch.head_short_id == crate::graph::shorten_id(&root))
        );
        assert!(
            activated_shell
                .branches
                .iter()
                .all(|branch| branch.state == SessionState::Active)
        );
    }

    #[tokio::test]
    async fn restart_reconciles_offline_branch_changes() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let path = writer.store_path().to_owned();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        writer.fork("stale", &root).await.unwrap();
        let publisher = ConsolePublisher::new();
        publisher.mark_changed();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            writer.clone(),
            publisher,
            path.clone(),
        )
        .await
        .unwrap();
        cache.snapshot_current(GraphMode::All).await.unwrap();
        drop(cache);

        let child = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("offline branch update".to_owned()),
            })
            .await
            .unwrap();
        writer.set_branch_head("main", &root, &child).await.unwrap();
        writer.delete_branch("stale").await.unwrap();

        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            writer.clone(),
            ConsolePublisher::new(),
            path,
        )
        .await
        .unwrap();
        let (anchors, all) = tokio::join!(
            cache.snapshot_current(GraphMode::Anchors),
            cache.snapshot_current(GraphMode::All),
        );
        let anchors = anchors.unwrap();
        let all = all.unwrap();
        let expected_anchors =
            build_graph_snapshot_with_mode(&writer, anchors.version, GraphMode::Anchors)
                .await
                .unwrap();
        let expected_all = build_graph_snapshot_with_mode(&writer, all.version, GraphMode::All)
            .await
            .unwrap();
        let index = cache.persistent_index.as_ref().unwrap().lock().await;

        assert_eq!(*anchors, expected_anchors);
        assert_eq!(*all, expected_all);
        assert_eq!(index.refresh_count(), 1);
        assert_eq!(index.branch_refresh_count(), 1);
    }

    #[tokio::test]
    async fn durable_poll_detects_another_store_handle_without_publisher_payload() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let path = writer.store_path().to_owned();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            writer.clone(),
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        cache.snapshot_current(GraphMode::All).await.unwrap();
        let (initial_branch_refreshes, initial_traversed_nodes) = {
            let index = cache.persistent_index.as_ref().unwrap().lock().await;
            (index.branch_refresh_count(), index.traversed_node_count())
        };
        let process_local_version = publisher.current_version();

        let second_writer = SqliteStore::open(&path).await.unwrap();
        let child = second_writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("cross-handle durable mutation".to_owned()),
            })
            .await
            .unwrap();
        second_writer
            .set_branch_head("main", &root, &child)
            .await
            .unwrap();
        assert_eq!(publisher.current_version(), process_local_version);
        assert!(cache.poll_durable_graph_mutations().await.unwrap());
        assert!(!cache.poll_durable_graph_mutations().await.unwrap());

        let (anchors, all) = tokio::join!(
            cache.snapshot_current(GraphMode::Anchors),
            cache.snapshot_current(GraphMode::All),
        );
        let anchors = anchors.unwrap();
        let all = all.unwrap();
        let expected_anchors =
            build_graph_snapshot_with_mode(&second_writer, anchors.version, GraphMode::Anchors)
                .await
                .unwrap();
        let expected_all =
            build_graph_snapshot_with_mode(&second_writer, all.version, GraphMode::All)
                .await
                .unwrap();
        assert_eq!(*anchors, expected_anchors);
        assert_eq!(*all, expected_all);

        let index = cache.persistent_index.as_ref().unwrap().lock().await;
        assert_eq!(index.branch_refresh_count(), initial_branch_refreshes + 1);
        assert_eq!(index.traversed_node_count(), initial_traversed_nodes + 1);
        assert_eq!(
            index.consumed_graph_mutation_revision().await.unwrap(),
            cache
                .persistent_graph_store
                .as_ref()
                .unwrap()
                .graph_mutation_revision()
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn dirty_branch_refresh_matches_full_rebuild_for_both_modes() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let path = writer.store_path().to_owned();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let publisher = ConsolePublisher::new();
        publisher.mark_changed();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            writer.clone(),
            publisher.clone(),
            path,
        )
        .await
        .unwrap();
        cache.snapshot_current(GraphMode::All).await.unwrap();
        let child = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("dirty branch".to_owned()),
            })
            .await
            .unwrap();
        writer.set_branch_head("main", &root, &child).await.unwrap();
        let version = publisher.notify_durable_change();

        let (anchors, all) = tokio::join!(
            cache.snapshot_current(GraphMode::Anchors),
            cache.snapshot_current(GraphMode::All),
        );
        let expected_anchors =
            crate::graph::build_graph_snapshot_with_mode(&writer, version, GraphMode::Anchors)
                .await
                .unwrap();
        let expected_all =
            crate::graph::build_graph_snapshot_with_mode(&writer, version, GraphMode::All)
                .await
                .unwrap();

        assert_eq!(*anchors.unwrap(), expected_anchors);
        assert_eq!(*all.unwrap(), expected_all);
        assert_eq!(
            cache
                .persistent_index
                .as_ref()
                .unwrap()
                .lock()
                .await
                .refresh_count(),
            2
        );

        writer.delete_branch("main").await.unwrap();
        let version = publisher.notify_durable_change();
        let (anchors, all) = tokio::join!(
            cache.snapshot_current(GraphMode::Anchors),
            cache.snapshot_current(GraphMode::All),
        );
        let expected_anchors =
            crate::graph::build_graph_snapshot_with_mode(&writer, version, GraphMode::Anchors)
                .await
                .unwrap();
        let expected_all =
            crate::graph::build_graph_snapshot_with_mode(&writer, version, GraphMode::All)
                .await
                .unwrap();
        assert_eq!(*anchors.unwrap(), expected_anchors);
        assert_eq!(*all.unwrap(), expected_all);
        let index = cache.persistent_index.as_ref().unwrap().lock().await;
        assert_eq!(index.refresh_count(), 3);
        assert_eq!(index.node_count().await.unwrap(), 0);
    }
}
