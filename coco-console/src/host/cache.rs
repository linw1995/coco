use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::incremental_build::build_incremental_generation;
use super::snapshot_store::{
    ConsoleGraphSnapshotStore, INCREMENTAL_BUILD_LEASE_HEARTBEAT_INTERVAL, IncrementalBuildLease,
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

struct TakenInvalidations {
    publisher: ConsolePublisher,
    batch: Option<ConsoleInvalidationBatch>,
}

const BUILD_LEASE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

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

    async fn abandon(&mut self) {
        let Some(lease) = self.lease.as_ref().cloned() else {
            return;
        };
        if abandon_build_lease(&self.store, &lease).await {
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
                generation = lease.generation(),
                "could not schedule console graph build lease cleanup outside a Tokio runtime",
            );
            return;
        };
        runtime.spawn(async move {
            let cleanup = async {
                let mut retry_delay = Duration::from_millis(25);
                loop {
                    if abandon_build_lease(&store, &lease).await {
                        return;
                    }
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(Duration::from_millis(500));
                }
            };
            if tokio::time::timeout(BUILD_LEASE_CLEANUP_TIMEOUT, cleanup)
                .await
                .is_err()
            {
                tracing::warn!(
                    generation = lease.generation(),
                    "timed out cleaning cancelled console graph build lease",
                );
            }
        });
    }
}

async fn abandon_build_lease(
    store: &ConsoleGraphSnapshotStore,
    lease: &IncrementalBuildLease,
) -> bool {
    match store.abandon_incremental_build(lease).await {
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(
                generation = lease.generation(),
                %error,
                "failed to abandon console graph build lease",
            );
            return false;
        }
    }
    if let Err(error) = store.cleanup_abandoned_generations().await {
        tracing::warn!(
            generation = lease.generation(),
            %error,
            "failed to clean abandoned console graph generation",
        );
    }
    true
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
        if status.state == ConsoleGraphRebuildState::Failed {
            tracing::warn!(
                mode = ?status.mode,
                source_version = status.source_version,
                phase = ?status.phase,
                processed = status.processed,
                total = status.total,
                progress_percent = rebuild_progress_percent(status),
                message = %status.message,
                "console graph rebuild failed",
            );
        } else if should_log_rebuild_status_at_info(status) {
            tracing::info!(
                mode = ?status.mode,
                source_version = status.source_version,
                state = ?status.state,
                phase = ?status.phase,
                processed = status.processed,
                total = status.total,
                progress_percent = rebuild_progress_percent(status),
                message = %status.message,
                "console graph rebuild status",
            );
        } else {
            tracing::debug!(
                mode = ?status.mode,
                source_version = status.source_version,
                state = ?status.state,
                phase = ?status.phase,
                processed = status.processed,
                total = status.total,
                progress_percent = rebuild_progress_percent(status),
                message = %status.message,
                "console graph rebuild progress",
            );
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
        let ready = ConsolePublisher::new();
        if let Some(version) = persisted_version {
            ready.advance_to(version);
        }
        invalidations.mark_full_at_least(
            persisted_version
                .map(|version| version.saturating_add(1))
                .unwrap_or(1),
        );
        Ok(Self {
            source: ConsoleGraphSource::PersistentStore(path),
            invalidations,
            ready,
            progress: ConsolePublisher::new(),
            snapshots: Some(snapshots),
            persistent_graph_store: Some(persistent_graph_store),
            persistent_index: Some(Arc::new(tokio::sync::Mutex::new(persistent_index))),
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
            let source_version = self.invalidations.current_version();
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
            let mut batch = self.invalidations.take_invalidations();
            batch.version = source_version;
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
                    for mode in [GraphMode::Anchors, GraphMode::All] {
                        set_rebuild_status!(
                            self,
                            rebuild_status(
                                mode,
                                source_version,
                                ConsoleGraphRebuildState::Failed,
                                None,
                                0,
                                0,
                                error.to_string(),
                            )
                        );
                    }
                    return Err(error);
                }
            }

            if self.invalidations.current_version() <= source_version {
                return Ok(());
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
        if let Err(error) = snapshot_store.cleanup_abandoned_generations().await {
            tracing::warn!(%error, "failed to clean abandoned console graph generations");
        }
        let lease = loop {
            let anchors_version = snapshot_store
                .latest_materialization_version(GraphMode::Anchors)
                .await?;
            let all_version = snapshot_store
                .latest_materialization_version(GraphMode::All)
                .await?;
            if anchors_version.is_some_and(|version| version >= source_version)
                && all_version.is_some_and(|version| version >= source_version)
            {
                return Ok(GraphRefreshResult { snapshots: None });
            }
            if let Some(lease) = snapshot_store
                .acquire_incremental_build_lease(source_version)
                .await?
            {
                break lease;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        };

        let mut active_lease = ActiveBuildLease::new(snapshot_store.clone(), lease);
        let lease = active_lease.lease().clone();
        let result = {
            let build = self.refresh_persistent_snapshot_set_with_lease(
                store,
                index,
                snapshot_store,
                source_version,
                invalidations,
                &lease,
            );
            tokio::pin!(build);
            let mut heartbeat = tokio::time::interval(INCREMENTAL_BUILD_LEASE_HEARTBEAT_INTERVAL);
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            heartbeat.tick().await;
            loop {
                tokio::select! {
                    result = &mut build => break result,
                    _ = heartbeat.tick() => {
                        match snapshot_store.renew_incremental_build_lease(&lease).await {
                            Ok(true) => {}
                            Ok(false) => {
                                break crate::error::InvalidGraphSnapshotStoreValueSnafu {
                                    column: "incremental_build_lease",
                                    value: format!(
                                        "generation {} is no longer owned",
                                        lease.generation(),
                                    ),
                                }
                                .fail();
                            }
                            Err(error) => {
                                tracing::warn!(
                                    generation = lease.generation(),
                                    %error,
                                    "failed to renew console graph build lease",
                                );
                            }
                        }
                    }
                }
            }
        };
        if result.is_err() {
            active_lease.abandon().await;
        } else {
            active_lease.complete();
        }
        result
    }

    async fn refresh_persistent_snapshot_set_with_lease(
        &self,
        store: &SqliteGraphStore,
        index: &Arc<tokio::sync::Mutex<PersistentGraphIndex>>,
        snapshot_store: &ConsoleGraphSnapshotStore,
        source_version: u64,
        invalidations: &ConsoleInvalidationBatch,
        lease: &IncrementalBuildLease,
    ) -> crate::Result<GraphRefreshResult> {
        let baseline_generation = snapshot_store.active_generation().await?;
        let mut index = index.lock().await;
        index.start_refresh().await?;

        if invalidations.full || index.is_empty().await? {
            index.refresh_all_branches_bounded(store).await?;
        } else {
            let names = invalidations.branches.iter().cloned().collect::<Vec<_>>();
            index.refresh_named_batch(store, &names).await?;
        }

        ensure!(
            snapshot_store.renew_incremental_build_lease(lease).await?,
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "incremental_build_lease",
                value: format!("generation {} is no longer owned", lease.generation()),
            }
        );
        let generation = lease.generation();
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
            let graph_store = index.graph_store();
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

        snapshot_store
            .publish_incremental_generation(lease, source_version)
            .await?;
        tracing::info!(
            source_version,
            generation,
            processed_nodes = stats.processed_nodes,
            all_nodes = stats.all_nodes,
            anchors_nodes = stats.anchors_nodes,
            reused_baseline = stats.reused_baseline,
            frontier_hot_to_spilled = stats.frontier.hot_to_spilled,
            frontier_spilled_to_hot = stats.frontier.spilled_to_hot,
            "console graph incremental generation activated",
        );

        if let Err(error) = index.prune_published_orphans().await {
            tracing::warn!(%error, "failed to prune published console graph source orphans");
        }
        if let Err(error) = snapshot_store.cleanup_obsolete_generations().await {
            tracing::warn!(%error, "failed to clean obsolete console graph generations");
        }
        if let Err(error) = snapshot_store.cleanup_completed_build_work().await {
            tracing::warn!(%error, "failed to clean completed console graph build work");
        }
        Ok(GraphRefreshResult {
            snapshots: final_snapshots,
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
            == Some(source_version)
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
    use super::*;
    use crate::graph::build_graph_snapshot_with_mode;
    use coco_mem::{BranchStore, Kind, NewNode, NodeStore, Role, SessionState, SqliteStore};

    #[test]
    fn dropped_rebuild_restores_taken_invalidations() {
        let publisher = ConsolePublisher::new();
        publisher.mark_branch_changed("main");
        let batch = publisher.take_invalidations();

        drop(TakenInvalidations::new(publisher.clone(), batch.clone()));

        assert_eq!(publisher.take_invalidations(), batch);
    }

    #[tokio::test]
    async fn cancelled_build_releases_lease_promptly() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let store = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let lease = store
            .acquire_incremental_build_lease(1)
            .await
            .unwrap()
            .unwrap();
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
                .acquire_incremental_build_lease(2)
                .await
                .unwrap()
                .is_none()
        );

        build.abort();
        assert!(build.await.unwrap_err().is_cancelled());
        let replacement = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(lease) = store.acquire_incremental_build_lease(2).await.unwrap() {
                    return lease;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cancelled build lease should be cleaned before its TTL");

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
        assert!(pending.branches.is_empty());
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
    async fn materialized_shell_branches_switch_with_the_active_generation() {
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
        let activated_generation = snapshots.active_generation().await.unwrap();
        let activated_shell = cache
            .materialized_shell_current_ready_or_schedule(GraphMode::All)
            .await
            .unwrap()
            .unwrap();

        assert_ne!(activated_generation, initial_generation);
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
        let version = publisher.mark_branch_changed("main");

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
        let version = publisher.mark_branch_changed("main");
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
