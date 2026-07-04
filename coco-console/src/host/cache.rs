use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use super::snapshot_store::ConsoleGraphSnapshotStore;
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
use crate::layout::{
    lane_key, layout_graph_viewport, layout_graph_viewport_diff, materialize_graph_viewport,
};
use crate::publisher::ConsolePublisher;
use coco_mem::{BranchStore, NodeStore, SessionState, SessionStore, SqliteGraphStore, Store};
use serde::Serialize;
use snafu::prelude::*;
#[cfg(test)]
use tokio::sync::Semaphore;

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
    #[cfg(test)]
    compute_permits: Arc<Semaphore>,
    publish_lock: Arc<Mutex<()>>,
    state: Arc<Mutex<CacheState>>,
}

#[derive(Clone)]
enum ConsoleGraphSource<S> {
    #[allow(dead_code)]
    Store(S),
    PersistentStore(SqliteGraphStore),
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
        log_rebuild_status!(status);
        $cache.set_rebuild_status(status);
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
            #[cfg(test)]
            compute_permits: Arc::new(Semaphore::new(1)),
            publish_lock: Arc::new(Mutex::new(())),
            state: Arc::new(Mutex::new(CacheState::default())),
        }
    }

    pub async fn new_with_persistent_store_path(
        _store: S,
        invalidations: ConsolePublisher,
        path: PathBuf,
    ) -> crate::Result<Self> {
        let store = SqliteGraphStore::open_read_only(&path)
            .await
            .context(crate::error::StoreSnafu)?;
        let snapshots = ConsoleGraphSnapshotStore::open(&path).await?;
        let persisted_version = latest_persistent_materialization_version(&snapshots)?;
        let ready = ConsolePublisher::new();
        if let Some(version) = persisted_version {
            ready.advance_to(version);
        }
        invalidations.advance_to(
            persisted_version
                .map(|version| version.saturating_add(1))
                .unwrap_or(1),
        );
        Ok(Self {
            source: ConsoleGraphSource::PersistentStore(store),
            invalidations,
            ready,
            progress: ConsolePublisher::new(),
            snapshots: Some(snapshots),
            #[cfg(test)]
            compute_permits: Arc::new(Semaphore::new(1)),
            publish_lock: Arc::new(Mutex::new(())),
            state: Arc::new(Mutex::new(CacheState::default())),
        })
    }

    pub fn current_version(&self) -> u64 {
        self.ready.current_version()
    }

    pub fn current_viewport_version(&self, mode: GraphMode) -> u64 {
        match self.snapshots.as_ref() {
            Some(snapshots) => snapshots
                .latest_materialization_version(mode)
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
    pub async fn run_blocking_graph_compute<T, F>(&self, compute: F) -> crate::Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.run_blocking_graph_compute_with(|| (), |_| compute())
            .await
    }

    #[cfg(test)]
    pub async fn run_blocking_graph_compute_with<T, I, P, F>(
        &self,
        prepare: P,
        compute: F,
    ) -> crate::Result<T>
    where
        T: Send + 'static,
        I: Send + 'static,
        P: FnOnce() -> I + Send,
        F: FnOnce(I) -> T + Send + 'static,
    {
        let Ok(_permit) = self.compute_permits.clone().acquire_owned().await else {
            return Err(crate::Error::ConsoleGraphRebuild {
                mode: "any",
                source_version: self.invalidations.current_version(),
                message: "graph compute limiter closed".to_owned(),
            });
        };
        let input = prepare();
        tokio::task::spawn_blocking(move || compute(input))
            .await
            .context(crate::error::JoinConsoleServerSnafu)
    }

    pub fn rebuild_requested_modes(&self) {
        for mode in [GraphMode::Anchors, GraphMode::All] {
            self.ensure_viewport_current(mode);
        }
    }

    #[cfg(test)]
    pub(crate) async fn snapshot_current(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Arc<GraphSnapshot>> {
        let source_version = self.invalidations.current_version();
        if let Some(snapshot) = self.cached_snapshot(mode, source_version) {
            return Ok(snapshot);
        }
        let graph_version = source_version;
        let source = self.source.clone();
        set_rebuild_status!(
            self,
            rebuild_status(
                mode,
                source_version,
                ConsoleGraphRebuildState::Building,
                None,
                0,
                0,
                "Building graph snapshot",
            )
        );
        let progress_cache = self.clone();
        let snapshot = source
            .build_snapshot_with_progress(mode, graph_version, |progress| {
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
            })
            .await?;
        let snapshot = Arc::new(snapshot);
        self.store_cached_snapshot(mode, source_version, snapshot.clone());
        self.publish_ready_version(source_version);
        set_rebuild_status!(
            self,
            rebuild_status(
                mode,
                source_version,
                ConsoleGraphRebuildState::Ready,
                None,
                1,
                1,
                "Graph snapshot ready",
            )
        );
        Ok(snapshot)
    }

    pub(crate) fn snapshot_current_ready_or_schedule(
        &self,
        mode: GraphMode,
    ) -> Option<Arc<GraphSnapshot>> {
        let source_version = self.invalidations.current_version();
        if let Some(snapshot) = self.cached_snapshot(mode, source_version) {
            return Some(snapshot);
        }
        self.ensure_snapshot_current(mode);
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
        self.ensure_viewport_current(mode);
        let reference = snapshots.materialized_node_reference(mode, target)?;
        let (node_id, labels) = match reference {
            Some(reference) => (reference.node_id, reference.labels),
            None => {
                let Some(node_id) = node_id_from_graph_target(target) else {
                    return Ok(None);
                };
                if snapshots.has_materialization(mode)?
                    && !self
                        .node_is_in_materialized_provider_context(snapshots, mode, &node_id)
                        .await?
                {
                    return Ok(None);
                }
                (node_id, Vec::new())
            }
        };
        match self.source.clone().get_node(&node_id) {
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
            .materialized_node_points(mode, &node_ids)?
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
        self.ensure_viewport_current(mode);
        let materialized_reference = snapshots.materialized_node_reference(mode, target)?;
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
        let points = snapshots.materialized_node_points(mode, &node_ids)?;
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

    pub(crate) async fn materialized_fragment_current_ready_or_schedule(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Option<MaterializedGraphShell>> {
        let Some(snapshots) = &self.snapshots else {
            return Ok(None);
        };
        self.ensure_viewport_current(mode);
        let Some(facts) = snapshots.materialized_shell_facts(mode)? else {
            return Ok(None);
        };
        let branches = self
            .source
            .clone()
            .materialized_shell_branches(&facts.lanes)
            .await?;
        let mut time_ticks = Vec::with_capacity(facts.nodes.len());
        for node in &facts.nodes {
            let store_node = self.source.clone().get_node(&node.node_id)?;
            time_ticks.push(MaterializedGraphShellTick {
                time_ns: store_node.created_at.as_nanosecond(),
                label: store_node.created_at.to_string(),
                graph_x: f64::from(node.point.x),
            });
        }
        Ok(Some(MaterializedGraphShell {
            version: facts.version,
            mode,
            node_count: facts.nodes.len(),
            edge_count: facts.edge_count,
            branches,
            time_ticks,
        }))
    }

    pub(crate) async fn snapshot_after(
        &self,
        mode: GraphMode,
        observed_version: u64,
    ) -> crate::Result<Arc<GraphSnapshot>> {
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
                    self.ensure_viewport_current(mode);
                }
            }
        }
    }

    pub fn viewport_current_ready_or_schedule(
        &self,
        mode: GraphMode,
        request: GraphViewportRequest,
    ) -> Option<GraphViewportResponse> {
        if let Some(snapshots) = &self.snapshots {
            self.ensure_viewport_current(mode);
            return snapshots.latest_viewport(mode, request).ok().flatten();
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
            let mut invalidations = self.invalidations.subscribe();
            let mut progress = self.progress.subscribe();
            if let Some(response) = self.viewport_current_ready_or_schedule(mode, request)
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
                changed = invalidations.changed() => {
                    if changed.is_err() {
                        continue;
                    }
                    self.ensure_viewport_current(mode);
                }
            }
        }
    }

    pub fn viewport_diff_current_ready_or_schedule(
        &self,
        mode: GraphMode,
        request: GraphViewportDiffRequest,
    ) -> Option<GraphViewportDiffResponse> {
        if let Some(snapshots) = &self.snapshots {
            self.ensure_viewport_current(mode);
            return snapshots.latest_viewport_diff(mode, request).ok().flatten();
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
            let mut invalidations = self.invalidations.subscribe();
            let mut progress = self.progress.subscribe();
            if let Some(response) =
                self.viewport_diff_current_ready_or_schedule(mode, request.clone())
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
                changed = invalidations.changed() => {
                    if changed.is_err() {
                        continue;
                    }
                    self.ensure_viewport_current(mode);
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
        self.ensure_viewport_current(mode);
        for _ in 0..50 {
            if self.materialization_current(mode, source_version) {
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

    fn ensure_snapshot_current(&self, mode: GraphMode) {
        let source_version = self.invalidations.current_version();
        if self.cached_snapshot(mode, source_version).is_some() {
            set_rebuild_status!(
                self,
                rebuild_status(
                    mode,
                    source_version,
                    ConsoleGraphRebuildState::Ready,
                    None,
                    1,
                    1,
                    "Graph snapshot ready",
                )
            );
            return;
        }
        if !self.mark_rebuild_scheduled(mode, source_version) {
            return;
        }

        let cache = self.clone();
        tokio::spawn(async move {
            cache.rebuild_snapshot(mode, source_version).await;
        });
    }

    fn ensure_viewport_current(&self, mode: GraphMode) {
        let source_version = self.invalidations.current_version();
        if (self.snapshots.is_none() && self.cached_snapshot(mode, source_version).is_some())
            || self.materialization_current(mode, source_version)
        {
            set_rebuild_status!(
                self,
                rebuild_status(
                    mode,
                    source_version,
                    ConsoleGraphRebuildState::Ready,
                    None,
                    1,
                    1,
                    "Graph materialization ready",
                )
            );
            return;
        }
        if !self.mark_rebuild_scheduled(mode, source_version) {
            return;
        }

        let cache = self.clone();
        tokio::spawn(async move {
            cache.rebuild_snapshot(mode, source_version).await;
        });
    }

    async fn rebuild_snapshot(&self, mode: GraphMode, source_version: u64) {
        let uses_materialized_store = self.snapshots.is_some();
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
        if let Some(snapshots) = self.snapshots.clone() {
            let has_non_empty_materialization = snapshots.has_non_empty_materialization(mode);
            let source = self.source.clone();
            let incremental_result = source
                .try_append_linear_materialization(snapshots, mode, source_version)
                .await;
            let fallback_message = match incremental_result {
                Ok(true) => {
                    self.publish_ready_version(source_version);
                    set_rebuild_status!(
                        self,
                        rebuild_status(
                            mode,
                            source_version,
                            ConsoleGraphRebuildState::Ready,
                            None,
                            1,
                            1,
                            "Graph materialization updated",
                        )
                    );
                    return;
                }
                Ok(false) => match has_non_empty_materialization {
                    Ok(true) => {
                        set_rebuild_status!(
                            self,
                            rebuild_status(
                                mode,
                                source_version,
                                ConsoleGraphRebuildState::Failed,
                                None,
                                0,
                                0,
                                "Incremental graph materialization could not apply this store change",
                            )
                        );
                        return;
                    }
                    Ok(false) => {
                        "Incremental graph materialization could not seed this store state; rebuilding full materialization"
                    }
                    Err(error) => {
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
                        return;
                    }
                },
                Err(error) => {
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
                    return;
                }
            };
            set_rebuild_status!(
                self,
                rebuild_status(
                    mode,
                    source_version,
                    ConsoleGraphRebuildState::Building,
                    None,
                    0,
                    0,
                    fallback_message,
                )
            );
        }
        let source = self.source.clone();
        let progress_cache = self.clone();
        let snapshots = self.snapshots.clone();
        let result = async move {
            let snapshot = source
                .build_snapshot_with_progress(mode, source_version, |progress| {
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
                })
                .await?;
            if let Some(snapshots) = snapshots {
                let branch_labels = visible_full_layout_branch_labels(&snapshot);
                snapshots.replace_materialization_from_viewport(
                    mode,
                    materialize_graph_viewport(&snapshot),
                    branch_labels,
                )?;
            }
            crate::Result::Ok(snapshot)
        }
        .await;
        match result {
            Ok(snapshot) => {
                self.store_cached_snapshot(mode, source_version, Arc::new(snapshot));
                self.publish_ready_version(source_version);
                set_rebuild_status!(
                    self,
                    rebuild_status(
                        mode,
                        source_version,
                        ConsoleGraphRebuildState::Ready,
                        None,
                        1,
                        1,
                        "Graph snapshot ready",
                    )
                );
            }
            Err(error) => {
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
        }
    }

    fn materialization_current(&self, mode: GraphMode, source_version: u64) -> bool {
        self.snapshots.as_ref().and_then(|snapshots| {
            snapshots
                .latest_materialization_version(mode)
                .ok()
                .flatten()
        }) == Some(source_version)
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

    fn mark_rebuild_scheduled(&self, mode: GraphMode, source_version: u64) -> bool {
        let mut state = self
            .state
            .lock()
            .expect("console graph cache lock poisoned");
        if self.snapshots.is_none()
            && state
                .slot(mode)
                .as_ref()
                .is_some_and(|cached| cached.source_version == source_version)
        {
            return false;
        }
        if state.rebuild_slot(mode).as_ref().is_some_and(|status| {
            status.source_version == source_version
                && matches!(
                    status.state,
                    ConsoleGraphRebuildState::Scheduled
                        | ConsoleGraphRebuildState::Building
                        | ConsoleGraphRebuildState::Failed
                )
        }) {
            return false;
        }
        let status = rebuild_status(
            mode,
            source_version,
            ConsoleGraphRebuildState::Scheduled,
            None,
            0,
            0,
            "Graph snapshot scheduled",
        );
        log_rebuild_status!(status);
        *state.rebuild_slot_mut(mode) = Some(status);
        drop(state);
        self.progress.mark_changed();
        true
    }

    fn set_rebuild_status(&self, status: ConsoleGraphRebuildStatus) {
        let mode = status.mode;
        let mut state = self
            .state
            .lock()
            .expect("console graph cache lock poisoned");
        *state.rebuild_slot_mut(mode) = Some(status);
        drop(state);
        self.progress.mark_changed();
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

fn latest_persistent_materialization_version(
    snapshots: &ConsoleGraphSnapshotStore,
) -> crate::Result<Option<u64>> {
    [GraphMode::Anchors, GraphMode::All]
        .into_iter()
        .map(|mode| snapshots.latest_materialization_version(mode))
        .try_fold(None, |latest: Option<u64>, version| {
            version.map(|version| match (latest, version) {
                (Some(left), Some(right)) => Some(left.max(right)),
                (Some(left), None) => Some(left),
                (None, right) => right,
            })
        })
}

fn visible_full_layout_branch_labels(snapshot: &GraphSnapshot) -> BTreeSet<String> {
    let node_ids = snapshot
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<BTreeSet<_>>();
    snapshot
        .branches
        .iter()
        .filter(|branch| {
            branch
                .visible_head_id
                .as_deref()
                .is_some_and(|head_id| node_ids.contains(head_id))
        })
        .map(|branch| branch.name.clone())
        .collect()
}

fn node_id_from_graph_target(target: &str) -> Option<String> {
    target
        .strip_prefix("detail-")
        .filter(|node_id| !node_id.is_empty())
        .map(str::to_owned)
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
            Self::PersistentStore(store) => {
                build_graph_snapshot_with_mode_and_progress(&store, version, mode, progress).await
            }
        }
    }

    async fn try_append_linear_materialization(
        self,
        snapshots: ConsoleGraphSnapshotStore,
        mode: GraphMode,
        source_version: u64,
    ) -> crate::Result<bool> {
        match self {
            Self::Store(store) => {
                snapshots
                    .try_append_linear_branch(source_version, mode, &store)
                    .await
            }
            Self::PersistentStore(store) => {
                snapshots
                    .try_append_linear_branch(source_version, mode, &store)
                    .await
            }
        }
    }

    fn get_node(self, node_id: &str) -> crate::Result<coco_mem::Node> {
        match self {
            Self::Store(store) => store.get_node(node_id).context(crate::error::StoreSnafu),
            Self::PersistentStore(store) => {
                store.get_node(node_id).context(crate::error::StoreSnafu)
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
            Self::PersistentStore(store) => {
                provider_context_for_node(&store, target_node_id, context).await
            }
        }
    }

    async fn materialized_shell_branches(
        self,
        lanes: &[crate::api::GraphViewportLane],
    ) -> crate::Result<Vec<MaterializedGraphShellBranch>> {
        match self {
            Self::Store(store) => materialized_shell_branches(&store, lanes).await,
            Self::PersistentStore(store) => materialized_shell_branches(&store, lanes).await,
        }
    }
}

async fn materialized_shell_branches(
    store: &(impl BranchStore + SessionStore),
    lanes: &[crate::api::GraphViewportLane],
) -> crate::Result<Vec<MaterializedGraphShellBranch>> {
    let lane_by_key = lanes
        .iter()
        .map(|lane| (lane.key.as_str(), lane))
        .collect::<BTreeMap<_, _>>();
    let mut states = store
        .list_session_states()
        .await
        .context(crate::error::StoreSnafu)?
        .into_iter()
        .collect::<Vec<(String, SessionState)>>();
    states.sort_by(|(left_branch, _), (right_branch, _)| {
        let left_key = lane_key(left_branch);
        let left_lane = lane_by_key
            .get(left_key.as_str())
            .map(|lane| lane.y)
            .unwrap_or(i32::MAX);
        let right_key = lane_key(right_branch);
        let right_lane = lane_by_key
            .get(right_key.as_str())
            .map(|lane| lane.y)
            .unwrap_or(i32::MAX);
        left_lane
            .cmp(&right_lane)
            .then_with(|| left_branch.cmp(right_branch))
    });

    let mut branches = Vec::new();
    for (branch, state) in states {
        let branch_key = lane_key(&branch);
        let lane = lane_by_key.get(branch_key.as_str());
        let head_id = store
            .get_branch_head(&branch)
            .await
            .context(crate::error::StoreSnafu)?;
        branches.push(MaterializedGraphShellBranch {
            name: branch,
            key: lane
                .map(|lane| lane.key.clone())
                .unwrap_or_else(|| branch_key.clone()),
            lane_y: lane.map(|lane| lane.y),
            head_short_id: crate::graph::shorten_id(&head_id),
            state,
        });
    }
    Ok(branches)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ConsoleStore;
    use crate::host::snapshot_store::ConsoleGraphSnapshotStore;
    use coco_mem::{
        Anchor, BranchStore, JobStore, Kind, MergeParent, NewNode, NodeStore, PersistentStore,
        PromptAnchor, Role, SessionAnchor, SessionRole, SkillInvocationAnchor, SkillInvocationMode,
        SkillResultAnchor, SqliteStore, Tool, ToolUse,
    };
    use serde_json::json;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::oneshot;
    use tokio::time::{Duration, sleep};

    async fn test_store() -> SqliteStore {
        SqliteStore::open_temporary()
            .await
            .expect("temporary SQLite store should open")
    }

    fn session_anchor() -> SessionAnchor {
        SessionAnchor {
            role: SessionRole::Orchestrator,
            provider_profile: None,
            provider: Some("openai".to_owned()),
            model: "gpt-4.1-mini".to_owned(),
            tools: Vec::<Tool>::new(),
            system_prompt: "You are helpful.".to_owned(),
            prompt: "Start".to_owned(),
            temperature: None,
            max_tokens: None,
            additional_params: None,
            enable_coco_shim: false,
            active_skill: None,
        }
    }

    async fn graph_store(
        publisher: ConsolePublisher,
    ) -> (ConsoleStore<SqliteStore>, String, String) {
        let store = ConsoleStore::new(test_store().await, publisher);
        let root = store.root_id();
        let session = store
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        store.fork("main", &session).await.unwrap();
        let text = store
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("visible only in all mode".to_owned()),
            })
            .await
            .unwrap();
        store
            .set_branch_head("main", &session, &text)
            .await
            .unwrap();

        (store, session, text)
    }

    fn temp_store_path() -> PathBuf {
        static TEMP_STORE_COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let process_id = std::process::id();
        let counter = TEMP_STORE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("coco-console-graph-{process_id}-{nanos}-{counter}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn materialized_shell_branches_use_branch_lane_keys() {
        let store = test_store().await;
        let root = store.root_id();
        let session = store
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        let branch = "orphan abc123";
        store.fork(branch, &session).await.unwrap();
        let branch_lane = crate::api::GraphViewportLane {
            key: lane_key(branch),
            label: branch.to_owned(),
            y: 10,
        };
        let derived_lane = crate::api::GraphViewportLane {
            key: "derived:orphan:abc123".to_owned(),
            label: branch.to_owned(),
            y: 20,
        };

        let branches = materialized_shell_branches(&store, &[branch_lane.clone(), derived_lane])
            .await
            .unwrap();

        assert_eq!(branches.len(), 1);
        assert_eq!(branches[0].name, branch);
        assert_eq!(branches[0].key, branch_lane.key);
        assert_eq!(branches[0].lane_y, Some(branch_lane.y));
    }

    #[tokio::test]
    async fn materialized_shell_branches_preserve_hidden_branches() {
        let store = test_store().await;
        let root = store.root_id();
        store.fork("hidden", &root).await.unwrap();

        let branches = materialized_shell_branches(&store, &[]).await.unwrap();

        assert_eq!(branches.len(), 1);
        assert_eq!(branches[0].name, "hidden");
        assert_eq!(branches[0].key, lane_key("hidden"));
        assert_eq!(branches[0].lane_y, None);
    }

    #[test]
    fn graph_rebuild_status_logs_phase_boundaries_and_progress_buckets_at_info() {
        let phase_start = rebuild_status(
            GraphMode::All,
            1,
            ConsoleGraphRebuildState::Building,
            Some(crate::graph::GraphBuildPhase::Entries),
            0,
            10,
            "Building graph entries",
        );
        let phase_progress_before_bucket = rebuild_status(
            GraphMode::All,
            1,
            ConsoleGraphRebuildState::Building,
            Some(crate::graph::GraphBuildPhase::Entries),
            4,
            100,
            "Building graph entries",
        );
        let phase_progress_bucket = rebuild_status(
            GraphMode::All,
            1,
            ConsoleGraphRebuildState::Building,
            Some(crate::graph::GraphBuildPhase::Entries),
            10,
            100,
            "Building graph entries",
        );
        let phase_complete = rebuild_status(
            GraphMode::All,
            1,
            ConsoleGraphRebuildState::Building,
            Some(crate::graph::GraphBuildPhase::Entries),
            10,
            10,
            "Building graph entries",
        );
        let ready = rebuild_status(
            GraphMode::All,
            1,
            ConsoleGraphRebuildState::Ready,
            None,
            1,
            1,
            "Graph snapshot ready",
        );

        assert!(should_log_rebuild_status_at_info(&phase_start));
        assert!(!should_log_rebuild_status_at_info(
            &phase_progress_before_bucket
        ));
        assert!(should_log_rebuild_status_at_info(&phase_progress_bucket));
        assert!(should_log_rebuild_status_at_info(&phase_complete));
        assert!(should_log_rebuild_status_at_info(&ready));
        assert_eq!(rebuild_progress_percent(&phase_progress_bucket), 10);
    }

    #[tokio::test]
    async fn cache_builds_snapshots_per_mode_on_demand() {
        let publisher = ConsolePublisher::new();
        let (store, session, text) = graph_store(publisher.clone()).await;
        let cache = ConsoleGraphCache::new(store, publisher);

        let all = cache.current_snapshot(GraphMode::All).await;
        let anchors = cache.current_snapshot(GraphMode::Anchors).await;

        assert_eq!(all.mode, GraphMode::All);
        assert!(all.nodes.iter().any(|node| node.id == text));
        assert!(all.nodes.iter().any(|node| node.id == session));
        assert_eq!(anchors.mode, GraphMode::Anchors);
        assert!(anchors.nodes.iter().any(|node| node.id == session));
        assert!(!anchors.nodes.iter().any(|node| node.id == text));
    }

    #[tokio::test]
    async fn cache_reads_latest_store_state_without_background_rebuild() {
        let publisher = ConsolePublisher::new();
        let (store, _, text) = graph_store(publisher.clone()).await;
        let cache = ConsoleGraphCache::new(store.clone(), publisher.clone());

        let initial = cache.current_snapshot(GraphMode::All).await;
        let next_text = store
            .append(NewNode {
                parent: text.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("new all-mode node".to_owned()),
            })
            .await
            .unwrap();
        store
            .set_branch_head("main", &text, &next_text)
            .await
            .unwrap();

        let refreshed = cache.current_snapshot(GraphMode::All).await;

        assert!(refreshed.version > initial.version);
        assert!(refreshed.nodes.iter().any(|node| node.id == next_text));
    }

    #[tokio::test]
    async fn cache_reuses_snapshot_for_same_source_version() {
        let publisher = ConsolePublisher::new();
        let (store, _, text) = graph_store(publisher.clone()).await;
        let cache = ConsoleGraphCache::new(store.clone(), publisher);

        let first = cache.current_snapshot(GraphMode::All).await;
        let second = cache.current_snapshot(GraphMode::All).await;

        assert!(Arc::ptr_eq(&first, &second));

        let next_text = store
            .append(NewNode {
                parent: text.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("new version".to_owned()),
            })
            .await
            .unwrap();
        store
            .set_branch_head("main", &text, &next_text)
            .await
            .unwrap();
        let third = cache.current_snapshot(GraphMode::All).await;

        assert!(!Arc::ptr_eq(&first, &third));
        assert!(third.nodes.iter().any(|node| node.id == next_text));
    }

    #[tokio::test]
    async fn cache_publishes_ready_version_after_snapshot_build() {
        let publisher = ConsolePublisher::new();
        let (store, _, _) = graph_store(publisher.clone()).await;
        let cache = ConsoleGraphCache::new(store, publisher);
        assert_eq!(cache.current_version(), 0);

        let first = cache.current_snapshot(GraphMode::All).await;
        let ready_version = cache.current_version();
        let second = cache.current_snapshot(GraphMode::All).await;

        assert_eq!(first.version, ready_version);
        assert_eq!(second.version, ready_version);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn cache_reports_viewport_version_per_materialized_mode() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert_eq!(
            cache.current_viewport_version(GraphMode::All),
            target_version
        );
        assert_eq!(cache.current_viewport_version(GraphMode::Anchors), 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_schedules_snapshot_without_blocking_ready_reader() {
        let publisher = ConsolePublisher::new();
        let (store, _, _) = graph_store(publisher.clone()).await;
        let cache = ConsoleGraphCache::new(store, publisher.clone());
        publisher.mark_changed();
        let source_version = publisher.current_version();

        let snapshot = cache.snapshot_current_ready_or_schedule(GraphMode::All);

        assert!(snapshot.is_none());
        assert_eq!(cache.current_version(), 0);
        assert!(cache.rebuild_statuses().iter().any(|status| {
            status.mode == GraphMode::All
                && status.source_version == source_version
                && matches!(
                    status.state,
                    ConsoleGraphRebuildState::Scheduled | ConsoleGraphRebuildState::Building
                )
        }));
    }

    #[tokio::test]
    async fn cache_reopens_persistent_store_path_for_latest_graph_state() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("visible child".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &text)
            .await
            .unwrap();
        publisher.mark_changed();

        let snapshot = cache.current_snapshot(GraphMode::All).await;

        assert!(snapshot.nodes.iter().any(|node| node.id == session));
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_persists_graph_locations_to_sqlite_graph_database() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("visible child".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &text)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        assert!(
            cache
                .viewport_current_ready_or_schedule(
                    GraphMode::All,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .is_none()
        );
        let mut materialized = None;
        for _ in 0..50 {
            materialized = ConsoleGraphSnapshotStore::open(&path)
                .await
                .unwrap()
                .latest_viewport(
                    GraphMode::All,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .unwrap();
            if materialized
                .as_ref()
                .is_some_and(|viewport| viewport.version == target_version)
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let materialized = materialized.expect("materialized viewport should be stored");
        let database_path = crate::host::snapshot_store::database_path(&path);
        assert_eq!(
            sqlite_table_row_count(&database_path, "console_graph_materializations").await,
            1
        );
        assert!(sqlite_table_row_count(&database_path, "console_graph_node_locations").await > 0);
        assert!(sqlite_table_row_count(&database_path, "console_graph_edge_routes").await > 0);
        assert!(
            !sqlite_table_has_column(
                &database_path,
                "console_graph_node_locations",
                "source_version",
            )
            .await
        );
        assert!(
            !sqlite_table_has_column(
                &database_path,
                "console_graph_edge_routes",
                "source_version",
            )
            .await
        );
        assert!(!sqlite_table_exists(&database_path, "console_graph_snapshots").await);
        let reopened_publisher = ConsolePublisher::new();
        let reopened_cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            reopened_publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let reopened = reopened_cache.snapshot_current_ready(GraphMode::All);
        assert_eq!(reopened_cache.current_version(), target_version);
        assert_eq!(reopened_publisher.current_version(), target_version + 1);
        let stale_reopened_materialized = reopened_cache
            .viewport_current_ready_or_schedule(
                GraphMode::All,
                crate::host::api::GraphViewportRequest::default(),
            )
            .unwrap();
        let reopened_materialized = reopened_cache
            .viewport_after(
                GraphMode::All,
                target_version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(materialized.version, target_version);
        assert_eq!(stale_reopened_materialized.version, target_version);
        assert_eq!(reopened_materialized.version, target_version + 1);
        assert!(materialized.nodes.iter().any(|node| node.id == session));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert!(reopened.is_none());
        assert!(
            reopened_materialized
                .nodes
                .iter()
                .any(|node| node.id == session)
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn graph_materialization_preflight_read_does_not_block_store_writes() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        ConsoleGraphSnapshotStore::open(&path).await.unwrap();

        let graph_database_path = crate::host::snapshot_store::database_path(&path);
        let graph_database =
            coco_mem::SqliteDatabase::open_unshared_file_path(&graph_database_path)
                .await
                .unwrap();
        let (transaction_started_tx, transaction_started_rx) = mpsc::channel();
        let (release_transaction_tx, release_transaction_rx) = mpsc::channel();
        let transaction = std::thread::spawn(move || {
            use crate::schema::console_graph_materializations::dsl as materializations;
            use diesel::Connection;
            use diesel::prelude::*;

            with_sqlite_test_connection(graph_database, move |connection| {
                connection
                    .transaction::<(), diesel::result::Error, _>(|connection| {
                        assert_eq!(
                            materializations::console_graph_materializations
                                .count()
                                .get_result::<i64>(connection)?,
                            0,
                        );
                        transaction_started_tx.send(()).unwrap();
                        release_transaction_rx.recv().unwrap();
                        Ok(())
                    })
                    .unwrap();
            });
        });
        transaction_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("graph materialization preflight read should start");

        let writer_for_thread = writer.clone();
        let root = writer.root_id();
        let (write_tx, write_rx) = oneshot::channel();
        let write = tokio::spawn(async move {
            let node = writer_for_thread
                .append(NewNode {
                    parent: root,
                    role: Role::User,
                    metadata: None,
                    kind: Kind::Text("store write while graph db is locked".to_owned()),
                })
                .await
                .unwrap();
            write_tx.send(node).unwrap();
        });

        let written = tokio::time::timeout(Duration::from_secs(1), write_rx).await;
        release_transaction_tx.send(()).unwrap();
        transaction.join().unwrap();
        write.await.unwrap();
        let written = written
            .expect("store write should not wait for graph transaction release")
            .unwrap();
        assert_eq!(writer.get_node(&written).unwrap().id, written);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn graph_materialization_write_does_not_block_store_writes() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        ConsoleGraphSnapshotStore::open(&path).await.unwrap();

        let graph_database_path = crate::host::snapshot_store::database_path(&path);
        let graph_database =
            coco_mem::SqliteDatabase::open_unshared_file_path(&graph_database_path)
                .await
                .unwrap();
        let (transaction_started_tx, transaction_started_rx) = mpsc::channel();
        let (release_transaction_tx, release_transaction_rx) = mpsc::channel();
        let transaction = std::thread::spawn(move || {
            with_sqlite_test_connection(graph_database, move |connection| {
                connection
                    .immediate_transaction::<(), diesel::result::Error, _>(|_| {
                        transaction_started_tx.send(()).unwrap();
                        release_transaction_rx.recv().unwrap();
                        Ok(())
                    })
                    .unwrap();
            });
        });
        transaction_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("graph materialization write transaction should start");

        let writer_for_thread = writer.clone();
        let root = writer.root_id();
        let (write_tx, write_rx) = oneshot::channel();
        let write = tokio::spawn(async move {
            let node = writer_for_thread
                .append(NewNode {
                    parent: root,
                    role: Role::User,
                    metadata: None,
                    kind: Kind::Text("store write while graph db is locked".to_owned()),
                })
                .await
                .unwrap();
            write_tx.send(node).unwrap();
        });

        let written = tokio::time::timeout(Duration::from_secs(1), write_rx)
            .await
            .expect("store write should not wait for graph transaction release")
            .unwrap();
        release_transaction_tx.send(()).unwrap();
        transaction.join().unwrap();
        write.await.unwrap();
        assert_eq!(writer.get_node(&written).unwrap().id, written);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_seeds_initial_materialization_with_skill_invocation_subtree() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-1".to_owned(),
                    name: "skill".to_owned(),
                    input: json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &tool_use)
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
                        skill_name: "fast-rust".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let result = writer
            .append(NewNode {
                parent: invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "fast-rust".to_owned(),
                        output: "done".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        assert!(
            cache
                .viewport_current_ready_or_schedule(
                    GraphMode::All,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .is_none()
        );
        let mut materialized = None;
        for _ in 0..50 {
            materialized = ConsoleGraphSnapshotStore::open(&path)
                .await
                .unwrap()
                .latest_viewport(
                    GraphMode::All,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .unwrap();
            if materialized
                .as_ref()
                .is_some_and(|viewport| viewport.version == target_version)
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let materialized = materialized.expect("materialized viewport should be stored");
        let node_ids = materialized
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<BTreeSet<_>>();

        assert!(node_ids.contains(invocation.as_str()));
        assert!(node_ids.contains(result.as_str()));
        let main_y = materialized
            .lanes
            .iter()
            .find(|lane| lane.label == "main")
            .unwrap()
            .y;
        assert_eq!(
            materialized
                .nodes
                .iter()
                .find(|node| node.id == tool_use)
                .unwrap()
                .y,
            main_y
        );
        assert_ne!(
            materialized
                .nodes
                .iter()
                .find(|node| node.id == invocation)
                .unwrap()
                .y,
            main_y
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_falls_back_when_unchanged_head_gains_skill_invocation_subtree() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-1".to_owned(),
                    name: "skill".to_owned(),
                    input: json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &tool_use)
            .await
            .unwrap();
        publisher.mark_changed();
        let initial_version = publisher.current_version();
        let initial = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        assert_eq!(initial.version, initial_version);

        let invocation = writer
            .append(NewNode {
                parent: tool_use.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_invocation(
                    Vec::new(),
                    SkillInvocationAnchor {
                        skill_name: "fast-rust".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let result = writer
            .append(NewNode {
                parent: invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "fast-rust".to_owned(),
                        output: "done".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let node_ids = viewport
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<BTreeSet<_>>();

        assert_eq!(viewport.version, target_version);
        assert!(node_ids.contains(invocation.as_str()));
        assert!(node_ids.contains(result.as_str()));
        let main_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == "main")
            .unwrap()
            .y;
        assert_eq!(
            viewport
                .nodes
                .iter()
                .find(|node| node.id == tool_use)
                .unwrap()
                .y,
            main_y
        );
        assert_ne!(
            viewport
                .nodes
                .iter()
                .find(|node| node.id == result)
                .unwrap()
                .y,
            main_y
        );
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_falls_back_when_appended_tool_use_has_skill_invocation_subtree() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        publisher.mark_changed();
        let initial = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-1".to_owned(),
                    name: "skill".to_owned(),
                    input: json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &tool_use)
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
                        skill_name: "fast-rust".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let result = writer
            .append(NewNode {
                parent: invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "fast-rust".to_owned(),
                        output: "done".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let node_ids = viewport
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<BTreeSet<_>>();

        assert_eq!(viewport.version, target_version);
        assert!(node_ids.contains(invocation.as_str()));
        assert!(node_ids.contains(result.as_str()));
        let main_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == "main")
            .unwrap()
            .y;
        assert_eq!(
            viewport
                .nodes
                .iter()
                .find(|node| node.id == tool_use)
                .unwrap()
                .y,
            main_y
        );
        assert_ne!(
            viewport
                .nodes
                .iter()
                .find(|node| node.id == invocation)
                .unwrap()
                .y,
            main_y
        );
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_adopts_skill_lane_when_branch_forks_at_skill_result() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-1".to_owned(),
                    name: "skill".to_owned(),
                    input: json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &tool_use)
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
                        skill_name: "fast-rust".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let result = writer
            .append(NewNode {
                parent: invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "fast-rust".to_owned(),
                        output: "done".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        publisher.mark_changed();
        let initial = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let skill_lane = format!("skill {}", crate::graph::shorten_id(&result));
        assert!(initial.lanes.iter().any(|lane| lane.label == skill_lane));

        writer.fork("draft", &result).await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();
        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let draft_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == "draft")
            .unwrap()
            .y;

        assert_eq!(viewport.version, target_version);
        assert!(!viewport.lanes.iter().any(|lane| lane.label == skill_lane));
        assert_eq!(
            viewport
                .nodes
                .iter()
                .find(|node| node.id == invocation)
                .unwrap()
                .y,
            draft_y
        );
        assert_eq!(
            viewport
                .nodes
                .iter()
                .find(|node| node.id == result)
                .unwrap()
                .y,
            draft_y
        );
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == tool_use
                && edge.target_id == invocation
                && edge.target.y == draft_y
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_appends_missing_skill_result_to_derived_lane_when_branch_has_invocation() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-1".to_owned(),
                    name: "skill".to_owned(),
                    input: json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &tool_use)
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
                        skill_name: "fast-rust".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        writer.fork("draft", &invocation).await.unwrap();
        publisher.mark_changed();
        let initial = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        let result = writer
            .append(NewNode {
                parent: invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "fast-rust".to_owned(),
                        output: "done".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();
        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let draft_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == "draft")
            .unwrap()
            .y;
        let result_node = viewport
            .nodes
            .iter()
            .find(|node| node.id == result)
            .unwrap()
            .clone();
        let skill_y = result_node.y;

        assert_eq!(viewport.version, target_version);
        assert_ne!(skill_y, draft_y);
        assert!(
            viewport
                .lanes
                .iter()
                .any(|lane| lane.label.starts_with("skill ") && lane.y == skill_y)
        );
        assert!(viewport.nodes.iter().any(|node| {
            node.id == invocation && node.y == draft_y && node.labels == vec!["draft".to_owned()]
        }));
        assert!(result_node.labels.is_empty());
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == tool_use
                && edge.target_id == invocation
                && edge.target.y == skill_y
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_prunes_skill_lane_when_tool_use_source_is_rewound() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-1".to_owned(),
                    name: "skill".to_owned(),
                    input: json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &tool_use)
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
                        skill_name: "fast-rust".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let result = writer
            .append(NewNode {
                parent: invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "fast-rust".to_owned(),
                        output: "done".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        publisher.mark_changed();
        let initial = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let skill_lane = format!("skill {}", crate::graph::shorten_id(&result));
        assert!(initial.lanes.iter().any(|lane| lane.label == skill_lane));

        writer
            .set_branch_head("main", &tool_use, &session)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();
        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(!viewport.lanes.iter().any(|lane| lane.label == skill_lane));
        assert!(!viewport.nodes.iter().any(|node| node.id == invocation));
        assert!(!viewport.nodes.iter().any(|node| node.id == result));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_materializes_sibling_skill_invocation_subtrees() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-1".to_owned(),
                    name: "skill".to_owned(),
                    input: json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &tool_use)
            .await
            .unwrap();
        let first_invocation = writer
            .append(NewNode {
                parent: tool_use.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_invocation(
                    Vec::new(),
                    SkillInvocationAnchor {
                        skill_name: "fast-rust".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let first_result = writer
            .append(NewNode {
                parent: first_invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "fast-rust".to_owned(),
                        output: "done".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        let second_invocation = writer
            .append(NewNode {
                parent: tool_use.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_invocation(
                    Vec::new(),
                    SkillInvocationAnchor {
                        skill_name: "careful-rust".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let second_result = writer
            .append(NewNode {
                parent: second_invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "careful-rust".to_owned(),
                        output: "done".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let node_ids = viewport
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<BTreeSet<_>>();
        let first_skill_lane = format!("skill {}", crate::graph::shorten_id(&first_result));
        let second_skill_lane = format!("skill {}", crate::graph::shorten_id(&second_result));
        let main_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == "main")
            .unwrap()
            .y;

        assert_eq!(viewport.version, target_version);
        assert!(node_ids.contains(first_invocation.as_str()));
        assert!(node_ids.contains(first_result.as_str()));
        assert!(node_ids.contains(second_invocation.as_str()));
        assert!(node_ids.contains(second_result.as_str()));
        assert!(
            viewport
                .lanes
                .iter()
                .any(|lane| lane.label == first_skill_lane)
        );
        assert!(
            viewport
                .lanes
                .iter()
                .any(|lane| lane.label == second_skill_lane)
        );
        assert_ne!(
            viewport
                .nodes
                .iter()
                .find(|node| node.id == first_invocation)
                .unwrap()
                .y,
            main_y
        );
        assert_ne!(
            viewport
                .nodes
                .iter()
                .find(|node| node.id == second_invocation)
                .unwrap()
                .y,
            main_y
        );
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_materializes_branched_skill_invocation_subtree() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-1".to_owned(),
                    name: "skill".to_owned(),
                    input: json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &tool_use)
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
                        skill_name: "branched-rust".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let first_result = writer
            .append(NewNode {
                parent: invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "branched-rust".to_owned(),
                        output: "first".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        let second_result = writer
            .append(NewNode {
                parent: invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "branched-rust".to_owned(),
                        output: "second".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let first_skill_lane = format!("skill {}", crate::graph::shorten_id(&first_result));
        let invocation_count = viewport
            .nodes
            .iter()
            .filter(|node| node.id == invocation)
            .count();
        let skill_lane_count = viewport
            .lanes
            .iter()
            .filter(|lane| lane.label.starts_with("skill "))
            .count();

        assert_eq!(viewport.version, target_version);
        assert_eq!(invocation_count, 1);
        assert!(viewport.nodes.iter().any(|node| node.id == first_result));
        assert!(viewport.nodes.iter().any(|node| node.id == second_result));
        assert_eq!(skill_lane_count, 1);
        assert!(
            viewport
                .lanes
                .iter()
                .any(|lane| lane.label == first_skill_lane)
        );
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == invocation
                && edge.target_id == first_result
                && edge.kind == crate::api::GraphViewportEdgeKind::PrimaryParent
        }));
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == invocation
                && edge.target_id == second_result
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_prunes_branch_suffix_when_rewound_tool_use_keeps_skill_subtree() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-1".to_owned(),
                    name: "skill".to_owned(),
                    input: json!({}),
                }),
            })
            .await
            .unwrap();
        let child = writer
            .append(NewNode {
                parent: tool_use.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "next".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &child)
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
                        skill_name: "fast-rust".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let result = writer
            .append(NewNode {
                parent: invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "fast-rust".to_owned(),
                        output: "done".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        publisher.mark_changed();
        let initial = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        assert!(initial.nodes.iter().any(|node| node.id == child));
        assert!(initial.nodes.iter().any(|node| node.id == invocation));
        assert!(initial.nodes.iter().any(|node| node.id == result));

        writer
            .set_branch_head("main", &child, &tool_use)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();
        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(!viewport.nodes.iter().any(|node| node.id == child));
        assert!(viewport.nodes.iter().any(|node| node.id == invocation));
        assert!(viewport.nodes.iter().any(|node| node.id == result));
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == tool_use
                && edge.target_id == invocation
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_refreshes_tail_tool_use_skill_subtree_before_branch_append() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-1".to_owned(),
                    name: "skill".to_owned(),
                    input: json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &tool_use)
            .await
            .unwrap();
        publisher.mark_changed();
        let initial = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
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
                        skill_name: "fast-rust".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let next = writer
            .append(NewNode {
                parent: tool_use.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::Text("branch continued after tool use".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &tool_use, &next)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let skill_lane = format!("skill {}", crate::graph::shorten_id(&invocation));
        let skill_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == skill_lane)
            .unwrap()
            .y;

        assert_eq!(viewport.version, target_version);
        assert!(
            viewport
                .nodes
                .iter()
                .any(|node| node.id == invocation && node.y == skill_y)
        );
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == tool_use
                && edge.target_id == invocation
                && edge.target.y == skill_y
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_compacts_lanes_after_pruning_derived_skill_lane() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_tool_use = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-1".to_owned(),
                    name: "skill".to_owned(),
                    input: json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_tool_use)
            .await
            .unwrap();
        let main_invocation = writer
            .append(NewNode {
                parent: main_tool_use.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_invocation(
                    Vec::new(),
                    SkillInvocationAnchor {
                        skill_name: "fast-rust".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let main_result = writer
            .append(NewNode {
                parent: main_invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "fast-rust".to_owned(),
                        output: "done".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        let draft_tool_use = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-2".to_owned(),
                    name: "skill".to_owned(),
                    input: json!({}),
                }),
            })
            .await
            .unwrap();
        writer.fork("draft", &draft_tool_use).await.unwrap();
        let draft_invocation = writer
            .append(NewNode {
                parent: draft_tool_use.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_invocation(
                    Vec::new(),
                    SkillInvocationAnchor {
                        skill_name: "careful-rust".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let draft_result = writer
            .append(NewNode {
                parent: draft_invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "careful-rust".to_owned(),
                        output: "done".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        publisher.mark_changed();
        let initial = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let main_skill_lane = format!("skill {}", crate::graph::shorten_id(&main_result));
        let draft_skill_lane = format!("skill {}", crate::graph::shorten_id(&draft_result));
        let main_skill_y = initial
            .lanes
            .iter()
            .find(|lane| lane.label == main_skill_lane)
            .unwrap()
            .y;
        let draft_skill_y = initial
            .lanes
            .iter()
            .find(|lane| lane.label == draft_skill_lane)
            .unwrap()
            .y;
        assert!(main_skill_y < draft_skill_y);

        writer
            .set_branch_head("main", &main_tool_use, &session)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();
        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let compacted_draft_skill_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == draft_skill_lane)
            .unwrap()
            .y;

        assert_eq!(viewport.version, target_version);
        assert!(
            !viewport
                .lanes
                .iter()
                .any(|lane| lane.label == main_skill_lane)
        );
        assert_eq!(compacted_draft_skill_y, main_skill_y);
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_trims_skill_prefix_when_branch_adopts_result_middle() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-1".to_owned(),
                    name: "skill".to_owned(),
                    input: json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &tool_use)
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
                        skill_name: "fast-rust".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let result_first = writer
            .append(NewNode {
                parent: invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "fast-rust".to_owned(),
                        output: "first".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        let result_second = writer
            .append(NewNode {
                parent: result_first.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "fast-rust".to_owned(),
                        output: "second".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        publisher.mark_changed();
        let initial = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let skill_lane = format!("skill {}", crate::graph::shorten_id(&result_second));
        assert!(initial.lanes.iter().any(|lane| lane.label == skill_lane));

        writer.fork("draft", &result_first).await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let draft_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == "draft")
            .unwrap()
            .y;
        let skill_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == skill_lane)
            .unwrap()
            .y;

        assert_eq!(viewport.version, target_version);
        assert!(
            viewport
                .nodes
                .iter()
                .any(|node| node.id == invocation && node.y == draft_y)
        );
        assert!(
            viewport
                .nodes
                .iter()
                .any(|node| node.id == result_first && node.y == draft_y)
        );
        assert!(
            viewport
                .nodes
                .iter()
                .any(|node| node.id == result_second && node.y == skill_y)
        );
        assert!(
            !viewport
                .nodes
                .iter()
                .any(|node| node.id == invocation && node.y == skill_y)
        );
        assert!(
            !viewport
                .nodes
                .iter()
                .any(|node| node.id == result_first && node.y == skill_y)
        );
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == result_first
                && edge.source.y == draft_y
                && edge.target_id == result_second
                && edge.target.y == skill_y
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_reserves_new_branch_lane_when_seeding_merge_orphan() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        publisher.mark_changed();
        let initial = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let orphan_parent = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "orphan parent".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer.fork("draft", &session).await.unwrap();
        let draft_merge = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(orphan_parent.clone())],
                    PromptAnchor {
                        prompt: "draft merge".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &session, &draft_merge)
            .await
            .unwrap();
        publisher.mark_changed();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let orphan_lane = format!("orphan {}", crate::graph::shorten_id(&orphan_parent));
        let draft_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == "draft")
            .unwrap()
            .y;
        let orphan_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == orphan_lane)
            .unwrap()
            .y;

        assert_ne!(draft_y, orphan_y);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_prunes_anchor_orphan_merge_parent_after_rewind_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let orphan_parent = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "orphan parent".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        let merge_anchor = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(orphan_parent.clone())],
                    PromptAnchor {
                        prompt: "merge anchor".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &merge_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let initial = cache
            .viewport_after(
                GraphMode::Anchors,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        writer
            .set_branch_head("main", &merge_anchor, &session)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::Anchors,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(
            !viewport
                .lanes
                .iter()
                .any(|lane| lane.label.starts_with("orphan "))
        );
        assert!(
            cache
                .cached_snapshot(GraphMode::Anchors, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_refreshes_graph_locations_incrementally() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("visible child".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &text)
            .await
            .unwrap();
        publisher.mark_changed();

        cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        let next = writer
            .append(NewNode {
                parent: text.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("next visible child".to_owned()),
            })
            .await
            .unwrap();
        writer.set_branch_head("main", &text, &next).await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let mut materialized = None;
        for _ in 0..50 {
            materialized = ConsoleGraphSnapshotStore::open(&path)
                .await
                .unwrap()
                .latest_viewport(
                    GraphMode::All,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .unwrap();
            if materialized
                .as_ref()
                .is_some_and(|viewport| viewport.version == target_version)
            {
                break;
            }
            let _ = cache.viewport_current_ready_or_schedule(
                GraphMode::All,
                crate::host::api::GraphViewportRequest::default(),
            );
            sleep(Duration::from_millis(10)).await;
        }
        let materialized = materialized.expect("materialization should append incrementally");

        assert_eq!(materialized.version, target_version);
        assert!(materialized.nodes.iter().any(|node| node.id == next));

        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_appends_linear_graph_materialization_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("first child".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        let second = writer
            .append(NewNode {
                parent: first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("second child".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &first, &second)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(viewport.nodes.iter().any(|node| node.id == second));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);
        assert!(cache.rebuild_statuses().iter().any(|status| {
            status.mode == GraphMode::All
                && status.source_version == target_version
                && status.state == ConsoleGraphRebuildState::Ready
        }));

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_seeds_single_branch_materialization_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("initial materialized seed".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &text)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        assert!(
            cache
                .viewport_current_ready_or_schedule(
                    GraphMode::All,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .is_none()
        );
        let mut materialized = None;
        for _ in 0..50 {
            materialized = ConsoleGraphSnapshotStore::open(&path)
                .await
                .unwrap()
                .latest_viewport(
                    GraphMode::All,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .unwrap();
            if materialized
                .as_ref()
                .is_some_and(|viewport| viewport.version == target_version)
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let materialized = materialized.expect("initial materialization should be seeded");

        assert_eq!(materialized.version, target_version);
        assert!(materialized.nodes.iter().any(|node| node.id == session));
        assert!(materialized.nodes.iter().any(|node| node.id == text));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_replaces_materialization_from_full_snapshot_layout() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("full materialization fallback".to_owned()),
            })
            .await
            .unwrap();
        let orphan = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("full materialization orphan".to_owned()),
            })
            .await
            .unwrap();
        let orphan_lane = format!("orphan {}", crate::graph::shorten_id(&orphan));
        writer.fork(&orphan_lane, &root).await.unwrap();
        let merge_anchor = writer
            .append(NewNode {
                parent: text.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(orphan.clone())],
                    PromptAnchor {
                        prompt: "merge full materialization orphan".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &merge_anchor)
            .await
            .unwrap();
        writer.fork("orphan branch", &session).await.unwrap();
        let reserved_label_branch_head = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("reserved label branch".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("orphan branch", &session, &reserved_label_branch_head)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let snapshot = cache.snapshot_current(GraphMode::All).await.unwrap();
        let branch_labels = visible_full_layout_branch_labels(&snapshot);
        let viewport = materialize_graph_viewport(&snapshot);
        ConsoleGraphSnapshotStore::open(&path)
            .await
            .unwrap()
            .replace_materialization_from_viewport(GraphMode::All, viewport, branch_labels)
            .unwrap();
        let materialized = ConsoleGraphSnapshotStore::open(&path)
            .await
            .unwrap()
            .latest_viewport(
                GraphMode::All,
                crate::host::api::GraphViewportRequest::default(),
            )
            .unwrap()
            .unwrap();

        assert_eq!(materialized.version, target_version);
        assert!(materialized.nodes.iter().any(|node| node.id == session));
        assert!(materialized.nodes.iter().any(|node| node.id == text));
        assert!(
            materialized
                .nodes
                .iter()
                .any(|node| node.id == merge_anchor)
        );
        assert!(
            materialized
                .nodes
                .iter()
                .any(|node| node.id == reserved_label_branch_head)
        );
        assert!(materialized.nodes.iter().any(|node| node.id == orphan));
        assert!(materialized.edges.iter().any(|edge| edge.target_id == text));
        assert!(materialized.lanes.iter().any(|lane| {
            lane.key == lane_key("orphan branch") && lane.label == "orphan branch"
        }));
        assert!(materialized.lanes.iter().any(|lane| {
            lane.key == format!("derived:orphan:{orphan}") && lane.label == orphan_lane
        }));

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_seeds_multiple_branch_materialization_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let shared = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("initial shared node".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &shared)
            .await
            .unwrap();
        writer.fork("feature", &shared).await.unwrap();
        let main_head = writer
            .append(NewNode {
                parent: shared.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("initial main head".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &shared, &main_head)
            .await
            .unwrap();
        let feature_head = writer
            .append(NewNode {
                parent: shared.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("initial feature head".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("feature", &shared, &feature_head)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        assert!(
            cache
                .viewport_current_ready_or_schedule(
                    GraphMode::All,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .is_none()
        );
        let mut materialized = None;
        for _ in 0..50 {
            materialized = ConsoleGraphSnapshotStore::open(&path)
                .await
                .unwrap()
                .latest_viewport(
                    GraphMode::All,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .unwrap();
            if materialized
                .as_ref()
                .is_some_and(|viewport| viewport.version == target_version)
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let materialized = materialized.expect("initial materialization should be seeded");

        assert_eq!(materialized.version, target_version);
        assert!(materialized.nodes.iter().any(|node| node.id == main_head));
        assert!(
            materialized
                .nodes
                .iter()
                .any(|node| node.id == feature_head)
        );
        assert!(materialized.lanes.iter().any(|lane| lane.label == "main"));
        assert!(
            materialized
                .lanes
                .iter()
                .any(|lane| lane.label == "feature")
        );
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_seeds_initial_materialization_rebalances_derived_route_slots() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let shared = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("shared".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &shared)
            .await
            .unwrap();
        let orphan_parent = writer
            .append(NewNode {
                parent: shared.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan parent".to_owned()),
            })
            .await
            .unwrap();
        let merge_anchor = writer
            .append(NewNode {
                parent: shared.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(orphan_parent.clone())],
                    PromptAnchor {
                        prompt: "merge orphan".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &shared, &merge_anchor)
            .await
            .unwrap();
        writer.fork("beta", &shared).await.unwrap();
        let beta_first = writer
            .append(NewNode {
                parent: shared.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("beta first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("beta", &shared, &beta_first)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        assert!(
            cache
                .viewport_current_ready_or_schedule(
                    GraphMode::All,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .is_none()
        );
        let mut materialized = None;
        for _ in 0..50 {
            materialized = ConsoleGraphSnapshotStore::open(&path)
                .await
                .unwrap()
                .latest_viewport(
                    GraphMode::All,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .unwrap();
            if materialized
                .as_ref()
                .is_some_and(|viewport| viewport.version == target_version)
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let materialized = materialized.expect("initial materialization should be seeded");
        let beta_edge = materialized
            .edges
            .iter()
            .find(|edge| {
                edge.source_id == shared
                    && edge.target_id == beta_first
                    && edge.kind == crate::api::GraphViewportEdgeKind::Fork
            })
            .unwrap();
        let orphan_edge = materialized
            .edges
            .iter()
            .find(|edge| {
                edge.source_id == shared
                    && edge.target_id == orphan_parent
                    && edge.kind == crate::api::GraphViewportEdgeKind::Fork
            })
            .unwrap();

        assert_eq!(materialized.version, target_version);
        assert_eq!(beta_edge.route_slot, 0);
        assert_eq!(orphan_edge.route_slot, 1);
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_seeds_anchor_materialization_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let hidden = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("hidden anchor seed context".to_owned()),
            })
            .await
            .unwrap();
        let prompt = writer
            .append(NewNode {
                parent: hidden,
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "initial anchor prompt".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &prompt)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        assert!(
            cache
                .viewport_current_ready_or_schedule(
                    GraphMode::Anchors,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .is_none()
        );
        let mut materialized = None;
        for _ in 0..50 {
            materialized = ConsoleGraphSnapshotStore::open(&path)
                .await
                .unwrap()
                .latest_viewport(
                    GraphMode::Anchors,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .unwrap();
            if materialized
                .as_ref()
                .is_some_and(|viewport| viewport.version == target_version)
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let materialized = materialized.expect("anchor materialization should be seeded");

        assert_eq!(materialized.version, target_version);
        assert!(materialized.nodes.iter().any(|node| node.id == session));
        assert!(materialized.nodes.iter().any(|node| node.id == prompt));
        assert_eq!(materialized.nodes.len(), 2);
        assert!(
            cache
                .cached_snapshot(GraphMode::Anchors, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_seeds_anchor_alias_labels_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        writer.fork("draft", &session).await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::Anchors,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let aliased_nodes = viewport
            .nodes
            .iter()
            .filter(|node| {
                node.id == session && node.labels == vec!["draft".to_owned(), "main".to_owned()]
            })
            .count();

        assert_eq!(viewport.version, target_version);
        assert_eq!(aliased_nodes, 2);
        assert!(
            cache
                .cached_snapshot(GraphMode::Anchors, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_preserves_branches_named_like_derived_lanes() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("skill research", &session).await.unwrap();
        writer.fork("orphan fix", &session).await.unwrap();
        publisher.mark_changed();

        let initial = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        assert!(
            initial
                .lanes
                .iter()
                .any(|lane| lane.label == "skill research")
        );
        assert!(initial.lanes.iter().any(|lane| lane.label == "orphan fix"));

        let next = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("next".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("skill research", &session, &next)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();
        let refreshed = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(refreshed.version, target_version);
        assert!(
            refreshed
                .lanes
                .iter()
                .any(|lane| lane.label == "skill research")
        );
        assert!(
            refreshed
                .lanes
                .iter()
                .any(|lane| lane.label == "orphan fix")
        );
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_appends_branch_named_like_exact_orphan_lane_label() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let orphan_tail = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan tail".to_owned()),
            })
            .await
            .unwrap();
        let merge_anchor = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(orphan_tail.clone())],
                    PromptAnchor {
                        prompt: "merge orphan tail".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_first, &merge_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let merged = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let orphan_lane = format!("orphan {}", crate::graph::shorten_id(&orphan_tail));
        assert!(merged.lanes.iter().any(|lane| lane.label == orphan_lane));

        writer.fork(&orphan_lane, &session).await.unwrap();
        publisher.mark_changed();
        let branch_created_version = publisher.current_version();
        let branch_created = cache
            .viewport_after(
                GraphMode::All,
                merged.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        assert_eq!(branch_created.version, branch_created_version);

        let branch_next = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("branch next".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head(&orphan_lane, &session, &branch_next)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();
        let refreshed = cache
            .viewport_after(
                GraphMode::All,
                branch_created.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(refreshed.version, target_version);
        assert!(
            refreshed
                .nodes
                .iter()
                .any(|node| { node.id == branch_next && node.labels == vec![orphan_lane.clone()] })
        );
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(cache);
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_seeds_empty_materialization_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        assert!(
            cache
                .viewport_current_ready_or_schedule(
                    GraphMode::All,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .is_none()
        );
        let mut materialized = None;
        for _ in 0..50 {
            materialized = ConsoleGraphSnapshotStore::open(&path)
                .await
                .unwrap()
                .latest_viewport(
                    GraphMode::All,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .unwrap();
            if materialized
                .as_ref()
                .is_some_and(|viewport| viewport.version == target_version)
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let materialized = materialized.expect("empty materialization should be seeded");

        assert_eq!(materialized.version, target_version);
        assert!(materialized.nodes.is_empty());
        assert!(materialized.edges.is_empty());
        assert!(materialized.lanes.is_empty());
        assert!(
            !ConsoleGraphSnapshotStore::open(&path)
                .await
                .unwrap()
                .has_non_empty_materialization(GraphMode::All)
                .unwrap()
        );
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("first non-empty node".to_owned()),
            })
            .await
            .unwrap();
        writer.set_branch_head("main", &root, &text).await.unwrap();
        publisher.mark_changed();
        let non_empty_version = publisher.current_version();
        let non_empty = cache
            .viewport_after(
                GraphMode::All,
                target_version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(non_empty.version, non_empty_version);
        assert!(non_empty.nodes.iter().any(|node| node.id == session));
        assert!(non_empty.nodes.iter().any(|node| node.id == text));
        assert!(non_empty.lanes.iter().any(|lane| lane.label == "main"));
        assert!(
            ConsoleGraphSnapshotStore::open(&path)
                .await
                .unwrap()
                .has_non_empty_materialization(GraphMode::All)
                .unwrap()
        );

        writer.delete_branch("main").await.unwrap();
        publisher.mark_changed();
        let empty_again_version = publisher.current_version();
        let empty_again = cache
            .viewport_after(
                GraphMode::All,
                non_empty_version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(empty_again.version, empty_again_version);
        assert!(empty_again.nodes.is_empty());
        assert!(empty_again.edges.is_empty());
        assert!(empty_again.lanes.is_empty());

        drop(cache);
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_skips_root_only_leading_branch_when_seeding_materialization() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("draft history".to_owned()),
            })
            .await
            .unwrap();
        writer.fork("draft", &text).await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(viewport.nodes.iter().any(|node| node.id == session));
        assert!(viewport.nodes.iter().any(|node| node.id == text));
        assert!(viewport.lanes.iter().any(|lane| lane.label == "draft"));
        assert!(!viewport.lanes.iter().any(|lane| lane.label == "main"));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(cache);
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_skips_root_only_trailing_branch_when_seeding_materialization() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main history".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &text)
            .await
            .unwrap();
        writer.fork("empty", &root).await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(viewport.nodes.iter().any(|node| node.id == session));
        assert!(viewport.nodes.iter().any(|node| node.id == text));
        assert!(viewport.lanes.iter().any(|lane| lane.label == "main"));
        assert!(!viewport.lanes.iter().any(|lane| lane.label == "empty"));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(cache);
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_skips_root_only_new_branch_when_appending_materialization() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main history".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &text)
            .await
            .unwrap();
        publisher.mark_changed();
        let initial = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        writer.fork("empty", &root).await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();
        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(viewport.nodes.iter().any(|node| node.id == session));
        assert!(viewport.nodes.iter().any(|node| node.id == text));
        assert!(viewport.lanes.iter().any(|lane| lane.label == "main"));
        assert!(!viewport.lanes.iter().any(|lane| lane.label == "empty"));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(cache);
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_deletes_branch_lane_rewound_to_root_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main history".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &text)
            .await
            .unwrap();
        publisher.mark_changed();
        let initial = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        writer.set_branch_head("main", &text, &root).await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();
        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(viewport.nodes.is_empty());
        assert!(viewport.edges.is_empty());
        assert!(viewport.lanes.is_empty());
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(cache);
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn pending_viewport_after_refreshes_materialization_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let cache_for_wait = cache.clone();
        let waiter = tokio::spawn(async move {
            cache_for_wait
                .viewport_after(
                    GraphMode::All,
                    initial.version,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .await
                .unwrap()
        });
        sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("pending viewport update".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &text)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = waiter.await.unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(viewport.nodes.iter().any(|node| node.id == text));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn pending_viewport_diff_after_refreshes_materialization_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let cache_for_wait = cache.clone();
        let waiter = tokio::spawn(async move {
            cache_for_wait
                .viewport_diff_after(
                    GraphMode::All,
                    initial.version,
                    crate::host::api::GraphViewportDiffRequest {
                        previous: crate::host::api::GraphViewportRequest::default(),
                        current: crate::host::api::GraphViewportRequest::default(),
                        known: Some(crate::host::api::GraphViewportKnownItems::default()),
                    },
                )
                .await
                .unwrap()
        });
        sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("pending viewport diff update".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &text)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let diff = waiter.await.unwrap();

        assert_eq!(diff.version, target_version);
        assert!(diff.added.nodes.iter().any(|node| node.id == text));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_does_not_full_rebuild_when_incremental_materialization_cannot_apply() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("first materialized head".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let sibling = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("sibling head cannot append incrementally".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &first, &sibling)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let stale = cache
            .viewport_current_ready_or_schedule(
                GraphMode::All,
                crate::host::api::GraphViewportRequest::default(),
            )
            .unwrap();
        for _ in 0..50 {
            if cache.rebuild_statuses().iter().any(|status| {
                status.mode == GraphMode::All
                    && status.source_version == target_version
                    && status.state == ConsoleGraphRebuildState::Failed
            }) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let materialized = ConsoleGraphSnapshotStore::open(&path)
            .await
            .unwrap()
            .latest_viewport(
                GraphMode::All,
                crate::host::api::GraphViewportRequest::default(),
            )
            .unwrap()
            .unwrap();

        assert_eq!(stale.version, initial.version);
        assert_eq!(materialized.version, initial.version);
        assert!(!materialized.nodes.iter().any(|node| node.id == sibling));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert!(cache.rebuild_statuses().iter().any(|status| {
            status.mode == GraphMode::All
                && status.source_version == target_version
                && status.state == ConsoleGraphRebuildState::Failed
        }));
        let viewport_error = tokio::time::timeout(
            Duration::from_millis(200),
            cache.viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            ),
        )
        .await
        .expect("failed materialization viewport wait should not hang")
        .unwrap_err();
        assert!(matches!(
            viewport_error,
            crate::error::Error::ConsoleGraphRebuild {
                source_version,
                message,
                ..
            } if source_version == target_version && message.contains("could not apply")
        ));
        let diff_error = tokio::time::timeout(
            Duration::from_millis(200),
            cache.viewport_diff_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportDiffRequest {
                    previous: crate::host::api::GraphViewportRequest::default(),
                    current: crate::host::api::GraphViewportRequest::default(),
                    known: None,
                },
            ),
        )
        .await
        .expect("failed materialization viewport diff wait should not hang")
        .unwrap_err();
        assert!(matches!(
            diff_error,
            crate::error::Error::ConsoleGraphRebuild {
                source_version,
                message,
                ..
            } if source_version == target_version && message.contains("could not apply")
        ));
        let equal_observed_diff_error = tokio::time::timeout(
            Duration::from_millis(200),
            cache.viewport_diff_after(
                GraphMode::All,
                target_version,
                crate::host::api::GraphViewportDiffRequest {
                    previous: crate::host::api::GraphViewportRequest::default(),
                    current: crate::host::api::GraphViewportRequest::default(),
                    known: None,
                },
            ),
        )
        .await
        .expect("failed materialization viewport diff at observed version should not hang")
        .unwrap_err();
        assert!(matches!(
            equal_observed_diff_error,
            crate::error::Error::ConsoleGraphRebuild {
                source_version,
                message,
                ..
            } if source_version == target_version && message.contains("could not apply")
        ));

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_appends_linear_graph_merge_route_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_anchor = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "main anchor".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        let main_hidden = writer
            .append(NewNode {
                parent: main_anchor.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main hidden".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_hidden)
            .await
            .unwrap();
        writer.fork("draft", &session).await.unwrap();
        let draft_hidden = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("draft hidden".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &session, &draft_hidden)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        let merge_anchor = writer
            .append(NewNode {
                parent: main_hidden.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(draft_hidden.clone())],
                    PromptAnchor {
                        prompt: "merge anchor".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_hidden, &merge_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        assert_eq!(viewport.version, target_version);
        assert!(
            viewport
                .nodes
                .iter()
                .any(|node| node.id == merge_anchor && node.labels == vec!["main".to_owned()])
        );
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == main_hidden
                && edge.target_id == merge_anchor
                && edge.kind == crate::api::GraphViewportEdgeKind::PrimaryParent
        }));
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == draft_hidden
                && edge.target_id == merge_anchor
                && edge.kind == crate::api::GraphViewportEdgeKind::MergeParent
                && edge.target_port_offset == crate::layout::EDGE_TARGET_PORT_STEP
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_appends_merge_after_farther_lane_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        writer.fork("draft", &session).await.unwrap();
        let draft_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("draft first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &session, &draft_first)
            .await
            .unwrap();
        let draft_second = writer
            .append(NewNode {
                parent: draft_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("draft second".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &draft_first, &draft_second)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        let merge_anchor = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(draft_second.clone())],
                    PromptAnchor {
                        prompt: "merge farther lane".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_first, &merge_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let full_snapshot =
            crate::graph::build_graph_snapshot_with_mode(&writer, target_version, GraphMode::All)
                .await
                .unwrap();
        let full_viewport = crate::layout::materialize_graph_viewport(&full_snapshot);
        let incremental_merge = viewport
            .nodes
            .iter()
            .find(|node| node.id == merge_anchor)
            .unwrap();
        let full_merge = full_viewport
            .nodes
            .iter()
            .find(|node| node.id == merge_anchor)
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert_eq!(incremental_merge.x, full_merge.x);
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == draft_second
                && edge.target_id == merge_anchor
                && edge.kind == crate::api::GraphViewportEdgeKind::MergeParent
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_seeds_orphan_merge_parent_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        let orphan_parent = writer
            .append(NewNode {
                parent: root,
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan feedback".to_owned()),
            })
            .await
            .unwrap();
        let merge_anchor = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(orphan_parent.clone())],
                    PromptAnchor {
                        prompt: "merge orphan".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_first, &merge_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let orphan_lane = format!("orphan {}", crate::graph::shorten_id(&orphan_parent));

        assert_eq!(viewport.version, target_version);
        assert!(viewport.lanes.iter().any(|lane| lane.label == orphan_lane));
        assert!(viewport.nodes.iter().any(|node| node.id == orphan_parent));
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == orphan_parent
                && edge.target_id == merge_anchor
                && edge.kind == crate::api::GraphViewportEdgeKind::MergeParent
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_seeds_orphan_tool_use_skill_subtree_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        publisher.mark_changed();
        let initial = cache.current_snapshot(GraphMode::All).await;

        let orphan_tool_use = writer
            .append(NewNode {
                parent: root,
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "tool-1".to_owned(),
                    name: "skill".to_owned(),
                    input: json!({}),
                }),
            })
            .await
            .unwrap();
        let invocation = writer
            .append(NewNode {
                parent: orphan_tool_use.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_invocation(
                    Vec::new(),
                    SkillInvocationAnchor {
                        skill_name: "fast-rust".to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let result = writer
            .append(NewNode {
                parent: invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    Vec::new(),
                    SkillResultAnchor {
                        skill_name: "fast-rust".to_owned(),
                        output: "done".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        let merge_anchor = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(orphan_tool_use.clone())],
                    PromptAnchor {
                        prompt: "merge orphan tool use".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_first, &merge_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let orphan_lane = format!("orphan {}", crate::graph::shorten_id(&orphan_tool_use));
        let orphan_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == orphan_lane)
            .unwrap()
            .y;
        let skill_lane = format!("skill {}", crate::graph::shorten_id(&result));
        let skill_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == skill_lane)
            .unwrap()
            .y;

        assert_eq!(viewport.version, target_version);
        assert_ne!(skill_y, orphan_y);
        assert!(viewport.nodes.iter().any(|node| {
            node.id == orphan_tool_use && node.y == orphan_y && node.labels.is_empty()
        }));
        assert!(
            viewport.nodes.iter().any(|node| {
                node.id == invocation && node.y == skill_y && node.labels.is_empty()
            })
        );
        assert!(
            viewport
                .nodes
                .iter()
                .any(|node| { node.id == result && node.y == skill_y && node.labels.is_empty() })
        );
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == orphan_tool_use
                && edge.target_id == invocation
                && edge.target.y == skill_y
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_drops_orphan_lane_when_branch_adopts_merge_parent() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let orphan_parent = writer
            .append(NewNode {
                parent: root,
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan feedback".to_owned()),
            })
            .await
            .unwrap();
        let merge_anchor = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(orphan_parent.clone())],
                    PromptAnchor {
                        prompt: "merge orphan".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_first, &merge_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let merge_version = publisher.current_version();

        let merged = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let orphan_lane = format!("orphan {}", crate::graph::shorten_id(&orphan_parent));
        assert_eq!(merged.version, merge_version);
        assert!(merged.lanes.iter().any(|lane| lane.label == orphan_lane));

        writer.fork("draft", &orphan_parent).await.unwrap();
        publisher.mark_changed();
        let adopt_version = publisher.current_version();
        let adopted = cache
            .viewport_after(
                GraphMode::All,
                merge_version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let draft_y = adopted
            .lanes
            .iter()
            .find(|lane| lane.label == "draft")
            .unwrap()
            .y;
        let adopted_node = adopted
            .nodes
            .iter()
            .find(|node| node.id == orphan_parent)
            .unwrap();

        assert_eq!(adopted.version, adopt_version);
        assert!(!adopted.lanes.iter().any(|lane| lane.label == orphan_lane));
        assert_eq!(adopted_node.y, draft_y);
        assert!(adopted.edges.iter().any(|edge| {
            edge.source_id == orphan_parent
                && edge.source.y == draft_y
                && edge.target_id == merge_anchor
                && edge.kind == crate::api::GraphViewportEdgeKind::MergeParent
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, adopt_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_preserves_orphan_ancestry_when_branch_adopts_chain_tail() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let shared = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("shared".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &shared)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let orphan_first = writer
            .append(NewNode {
                parent: shared.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan first".to_owned()),
            })
            .await
            .unwrap();
        let orphan_tail = writer
            .append(NewNode {
                parent: orphan_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan tail".to_owned()),
            })
            .await
            .unwrap();
        let merge_anchor = writer
            .append(NewNode {
                parent: shared.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(orphan_tail.clone())],
                    PromptAnchor {
                        prompt: "merge orphan tail".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &shared, &merge_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let merge_version = publisher.current_version();

        let merged = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let orphan_lane = format!("orphan {}", crate::graph::shorten_id(&orphan_tail));
        assert_eq!(merged.version, merge_version);
        assert!(merged.lanes.iter().any(|lane| lane.label == orphan_lane));

        writer.fork("draft", &orphan_tail).await.unwrap();
        publisher.mark_changed();
        let adopt_version = publisher.current_version();
        let adopted = cache
            .viewport_after(
                GraphMode::All,
                merge_version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let draft_y = adopted
            .lanes
            .iter()
            .find(|lane| lane.label == "draft")
            .unwrap()
            .y;

        assert_eq!(adopted.version, adopt_version);
        assert!(!adopted.lanes.iter().any(|lane| lane.label == orphan_lane));
        assert!(
            adopted
                .nodes
                .iter()
                .any(|node| node.id == orphan_first && node.y == draft_y)
        );
        assert!(
            adopted
                .nodes
                .iter()
                .any(|node| node.id == orphan_tail && node.y == draft_y)
        );
        assert!(adopted.edges.iter().any(|edge| {
            edge.source_id == shared
                && edge.target_id == orphan_first
                && edge.target.y == draft_y
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
        }));
        assert!(adopted.edges.iter().any(|edge| {
            edge.source_id == orphan_first
                && edge.target_id == orphan_tail
                && edge.source.y == draft_y
                && edge.target.y == draft_y
                && edge.kind == crate::api::GraphViewportEdgeKind::PrimaryParent
        }));
        assert!(adopted.edges.iter().any(|edge| {
            edge.source_id == orphan_tail
                && edge.source.y == draft_y
                && edge.target_id == merge_anchor
                && edge.kind == crate::api::GraphViewportEdgeKind::MergeParent
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, adopt_version)
                .is_none()
        );

        drop(cache);
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_trims_orphan_prefix_when_branch_adopts_chain_middle() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let orphan_first = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan first".to_owned()),
            })
            .await
            .unwrap();
        let orphan_middle = writer
            .append(NewNode {
                parent: orphan_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan middle".to_owned()),
            })
            .await
            .unwrap();
        let orphan_tail = writer
            .append(NewNode {
                parent: orphan_middle.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan tail".to_owned()),
            })
            .await
            .unwrap();
        let merge_anchor = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(orphan_tail.clone())],
                    PromptAnchor {
                        prompt: "merge orphan tail".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_first, &merge_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let merge_version = publisher.current_version();

        let merged = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let orphan_lane = format!("orphan {}", crate::graph::shorten_id(&orphan_tail));
        assert_eq!(merged.version, merge_version);
        assert!(merged.lanes.iter().any(|lane| lane.label == orphan_lane));

        writer.fork("draft", &orphan_middle).await.unwrap();
        publisher.mark_changed();
        let adopt_version = publisher.current_version();
        let adopted = cache
            .viewport_after(
                GraphMode::All,
                merge_version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let draft_y = adopted
            .lanes
            .iter()
            .find(|lane| lane.label == "draft")
            .unwrap()
            .y;
        let orphan_y = adopted
            .lanes
            .iter()
            .find(|lane| lane.label == orphan_lane)
            .unwrap()
            .y;

        assert_eq!(adopted.version, adopt_version);
        assert!(
            adopted
                .nodes
                .iter()
                .any(|node| node.id == orphan_first && node.y == draft_y)
        );
        assert!(
            adopted
                .nodes
                .iter()
                .any(|node| node.id == orphan_middle && node.y == draft_y)
        );
        assert!(
            adopted
                .nodes
                .iter()
                .any(|node| node.id == orphan_tail && node.y == orphan_y)
        );
        assert!(
            !adopted
                .nodes
                .iter()
                .any(|node| node.id == orphan_first && node.y == orphan_y)
        );
        assert!(
            !adopted
                .nodes
                .iter()
                .any(|node| node.id == orphan_middle && node.y == orphan_y)
        );
        assert!(adopted.edges.iter().any(|edge| {
            edge.source_id == orphan_middle
                && edge.source.y == draft_y
                && edge.target_id == orphan_tail
                && edge.target.y == orphan_y
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
        }));
        assert!(adopted.edges.iter().any(|edge| {
            edge.source_id == orphan_tail
                && edge.source.y == orphan_y
                && edge.target_id == merge_anchor
                && edge.kind == crate::api::GraphViewportEdgeKind::MergeParent
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, adopt_version)
                .is_none()
        );

        drop(cache);
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_adopts_root_started_orphan_chain_tail_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let orphan_first = writer
            .append(NewNode {
                parent: root,
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan first".to_owned()),
            })
            .await
            .unwrap();
        let orphan_tail = writer
            .append(NewNode {
                parent: orphan_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan tail".to_owned()),
            })
            .await
            .unwrap();
        let merge_anchor = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(orphan_tail.clone())],
                    PromptAnchor {
                        prompt: "merge root orphan tail".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_first, &merge_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let merge_version = publisher.current_version();

        let merged = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let orphan_lane = format!("orphan {}", crate::graph::shorten_id(&orphan_tail));
        assert_eq!(merged.version, merge_version);
        assert!(merged.lanes.iter().any(|lane| lane.label == orphan_lane));

        writer.fork("draft", &orphan_tail).await.unwrap();
        publisher.mark_changed();
        let adopt_version = publisher.current_version();
        let adopted = cache
            .viewport_after(
                GraphMode::All,
                merge_version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let draft_y = adopted
            .lanes
            .iter()
            .find(|lane| lane.label == "draft")
            .unwrap()
            .y;

        assert_eq!(adopted.version, adopt_version);
        assert!(!adopted.lanes.iter().any(|lane| lane.label == orphan_lane));
        assert!(
            adopted
                .nodes
                .iter()
                .any(|node| node.id == orphan_first && node.y == draft_y)
        );
        assert!(
            adopted
                .nodes
                .iter()
                .any(|node| node.id == orphan_tail && node.y == draft_y)
        );
        assert!(adopted.edges.iter().any(|edge| {
            edge.source_id == orphan_first
                && edge.source.y == draft_y
                && edge.target_id == orphan_tail
                && edge.target.y == draft_y
                && edge.kind == crate::api::GraphViewportEdgeKind::PrimaryParent
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, adopt_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_prunes_orphan_merge_parent_after_rewind_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let orphan_parent = writer
            .append(NewNode {
                parent: root,
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan feedback".to_owned()),
            })
            .await
            .unwrap();
        let merge_anchor = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(orphan_parent.clone())],
                    PromptAnchor {
                        prompt: "merge orphan".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_first, &merge_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let merge_version = publisher.current_version();

        let merged = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let orphan_lane = format!("orphan {}", crate::graph::shorten_id(&orphan_parent));

        assert_eq!(merged.version, merge_version);
        assert!(merged.lanes.iter().any(|lane| lane.label == orphan_lane));
        assert!(merged.nodes.iter().any(|node| node.id == orphan_parent));

        writer
            .set_branch_head("main", &merge_anchor, &main_first)
            .await
            .unwrap();
        publisher.mark_changed();
        let rewind_version = publisher.current_version();

        let rewound = cache
            .viewport_after(
                GraphMode::All,
                merge_version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(rewound.version, rewind_version);
        assert!(!rewound.lanes.iter().any(|lane| lane.label == orphan_lane));
        assert!(!rewound.nodes.iter().any(|node| node.id == orphan_parent));
        assert!(!rewound.edges.iter().any(|edge| {
            edge.source_id == orphan_parent
                && edge.target_id == merge_anchor
                && edge.kind == crate::api::GraphViewportEdgeKind::MergeParent
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, rewind_version)
                .is_none()
        );

        drop(cache);
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_seeds_initial_merge_after_orphan_parent_column_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        let orphan_first = writer
            .append(NewNode {
                parent: root,
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan first".to_owned()),
            })
            .await
            .unwrap();
        let orphan_second = writer
            .append(NewNode {
                parent: orphan_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan second".to_owned()),
            })
            .await
            .unwrap();
        let orphan_third = writer
            .append(NewNode {
                parent: orphan_second.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan third".to_owned()),
            })
            .await
            .unwrap();
        let orphan_parent = writer
            .append(NewNode {
                parent: orphan_third.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan parent".to_owned()),
            })
            .await
            .unwrap();
        let merge_anchor = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(orphan_parent.clone())],
                    PromptAnchor {
                        prompt: "seed merge orphan".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &merge_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                0,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let full_snapshot =
            crate::graph::build_graph_snapshot_with_mode(&writer, target_version, GraphMode::All)
                .await
                .unwrap();
        let full_viewport = crate::layout::materialize_graph_viewport(&full_snapshot);
        let incremental_merge = viewport
            .nodes
            .iter()
            .find(|node| node.id == merge_anchor)
            .unwrap();
        let full_merge = full_viewport
            .nodes
            .iter()
            .find(|node| node.id == merge_anchor)
            .unwrap();
        let orphan_lane = format!("orphan {}", crate::graph::shorten_id(&orphan_parent));
        let main_lane_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == "main")
            .unwrap()
            .y;
        let orphan_lane_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == orphan_lane)
            .unwrap()
            .y;

        assert_eq!(viewport.version, target_version);
        assert_eq!(incremental_merge.x, full_merge.x);
        assert_ne!(main_lane_y, orphan_lane_y);
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == orphan_parent
                && edge.target_id == merge_anchor
                && edge.kind == crate::api::GraphViewportEdgeKind::MergeParent
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_applies_merge_constraints_to_orphan_lane_nodes() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        let main_second = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main second".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_second)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let orphan_base = writer
            .append(NewNode {
                parent: root,
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan base".to_owned()),
            })
            .await
            .unwrap();
        let orphan_parent = writer
            .append(NewNode {
                parent: orphan_base,
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(main_second.clone())],
                    PromptAnchor {
                        prompt: "orphan constrained".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        let merge_anchor = writer
            .append(NewNode {
                parent: main_second.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(orphan_parent.clone())],
                    PromptAnchor {
                        prompt: "merge constrained orphan".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_second, &merge_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let full_snapshot =
            crate::graph::build_graph_snapshot_with_mode(&writer, target_version, GraphMode::All)
                .await
                .unwrap();
        let full_viewport = crate::layout::materialize_graph_viewport(&full_snapshot);
        let incremental_orphan = viewport
            .nodes
            .iter()
            .find(|node| node.id == orphan_parent)
            .unwrap();
        let full_orphan = full_viewport
            .nodes
            .iter()
            .find(|node| node.id == orphan_parent)
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert_eq!(incremental_orphan.x, full_orphan.x);
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == main_second
                && edge.target_id == orphan_parent
                && edge.kind == crate::api::GraphViewportEdgeKind::MergeParent
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_updates_anchor_materialization_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("first child".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::Anchors).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        let second = writer
            .append(NewNode {
                parent: first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("second child".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &first, &second)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::Anchors,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let session_node = viewport
            .nodes
            .iter()
            .find(|node| node.id == session)
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert_eq!(session_node.labels, vec!["main".to_owned()]);
        assert!(!viewport.nodes.iter().any(|node| node.id == first));
        assert!(!viewport.nodes.iter().any(|node| node.id == second));
        assert!(
            cache
                .cached_snapshot(GraphMode::Anchors, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_appends_anchor_materialization_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::Anchors).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        let prompt = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "next prompt".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &prompt)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::Anchors,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let session_node = viewport
            .nodes
            .iter()
            .find(|node| node.id == session)
            .unwrap();
        let prompt_node = viewport
            .nodes
            .iter()
            .find(|node| node.id == prompt)
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(session_node.labels.is_empty());
        assert_eq!(prompt_node.labels, vec!["main".to_owned()]);
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == session
                && edge.target_id == prompt
                && edge.kind == crate::api::GraphViewportEdgeKind::PrimaryParent
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::Anchors, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_rewinds_anchor_materialization_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let first_prompt = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "first prompt".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &first_prompt)
            .await
            .unwrap();
        let second_prompt = writer
            .append(NewNode {
                parent: first_prompt.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "second prompt".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &first_prompt, &second_prompt)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::Anchors).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer
            .set_branch_head("main", &second_prompt, &first_prompt)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::Anchors,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let first_node = viewport
            .nodes
            .iter()
            .find(|node| node.id == first_prompt)
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert_eq!(first_node.labels, vec!["main".to_owned()]);
        assert!(!viewport.nodes.iter().any(|node| node.id == second_prompt));
        assert!(
            cache
                .cached_snapshot(GraphMode::Anchors, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 1);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_deletes_anchor_branch_lane_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_anchor = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "main anchor".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_anchor)
            .await
            .unwrap();
        writer.fork("draft", &session).await.unwrap();
        let draft_anchor = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "draft anchor".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &session, &draft_anchor)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::Anchors).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer.delete_branch("draft").await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::Anchors,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(!viewport.nodes.iter().any(|node| node.id == draft_anchor));
        assert!(
            viewport
                .nodes
                .iter()
                .any(|node| node.id == main_anchor && node.labels == vec!["main".to_owned()])
        );
        assert!(
            cache
                .cached_snapshot(GraphMode::Anchors, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 1);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_appends_new_anchor_branch_lane_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_anchor = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "main anchor".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_anchor)
            .await
            .unwrap();
        let main_followup = writer
            .append(NewNode {
                parent: main_anchor.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "main followup".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_anchor, &main_followup)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::Anchors).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer.fork("draft", &session).await.unwrap();
        let draft_anchor = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "draft anchor".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &session, &draft_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::Anchors,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let full_snapshot = crate::graph::build_graph_snapshot_with_mode(
            &writer,
            target_version,
            GraphMode::Anchors,
        )
        .await
        .unwrap();
        let full_viewport = crate::layout::materialize_graph_viewport(&full_snapshot);
        let incremental_draft = viewport
            .nodes
            .iter()
            .find(|node| node.id == draft_anchor)
            .unwrap();
        let full_draft = full_viewport
            .nodes
            .iter()
            .find(|node| node.id == draft_anchor)
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert_eq!(incremental_draft.x, full_draft.x);
        assert!(
            viewport
                .nodes
                .iter()
                .any(|node| node.id == draft_anchor && node.labels == vec!["draft".to_owned()])
        );
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == session
                && edge.target_id == draft_anchor
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::Anchors, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_scopes_new_anchor_branch_lane_to_current_context_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let old_session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &old_session).await.unwrap();
        let old_prompt = writer
            .append(NewNode {
                parent: old_session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "old prompt".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &old_session, &old_prompt)
            .await
            .unwrap();
        let current_session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &old_prompt, &current_session)
            .await
            .unwrap();
        let main_anchor = writer
            .append(NewNode {
                parent: current_session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "main anchor".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &current_session, &main_anchor)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::Anchors).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer.fork("draft", &current_session).await.unwrap();
        let draft_anchor = writer
            .append(NewNode {
                parent: current_session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "draft anchor".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &current_session, &draft_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::Anchors,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(viewport.nodes.iter().any(|node| node.id == current_session));
        assert!(viewport.nodes.iter().any(|node| node.id == draft_anchor));
        assert!(!viewport.nodes.iter().any(|node| node.id == old_session));
        assert!(!viewport.nodes.iter().any(|node| node.id == old_prompt));
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == current_session
                && edge.target_id == draft_anchor
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::Anchors, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);

        drop(cache);
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_scopes_existing_anchor_branch_append_to_current_context_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let old_session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &old_session).await.unwrap();
        let old_prompt = writer
            .append(NewNode {
                parent: old_session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "old prompt".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &old_session, &old_prompt)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::Anchors).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        let current_session = writer
            .append(NewNode {
                parent: old_prompt.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &old_prompt, &current_session)
            .await
            .unwrap();
        let current_prompt = writer
            .append(NewNode {
                parent: current_session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "current prompt".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &current_session, &current_prompt)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::Anchors,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(!viewport.nodes.iter().any(|node| node.id == old_session));
        assert!(!viewport.nodes.iter().any(|node| node.id == old_prompt));
        assert!(viewport.nodes.iter().any(|node| node.id == current_session));
        assert!(
            viewport
                .nodes
                .iter()
                .any(|node| node.id == current_prompt && node.labels == vec!["main".to_owned()])
        );
        assert!(
            cache
                .cached_snapshot(GraphMode::Anchors, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 2);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 1);

        drop(cache);
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_skips_hidden_new_anchor_branch_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_anchor = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "main anchor".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_anchor)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::Anchors).await;
        writer.fork("hidden", &root).await.unwrap();
        let hidden_text = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("hidden branch text".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("hidden", &root, &hidden_text)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::Anchors,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let main_lane = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == "main")
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert_eq!(main_lane.y, crate::layout::GRAPH_TOP_Y);
        assert!(viewport.lanes.iter().any(|lane| lane.label == "main"));
        assert!(!viewport.lanes.iter().any(|lane| lane.label == "hidden"));
        assert!(viewport.nodes.iter().any(|node| node.id == main_anchor));
        assert!(!viewport.nodes.iter().any(|node| node.id == hidden_text));
        assert!(
            cache
                .cached_snapshot(GraphMode::Anchors, target_version)
                .is_none()
        );

        drop(cache);
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_appends_anchor_branch_alias_lane_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_anchor = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "main anchor".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_anchor)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::Anchors).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer.fork("draft", &main_anchor).await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::Anchors,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let branch_labels = vec!["draft".to_owned(), "main".to_owned()];
        let alias_nodes = viewport
            .nodes
            .iter()
            .filter(|node| node.id == main_anchor && node.labels == branch_labels)
            .count();

        assert_eq!(viewport.version, target_version);
        assert_eq!(alias_nodes, 2);
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == session
                && edge.target_id == main_anchor
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::Anchors, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_appends_anchor_merge_route_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_anchor = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "main anchor".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        let main_hidden = writer
            .append(NewNode {
                parent: main_anchor.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main hidden".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_hidden)
            .await
            .unwrap();
        writer.fork("draft", &session).await.unwrap();
        let draft_anchor = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "draft anchor".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        let draft_hidden = writer
            .append(NewNode {
                parent: draft_anchor.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("draft hidden".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &session, &draft_hidden)
            .await
            .unwrap();
        let draft_second = writer
            .append(NewNode {
                parent: draft_hidden.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "draft second".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &draft_hidden, &draft_second)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::Anchors).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        let merge_anchor = writer
            .append(NewNode {
                parent: main_hidden.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(draft_second.clone())],
                    PromptAnchor {
                        prompt: "merge anchor".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_hidden, &merge_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::Anchors,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let full_snapshot = crate::graph::build_graph_snapshot_with_mode(
            &writer,
            target_version,
            GraphMode::Anchors,
        )
        .await
        .unwrap();
        let full_viewport = crate::layout::materialize_graph_viewport(&full_snapshot);
        let incremental_merge = viewport
            .nodes
            .iter()
            .find(|node| node.id == merge_anchor)
            .unwrap();
        let full_merge = full_viewport
            .nodes
            .iter()
            .find(|node| node.id == merge_anchor)
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert_eq!(incremental_merge.x, full_merge.x);
        assert!(
            viewport
                .nodes
                .iter()
                .any(|node| node.id == merge_anchor && node.labels == vec!["main".to_owned()])
        );
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == main_anchor
                && edge.target_id == merge_anchor
                && edge.kind == crate::api::GraphViewportEdgeKind::PrimaryParent
        }));
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == draft_second
                && edge.target_id == merge_anchor
                && edge.kind == crate::api::GraphViewportEdgeKind::MergeParent
                && edge.target_port_offset == crate::layout::EDGE_TARGET_PORT_STEP
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::Anchors, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_skips_anchor_merge_parent_outside_current_context_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let old_session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        let old_prompt = writer
            .append(NewNode {
                parent: old_session,
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "old prompt".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        let current_session = writer
            .append(NewNode {
                parent: old_prompt.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &current_session).await.unwrap();
        let current_prompt = writer
            .append(NewNode {
                parent: current_session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "current prompt".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &current_session, &current_prompt)
            .await
            .unwrap();
        publisher.mark_changed();
        let initial = cache.current_snapshot(GraphMode::Anchors).await;
        let merge_anchor = writer
            .append(NewNode {
                parent: current_prompt.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(old_prompt.clone())],
                    PromptAnchor {
                        prompt: "merge old prompt".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &current_prompt, &merge_anchor)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::Anchors,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        assert_eq!(viewport.version, target_version);
        assert!(viewport.nodes.iter().any(|node| node.id == merge_anchor));
        assert!(!viewport.nodes.iter().any(|node| node.id == old_prompt));
        assert!(
            !viewport
                .lanes
                .iter()
                .any(|lane| lane.label
                    == format!("orphan {}", crate::graph::shorten_id(&old_prompt)))
        );
        assert!(!viewport.edges.iter().any(|edge| {
            edge.source_id == old_prompt
                && edge.target_id == merge_anchor
                && edge.kind == crate::api::GraphViewportEdgeKind::MergeParent
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::Anchors, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_rewinds_branch_lane_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("first child".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &first)
            .await
            .unwrap();
        let second = writer
            .append(NewNode {
                parent: first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("second child".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &first, &second)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer
            .set_branch_head("main", &second, &first)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let first_node = viewport.nodes.iter().find(|node| node.id == first).unwrap();

        assert_eq!(viewport.version, target_version);
        assert_eq!(first_node.labels, vec!["main".to_owned()]);
        assert!(!viewport.nodes.iter().any(|node| node.id == second));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 1);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_rejects_branch_rewind_that_orphans_dependent_lane() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        let main_second = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main second".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_first, &main_second)
            .await
            .unwrap();
        writer.fork("draft", &main_second).await.unwrap();
        let draft_first = writer
            .append(NewNode {
                parent: main_second.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("draft first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &main_second, &draft_first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer
            .set_branch_head("main", &main_second, &main_first)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let stale = cache
            .viewport_current_ready_or_schedule(
                GraphMode::All,
                crate::host::api::GraphViewportRequest::default(),
            )
            .unwrap();
        for _ in 0..50 {
            if cache.rebuild_statuses().iter().any(|status| {
                status.mode == GraphMode::All
                    && status.source_version == target_version
                    && status.state == ConsoleGraphRebuildState::Failed
            }) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let materialized = ConsoleGraphSnapshotStore::open(&path)
            .await
            .unwrap()
            .latest_viewport(
                GraphMode::All,
                crate::host::api::GraphViewportRequest::default(),
            )
            .unwrap()
            .unwrap();

        assert_eq!(stale.version, initial.version);
        assert_eq!(materialized.version, initial.version);
        assert!(materialized.nodes.iter().any(|node| node.id == main_second));
        assert!(materialized.nodes.iter().any(|node| node.id == draft_first));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);
        let viewport_error = tokio::time::timeout(
            Duration::from_millis(200),
            cache.viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            ),
        )
        .await
        .expect("failed dependent rewind should not hang")
        .unwrap_err();
        assert!(matches!(
            viewport_error,
            crate::error::Error::ConsoleGraphRebuild {
                source_version,
                message,
                ..
            } if source_version == target_version && message.contains("could not apply")
        ));

        drop(cache);
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_deletes_branch_with_orphan_lane_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        writer.fork("draft", &main_first).await.unwrap();
        let draft_first = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("draft first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &main_first, &draft_first)
            .await
            .unwrap();
        let orphan_parent = writer
            .append(NewNode {
                parent: draft_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("orphan parent".to_owned()),
            })
            .await
            .unwrap();
        let draft_merge = writer
            .append(NewNode {
                parent: draft_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(orphan_parent.clone())],
                    PromptAnchor {
                        prompt: "draft merge".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &draft_first, &draft_merge)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer.delete_branch("draft").await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(!viewport.lanes.iter().any(|lane| lane.label == "draft"));
        assert!(!viewport.nodes.iter().any(|node| node.id == draft_first));
        assert!(!viewport.nodes.iter().any(|node| node.id == orphan_parent));
        assert!(!viewport.nodes.iter().any(|node| node.id == draft_merge));
        assert!(viewport.nodes.iter().any(|node| node.id == main_first));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert!(audit.row_count(&database_path, "node_delete").await >= 3);
        assert!(audit.row_count(&database_path, "edge_delete").await >= 3);

        drop(cache);
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_appends_multiple_linear_graph_materializations_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        writer.fork("draft", &main_first).await.unwrap();
        let draft_first = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("draft first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &main_first, &draft_first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        let main_second = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main second".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_first, &main_second)
            .await
            .unwrap();
        let draft_second = writer
            .append(NewNode {
                parent: draft_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("draft second".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &draft_first, &draft_second)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(viewport.nodes.iter().any(|node| node.id == main_second));
        assert!(viewport.nodes.iter().any(|node| node.id == draft_second));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 2);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_appends_new_branch_lane_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        let main_second = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main second".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_first, &main_second)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer.fork("draft", &main_first).await.unwrap();
        let draft_first = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("draft first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &main_first, &draft_first)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let full_snapshot =
            crate::graph::build_graph_snapshot_with_mode(&writer, target_version, GraphMode::All)
                .await
                .unwrap();
        let full_viewport = crate::layout::materialize_graph_viewport(&full_snapshot);
        let incremental_draft = viewport
            .nodes
            .iter()
            .find(|node| node.id == draft_first)
            .unwrap();
        let full_draft = full_viewport
            .nodes
            .iter()
            .find(|node| node.id == draft_first)
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert_eq!(incremental_draft.x, full_draft.x);
        assert!(viewport.nodes.iter().any(|node| node.id == draft_first));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_inserts_sorted_new_branch_from_earlier_lane_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("zeta", &session).await.unwrap();
        let shared = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("shared".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("zeta", &session, &shared)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        writer.fork("alpha", &shared).await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let alpha_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == "alpha")
            .unwrap()
            .y;
        let zeta_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == "zeta")
            .unwrap()
            .y;

        assert_eq!(viewport.version, target_version);
        assert!(alpha_y < zeta_y);
        assert!(
            viewport
                .nodes
                .iter()
                .any(|node| node.id == session && node.y == alpha_y)
        );
        assert!(
            viewport
                .nodes
                .iter()
                .any(|node| node.id == shared && node.y == alpha_y)
        );
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == session
                && edge.target_id == shared
                && edge.source.y == alpha_y
                && edge.target.y == alpha_y
                && edge.kind == crate::api::GraphViewportEdgeKind::PrimaryParent
        }));
        assert!(
            !viewport
                .nodes
                .iter()
                .any(|node| node.id == session && node.y == zeta_y)
        );
        assert!(
            viewport
                .nodes
                .iter()
                .any(|node| node.id == shared && node.y == zeta_y)
        );
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == session
                && edge.source.y == alpha_y
                && edge.target_id == shared
                && edge.target.y == zeta_y
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
        }));
        assert!(!viewport.edges.iter().any(|edge| {
            edge.target_id == shared && edge.target.y == alpha_y && edge.source.y == zeta_y
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_trims_lower_anchor_lane_after_earlier_branch_insert() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("zeta", &session).await.unwrap();
        let prompt = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    Vec::new(),
                    PromptAnchor {
                        prompt: "shared anchor node".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("zeta", &session, &prompt)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::Anchors).await;
        writer.fork("alpha", &prompt).await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::Anchors,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let alpha_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == "alpha")
            .unwrap()
            .y;
        let zeta_y = viewport
            .lanes
            .iter()
            .find(|lane| lane.label == "zeta")
            .unwrap()
            .y;

        assert_eq!(viewport.version, target_version);
        assert!(alpha_y < zeta_y);
        assert!(
            !viewport
                .nodes
                .iter()
                .any(|node| node.id == session && node.y == zeta_y)
        );
        assert!(
            viewport
                .nodes
                .iter()
                .any(|node| node.id == prompt && node.y == zeta_y)
        );
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == session
                && edge.source.y == alpha_y
                && edge.target_id == prompt
                && edge.target.y == zeta_y
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::Anchors, target_version)
                .is_none()
        );

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_appends_new_branch_merge_lane_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        let main_second = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main second".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_first, &main_second)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer.fork("draft", &main_first).await.unwrap();
        let draft_merge = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![MergeParent::merge(main_second.clone())],
                    PromptAnchor {
                        prompt: "draft merge".to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &main_first, &draft_merge)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(
            viewport
                .nodes
                .iter()
                .any(|node| node.id == draft_merge && node.labels == vec!["draft".to_owned()])
        );
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == main_first
                && edge.target_id == draft_merge
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
                && edge.target_port_offset == -crate::layout::EDGE_TARGET_PORT_STEP / 2.0
        }));
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == main_second
                && edge.target_id == draft_merge
                && edge.kind == crate::api::GraphViewportEdgeKind::MergeParent
                && edge.target_port_offset == crate::layout::EDGE_TARGET_PORT_STEP / 2.0
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_appends_new_branch_alias_lane_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer.fork("draft", &main_first).await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let draft_nodes = viewport
            .nodes
            .iter()
            .filter(|node| {
                node.id == main_first && node.labels == vec!["draft".to_owned(), "main".to_owned()]
            })
            .count();

        assert_eq!(viewport.version, target_version);
        assert_eq!(draft_nodes, 2);
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == session
                && edge.target_id == main_first
                && edge.target.y
                    == viewport
                        .lanes
                        .iter()
                        .find(|lane| lane.label == "draft")
                        .unwrap()
                        .y
                && edge.kind == crate::api::GraphViewportEdgeKind::Fork
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        publisher.mark_changed();
        let refresh_version = publisher.current_version();
        let refreshed = cache
            .viewport_after(
                GraphMode::All,
                target_version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let refreshed_alias_nodes = refreshed
            .nodes
            .iter()
            .filter(|node| {
                node.id == main_first && node.labels == vec!["draft".to_owned(), "main".to_owned()]
            })
            .count();

        assert_eq!(refreshed.version, refresh_version);
        assert_eq!(refreshed_alias_nodes, 2);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 1);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_appends_first_visible_branch_alias_lane_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer.fork("draft", &session).await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let draft_nodes = viewport
            .nodes
            .iter()
            .filter(|node| {
                node.id == session && node.labels == vec!["draft".to_owned(), "main".to_owned()]
            })
            .count();
        let reference = ConsoleGraphSnapshotStore::open(&path)
            .await
            .unwrap()
            .materialized_node_reference(GraphMode::All, &crate::graph::node_target_id(&session))
            .unwrap()
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert_eq!(draft_nodes, 2);
        assert_eq!(
            reference.labels,
            vec!["draft".to_owned(), "main".to_owned()]
        );
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_appends_from_duplicated_branch_tail_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let shared = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("shared tail".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &shared)
            .await
            .unwrap();
        writer.fork("draft", &shared).await.unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        let main_next = writer
            .append(NewNode {
                parent: shared.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main next".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &shared, &main_next)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let draft_tail_count = viewport
            .nodes
            .iter()
            .filter(|node| node.id == shared && node.labels == vec!["draft".to_owned()])
            .count();
        let main_next_node = viewport
            .nodes
            .iter()
            .find(|node| node.id == main_next)
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert_eq!(main_next_node.labels, vec!["main".to_owned()]);
        assert_eq!(draft_tail_count, 2);
        assert!(viewport.edges.iter().any(|edge| {
            edge.source_id == shared
                && edge.target_id == main_next
                && edge.kind == crate::api::GraphViewportEdgeKind::PrimaryParent
        }));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 2);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_inserts_middle_branch_lane_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        writer.fork("zeta", &main_first).await.unwrap();
        let zeta_first = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("zeta first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("zeta", &main_first, &zeta_first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer.fork("beta", &main_first).await.unwrap();
        let beta_first = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("beta first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("beta", &main_first, &beta_first)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let beta = viewport
            .nodes
            .iter()
            .find(|node| node.id == beta_first)
            .unwrap();
        let zeta = viewport
            .nodes
            .iter()
            .find(|node| node.id == zeta_first)
            .unwrap();
        let beta_edge = viewport
            .edges
            .iter()
            .find(|edge| {
                edge.source_id == main_first
                    && edge.target_id == beta_first
                    && edge.kind == crate::api::GraphViewportEdgeKind::Fork
            })
            .unwrap();
        let zeta_edge = viewport
            .edges
            .iter()
            .find(|edge| {
                edge.source_id == main_first
                    && edge.target_id == zeta_first
                    && edge.kind == crate::api::GraphViewportEdgeKind::Fork
            })
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert_eq!(beta_edge.route_slot, 0);
        assert_eq!(zeta_edge.route_slot, 1);
        assert_eq!(
            beta.y,
            crate::layout::GRAPH_TOP_Y + crate::layout::GRAPH_LANE_HEIGHT
        );
        assert_eq!(
            zeta.y,
            crate::layout::GRAPH_TOP_Y + crate::layout::GRAPH_LANE_HEIGHT * 2
        );
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 1);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_deletes_trailing_branch_lane_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        writer.fork("draft", &main_first).await.unwrap();
        let draft_first = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("draft first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &main_first, &draft_first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer.delete_branch("draft").await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(!viewport.nodes.iter().any(|node| node.id == draft_first));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 1);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 1);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_rejects_branch_delete_that_orphans_dependent_lane() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        let main_second = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main second".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &main_first, &main_second)
            .await
            .unwrap();
        writer.fork("draft", &main_second).await.unwrap();
        let draft_first = writer
            .append(NewNode {
                parent: main_second.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("draft first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("draft", &main_second, &draft_first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer.delete_branch("main").await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let stale = cache
            .viewport_current_ready_or_schedule(
                GraphMode::All,
                crate::host::api::GraphViewportRequest::default(),
            )
            .unwrap();
        for _ in 0..50 {
            if cache.rebuild_statuses().iter().any(|status| {
                status.mode == GraphMode::All
                    && status.source_version == target_version
                    && status.state == ConsoleGraphRebuildState::Failed
            }) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let materialized = ConsoleGraphSnapshotStore::open(&path)
            .await
            .unwrap()
            .latest_viewport(
                GraphMode::All,
                crate::host::api::GraphViewportRequest::default(),
            )
            .unwrap()
            .unwrap();

        assert_eq!(stale.version, initial.version);
        assert_eq!(materialized.version, initial.version);
        assert!(materialized.nodes.iter().any(|node| node.id == main_second));
        assert!(materialized.nodes.iter().any(|node| node.id == draft_first));
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 0);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);
        let viewport_error = tokio::time::timeout(
            Duration::from_millis(200),
            cache.viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            ),
        )
        .await
        .expect("failed dependent delete should not hang")
        .unwrap_err();
        assert!(matches!(
            viewport_error,
            crate::error::Error::ConsoleGraphRebuild {
                source_version,
                message,
                ..
            } if source_version == target_version && message.contains("could not apply")
        ));

        drop(cache);
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_deletes_middle_branch_lane_without_full_snapshot() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        let main_first = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("main first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &main_first)
            .await
            .unwrap();
        writer.fork("beta", &main_first).await.unwrap();
        let beta_first = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("beta first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("beta", &main_first, &beta_first)
            .await
            .unwrap();
        writer.fork("zeta", &main_first).await.unwrap();
        let zeta_first = writer
            .append(NewNode {
                parent: main_first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("zeta first".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("zeta", &main_first, &zeta_first)
            .await
            .unwrap();
        publisher.mark_changed();

        let initial = cache.current_snapshot(GraphMode::All).await;
        let database_path = crate::host::snapshot_store::database_path(&path);
        let audit = GraphFactAuditSnapshot::capture(&database_path).await;
        writer.delete_branch("beta").await.unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let viewport = cache
            .viewport_after(
                GraphMode::All,
                initial.version,
                crate::host::api::GraphViewportRequest::default(),
            )
            .await
            .unwrap();
        let zeta = viewport
            .nodes
            .iter()
            .find(|node| node.id == zeta_first)
            .unwrap();
        let zeta_edge = viewport
            .edges
            .iter()
            .find(|edge| {
                edge.source_id == main_first
                    && edge.target_id == zeta_first
                    && edge.kind == crate::api::GraphViewportEdgeKind::Fork
            })
            .unwrap();

        assert_eq!(viewport.version, target_version);
        assert!(!viewport.nodes.iter().any(|node| node.id == beta_first));
        assert_eq!(zeta_edge.route_slot, 0);
        assert_eq!(
            zeta.y,
            crate::layout::GRAPH_TOP_Y + crate::layout::GRAPH_LANE_HEIGHT
        );
        assert!(
            cache
                .cached_snapshot(GraphMode::All, target_version)
                .is_none()
        );
        assert_eq!(audit.row_count(&database_path, "node_delete").await, 2);
        assert_eq!(audit.row_count(&database_path, "edge_delete").await, 2);
        assert_eq!(audit.row_count(&database_path, "node_update").await, 0);
        assert_eq!(audit.row_count(&database_path, "edge_update").await, 0);

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_uses_snapshot_materialization_database() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let main_database_path = path.join("store.sqlite3");
        let graph_database_path = path.join("console-graph.sqlite3");

        ConsoleGraphSnapshotStore::open(&path).await.unwrap();

        assert_eq!(
            crate::host::snapshot_store::database_path(&path),
            graph_database_path
        );
        for table in [
            "console_graph_materializations",
            "console_graph_node_locations",
            "console_graph_edge_routes",
        ] {
            assert!(sqlite_table_exists(&graph_database_path, table).await);
            assert!(!sqlite_table_exists(&main_database_path, table).await);
        }

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn snapshot_write_transaction_allows_store_write_after_short_lock() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let snapshot = ConsoleGraphSnapshotStore::open(&path).await.unwrap();
        let base = root.clone();

        let (started_tx, started_rx) = mpsc::channel();
        let transaction = std::thread::spawn(move || {
            snapshot
                .with_connection_for_tests(move |connection| {
                    connection
                        .immediate_transaction::<(), diesel::result::Error, _>(|_| {
                            started_tx.send(()).unwrap();
                            std::thread::sleep(Duration::from_millis(200));
                            Ok(())
                        })
                        .unwrap();
                    Ok(())
                })
                .unwrap();
        });
        started_rx.recv().unwrap();

        writer.submit_job("main", &base).await.unwrap();
        transaction.join().unwrap();

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    diesel::table! {
        sqlite_master (name) {
            #[sql_name = "type"]
            object_type -> Text,
            name -> Text,
        }
    }

    fn with_sqlite_test_connection<T, F>(database: coco_mem::SqliteDatabase, operation: F) -> T
    where
        T: Send + 'static,
        F: FnOnce(&mut diesel::sqlite::SqliteConnection) -> T + Send + 'static,
    {
        database
            .with_sync_connection(
                |connection| Ok(operation(connection)),
                |source| panic!("{source}"),
                std::convert::identity,
            )
            .unwrap()
    }

    #[derive(Clone, PartialEq)]
    struct MaterializedNodeFact {
        node_id: String,
        node_target: String,
        short_id: String,
        node_kind: String,
        summary: String,
        labels_json: String,
        lane_key: String,
        lane_label: String,
        lane_y: i32,
        x: i32,
        y: i32,
        min_x: i32,
        min_y: i32,
        max_x: i32,
        max_y: i32,
    }

    #[derive(Clone, PartialEq)]
    struct MaterializedEdgeFact {
        edge_kind: String,
        source_id: String,
        target_id: String,
        source_x: i32,
        source_y: i32,
        target_x: i32,
        target_y: i32,
        route_slot: i32,
        target_port_offset_bits: u64,
        min_x: i32,
        min_y: i32,
        max_x: i32,
        max_y: i32,
    }

    struct GraphFactAuditSnapshot {
        nodes: BTreeMap<(String, String), MaterializedNodeFact>,
        edges: BTreeMap<(String, String), MaterializedEdgeFact>,
    }

    impl GraphFactAuditSnapshot {
        async fn capture(database_path: &std::path::Path) -> Self {
            use crate::schema::{console_graph_edge_routes, console_graph_node_locations};
            use diesel::prelude::*;

            let database = coco_mem::SqliteDatabase::open_unshared_file_path(database_path)
                .await
                .unwrap();
            with_sqlite_test_connection(database, |connection| {
                let nodes = console_graph_node_locations::table
                    .select((
                        console_graph_node_locations::mode,
                        console_graph_node_locations::node_key,
                        console_graph_node_locations::node_id,
                        console_graph_node_locations::node_target,
                        console_graph_node_locations::short_id,
                        console_graph_node_locations::node_kind,
                        console_graph_node_locations::summary,
                        console_graph_node_locations::labels_json,
                        console_graph_node_locations::lane_key,
                        console_graph_node_locations::lane_label,
                        console_graph_node_locations::lane_y,
                        console_graph_node_locations::x,
                        console_graph_node_locations::y,
                        console_graph_node_locations::min_x,
                        console_graph_node_locations::min_y,
                        console_graph_node_locations::max_x,
                        console_graph_node_locations::max_y,
                    ))
                    .load::<(
                        String,
                        String,
                        String,
                        String,
                        String,
                        String,
                        String,
                        String,
                        String,
                        String,
                        i32,
                        i32,
                        i32,
                        i32,
                        i32,
                        i32,
                        i32,
                    )>(connection)
                    .unwrap()
                    .into_iter()
                    .map(
                        |(
                            mode,
                            node_key,
                            node_id,
                            node_target,
                            short_id,
                            node_kind,
                            summary,
                            labels_json,
                            lane_key,
                            lane_label,
                            lane_y,
                            x,
                            y,
                            min_x,
                            min_y,
                            max_x,
                            max_y,
                        )| {
                            (
                                (mode, node_key),
                                MaterializedNodeFact {
                                    node_id,
                                    node_target,
                                    short_id,
                                    node_kind,
                                    summary,
                                    labels_json,
                                    lane_key,
                                    lane_label,
                                    lane_y,
                                    x,
                                    y,
                                    min_x,
                                    min_y,
                                    max_x,
                                    max_y,
                                },
                            )
                        },
                    )
                    .collect();
                let edges = console_graph_edge_routes::table
                    .select((
                        console_graph_edge_routes::mode,
                        console_graph_edge_routes::edge_key,
                        console_graph_edge_routes::edge_kind,
                        console_graph_edge_routes::source_id,
                        console_graph_edge_routes::target_id,
                        console_graph_edge_routes::source_x,
                        console_graph_edge_routes::source_y,
                        console_graph_edge_routes::target_x,
                        console_graph_edge_routes::target_y,
                        console_graph_edge_routes::route_slot,
                        console_graph_edge_routes::target_port_offset,
                        console_graph_edge_routes::min_x,
                        console_graph_edge_routes::min_y,
                        console_graph_edge_routes::max_x,
                        console_graph_edge_routes::max_y,
                    ))
                    .load::<(
                        String,
                        String,
                        String,
                        String,
                        String,
                        i32,
                        i32,
                        i32,
                        i32,
                        i32,
                        f64,
                        i32,
                        i32,
                        i32,
                        i32,
                    )>(connection)
                    .unwrap()
                    .into_iter()
                    .map(
                        |(
                            mode,
                            edge_key,
                            edge_kind,
                            source_id,
                            target_id,
                            source_x,
                            source_y,
                            target_x,
                            target_y,
                            route_slot,
                            target_port_offset,
                            min_x,
                            min_y,
                            max_x,
                            max_y,
                        )| {
                            (
                                (mode, edge_key),
                                MaterializedEdgeFact {
                                    edge_kind,
                                    source_id,
                                    target_id,
                                    source_x,
                                    source_y,
                                    target_x,
                                    target_y,
                                    route_slot,
                                    target_port_offset_bits: target_port_offset.to_bits(),
                                    min_x,
                                    min_y,
                                    max_x,
                                    max_y,
                                },
                            )
                        },
                    )
                    .collect();
                Self { nodes, edges }
            })
        }

        async fn row_count(&self, database_path: &std::path::Path, kind: &str) -> i64 {
            let next = Self::capture(database_path).await;
            match kind {
                "node_delete" => deleted_count(&self.nodes, &next.nodes),
                "edge_delete" => deleted_count(&self.edges, &next.edges),
                "node_update" => updated_count(&self.nodes, &next.nodes),
                "edge_update" => updated_count(&self.edges, &next.edges),
                kind => panic!("unsupported graph fact audit kind {kind}"),
            }
        }
    }

    fn deleted_count<T>(
        before: &BTreeMap<(String, String), T>,
        after: &BTreeMap<(String, String), T>,
    ) -> i64 {
        before
            .keys()
            .filter(|key| !after.contains_key(*key))
            .count() as i64
    }

    fn updated_count<T: PartialEq>(
        before: &BTreeMap<(String, String), T>,
        after: &BTreeMap<(String, String), T>,
    ) -> i64 {
        before
            .iter()
            .filter(|(key, value)| after.get(*key).is_some_and(|next| next != *value))
            .count() as i64
    }

    async fn sqlite_table_exists(database_path: &std::path::Path, table: &str) -> bool {
        use diesel::prelude::*;

        let table = table.to_owned();
        let database = coco_mem::SqliteDatabase::open_unshared_file_path(database_path)
            .await
            .unwrap();
        with_sqlite_test_connection(database, move |connection| {
            sqlite_master::table
                .filter(sqlite_master::object_type.eq("table"))
                .filter(sqlite_master::name.eq(table))
                .count()
                .get_result::<i64>(connection)
                .unwrap()
                > 0
        })
    }

    async fn sqlite_table_has_column(
        database_path: &std::path::Path,
        table: &str,
        column: &str,
    ) -> bool {
        use crate::schema::{
            console_graph_edge_routes, console_graph_materializations, console_graph_node_locations,
        };
        use diesel::prelude::*;

        let table = table.to_owned();
        let column = column.to_owned();
        let database = coco_mem::SqliteDatabase::open_unshared_file_path(database_path)
            .await
            .unwrap();
        with_sqlite_test_connection(database, move |connection| {
            match (table.as_str(), column.as_str()) {
                ("console_graph_materializations", "source_version") => {
                    console_graph_materializations::table
                        .select(console_graph_materializations::source_version)
                        .limit(0)
                        .load::<i64>(connection)
                        .is_ok()
                }
                ("console_graph_node_locations", "node_target") => {
                    console_graph_node_locations::table
                        .select(console_graph_node_locations::node_target)
                        .limit(0)
                        .load::<String>(connection)
                        .is_ok()
                }
                ("console_graph_edge_routes", "edge_kind") => console_graph_edge_routes::table
                    .select(console_graph_edge_routes::edge_kind)
                    .limit(0)
                    .load::<String>(connection)
                    .is_ok(),
                _ => false,
            }
        })
    }

    async fn sqlite_table_row_count(database_path: &std::path::Path, table: &str) -> i64 {
        use crate::schema::{
            console_graph_edge_routes, console_graph_materializations, console_graph_node_locations,
        };
        use diesel::prelude::*;

        let table = table.to_owned();
        let database = coco_mem::SqliteDatabase::open_unshared_file_path(database_path)
            .await
            .unwrap();
        with_sqlite_test_connection(database, move |connection| match table.as_str() {
            "console_graph_materializations" => console_graph_materializations::table
                .count()
                .get_result(connection)
                .unwrap(),
            "console_graph_node_locations" => console_graph_node_locations::table
                .count()
                .get_result(connection)
                .unwrap(),
            "console_graph_edge_routes" => console_graph_edge_routes::table
                .count()
                .get_result(connection)
                .unwrap(),
            table => panic!("unsupported test table {table}"),
        })
    }

    #[tokio::test]
    async fn cache_current_snapshot_does_not_rewrite_materialized_facts() {
        let path = temp_store_path();
        let writer = PersistentStore::open(&path).await.unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            test_store().await,
            publisher.clone(),
            path.clone(),
        )
        .await
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .await
            .unwrap();
        writer.fork("main", &session).await.unwrap();
        publisher.mark_changed();
        let first_version = publisher.current_version();
        assert!(
            cache
                .viewport_current_ready_or_schedule(
                    GraphMode::All,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .is_none()
        );
        let mut first_materialized = None;
        for _ in 0..50 {
            first_materialized = ConsoleGraphSnapshotStore::open(&path)
                .await
                .unwrap()
                .latest_viewport(
                    GraphMode::All,
                    crate::host::api::GraphViewportRequest::default(),
                )
                .unwrap();
            if first_materialized
                .as_ref()
                .is_some_and(|viewport| viewport.version == first_version)
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let first_materialized =
            first_materialized.expect("initial materialization should be seeded");
        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("new node should roll back".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &session, &text)
            .await
            .unwrap();
        publisher.mark_changed();
        let target_version = publisher.current_version();

        let snapshot = cache.snapshot_current(GraphMode::All).await.unwrap();
        let materialized = ConsoleGraphSnapshotStore::open(&path)
            .await
            .unwrap()
            .latest_viewport(
                GraphMode::All,
                crate::host::api::GraphViewportRequest::default(),
            )
            .unwrap()
            .unwrap();

        assert_eq!(first_materialized.version, first_version);
        assert_eq!(snapshot.version, target_version);
        assert_eq!(materialized.version, first_version);
        assert!(!materialized.nodes.iter().any(|node| node.id == text));

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn graph_compute_permit_serializes_blocking_work() {
        let publisher = ConsolePublisher::new();
        let (store, _, _) = graph_store(publisher.clone()).await;
        let cache = Arc::new(ConsoleGraphCache::new(store, publisher));
        let (first_started_tx, first_started_rx) = tokio::sync::oneshot::channel();
        let (release_first_tx, release_first_rx) = mpsc::channel();
        let second_prepared = Arc::new(AtomicBool::new(false));
        let second_started = Arc::new(AtomicBool::new(false));

        let first = tokio::spawn({
            let cache = cache.clone();
            async move {
                cache
                    .run_blocking_graph_compute(move || {
                        first_started_tx.send(()).unwrap();
                        release_first_rx.recv().unwrap();
                    })
                    .await
                    .unwrap();
            }
        });
        first_started_rx.await.unwrap();

        let second = tokio::spawn({
            let cache = cache.clone();
            let second_prepared = second_prepared.clone();
            let second_started = second_started.clone();
            async move {
                cache
                    .run_blocking_graph_compute_with(
                        move || {
                            second_prepared.store(true, Ordering::SeqCst);
                        },
                        move |()| {
                            second_started.store(true, Ordering::SeqCst);
                        },
                    )
                    .await
                    .unwrap();
            }
        });

        sleep(Duration::from_millis(50)).await;
        assert!(
            !second_prepared.load(Ordering::SeqCst),
            "second graph compute should prepare after the shared permit"
        );
        assert!(
            !second_started.load(Ordering::SeqCst),
            "second graph compute should wait for the shared permit"
        );

        release_first_tx.send(()).unwrap();
        first.await.unwrap();
        second.await.unwrap();
        assert!(second_prepared.load(Ordering::SeqCst));
        assert!(second_started.load(Ordering::SeqCst));
    }
}
