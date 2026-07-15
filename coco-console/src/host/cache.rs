use std::collections::BTreeSet;
use std::future::Future;
use std::path::{Path, PathBuf};
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
use crate::layout::{layout_graph_viewport, layout_graph_viewport_diff};
use crate::publisher::ConsolePublisher;
use coco_mem::{BranchStore, NodeStore, SessionState, SessionStore, SqliteGraphStore, Store};
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
            publish_lock: Arc::new(Mutex::new(())),
            state: Arc::new(Mutex::new(CacheState::default())),
        }
    }

    pub async fn new_with_persistent_store_path(
        _store: S,
        invalidations: ConsolePublisher,
        path: PathBuf,
    ) -> crate::Result<Self> {
        SqliteGraphStore::open_read_only(&path)
            .await
            .context(crate::error::StoreSnafu)?;
        let snapshots = ConsoleGraphSnapshotStore::open(&path).await?;
        let persisted_version = latest_persistent_materialization_version(&snapshots).await?;
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
            source: ConsoleGraphSource::PersistentStore(path),
            invalidations,
            ready,
            progress: ConsolePublisher::new(),
            snapshots: Some(snapshots),
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

    pub async fn rebuild_requested_modes(&self) {
        for mode in [GraphMode::Anchors, GraphMode::All] {
            self.ensure_viewport_current(mode).await;
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
        set_rebuild_status!(self, ready_rebuild_status(mode, source_version));
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
        self.ensure_viewport_current(mode).await;
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
        self.ensure_viewport_current(mode).await;
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

    pub(crate) async fn materialized_fragment_current_ready_or_schedule(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Option<MaterializedGraphShell>> {
        let Some(snapshots) = &self.snapshots else {
            return Ok(None);
        };
        self.ensure_viewport_current(mode).await;
        let Some(facts) = snapshots.materialized_shell_facts(mode).await? else {
            return Ok(None);
        };
        let branches = self.source.clone().materialized_shell_branches().await?;
        let time_ticks = facts
            .nodes
            .iter()
            .map(|node| MaterializedGraphShellTick {
                time_ns: node.created_at_ns,
                label: node.created_at.clone(),
                node_target: node.node_target.clone(),
                point: node.point,
            })
            .collect();
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
                    self.ensure_viewport_current(mode).await;
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
            self.ensure_viewport_current(mode).await;
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
            let mut invalidations = self.invalidations.subscribe();
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
                changed = invalidations.changed() => {
                    if changed.is_err() {
                        continue;
                    }
                    self.ensure_viewport_current(mode).await;
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
            self.ensure_viewport_current(mode).await;
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
            let mut invalidations = self.invalidations.subscribe();
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
                changed = invalidations.changed() => {
                    if changed.is_err() {
                        continue;
                    }
                    self.ensure_viewport_current(mode).await;
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
        self.ensure_viewport_current(mode).await;
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

    fn ensure_snapshot_current(&self, mode: GraphMode) {
        let source_version = self.invalidations.current_version();
        if self.cached_snapshot(mode, source_version).is_some() {
            set_rebuild_status!(self, ready_rebuild_status(mode, source_version));
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

    async fn ensure_viewport_current(&self, mode: GraphMode) {
        let source_version = self.invalidations.current_version();
        if (self.snapshots.is_none() && self.cached_snapshot(mode, source_version).is_some())
            || self.materialization_current(mode, source_version).await
        {
            set_rebuild_status!(self, ready_rebuild_status(mode, source_version));
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
                snapshots.materialize_snapshot(&snapshot).await?;
            }
            crate::Result::Ok(snapshot)
        }
        .await;
        match result {
            Ok(snapshot) => {
                self.store_cached_snapshot(mode, source_version, Arc::new(snapshot));
                self.publish_ready_version(source_version);
                set_rebuild_status!(self, ready_rebuild_status(mode, source_version));
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

    fn set_rebuild_status(&self, status: &ConsoleGraphRebuildStatus) -> bool {
        let mode = status.mode;
        let mut state = self
            .state
            .lock()
            .expect("console graph cache lock poisoned");
        if state.rebuild_slot(mode).as_ref() == Some(status) {
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

async fn run_sqlite_graph_read_transaction<T, Fut>(
    store: &SqliteGraphStore,
    operation: Fut,
) -> crate::Result<T>
where
    Fut: Future<Output = crate::Result<T>>,
{
    store
        .begin_read_transaction()
        .await
        .context(crate::error::StoreSnafu)?;

    let result = operation.await;
    match result {
        Ok(value) => {
            store
                .commit_read_transaction()
                .await
                .context(crate::error::StoreSnafu)?;
            Ok(value)
        }
        Err(error) => {
            if let Err(rollback_error) = store.rollback_read_transaction().await {
                tracing::warn!(
                    error = %rollback_error,
                    "failed to roll back SQLite graph read transaction after console graph read error"
                );
            }
            Err(error)
        }
    }
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
                run_sqlite_graph_read_transaction(
                    &store,
                    build_graph_snapshot_with_mode_and_progress(&store, version, mode, progress),
                )
                .await
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

    async fn materialized_shell_branches(self) -> crate::Result<Vec<MaterializedGraphShellBranch>> {
        match self {
            Self::Store(store) => materialized_shell_branches(&store).await,
            Self::PersistentStore(path) => {
                let store = open_persistent_graph_store(&path).await?;
                materialized_shell_branches(&store).await
            }
        }
    }
}

async fn materialized_shell_branches(
    store: &(impl BranchStore + SessionStore),
) -> crate::Result<Vec<MaterializedGraphShellBranch>> {
    let mut states = store
        .list_session_states()
        .await
        .context(crate::error::StoreSnafu)?
        .into_iter()
        .collect::<Vec<(String, SessionState)>>();
    states.sort_by(|(left_branch, _), (right_branch, _)| {
        branch_order(left_branch).cmp(&branch_order(right_branch))
    });

    let mut branches = Vec::new();
    for (branch, state) in states {
        let head_id = store
            .get_branch_head(&branch)
            .await
            .context(crate::error::StoreSnafu)?;
        branches.push(MaterializedGraphShellBranch {
            name: branch,
            head_short_id: crate::graph::shorten_id(&head_id),
            state,
        });
    }
    Ok(branches)
}

fn branch_order(branch: &str) -> (u8, &str) {
    (u8::from(branch != "main"), branch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use coco_mem::SqliteStore;

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
}
