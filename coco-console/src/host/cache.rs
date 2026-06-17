use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::graph::{
    GraphBuildPhase, GraphBuildProgress, GraphMode, GraphSnapshot,
    build_graph_snapshot_with_mode_and_progress,
};
use crate::publisher::ConsolePublisher;
use coco_mem::Store;
use serde::Serialize;
use tokio::sync::Semaphore;

const GRAPH_REBUILD_THROTTLE: Duration = Duration::from_millis(75);
const GRAPH_REBUILD_COOLDOWN: Duration = Duration::from_millis(150);
const GRAPH_REBUILD_PROGRESS_INTERVAL: Duration = Duration::from_millis(100);

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
    pub phase: Option<GraphBuildPhase>,
    pub processed: usize,
    pub total: usize,
    pub message: String,
}

#[derive(Clone)]
pub struct ConsoleGraphCache<S> {
    store: S,
    invalidations: ConsolePublisher,
    ready: ConsolePublisher,
    progress: ConsolePublisher,
    build_permits: Arc<Semaphore>,
    state: Arc<Mutex<CacheState>>,
}

#[derive(Default)]
struct CacheState {
    anchors: CacheSlot,
    all: CacheSlot,
}

#[derive(Default)]
struct CacheSlot {
    snapshot: Option<Arc<GraphSnapshot>>,
    source_version: u64,
    scheduled_source_version: Option<u64>,
    building_source_version: Option<u64>,
    failure: Option<CacheFailure>,
    requested: bool,
    status: Option<ConsoleGraphRebuildStatus>,
    last_status_published_at: Option<Instant>,
}

#[derive(Clone)]
struct CacheFailure {
    source_version: u64,
    message: Arc<str>,
}

impl<S> ConsoleGraphCache<S>
where
    S: Store + Clone + Send + Sync + 'static,
{
    pub fn new(store: S, invalidations: ConsolePublisher) -> Self {
        Self {
            store,
            invalidations,
            ready: ConsolePublisher::new(),
            progress: ConsolePublisher::new(),
            build_permits: Arc::new(Semaphore::new(1)),
            state: Arc::new(Mutex::new(CacheState::default())),
        }
    }

    pub fn current_version(&self) -> u64 {
        self.ready.current_version()
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
        [GraphMode::Anchors, GraphMode::All]
            .into_iter()
            .filter_map(|mode| state.slot(mode).status.clone())
            .collect()
    }

    pub fn subscribe_invalidations(&self) -> tokio::sync::watch::Receiver<u64> {
        self.invalidations.subscribe()
    }

    pub fn rebuild_requested_modes(&self) {
        self.ensure_rebuild_if_requested(GraphMode::Anchors);
        self.ensure_rebuild_if_requested(GraphMode::All);
    }

    pub fn snapshot_or_placeholder(&self, mode: GraphMode) -> Arc<GraphSnapshot> {
        self.ensure_rebuild(mode);
        if let Some(snapshot) = self.cached_snapshot(mode) {
            return snapshot;
        }
        Arc::new(self.placeholder_snapshot(mode))
    }

    pub async fn snapshot_after(
        &self,
        mode: GraphMode,
        observed_version: u64,
    ) -> crate::Result<Arc<GraphSnapshot>> {
        loop {
            let mut rx = self.ready.subscribe();
            let mut invalidations = self.invalidations.subscribe();
            self.ensure_rebuild(mode);
            if let Some(snapshot) = self.cached_snapshot(mode)
                && snapshot.version > observed_version
            {
                return Ok(snapshot);
            }
            let source_version = self.invalidations.current_version();
            if let Some(failure) = self.cached_failure(mode, source_version) {
                return Err(crate::Error::ConsoleGraphRebuild {
                    mode: mode.as_query_value(),
                    source_version,
                    message: failure.message.to_string(),
                });
            }
            tokio::select! {
                changed = rx.changed() => {
                    if changed.is_err() {
                        return Ok(self.snapshot_or_placeholder(mode));
                    }
                }
                changed = invalidations.changed() => {
                    if changed.is_err() {
                        return Ok(self.snapshot_or_placeholder(mode));
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub async fn current_snapshot(&self, mode: GraphMode) -> Arc<GraphSnapshot> {
        loop {
            let mut rx = self.ready.subscribe();
            let mut invalidations = self.invalidations.subscribe();
            self.ensure_rebuild(mode);
            let source_version = self.invalidations.current_version();
            if let Some(snapshot) = self.cached_snapshot(mode)
                && self.cached_source_version(mode) >= source_version
            {
                return snapshot;
            }
            tokio::select! {
                changed = rx.changed() => {
                    if changed.is_err() {
                        return self.snapshot_or_placeholder(mode);
                    }
                }
                changed = invalidations.changed() => {
                    if changed.is_err() {
                        return self.snapshot_or_placeholder(mode);
                    }
                }
            }
        }
    }

    fn cached_snapshot(&self, mode: GraphMode) -> Option<Arc<GraphSnapshot>> {
        let state = self
            .state
            .lock()
            .expect("console graph cache lock poisoned");
        state.slot(mode).snapshot.clone()
    }

    fn cached_failure(&self, mode: GraphMode, source_version: u64) -> Option<CacheFailure> {
        let state = self
            .state
            .lock()
            .expect("console graph cache lock poisoned");
        let failure = state.slot(mode).failure.as_ref()?;
        (failure.source_version == source_version).then(|| failure.clone())
    }

    #[cfg(test)]
    fn cached_source_version(&self, mode: GraphMode) -> u64 {
        let state = self
            .state
            .lock()
            .expect("console graph cache lock poisoned");
        state.slot(mode).source_version
    }

    fn ensure_rebuild(&self, mode: GraphMode) {
        self.ensure_rebuild_with_request(mode, true);
    }

    fn ensure_rebuild_if_requested(&self, mode: GraphMode) {
        self.ensure_rebuild_with_request(mode, false);
    }

    fn ensure_rebuild_with_request(&self, mode: GraphMode, mark_requested: bool) {
        let source_version = self.invalidations.current_version();
        let action = {
            let mut state = self
                .state
                .lock()
                .expect("console graph cache lock poisoned");
            let slot = state.slot_mut(mode);
            if mark_requested {
                slot.requested = true;
            }
            if !slot.requested
                || !slot.needs_rebuild(source_version)
                || slot.building_source_version.is_some()
                || slot.scheduled_source_version.is_some()
            {
                RebuildAction::None
            } else if slot.snapshot.is_some() {
                slot.scheduled_source_version = Some(source_version);
                slot.set_status(
                    ConsoleGraphRebuildStatus::scheduled(mode, source_version),
                    Instant::now(),
                    true,
                );
                RebuildAction::Schedule
            } else {
                slot.building_source_version = Some(source_version);
                slot.set_status(
                    ConsoleGraphRebuildStatus::scheduled(mode, source_version),
                    Instant::now(),
                    true,
                );
                RebuildAction::Spawn(source_version)
            }
        };

        match action {
            RebuildAction::None => {}
            RebuildAction::Schedule => {
                self.progress.mark_changed();
                self.spawn_scheduled_rebuild(mode);
            }
            RebuildAction::Spawn(source_version) => {
                self.progress.mark_changed();
                self.spawn_rebuild(mode, source_version);
            }
        }
    }

    fn spawn_scheduled_rebuild(&self, mode: GraphMode) {
        let cache = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(GRAPH_REBUILD_THROTTLE).await;
            cache.start_scheduled_rebuild(mode);
        });
    }

    fn start_scheduled_rebuild(&self, mode: GraphMode) {
        let source_version = self.invalidations.current_version();
        let should_spawn = {
            let mut state = self
                .state
                .lock()
                .expect("console graph cache lock poisoned");
            let slot = state.slot_mut(mode);
            slot.scheduled_source_version = None;
            if !slot.requested
                || !slot.needs_rebuild(source_version)
                || slot.building_source_version.is_some()
            {
                false
            } else {
                slot.building_source_version = Some(source_version);
                slot.set_status(
                    ConsoleGraphRebuildStatus::scheduled(mode, source_version),
                    Instant::now(),
                    true,
                );
                true
            }
        };

        if should_spawn {
            self.progress.mark_changed();
            self.spawn_rebuild(mode, source_version);
        }
    }

    fn spawn_rebuild(&self, mode: GraphMode, source_version: u64) {
        let cache = self.clone();
        tokio::spawn(async move {
            let Ok(permit) = cache.build_permits.clone().acquire_owned().await else {
                return;
            };
            let Some(source_version) = cache.begin_rebuild_after_permit(mode, source_version)
            else {
                return;
            };
            let store = cache.store.clone();
            let progress_cache = cache.clone();
            let result = match tokio::task::spawn_blocking(move || {
                build_graph_snapshot_with_mode_and_progress(&store, 0, mode, |progress| {
                    progress_cache.record_rebuild_progress(mode, source_version, progress);
                })
            })
            .await
            {
                Ok(result) => result,
                Err(source) => Err(crate::Error::JoinConsoleServer { source }),
            };
            cache.finish_rebuild(mode, source_version, result);
            tokio::time::sleep(GRAPH_REBUILD_COOLDOWN).await;
            drop(permit);
        });
    }

    fn begin_rebuild_after_permit(
        &self,
        mode: GraphMode,
        queued_source_version: u64,
    ) -> Option<u64> {
        let source_version = self.invalidations.current_version();
        let should_publish = {
            let mut state = self
                .state
                .lock()
                .expect("console graph cache lock poisoned");
            let slot = state.slot_mut(mode);
            if slot.building_source_version != Some(queued_source_version) {
                return None;
            }
            slot.building_source_version = Some(source_version);
            slot.set_status(
                ConsoleGraphRebuildStatus::building(
                    mode,
                    source_version,
                    GraphBuildProgress {
                        phase: GraphBuildPhase::Branches,
                        processed: 0,
                        total: 0,
                    },
                ),
                Instant::now(),
                true,
            )
        };
        if should_publish {
            self.progress.mark_changed();
        }
        Some(source_version)
    }

    fn record_rebuild_progress(
        &self,
        mode: GraphMode,
        source_version: u64,
        progress: GraphBuildProgress,
    ) {
        let should_publish = {
            let mut state = self
                .state
                .lock()
                .expect("console graph cache lock poisoned");
            let slot = state.slot_mut(mode);
            if slot.building_source_version != Some(source_version) {
                return;
            }
            slot.set_status(
                ConsoleGraphRebuildStatus::building(mode, source_version, progress),
                Instant::now(),
                false,
            )
        };
        if should_publish {
            self.progress.mark_changed();
        }
    }

    fn finish_rebuild(
        &self,
        mode: GraphMode,
        source_version: u64,
        result: crate::Result<GraphSnapshot>,
    ) {
        let mut snapshot = match result {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(
                    mode = mode.as_query_value(),
                    source_version,
                    error = %error,
                    "console graph rebuild failed"
                );
                self.record_rebuild_failure(mode, source_version, error.to_string());
                return;
            }
        };

        let should_rebuild = {
            let mut state = self
                .state
                .lock()
                .expect("console graph cache lock poisoned");
            let slot = state.slot_mut(mode);
            if slot.building_source_version != Some(source_version) {
                return;
            }
            let graph_version = self.ready.current_version() + 1;
            snapshot.version = graph_version;
            slot.snapshot = Some(Arc::new(snapshot));
            slot.source_version = source_version;
            slot.building_source_version = None;
            slot.failure = None;
            slot.set_status(
                ConsoleGraphRebuildStatus::ready(mode, source_version, graph_version),
                Instant::now(),
                true,
            );
            let published_version = self.ready.mark_changed();
            debug_assert_eq!(published_version, graph_version);
            slot.requested && slot.needs_rebuild(self.invalidations.current_version())
        };

        self.progress.mark_changed();
        if should_rebuild {
            self.ensure_rebuild(mode);
        }
    }

    fn record_rebuild_failure(&self, mode: GraphMode, source_version: u64, message: String) {
        let should_rebuild = {
            let mut state = self
                .state
                .lock()
                .expect("console graph cache lock poisoned");
            let slot = state.slot_mut(mode);
            if slot.building_source_version == Some(source_version) {
                slot.building_source_version = None;
                slot.failure = Some(CacheFailure {
                    source_version,
                    message: Arc::from(message),
                });
                slot.set_status(
                    ConsoleGraphRebuildStatus::failed(mode, source_version),
                    Instant::now(),
                    true,
                );
                self.ready.mark_changed();
                self.progress.mark_changed();
            }
            slot.requested && slot.needs_rebuild(self.invalidations.current_version())
        };
        if should_rebuild {
            self.ensure_rebuild(mode);
        }
    }

    fn placeholder_snapshot(&self, mode: GraphMode) -> GraphSnapshot {
        GraphSnapshot {
            version: self.ready.current_version(),
            mode,
            root_id: self.store.root_id(),
            nodes: Vec::new(),
            edges: Vec::new(),
            branches: Vec::new(),
            provider_contexts: Vec::new(),
        }
    }
}

impl CacheState {
    fn slot(&self, mode: GraphMode) -> &CacheSlot {
        match mode {
            GraphMode::Anchors => &self.anchors,
            GraphMode::All => &self.all,
        }
    }

    fn slot_mut(&mut self, mode: GraphMode) -> &mut CacheSlot {
        match mode {
            GraphMode::Anchors => &mut self.anchors,
            GraphMode::All => &mut self.all,
        }
    }
}

impl CacheSlot {
    fn needs_rebuild(&self, source_version: u64) -> bool {
        let snapshot_is_missing = self.snapshot.is_none();
        let snapshot_is_stale = self.source_version < source_version;
        (snapshot_is_missing || snapshot_is_stale)
            && self.scheduled_source_version != Some(source_version)
            && self.building_source_version != Some(source_version)
            && self
                .failure
                .as_ref()
                .is_none_or(|failure| failure.source_version != source_version)
    }

    fn set_status(
        &mut self,
        status: ConsoleGraphRebuildStatus,
        now: Instant,
        force_publish: bool,
    ) -> bool {
        let phase_changed = self
            .status
            .as_ref()
            .is_none_or(|current| current.state != status.state || current.phase != status.phase);
        self.status = Some(status);
        if force_publish
            || phase_changed
            || self
                .last_status_published_at
                .is_none_or(|last| now.duration_since(last) >= GRAPH_REBUILD_PROGRESS_INTERVAL)
        {
            self.last_status_published_at = Some(now);
            true
        } else {
            false
        }
    }
}

enum RebuildAction {
    None,
    Schedule,
    Spawn(u64),
}

impl ConsoleGraphRebuildStatus {
    fn scheduled(mode: GraphMode, source_version: u64) -> Self {
        Self {
            mode,
            source_version,
            state: ConsoleGraphRebuildState::Scheduled,
            phase: None,
            processed: 0,
            total: 0,
            message: "Graph rebuild queued".to_owned(),
        }
    }

    fn building(mode: GraphMode, source_version: u64, progress: GraphBuildProgress) -> Self {
        Self {
            mode,
            source_version,
            state: ConsoleGraphRebuildState::Building,
            phase: Some(progress.phase),
            processed: progress.processed,
            total: progress.total,
            message: progress.phase.label().to_owned(),
        }
    }

    fn ready(mode: GraphMode, source_version: u64, graph_version: u64) -> Self {
        Self {
            mode,
            source_version,
            state: ConsoleGraphRebuildState::Ready,
            phase: Some(GraphBuildPhase::Snapshot),
            processed: 1,
            total: 1,
            message: format!("Graph version {graph_version} ready"),
        }
    }

    fn failed(mode: GraphMode, source_version: u64) -> Self {
        Self {
            mode,
            source_version,
            state: ConsoleGraphRebuildState::Failed,
            phase: None,
            processed: 0,
            total: 0,
            message: "Graph rebuild failed".to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ConsoleStore;
    use coco_mem::{
        Anchor, BranchStore, Kind, MemoryStore, NewNode, NodeStore, Role, SessionAnchor,
        SessionRole, Tool,
    };

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

    fn graph_store(publisher: ConsolePublisher) -> (ConsoleStore<MemoryStore>, String, String) {
        let store = ConsoleStore::new(MemoryStore::new(), publisher);
        let root = store.root_id();
        let session = store
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .unwrap();
        store.fork("main", &session).unwrap();
        let text = store
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("visible only in all mode".to_owned()),
            })
            .unwrap();
        store.set_branch_head("main", &session, &text).unwrap();

        (store, session, text)
    }

    #[tokio::test]
    async fn cache_keeps_snapshots_per_mode() {
        let publisher = ConsolePublisher::new();
        let (store, session, text) = graph_store(publisher.clone());
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
    async fn cache_rebuilds_requested_mode_without_refreshing_other_modes() {
        let publisher = ConsolePublisher::new();
        let (store, _, text) = graph_store(publisher.clone());
        let cache = ConsoleGraphCache::new(store.clone(), publisher.clone());

        cache.current_snapshot(GraphMode::All).await;
        cache.current_snapshot(GraphMode::Anchors).await;
        let initial_anchor_source_version = cache.cached_source_version(GraphMode::Anchors);

        let next_text = store
            .append(NewNode {
                parent: text.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("new all-mode node".to_owned()),
            })
            .unwrap();
        store.set_branch_head("main", &text, &next_text).unwrap();
        let current_source_version = publisher.current_version();

        let all = cache.current_snapshot(GraphMode::All).await;

        assert!(all.nodes.iter().any(|node| node.id == next_text));
        assert_eq!(
            cache.cached_source_version(GraphMode::All),
            current_source_version
        );
        assert_eq!(
            cache.cached_source_version(GraphMode::Anchors),
            initial_anchor_source_version
        );

        cache.current_snapshot(GraphMode::Anchors).await;
        assert_eq!(
            cache.cached_source_version(GraphMode::Anchors),
            current_source_version
        );
    }

    #[tokio::test]
    async fn cache_queues_stale_rebuilds_before_refreshing_snapshot() {
        let publisher = ConsolePublisher::new();
        let (store, _, text) = graph_store(publisher.clone());
        let cache = ConsoleGraphCache::new(store.clone(), publisher.clone());

        let initial = cache.current_snapshot(GraphMode::All).await;
        let next_text = store
            .append(NewNode {
                parent: text.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("queued all-mode node".to_owned()),
            })
            .unwrap();
        store.set_branch_head("main", &text, &next_text).unwrap();

        let stale = cache.snapshot_or_placeholder(GraphMode::All);
        let statuses = cache.rebuild_statuses();

        assert_eq!(stale.version, initial.version);
        assert!(
            statuses.iter().any(|status| {
                status.mode == GraphMode::All && status.state == ConsoleGraphRebuildState::Scheduled
            }),
            "stale requested mode should publish a queued rebuild status"
        );

        let refreshed = cache.current_snapshot(GraphMode::All).await;

        assert!(refreshed.version > initial.version);
        assert!(refreshed.nodes.iter().any(|node| node.id == next_text));
        assert!(cache.rebuild_statuses().iter().any(|status| {
            status.mode == GraphMode::All && status.state == ConsoleGraphRebuildState::Ready
        }));
    }

    #[test]
    fn failed_source_version_does_not_need_rebuild() {
        let slot = CacheSlot {
            failure: Some(CacheFailure {
                source_version: 7,
                message: Arc::from("failed"),
            }),
            ..CacheSlot::default()
        };

        assert!(!slot.needs_rebuild(7));
    }

    #[test]
    fn newer_source_version_needs_rebuild_after_failure() {
        let slot = CacheSlot {
            failure: Some(CacheFailure {
                source_version: 7,
                message: Arc::from("failed"),
            }),
            ..CacheSlot::default()
        };

        assert!(slot.needs_rebuild(8));
    }
}
