use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use super::snapshot_store::ConsoleGraphSnapshotStore;
use crate::api::{GraphViewportDiffResponse, GraphViewportResponse};
use crate::graph::{GraphMode, GraphSnapshot, build_graph_snapshot_with_mode_and_progress};
use crate::host::api::{GraphViewportDiffRequest, GraphViewportRequest};
use crate::layout::{layout_graph_viewport, layout_graph_viewport_diff};
use crate::publisher::ConsolePublisher;
use coco_mem::{SqliteGraphStore, Store};
use serde::Serialize;
use snafu::prelude::*;
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
    compute_permits: Arc<Semaphore>,
    publish_lock: Arc<Mutex<()>>,
    state: Arc<Mutex<CacheState>>,
}

#[derive(Clone)]
enum ConsoleGraphSource<S> {
    Store(S),
    PersistentStorePath(PathBuf),
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

impl<S> ConsoleGraphCache<S>
where
    S: Store + Clone + Send + Sync + 'static,
{
    pub fn new(store: S, invalidations: ConsolePublisher) -> Self {
        Self {
            source: ConsoleGraphSource::Store(store),
            invalidations,
            ready: ConsolePublisher::new(),
            progress: ConsolePublisher::new(),
            snapshots: None,
            compute_permits: Arc::new(Semaphore::new(1)),
            publish_lock: Arc::new(Mutex::new(())),
            state: Arc::new(Mutex::new(CacheState::default())),
        }
    }

    pub fn new_with_persistent_store_path(
        _store: S,
        invalidations: ConsolePublisher,
        path: PathBuf,
    ) -> crate::Result<Self> {
        SqliteGraphStore::open_read_only(&path).context(crate::error::StoreSnafu)?;
        let snapshots = ConsoleGraphSnapshotStore::open(&path)?;
        Ok(Self {
            source: ConsoleGraphSource::PersistentStorePath(path),
            invalidations,
            ready: ConsolePublisher::new(),
            progress: ConsolePublisher::new(),
            snapshots: Some(snapshots),
            compute_permits: Arc::new(Semaphore::new(1)),
            publish_lock: Arc::new(Mutex::new(())),
            state: Arc::new(Mutex::new(CacheState::default())),
        })
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
        [state.anchors_rebuild.clone(), state.all_rebuild.clone()]
            .into_iter()
            .flatten()
            .collect()
    }

    pub fn subscribe_invalidations(&self) -> tokio::sync::watch::Receiver<u64> {
        self.invalidations.subscribe()
    }

    pub async fn run_blocking_graph_compute<T, F>(&self, compute: F) -> crate::Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.run_blocking_graph_compute_with(|| (), |_| compute())
            .await
    }

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
            self.ensure_snapshot_current(mode);
        }
    }

    pub async fn snapshot_current(&self, mode: GraphMode) -> crate::Result<Arc<GraphSnapshot>> {
        let source_version = self.invalidations.current_version();
        if let Some(snapshot) = self.cached_snapshot(mode, source_version) {
            return Ok(snapshot);
        }
        let graph_version = source_version;
        let source = self.source.clone();
        self.set_rebuild_status(rebuild_status(
            mode,
            source_version,
            ConsoleGraphRebuildState::Building,
            None,
            0,
            0,
            "Building graph snapshot",
        ));
        let progress_cache = self.clone();
        let snapshot = self
            .run_blocking_graph_compute(move || {
                source.build_snapshot_with_progress(mode, graph_version, |progress| {
                    progress_cache.set_rebuild_status(rebuild_status(
                        mode,
                        source_version,
                        ConsoleGraphRebuildState::Building,
                        Some(progress.phase),
                        progress.processed,
                        progress.total,
                        progress.phase.label(),
                    ));
                })
            })
            .await??;
        let snapshot = Arc::new(snapshot);
        self.store_cached_snapshot(mode, source_version, snapshot.clone())?;
        self.publish_ready_version(source_version);
        self.set_rebuild_status(rebuild_status(
            mode,
            source_version,
            ConsoleGraphRebuildState::Ready,
            None,
            1,
            1,
            "Graph snapshot ready",
        ));
        Ok(snapshot)
    }

    pub fn snapshot_current_ready_or_schedule(
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

    pub async fn snapshot_after(
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
                    self.ensure_snapshot_current(mode);
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
            self.ensure_snapshot_current(mode);
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
            if let Some(response) = self.viewport_current_ready_or_schedule(mode, request)
                && response.version > observed_version
            {
                return Ok(response);
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
                    self.ensure_snapshot_current(mode);
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
            self.ensure_snapshot_current(mode);
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
            if let Some(response) =
                self.viewport_diff_current_ready_or_schedule(mode, request.clone())
                && response.version > observed_version
            {
                return Ok(response);
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
                    self.ensure_snapshot_current(mode);
                }
            }
        }
    }

    pub async fn snapshot_for_current_source(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Arc<GraphSnapshot>> {
        self.snapshot_current(mode).await
    }

    #[cfg(test)]
    pub async fn current_snapshot(&self, mode: GraphMode) -> Arc<GraphSnapshot> {
        self.snapshot_for_current_source(mode)
            .await
            .expect("graph snapshot should build")
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
    ) -> crate::Result<()> {
        if let Some(snapshots) = &self.snapshots {
            snapshots.put(source_version, &snapshot)?;
        }
        let mut state = self
            .state
            .lock()
            .expect("console graph cache lock poisoned");
        *state.slot_mut(mode) = Some(CachedGraphSnapshot {
            source_version,
            snapshot,
        });
        Ok(())
    }

    fn ensure_snapshot_current(&self, mode: GraphMode) {
        let source_version = self.invalidations.current_version();
        if self.cached_snapshot(mode, source_version).is_some() {
            self.set_rebuild_status(rebuild_status(
                mode,
                source_version,
                ConsoleGraphRebuildState::Ready,
                None,
                1,
                1,
                "Graph snapshot ready",
            ));
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
        self.set_rebuild_status(rebuild_status(
            mode,
            source_version,
            ConsoleGraphRebuildState::Building,
            None,
            0,
            0,
            "Building graph snapshot",
        ));
        let source = self.source.clone();
        let progress_cache = self.clone();
        let result = self
            .run_blocking_graph_compute(move || {
                source.build_snapshot_with_progress(mode, source_version, |progress| {
                    progress_cache.set_rebuild_status(rebuild_status(
                        mode,
                        source_version,
                        ConsoleGraphRebuildState::Building,
                        Some(progress.phase),
                        progress.processed,
                        progress.total,
                        progress.phase.label(),
                    ));
                })
            })
            .await;
        match result {
            Ok(Ok(snapshot)) => {
                match self.store_cached_snapshot(mode, source_version, Arc::new(snapshot)) {
                    Ok(()) => {
                        self.publish_ready_version(source_version);
                        self.set_rebuild_status(rebuild_status(
                            mode,
                            source_version,
                            ConsoleGraphRebuildState::Ready,
                            None,
                            1,
                            1,
                            "Graph snapshot ready",
                        ));
                    }
                    Err(error) => self.set_rebuild_status(rebuild_status(
                        mode,
                        source_version,
                        ConsoleGraphRebuildState::Failed,
                        None,
                        0,
                        0,
                        error.to_string(),
                    )),
                }
            }
            Ok(Err(error)) => self.set_rebuild_status(rebuild_status(
                mode,
                source_version,
                ConsoleGraphRebuildState::Failed,
                None,
                0,
                0,
                error.to_string(),
            )),
            Err(error) => self.set_rebuild_status(rebuild_status(
                mode,
                source_version,
                ConsoleGraphRebuildState::Failed,
                None,
                0,
                0,
                error.to_string(),
            )),
        }
    }

    fn mark_rebuild_scheduled(&self, mode: GraphMode, source_version: u64) -> bool {
        let mut state = self
            .state
            .lock()
            .expect("console graph cache lock poisoned");
        if state
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
                    ConsoleGraphRebuildState::Scheduled | ConsoleGraphRebuildState::Building
                )
        }) {
            return false;
        }
        *state.rebuild_slot_mut(mode) = Some(rebuild_status(
            mode,
            source_version,
            ConsoleGraphRebuildState::Scheduled,
            None,
            0,
            0,
            "Graph snapshot scheduled",
        ));
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

impl<S> ConsoleGraphSource<S>
where
    S: Store + Clone + Send + Sync + 'static,
{
    fn build_snapshot_with_progress<F>(
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
                build_graph_snapshot_with_mode_and_progress(&store, version, mode, progress)
            }
            Self::PersistentStorePath(path) => {
                let store =
                    SqliteGraphStore::open_read_only(&path).context(crate::error::StoreSnafu)?;
                build_graph_snapshot_with_mode_and_progress(&store, version, mode, progress)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ConsoleStore;
    use crate::host::snapshot_store::ConsoleGraphSnapshotStore;
    use coco_mem::{
        Anchor, BranchStore, Kind, MemoryStore, NewNode, NodeStore, PersistentStore, Role,
        SessionAnchor, SessionRole, Tool,
    };
    use diesel::QueryableByName;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::time::{Duration, sleep};

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

    fn temp_store_path() -> PathBuf {
        static TEMP_STORE_COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEMP_STORE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("coco-console-graph-{nanos}-{counter}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn cache_builds_snapshots_per_mode_on_demand() {
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
    async fn cache_reads_latest_store_state_without_background_rebuild() {
        let publisher = ConsolePublisher::new();
        let (store, _, text) = graph_store(publisher.clone());
        let cache = ConsoleGraphCache::new(store.clone(), publisher.clone());

        let initial = cache.current_snapshot(GraphMode::All).await;
        let next_text = store
            .append(NewNode {
                parent: text.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("new all-mode node".to_owned()),
            })
            .unwrap();
        store.set_branch_head("main", &text, &next_text).unwrap();

        let refreshed = cache.current_snapshot(GraphMode::All).await;

        assert!(refreshed.version > initial.version);
        assert!(refreshed.nodes.iter().any(|node| node.id == next_text));
    }

    #[tokio::test]
    async fn cache_reuses_snapshot_for_same_source_version() {
        let publisher = ConsolePublisher::new();
        let (store, _, text) = graph_store(publisher.clone());
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
            .unwrap();
        store.set_branch_head("main", &text, &next_text).unwrap();
        let third = cache.current_snapshot(GraphMode::All).await;

        assert!(!Arc::ptr_eq(&first, &third));
        assert!(third.nodes.iter().any(|node| node.id == next_text));
    }

    #[tokio::test]
    async fn cache_publishes_ready_version_after_snapshot_build() {
        let publisher = ConsolePublisher::new();
        let (store, _, _) = graph_store(publisher.clone());
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
    async fn cache_schedules_snapshot_without_blocking_ready_reader() {
        let publisher = ConsolePublisher::new();
        let (store, _, _) = graph_store(publisher.clone());
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
        let writer = PersistentStore::open_or_migrate_fs(&path).unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            MemoryStore::new(),
            publisher.clone(),
            path.clone(),
        )
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .unwrap();
        writer.fork("main", &session).unwrap();
        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("visible child".to_owned()),
            })
            .unwrap();
        writer.set_branch_head("main", &session, &text).unwrap();
        publisher.mark_changed();

        let snapshot = cache.current_snapshot(GraphMode::All).await;

        assert!(snapshot.nodes.iter().any(|node| node.id == session));
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn cache_persists_graph_locations_to_sqlite_store_database() {
        let path = temp_store_path();
        let writer = PersistentStore::open_or_migrate_fs(&path).unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            MemoryStore::new(),
            publisher.clone(),
            path.clone(),
        )
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .unwrap();
        writer.fork("main", &session).unwrap();
        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("visible child".to_owned()),
            })
            .unwrap();
        writer.set_branch_head("main", &session, &text).unwrap();
        publisher.mark_changed();

        let snapshot = cache.current_snapshot(GraphMode::All).await;
        let materialized = ConsoleGraphSnapshotStore::open(&path)
            .unwrap()
            .latest_viewport(
                GraphMode::All,
                crate::host::api::GraphViewportRequest::default(),
            )
            .unwrap()
            .unwrap();
        let database_path = path.join("store.sqlite3");
        assert_eq!(
            sqlite_table_row_count(&database_path, "console_graph_materializations"),
            1
        );
        assert!(sqlite_table_row_count(&database_path, "console_graph_node_locations") > 0);
        assert!(sqlite_table_row_count(&database_path, "console_graph_edge_routes") > 0);
        assert!(!sqlite_table_has_column(
            &database_path,
            "console_graph_node_locations",
            "source_version",
        ));
        assert!(!sqlite_table_has_column(
            &database_path,
            "console_graph_edge_routes",
            "source_version",
        ));
        assert!(!sqlite_table_exists(
            &database_path,
            "console_graph_snapshots"
        ));
        let reopened_cache = ConsoleGraphCache::new_with_persistent_store_path(
            MemoryStore::new(),
            ConsolePublisher::new(),
            path.clone(),
        )
        .unwrap();
        let reopened = reopened_cache.snapshot_current_ready_or_schedule(GraphMode::All);
        let reopened_materialized = reopened_cache
            .viewport_current_ready_or_schedule(
                GraphMode::All,
                crate::host::api::GraphViewportRequest::default(),
            )
            .unwrap();

        assert_eq!(materialized.version, snapshot.version);
        assert!(materialized.nodes.iter().any(|node| node.id == session));
        assert!(reopened.is_none());
        assert!(!path.join("console-graph-snapshots.sqlite3").exists());
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
    async fn cache_drops_legacy_snapshot_materialization_tables() {
        let path = temp_store_path();
        let writer = PersistentStore::open_or_migrate_fs(&path).unwrap();
        let database_path = path.join("store.sqlite3");
        create_legacy_snapshot_materialization_tables(&database_path);

        let publisher = ConsolePublisher::new();
        let _cache = ConsoleGraphCache::new_with_persistent_store_path(
            MemoryStore::new(),
            publisher,
            path.clone(),
        )
        .unwrap();

        for table in [
            "console_graph_snapshots",
            "console_graph_viewports",
            "console_graph_viewport_lanes",
            "console_graph_viewport_nodes",
            "console_graph_viewport_edges",
        ] {
            assert!(!sqlite_table_exists(&database_path, table));
        }

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[derive(QueryableByName)]
    struct SqliteCount {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }

    fn sqlite_table_exists(database_path: &std::path::Path, table: &str) -> bool {
        use diesel::prelude::*;
        use diesel::sql_query;
        use diesel::sql_types::Text;

        let database_url = database_path.to_string_lossy().into_owned();
        let mut connection = diesel::sqlite::SqliteConnection::establish(&database_url).unwrap();
        let row = sql_query(
            "SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind::<Text, _>(table)
        .get_result::<SqliteCount>(&mut connection)
        .unwrap();
        row.count > 0
    }

    fn sqlite_table_row_count(database_path: &std::path::Path, table: &str) -> i64 {
        use diesel::prelude::*;
        use diesel::sql_query;

        let database_url = database_path.to_string_lossy().into_owned();
        let mut connection = diesel::sqlite::SqliteConnection::establish(&database_url).unwrap();
        sql_query(format!("SELECT COUNT(*) AS count FROM {table}"))
            .get_result::<SqliteCount>(&mut connection)
            .unwrap()
            .count
    }

    fn sqlite_table_has_column(database_path: &std::path::Path, table: &str, column: &str) -> bool {
        use diesel::prelude::*;
        use diesel::sql_query;
        use diesel::sql_types::Text;

        let database_url = database_path.to_string_lossy().into_owned();
        let mut connection = diesel::sqlite::SqliteConnection::establish(&database_url).unwrap();
        let row = sql_query(format!(
            "SELECT COUNT(*) AS count FROM pragma_table_info('{table}') WHERE name = ?"
        ))
        .bind::<Text, _>(column)
        .get_result::<SqliteCount>(&mut connection)
        .unwrap();
        row.count > 0
    }

    fn create_legacy_snapshot_materialization_tables(database_path: &std::path::Path) {
        use diesel::connection::SimpleConnection;
        use diesel::prelude::*;

        let database_url = database_path.to_string_lossy().into_owned();
        let mut connection = diesel::sqlite::SqliteConnection::establish(&database_url).unwrap();
        connection
            .batch_execute(
                r#"
CREATE TABLE console_graph_snapshots (mode TEXT PRIMARY KEY NOT NULL);
CREATE TABLE console_graph_viewports (mode TEXT PRIMARY KEY NOT NULL);
CREATE TABLE console_graph_viewport_lanes (mode TEXT NOT NULL);
CREATE TABLE console_graph_viewport_nodes (mode TEXT NOT NULL);
CREATE TABLE console_graph_viewport_edges (mode TEXT NOT NULL);
"#,
            )
            .unwrap();
    }

    #[tokio::test]
    async fn cache_rolls_back_snapshot_store_refresh_when_materialization_fails() {
        let path = temp_store_path();
        let writer = PersistentStore::open_or_migrate_fs(&path).unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            MemoryStore::new(),
            publisher.clone(),
            path.clone(),
        )
        .unwrap();
        let root = writer.root_id();
        let session = writer
            .append(NewNode {
                parent: root,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(Vec::new(), session_anchor())),
            })
            .unwrap();
        writer.fork("main", &session).unwrap();
        publisher.mark_changed();
        let first = cache.current_snapshot(GraphMode::All).await;
        drop_materialized_nodes_table(&path);
        let text = writer
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("new node should roll back".to_owned()),
            })
            .unwrap();
        writer.set_branch_head("main", &session, &text).unwrap();
        publisher.mark_changed();

        let result = cache.snapshot_current(GraphMode::All).await;
        let materialized = ConsoleGraphSnapshotStore::open(&path)
            .unwrap()
            .latest_viewport(
                GraphMode::All,
                crate::host::api::GraphViewportRequest::default(),
            )
            .unwrap()
            .unwrap();

        assert!(result.is_err());
        assert_eq!(materialized.version, first.version);
        assert!(!materialized.nodes.iter().any(|node| node.id == text));

        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
    }

    fn drop_materialized_nodes_table(path: &std::path::Path) {
        use diesel::prelude::*;
        use diesel::sql_query;

        let database_path = path.join("store.sqlite3");
        let database_url = database_path.to_string_lossy().into_owned();
        let mut connection = diesel::sqlite::SqliteConnection::establish(&database_url).unwrap();
        sql_query("DROP TABLE console_graph_node_locations")
            .execute(&mut connection)
            .unwrap();
    }

    #[tokio::test]
    async fn graph_compute_permit_serializes_blocking_work() {
        let publisher = ConsolePublisher::new();
        let (store, _, _) = graph_store(publisher.clone());
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
