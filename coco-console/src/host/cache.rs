use std::path::PathBuf;
use std::sync::Arc;

use crate::graph::{GraphMode, GraphSnapshot, build_graph_snapshot_with_mode_and_progress};
use crate::publisher::ConsolePublisher;
use coco_mem::{PersistentStore, Store};
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
    compute_permits: Arc<Semaphore>,
}

#[derive(Clone)]
enum ConsoleGraphSource<S> {
    Store(S),
    PersistentStorePath(PathBuf),
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
            compute_permits: Arc::new(Semaphore::new(1)),
        }
    }

    pub fn new_with_persistent_store_path(
        _store: S,
        invalidations: ConsolePublisher,
        path: PathBuf,
    ) -> Self {
        Self {
            source: ConsoleGraphSource::PersistentStorePath(path),
            invalidations,
            ready: ConsolePublisher::new(),
            compute_permits: Arc::new(Semaphore::new(1)),
        }
    }

    pub fn current_version(&self) -> u64 {
        self.ready.current_version()
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.ready.subscribe()
    }

    pub fn subscribe_progress(&self) -> tokio::sync::watch::Receiver<u64> {
        self.ready.subscribe()
    }

    pub fn rebuild_statuses(&self) -> Vec<ConsoleGraphRebuildStatus> {
        Vec::new()
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
        self.publish_current_source_version();
    }

    pub async fn snapshot_current(&self, mode: GraphMode) -> crate::Result<Arc<GraphSnapshot>> {
        let source_version = self.invalidations.current_version();
        let graph_version = self.publish_source_version(source_version);
        let source = self.source.clone();
        let snapshot = self
            .run_blocking_graph_compute(move || source.build_snapshot(mode, graph_version))
            .await??;
        Ok(Arc::new(snapshot))
    }

    pub async fn snapshot_after(
        &self,
        mode: GraphMode,
        observed_version: u64,
    ) -> crate::Result<Arc<GraphSnapshot>> {
        loop {
            let mut rx = self.ready.subscribe();
            let mut invalidations = self.invalidations.subscribe();
            let snapshot = self.snapshot_current(mode).await?;
            if snapshot.version > observed_version {
                return Ok(snapshot);
            }
            tokio::select! {
                changed = rx.changed() => {
                    if changed.is_err() {
                        return self.snapshot_current(mode).await;
                    }
                }
                changed = invalidations.changed() => {
                    if changed.is_err() {
                        return self.snapshot_current(mode).await;
                    }
                    self.publish_current_source_version();
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

    fn publish_current_source_version(&self) -> u64 {
        self.publish_source_version(self.invalidations.current_version())
    }

    fn publish_source_version(&self, source_version: u64) -> u64 {
        let target = source_version;
        while self.ready.current_version() < target {
            self.ready.mark_changed();
        }
        self.ready.current_version()
    }
}

impl<S> ConsoleGraphSource<S>
where
    S: Store + Clone + Send + Sync + 'static,
{
    fn build_snapshot(self, mode: GraphMode, version: u64) -> crate::Result<GraphSnapshot> {
        match self {
            Self::Store(store) => {
                build_graph_snapshot_with_mode_and_progress(&store, version, mode, |_| {})
            }
            Self::PersistentStorePath(path) => {
                let store = PersistentStore::open_read_only_or_migrate_fs(&path)
                    .context(crate::error::StoreSnafu)?;
                build_graph_snapshot_with_mode_and_progress(&store, version, mode, |_| {})
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ConsoleStore;
    use coco_mem::{
        Anchor, BranchStore, Kind, MemoryStore, NewNode, NodeStore, PersistentStore, Role,
        SessionAnchor, SessionRole, Tool,
    };
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
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
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("coco-console-graph-{nanos}"));
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
    async fn cache_reopens_persistent_store_path_for_latest_graph_state() {
        let path = temp_store_path();
        let writer = PersistentStore::open_or_migrate_fs(&path).unwrap();
        let publisher = ConsolePublisher::new();
        let cache = ConsoleGraphCache::new_with_persistent_store_path(
            MemoryStore::new(),
            publisher.clone(),
            path.clone(),
        );
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

        let snapshot = cache.current_snapshot(GraphMode::All).await;

        assert!(snapshot.nodes.iter().any(|node| node.id == session));
        drop(writer);
        std::fs::remove_dir_all(path).unwrap();
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
