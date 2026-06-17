use std::sync::{Arc, Mutex};

use crate::graph::{GraphMode, GraphSnapshot, build_graph_snapshot_with_mode};
use crate::publisher::ConsolePublisher;
use coco_mem::Store;

#[derive(Clone)]
pub struct ConsoleGraphCache<S> {
    store: S,
    invalidations: ConsolePublisher,
    ready: ConsolePublisher,
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
    building_source_version: Option<u64>,
    failure: Option<CacheFailure>,
    requested: bool,
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
            state: Arc::new(Mutex::new(CacheState::default())),
        }
    }

    pub fn current_version(&self) -> u64 {
        self.ready.current_version()
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.ready.subscribe()
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
        let should_spawn = {
            let mut state = self
                .state
                .lock()
                .expect("console graph cache lock poisoned");
            let slot = state.slot_mut(mode);
            if mark_requested {
                slot.requested = true;
            }
            if !slot.requested || !slot.needs_rebuild(source_version) {
                false
            } else {
                slot.building_source_version = Some(source_version);
                true
            }
        };

        if should_spawn {
            self.spawn_rebuild(mode, source_version);
        }
    }

    fn spawn_rebuild(&self, mode: GraphMode, source_version: u64) {
        let cache = self.clone();
        tokio::spawn(async move {
            let store = cache.store.clone();
            let result = match tokio::task::spawn_blocking(move || {
                build_graph_snapshot_with_mode(&store, 0, mode)
            })
            .await
            {
                Ok(result) => result,
                Err(source) => Err(crate::Error::JoinConsoleServer { source }),
            };
            cache.finish_rebuild(mode, source_version, result);
        });
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
            let published_version = self.ready.mark_changed();
            debug_assert_eq!(published_version, graph_version);
            slot.requested && slot.needs_rebuild(self.invalidations.current_version())
        };

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
                self.ready.mark_changed();
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
            && self.building_source_version != Some(source_version)
            && self
                .failure
                .as_ref()
                .is_none_or(|failure| failure.source_version != source_version)
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
