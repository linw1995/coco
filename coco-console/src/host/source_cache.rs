use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use coco_mem::{
    BranchAppendSessionState, BranchStore, GRAPH_READ_BATCH_SIZE, GraphBranchRecord, Kind, NewNode,
    NewNodeContent, Node, NodeStore, SessionAnchorPatch, SessionState, SessionStore,
    SqliteGraphStore, StoreError, StoreResult,
};

#[derive(Debug, Default)]
pub(crate) struct PersistentGraphIndex {
    nodes: HashMap<String, CachedNode>,
    branches: HashMap<String, BranchContribution>,
    #[cfg(test)]
    refresh_count: usize,
}

#[derive(Debug)]
struct CachedNode {
    node: Node,
    references: usize,
}

#[derive(Debug)]
struct BranchContribution {
    record: GraphBranchRecord,
    node_ids: BTreeSet<String>,
}

#[derive(Clone)]
pub(crate) struct InMemoryGraphStore {
    root_id: String,
    nodes: Arc<HashMap<String, Node>>,
    branches: Arc<HashMap<String, GraphBranchRecord>>,
    children: Arc<HashMap<String, Vec<String>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TraversalKind {
    Graph,
    SkillSubtree,
}

impl PersistentGraphIndex {
    pub(crate) fn is_empty(&self) -> bool {
        self.branches.is_empty()
    }

    pub(crate) async fn refresh_all(&mut self, store: &SqliteGraphStore) -> StoreResult<()> {
        #[cfg(test)]
        {
            self.refresh_count += 1;
        }
        let records = store.graph_branches().await?;
        let current_names = records
            .iter()
            .map(|record| record.name.clone())
            .collect::<HashSet<_>>();
        let removed = self
            .branches
            .keys()
            .filter(|name| !current_names.contains(*name))
            .cloned()
            .collect::<Vec<_>>();
        for name in removed {
            self.remove_branch(&name);
        }
        for record in records {
            self.refresh_branch(store, record).await?;
        }
        Ok(())
    }

    pub(crate) async fn refresh_branches(
        &mut self,
        store: &SqliteGraphStore,
        names: &BTreeSet<String>,
    ) -> StoreResult<()> {
        #[cfg(test)]
        {
            self.refresh_count += 1;
        }
        for batch in names
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .chunks(GRAPH_READ_BATCH_SIZE)
        {
            let records = store.graph_branches_by_names(batch).await?;
            let found = records
                .iter()
                .map(|record| record.name.clone())
                .collect::<HashSet<_>>();
            for name in batch.iter().filter(|name| !found.contains(*name)) {
                self.remove_branch(name);
            }
            for record in records {
                self.refresh_branch(store, record).await?;
            }
        }
        Ok(())
    }

    async fn refresh_branch(
        &mut self,
        store: &SqliteGraphStore,
        record: GraphBranchRecord,
    ) -> StoreResult<()> {
        let node_ids = self.load_branch_nodes(store, &record.head_id).await?;
        if let Some(previous) = self.branches.remove(&record.name) {
            self.release_nodes(&previous.node_ids);
        }
        for node_id in &node_ids {
            self.nodes
                .get_mut(node_id)
                .expect("loaded graph node should be cached")
                .references += 1;
        }
        self.branches
            .insert(record.name.clone(), BranchContribution { record, node_ids });
        self.prune_unreferenced_nodes();
        Ok(())
    }

    async fn load_branch_nodes(
        &mut self,
        store: &SqliteGraphStore,
        head_id: &str,
    ) -> StoreResult<BTreeSet<String>> {
        let mut pending = vec![(head_id.to_owned(), TraversalKind::Graph)];
        let mut visited = HashSet::<(String, TraversalKind)>::new();
        let mut contribution = BTreeSet::new();

        while !pending.is_empty() {
            let mut batch = Vec::new();
            while batch.len() < GRAPH_READ_BATCH_SIZE {
                let Some(item) = pending.pop() else {
                    break;
                };
                if !item.0.is_empty() && visited.insert(item.clone()) {
                    batch.push(item);
                }
            }
            if batch.is_empty() {
                continue;
            }

            let ids = batch
                .iter()
                .map(|(node_id, _)| node_id.clone())
                .collect::<Vec<_>>();
            self.ensure_nodes(store, &ids).await?;

            let child_parents = batch
                .iter()
                .filter_map(|(node_id, traversal)| {
                    let node = &self.nodes.get(node_id)?.node;
                    ((*traversal == TraversalKind::SkillSubtree)
                        || node.kind.as_tool_uses().is_some())
                    .then_some(node_id.clone())
                })
                .collect::<Vec<_>>();
            let children = self.load_children(store, &child_parents).await?;
            let child_ids = children.values().flatten().cloned().collect::<Vec<_>>();
            self.ensure_nodes(store, &child_ids).await?;

            for (node_id, traversal) in batch {
                let node = self
                    .nodes
                    .get(&node_id)
                    .expect("loaded graph node should be cached");
                contribution.insert(node_id.clone());
                if !node.node.parent.is_empty() {
                    pending.push((node.node.parent.clone(), TraversalKind::Graph));
                }
                if let Kind::Anchor(anchor) = &node.node.kind {
                    pending.extend(
                        anchor
                            .merge_parents()
                            .iter()
                            .map(|parent| (parent.node_id().to_owned(), TraversalKind::Graph)),
                    );
                }

                let direct_children = children.get(&node_id).into_iter().flatten();
                match traversal {
                    TraversalKind::SkillSubtree => pending.extend(
                        direct_children
                            .cloned()
                            .map(|child_id| (child_id, TraversalKind::SkillSubtree)),
                    ),
                    TraversalKind::Graph if node.node.kind.as_tool_uses().is_some() => {
                        for child_id in direct_children {
                            let child = &self
                                .nodes
                                .get(child_id)
                                .expect("loaded child node should be cached")
                                .node;
                            if matches!(
                                &child.kind,
                                Kind::Anchor(anchor) if anchor.as_skill_invocation().is_some()
                            ) {
                                pending.push((child_id.clone(), TraversalKind::SkillSubtree));
                            }
                        }
                    }
                    TraversalKind::Graph => {}
                }
            }
        }

        Ok(contribution)
    }

    async fn ensure_nodes(&mut self, store: &SqliteGraphStore, ids: &[String]) -> StoreResult<()> {
        let missing = ids
            .iter()
            .filter(|node_id| !self.nodes.contains_key(*node_id))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        for batch in missing.chunks(GRAPH_READ_BATCH_SIZE) {
            let nodes = store.graph_nodes_by_ids(batch).await?;
            tokio::task::yield_now().await;
            for node in nodes {
                self.nodes.insert(
                    node.id.clone(),
                    CachedNode {
                        node,
                        references: 0,
                    },
                );
            }
        }
        Ok(())
    }

    async fn load_children(
        &self,
        store: &SqliteGraphStore,
        parent_ids: &[String],
    ) -> StoreResult<HashMap<String, Vec<String>>> {
        let mut children = HashMap::new();
        for batch in parent_ids.chunks(GRAPH_READ_BATCH_SIZE) {
            let batch_children = store.graph_child_ids(batch).await?;
            tokio::task::yield_now().await;
            children.extend(batch_children);
        }
        Ok(children)
    }

    fn remove_branch(&mut self, name: &str) {
        if let Some(previous) = self.branches.remove(name) {
            self.release_nodes(&previous.node_ids);
            self.prune_unreferenced_nodes();
        }
    }

    fn release_nodes(&mut self, node_ids: &BTreeSet<String>) {
        for node_id in node_ids {
            let cached = self
                .nodes
                .get_mut(node_id)
                .expect("referenced graph node should be cached");
            cached.references = cached.references.saturating_sub(1);
        }
    }

    fn prune_unreferenced_nodes(&mut self) {
        self.nodes.retain(|_, node| node.references > 0);
    }

    pub(crate) fn snapshot_store(&self, root_id: String) -> InMemoryGraphStore {
        let nodes = self
            .nodes
            .iter()
            .filter(|(_, cached)| cached.references > 0)
            .map(|(id, cached)| (id.clone(), cached.node.clone()))
            .collect::<HashMap<_, _>>();
        let branches = self
            .branches
            .iter()
            .map(|(name, contribution)| (name.clone(), contribution.record.clone()))
            .collect::<HashMap<_, _>>();
        let mut children = HashMap::<String, Vec<String>>::new();
        for node in nodes.values() {
            if !node.parent.is_empty() {
                children
                    .entry(node.parent.clone())
                    .or_default()
                    .push(node.id.clone());
            }
            if let Kind::Anchor(anchor) = &node.kind {
                for parent in anchor.merge_parents() {
                    children
                        .entry(parent.node_id().to_owned())
                        .or_default()
                        .push(node.id.clone());
                }
            }
        }
        for child_ids in children.values_mut() {
            child_ids.sort();
            child_ids.dedup();
        }
        InMemoryGraphStore {
            root_id,
            nodes: Arc::new(nodes),
            branches: Arc::new(branches),
            children: Arc::new(children),
        }
    }

    #[cfg(test)]
    pub(crate) fn refresh_count(&self) -> usize {
        self.refresh_count
    }

    #[cfg(test)]
    pub(crate) fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

impl InMemoryGraphStore {
    fn read_only<T>() -> StoreResult<T> {
        Err(StoreError::StoreReadOnly {
            path: PathBuf::from("console graph cache"),
        })
    }

    fn resolve_node(&self, reference: &str) -> StoreResult<Node> {
        let id = self
            .branches
            .get(reference)
            .map(|branch| branch.head_id.as_str())
            .unwrap_or(reference);
        self.nodes
            .get(id)
            .cloned()
            .ok_or_else(|| StoreError::NotFound { id: id.to_owned() })
    }
}

#[async_trait]
impl NodeStore for InMemoryGraphStore {
    fn root_id(&self) -> String {
        self.root_id.clone()
    }

    async fn append(&self, _node: NewNode) -> StoreResult<String> {
        Self::read_only()
    }

    async fn ancestry(&self, head_ref: &str) -> StoreResult<Vec<Node>> {
        let mut node = self.resolve_node(head_ref)?;
        let mut ancestry = Vec::new();
        let mut seen = HashSet::new();
        loop {
            if !seen.insert(node.id.clone()) {
                return Err(StoreError::CorruptedStore {
                    path: PathBuf::from("console graph cache"),
                    message: "in-memory graph contains cyclic parents".to_owned(),
                });
            }
            let is_root = node.is_root();
            let parent = node.parent.clone();
            ancestry.push(node);
            if is_root {
                break;
            }
            node = self
                .nodes
                .get(&parent)
                .cloned()
                .ok_or_else(|| StoreError::ParentNotFound { id: parent })?;
        }
        Ok(ancestry)
    }

    async fn log(&self, base_ref: &str, head_ref: &str) -> StoreResult<Vec<Node>> {
        let base = self.resolve_node(base_ref)?.id;
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
        self.resolve_node(id)
    }

    async fn list_children(&self, node_id: &str) -> StoreResult<Vec<Node>> {
        self.resolve_node(node_id)?;
        self.children
            .get(node_id)
            .into_iter()
            .flatten()
            .map(|child_id| self.resolve_node(child_id))
            .collect()
    }
}

#[async_trait]
impl BranchStore for InMemoryGraphStore {
    async fn fork(&self, _name: &str, _from_ref: &str) -> StoreResult<String> {
        Self::read_only()
    }

    async fn get_branch_head(&self, name: &str) -> StoreResult<String> {
        self.branches
            .get(name)
            .map(|branch| branch.head_id.clone())
            .ok_or_else(|| StoreError::BranchNotFound {
                name: name.to_owned(),
            })
    }

    async fn delete_branch(&self, _name: &str) -> StoreResult<()> {
        Self::read_only()
    }

    async fn set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _new_head: &str,
    ) -> StoreResult<()> {
        Self::read_only()
    }

    async fn append_nodes_and_set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _parent: &str,
        _nodes: Vec<NewNodeContent>,
    ) -> StoreResult<String> {
        Self::read_only()
    }

    async fn append_nodes_and_set_branch_head_to(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _parent: &str,
        _new_head: &str,
        _nodes: Vec<NewNodeContent>,
    ) -> StoreResult<String> {
        Self::read_only()
    }

    async fn append_nodes_and_set_branch_head_with_session_state(
        &self,
        _update: BranchAppendSessionState,
    ) -> StoreResult<String> {
        Self::read_only()
    }
}

#[async_trait]
impl SessionStore for InMemoryGraphStore {
    async fn list_session_states(&self) -> StoreResult<HashMap<String, SessionState>> {
        Ok(self
            .branches
            .iter()
            .map(|(name, branch)| (name.clone(), branch.state.clone()))
            .collect())
    }

    async fn get_session_state(&self, name: &str) -> StoreResult<SessionState> {
        self.branches
            .get(name)
            .map(|branch| branch.state.clone())
            .ok_or_else(|| StoreError::BranchNotFound {
                name: name.to_owned(),
            })
    }

    async fn set_session_state(
        &self,
        _name: &str,
        _expected: Option<&SessionState>,
        _next: SessionState,
    ) -> StoreResult<SessionState> {
        Self::read_only()
    }

    async fn rebase_session(
        &self,
        _name: &str,
        _patch: &SessionAnchorPatch,
    ) -> StoreResult<String> {
        Self::read_only()
    }

    async fn handoff_session(
        &self,
        _name: &str,
        _patch: &SessionAnchorPatch,
        _prompt: &str,
    ) -> StoreResult<String> {
        Self::read_only()
    }
}
