use std::collections::{BTreeSet, HashMap};

use coco_mem::{
    Anchor, AnchorPayload, BranchStore, Kind, ManyOrOne, MergeParent, Node, NodeStore, PauseReason,
    SessionAnchor, SessionState, SessionStore, ToolResult, ToolUse,
};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use crate::Result;
use crate::error::StoreSnafu;

const SUMMARY_LIMIT: usize = 140;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphMode {
    Anchors,
    All,
}

impl GraphMode {
    pub fn as_query_value(self) -> &'static str {
        match self {
            Self::Anchors => "anchors",
            Self::All => "all",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Anchors => "Anchors",
            Self::All => "All",
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct GraphSnapshot {
    pub version: u64,
    pub mode: GraphMode,
    pub root_id: String,
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
    pub branches: Vec<GraphBranch>,
    pub provider_contexts: Vec<GraphProviderContext>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphBuildPhase {
    Branches,
    ProviderContexts,
    Entries,
    Snapshot,
}

impl GraphBuildPhase {
    pub fn label(self) -> &'static str {
        match self {
            Self::Branches => "Collecting branches",
            Self::ProviderContexts => "Collecting provider contexts",
            Self::Entries => "Building graph entries",
            Self::Snapshot => "Finalizing snapshot",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphBuildProgress {
    pub phase: GraphBuildPhase,
    pub processed: usize,
    pub total: usize,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct GraphNode {
    pub id: String,
    pub short_id: String,
    pub kind: String,
    pub role: String,
    pub created_at: String,
    pub created_at_ns: i128,
    pub content: String,
    pub summary: String,
    pub labels: Vec<String>,
    pub provider_context_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphProviderContext {
    pub id: String,
    pub nodes: Vec<GraphProviderContextNode>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphProviderContextNode {
    pub id: String,
    pub short_id: String,
    pub kind: String,
    pub role: String,
    pub created_at: String,
    pub created_at_ns: i128,
    pub content: String,
    pub summary: String,
    pub visible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GraphProviderContextSelection {
    pub context: GraphProviderContext,
    pub selected_id: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphEdge {
    pub source: String,
    pub target: String,
    pub kind: GraphEdgeKind,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GraphEdgeKind {
    Primary,
    Merge,
    Shadow,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct GraphBranch {
    pub name: String,
    pub head_id: String,
    pub visible_head_id: Option<String>,
    pub state: SessionState,
}

#[derive(Debug, Clone)]
struct GraphBranchLabel {
    branch: String,
    state: SessionState,
}

#[derive(Debug, Clone)]
struct GraphNodeEntry {
    node: Node,
    primary_parent: Option<String>,
    merge_parents: Vec<MergeParent>,
    labels: Vec<GraphBranchLabel>,
    provider_context_ids: Vec<String>,
}

struct GraphBuildState {
    mode: GraphMode,
    visible_node_ids: BTreeSet<String>,
    visible_nodes: HashMap<String, Node>,
    visible_node_scopes: HashMap<String, BTreeSet<String>>,
    labels_by_node: HashMap<String, Vec<GraphBranchLabel>>,
    branches: Vec<GraphBranch>,
}

struct BranchGraphScope {
    node_ids: BTreeSet<String>,
}

struct VisibleGraphCollector<'a, S: NodeStore> {
    store: &'a S,
    mode: GraphMode,
    scope_node_ids: &'a mut BTreeSet<String>,
    visible_node_ids: &'a mut BTreeSet<String>,
    visible_nodes: &'a mut HashMap<String, Node>,
    branch_visible_node_ids: &'a mut BTreeSet<String>,
    pending: Vec<String>,
    visited: BTreeSet<String>,
}

#[cfg(test)]
pub fn build_graph_snapshot(
    store: &(impl BranchStore + NodeStore + SessionStore),
    version: u64,
) -> Result<GraphSnapshot> {
    build_graph_snapshot_with_mode(store, version, GraphMode::All)
}

pub fn build_graph_snapshot_with_mode(
    store: &(impl BranchStore + NodeStore + SessionStore),
    version: u64,
    mode: GraphMode,
) -> Result<GraphSnapshot> {
    build_graph_snapshot_with_mode_and_progress(store, version, mode, |_| {})
}

pub fn build_graph_snapshot_with_mode_and_progress<F>(
    store: &(impl BranchStore + NodeStore + SessionStore),
    version: u64,
    mode: GraphMode,
    mut progress: F,
) -> Result<GraphSnapshot>
where
    F: FnMut(GraphBuildProgress),
{
    let mut state = collect_graph_state(store, mode, &mut progress)?;
    let contexts = state.provider_contexts(store, &mut progress)?;
    let entries = sorted_graph_entries(&mut state, store, &contexts, &mut progress)?;
    let (nodes, edges) = graph_items_from_entries(entries);
    progress(GraphBuildProgress {
        phase: GraphBuildPhase::Snapshot,
        processed: 1,
        total: 1,
    });

    Ok(GraphSnapshot {
        version,
        mode,
        root_id: store.root_id(),
        nodes,
        edges,
        branches: state.branches,
        provider_contexts: contexts,
    })
}

fn collect_graph_state<F>(
    store: &(impl BranchStore + NodeStore + SessionStore),
    mode: GraphMode,
    progress: &mut F,
) -> Result<GraphBuildState>
where
    F: FnMut(GraphBuildProgress),
{
    let mut state = GraphBuildState::new(mode);
    let session_states = sorted_session_states(store)?;
    let total = session_states.len();
    (*progress)(GraphBuildProgress {
        phase: GraphBuildPhase::Branches,
        processed: 0,
        total,
    });
    for (index, (branch, session_state)) in session_states.into_iter().enumerate() {
        state.collect_branch(store, branch, session_state)?;
        (*progress)(GraphBuildProgress {
            phase: GraphBuildPhase::Branches,
            processed: index + 1,
            total,
        });
    }
    Ok(state)
}

fn sorted_graph_entries<F>(
    state: &mut GraphBuildState,
    store: &impl NodeStore,
    contexts: &[GraphProviderContext],
    progress: &mut F,
) -> Result<Vec<GraphNodeEntry>>
where
    F: FnMut(GraphBuildProgress),
{
    let mut entries = state.node_entries(store, contexts, progress)?;
    entries.sort_by(graph_entry_order);
    Ok(entries)
}

fn graph_entry_order(left: &GraphNodeEntry, right: &GraphNodeEntry) -> std::cmp::Ordering {
    left.node
        .created_at
        .to_string()
        .cmp(&right.node.created_at.to_string())
        .then_with(|| left.node.id.cmp(&right.node.id))
}

fn graph_items_from_entries(entries: Vec<GraphNodeEntry>) -> (Vec<GraphNode>, Vec<GraphEdge>) {
    let mut nodes = Vec::with_capacity(entries.len());
    let mut edges = Vec::new();
    for entry in entries {
        edges.extend(graph_edges_from_entry(&entry));
        nodes.push(graph_node_from_entry(entry));
    }
    (nodes, edges)
}

fn graph_edges_from_entry(entry: &GraphNodeEntry) -> Vec<GraphEdge> {
    let mut edges = primary_graph_edge(entry).into_iter().collect::<Vec<_>>();
    edges.extend(
        entry
            .merge_parents
            .iter()
            .map(|parent| merge_graph_edge(parent, &entry.node.id)),
    );
    edges
}

fn primary_graph_edge(entry: &GraphNodeEntry) -> Option<GraphEdge> {
    entry.primary_parent.as_ref().map(|parent| GraphEdge {
        source: parent.clone(),
        target: entry.node.id.clone(),
        kind: GraphEdgeKind::Primary,
    })
}

fn merge_graph_edge(parent: &MergeParent, target: &str) -> GraphEdge {
    GraphEdge {
        source: parent.node_id().to_owned(),
        target: target.to_owned(),
        kind: graph_merge_edge_kind(parent),
    }
}

fn graph_merge_edge_kind(parent: &MergeParent) -> GraphEdgeKind {
    if parent.is_shadow() {
        GraphEdgeKind::Shadow
    } else {
        GraphEdgeKind::Merge
    }
}

fn graph_node_from_entry(entry: GraphNodeEntry) -> GraphNode {
    graph_node_from_node(
        entry.node,
        render_graph_labels(&entry.labels),
        entry.provider_context_ids,
    )
}

pub(crate) fn graph_node_from_node(
    node: Node,
    labels: Vec<String>,
    provider_context_ids: Vec<String>,
) -> GraphNode {
    GraphNode {
        id: node.id.clone(),
        short_id: shorten_id(&node.id),
        kind: graph_kind_name(&node).to_owned(),
        role: format!("{:?}", node.role),
        created_at: node.created_at.to_string(),
        created_at_ns: node.created_at.as_nanosecond(),
        content: render_node_content(&node),
        summary: summarize_node(&node),
        labels,
        provider_context_ids,
    }
}

impl GraphBuildState {
    fn new(mode: GraphMode) -> Self {
        Self {
            mode,
            visible_node_ids: BTreeSet::new(),
            visible_nodes: HashMap::new(),
            visible_node_scopes: HashMap::new(),
            labels_by_node: HashMap::new(),
            branches: Vec::new(),
        }
    }

    fn collect_branch(
        &mut self,
        store: &(impl BranchStore + NodeStore),
        branch: String,
        state: SessionState,
    ) -> Result<()> {
        let head_id = store.get_branch_head(&branch).context(StoreSnafu)?;
        let scope = self.collect_branch_scope(store, &head_id)?;
        let visible_head_id = self.resolve_visible_parent(store, &scope.node_ids, &head_id)?;
        self.add_branch_label(&branch, &state, visible_head_id.as_deref());
        self.branches.push(GraphBranch {
            name: branch,
            head_id,
            visible_head_id,
            state,
        });
        Ok(())
    }

    fn collect_branch_scope(
        &mut self,
        store: &impl NodeStore,
        head_id: &str,
    ) -> Result<BranchGraphScope> {
        let mut scope_node_ids = initial_graph_scope(store, head_id, self.mode)?;
        let mut branch_visible_node_ids = BTreeSet::new();
        collect_visible_graph_nodes(
            store,
            head_id,
            &mut scope_node_ids,
            self.mode,
            &mut self.visible_node_ids,
            &mut self.visible_nodes,
            &mut branch_visible_node_ids,
        )?;
        self.merge_visible_node_scopes(&scope_node_ids, &branch_visible_node_ids);
        Ok(BranchGraphScope {
            node_ids: scope_node_ids,
        })
    }

    fn merge_visible_node_scopes(
        &mut self,
        scope_node_ids: &BTreeSet<String>,
        visible_node_ids: &BTreeSet<String>,
    ) {
        for node_id in visible_node_ids {
            self.visible_node_scopes
                .entry(node_id.clone())
                .or_default()
                .extend(scope_node_ids.iter().cloned());
        }
    }

    fn add_branch_label(
        &mut self,
        branch: &str,
        state: &SessionState,
        visible_head_id: Option<&str>,
    ) {
        let Some(label_node_id) = visible_head_id else {
            return;
        };
        self.labels_by_node
            .entry(label_node_id.to_owned())
            .or_default()
            .push(GraphBranchLabel {
                branch: branch.to_owned(),
                state: state.clone(),
            });
    }

    fn node_entries<F>(
        &mut self,
        store: &impl NodeStore,
        contexts: &[GraphProviderContext],
        progress: &mut F,
    ) -> Result<Vec<GraphNodeEntry>>
    where
        F: FnMut(GraphBuildProgress),
    {
        let context_ids_by_node = provider_context_ids_by_node(contexts, &self.visible_node_ids);
        let visible_nodes = std::mem::take(&mut self.visible_nodes);
        let total = visible_nodes.len();
        (*progress)(GraphBuildProgress {
            phase: GraphBuildPhase::Entries,
            processed: 0,
            total,
        });
        let mut entries = Vec::with_capacity(total);
        for (index, node) in visible_nodes.into_values().enumerate() {
            entries.push(self.node_entry(store, node, &context_ids_by_node)?);
            (*progress)(GraphBuildProgress {
                phase: GraphBuildPhase::Entries,
                processed: index + 1,
                total,
            });
        }
        Ok(entries)
    }

    fn node_entry(
        &mut self,
        store: &impl NodeStore,
        node: Node,
        context_ids_by_node: &HashMap<String, Vec<String>>,
    ) -> Result<GraphNodeEntry> {
        let scope_node_ids = self
            .visible_node_scopes
            .remove(&node.id)
            .unwrap_or_default();
        let primary_parent = self.resolve_visible_parent(store, &scope_node_ids, &node.parent)?;
        let merge_parents =
            self.visible_merge_parents(store, &node, &scope_node_ids, &primary_parent)?;
        let labels = self.labels_by_node.remove(&node.id).unwrap_or_default();
        let provider_context_ids = context_ids_by_node
            .get(&node.id)
            .cloned()
            .unwrap_or_default();
        Ok(GraphNodeEntry {
            node,
            primary_parent,
            merge_parents,
            labels,
            provider_context_ids,
        })
    }

    fn provider_contexts<F>(
        &self,
        store: &impl NodeStore,
        progress: &mut F,
    ) -> Result<Vec<GraphProviderContext>>
    where
        F: FnMut(GraphBuildProgress),
    {
        let mut contexts = HashMap::<String, GraphProviderContext>::new();
        let total = self.branches.len();
        progress(GraphBuildProgress {
            phase: GraphBuildPhase::ProviderContexts,
            processed: 0,
            total,
        });
        for (index, branch) in self.branches.iter().enumerate() {
            let ancestry = store.ancestry(&branch.head_id).context(StoreSnafu)?;
            for context_nodes in provider_contexts_from_head(ancestry) {
                self.insert_provider_context(&mut contexts, &branch.name, context_nodes);
            }
            progress(GraphBuildProgress {
                phase: GraphBuildPhase::ProviderContexts,
                processed: index + 1,
                total,
            });
        }

        let mut contexts = contexts.into_values().collect::<Vec<_>>();
        contexts.sort_by(provider_context_order);
        Ok(contexts)
    }

    fn insert_provider_context(
        &self,
        contexts: &mut HashMap<String, GraphProviderContext>,
        branch_name: &str,
        context: Vec<Node>,
    ) {
        if !context
            .iter()
            .any(|node| self.visible_node_ids.contains(&node.id))
        {
            return;
        }

        let context_nodes = context
            .iter()
            .map(|node| graph_provider_context_node(node, self.visible_node_ids.contains(&node.id)))
            .collect::<Vec<_>>();
        let Some(id) = provider_context_id(branch_name, &context) else {
            return;
        };
        contexts.entry(id.clone()).or_insert(GraphProviderContext {
            id,
            nodes: context_nodes,
        });
    }

    fn visible_merge_parents(
        &self,
        store: &impl NodeStore,
        node: &Node,
        scope_node_ids: &BTreeSet<String>,
        primary_parent: &Option<String>,
    ) -> Result<Vec<MergeParent>> {
        let Some(anchor) = node_anchor(node) else {
            return Ok(Vec::new());
        };

        let mut parents = Vec::new();
        for merge_parent in anchor.merge_parents() {
            self.push_visible_merge_parent(
                store,
                scope_node_ids,
                primary_parent,
                merge_parent,
                &mut parents,
            )?;
        }
        Ok(parents)
    }

    fn push_visible_merge_parent(
        &self,
        store: &impl NodeStore,
        scope_node_ids: &BTreeSet<String>,
        primary_parent: &Option<String>,
        merge_parent: &MergeParent,
        parents: &mut Vec<MergeParent>,
    ) -> Result<()> {
        let Some(parent_id) =
            self.resolve_visible_parent(store, scope_node_ids, merge_parent.node_id())?
        else {
            return Ok(());
        };
        if is_duplicate_visible_merge_parent(primary_parent, parents, &parent_id) {
            return Ok(());
        }
        parents.push(visible_merge_parent(merge_parent, parent_id));
        Ok(())
    }

    fn resolve_visible_parent(
        &self,
        store: &impl NodeStore,
        scope_node_ids: &BTreeSet<String>,
        start_id: &str,
    ) -> Result<Option<String>> {
        resolve_visible_parent(store, &self.visible_node_ids, scope_node_ids, start_id)
    }
}

impl<S: NodeStore> VisibleGraphCollector<'_, S> {
    fn collect(&mut self) -> Result<()> {
        while let Some(node_id) = self.next_node_id() {
            self.visit(node_id)?;
        }
        Ok(())
    }

    fn next_node_id(&mut self) -> Option<String> {
        while let Some(node_id) = self.pending.pop() {
            if self.should_visit(&node_id) {
                return Some(node_id);
            }
        }
        None
    }

    fn should_visit(&mut self, node_id: &str) -> bool {
        !node_id.is_empty()
            && self.scope_node_ids.contains(node_id)
            && self.visited.insert(node_id.to_owned())
    }

    fn visit(&mut self, node_id: String) -> Result<()> {
        let node = self.store.get_node(&node_id).context(StoreSnafu)?;
        if node.is_root() {
            return Ok(());
        }
        self.enqueue_traversal_targets(&node)?;
        self.record_if_visible(node);
        Ok(())
    }

    fn enqueue_traversal_targets(&mut self, node: &Node) -> Result<()> {
        self.enqueue_primary_parent(node);
        self.enqueue_merge_parents(node);
        self.enqueue_skill_invocation_subtrees(node)
    }

    fn enqueue_primary_parent(&mut self, node: &Node) {
        if should_follow_primary_parent(node, self.mode) {
            self.push_node(node.parent.clone());
        }
    }

    fn enqueue_merge_parents(&mut self, node: &Node) {
        let Some(anchor) = node_anchor(node) else {
            return;
        };
        for merge_parent in anchor.merge_parents() {
            self.push_node(merge_parent.node_id().to_owned());
        }
    }

    fn enqueue_skill_invocation_subtrees(&mut self, node: &Node) -> Result<()> {
        if node.kind.as_tool_uses().is_none() {
            return Ok(());
        }
        collect_visible_skill_invocation_subtrees(
            self.store,
            &node.id,
            self.scope_node_ids,
            &mut self.pending,
        )
    }

    fn record_if_visible(&mut self, node: Node) {
        if !is_visible_graph_node(&node, self.mode) {
            return;
        }
        self.visible_node_ids.insert(node.id.clone());
        self.branch_visible_node_ids.insert(node.id.clone());
        self.visible_nodes.insert(node.id.clone(), node);
    }

    fn push_node(&mut self, node_id: String) {
        push_scoped_graph_node(self.scope_node_ids, &mut self.pending, node_id);
    }
}

fn sorted_session_states(store: &impl SessionStore) -> Result<Vec<(String, SessionState)>> {
    let mut branches = store
        .list_session_states()
        .context(StoreSnafu)?
        .into_iter()
        .collect::<Vec<_>>();
    branches.sort_by(|(left, _), (right, _)| left.cmp(right));
    Ok(branches)
}

pub(crate) fn provider_context_for_node(
    store: &(impl BranchStore + NodeStore + SessionStore),
    target_node_id: &str,
    context_id: Option<&str>,
) -> Result<Option<GraphProviderContextSelection>> {
    let mut contexts = HashMap::<String, GraphProviderContext>::new();
    for (branch_name, _) in sorted_session_states(store)? {
        let head_id = store.get_branch_head(&branch_name).context(StoreSnafu)?;
        let ancestry = store.ancestry(&head_id).context(StoreSnafu)?;
        for context_nodes in provider_contexts_from_head(ancestry) {
            let Some(id) = provider_context_id(&branch_name, &context_nodes) else {
                continue;
            };
            if context_id.is_some_and(|selected| selected != id) {
                continue;
            }
            if !context_nodes.iter().any(|node| node.id == target_node_id) {
                continue;
            }
            let nodes = context_nodes
                .iter()
                .map(|node| graph_provider_context_node(node, false))
                .collect();
            contexts
                .entry(id.clone())
                .or_insert(GraphProviderContext { id, nodes });
        }
    }

    let mut contexts = contexts.into_values().collect::<Vec<_>>();
    contexts.sort_by(provider_context_order);
    Ok(contexts.into_iter().find_map(|context| {
        let selected_id = context
            .nodes
            .iter()
            .find(|node| node.id == target_node_id)
            .map(|node| node.id.clone())?;
        Some(GraphProviderContextSelection {
            selected_id,
            context,
        })
    }))
}

fn node_anchor(node: &Node) -> Option<&Anchor> {
    match &node.kind {
        Kind::Anchor(anchor) => Some(anchor),
        _ => None,
    }
}

fn should_follow_primary_parent(node: &Node, mode: GraphMode) -> bool {
    mode == GraphMode::All || !is_provider_context_start(node)
}

fn is_duplicate_visible_merge_parent(
    primary_parent: &Option<String>,
    parents: &[MergeParent],
    parent_id: &str,
) -> bool {
    primary_parent.as_deref() == Some(parent_id)
        || parents
            .iter()
            .any(|existing| existing.node_id() == parent_id)
}

fn collect_visible_graph_nodes<S: NodeStore>(
    store: &S,
    head_id: &str,
    scope_node_ids: &mut BTreeSet<String>,
    mode: GraphMode,
    visible_node_ids: &mut BTreeSet<String>,
    visible_nodes: &mut HashMap<String, Node>,
    branch_visible_node_ids: &mut BTreeSet<String>,
) -> Result<()> {
    VisibleGraphCollector {
        store,
        mode,
        scope_node_ids,
        visible_node_ids,
        visible_nodes,
        branch_visible_node_ids,
        pending: vec![head_id.to_owned()],
        visited: BTreeSet::new(),
    }
    .collect()
}

fn initial_graph_scope(
    store: &impl NodeStore,
    head_id: &str,
    mode: GraphMode,
) -> Result<BTreeSet<String>> {
    match mode {
        GraphMode::Anchors => collect_provider_context_node_ids(store, head_id),
        GraphMode::All => Ok(BTreeSet::from([head_id.to_owned()])),
    }
}

fn push_scoped_graph_node(
    scope_node_ids: &mut BTreeSet<String>,
    pending: &mut Vec<String>,
    node_id: String,
) {
    if node_id.is_empty() {
        return;
    }

    scope_node_ids.insert(node_id.clone());
    pending.push(node_id);
}

fn collect_provider_context_node_ids(
    store: &impl NodeStore,
    head_id: &str,
) -> Result<BTreeSet<String>> {
    let ancestry = store.ancestry(head_id).context(StoreSnafu)?;
    Ok(provider_context_node_ids(ancestry))
}

fn provider_context_node_ids(ancestry: Vec<Node>) -> BTreeSet<String> {
    provider_context_ancestry_nodes(ancestry)
        .into_iter()
        .map(|node| node.id)
        .collect()
}

pub(crate) fn provider_context_ancestry_nodes(ancestry: Vec<Node>) -> Vec<Node> {
    ancestry
        .into_iter()
        .take_while(|node| !node.is_root())
        .scan(false, provider_context_node)
        .collect()
}

fn provider_context_node(done: &mut bool, node: Node) -> Option<Node> {
    if *done {
        return None;
    }
    *done = is_provider_context_start(&node);
    Some(node)
}

fn provider_contexts_from_head(ancestry: Vec<Node>) -> Vec<Vec<Node>> {
    let mut contexts = Vec::new();
    let mut current = Vec::new();
    let mut previous_is_skill_invocation = false;

    for node in ancestry.into_iter().take_while(|node| !node.is_root()) {
        if should_skip_skill_invocation_tool_use(&node, previous_is_skill_invocation) {
            continue;
        }
        let is_start = is_provider_context_start(&node);
        previous_is_skill_invocation = is_skill_invocation_anchor(&node);
        current.push(node);
        if is_start {
            contexts.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        contexts.push(current);
    }

    contexts
}

fn should_skip_skill_invocation_tool_use(node: &Node, previous_is_skill_invocation: bool) -> bool {
    node.kind.as_tool_uses().is_some() && previous_is_skill_invocation
}

fn is_skill_invocation_anchor(node: &Node) -> bool {
    matches!(
        &node.kind,
        Kind::Anchor(anchor) if anchor.as_skill_invocation().is_some()
    )
}

fn provider_context_ids_by_node(
    contexts: &[GraphProviderContext],
    visible_node_ids: &BTreeSet<String>,
) -> HashMap<String, Vec<String>> {
    let mut ids_by_node = HashMap::<String, Vec<String>>::new();
    for context in contexts {
        for node in context
            .nodes
            .iter()
            .filter(|node| visible_node_ids.contains(&node.id))
        {
            ids_by_node
                .entry(node.id.clone())
                .or_default()
                .push(context.id.clone());
        }
    }
    ids_by_node
}

fn provider_context_id(branch_name: &str, context: &[Node]) -> Option<String> {
    let context_start = context.last()?;
    Some(format!(
        "{}-context-{}",
        node_target_id(&context_start.id),
        stable_token(branch_name)
    ))
}

fn stable_token(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    value
        .bytes()
        .flat_map(|byte| {
            [
                HEX[(byte >> 4) as usize] as char,
                HEX[(byte & 0x0f) as usize] as char,
            ]
        })
        .collect()
}

fn provider_context_order(
    left: &GraphProviderContext,
    right: &GraphProviderContext,
) -> std::cmp::Ordering {
    provider_context_head_time_ns(left)
        .cmp(&provider_context_head_time_ns(right))
        .then_with(|| left.id.cmp(&right.id))
}

fn provider_context_head_time_ns(context: &GraphProviderContext) -> i128 {
    context
        .nodes
        .first()
        .map(|node| node.created_at_ns)
        .unwrap_or_default()
}

fn graph_provider_context_node(node: &Node, visible: bool) -> GraphProviderContextNode {
    GraphProviderContextNode {
        id: node.id.clone(),
        short_id: shorten_id(&node.id),
        kind: graph_kind_name(node).to_owned(),
        role: format!("{:?}", node.role),
        created_at: node.created_at.to_string(),
        created_at_ns: node.created_at.as_nanosecond(),
        content: render_node_content(node),
        summary: summarize_node(node),
        visible,
    }
}

fn is_provider_context_start(node: &Node) -> bool {
    matches!(
        &node.kind,
        Kind::Anchor(anchor)
            if anchor.as_session().is_some_and(is_context_start_session)
    )
}

fn is_context_start_session(session: &SessionAnchor) -> bool {
    session
        .active_skill
        .as_ref()
        .is_none_or(|active_skill| active_skill.handoff.is_some())
}

fn is_visible_graph_node(node: &Node, mode: GraphMode) -> bool {
    match mode {
        GraphMode::Anchors => matches!(&node.kind, Kind::Anchor(_)),
        GraphMode::All => true,
    }
}

pub(crate) fn initial_visible_graph_lane_nodes(
    _store: &impl NodeStore,
    mode: GraphMode,
    ancestry: Vec<Node>,
) -> Result<Vec<Node>> {
    let mut nodes = Vec::new();
    let mut seen = BTreeSet::new();
    let ancestry = match mode {
        GraphMode::Anchors => provider_context_ancestry_nodes(ancestry),
        GraphMode::All => ancestry,
    };
    for node in ancestry.into_iter().rev() {
        push_initial_lane_node(mode, node.clone(), &mut seen, &mut nodes);
    }
    Ok(nodes)
}

pub(crate) fn visible_skill_invocation_subtree_nodes(
    store: &impl NodeStore,
    mode: GraphMode,
    parent_id: &str,
) -> Result<Vec<Node>> {
    let mut nodes = Vec::new();
    let mut seen = BTreeSet::new();
    push_initial_skill_invocation_subtrees(store, mode, parent_id, &mut seen, &mut nodes)?;
    Ok(nodes)
}

fn push_initial_skill_invocation_subtrees(
    store: &impl NodeStore,
    mode: GraphMode,
    parent_id: &str,
    seen: &mut BTreeSet<String>,
    nodes: &mut Vec<Node>,
) -> Result<()> {
    let mut pending = skill_invocation_children(store, parent_id)?;
    pending.reverse();
    let mut visited = BTreeSet::new();

    while let Some(node_id) = pending.pop() {
        if node_id.is_empty() || !visited.insert(node_id.clone()) {
            continue;
        }
        let node = store.get_node(&node_id).context(StoreSnafu)?;
        let mut children = child_ids(store, &node.id)?;
        children.reverse();
        pending.extend(children);
        push_initial_lane_node(mode, node, seen, nodes);
    }
    Ok(())
}

fn push_initial_lane_node(
    mode: GraphMode,
    node: Node,
    seen: &mut BTreeSet<String>,
    nodes: &mut Vec<Node>,
) {
    if !node.is_root() && is_visible_graph_node(&node, mode) && seen.insert(node.id.clone()) {
        nodes.push(node);
    }
}

fn collect_visible_skill_invocation_subtrees(
    store: &impl NodeStore,
    parent_id: &str,
    scope_node_ids: &mut BTreeSet<String>,
    pending: &mut Vec<String>,
) -> Result<()> {
    let mut descendants = skill_invocation_children(store, parent_id)?;
    let mut visited = BTreeSet::new();

    while let Some(node_id) = next_unvisited_descendant(&mut descendants, &mut visited) {
        push_scoped_graph_node(scope_node_ids, pending, node_id.clone());
        descendants.extend(child_ids(store, &node_id)?);
    }

    Ok(())
}

fn skill_invocation_children(store: &impl NodeStore, parent_id: &str) -> Result<Vec<String>> {
    Ok(store
        .list_children(parent_id)
        .context(StoreSnafu)?
        .into_iter()
        .filter_map(skill_invocation_child_id)
        .collect())
}

fn skill_invocation_child_id(child: Node) -> Option<String> {
    match child.kind {
        Kind::Anchor(anchor) if anchor.as_skill_invocation().is_some() => Some(child.id),
        _ => None,
    }
}

fn next_unvisited_descendant(
    descendants: &mut Vec<String>,
    visited: &mut BTreeSet<String>,
) -> Option<String> {
    while let Some(node_id) = descendants.pop() {
        if !node_id.is_empty() && visited.insert(node_id.clone()) {
            return Some(node_id);
        }
    }
    None
}

fn child_ids(store: &impl NodeStore, node_id: &str) -> Result<Vec<String>> {
    Ok(store
        .list_children(node_id)
        .context(StoreSnafu)?
        .into_iter()
        .map(|child| child.id)
        .collect())
}

fn resolve_visible_parent(
    store: &impl NodeStore,
    visible_node_ids: &BTreeSet<String>,
    scope_node_ids: &BTreeSet<String>,
    start_id: &str,
) -> Result<Option<String>> {
    let Some(start_id) = non_empty_node_id(start_id) else {
        return Ok(None);
    };

    let ancestry = store.ancestry(start_id).context(StoreSnafu)?;
    Ok(visible_parent_in_ancestry(
        ancestry,
        visible_node_ids,
        scope_node_ids,
    ))
}

fn non_empty_node_id(node_id: &str) -> Option<&str> {
    (!node_id.is_empty()).then_some(node_id)
}

fn visible_parent_in_ancestry(
    ancestry: Vec<Node>,
    visible_node_ids: &BTreeSet<String>,
    scope_node_ids: &BTreeSet<String>,
) -> Option<String> {
    ancestry
        .into_iter()
        .take_while(|node| is_scoped_non_root(node, scope_node_ids))
        .find(|node| visible_node_ids.contains(&node.id))
        .map(|node| node.id)
}

fn is_scoped_non_root(node: &Node, scope_node_ids: &BTreeSet<String>) -> bool {
    scope_node_ids.contains(&node.id) && !node.is_root()
}

fn visible_merge_parent(parent: &MergeParent, node_id: String) -> MergeParent {
    if parent.is_shadow() {
        MergeParent::shadow(node_id)
    } else {
        MergeParent::merge(node_id)
    }
}

pub(crate) fn graph_kind_name(node: &Node) -> &'static str {
    match &node.kind {
        Kind::Anchor(anchor) => match &anchor.payload {
            AnchorPayload::Session(_) => "session",
            AnchorPayload::SessionPatch(_) => "session_patch",
            AnchorPayload::Prompt(_) => "prompt",
            AnchorPayload::SkillInvocation(_) => "skill_invocation",
            AnchorPayload::SkillResult(_) => "skill_result",
        },
        Kind::ToolUse(_) => "tool_use",
        Kind::ToolResult(_) => "tool_result",
        Kind::Text(_) => "text",
        Kind::Failure(_) => "failure",
    }
}

pub(crate) fn summarize_node(node: &Node) -> String {
    truncate_summary(&render_node_content(node))
}

fn render_node_content(node: &Node) -> String {
    match &node.kind {
        Kind::Anchor(anchor) => render_anchor_content(anchor),
        Kind::ToolUse(tool_uses) => render_tool_use_content(tool_uses),
        Kind::ToolResult(tool_results) => render_tool_result_content(tool_results),
        Kind::Text(text) => text.clone(),
        Kind::Failure(message) => message.clone(),
    }
}

fn render_anchor_content(anchor: &Anchor) -> String {
    match &anchor.payload {
        AnchorPayload::Session(session) => render_session_content(session),
        AnchorPayload::SessionPatch(patch) => {
            serde_json::to_string(patch).expect("session patch should serialize")
        }
        AnchorPayload::Prompt(prompt) => prompt.prompt.clone(),
        AnchorPayload::SkillInvocation(invocation) => invocation.skill_name.clone(),
        AnchorPayload::SkillResult(skill_result) => skill_result.output.clone(),
    }
}

fn render_session_content(session: &SessionAnchor) -> String {
    if session.prompt.trim().is_empty() {
        session.system_prompt.clone()
    } else {
        session.prompt.clone()
    }
}

fn render_tool_use_content(tool_uses: &ManyOrOne<ToolUse>) -> String {
    tool_uses
        .first()
        .map(|tool_use| tool_use.input.to_string())
        .unwrap_or_default()
}

fn render_tool_result_content(tool_results: &ManyOrOne<ToolResult>) -> String {
    tool_results
        .first()
        .map(|tool_result| tool_result.output.clone())
        .unwrap_or_default()
}

fn truncate_summary(raw: &str) -> String {
    let collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = collapsed.chars();
    let truncated = chars.by_ref().take(SUMMARY_LIMIT).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

pub fn shorten_id(id: &str) -> String {
    id.chars().take(8).collect()
}

pub fn css_token(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

pub fn node_target_id(node_id: &str) -> String {
    format!("detail-{}", css_token(node_id))
}

fn render_graph_labels(labels: &[GraphBranchLabel]) -> Vec<String> {
    let mut labels = labels
        .iter()
        .map(|label| format!("{}{}", label.branch, format_state_suffix(&label.state)))
        .collect::<Vec<_>>();
    labels.sort();
    labels
}

fn format_state_suffix(state: &SessionState) -> String {
    match state {
        SessionState::Active => String::new(),
        SessionState::Attached { target_branch, .. } => format!("@Attached({target_branch})"),
        SessionState::Paused {
            target_branch,
            reason,
        } => match reason {
            PauseReason::Merged { .. } => format!("@Paused({target_branch},merged)"),
            PauseReason::Closed => format!("@Paused({target_branch},closed)"),
        },
    }
}
