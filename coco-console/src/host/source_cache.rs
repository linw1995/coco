use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;

use async_trait::async_trait;
use coco_mem::{
    BranchAppendSessionState, BranchStore, GRAPH_READ_BATCH_SIZE, GraphBranchRecord, Kind, NewNode,
    NewNodeContent, Node, NodeStore, SessionAnchorPatch, SessionState, SessionStore,
    SqliteGraphStore, StoreError, StoreResult,
};
use diesel::prelude::*;
use diesel::sql_types::Text;
use snafu::prelude::*;

use super::snapshot_store::{ConsoleGraphSnapshotStore, SnapshotDatabase};
use crate::error::{
    ParseGraphSnapshotStoreValueSnafu, QueryGraphSnapshotStoreSnafu,
    SerializeGraphSnapshotStoreValueSnafu,
};
use crate::schema::{
    console_graph_source_branch_nodes, console_graph_source_branches,
    console_graph_source_node_relations, console_graph_source_nodes,
    console_graph_source_refresh_queue, console_graph_source_state,
};

const SOURCE_CACHE_BATCH_SIZE: usize = GRAPH_READ_BATCH_SIZE;
const TRAVERSAL_GRAPH: &str = "graph";
const TRAVERSAL_SKILL_SUBTREE: &str = "skill_subtree";

#[derive(Debug)]
pub(crate) struct PersistentGraphIndex {
    root_id: String,
    database: SnapshotDatabase,
    path: PathBuf,
    #[cfg(test)]
    refresh_count: usize,
    #[cfg(test)]
    branch_refresh_count: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct PersistentGraphStore {
    root_id: String,
    database: SnapshotDatabase,
    path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum TraversalKind {
    Graph,
    SkillSubtree,
}

impl TraversalKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Graph => TRAVERSAL_GRAPH,
            Self::SkillSubtree => TRAVERSAL_SKILL_SUBTREE,
        }
    }

    fn parse(value: &str) -> crate::Result<Self> {
        match value {
            TRAVERSAL_GRAPH => Ok(Self::Graph),
            TRAVERSAL_SKILL_SUBTREE => Ok(Self::SkillSubtree),
            _ => crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "traversal_kind",
                value: value.to_owned(),
            }
            .fail(),
        }
    }
}

#[derive(Debug)]
struct QueueItem {
    node_id: String,
    traversal: TraversalKind,
}

#[derive(Debug)]
struct PersistedSourceNode {
    node_id: String,
    parent_id: String,
    node_json: String,
    parent_ids: BTreeSet<String>,
}

#[derive(Debug, QueryableByName)]
struct NodeIdRow {
    #[diesel(sql_type = Text)]
    node_id: String,
}

#[derive(Debug, QueryableByName)]
struct NodeJsonRow {
    #[diesel(sql_type = Text)]
    node_json: String,
}

#[derive(Debug, QueryableByName)]
struct BranchRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    state_json: String,
}

impl PersistentGraphIndex {
    pub(crate) async fn open(
        snapshots: &ConsoleGraphSnapshotStore,
        root_id: String,
    ) -> crate::Result<Self> {
        let database = snapshots.database();
        let path = database.path().to_owned();
        let mut index = Self {
            root_id,
            database,
            path,
            #[cfg(test)]
            refresh_count: 0,
            #[cfg(test)]
            branch_refresh_count: 0,
        };
        index.discard_incomplete_refreshes().await?;
        Ok(index)
    }

    pub(crate) async fn is_empty(&self) -> crate::Result<bool> {
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                console_graph_source_branches::table
                    .count()
                    .get_result::<i64>(connection)
                    .map(|count| count == 0)
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    pub(crate) async fn start_refresh(&mut self) -> crate::Result<()> {
        #[cfg(test)]
        {
            self.refresh_count += 1;
        }
        self.discard_incomplete_refreshes().await
    }

    pub(crate) async fn reconcile_full_refresh(
        &mut self,
        records: &[GraphBranchRecord],
    ) -> crate::Result<()> {
        let current_names = records
            .iter()
            .map(|record| record.name.clone())
            .collect::<HashSet<_>>();
        let path = self.path.clone();
        let existing = self
            .database
            .with_connection(move |connection| {
                console_graph_source_branches::table
                    .select(console_graph_source_branches::name)
                    .load::<String>(connection)
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        for name in existing
            .into_iter()
            .filter(|name| !current_names.contains(name))
        {
            self.remove_branch(&name).await?;
        }
        Ok(())
    }

    pub(crate) async fn refresh_records(
        &mut self,
        store: &SqliteGraphStore,
        records: impl IntoIterator<Item = GraphBranchRecord>,
    ) -> crate::Result<()> {
        for record in records {
            self.refresh_branch(store, record).await?;
        }
        Ok(())
    }

    pub(crate) async fn refresh_named_batch(
        &mut self,
        store: &SqliteGraphStore,
        names: &[String],
    ) -> crate::Result<()> {
        let records = store
            .graph_branches_by_names(names)
            .await
            .context(crate::error::StoreSnafu)?;
        let found = records
            .iter()
            .map(|record| record.name.clone())
            .collect::<HashSet<_>>();
        for name in names.iter().filter(|name| !found.contains(*name)) {
            self.remove_branch(name).await?;
        }
        self.refresh_records(store, records).await
    }

    pub(crate) fn graph_store(&self) -> PersistentGraphStore {
        PersistentGraphStore {
            root_id: self.root_id.clone(),
            database: self.database.clone(),
            path: self.path.clone(),
        }
    }

    async fn refresh_branch(
        &mut self,
        store: &SqliteGraphStore,
        record: GraphBranchRecord,
    ) -> crate::Result<()> {
        #[cfg(test)]
        {
            self.branch_refresh_count += 1;
        }
        let generation = self.allocate_generation().await?;
        self.seed_refresh_queue(generation, &record.name, &record.head_id)
            .await?;
        let mut processed_nodes = 0usize;

        loop {
            let batch = self.claim_refresh_batch(generation).await?;
            if batch.is_empty() {
                break;
            }
            let ids = batch
                .iter()
                .map(|item| item.node_id.clone())
                .collect::<Vec<_>>();
            let nodes = self.load_nodes(store, &ids).await?;
            let child_parents = child_parent_ids(&batch, &nodes);
            let children = self.load_children(store, &child_parents).await?;
            let child_ids = children.values().flatten().cloned().collect::<Vec<_>>();
            let child_nodes = self.load_nodes(store, &child_ids).await?;
            let pending = pending_traversals(&batch, &nodes, &children, &child_nodes)?;
            self.commit_refresh_batch(generation, &record.name, &batch, &pending)
                .await?;
            processed_nodes += batch.len();
            tracing::info!(
                branch = %record.name,
                contribution_generation = generation,
                batch_nodes = batch.len(),
                processed_nodes,
                queued_nodes = pending.len(),
                "console graph source batch committed",
            );
            tokio::task::yield_now().await;
        }

        self.commit_branch(generation, record).await?;
        self.prune_orphan_nodes().await
    }

    async fn load_nodes(
        &self,
        store: &SqliteGraphStore,
        ids: &[String],
    ) -> crate::Result<HashMap<String, Node>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let unique_ids = ids.iter().cloned().collect::<BTreeSet<_>>();
        let query_ids = unique_ids.iter().cloned().collect::<Vec<_>>();
        let mut nodes = HashMap::new();
        for batch in query_ids.chunks(SOURCE_CACHE_BATCH_SIZE) {
            let batch = batch.to_vec();
            let path = self.path.clone();
            let cached = self
                .database
                .with_connection(move |connection| {
                    console_graph_source_nodes::table
                        .filter(console_graph_source_nodes::node_id.eq_any(batch))
                        .select(console_graph_source_nodes::node_json)
                        .load::<String>(connection)
                        .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
            for value in cached {
                let node = serde_json::from_str::<Node>(&value).context(
                    ParseGraphSnapshotStoreValueSnafu {
                        column: "node_json",
                    },
                )?;
                nodes.insert(node.id.clone(), node);
            }
        }
        let missing = unique_ids
            .into_iter()
            .filter(|node_id| !nodes.contains_key(node_id))
            .collect::<Vec<_>>();
        for batch in missing.chunks(SOURCE_CACHE_BATCH_SIZE) {
            let loaded = store
                .graph_nodes_by_ids(batch)
                .await
                .context(crate::error::StoreSnafu)?;
            self.persist_nodes(&loaded).await?;
            nodes.extend(loaded.into_iter().map(|node| (node.id.clone(), node)));
            tokio::task::yield_now().await;
        }
        Ok(nodes)
    }

    async fn load_children(
        &self,
        store: &SqliteGraphStore,
        parent_ids: &[String],
    ) -> crate::Result<HashMap<String, Vec<String>>> {
        let mut children = HashMap::new();
        for batch in parent_ids.chunks(SOURCE_CACHE_BATCH_SIZE) {
            let batch_children = store
                .graph_child_ids(batch)
                .await
                .context(crate::error::StoreSnafu)?;
            children.extend(batch_children);
            tokio::task::yield_now().await;
        }
        Ok(children)
    }

    async fn persist_nodes(&self, nodes: &[Node]) -> crate::Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        for batch in nodes.chunks(SOURCE_CACHE_BATCH_SIZE) {
            let batch = batch
                .iter()
                .map(PersistedSourceNode::try_from)
                .collect::<crate::Result<Vec<_>>>()?;
            let path = self.path.clone();
            self.database
                .with_connection(move |connection| {
                    connection
                        .transaction::<_, diesel::result::Error, _>(|connection| {
                            for node in &batch {
                                diesel::insert_into(console_graph_source_nodes::table)
                                    .values((
                                        console_graph_source_nodes::node_id.eq(&node.node_id),
                                        console_graph_source_nodes::parent_id.eq(&node.parent_id),
                                        console_graph_source_nodes::node_json.eq(&node.node_json),
                                    ))
                                    .on_conflict(console_graph_source_nodes::node_id)
                                    .do_nothing()
                                    .execute(connection)?;
                                for parent_id in &node.parent_ids {
                                    diesel::insert_into(console_graph_source_node_relations::table)
                                        .values((
                                            console_graph_source_node_relations::parent_id
                                                .eq(parent_id),
                                            console_graph_source_node_relations::child_id
                                                .eq(&node.node_id),
                                        ))
                                        .on_conflict((
                                            console_graph_source_node_relations::parent_id,
                                            console_graph_source_node_relations::child_id,
                                        ))
                                        .do_nothing()
                                        .execute(connection)?;
                                }
                            }
                            Ok(())
                        })
                        .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
        }
        Ok(())
    }

    async fn allocate_generation(&self) -> crate::Result<i64> {
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                connection
                    .transaction::<_, diesel::result::Error, _>(|connection| {
                        let generation = console_graph_source_state::table
                            .filter(console_graph_source_state::id.eq(1))
                            .select(console_graph_source_state::next_generation)
                            .first::<i64>(connection)?;
                        diesel::update(
                            console_graph_source_state::table
                                .filter(console_graph_source_state::id.eq(1)),
                        )
                        .set(
                            console_graph_source_state::next_generation
                                .eq(generation.saturating_add(1)),
                        )
                        .execute(connection)?;
                        Ok(generation)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn seed_refresh_queue(
        &self,
        generation: i64,
        branch: &str,
        head_id: &str,
    ) -> crate::Result<()> {
        let branch = branch.to_owned();
        let head_id = head_id.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                diesel::insert_into(console_graph_source_refresh_queue::table)
                    .values((
                        console_graph_source_refresh_queue::contribution_generation.eq(generation),
                        console_graph_source_refresh_queue::branch_name.eq(branch),
                        console_graph_source_refresh_queue::node_id.eq(head_id),
                        console_graph_source_refresh_queue::traversal_kind.eq(TRAVERSAL_GRAPH),
                        console_graph_source_refresh_queue::processed.eq(0),
                    ))
                    .execute(connection)
                    .map(|_| ())
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn claim_refresh_batch(&self, generation: i64) -> crate::Result<Vec<QueueItem>> {
        let path = self.path.clone();
        let rows = self
            .database
            .with_connection(move |connection| {
                connection
                    .transaction::<_, diesel::result::Error, _>(|connection| {
                        let rows = console_graph_source_refresh_queue::table
                            .filter(
                                console_graph_source_refresh_queue::contribution_generation
                                    .eq(generation),
                            )
                            .filter(console_graph_source_refresh_queue::processed.eq(0))
                            .select((
                                console_graph_source_refresh_queue::node_id,
                                console_graph_source_refresh_queue::traversal_kind,
                            ))
                            .order((
                                console_graph_source_refresh_queue::node_id,
                                console_graph_source_refresh_queue::traversal_kind,
                            ))
                            .limit(SOURCE_CACHE_BATCH_SIZE as i64)
                            .load::<(String, String)>(connection)?;
                        for (node_id, traversal_kind) in &rows {
                            diesel::update(
                                console_graph_source_refresh_queue::table
                                    .filter(
                                        console_graph_source_refresh_queue::contribution_generation
                                            .eq(generation),
                                    )
                                    .filter(console_graph_source_refresh_queue::node_id.eq(node_id))
                                    .filter(
                                        console_graph_source_refresh_queue::traversal_kind
                                            .eq(traversal_kind),
                                    ),
                            )
                            .set(console_graph_source_refresh_queue::processed.eq(1))
                            .execute(connection)?;
                        }
                        Ok(rows)
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        rows.into_iter()
            .map(|(node_id, traversal_kind)| {
                Ok(QueueItem {
                    node_id,
                    traversal: TraversalKind::parse(&traversal_kind)?,
                })
            })
            .collect()
    }

    async fn commit_refresh_batch(
        &self,
        generation: i64,
        branch: &str,
        processed: &[QueueItem],
        pending: &[(String, TraversalKind)],
    ) -> crate::Result<()> {
        let branch = branch.to_owned();
        let processed = processed
            .iter()
            .map(|item| item.node_id.clone())
            .collect::<BTreeSet<_>>();
        let pending = pending
            .iter()
            .filter(|(node_id, _)| !node_id.is_empty())
            .cloned()
            .collect::<BTreeSet<_>>();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                connection
                    .transaction::<_, diesel::result::Error, _>(|connection| {
                        for node_id in &processed {
                            diesel::insert_into(console_graph_source_branch_nodes::table)
                                .values((
                                    console_graph_source_branch_nodes::branch_name.eq(&branch),
                                    console_graph_source_branch_nodes::contribution_generation
                                        .eq(generation),
                                    console_graph_source_branch_nodes::node_id.eq(node_id),
                                ))
                                .on_conflict((
                                    console_graph_source_branch_nodes::branch_name,
                                    console_graph_source_branch_nodes::contribution_generation,
                                    console_graph_source_branch_nodes::node_id,
                                ))
                                .do_nothing()
                                .execute(connection)?;
                        }
                        for (node_id, traversal) in &pending {
                            diesel::insert_into(console_graph_source_refresh_queue::table)
                                .values((
                                    console_graph_source_refresh_queue::contribution_generation
                                        .eq(generation),
                                    console_graph_source_refresh_queue::branch_name.eq(&branch),
                                    console_graph_source_refresh_queue::node_id.eq(node_id),
                                    console_graph_source_refresh_queue::traversal_kind
                                        .eq(traversal.as_str()),
                                    console_graph_source_refresh_queue::processed.eq(0),
                                ))
                                .on_conflict((
                                    console_graph_source_refresh_queue::contribution_generation,
                                    console_graph_source_refresh_queue::node_id,
                                    console_graph_source_refresh_queue::traversal_kind,
                                ))
                                .do_nothing()
                                .execute(connection)?;
                        }
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn commit_branch(&self, generation: i64, record: GraphBranchRecord) -> crate::Result<()> {
        let state_json = serde_json::to_string(&record.state).context(
            SerializeGraphSnapshotStoreValueSnafu {
                column: "state_json",
            },
        )?;
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                connection
                    .transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::insert_into(console_graph_source_branches::table)
                            .values((
                                console_graph_source_branches::name.eq(&record.name),
                                console_graph_source_branches::head_id.eq(&record.head_id),
                                console_graph_source_branches::state_json.eq(&state_json),
                                console_graph_source_branches::contribution_generation
                                    .eq(generation),
                            ))
                            .on_conflict(console_graph_source_branches::name)
                            .do_update()
                            .set((
                                console_graph_source_branches::head_id.eq(&record.head_id),
                                console_graph_source_branches::state_json.eq(&state_json),
                                console_graph_source_branches::contribution_generation
                                    .eq(generation),
                            ))
                            .execute(connection)?;
                        diesel::delete(
                            console_graph_source_branch_nodes::table
                                .filter(
                                    console_graph_source_branch_nodes::branch_name.eq(&record.name),
                                )
                                .filter(
                                    console_graph_source_branch_nodes::contribution_generation
                                        .ne(generation),
                                ),
                        )
                        .execute(connection)?;
                        diesel::delete(
                            console_graph_source_refresh_queue::table.filter(
                                console_graph_source_refresh_queue::contribution_generation
                                    .eq(generation),
                            ),
                        )
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn remove_branch(&self, name: &str) -> crate::Result<()> {
        let name = name.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                connection
                    .transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::delete(
                            console_graph_source_branches::table
                                .filter(console_graph_source_branches::name.eq(&name)),
                        )
                        .execute(connection)?;
                        diesel::delete(
                            console_graph_source_branch_nodes::table
                                .filter(console_graph_source_branch_nodes::branch_name.eq(&name)),
                        )
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        self.prune_orphan_nodes().await
    }

    async fn discard_incomplete_refreshes(&mut self) -> crate::Result<()> {
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                connection
                    .transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::delete(console_graph_source_refresh_queue::table)
                            .execute(connection)?;
                        diesel::sql_query(
                            "DELETE FROM console_graph_source_branch_nodes \
                             WHERE NOT EXISTS ( \
                                 SELECT 1 \
                                 FROM console_graph_source_branches \
                                 WHERE console_graph_source_branches.name = \
                                     console_graph_source_branch_nodes.branch_name \
                                   AND console_graph_source_branches.contribution_generation = \
                                     console_graph_source_branch_nodes.contribution_generation \
                             )",
                        )
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        self.prune_orphan_nodes().await
    }

    async fn prune_orphan_nodes(&self) -> crate::Result<()> {
        loop {
            let path = self.path.clone();
            let ids = self
                .database
                .with_connection(move |connection| {
                    diesel::sql_query(
                        "SELECT node_id \
                         FROM console_graph_source_nodes \
                         WHERE NOT EXISTS ( \
                             SELECT 1 \
                             FROM console_graph_source_branch_nodes AS branch_nodes \
                             INNER JOIN console_graph_source_branches AS branches \
                                 ON branches.name = branch_nodes.branch_name \
                                AND branches.contribution_generation = \
                                    branch_nodes.contribution_generation \
                             WHERE branch_nodes.node_id = console_graph_source_nodes.node_id \
                         ) \
                         ORDER BY node_id \
                         LIMIT 128",
                    )
                    .load::<NodeIdRow>(connection)
                    .map(|rows| rows.into_iter().map(|row| row.node_id).collect::<Vec<_>>())
                    .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
            if ids.is_empty() {
                return Ok(());
            }
            let path = self.path.clone();
            self.database
                .with_connection(move |connection| {
                    connection
                        .transaction::<_, diesel::result::Error, _>(|connection| {
                            diesel::delete(console_graph_source_node_relations::table.filter(
                                console_graph_source_node_relations::parent_id.eq_any(&ids),
                            ))
                            .execute(connection)?;
                            diesel::delete(
                                console_graph_source_nodes::table
                                    .filter(console_graph_source_nodes::node_id.eq_any(&ids)),
                            )
                            .execute(connection)?;
                            Ok(())
                        })
                        .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
            tokio::task::yield_now().await;
        }
    }

    #[cfg(test)]
    pub(crate) fn refresh_count(&self) -> usize {
        self.refresh_count
    }

    #[cfg(test)]
    pub(crate) fn branch_refresh_count(&self) -> usize {
        self.branch_refresh_count
    }

    #[cfg(test)]
    pub(crate) async fn node_count(&self) -> crate::Result<usize> {
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                console_graph_source_nodes::table
                    .count()
                    .get_result::<i64>(connection)
                    .map(|count| count.max(0) as usize)
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }
}

fn child_parent_ids(batch: &[QueueItem], nodes: &HashMap<String, Node>) -> Vec<String> {
    batch
        .iter()
        .filter(|item| {
            nodes
                .get(&item.node_id)
                .is_some_and(|node| needs_children(item.traversal, node))
        })
        .map(|item| item.node_id.clone())
        .collect()
}

fn needs_children(traversal: TraversalKind, node: &Node) -> bool {
    traversal == TraversalKind::SkillSubtree || node.kind.as_tool_uses().is_some()
}

fn pending_traversals(
    batch: &[QueueItem],
    nodes: &HashMap<String, Node>,
    children: &HashMap<String, Vec<String>>,
    child_nodes: &HashMap<String, Node>,
) -> crate::Result<Vec<(String, TraversalKind)>> {
    let mut pending = Vec::new();
    for item in batch {
        let node = required_node(nodes, "node_id", &item.node_id)?;
        pending.extend(graph_parent_traversals(node));
        pending.extend(child_traversals(item, node, children, child_nodes)?);
    }
    Ok(pending)
}

fn required_node<'a>(
    nodes: &'a HashMap<String, Node>,
    column: &'static str,
    node_id: &str,
) -> crate::Result<&'a Node> {
    nodes
        .get(node_id)
        .with_context(|| crate::error::InvalidGraphSnapshotStoreValueSnafu {
            column,
            value: node_id.to_owned(),
        })
}

fn graph_parent_traversals(node: &Node) -> Vec<(String, TraversalKind)> {
    let mut parents = Vec::new();
    if !node.parent.is_empty() {
        parents.push((node.parent.clone(), TraversalKind::Graph));
    }
    if let Kind::Anchor(anchor) = &node.kind {
        parents.extend(
            anchor
                .merge_parents()
                .iter()
                .map(|parent| (parent.node_id().to_owned(), TraversalKind::Graph)),
        );
    }
    parents
}

fn child_traversals(
    item: &QueueItem,
    node: &Node,
    children: &HashMap<String, Vec<String>>,
    child_nodes: &HashMap<String, Node>,
) -> crate::Result<Vec<(String, TraversalKind)>> {
    let direct_children = children
        .get(&item.node_id)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    match item.traversal {
        TraversalKind::SkillSubtree => Ok(direct_children
            .iter()
            .cloned()
            .map(|child_id| (child_id, TraversalKind::SkillSubtree))
            .collect()),
        TraversalKind::Graph if node.kind.as_tool_uses().is_some() => {
            skill_invocation_traversals(direct_children, child_nodes)
        }
        TraversalKind::Graph => Ok(Vec::new()),
    }
}

fn skill_invocation_traversals(
    child_ids: &[String],
    child_nodes: &HashMap<String, Node>,
) -> crate::Result<Vec<(String, TraversalKind)>> {
    let mut traversals = Vec::new();
    for child_id in child_ids {
        let child = required_node(child_nodes, "child_id", child_id)?;
        if matches!(
            &child.kind,
            Kind::Anchor(anchor) if anchor.as_skill_invocation().is_some()
        ) {
            traversals.push((child_id.clone(), TraversalKind::SkillSubtree));
        }
    }
    Ok(traversals)
}

impl TryFrom<&Node> for PersistedSourceNode {
    type Error = crate::Error;

    fn try_from(node: &Node) -> Result<Self, Self::Error> {
        let node_json =
            serde_json::to_string(node).context(SerializeGraphSnapshotStoreValueSnafu {
                column: "node_json",
            })?;
        let mut parent_ids = BTreeSet::new();
        if !node.parent.is_empty() {
            parent_ids.insert(node.parent.clone());
        }
        if let Kind::Anchor(anchor) = &node.kind {
            parent_ids.extend(
                anchor
                    .merge_parents()
                    .iter()
                    .map(|parent| parent.node_id().to_owned()),
            );
        }
        Ok(Self {
            node_id: node.id.clone(),
            parent_id: node.parent.clone(),
            node_json,
            parent_ids,
        })
    }
}

impl PersistentGraphStore {
    fn read_only<T>(&self) -> StoreResult<T> {
        Err(StoreError::StoreReadOnly {
            path: self.path.clone(),
        })
    }

    fn map_error(&self, error: crate::Error) -> StoreError {
        StoreError::CorruptedStore {
            path: self.path.clone(),
            message: error.to_string(),
        }
    }

    async fn resolve_node(&self, reference: &str) -> StoreResult<Node> {
        let reference = reference.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                let node_id = console_graph_source_branches::table
                    .filter(console_graph_source_branches::name.eq(&reference))
                    .select(console_graph_source_branches::head_id)
                    .first::<String>(connection)
                    .optional()
                    .context(QueryGraphSnapshotStoreSnafu { path: path.clone() })?
                    .unwrap_or(reference);
                let value = console_graph_source_nodes::table
                    .filter(console_graph_source_nodes::node_id.eq(&node_id))
                    .select(console_graph_source_nodes::node_json)
                    .first::<String>(connection)
                    .optional()
                    .context(QueryGraphSnapshotStoreSnafu { path })?
                    .with_context(|| crate::error::InvalidGraphSnapshotStoreValueSnafu {
                        column: "node_id",
                        value: node_id,
                    })?;
                serde_json::from_str(&value).context(ParseGraphSnapshotStoreValueSnafu {
                    column: "node_json",
                })
            })
            .await
            .map_err(|error| self.map_error(error))
    }
}

#[async_trait]
impl NodeStore for PersistentGraphStore {
    fn root_id(&self) -> String {
        self.root_id.clone()
    }

    async fn append(&self, _node: NewNode) -> StoreResult<String> {
        self.read_only()
    }

    async fn ancestry(&self, head_ref: &str) -> StoreResult<Vec<Node>> {
        let head_ref = head_ref.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                let mut node_id = console_graph_source_branches::table
                    .filter(console_graph_source_branches::name.eq(&head_ref))
                    .select(console_graph_source_branches::head_id)
                    .first::<String>(connection)
                    .optional()
                    .context(QueryGraphSnapshotStoreSnafu { path: path.clone() })?
                    .unwrap_or(head_ref);
                let mut ancestry = Vec::new();
                let mut seen = HashSet::new();
                loop {
                    ensure!(
                        seen.insert(node_id.clone()),
                        crate::error::InvalidGraphSnapshotStoreValueSnafu {
                            column: "node_id",
                            value: format!("cyclic parent chain at {node_id}"),
                        }
                    );
                    let value = console_graph_source_nodes::table
                        .filter(console_graph_source_nodes::node_id.eq(&node_id))
                        .select(console_graph_source_nodes::node_json)
                        .first::<String>(connection)
                        .optional()
                        .context(QueryGraphSnapshotStoreSnafu { path: path.clone() })?
                        .with_context(|| crate::error::InvalidGraphSnapshotStoreValueSnafu {
                            column: "node_id",
                            value: node_id.clone(),
                        })?;
                    let node = serde_json::from_str::<Node>(&value).context(
                        ParseGraphSnapshotStoreValueSnafu {
                            column: "node_json",
                        },
                    )?;
                    let is_root = node.is_root();
                    node_id.clone_from(&node.parent);
                    ancestry.push(node);
                    if is_root {
                        return Ok(ancestry);
                    }
                }
            })
            .await
            .map_err(|error| self.map_error(error))
    }

    async fn log(&self, base_ref: &str, head_ref: &str) -> StoreResult<Vec<Node>> {
        let base = self.resolve_node(base_ref).await?.id;
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
        self.resolve_node(id).await
    }

    async fn list_children(&self, node_id: &str) -> StoreResult<Vec<Node>> {
        self.resolve_node(node_id).await?;
        let node_id = node_id.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                let rows = diesel::sql_query(
                    "SELECT DISTINCT nodes.node_json \
                     FROM console_graph_source_node_relations AS relations \
                     INNER JOIN console_graph_source_nodes AS nodes \
                         ON nodes.node_id = relations.child_id \
                     INNER JOIN console_graph_source_branch_nodes AS branch_nodes \
                         ON branch_nodes.node_id = nodes.node_id \
                     INNER JOIN console_graph_source_branches AS branches \
                         ON branches.name = branch_nodes.branch_name \
                        AND branches.contribution_generation = \
                            branch_nodes.contribution_generation \
                     WHERE relations.parent_id = ? \
                     ORDER BY nodes.node_id",
                )
                .bind::<Text, _>(node_id)
                .load::<NodeJsonRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })?;
                rows.into_iter()
                    .map(|row| {
                        serde_json::from_str(&row.node_json).context(
                            ParseGraphSnapshotStoreValueSnafu {
                                column: "node_json",
                            },
                        )
                    })
                    .collect()
            })
            .await
            .map_err(|error| self.map_error(error))
    }
}

#[async_trait]
impl BranchStore for PersistentGraphStore {
    async fn fork(&self, _name: &str, _from_ref: &str) -> StoreResult<String> {
        self.read_only()
    }

    async fn get_branch_head(&self, name: &str) -> StoreResult<String> {
        let name = name.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                console_graph_source_branches::table
                    .filter(console_graph_source_branches::name.eq(&name))
                    .select(console_graph_source_branches::head_id)
                    .first::<String>(connection)
                    .optional()
                    .context(QueryGraphSnapshotStoreSnafu { path })?
                    .with_context(|| crate::error::InvalidGraphSnapshotStoreValueSnafu {
                        column: "branch_name",
                        value: name,
                    })
            })
            .await
            .map_err(|error| self.map_error(error))
    }

    async fn delete_branch(&self, _name: &str) -> StoreResult<()> {
        self.read_only()
    }

    async fn set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _new_head: &str,
    ) -> StoreResult<()> {
        self.read_only()
    }

    async fn append_nodes_and_set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _parent: &str,
        _nodes: Vec<NewNodeContent>,
    ) -> StoreResult<String> {
        self.read_only()
    }

    async fn append_nodes_and_set_branch_head_to(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _parent: &str,
        _new_head: &str,
        _nodes: Vec<NewNodeContent>,
    ) -> StoreResult<String> {
        self.read_only()
    }

    async fn append_nodes_and_set_branch_head_with_session_state(
        &self,
        _update: BranchAppendSessionState,
    ) -> StoreResult<String> {
        self.read_only()
    }
}

#[async_trait]
impl SessionStore for PersistentGraphStore {
    async fn list_session_states(&self) -> StoreResult<HashMap<String, SessionState>> {
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                let rows = diesel::sql_query(
                    "SELECT name, state_json \
                     FROM console_graph_source_branches \
                     ORDER BY name",
                )
                .load::<BranchRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })?;
                rows.into_iter()
                    .map(|row| {
                        let state = serde_json::from_str(&row.state_json).context(
                            ParseGraphSnapshotStoreValueSnafu {
                                column: "state_json",
                            },
                        )?;
                        Ok((row.name, state))
                    })
                    .collect()
            })
            .await
            .map_err(|error| self.map_error(error))
    }

    async fn get_session_state(&self, name: &str) -> StoreResult<SessionState> {
        let name = name.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                let value = console_graph_source_branches::table
                    .filter(console_graph_source_branches::name.eq(&name))
                    .select(console_graph_source_branches::state_json)
                    .first::<String>(connection)
                    .optional()
                    .context(QueryGraphSnapshotStoreSnafu { path })?
                    .with_context(|| crate::error::InvalidGraphSnapshotStoreValueSnafu {
                        column: "branch_name",
                        value: name,
                    })?;
                serde_json::from_str(&value).context(ParseGraphSnapshotStoreValueSnafu {
                    column: "state_json",
                })
            })
            .await
            .map_err(|error| self.map_error(error))
    }

    async fn set_session_state(
        &self,
        _name: &str,
        _expected: Option<&SessionState>,
        _next: SessionState,
    ) -> StoreResult<SessionState> {
        self.read_only()
    }

    async fn rebase_session(
        &self,
        _name: &str,
        _patch: &SessionAnchorPatch,
    ) -> StoreResult<String> {
        self.read_only()
    }

    async fn handoff_session(
        &self,
        _name: &str,
        _patch: &SessionAnchorPatch,
        _prompt: &str,
    ) -> StoreResult<String> {
        self.read_only()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{GraphMode, build_graph_snapshot_with_mode};
    use coco_mem::{Kind, Role, SqliteStore};

    #[tokio::test]
    async fn source_cache_survives_index_reopen() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let child = writer
            .append(NewNode {
                parent: root,
                role: Role::User,
                metadata: None,
                kind: Kind::Text("persistent source cache".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &writer.root_id(), &child)
            .await
            .unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let records = source.graph_branches().await.unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        index.reconcile_full_refresh(&records).await.unwrap();
        index.refresh_records(&source, records).await.unwrap();
        drop(index);

        let reopened = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        assert!(!reopened.is_empty().await.unwrap());
        let actual = build_graph_snapshot_with_mode(&reopened.graph_store(), 7, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 7, GraphMode::All)
            .await
            .unwrap();

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn deleting_branch_prunes_persisted_source_nodes() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        assert_eq!(index.node_count().await.unwrap(), 1);

        writer.delete_branch("main").await.unwrap();
        index
            .refresh_named_batch(&source, &["main".to_owned()])
            .await
            .unwrap();

        assert!(index.is_empty().await.unwrap());
        assert_eq!(index.node_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn reopening_discards_incomplete_contribution_generation() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let stale_generation = index.allocate_generation().await.unwrap();
        index
            .seed_refresh_queue(stale_generation, "main", &root)
            .await
            .unwrap();
        let batch = index.claim_refresh_batch(stale_generation).await.unwrap();
        index
            .commit_refresh_batch(stale_generation, "main", &batch, &[])
            .await
            .unwrap();
        drop(index);

        let reopened = PersistentGraphIndex::open(&snapshots, writer.root_id())
            .await
            .unwrap();
        let path = reopened.path.clone();
        let database = reopened.database.clone();
        let (queue_rows, stale_nodes) = database
            .with_connection(move |connection| {
                let queue_rows = console_graph_source_refresh_queue::table
                    .count()
                    .get_result::<i64>(connection)
                    .context(QueryGraphSnapshotStoreSnafu { path: path.clone() })?;
                let stale_nodes = console_graph_source_branch_nodes::table
                    .filter(
                        console_graph_source_branch_nodes::contribution_generation
                            .eq(stale_generation),
                    )
                    .count()
                    .get_result::<i64>(connection)
                    .context(QueryGraphSnapshotStoreSnafu { path })?;
                Ok((queue_rows, stale_nodes))
            })
            .await
            .unwrap();

        assert_eq!(queue_rows, 0);
        assert_eq!(stale_nodes, 0);
        assert!(!reopened.is_empty().await.unwrap());
    }
}
