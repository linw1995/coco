use std::collections::{BTreeSet, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::ops::Bound::{Excluded, Unbounded};
use std::path::PathBuf;

use async_trait::async_trait;
use coco_mem::{
    BranchAppendSessionState, BranchStore, GRAPH_READ_BATCH_SIZE, GraphBranchRecord,
    GraphChildPageCursor, Kind, NewNode, NewNodeContent, Node, NodeStore, SessionAnchorPatch,
    SessionState, SessionStore, SqliteGraphStore, StoreError, StoreResult,
};
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Text};
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
const TARGETED_DYNAMIC_BRANCH_LIMIT: usize = GRAPH_READ_BATCH_SIZE;
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
    #[cfg(test)]
    traversed_node_count: usize,
    #[cfg(test)]
    branch_refresh_history: Vec<String>,
    #[cfg(test)]
    fail_next_branch_refresh: Option<String>,
    #[cfg(test)]
    targeted_dynamic_branch_limit: usize,
    #[cfg(test)]
    full_refresh_branch_page_size: NonZeroUsize,
    #[cfg(test)]
    full_refresh_source_page_count: usize,
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

#[derive(Debug)]
enum DynamicBranchScope {
    Targeted(BTreeSet<String>),
    FullRefresh,
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

#[derive(Clone, Debug)]
struct QueueItem {
    node_id: String,
    traversal: TraversalKind,
}

#[derive(Debug)]
struct PublishedBranch {
    head_id: String,
    contribution_generation: i64,
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
struct ChildRecheckRow {
    #[diesel(sql_type = Text)]
    node_id: String,
    #[diesel(sql_type = Text)]
    traversal_kind: String,
}

#[derive(Debug, QueryableByName)]
struct BranchRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    state_json: String,
}

#[derive(Debug, QueryableByName)]
struct BranchNameRow {
    #[diesel(sql_type = Text)]
    name: String,
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
            #[cfg(test)]
            traversed_node_count: 0,
            #[cfg(test)]
            branch_refresh_history: Vec::new(),
            #[cfg(test)]
            fail_next_branch_refresh: None,
            #[cfg(test)]
            targeted_dynamic_branch_limit: TARGETED_DYNAMIC_BRANCH_LIMIT,
            #[cfg(test)]
            full_refresh_branch_page_size: NonZeroUsize::new(GRAPH_READ_BATCH_SIZE)
                .expect("graph read batch size should be non-zero"),
            #[cfg(test)]
            full_refresh_source_page_count: 0,
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
        let requested = names.iter().cloned().collect::<BTreeSet<_>>();
        if requested.is_empty() {
            return Ok(());
        }
        if self
            .requires_full_refresh_for_unknown_missing_branch(store, &requested)
            .await?
        {
            return self
                .refresh_all_branches_peer_first(store, &requested)
                .await;
        }

        let pre_dependents = match self.branches_sharing_dynamic_parents(&requested).await? {
            DynamicBranchScope::Targeted(branches) => branches,
            DynamicBranchScope::FullRefresh => {
                return self
                    .refresh_all_branches_peer_first(store, &requested)
                    .await;
            }
        };
        let pre_peers = pre_dependents
            .difference(&requested)
            .cloned()
            .collect::<BTreeSet<_>>();
        self.refresh_exact_branch_names(store, &pre_peers).await?;
        self.refresh_exact_branch_names(store, &requested).await?;

        let post_dependents = match self.branches_sharing_dynamic_parents(&requested).await? {
            DynamicBranchScope::Targeted(branches) => branches,
            DynamicBranchScope::FullRefresh => {
                return self
                    .refresh_all_branches_peer_first(store, &requested)
                    .await;
            }
        };
        let post_peers = post_dependents
            .difference(&requested)
            .filter(|name| !pre_peers.contains(*name))
            .cloned()
            .collect::<BTreeSet<_>>();
        self.refresh_exact_branch_names(store, &post_peers).await
    }

    async fn refresh_exact_branch_names(
        &mut self,
        store: &SqliteGraphStore,
        names: &BTreeSet<String>,
    ) -> crate::Result<()> {
        let mut after = None;
        loop {
            let names = branch_name_batch_after(names, after.as_deref());
            if names.is_empty() {
                return Ok(());
            }
            after = names.last().cloned();
            self.refresh_exact_branch_batch(store, &names).await?;
            tokio::task::yield_now().await;
        }
    }

    async fn refresh_exact_branch_batch(
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

    async fn refresh_all_branches_peer_first(
        &mut self,
        store: &SqliteGraphStore,
        requested: &BTreeSet<String>,
    ) -> crate::Result<()> {
        let page_size = self.full_refresh_branch_page_size();
        let high_watermark = store
            .graph_branch_name_high_watermark()
            .await
            .context(crate::error::StoreSnafu)?;
        if let Some(high_watermark) = high_watermark {
            let mut cursor = None;
            loop {
                let page = store
                    .graph_branches_page(cursor.as_ref(), &high_watermark, page_size)
                    .await
                    .context(crate::error::StoreSnafu)?;
                let mut records = page.branches;
                if records.is_empty() {
                    break;
                }
                #[cfg(test)]
                {
                    self.full_refresh_source_page_count += 1;
                }
                records.retain(|record| !requested.contains(&record.name));
                self.refresh_records(store, records).await?;
                if page.complete {
                    break;
                }
                cursor = page.next_cursor;
                tokio::task::yield_now().await;
            }
        }

        self.refresh_exact_branch_names(store, requested).await?;
        self.reconcile_full_refresh_bounded(store).await
    }

    pub(crate) async fn refresh_all_branches_bounded(
        &mut self,
        store: &SqliteGraphStore,
    ) -> crate::Result<()> {
        self.refresh_all_branches_peer_first(store, &BTreeSet::new())
            .await
    }

    async fn reconcile_full_refresh_bounded(
        &mut self,
        store: &SqliteGraphStore,
    ) -> crate::Result<()> {
        let page_size = self.full_refresh_branch_page_size();
        let mut after = None;
        loop {
            let names = self
                .published_branch_name_page(after.as_deref(), page_size)
                .await?;
            if names.is_empty() {
                return Ok(());
            }
            let complete = names.len() < page_size.get();
            after = names.last().cloned();
            let current = store
                .graph_branches_by_names(&names)
                .await
                .context(crate::error::StoreSnafu)?
                .into_iter()
                .map(|record| record.name)
                .collect::<HashSet<_>>();
            for name in names.iter().filter(|name| !current.contains(*name)) {
                self.remove_branch(name).await?;
            }
            if complete {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    async fn published_branch_name_page(
        &self,
        after: Option<&str>,
        page_size: NonZeroUsize,
    ) -> crate::Result<Vec<String>> {
        let after = after.map(str::to_owned);
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                let mut query = console_graph_source_branches::table
                    .select(console_graph_source_branches::name)
                    .order(console_graph_source_branches::name)
                    .limit(page_size.get() as i64)
                    .into_boxed();
                if let Some(after) = after {
                    query = query.filter(console_graph_source_branches::name.gt(after));
                }
                query
                    .load::<String>(connection)
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn requires_full_refresh_for_unknown_missing_branch(
        &self,
        store: &SqliteGraphStore,
        requested: &BTreeSet<String>,
    ) -> crate::Result<bool> {
        let mut after = None;
        loop {
            let names = branch_name_batch_after(requested, after.as_deref());
            if names.is_empty() {
                return Ok(false);
            }
            after = names.last().cloned();
            let path = self.path.clone();
            let query_names = names.clone();
            let published = self
                .database
                .with_connection(move |connection| {
                    console_graph_source_branches::table
                        .filter(console_graph_source_branches::name.eq_any(query_names))
                        .select(console_graph_source_branches::name)
                        .load::<String>(connection)
                        .map(|names| names.into_iter().collect::<HashSet<_>>())
                        .context(QueryGraphSnapshotStoreSnafu { path })
                })
                .await?;
            let current = store
                .graph_branches_by_names(&names)
                .await
                .context(crate::error::StoreSnafu)?
                .into_iter()
                .map(|record| record.name)
                .collect::<HashSet<_>>();
            if names
                .iter()
                .any(|name| !published.contains(name) && !current.contains(name))
            {
                return Ok(true);
            }
            tokio::task::yield_now().await;
        }
    }

    async fn branches_sharing_dynamic_parents(
        &self,
        origin_branches: &BTreeSet<String>,
    ) -> crate::Result<DynamicBranchScope> {
        let targeted_limit = self.targeted_dynamic_branch_limit();
        if origin_branches.len() > targeted_limit {
            return Ok(DynamicBranchScope::FullRefresh);
        }

        let mut affected = BTreeSet::new();
        for origin_branch in origin_branches {
            let mut after = None;
            loop {
                let rows = self
                    .load_affected_branch_page(origin_branch, after.as_deref())
                    .await?;
                if rows.is_empty() {
                    break;
                }
                let complete = rows.len() < SOURCE_CACHE_BATCH_SIZE;
                after = rows.last().map(|row| row.name.clone());
                for row in rows {
                    affected.insert(row.name);
                    if affected.len() > targeted_limit {
                        return Ok(DynamicBranchScope::FullRefresh);
                    }
                }
                if complete {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        Ok(DynamicBranchScope::Targeted(affected))
    }

    async fn load_affected_branch_page(
        &self,
        origin_branch: &str,
        after: Option<&str>,
    ) -> crate::Result<Vec<BranchNameRow>> {
        let origin_branch = origin_branch.to_owned();
        let after = after.map(str::to_owned);
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                let rows = if let Some(after) = after {
                    diesel::sql_query(
                        "SELECT DISTINCT affected.branch_name AS name \
                         FROM console_graph_source_child_rechecks AS origin \
                         INNER JOIN console_graph_source_branches AS origin_branch \
                             ON origin_branch.name = origin.branch_name \
                            AND origin_branch.contribution_generation = \
                                origin.contribution_generation \
                         INNER JOIN console_graph_source_child_rechecks AS affected \
                             ON affected.node_id = origin.node_id \
                         INNER JOIN console_graph_source_branches AS affected_branch \
                             ON affected_branch.name = affected.branch_name \
                            AND affected_branch.contribution_generation = \
                                affected.contribution_generation \
                         WHERE origin.branch_name = ? AND affected.branch_name > ? \
                         ORDER BY affected.branch_name \
                         LIMIT ?",
                    )
                    .bind::<Text, _>(&origin_branch)
                    .bind::<Text, _>(after)
                    .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                    .load::<BranchNameRow>(connection)
                } else {
                    diesel::sql_query(
                        "SELECT DISTINCT affected.branch_name AS name \
                         FROM console_graph_source_child_rechecks AS origin \
                         INNER JOIN console_graph_source_branches AS origin_branch \
                             ON origin_branch.name = origin.branch_name \
                            AND origin_branch.contribution_generation = \
                                origin.contribution_generation \
                         INNER JOIN console_graph_source_child_rechecks AS affected \
                             ON affected.node_id = origin.node_id \
                         INNER JOIN console_graph_source_branches AS affected_branch \
                             ON affected_branch.name = affected.branch_name \
                            AND affected_branch.contribution_generation = \
                                affected.contribution_generation \
                         WHERE origin.branch_name = ? \
                         ORDER BY affected.branch_name \
                         LIMIT ?",
                    )
                    .bind::<Text, _>(&origin_branch)
                    .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                    .load::<BranchNameRow>(connection)
                };
                rows.context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    fn targeted_dynamic_branch_limit(&self) -> usize {
        #[cfg(test)]
        {
            self.targeted_dynamic_branch_limit
        }
        #[cfg(not(test))]
        {
            TARGETED_DYNAMIC_BRANCH_LIMIT
        }
    }

    fn full_refresh_branch_page_size(&self) -> NonZeroUsize {
        #[cfg(test)]
        {
            self.full_refresh_branch_page_size
        }
        #[cfg(not(test))]
        {
            NonZeroUsize::new(GRAPH_READ_BATCH_SIZE)
                .expect("graph read batch size should be non-zero")
        }
    }

    pub(crate) fn graph_store(&self) -> PersistentGraphStore {
        PersistentGraphStore {
            root_id: self.root_id.clone(),
            database: self.database.clone(),
            path: self.path.clone(),
        }
    }

    async fn published_branch(&self, name: &str) -> crate::Result<Option<PublishedBranch>> {
        let name = name.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                console_graph_source_branches::table
                    .filter(console_graph_source_branches::name.eq(name))
                    .select((
                        console_graph_source_branches::head_id,
                        console_graph_source_branches::contribution_generation,
                    ))
                    .first::<(String, i64)>(connection)
                    .optional()
                    .map(|published| {
                        published.map(|(head_id, contribution_generation)| PublishedBranch {
                            head_id,
                            contribution_generation,
                        })
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn update_published_branch_state(&self, record: &GraphBranchRecord) -> crate::Result<()> {
        let state_json = serde_json::to_string(&record.state).context(
            SerializeGraphSnapshotStoreValueSnafu {
                column: "state_json",
            },
        )?;
        let name = record.name.clone();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                diesel::update(
                    console_graph_source_branches::table
                        .filter(console_graph_source_branches::name.eq(name)),
                )
                .set(console_graph_source_branches::state_json.eq(state_json))
                .execute(connection)
                .map(|_| ())
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn refresh_branch(
        &mut self,
        store: &SqliteGraphStore,
        record: GraphBranchRecord,
    ) -> crate::Result<()> {
        #[cfg(test)]
        {
            self.branch_refresh_count += 1;
            self.branch_refresh_history.push(record.name.clone());
            if self.fail_next_branch_refresh.as_deref() == Some(record.name.as_str()) {
                self.fail_next_branch_refresh = None;
                return crate::error::InvalidGraphSnapshotStoreValueSnafu {
                    column: "branch_name",
                    value: record.name,
                }
                .fail();
            }
        }
        let previous = self.published_branch(&record.name).await?;
        let same_head = previous
            .as_ref()
            .is_some_and(|previous| previous.head_id == record.head_id);
        if same_head {
            let previous = previous
                .as_ref()
                .expect("same-head refresh should have a previous branch");
            if !self
                .dynamic_children_changed(store, &record.name, previous.contribution_generation)
                .await?
            {
                return self.update_published_branch_state(&record).await;
            }
        }

        let generation = self.allocate_generation().await?;
        if same_head {
            let previous = previous
                .as_ref()
                .expect("same-head refresh should have a previous branch");
            self.copy_previous_contribution(
                &record.name,
                previous.contribution_generation,
                generation,
            )
            .await?;
            self.enqueue_dynamic_child_changes(
                store,
                &record.name,
                previous.contribution_generation,
                generation,
            )
            .await?;
        } else {
            self.seed_refresh_queue(generation, &record.name, &record.head_id)
                .await?;
        }
        let mut processed_nodes = 0usize;
        let mut reused_previous = same_head;

        loop {
            let batch = self.claim_refresh_batch(generation).await?;
            if batch.is_empty() {
                break;
            }
            if !reused_previous
                && previous.as_ref().is_some_and(|previous| {
                    batch.iter().any(|item| {
                        item.traversal == TraversalKind::Graph && item.node_id == previous.head_id
                    })
                })
            {
                let previous = previous
                    .as_ref()
                    .expect("previous branch should exist when its head is reached");
                self.copy_previous_contribution(
                    &record.name,
                    previous.contribution_generation,
                    generation,
                )
                .await?;
                self.enqueue_dynamic_child_changes(
                    store,
                    &record.name,
                    previous.contribution_generation,
                    generation,
                )
                .await?;
                reused_previous = true;
            }
            let reused_node_ids = if reused_previous {
                let previous = previous
                    .as_ref()
                    .expect("reused contribution should have a previous branch");
                self.previous_contribution_node_ids(
                    &record.name,
                    previous.contribution_generation,
                    &batch,
                )
                .await?
            } else {
                HashSet::new()
            };
            let traversal_batch = batch
                .iter()
                .filter(|item| !reused_node_ids.contains(&item.node_id))
                .cloned()
                .collect::<Vec<_>>();
            let ids = traversal_batch
                .iter()
                .map(|item| item.node_id.clone())
                .collect::<Vec<_>>();
            let nodes = self.load_nodes(store, &ids).await?;
            let mut pending = Vec::new();
            let mut child_rechecks = Vec::new();
            let mut queued_children = 0usize;
            for item in &traversal_batch {
                let node = required_node(&nodes, "node_id", &item.node_id)?;
                pending.extend(graph_parent_traversals(node));
                if needs_children(item.traversal, node) {
                    child_rechecks.push(item.clone());
                    queued_children += self
                        .enqueue_child_traversals(store, generation, &record.name, item, None)
                        .await?;
                }
            }
            self.commit_refresh_batch(generation, &record.name, &batch, &pending, &child_rechecks)
                .await?;
            processed_nodes += traversal_batch.len();
            #[cfg(test)]
            {
                self.traversed_node_count += traversal_batch.len();
            }
            tracing::info!(
                branch = %record.name,
                contribution_generation = generation,
                claimed_nodes = batch.len(),
                reused_nodes = reused_node_ids.len(),
                processed_nodes,
                queued_nodes = pending.len() + queued_children,
                "console graph source batch committed",
            );
            tokio::task::yield_now().await;
        }

        self.commit_branch(generation, record).await
    }

    async fn copy_previous_contribution(
        &self,
        branch: &str,
        previous_generation: i64,
        generation: i64,
    ) -> crate::Result<()> {
        let branch = branch.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                connection
                    .transaction::<_, diesel::result::Error, _>(|connection| {
                        diesel::sql_query(
                            "INSERT OR IGNORE INTO console_graph_source_branch_nodes ( \
                                 branch_name, contribution_generation, node_id \
                             ) \
                             SELECT branch_name, ?, node_id \
                             FROM console_graph_source_branch_nodes \
                             WHERE branch_name = ? AND contribution_generation = ?",
                        )
                        .bind::<BigInt, _>(generation)
                        .bind::<Text, _>(&branch)
                        .bind::<BigInt, _>(previous_generation)
                        .execute(connection)?;
                        diesel::sql_query(
                            "INSERT OR IGNORE INTO console_graph_source_child_rechecks ( \
                                 branch_name, contribution_generation, node_id, traversal_kind \
                             ) \
                             SELECT branch_name, ?, node_id, traversal_kind \
                             FROM console_graph_source_child_rechecks \
                             WHERE branch_name = ? AND contribution_generation = ?",
                        )
                        .bind::<BigInt, _>(generation)
                        .bind::<Text, _>(&branch)
                        .bind::<BigInt, _>(previous_generation)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn previous_contribution_node_ids(
        &self,
        branch: &str,
        previous_generation: i64,
        batch: &[QueueItem],
    ) -> crate::Result<HashSet<String>> {
        let node_ids = batch
            .iter()
            .map(|item| item.node_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        self.contribution_node_ids(branch, previous_generation, &node_ids)
            .await
    }

    async fn contribution_node_ids(
        &self,
        branch: &str,
        generation: i64,
        node_ids: &[String],
    ) -> crate::Result<HashSet<String>> {
        if node_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let node_ids = node_ids.to_vec();
        let branch = branch.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                console_graph_source_branch_nodes::table
                    .filter(console_graph_source_branch_nodes::branch_name.eq(branch))
                    .filter(
                        console_graph_source_branch_nodes::contribution_generation.eq(generation),
                    )
                    .filter(console_graph_source_branch_nodes::node_id.eq_any(node_ids))
                    .select(console_graph_source_branch_nodes::node_id)
                    .load::<String>(connection)
                    .map(|node_ids| node_ids.into_iter().collect())
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
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

    async fn dynamic_children_changed(
        &self,
        store: &SqliteGraphStore,
        branch: &str,
        generation: i64,
    ) -> crate::Result<bool> {
        let mut after = None;
        loop {
            let page = self
                .load_child_recheck_page(branch, generation, after.as_ref())
                .await?;
            if page.is_empty() {
                return Ok(false);
            }
            for item in &page {
                if self
                    .child_recheck_has_changes(store, branch, generation, item)
                    .await?
                {
                    return Ok(true);
                }
            }
            let complete = page.len() < SOURCE_CACHE_BATCH_SIZE;
            after = page.last().cloned();
            if complete {
                return Ok(false);
            }
            tokio::task::yield_now().await;
        }
    }

    async fn enqueue_dynamic_child_changes(
        &self,
        store: &SqliteGraphStore,
        branch: &str,
        previous_generation: i64,
        generation: i64,
    ) -> crate::Result<()> {
        let mut after = None;
        loop {
            let page = self
                .load_child_recheck_page(branch, previous_generation, after.as_ref())
                .await?;
            if page.is_empty() {
                return Ok(());
            }
            for item in &page {
                self.enqueue_child_traversals(
                    store,
                    generation,
                    branch,
                    item,
                    Some(previous_generation),
                )
                .await?;
            }
            let complete = page.len() < SOURCE_CACHE_BATCH_SIZE;
            after = page.last().cloned();
            if complete {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    async fn load_child_recheck_page(
        &self,
        branch: &str,
        generation: i64,
        after: Option<&QueueItem>,
    ) -> crate::Result<Vec<QueueItem>> {
        let branch = branch.to_owned();
        let after = after.map(|item| (item.node_id.clone(), item.traversal.as_str().to_owned()));
        let path = self.path.clone();
        let rows = self
            .database
            .with_connection(move |connection| {
                let rows = if let Some((node_id, traversal_kind)) = after {
                    diesel::sql_query(
                        "SELECT node_id, traversal_kind \
                         FROM console_graph_source_child_rechecks \
                         WHERE branch_name = ? AND contribution_generation = ? \
                           AND (node_id > ? OR (node_id = ? AND traversal_kind > ?)) \
                         ORDER BY node_id, traversal_kind \
                         LIMIT ?",
                    )
                    .bind::<Text, _>(&branch)
                    .bind::<BigInt, _>(generation)
                    .bind::<Text, _>(&node_id)
                    .bind::<Text, _>(&node_id)
                    .bind::<Text, _>(&traversal_kind)
                    .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                    .load::<ChildRecheckRow>(connection)
                } else {
                    diesel::sql_query(
                        "SELECT node_id, traversal_kind \
                         FROM console_graph_source_child_rechecks \
                         WHERE branch_name = ? AND contribution_generation = ? \
                         ORDER BY node_id, traversal_kind \
                         LIMIT ?",
                    )
                    .bind::<Text, _>(&branch)
                    .bind::<BigInt, _>(generation)
                    .bind::<BigInt, _>(SOURCE_CACHE_BATCH_SIZE as i64)
                    .load::<ChildRecheckRow>(connection)
                };
                rows.context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await?;
        rows.into_iter()
            .map(|row| {
                Ok(QueueItem {
                    node_id: row.node_id,
                    traversal: TraversalKind::parse(&row.traversal_kind)?,
                })
            })
            .collect()
    }

    async fn child_recheck_has_changes(
        &self,
        store: &SqliteGraphStore,
        branch: &str,
        generation: i64,
        item: &QueueItem,
    ) -> crate::Result<bool> {
        let mut cursor = None;
        loop {
            let (pending, next_cursor, complete) = self
                .load_child_traversal_page(store, item, cursor.as_ref())
                .await?;
            let node_ids = pending
                .iter()
                .map(|(node_id, _)| node_id.clone())
                .collect::<Vec<_>>();
            let existing = self
                .contribution_node_ids(branch, generation, &node_ids)
                .await?;
            if pending
                .iter()
                .any(|(node_id, _)| !existing.contains(node_id))
            {
                return Ok(true);
            }
            if complete {
                return Ok(false);
            }
            cursor = Some(next_cursor.expect("incomplete child page should provide a cursor"));
            tokio::task::yield_now().await;
        }
    }

    async fn enqueue_child_traversals(
        &self,
        store: &SqliteGraphStore,
        generation: i64,
        branch: &str,
        item: &QueueItem,
        previous_generation: Option<i64>,
    ) -> crate::Result<usize> {
        let mut cursor = None;
        let mut queued = 0usize;
        loop {
            let (mut pending, next_cursor, complete) = self
                .load_child_traversal_page(store, item, cursor.as_ref())
                .await?;
            if let Some(previous_generation) = previous_generation {
                let node_ids = pending
                    .iter()
                    .map(|(node_id, _)| node_id.clone())
                    .collect::<Vec<_>>();
                let existing = self
                    .contribution_node_ids(branch, previous_generation, &node_ids)
                    .await?;
                pending.retain(|(node_id, _)| !existing.contains(node_id));
            }
            queued += pending.len();
            self.enqueue_refresh_items(generation, branch, &pending)
                .await?;
            if complete {
                return Ok(queued);
            }
            cursor = Some(next_cursor.expect("incomplete child page should provide a cursor"));
            tokio::task::yield_now().await;
        }
    }

    async fn load_child_traversal_page(
        &self,
        store: &SqliteGraphStore,
        item: &QueueItem,
        cursor: Option<&GraphChildPageCursor>,
    ) -> crate::Result<(
        Vec<(String, TraversalKind)>,
        Option<GraphChildPageCursor>,
        bool,
    )> {
        let page_size = NonZeroUsize::new(SOURCE_CACHE_BATCH_SIZE)
            .expect("source cache batch size should be non-zero");
        let page = store
            .graph_child_ids_page(&item.node_id, cursor, page_size)
            .await
            .context(crate::error::StoreSnafu)?;
        let child_nodes = self.load_nodes(store, &page.child_ids).await?;
        let traversals = child_traversals_for_page(item, &page.child_ids, &child_nodes)?;
        Ok((traversals, page.next_cursor, page.complete))
    }

    async fn enqueue_refresh_items(
        &self,
        generation: i64,
        branch: &str,
        pending: &[(String, TraversalKind)],
    ) -> crate::Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let branch = branch.to_owned();
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
        child_rechecks: &[QueueItem],
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
        let child_rechecks = child_rechecks
            .iter()
            .filter(|item| !item.node_id.is_empty())
            .map(|item| (item.node_id.clone(), item.traversal))
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
                        for (node_id, traversal) in &child_rechecks {
                            diesel::sql_query(
                                "INSERT OR IGNORE INTO console_graph_source_child_rechecks ( \
                                     branch_name, contribution_generation, node_id, traversal_kind \
                                 ) VALUES (?, ?, ?, ?)",
                            )
                            .bind::<Text, _>(&branch)
                            .bind::<BigInt, _>(generation)
                            .bind::<Text, _>(node_id)
                            .bind::<Text, _>(traversal.as_str())
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
                        diesel::sql_query(
                            "DELETE FROM console_graph_source_child_rechecks \
                             WHERE branch_name = ? AND contribution_generation <> ?",
                        )
                        .bind::<Text, _>(&record.name)
                        .bind::<BigInt, _>(generation)
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
                        diesel::sql_query(
                            "DELETE FROM console_graph_source_child_rechecks \
                             WHERE branch_name = ?",
                        )
                        .bind::<Text, _>(&name)
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
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
                        diesel::sql_query(
                            "DELETE FROM console_graph_source_child_rechecks \
                             WHERE NOT EXISTS ( \
                                 SELECT 1 \
                                 FROM console_graph_source_branches \
                                 WHERE console_graph_source_branches.name = \
                                     console_graph_source_child_rechecks.branch_name \
                                   AND console_graph_source_branches.contribution_generation = \
                                     console_graph_source_child_rechecks.contribution_generation \
                             )",
                        )
                        .execute(connection)?;
                        Ok(())
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    pub(crate) async fn prune_published_orphans(&self) -> crate::Result<()> {
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
    pub(crate) fn branch_refresh_history(&self) -> &[String] {
        &self.branch_refresh_history
    }

    #[cfg(test)]
    pub(crate) fn fail_next_branch_refresh(&mut self, name: impl Into<String>) {
        self.fail_next_branch_refresh = Some(name.into());
    }

    #[cfg(test)]
    pub(crate) fn set_targeted_dynamic_branch_limit(&mut self, limit: usize) {
        self.targeted_dynamic_branch_limit = limit;
    }

    #[cfg(test)]
    pub(crate) fn set_full_refresh_branch_page_size(&mut self, page_size: NonZeroUsize) {
        self.full_refresh_branch_page_size = page_size;
    }

    #[cfg(test)]
    pub(crate) fn full_refresh_source_page_count(&self) -> usize {
        self.full_refresh_source_page_count
    }

    #[cfg(test)]
    pub(crate) fn traversed_node_count(&self) -> usize {
        self.traversed_node_count
    }

    #[cfg(test)]
    async fn published_branch_node_ids(&self, name: &str) -> crate::Result<BTreeSet<String>> {
        let published = self.published_branch(name).await?.with_context(|| {
            crate::error::InvalidGraphSnapshotStoreValueSnafu {
                column: "branch_name",
                value: name.to_owned(),
            }
        })?;
        let name = name.to_owned();
        let path = self.path.clone();
        self.database
            .with_connection(move |connection| {
                console_graph_source_branch_nodes::table
                    .filter(console_graph_source_branch_nodes::branch_name.eq(name))
                    .filter(
                        console_graph_source_branch_nodes::contribution_generation
                            .eq(published.contribution_generation),
                    )
                    .select(console_graph_source_branch_nodes::node_id)
                    .load::<String>(connection)
                    .map(|node_ids| node_ids.into_iter().collect())
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
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

fn needs_children(traversal: TraversalKind, node: &Node) -> bool {
    traversal == TraversalKind::SkillSubtree || node.kind.as_tool_uses().is_some()
}

fn branch_name_batch_after(names: &BTreeSet<String>, after: Option<&str>) -> Vec<String> {
    if let Some(after) = after {
        names
            .range((Excluded(after.to_owned()), Unbounded))
            .take(GRAPH_READ_BATCH_SIZE)
            .cloned()
            .collect()
    } else {
        names.iter().take(GRAPH_READ_BATCH_SIZE).cloned().collect()
    }
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

fn child_traversals_for_page(
    item: &QueueItem,
    child_ids: &[String],
    child_nodes: &HashMap<String, Node>,
) -> crate::Result<Vec<(String, TraversalKind)>> {
    match item.traversal {
        TraversalKind::SkillSubtree => Ok(child_ids
            .iter()
            .cloned()
            .map(|child_id| (child_id, TraversalKind::SkillSubtree))
            .collect()),
        TraversalKind::Graph => skill_invocation_traversals(child_ids, child_nodes),
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
    use coco_mem::{
        Anchor, Kind, MergeParent, Role, SkillInvocationAnchor, SkillInvocationMode,
        SkillResultAnchor, SqliteStore, ToolUse,
    };

    async fn append_text_chain(store: &SqliteStore, parent: &str, count: usize) -> Vec<String> {
        let mut parent = parent.to_owned();
        let mut node_ids = Vec::with_capacity(count);
        for index in 0..count {
            parent = store
                .append(NewNode {
                    parent,
                    role: Role::User,
                    metadata: None,
                    kind: Kind::Text(format!("chain node {index}")),
                })
                .await
                .unwrap();
            node_ids.push(parent.clone());
        }
        node_ids
    }

    async fn append_skill_subtree(
        store: &SqliteStore,
        tool_use: &str,
        skill_name: &str,
    ) -> (String, String) {
        let invocation = store
            .append(NewNode {
                parent: tool_use.to_owned(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_invocation(
                    Vec::new(),
                    SkillInvocationAnchor {
                        skill_name: skill_name.to_owned(),
                        mode: SkillInvocationMode::InheritContext,
                    },
                )),
            })
            .await
            .unwrap();
        let child = store
            .append(NewNode {
                parent: invocation.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Text(format!("{skill_name} child")),
            })
            .await
            .unwrap();
        (invocation, child)
    }

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
    async fn source_cache_refreshes_every_high_fan_out_skill_invocation() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "high-fan-out".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &root, &tool_use)
            .await
            .unwrap();
        let mut invocation_ids = BTreeSet::new();
        for index in 0..GRAPH_READ_BATCH_SIZE + 17 {
            invocation_ids.insert(
                writer
                    .append(NewNode {
                        parent: tool_use.clone(),
                        role: Role::System,
                        metadata: None,
                        kind: Kind::Anchor(Anchor::skill_invocation(
                            Vec::new(),
                            SkillInvocationAnchor {
                                skill_name: format!("fan-out-skill-{index}"),
                                mode: SkillInvocationMode::InheritContext,
                            },
                        )),
                    })
                    .await
                    .unwrap(),
            );
        }

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
        let actual = build_graph_snapshot_with_mode(&index.graph_store(), 11, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 11, GraphMode::All)
            .await
            .unwrap();
        let actual_ids = actual
            .nodes
            .iter()
            .map(|node| node.id.clone())
            .collect::<BTreeSet<_>>();

        assert!(invocation_ids.is_subset(&actual_ids));
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn source_cache_fast_forward_refresh_traverses_only_the_new_suffix() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let initial = append_text_chain(&writer, &root, SOURCE_CACHE_BATCH_SIZE + 17).await;
        let previous_head = initial.last().unwrap().clone();
        writer.fork("main", &previous_head).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let previous = index.published_branch("main").await.unwrap().unwrap();
        let traversed_before = index.traversed_node_count();

        let suffix = append_text_chain(&writer, &previous_head, 7).await;
        let next_head = suffix.last().unwrap().clone();
        writer
            .set_branch_head("main", &previous_head, &next_head)
            .await
            .unwrap();
        index
            .refresh_named_batch(&source, &["main".to_owned()])
            .await
            .unwrap();

        let current = index.published_branch("main").await.unwrap().unwrap();
        let mut expected_ids = BTreeSet::from([root]);
        expected_ids.extend(initial);
        expected_ids.extend(suffix.clone());
        assert_ne!(
            current.contribution_generation,
            previous.contribution_generation
        );
        assert_eq!(
            index.traversed_node_count() - traversed_before,
            suffix.len()
        );
        assert_eq!(
            index.published_branch_node_ids("main").await.unwrap(),
            expected_ids
        );
        let actual = build_graph_snapshot_with_mode(&index.graph_store(), 13, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 13, GraphMode::All)
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn source_cache_merge_extension_reuses_the_previous_contribution() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let initial = append_text_chain(&writer, &root, 3).await;
        let previous_head = initial.last().unwrap().clone();
        writer.fork("main", &previous_head).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let traversed_before = index.traversed_node_count();
        let other_parent = writer
            .append(NewNode {
                parent: root,
                role: Role::User,
                metadata: None,
                kind: Kind::Text("merge primary side".to_owned()),
            })
            .await
            .unwrap();
        let merge_head = writer
            .append(NewNode {
                parent: other_parent,
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::skill_result(
                    vec![MergeParent::merge(previous_head.clone())],
                    SkillResultAnchor {
                        skill_name: "merge-extension".to_owned(),
                        output: "complete".to_owned(),
                    },
                )),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &previous_head, &merge_head)
            .await
            .unwrap();

        index
            .refresh_named_batch(&source, &["main".to_owned()])
            .await
            .unwrap();

        assert_eq!(index.traversed_node_count() - traversed_before, 2);
        let actual = build_graph_snapshot_with_mode(&index.graph_store(), 17, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 17, GraphMode::All)
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn source_cache_same_head_refresh_updates_only_branch_state() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("base", &root).await.unwrap();
        writer.fork("main", &root).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let previous = index.published_branch("main").await.unwrap().unwrap();
        let traversed_before = index.traversed_node_count();
        let next_state = SessionState::Attached {
            target_branch: "base".to_owned(),
            base_head_id: root,
        };
        writer
            .set_session_state("main", Some(&SessionState::Active), next_state.clone())
            .await
            .unwrap();

        index
            .refresh_named_batch(&source, &["main".to_owned()])
            .await
            .unwrap();

        let current = index.published_branch("main").await.unwrap().unwrap();
        assert_eq!(current.head_id, previous.head_id);
        assert_eq!(
            current.contribution_generation,
            previous.contribution_generation
        );
        assert_eq!(index.traversed_node_count(), traversed_before);
        assert_eq!(
            index.graph_store().get_session_state("main").await.unwrap(),
            next_state
        );
    }

    #[tokio::test]
    async fn source_cache_same_head_refresh_discovers_new_skill_subtree() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "same-head-tool-use".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &root, &tool_use)
            .await
            .unwrap();
        for index in 0..SOURCE_CACHE_BATCH_SIZE + 1 {
            writer
                .append(NewNode {
                    parent: tool_use.clone(),
                    role: Role::System,
                    metadata: None,
                    kind: Kind::Text(format!("irrelevant tool child {index}")),
                })
                .await
                .unwrap();
        }
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let previous = index.published_branch("main").await.unwrap().unwrap();

        let (invocation, child) =
            append_skill_subtree(&writer, &tool_use, "same-head-dynamic").await;
        index
            .refresh_named_batch(&source, &["main".to_owned()])
            .await
            .unwrap();

        let current = index.published_branch("main").await.unwrap().unwrap();
        assert_ne!(
            current.contribution_generation,
            previous.contribution_generation
        );
        assert_eq!(
            index.published_branch_node_ids("main").await.unwrap(),
            BTreeSet::from([root, tool_use, invocation, child])
        );
        let actual = build_graph_snapshot_with_mode(&index.graph_store(), 19, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 19, GraphMode::All)
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn source_cache_refreshes_branches_sharing_a_dynamic_skill_parent() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "shared-dynamic-parent".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer.fork("branch-a", &tool_use).await.unwrap();
        writer.fork("branch-b", &tool_use).await.unwrap();
        writer.fork("unrelated", &root).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();

        let refreshes_before = index.branch_refresh_count();
        let (invocation, child) =
            append_skill_subtree(&writer, &tool_use, "shared-dynamic-child").await;
        index
            .refresh_named_batch(&source, &["branch-a".to_owned()])
            .await
            .unwrap();

        let shared_nodes = BTreeSet::from([
            root.clone(),
            tool_use.clone(),
            invocation.clone(),
            child.clone(),
        ]);
        assert_eq!(index.branch_refresh_count() - refreshes_before, 2);
        assert_eq!(
            index.published_branch_node_ids("branch-a").await.unwrap(),
            shared_nodes
        );
        assert_eq!(
            index.published_branch_node_ids("branch-b").await.unwrap(),
            shared_nodes
        );
        assert_eq!(
            index.published_branch_node_ids("unrelated").await.unwrap(),
            BTreeSet::from([root.clone()])
        );

        writer
            .set_branch_head("branch-a", &tool_use, &root)
            .await
            .unwrap();
        let refresh_history_start = index.branch_refresh_history().len();
        index.fail_next_branch_refresh("branch-b");
        assert!(
            index
                .refresh_named_batch(&source, &["branch-a".to_owned()])
                .await
                .is_err()
        );
        assert_eq!(
            &index.branch_refresh_history()[refresh_history_start..],
            &["branch-b".to_owned()]
        );
        assert_eq!(
            index
                .published_branch("branch-a")
                .await
                .unwrap()
                .unwrap()
                .head_id,
            tool_use
        );
        index
            .refresh_named_batch(&source, &["branch-a".to_owned()])
            .await
            .unwrap();
        assert_eq!(
            &index.branch_refresh_history()[refresh_history_start..],
            &[
                "branch-b".to_owned(),
                "branch-b".to_owned(),
                "branch-a".to_owned()
            ]
        );
        index.prune_published_orphans().await.unwrap();
        assert_eq!(
            index.published_branch_node_ids("branch-a").await.unwrap(),
            BTreeSet::from([root.clone()])
        );
        assert_eq!(
            index.published_branch_node_ids("branch-b").await.unwrap(),
            shared_nodes
        );

        writer.delete_branch("branch-a").await.unwrap();
        index
            .refresh_named_batch(&source, &["branch-a".to_owned()])
            .await
            .unwrap();
        index.prune_published_orphans().await.unwrap();
        assert!(index.published_branch("branch-a").await.unwrap().is_none());
        assert_eq!(
            index.published_branch_node_ids("branch-b").await.unwrap(),
            shared_nodes
        );
        assert_eq!(
            index.graph_store().get_node(&child).await.unwrap().id,
            child
        );
    }

    #[tokio::test]
    async fn source_cache_refreshes_post_peers_for_a_new_branch() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "new-branch-dynamic-parent".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer.fork("branch-b", &tool_use).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();

        let (invocation, child) =
            append_skill_subtree(&writer, &tool_use, "new-branch-dynamic-child").await;
        writer.fork("branch-new", &tool_use).await.unwrap();
        let refresh_history_start = index.branch_refresh_history().len();
        index
            .refresh_named_batch(&source, &["branch-new".to_owned()])
            .await
            .unwrap();

        assert_eq!(
            &index.branch_refresh_history()[refresh_history_start..],
            &["branch-new".to_owned(), "branch-b".to_owned()]
        );
        let expected = BTreeSet::from([root, tool_use, invocation, child]);
        assert_eq!(
            index.published_branch_node_ids("branch-new").await.unwrap(),
            expected
        );
        assert_eq!(
            index.published_branch_node_ids("branch-b").await.unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn source_cache_refreshes_peers_before_a_direct_branch_delete() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "direct-delete-dynamic-parent".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer.fork("branch-a", &tool_use).await.unwrap();
        writer.fork("branch-b", &tool_use).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();

        let (invocation, child) =
            append_skill_subtree(&writer, &tool_use, "direct-delete-dynamic-child").await;
        index
            .refresh_named_batch(&source, &["branch-a".to_owned()])
            .await
            .unwrap();
        writer.delete_branch("branch-a").await.unwrap();

        let refresh_history_start = index.branch_refresh_history().len();
        index
            .refresh_named_batch(&source, &["branch-a".to_owned()])
            .await
            .unwrap();
        assert_eq!(
            &index.branch_refresh_history()[refresh_history_start..],
            &["branch-b".to_owned()]
        );

        index.prune_published_orphans().await.unwrap();
        assert!(index.published_branch("branch-a").await.unwrap().is_none());
        assert_eq!(
            index.published_branch_node_ids("branch-b").await.unwrap(),
            BTreeSet::from([root, tool_use, invocation, child.clone()])
        );
        assert_eq!(
            index.graph_store().get_node(&child).await.unwrap().id,
            child
        );
    }

    #[tokio::test]
    async fn full_fallback_pages_branches_above_the_targeted_limit() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "paged-fallback-dynamic-parent".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        for index in 0..5 {
            let name = format!("branch-{index:02}");
            writer.fork(&name, &tool_use).await.unwrap();
        }
        writer.fork("stale", &root).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();

        let (invocation, child) =
            append_skill_subtree(&writer, &tool_use, "paged-fallback-dynamic-child").await;
        writer.delete_branch("stale").await.unwrap();
        index.set_targeted_dynamic_branch_limit(4);
        index.set_full_refresh_branch_page_size(NonZeroUsize::new(2).unwrap());
        let page_count_before = index.full_refresh_source_page_count();
        let refresh_history_start = index.branch_refresh_history().len();

        index
            .refresh_named_batch(&source, &["branch-00".to_owned()])
            .await
            .unwrap();

        assert_eq!(
            index.full_refresh_source_page_count() - page_count_before,
            3
        );
        assert_eq!(
            &index.branch_refresh_history()[refresh_history_start..],
            &[
                "branch-01".to_owned(),
                "branch-02".to_owned(),
                "branch-03".to_owned(),
                "branch-04".to_owned(),
                "branch-00".to_owned(),
            ]
        );
        assert!(index.published_branch("stale").await.unwrap().is_none());
        let expected = BTreeSet::from([root, tool_use, invocation, child]);
        assert_eq!(
            index.published_branch_node_ids("branch-00").await.unwrap(),
            expected
        );
        assert_eq!(
            index.published_branch_node_ids("branch-04").await.unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn unknown_deleted_branch_invalidation_falls_back_to_full_source_refresh() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "fallback-dynamic-parent".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer.fork("survivor", &tool_use).await.unwrap();

        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();

        let (invocation, child) =
            append_skill_subtree(&writer, &tool_use, "fallback-dynamic-child").await;
        index
            .refresh_named_batch(&source, &["never-observed-deleted-branch".to_owned()])
            .await
            .unwrap();

        assert_eq!(
            index.published_branch_node_ids("survivor").await.unwrap(),
            BTreeSet::from([root, tool_use, invocation, child])
        );
    }

    #[tokio::test]
    async fn source_cache_fast_forward_refresh_discovers_new_skill_subtree() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        writer.fork("main", &root).await.unwrap();
        let tool_use = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_use(ToolUse {
                    id: "fast-forward-tool-use".to_owned(),
                    name: "skill".to_owned(),
                    input: serde_json::json!({}),
                }),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("main", &root, &tool_use)
            .await
            .unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();

        let suffix = writer
            .append(NewNode {
                parent: tool_use.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("fast-forward suffix".to_owned()),
            })
            .await
            .unwrap();
        let (invocation, child) =
            append_skill_subtree(&writer, &tool_use, "fast-forward-dynamic").await;
        writer
            .set_branch_head("main", &tool_use, &suffix)
            .await
            .unwrap();
        index
            .refresh_named_batch(&source, &["main".to_owned()])
            .await
            .unwrap();

        assert_eq!(
            index.published_branch_node_ids("main").await.unwrap(),
            BTreeSet::from([root, tool_use, suffix, invocation, child])
        );
        let actual = build_graph_snapshot_with_mode(&index.graph_store(), 23, GraphMode::All)
            .await
            .unwrap();
        let expected = build_graph_snapshot_with_mode(&writer, 23, GraphMode::All)
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn source_cache_rewind_and_diverge_drop_the_previous_suffix() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let initial = append_text_chain(&writer, &root, 3).await;
        let base = initial[0].clone();
        let old_suffix = BTreeSet::from([initial[1].clone(), initial[2].clone()]);
        let previous_head = initial[2].clone();
        writer.fork("diverge", &previous_head).await.unwrap();
        writer.fork("rewind", &previous_head).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        let traversed_before = index.traversed_node_count();
        let diverged = writer
            .append(NewNode {
                parent: base.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("diverged".to_owned()),
            })
            .await
            .unwrap();
        writer
            .set_branch_head("diverge", &previous_head, &diverged)
            .await
            .unwrap();
        writer
            .set_branch_head("rewind", &previous_head, &base)
            .await
            .unwrap();

        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();

        assert_eq!(index.traversed_node_count() - traversed_before, 5);
        assert_eq!(
            index.published_branch_node_ids("rewind").await.unwrap(),
            BTreeSet::from([root.clone(), base.clone()])
        );
        assert_eq!(
            index.published_branch_node_ids("diverge").await.unwrap(),
            BTreeSet::from([root, base, diverged])
        );
        let retained = index
            .graph_store()
            .list_children(&writer.root_id())
            .await
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<BTreeSet<_>>();
        assert!(old_suffix.is_disjoint(&retained));
    }

    #[tokio::test]
    async fn source_cache_retains_superseded_facts_until_explicit_prune() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let superseded = writer
            .append(NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("superseded".to_owned()),
            })
            .await
            .unwrap();
        writer.fork("main", &superseded).await.unwrap();
        let source = SqliteGraphStore::open_read_only(writer.store_path())
            .await
            .unwrap();
        let snapshots = ConsoleGraphSnapshotStore::open(writer.store_path())
            .await
            .unwrap();
        let mut index = PersistentGraphIndex::open(&snapshots, root.clone())
            .await
            .unwrap();
        index
            .refresh_records(&source, source.graph_branches().await.unwrap())
            .await
            .unwrap();
        writer
            .set_branch_head("main", &superseded, &root)
            .await
            .unwrap();

        index
            .refresh_named_batch(&source, &["main".to_owned()])
            .await
            .unwrap();

        assert_eq!(index.node_count().await.unwrap(), 2);
        assert_eq!(
            index.graph_store().get_node(&superseded).await.unwrap().id,
            superseded
        );
        index.prune_published_orphans().await.unwrap();
        assert_eq!(index.node_count().await.unwrap(), 1);
        assert!(index.graph_store().get_node(&superseded).await.is_err());
    }

    #[tokio::test]
    async fn deleting_branch_waits_for_explicit_source_prune() {
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
        assert_eq!(index.node_count().await.unwrap(), 1);
        index.prune_published_orphans().await.unwrap();
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
        let published = index.published_branch("main").await.unwrap().unwrap();
        let stale_generation = index.allocate_generation().await.unwrap();
        index
            .seed_refresh_queue(stale_generation, "main", &root)
            .await
            .unwrap();
        index
            .copy_previous_contribution("main", published.contribution_generation, stale_generation)
            .await
            .unwrap();
        let batch = index.claim_refresh_batch(stale_generation).await.unwrap();
        index
            .commit_refresh_batch(stale_generation, "main", &batch, &[], &[])
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
