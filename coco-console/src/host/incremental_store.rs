use async_trait::async_trait;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Text};
use snafu::ResultExt;

use super::frontier::{ExternalFrontierStore, StoreMutation, StoreReplace};
use super::snapshot_store::SnapshotDatabase;
use crate::error::QueryGraphSnapshotStoreSnafu;

const SELECT_PENDING_COUNT: &str = "
    SELECT COUNT(*) AS count
    FROM console_graph_build_frontier
    WHERE run_id = ? AND pending = 1
";
const SELECT_PENDING_PREFIX: &str = "
    SELECT created_at_ns, node_id
    FROM console_graph_build_frontier
    WHERE run_id = ? AND pending = 1
    ORDER BY created_at_ns ASC, node_id ASC
    LIMIT ?
";
const SELECT_ALL_PENDING: &str = "
    SELECT created_at_ns, node_id
    FROM console_graph_build_frontier
    WHERE run_id = ? AND pending = 1
    ORDER BY created_at_ns ASC, node_id ASC
";
const SELECT_PENDING_MINIMUM: &str = "
    SELECT created_at_ns, node_id
    FROM console_graph_build_frontier
    WHERE run_id = ? AND pending = 1
    ORDER BY created_at_ns ASC, node_id ASC
    LIMIT 1
";
const MARK_SEEN: &str = "
    UPDATE console_graph_build_frontier
    SET pending = 0
    WHERE run_id = ?
      AND pending = 1
      AND created_at_ns = ?
      AND node_id = ?
";
const INSERT_PENDING_IF_UNSEEN: &str = "
    INSERT INTO console_graph_build_frontier (
        run_id,
        created_at_ns,
        node_id,
        pending
    ) VALUES (?, ?, ?, 1)
    ON CONFLICT (run_id, node_id) DO NOTHING
";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FrontierNode {
    pub created_at_ns: i64,
    pub node_id: String,
}

impl FrontierNode {
    pub fn new(created_at_ns: i64, node_id: impl Into<String>) -> Self {
        Self {
            created_at_ns,
            node_id: node_id.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SqliteFrontierStore {
    database: SnapshotDatabase,
    run_id: i64,
}

impl SqliteFrontierStore {
    pub fn new(database: SnapshotDatabase, run_id: i64) -> Self {
        Self { database, run_id }
    }
}

#[derive(Debug, QueryableByName)]
struct FrontierRow {
    #[diesel(sql_type = BigInt)]
    created_at_ns: i64,
    #[diesel(sql_type = Text)]
    node_id: String,
}

impl From<FrontierRow> for FrontierNode {
    fn from(row: FrontierRow) -> Self {
        Self {
            created_at_ns: row.created_at_ns,
            node_id: row.node_id,
        }
    }
}

#[derive(Debug, QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[async_trait]
impl ExternalFrontierStore<FrontierNode> for SqliteFrontierStore {
    type Error = crate::Error;

    async fn pending_len(&self) -> Result<usize, Self::Error> {
        let run_id = self.run_id;
        let path = self.database.path().to_owned();
        self.database
            .with_connection(move |connection| {
                let row = diesel::sql_query(SELECT_PENDING_COUNT)
                    .bind::<BigInt, _>(run_id)
                    .get_result::<CountRow>(connection)
                    .context(QueryGraphSnapshotStoreSnafu { path })?;
                Ok(count_as_usize(row.count))
            })
            .await
    }

    async fn ordered_prefix(&self, limit: usize) -> Result<Vec<FrontierNode>, Self::Error> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let run_id = self.run_id;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let path = self.database.path().to_owned();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(SELECT_PENDING_PREFIX)
                    .bind::<BigInt, _>(run_id)
                    .bind::<BigInt, _>(limit)
                    .load::<FrontierRow>(connection)
                    .map(|rows| rows.into_iter().map(FrontierNode::from).collect())
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn load_all(&self) -> Result<Vec<FrontierNode>, Self::Error> {
        let run_id = self.run_id;
        let path = self.database.path().to_owned();
        self.database
            .with_connection(move |connection| {
                diesel::sql_query(SELECT_ALL_PENDING)
                    .bind::<BigInt, _>(run_id)
                    .load::<FrontierRow>(connection)
                    .map(|rows| rows.into_iter().map(FrontierNode::from).collect())
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }

    async fn replace(
        &mut self,
        expected_min: Option<&FrontierNode>,
        additions: &[FrontierNode],
    ) -> Result<StoreReplace<FrontierNode>, Self::Error> {
        let run_id = self.run_id;
        let expected_min = expected_min.cloned();
        let additions = additions.to_vec();
        let path = self.database.path().to_owned();
        self.database
            .with_connection(move |connection| {
                connection
                    .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                        let current_min = diesel::sql_query(SELECT_PENDING_MINIMUM)
                            .bind::<BigInt, _>(run_id)
                            .get_result::<FrontierRow>(connection)
                            .optional()?
                            .map(FrontierNode::from);

                        if expected_min
                            .as_ref()
                            .is_some_and(|expected| current_min.as_ref() != Some(expected))
                        {
                            return Ok(StoreReplace::StaleMinimum);
                        }

                        if let Some(expected) = expected_min.as_ref() {
                            let updated = diesel::sql_query(MARK_SEEN)
                                .bind::<BigInt, _>(run_id)
                                .bind::<BigInt, _>(expected.created_at_ns)
                                .bind::<Text, _>(&expected.node_id)
                                .execute(connection)?;
                            if updated != 1 {
                                return Ok(StoreReplace::StaleMinimum);
                            }
                        }

                        let mut inserted = Vec::new();
                        for addition in additions {
                            let affected = diesel::sql_query(INSERT_PENDING_IF_UNSEEN)
                                .bind::<BigInt, _>(run_id)
                                .bind::<BigInt, _>(addition.created_at_ns)
                                .bind::<Text, _>(&addition.node_id)
                                .execute(connection)?;
                            if affected == 1 {
                                inserted.push(addition);
                            }
                        }

                        let pending_len = diesel::sql_query(SELECT_PENDING_COUNT)
                            .bind::<BigInt, _>(run_id)
                            .get_result::<CountRow>(connection)
                            .map(|row| count_as_usize(row.count))?;

                        Ok(StoreReplace::Applied(StoreMutation {
                            inserted,
                            pending_len,
                        }))
                    })
                    .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
    }
}

fn count_as_usize(count: i64) -> usize {
    usize::try_from(count.max(0)).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use diesel::sql_types::Integer;

    use super::super::snapshot_store::ConsoleGraphSnapshotStore;
    use super::*;

    #[derive(Debug, PartialEq, Eq, QueryableByName)]
    struct SeenRow {
        #[diesel(sql_type = BigInt)]
        created_at_ns: i64,
        #[diesel(sql_type = Text)]
        node_id: String,
        #[diesel(sql_type = Integer)]
        pending: i32,
    }

    #[tokio::test]
    async fn orders_pending_nodes_and_preserves_lifetime_seen_membership() {
        let database = database_with_runs(&[1]).await;
        let mut store = SqliteFrontierStore::new(database, 1);
        let original = vec![node(5, "b"), node(1, "z"), node(1, "a")];

        assert_eq!(
            store.replace(None, &original).await.unwrap(),
            StoreReplace::Applied(StoreMutation {
                inserted: original,
                pending_len: 3,
            })
        );
        assert_eq!(store.pending_len().await.unwrap(), 3);
        assert_eq!(
            store.ordered_prefix(2).await.unwrap(),
            vec![node(1, "a"), node(1, "z")]
        );
        assert_eq!(
            store.load_all().await.unwrap(),
            vec![node(1, "a"), node(1, "z"), node(5, "b")]
        );

        let minimum = node(1, "a");
        let new_node = node(2, "m");
        assert_eq!(
            store
                .replace(Some(&minimum), &[minimum.clone(), new_node.clone()])
                .await
                .unwrap(),
            StoreReplace::Applied(StoreMutation {
                inserted: vec![new_node],
                pending_len: 3,
            })
        );
        assert_eq!(
            store.load_all().await.unwrap(),
            vec![node(1, "z"), node(2, "m"), node(5, "b")]
        );

        assert_eq!(
            store.replace(None, &[node(0, "a")]).await.unwrap(),
            StoreReplace::Applied(StoreMutation {
                inserted: Vec::new(),
                pending_len: 3,
            })
        );
        assert_eq!(store.ordered_prefix(0).await.unwrap(), Vec::new());
    }

    #[tokio::test]
    async fn stale_minimum_performs_no_writes() {
        let database = database_with_runs(&[7]).await;
        let mut store = SqliteFrontierStore::new(database, 7);
        store
            .replace(None, &[node(1, "a"), node(2, "b")])
            .await
            .unwrap();
        let before = seen_rows(&store).await;

        assert_eq!(
            store
                .replace(Some(&node(2, "b")), &[node(0, "new")])
                .await
                .unwrap(),
            StoreReplace::StaleMinimum
        );

        assert_eq!(seen_rows(&store).await, before);
        assert_eq!(
            store.load_all().await.unwrap(),
            vec![node(1, "a"), node(2, "b")]
        );
    }

    #[tokio::test]
    async fn minimum_validation_and_queries_are_isolated_by_run() {
        let database = database_with_runs(&[11, 12]).await;
        let mut first = SqliteFrontierStore::new(database.clone(), 11);
        let mut second = SqliteFrontierStore::new(database, 12);
        first.replace(None, &[node(10, "first")]).await.unwrap();
        second.replace(None, &[node(0, "second")]).await.unwrap();

        assert_eq!(
            first.replace(Some(&node(10, "first")), &[]).await.unwrap(),
            StoreReplace::Applied(StoreMutation {
                inserted: Vec::new(),
                pending_len: 0,
            })
        );
        assert!(first.load_all().await.unwrap().is_empty());
        assert_eq!(second.load_all().await.unwrap(), vec![node(0, "second")]);
    }

    #[tokio::test]
    async fn concurrent_replacements_apply_the_expected_minimum_once() {
        let database = database_with_runs(&[20]).await;
        let mut seed = SqliteFrontierStore::new(database.clone(), 20);
        let minimum = node(1, "minimum");
        seed.replace(None, std::slice::from_ref(&minimum))
            .await
            .unwrap();
        let mut first = SqliteFrontierStore::new(database.clone(), 20);
        let mut second = SqliteFrontierStore::new(database, 20);
        let first_addition = node(2, "first");
        let second_addition = node(2, "second");

        let (first_result, second_result) = tokio::join!(
            first.replace(Some(&minimum), std::slice::from_ref(&first_addition)),
            second.replace(Some(&minimum), std::slice::from_ref(&second_addition))
        );
        let results = [first_result.unwrap(), second_result.unwrap()];

        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, StoreReplace::Applied(_)))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, StoreReplace::StaleMinimum))
                .count(),
            1
        );
        let pending = seed.load_all().await.unwrap();
        assert!(pending == vec![first_addition] || pending == vec![second_addition]);
    }

    async fn database_with_runs(run_ids: &[i64]) -> SnapshotDatabase {
        let snapshots = ConsoleGraphSnapshotStore::open(temp_dir()).await.unwrap();
        let database = snapshots.database();
        let run_ids = run_ids.to_vec();
        let path = database.path().to_owned();
        database
            .with_connection(move |connection| {
                for run_id in run_ids {
                    diesel::sql_query(
                        "INSERT INTO console_graph_build_runs \
                         (run_id, source_version, status, owner_id, lease_expires_at_ms) \
                         VALUES (?, 0, 'running', 'frontier-test', 0)",
                    )
                    .bind::<BigInt, _>(run_id)
                    .execute(connection)
                    .context(QueryGraphSnapshotStoreSnafu { path: path.clone() })?;
                }
                Ok(())
            })
            .await
            .unwrap();
        database
    }

    async fn seen_rows(store: &SqliteFrontierStore) -> Vec<SeenRow> {
        let run_id = store.run_id;
        let path = store.database.path().to_owned();
        store
            .database
            .with_connection(move |connection| {
                diesel::sql_query(
                    "SELECT created_at_ns, node_id, pending \
                     FROM console_graph_build_frontier \
                     WHERE run_id = ? \
                     ORDER BY created_at_ns ASC, node_id ASC",
                )
                .bind::<BigInt, _>(run_id)
                .load::<SeenRow>(connection)
                .context(QueryGraphSnapshotStoreSnafu { path })
            })
            .await
            .unwrap()
    }

    fn node(created_at_ns: i64, node_id: &str) -> FrontierNode {
        FrontierNode::new(created_at_ns, node_id)
    }

    fn temp_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "coco-console-frontier-{}-{nonce}-{counter}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
