use super::*;

#[derive(diesel::QueryableByName)]
struct GraphRelationRevision {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    current_revision: i64,
}

#[derive(diesel::QueryableByName)]
struct RelationRevisionBounds {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    minimum_revision: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    maximum_revision: Option<i64>,
}

#[derive(diesel::QueryableByName)]
struct QueryPlanDetail {
    #[diesel(sql_type = diesel::sql_types::Text)]
    detail: String,
}

#[tokio::test]
async fn graph_relation_revision_migration_round_trips_existing_relations() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let child_id = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("existing child".to_owned()),
        })
        .await
        .unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    diesel::connection::SimpleConnection::batch_execute(
        &mut connection,
        "PRAGMA foreign_keys = ON",
    )
    .unwrap();

    connection
        .revert_last_migration(super::STORE_MIGRATIONS)
        .unwrap();
    connection
        .revert_last_migration(super::STORE_MIGRATIONS)
        .unwrap();
    diesel::RunQueryDsl::execute(
        diesel::sql_query(
            "INSERT INTO node_relations (child_node_id, parent_node_id, kind, ordinal) \
             VALUES (?, ?, 'migration_test', 0)",
        )
        .bind::<diesel::sql_types::Text, _>(&child_id)
        .bind::<diesel::sql_types::Text, _>(&root_id),
        &mut connection,
    )
    .unwrap();
    connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap();

    let adjacency = diesel::RunQueryDsl::get_result::<ColumnCount>(
        diesel::sql_query(
            "SELECT COUNT(*) AS count FROM graph_child_adjacency \
             WHERE parent_node_id = ? AND child_node_id = ?",
        )
        .bind::<diesel::sql_types::Text, _>(&root_id)
        .bind::<diesel::sql_types::Text, _>(&child_id),
        &mut connection,
    )
    .unwrap();
    assert_eq!(adjacency.count, 1);
    let query_plan = diesel::RunQueryDsl::load::<QueryPlanDetail>(
        diesel::sql_query(
            "EXPLAIN QUERY PLAN \
             SELECT child_node_id FROM graph_child_adjacency \
             WHERE parent_node_id = 'parent' AND first_created_revision <= 10 \
               AND (first_created_revision > 3 \
                    OR (first_created_revision = 3 AND child_node_id > 'child')) \
             ORDER BY first_created_revision, child_node_id LIMIT 129",
        ),
        &mut connection,
    )
    .unwrap();
    assert!(
        query_plan
            .iter()
            .any(|row| { row.detail.contains("graph_child_adjacency_page_idx") })
    );
    assert!(
        query_plan
            .iter()
            .all(|row| !row.detail.contains("USE TEMP B-TREE"))
    );
    connection
        .revert_last_migration(super::STORE_MIGRATIONS)
        .unwrap();
    connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap();

    let revision = diesel::RunQueryDsl::get_result::<GraphRelationRevision>(
        diesel::sql_query("SELECT current_revision FROM graph_relation_state WHERE singleton = 1"),
        &mut connection,
    )
    .unwrap();
    assert_eq!(revision.current_revision, 0);
    let bounds = diesel::RunQueryDsl::get_result::<RelationRevisionBounds>(
        diesel::sql_query(
            "SELECT MIN(created_revision) AS minimum_revision, \
             MAX(created_revision) AS maximum_revision FROM node_relations",
        ),
        &mut connection,
    )
    .unwrap();
    assert_eq!(bounds.minimum_revision, Some(0));
    assert_eq!(bounds.maximum_revision, Some(0));
    connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let graph = SqliteGraphStore::open_read_only(&path).await.unwrap();
    assert_eq!(graph.graph_relation_revision().await.unwrap(), 0);
    let page = graph
        .graph_child_ids_page_at_revision(&root_id, None, 0, NonZeroUsize::new(1).unwrap())
        .await
        .unwrap();
    assert_eq!(page.child_ids, vec![child_id]);
}
