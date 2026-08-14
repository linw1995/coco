use std::collections::HashSet;

use super::*;

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[tokio::test]
async fn writable_open_adds_legacy_instances_without_inventing_node_origins() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root = store.root_id();
    let detached = store
        .append(NewNode {
            parent: root.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("legacy detached".to_owned()),
        })
        .await
        .unwrap();
    store.fork("main", &root).await.unwrap();
    store.fork("attached", &root).await.unwrap();
    store.fork("paused", &root).await.unwrap();
    store.fork("shared", &root).await.unwrap();
    store
        .set_session_state(
            "attached",
            Some(&SessionState::Active),
            SessionState::Attached {
                target_branch: "main".to_owned(),
                base_head_id: root.clone(),
            },
        )
        .await
        .unwrap();
    store
        .set_session_state(
            "paused",
            Some(&SessionState::Active),
            SessionState::Paused {
                target_branch: String::new(),
                reason: PauseReason::Closed,
            },
        )
        .await
        .unwrap();
    let legacy_job = store
        .submit_job_with_id("legacy-job", "main", &root)
        .await
        .unwrap();
    drop(store);

    let database_path = sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 25);
    drop(connection);

    let error = SqliteStore::open_read_only(&path).await.unwrap_err();
    assert!(error.to_string().contains("version 25, expected 26"));

    let reopened = SqliteStore::open(&path).await.unwrap();
    assert_eq!(reopened.schema_version().await.unwrap(), 26);
    assert_eq!(reopened.get_branch_head("main").await.unwrap(), root);
    assert_eq!(reopened.get_branch_head("shared").await.unwrap(), root);
    assert_eq!(
        reopened.get_session_state("attached").await.unwrap(),
        SessionState::Attached {
            target_branch: "main".to_owned(),
            base_head_id: root.clone(),
        }
    );
    assert_eq!(
        reopened.get_session_state("paused").await.unwrap(),
        SessionState::Paused {
            target_branch: String::new(),
            reason: PauseReason::Closed,
        }
    );
    assert_eq!(reopened.get_job("legacy-job").await.unwrap(), legacy_job);
    assert_eq!(reopened.get_node(&detached).await.unwrap().id, detached);

    let graph = SqliteGraphStore::open_read_only(&path).await.unwrap();
    let branches = graph.graph_branches().await.unwrap();
    assert_eq!(branches.len(), 4);
    assert_eq!(
        branches
            .iter()
            .map(|branch| branch.instance_id.clone())
            .collect::<HashSet<_>>()
            .len(),
        4
    );
    assert!(
        graph
            .graph_node_records_by_ids(&[root.clone(), detached.clone()])
            .await
            .unwrap()
            .into_iter()
            .all(|node| node.origin.is_none())
    );
    drop(graph);

    let mut connection = reopened.connect().await.unwrap();
    let legacy_instances = diesel::sql_query(
        "SELECT COUNT(*) AS count FROM branch_instances WHERE created_at IS NULL",
    )
    .get_result::<CountRow>(&mut connection)
    .await
    .unwrap()
    .count;
    assert_eq!(legacy_instances, 4);
    let origins = diesel::sql_query("SELECT COUNT(*) AS count FROM node_origins")
        .get_result::<CountRow>(&mut connection)
        .await
        .unwrap()
        .count;
    assert_eq!(origins, 0);
}

#[tokio::test]
async fn branch_aware_append_after_v25_migration_uses_the_legacy_instance() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root = store.root_id();
    store.fork("main", &root).await.unwrap();
    drop(store);

    let database_path = sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 25);
    drop(connection);

    let store = SqliteStore::open(&path).await.unwrap();
    let appended = store
        .append_on_branch(
            "main",
            NewNode {
                parent: root.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("after migration".to_owned()),
            },
        )
        .await
        .unwrap();
    let graph = SqliteGraphStore::open_read_only(&path).await.unwrap();
    let branch = graph.graph_branches().await.unwrap().pop().unwrap();
    let records = graph
        .graph_node_records_by_ids(&[root, appended.clone()])
        .await
        .unwrap();
    assert!(
        records
            .iter()
            .find(|record| record.id != appended)
            .unwrap()
            .origin
            .is_none()
    );
    assert_eq!(
        records
            .iter()
            .find(|record| record.id == appended)
            .unwrap()
            .origin
            .as_ref()
            .unwrap()
            .branch_instance_id,
        branch.instance_id
    );
}

#[tokio::test]
async fn down_migration_discards_provenance_and_restores_v25_shape() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root = store.root_id();
    store.fork("main", &root).await.unwrap();
    drop(store);

    let database_path = sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 25);

    let tables = diesel::RunQueryDsl::get_result::<CountRow>(
        diesel::sql_query(
            "SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'table' AND name IN ('branch_instances', 'node_origins')",
        ),
        &mut connection,
    )
    .unwrap()
    .count;
    assert_eq!(tables, 0);
    let instance_columns = diesel::RunQueryDsl::get_result::<CountRow>(
        diesel::sql_query(
            "SELECT COUNT(*) AS count FROM pragma_table_info('branches') WHERE name = 'instance_id'",
        ),
        &mut connection,
    )
    .unwrap()
    .count;
    assert_eq!(instance_columns, 0);

    let heads = diesel::RunQueryDsl::get_result::<CountRow>(
        diesel::sql_query("SELECT COUNT(*) AS count FROM branches WHERE head_id = ?")
            .bind::<diesel::sql_types::Text, _>(&root),
        &mut connection,
    )
    .unwrap()
    .count;
    assert_eq!(heads, 1);
}
