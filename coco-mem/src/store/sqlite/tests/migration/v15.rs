use super::*;

#[tokio::test]
async fn node_anchor_skill_invocation_migration_round_trips_relational_fields() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let merge_parent = store
        .append(NewNode {
            parent: store.root_id(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("merge parent".to_owned()),
        })
        .await
        .unwrap();
    let anchor_id = store
        .append(skill_invocation_anchor_node(
            &store.root_id(),
            vec![MergeParent::shadow(merge_parent)],
        ))
        .await
        .unwrap();
    let expected = store.get_node(&anchor_id).await.unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 14);
    connection
        .run_pending_migrations(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
    assert_eq!(
        node_anchor_skill_invocation_row(&reopened, &anchor_id).await,
        NodeAnchorSkillInvocationRow {
            node_id: anchor_id.clone(),
            skill_name: "compact".to_owned(),
            mode: "handoff".to_owned(),
            prompt: Some("Compact this branch".to_owned()),
        }
    );
}

#[tokio::test]
async fn node_anchor_skill_invocation_migration_rejects_invalid_mode() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let anchor_id = store
        .append(skill_invocation_anchor_node(&store.root_id(), vec![]))
        .await
        .unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 14);
    diesel::RunQueryDsl::execute(
        diesel::sql_query(
            "UPDATE node_anchors SET skill_invocation_mode = 'invalid' WHERE node_id = ?",
        )
        .bind::<diesel::sql_types::Text, _>(&anchor_id),
        &mut connection,
    )
    .unwrap();

    let error = connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap_err();

    assert!(error.to_string().contains("CHECK constraint failed"));
}
