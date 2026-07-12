use super::*;

#[tokio::test]
async fn node_kind_discriminator_migration_round_trips_all_anchor_kinds() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let new_nodes = [
        session_anchor_node(&store.root_id()),
        rich_session_patch_anchor_node(&store.root_id(), vec![]),
        rich_prompt_anchor_node(&store.root_id(), vec![]),
        skill_invocation_anchor_node(&store.root_id(), vec![]),
        skill_result_anchor_node(&store.root_id(), vec![]),
    ];
    let mut expected = Vec::new();
    for new_node in new_nodes {
        let node_id = store.append(new_node).await.unwrap();
        expected.push(store.get_node(&node_id).await.unwrap());
    }
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 17);
    for node in &expected {
        let kind = diesel::RunQueryDsl::get_result::<String>(
            nodes::table
                .filter(nodes::id.eq(&node.id))
                .select(nodes::kind),
            &mut connection,
        )
        .unwrap();
        assert_eq!(kind, "anchor");
    }

    connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap();
    for node in &expected {
        let kind = diesel::RunQueryDsl::get_result::<String>(
            nodes::table
                .filter(nodes::id.eq(&node.id))
                .select(nodes::kind),
            &mut connection,
        )
        .unwrap();
        assert_eq!(kind, super::node_storage_kind(&node.kind));
    }

    connection
        .revert_last_migration(super::STORE_MIGRATIONS)
        .unwrap();
    for node in &expected {
        let kind = diesel::RunQueryDsl::get_result::<String>(
            nodes::table
                .filter(nodes::id.eq(&node.id))
                .select(nodes::kind),
            &mut connection,
        )
        .unwrap();
        assert_eq!(kind, "anchor");
    }
    connection
        .run_pending_migrations(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    for node in expected {
        assert_eq!(reopened.get_node(&node.id).await.unwrap(), node);
    }
}

#[tokio::test]
async fn node_kind_discriminator_migration_rejects_mismatched_node_kind() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let anchor_id = store
        .append(session_anchor_node(&store.root_id()))
        .await
        .unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 17);
    diesel::RunQueryDsl::execute(
        diesel::update(nodes::table.filter(nodes::id.eq(&anchor_id))).set(nodes::kind.eq("text")),
        &mut connection,
    )
    .unwrap();

    let error = connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap_err();

    assert!(error.to_string().contains("CHECK constraint failed"));
}
