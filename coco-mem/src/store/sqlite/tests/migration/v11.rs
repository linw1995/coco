use super::*;

#[tokio::test]
async fn node_content_migration_down_restores_kind_json() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let failure_id = store
        .append(NewNode {
            parent: root_id.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Failure("boom".to_owned()),
        })
        .await
        .unwrap();
    let tool_use_id = store
        .append(NewNode {
            parent: failure_id.clone(),
            role: Role::LLM,
            metadata: None,
            kind: Kind::tool_uses(Vec::new()),
        })
        .await
        .unwrap();
    let tool_result_id = store
        .append(NewNode {
            parent: tool_use_id.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::tool_results(Vec::new()),
        })
        .await
        .unwrap();
    let anchor_id = store
        .append(session_anchor_node(&tool_result_id))
        .await
        .unwrap();
    let node_ids = [
        root_id.clone(),
        failure_id.clone(),
        tool_use_id.clone(),
        tool_result_id.clone(),
        anchor_id.clone(),
    ];
    let mut expected = Vec::new();
    for node_id in &node_ids {
        expected.push(store.get_node(node_id).await.unwrap());
    }
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 10);

    for (node_id, expected_json) in [
        (&root_id, serde_json::json!({ "Text": "The Big Bang" })),
        (&failure_id, serde_json::json!({ "Failure": "boom" })),
        (&tool_use_id, serde_json::json!({ "ToolUse": [] })),
        (&tool_result_id, serde_json::json!({ "ToolResult": [] })),
        (&anchor_id, serde_json::json!({ "Anchor": null })),
    ] {
        let row = diesel::RunQueryDsl::get_result::<LegacyKindJson>(
            diesel::sql_query("SELECT kind_json FROM nodes WHERE id = ?")
                .bind::<diesel::sql_types::Text, _>(node_id),
            &mut connection,
        )
        .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&row.kind_json).unwrap(),
            expected_json
        );
    }

    connection
        .run_pending_migrations(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert!(!node_has_kind_json_column(&reopened).await);
    for (node_id, expected) in node_ids.iter().zip(expected) {
        assert_eq!(reopened.get_node(node_id).await.unwrap(), expected);
    }
}

#[tokio::test]
async fn node_content_migration_rejects_mismatched_kind_json() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 10);
    diesel::RunQueryDsl::execute(
        diesel::sql_query("UPDATE nodes SET kind_json = ? WHERE id = ?")
            .bind::<diesel::sql_types::Text, _>(r#"{"Failure":"The Big Bang"}"#)
            .bind::<diesel::sql_types::Text, _>(&root_id),
        &mut connection,
    )
    .unwrap();

    let error = connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap_err();

    assert!(error.to_string().contains("CHECK constraint failed"));
}
