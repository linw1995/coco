use super::*;

#[tokio::test]
async fn node_anchor_migration_down_restores_node_columns() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let anchor_id = store
        .append(session_anchor_node(&store.root_id()))
        .await
        .unwrap();
    let expected = store.get_node(&anchor_id).await.unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 9);

    let legacy = diesel::RunQueryDsl::get_result::<LegacyNodeAnchorRow>(
        diesel::sql_query(
            "SELECT anchor_kind, anchor_session_role, anchor_provider_profile, \
             anchor_provider, anchor_model, anchor_prompt, anchor_skill_name, \
             anchor_skill_invocation_mode, kind_json \
             FROM nodes WHERE id = ?",
        )
        .bind::<diesel::sql_types::Text, _>(&anchor_id),
        &mut connection,
    )
    .unwrap();
    assert_eq!(legacy.anchor_kind.as_deref(), Some("session"));
    assert_eq!(legacy.anchor_session_role.as_deref(), Some("orchestrator"));
    assert_eq!(legacy.anchor_provider_profile, None);
    assert_eq!(legacy.anchor_provider.as_deref(), Some("openai"));
    assert_eq!(legacy.anchor_model.as_deref(), Some("gpt-5.4"));
    assert_eq!(legacy.anchor_prompt.as_deref(), Some("prompt"));
    assert_eq!(legacy.anchor_skill_name, None);
    assert_eq!(legacy.anchor_skill_invocation_mode, None);
    let legacy_kind_json = serde_json::from_str::<serde_json::Value>(&legacy.kind_json).unwrap();
    assert_eq!(
        legacy_kind_json.pointer("/Anchor/payload/Session/system_prompt"),
        Some(&serde_json::Value::String("system".to_owned()))
    );
    assert_eq!(
        legacy_kind_json.pointer("/Anchor/payload/Session/prompt"),
        None
    );

    connection
        .run_pending_migrations(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert!(!nodes_have_anchor_columns(&reopened).await);
    assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
}
