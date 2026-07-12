use super::*;

#[tokio::test]
async fn node_anchor_session_migration_round_trips_relational_fields() {
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
        .append(rich_session_anchor_node(
            &store.root_id(),
            vec![MergeParent::merge(merge_parent)],
        ))
        .await
        .unwrap();
    let expected = store.get_node(&anchor_id).await.unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 11);
    connection
        .run_pending_migrations(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
    assert_eq!(
        node_anchor_session_row(&reopened, &anchor_id).await,
        NodeAnchorSessionRow {
            node_id: anchor_id.clone(),
            role: "runner".to_owned(),
            provider_profile: Some("runner-profile".to_owned()),
            provider: Some("openai".to_owned()),
            model: "gpt-5.4".to_owned(),
            system_prompt: "system".to_owned(),
            prompt: "session prompt".to_owned(),
            temperature: Some(0.25),
            max_tokens: Some(u64::MAX.to_string()),
            additional_params_json: Some(r#"{"reasoning":{"effort":"high"}}"#.to_owned()),
            enable_coco_shim: true,
            active_skill_name: Some("compact".to_owned()),
            active_skill_handoff: Some("Preserve the decisions".to_owned()),
        }
    );
    assert_eq!(
        node_anchor_session_tool_rows(&reopened, &anchor_id).await,
        vec![
            NodeAnchorSessionToolRow {
                node_id: anchor_id.clone(),
                ordinal: 0,
                name: "lookup".to_owned(),
                description: "Look up a value".to_owned(),
                input_schema_json: r#"{"properties":{"key":{"type":"string"}},"type":"object"}"#
                    .to_owned(),
            },
            NodeAnchorSessionToolRow {
                node_id: anchor_id,
                ordinal: 1,
                name: "finish".to_owned(),
                description: "Finish the task".to_owned(),
                input_schema_json: r#"{"type":"object"}"#.to_owned(),
            },
        ]
    );
}

#[tokio::test]
async fn node_anchor_session_migration_defaults_missing_coco_shim_to_false() {
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
    revert_store_migrations_to(&mut connection, 11);
    diesel::RunQueryDsl::execute(
        diesel::sql_query(
            "UPDATE node_anchors SET kind_json = json_remove(\
             kind_json, '$.Anchor.payload.Session.enable_coco_shim') WHERE node_id = ?",
        )
        .bind::<diesel::sql_types::Text, _>(&anchor_id),
        &mut connection,
    )
    .unwrap();
    connection
        .run_pending_migrations(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
}

#[tokio::test]
async fn node_anchor_session_migration_accepts_missing_additional_params() {
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
    revert_store_migrations_to(&mut connection, 11);
    diesel::RunQueryDsl::execute(
        diesel::sql_query(
            "UPDATE node_anchors SET kind_json = json_remove(\
             kind_json, '$.Anchor.payload.Session.additional_params') WHERE node_id = ?",
        )
        .bind::<diesel::sql_types::Text, _>(&anchor_id),
        &mut connection,
    )
    .unwrap();
    connection
        .run_pending_migrations(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
}

#[tokio::test]
async fn node_anchor_session_migration_rejects_invalid_payload() {
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
    revert_store_migrations_to(&mut connection, 11);
    diesel::RunQueryDsl::execute(
        diesel::sql_query(
            "UPDATE node_anchors SET kind_json = json_set(\
             kind_json, '$.Anchor.payload.Session.system_prompt', 1) WHERE node_id = ?",
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
