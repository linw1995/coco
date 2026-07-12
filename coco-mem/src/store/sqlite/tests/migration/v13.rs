use super::*;

#[tokio::test]
async fn node_anchor_session_patch_migration_round_trips_relational_fields() {
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
        .append(rich_session_patch_anchor_node(
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
    revert_store_migrations_to(&mut connection, 12);
    connection
        .run_pending_migrations(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
    assert_eq!(
        node_anchor_session_patch_row(&reopened, &anchor_id).await,
        NodeAnchorSessionPatchRow {
            node_id: anchor_id.clone(),
            role: Some("runner".to_owned()),
            provider_profile_present: true,
            provider_profile: Some("runner-profile".to_owned()),
            provider_present: true,
            provider: Some("openai".to_owned()),
            model: Some("gpt-5.4".to_owned()),
            tools_present: true,
            system_prompt: Some("patched system".to_owned()),
            temperature_present: true,
            temperature: Some(0.5),
            max_tokens_present: true,
            max_tokens: Some(u64::MAX.to_string()),
            additional_params_present: true,
            additional_params_json: Some(r#""priority""#.to_owned()),
            enable_coco_shim: Some(true),
        }
    );
    assert_eq!(
        node_anchor_session_patch_tool_rows(&reopened, &anchor_id).await,
        vec![NodeAnchorSessionPatchToolRow {
            node_id: anchor_id,
            ordinal: 0,
            name: "lookup".to_owned(),
            description: "Look up a value".to_owned(),
            input_schema_json: r#"{"type":"object"}"#.to_owned(),
        }]
    );
}

#[tokio::test]
async fn node_anchor_session_patch_migration_accepts_absent_tools() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let anchor_id = store
        .append(NewNode {
            parent: store.root_id(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session_patch(vec![], SessionAnchorPatch::default())),
        })
        .await
        .unwrap();
    let expected = store.get_node(&anchor_id).await.unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 12);
    connection
        .run_pending_migrations(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
    assert!(
        !node_anchor_session_patch_row(&reopened, &anchor_id)
            .await
            .tools_present
    );
    assert!(
        node_anchor_session_patch_tool_rows(&reopened, &anchor_id)
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn node_anchor_session_patch_migration_accepts_missing_optional_fields() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let anchor_id = store
        .append(NewNode {
            parent: store.root_id(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session_patch(vec![], SessionAnchorPatch::default())),
        })
        .await
        .unwrap();
    let expected = store.get_node(&anchor_id).await.unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 12);
    diesel::RunQueryDsl::execute(
        diesel::sql_query(
            "UPDATE node_anchors SET kind_json = json_remove(\
             kind_json, \
             '$.Anchor.payload.SessionPatch.role', \
             '$.Anchor.payload.SessionPatch.provider_profile', \
             '$.Anchor.payload.SessionPatch.provider', \
             '$.Anchor.payload.SessionPatch.model', \
             '$.Anchor.payload.SessionPatch.tools', \
             '$.Anchor.payload.SessionPatch.system_prompt', \
             '$.Anchor.payload.SessionPatch.temperature', \
             '$.Anchor.payload.SessionPatch.max_tokens', \
             '$.Anchor.payload.SessionPatch.additional_params', \
             '$.Anchor.payload.SessionPatch.enable_coco_shim' \
             ) WHERE node_id = ?",
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
    assert_eq!(
        node_anchor_session_patch_row(&reopened, &anchor_id).await,
        NodeAnchorSessionPatchRow {
            node_id: anchor_id,
            role: None,
            provider_profile_present: false,
            provider_profile: None,
            provider_present: false,
            provider: None,
            model: None,
            tools_present: false,
            system_prompt: None,
            temperature_present: false,
            temperature: None,
            max_tokens_present: false,
            max_tokens: None,
            additional_params_present: false,
            additional_params_json: None,
            enable_coco_shim: None,
        }
    );
}

#[tokio::test]
async fn node_anchor_session_patch_migration_rejects_invalid_payload() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let anchor_id = store
        .append(rich_session_patch_anchor_node(&store.root_id(), vec![]))
        .await
        .unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 12);
    diesel::RunQueryDsl::execute(
        diesel::sql_query(
            "UPDATE node_anchors SET kind_json = json_set(\
             kind_json, '$.Anchor.payload.SessionPatch.role', 'invalid') WHERE node_id = ?",
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
