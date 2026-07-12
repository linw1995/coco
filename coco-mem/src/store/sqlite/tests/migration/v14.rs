use super::*;

#[tokio::test]
async fn node_anchor_prompt_migration_round_trips_relational_fields() {
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
        .append(rich_prompt_anchor_node(
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
    revert_store_migrations_to(&mut connection, 13);
    connection
        .run_pending_migrations(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
    assert_eq!(
        node_content(&reopened, &anchor_id).await.as_deref(),
        Some("Inspect these images")
    );
    assert_eq!(
        node_anchor_prompt_attachment_rows(&reopened, &anchor_id).await,
        vec![
            NodeAnchorPromptAttachmentRow {
                node_id: anchor_id.clone(),
                ordinal: 0,
                kind: "image".to_owned(),
                attachment_id: "image-a".to_owned(),
                width: Some(i64::from(u32::MAX)),
                height: Some(1080),
                file_size: Some(u64::MAX.to_string()),
                media_type: Some("image/png".to_owned()),
            },
            NodeAnchorPromptAttachmentRow {
                node_id: anchor_id,
                ordinal: 1,
                kind: "image".to_owned(),
                attachment_id: "image-b".to_owned(),
                width: None,
                height: None,
                file_size: None,
                media_type: None,
            },
        ]
    );
}

#[tokio::test]
async fn node_anchor_prompt_migration_rejects_invalid_attachment() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let anchor_id = store
        .append(rich_prompt_anchor_node(&store.root_id(), vec![]))
        .await
        .unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 13);
    diesel::RunQueryDsl::execute(
        diesel::sql_query(
            "UPDATE node_anchors SET kind_json = json_set(\
             kind_json, '$.Anchor.payload.Prompt.attachments[0].kind', 'video') \
             WHERE node_id = ?",
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
