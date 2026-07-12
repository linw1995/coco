use super::*;

#[tokio::test]
async fn node_anchor_kind_json_migration_round_trips_all_payloads() {
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
    let new_nodes = [
        rich_session_anchor_node(
            &store.root_id(),
            vec![MergeParent::merge(merge_parent.clone())],
        ),
        rich_session_patch_anchor_node(
            &store.root_id(),
            vec![MergeParent::shadow(merge_parent.clone())],
        ),
        rich_prompt_anchor_node(
            &store.root_id(),
            vec![MergeParent::merge(merge_parent.clone())],
        ),
        skill_invocation_anchor_node(
            &store.root_id(),
            vec![MergeParent::shadow(merge_parent.clone())],
        ),
        skill_result_anchor_node(&store.root_id(), vec![MergeParent::merge(merge_parent)]),
    ];
    let mut expected = Vec::new();
    for new_node in new_nodes {
        let node_id = store.append(new_node).await.unwrap();
        expected.push(store.get_node(&node_id).await.unwrap());
    }
    for node in &expected {
        assert_eq!(
            node_kind(&store, &node.id).await,
            super::node_storage_kind(&node.kind)
        );
    }
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 16);
    for node in &expected {
        let row = diesel::RunQueryDsl::get_result::<LegacyKindJson>(
            diesel::sql_query("SELECT kind_json FROM node_anchors WHERE node_id = ?")
                .bind::<diesel::sql_types::Text, _>(&node.id),
            &mut connection,
        )
        .unwrap();
        assert_eq!(
            serde_json::from_str::<Kind>(&row.kind_json).unwrap(),
            node.kind
        );
    }
    revert_store_migrations_to(&mut connection, 10);
    for node in &expected {
        let row = diesel::RunQueryDsl::get_result::<LegacyKindJson>(
            diesel::sql_query("SELECT kind_json FROM node_anchors WHERE node_id = ?")
                .bind::<diesel::sql_types::Text, _>(&node.id),
            &mut connection,
        )
        .unwrap();
        let value = serde_json::from_str::<serde_json::Value>(&row.kind_json).unwrap();
        match &node.kind {
            Kind::Anchor(Anchor {
                payload: crate::AnchorPayload::Session(_),
                ..
            }) => {
                assert_eq!(value.pointer("/Anchor/payload/Session/role"), None);
                assert_eq!(value.pointer("/Anchor/payload/Session/prompt"), None);
                assert!(
                    value
                        .pointer("/Anchor/payload/Session/system_prompt")
                        .is_some()
                );
            }
            Kind::Anchor(Anchor {
                payload: crate::AnchorPayload::SessionPatch(_),
                ..
            }) => {
                assert!(value.pointer("/Anchor/payload/SessionPatch").is_some());
            }
            Kind::Anchor(Anchor {
                payload: crate::AnchorPayload::Prompt(_),
                ..
            }) => {
                assert_eq!(value.pointer("/Anchor/payload/Prompt/prompt"), None);
                assert!(
                    value
                        .pointer("/Anchor/payload/Prompt/attachments")
                        .is_some()
                );
            }
            Kind::Anchor(Anchor {
                payload: crate::AnchorPayload::SkillInvocation(_),
                ..
            }) => {
                assert_eq!(
                    value.pointer("/Anchor/payload/SkillInvocation/skill_name"),
                    None
                );
                assert_eq!(
                    value.pointer("/Anchor/payload/SkillInvocation/mode/kind"),
                    None
                );
                assert_eq!(
                    value.pointer("/Anchor/payload/SkillInvocation/mode/prompt"),
                    None
                );
            }
            Kind::Anchor(Anchor {
                payload: crate::AnchorPayload::SkillResult(_),
                ..
            }) => {
                assert_eq!(
                    value.pointer("/Anchor/payload/SkillResult/skill_name"),
                    None
                );
                assert!(
                    value
                        .pointer("/Anchor/payload/SkillResult/output")
                        .is_some()
                );
            }
            Kind::ToolUse(_) | Kind::ToolResult(_) | Kind::Text(_) | Kind::Failure(_) => {
                panic!("expected anchor node")
            }
        }
    }
    connection
        .run_pending_migrations(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert!(!node_anchor_table_exists(&reopened).await);
    for node in expected {
        assert_eq!(
            node_kind(&reopened, &node.id).await,
            super::node_storage_kind(&node.kind)
        );
        assert_eq!(reopened.get_node(&node.id).await.unwrap(), node);
    }
}

#[tokio::test]
async fn node_anchor_kind_json_migration_rejects_incomplete_relational_payload() {
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
    revert_store_migrations_to(&mut connection, 16);
    diesel::RunQueryDsl::execute(
        diesel::sql_query("DELETE FROM node_anchor_sessions WHERE node_id = ?")
            .bind::<diesel::sql_types::Text, _>(&anchor_id),
        &mut connection,
    )
    .unwrap();

    let error = connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap_err();

    assert!(error.to_string().contains("CHECK constraint failed"));
}
