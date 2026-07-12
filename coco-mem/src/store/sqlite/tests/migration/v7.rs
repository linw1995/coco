use super::*;

#[tokio::test]
async fn node_item_migration_down_restores_kind_json() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    create_v6_store_with_legacy_data(&path);
    let store = SqliteStore::open(&path).await.unwrap();
    drop(store);
    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();

    revert_store_migrations_to(&mut connection, 6);

    let tool_use = diesel::RunQueryDsl::get_result::<LegacyKindJson>(
        diesel::sql_query("SELECT kind_json FROM nodes WHERE id = 'tool-use-node'"),
        &mut connection,
    )
    .unwrap();
    let tool_result = diesel::RunQueryDsl::get_result::<LegacyKindJson>(
        diesel::sql_query("SELECT kind_json FROM nodes WHERE id = 'tool-result-node'"),
        &mut connection,
    )
    .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&tool_use.kind_json).unwrap(),
        serde_json::json!({
            "ToolUse": [
                {
                    "id": "tool-call-1",
                    "name": "exec_command",
                    "input": { "cmd": "pwd" }
                },
                {
                    "id": "tool-call-2",
                    "name": "exec_command",
                    "input": { "cmd": "ls" }
                }
            ]
        })
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&tool_result.kind_json).unwrap(),
        serde_json::json!({
            "ToolResult": [
                { "id": "tool-call-1", "output": "ok" },
                { "id": "tool-call-2", "output": "done" }
            ]
        })
    );
    drop(connection);

    let reopened = SqliteStore::open(&path).await.unwrap();
    assert_eq!(
        reopened
            .get_node("tool-use-node")
            .await
            .unwrap()
            .kind
            .as_tool_uses()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        reopened
            .get_node("tool-result-node")
            .await
            .unwrap()
            .kind
            .as_tool_results()
            .unwrap()
            .len(),
        2
    );
}
