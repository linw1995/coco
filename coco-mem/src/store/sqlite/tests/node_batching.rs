use super::*;
use diesel::sql_types::Text;
use diesel_async::SimpleAsyncConnection;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn long_ancestry_preserves_payloads_without_exceeding_sqlite_variable_limit() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(tempdir.path()).await.unwrap();
    let mut connection = store.connect().await.unwrap();
    // Exceed the bundled SQLite default of 32766 variables in a single query.
    const NODE_COUNT: usize = 32767;
    diesel::sql_query(
        "WITH RECURSIVE sequence(n) AS (
            SELECT 1 UNION ALL SELECT n + 1 FROM sequence WHERE n < 32767
        )
        INSERT INTO nodes (id, parent_id, created_at, role, kind, metadata_present, content)
        SELECT printf('batch-%05d', n),
               CASE WHEN n = 1 THEN ? ELSE printf('batch-%05d', n - 1) END,
               '2026-01-01T00:00:00Z', 'llm', 'tool_use', 1, NULL
        FROM sequence",
    )
    .bind::<Text, _>(store.root_id())
    .execute(&mut connection)
    .await
    .unwrap();
    connection
        .batch_execute(
            "INSERT INTO node_relations (child_node_id, parent_node_id, kind, ordinal)
             SELECT id, parent_id, 'primary', 0 FROM nodes WHERE id LIKE 'batch-%';
             INSERT INTO node_metadata (node_id, ordinal, execution_id, call_id)
             SELECT id, 0, id, 'first' FROM nodes WHERE id LIKE 'batch-%';
             INSERT INTO node_metadata (node_id, ordinal, execution_id, call_id)
             SELECT id, 1, id, 'second' FROM nodes WHERE id LIKE 'batch-%';
             INSERT INTO node_tool_uses (node_id, ordinal, tool_use_id, name, input_json)
             SELECT id, 0, id, 'first', '{}' FROM nodes WHERE id LIKE 'batch-%';
             INSERT INTO node_tool_uses (node_id, ordinal, tool_use_id, name, input_json)
             SELECT id, 1, id, 'second', '{}' FROM nodes WHERE id LIKE 'batch-%';",
        )
        .await
        .unwrap();

    let max_variables = Arc::new(AtomicUsize::new(0));
    let captured_max = Arc::clone(&max_variables);
    connection.set_instrumentation(move |event: InstrumentationEvent<'_>| {
        if let InstrumentationEvent::StartQuery { query, .. } = event {
            let query = query.to_string();
            let sql = query.split("-- binds:").next().unwrap();
            captured_max.fetch_max(sql.matches('?').count(), Ordering::Relaxed);
        }
    });
    let ancestry = super::super::node::load_ancestry_nodes(
        &mut connection,
        &store.database_path,
        &format!("batch-{NODE_COUNT:05}"),
    )
    .await
    .unwrap();

    assert_eq!(ancestry.len(), NODE_COUNT + 1);
    assert_eq!(ancestry.last().unwrap().id, store.root_id());
    for (offset, node) in ancestry[..NODE_COUNT].iter().enumerate() {
        let id = format!("batch-{:05}", NODE_COUNT - offset);
        assert_eq!(node.id, id);
        assert_eq!(
            node.metadata,
            Some(
                ["first", "second"]
                    .map(|call| BackendMetadata {
                        execution_id: Some(id.clone()),
                        call_id: Some(call.to_owned()),
                    })
                    .to_vec()
            )
        );
        assert_eq!(
            node.kind,
            Kind::tool_uses(
                ["first", "second"]
                    .map(|name| ToolUse {
                        id: id.clone(),
                        name: name.to_owned(),
                        input: serde_json::json!({}),
                    })
                    .to_vec()
            )
        );
    }
    let max_variables = max_variables.as_ref().load(Ordering::Relaxed);
    assert!(max_variables > 0 && max_variables <= 999);
}
