use super::*;

#[tokio::test]
async fn open_migrates_v6_data_to_current_schema() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    create_v6_store_with_legacy_data(&path);

    let store = SqliteStore::open(&path).await.unwrap();

    assert_eq!(store.schema_version().await.unwrap(), 25);
    assert!(!nodes_have_anchor_columns(&store).await);
    assert!(!table_exists(&store, "store_meta").await);
    assert_eq!(
        node_tool_use_rows(&store, "tool-use-node").await,
        vec![
            NodeToolUseRow {
                node_id: "tool-use-node".to_owned(),
                ordinal: 0,
                tool_use_id: "tool-call-1".to_owned(),
                name: "exec_command".to_owned(),
                input_json: r#"{"cmd":"pwd"}"#.to_owned(),
            },
            NodeToolUseRow {
                node_id: "tool-use-node".to_owned(),
                ordinal: 1,
                tool_use_id: "tool-call-2".to_owned(),
                name: "exec_command".to_owned(),
                input_json: r#"{"cmd":"ls"}"#.to_owned(),
            },
        ]
    );
    assert_eq!(
        node_tool_result_rows(&store, "tool-result-node").await,
        vec![
            NodeToolResultRow {
                node_id: "tool-result-node".to_owned(),
                ordinal: 0,
                tool_result_id: "tool-call-1".to_owned(),
                output: "ok".to_owned(),
            },
            NodeToolResultRow {
                node_id: "tool-result-node".to_owned(),
                ordinal: 1,
                tool_result_id: "tool-call-2".to_owned(),
                output: "done".to_owned(),
            },
        ]
    );
    assert_eq!(
        node_content(&store, "root").await.as_deref(),
        Some("The Big Bang")
    );
    assert_eq!(node_content(&store, "tool-use-node").await, None);
    assert!(!node_has_kind_json_column(&store).await);
    assert!(!node_has_metadata_json_column(&store).await);
    assert!(!job_has_payload_json_column(&store).await);
    assert_eq!(
        store.get_job("job-v6").await.unwrap(),
        Job {
            job_id: "job-v6".to_owned(),
            created_at: "2026-03-25T09:10:13Z".parse().unwrap(),
            finished_at: None,
            branch: "main".to_owned(),
            work_branch: "main".to_owned(),
            base: "root".to_owned(),
            status: JobStatus::Running,
        }
    );
    let tool_use = store.get_node("tool-use-node").await.unwrap();
    assert_eq!(tool_use.kind.as_tool_uses().unwrap().len(), 2);
    assert_eq!(
        tool_use.metadata,
        Some(vec![
            BackendMetadata {
                execution_id: Some("execution-1".to_owned()),
                call_id: Some("call-1".to_owned()),
            },
            BackendMetadata {
                execution_id: Some("execution-1".to_owned()),
                call_id: Some("call-1".to_owned()),
            },
        ])
    );
    let tool_result = store.get_node("tool-result-node").await.unwrap();
    assert_eq!(tool_result.kind.as_tool_results().unwrap().len(), 2);
    assert_eq!(
        tool_result.metadata,
        Some(vec![BackendMetadata {
            execution_id: Some("execution-2".to_owned()),
            call_id: Some("call-result".to_owned()),
        }])
    );
}
