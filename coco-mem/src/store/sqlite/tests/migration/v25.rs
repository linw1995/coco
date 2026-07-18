use super::*;

#[derive(diesel::QueryableByName)]
struct BranchHistoryBaseline {
    #[diesel(sql_type = diesel::sql_types::Text)]
    name: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    revision: i64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    head_id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    state_json: String,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    removed: bool,
}

#[derive(diesel::QueryableByName)]
struct GraphRevisionState {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    current_revision: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    baseline_revision: i64,
}

#[derive(diesel::QueryableByName)]
struct QueryPlanDetail {
    #[diesel(sql_type = diesel::sql_types::Text)]
    detail: String,
}

#[tokio::test]
async fn graph_mutation_journal_uses_populated_v24_revision_as_its_baseline() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    store.fork("main", &root_id).await.unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    diesel::connection::SimpleConnection::batch_execute(
        &mut connection,
        "PRAGMA foreign_keys = ON",
    )
    .unwrap();
    connection
        .revert_last_migration(super::STORE_MIGRATIONS)
        .unwrap();
    connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap();

    let revision_state = diesel::RunQueryDsl::get_result::<GraphRevisionState>(
        diesel::sql_query(
            "SELECT current_revision, baseline_revision \
             FROM graph_relation_state WHERE singleton = 1",
        ),
        &mut connection,
    )
    .unwrap();
    assert!(revision_state.current_revision > 0);
    assert_eq!(
        revision_state.baseline_revision,
        revision_state.current_revision
    );
    let baseline = diesel::RunQueryDsl::get_result::<BranchHistoryBaseline>(
        diesel::sql_query(
            "SELECT name, revision, head_id, state_json, removed \
             FROM graph_branch_history WHERE name = 'main'",
        ),
        &mut connection,
    )
    .unwrap();
    assert_eq!(baseline.name, "main");
    assert_eq!(baseline.revision, revision_state.baseline_revision);
    assert_eq!(baseline.head_id, root_id);
    assert_eq!(
        serde_json::from_str::<SessionState>(&baseline.state_json).unwrap(),
        SessionState::Active
    );
    assert!(!baseline.removed);
    let query_plan = diesel::RunQueryDsl::load::<QueryPlanDetail>(
        diesel::sql_query(
            "EXPLAIN QUERY PLAN \
             SELECT name FROM graph_branch_names \
             WHERE first_revision <= 10 \
               AND (first_revision > 3 \
                    OR (first_revision = 3 AND name > 'branch')) \
             ORDER BY first_revision, name LIMIT 129",
        ),
        &mut connection,
    )
    .unwrap();
    assert!(
        query_plan
            .iter()
            .any(|row| { row.detail.contains("graph_branch_names_revision_name_idx") })
    );
    assert!(
        query_plan
            .iter()
            .all(|row| !row.detail.contains("USE TEMP B-TREE"))
    );
    let violations = diesel::RunQueryDsl::get_result::<ColumnCount>(
        diesel::sql_query("SELECT COUNT(*) AS count FROM pragma_foreign_key_check"),
        &mut connection,
    )
    .unwrap();
    assert_eq!(violations.count, 0);
    drop(connection);

    let store = SqliteStore::open(&path).await.unwrap();
    let graph = SqliteGraphStore::open_read_only(&path).await.unwrap();
    assert_eq!(
        graph.graph_mutation_revision_bounds().await.unwrap(),
        GraphMutationRevisionBounds {
            baseline_revision: revision_state.baseline_revision,
            current_revision: revision_state.current_revision,
        }
    );
    let error = graph
        .graph_branches_at_revision_by_names(
            revision_state.baseline_revision - 1,
            &["main".to_owned()],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        crate::StoreError::GraphRevisionOutOfRange {
            requested,
            minimum,
            maximum,
        } if requested == revision_state.baseline_revision - 1
            && minimum == revision_state.baseline_revision
            && maximum == revision_state.current_revision
    ));
    let stale_cursor_error = graph
        .graph_mutation_events_page(0, NonZeroUsize::new(1).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(
        stale_cursor_error,
        crate::StoreError::GraphRevisionOutOfRange {
            requested: 0,
            minimum,
            maximum,
        } if minimum == revision_state.baseline_revision
            && maximum == revision_state.current_revision
    ));
    let before = graph.graph_mutation_revision().await.unwrap();
    store
        .append(NewNode {
            parent: root_id,
            role: Role::User,
            metadata: None,
            kind: Kind::Text("after migration".to_owned()),
        })
        .await
        .unwrap();
    let events = graph
        .graph_mutation_events_page(before, NonZeroUsize::new(1).unwrap())
        .await
        .unwrap();
    assert_eq!(events.events.len(), 1);
    assert_eq!(events.events[0].revision, before + 1);

    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    let violations = diesel::RunQueryDsl::get_result::<ColumnCount>(
        diesel::sql_query("SELECT COUNT(*) AS count FROM pragma_foreign_key_check"),
        &mut connection,
    )
    .unwrap();
    assert_eq!(violations.count, 0);
}
