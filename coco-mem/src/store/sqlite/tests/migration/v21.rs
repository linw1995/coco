use super::*;

#[tokio::test]
async fn session_state_json_migration_round_trips_all_states() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let session = store.append(session_anchor_node(&root_id)).await.unwrap();

    let states = std::collections::HashMap::from([
        ("main".to_owned(), SessionState::Active),
        (
            "attached".to_owned(),
            SessionState::Attached {
                target_branch: "main".to_owned(),
                base_head_id: session.clone(),
            },
        ),
        (
            "closed".to_owned(),
            SessionState::Paused {
                target_branch: "main".to_owned(),
                reason: PauseReason::Closed,
            },
        ),
        (
            "merged".to_owned(),
            SessionState::Paused {
                target_branch: "main".to_owned(),
                reason: PauseReason::Merged {
                    merged_anchor_id: session.clone(),
                },
            },
        ),
    ]);
    for branch in states.keys() {
        store.fork(branch, &session).await.unwrap();
    }
    for (branch, state) in &states {
        if state != &SessionState::Active {
            store
                .set_session_state(branch, Some(&SessionState::Active), state.clone())
                .await
                .unwrap();
        }
    }
    assert!(!session_has_state_json_column(&store).await);
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 20);
    let legacy_rows = diesel::RunQueryDsl::load::<LegacySessionStateJson>(
        diesel::sql_query("SELECT branch_name, state_json FROM sessions ORDER BY branch_name"),
        &mut connection,
    )
    .unwrap();
    let restored = legacy_rows
        .into_iter()
        .map(|row| {
            (
                row.branch_name,
                serde_json::from_str::<SessionState>(&row.state_json).unwrap(),
            )
        })
        .collect::<std::collections::HashMap<_, _>>();

    assert_eq!(restored, states);

    connection
        .run_pending_migrations(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert!(!session_has_state_json_column(&reopened).await);
    assert_eq!(reopened.list_session_states().await.unwrap(), states);
}

#[tokio::test]
async fn session_state_json_migration_rejects_mismatched_relational_state() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let session = store.append(session_anchor_node(&root_id)).await.unwrap();
    store.fork("main", &session).await.unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 20);
    let mismatched = serde_json::to_string(&SessionState::Attached {
        target_branch: "main".to_owned(),
        base_head_id: session,
    })
    .unwrap();
    diesel::RunQueryDsl::execute(
        diesel::sql_query("UPDATE sessions SET state_json = ? WHERE branch_name = 'main'")
            .bind::<diesel::sql_types::Text, _>(mismatched),
        &mut connection,
    )
    .unwrap();

    let error = connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap_err();

    assert!(error.to_string().contains("CHECK constraint failed"));
}
