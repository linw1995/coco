use super::*;

#[tokio::test]
async fn job_payload_migration_down_restores_payload_json() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let session = store.append(session_anchor_node(&root_id)).await.unwrap();
    store.fork("main", &session).await.unwrap();
    store
        .submit_job_with_id("job-test", "main", &session)
        .await
        .unwrap();
    store
        .set_job_status("job-test", JobStatus::Queued, JobStatus::Running)
        .await
        .unwrap();
    let job = store
        .set_job_status("job-test", JobStatus::Running, JobStatus::Finished)
        .await
        .unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 8);
    let row = diesel::RunQueryDsl::get_result::<LegacyJobPayloadJson>(
        diesel::sql_query("SELECT payload_json FROM jobs WHERE job_id = 'job-test'"),
        &mut connection,
    )
    .unwrap();

    assert_eq!(serde_json::from_str::<Job>(&row.payload_json).unwrap(), job);

    connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open(&path).await.unwrap();
    assert!(!job_has_payload_json_column(&reopened).await);
    assert_eq!(reopened.get_job("job-test").await.unwrap(), job);
}

#[tokio::test]
async fn job_payload_migration_rejects_mismatched_summary() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root_id = store.root_id();
    let session = store.append(session_anchor_node(&root_id)).await.unwrap();
    store.fork("main", &session).await.unwrap();
    store
        .submit_job_with_id("job-test", "main", &session)
        .await
        .unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 8);
    diesel::RunQueryDsl::execute(
        diesel::sql_query(
            "UPDATE jobs SET payload_json = json_set(payload_json, '$.status', 'finished')",
        ),
        &mut connection,
    )
    .unwrap();

    let error = connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap_err();

    assert!(error.to_string().contains("CHECK constraint failed"));
}
