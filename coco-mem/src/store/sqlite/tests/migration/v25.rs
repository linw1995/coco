use super::*;

#[tokio::test]
async fn job_head_migration_backfills_connected_and_detached_jobs() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    let root = store.root_id();
    let connected_head = store
        .append(NewNode {
            parent: root.clone(),
            role: Role::LLM,
            metadata: None,
            kind: Kind::Text("connected".to_owned()),
        })
        .await
        .unwrap();
    let detached_base = store
        .append(NewNode {
            parent: root.clone(),
            role: Role::User,
            metadata: None,
            kind: Kind::Text("detached".to_owned()),
        })
        .await
        .unwrap();
    store.fork("connected", &connected_head).await.unwrap();
    store.fork("detached", &root).await.unwrap();
    store
        .submit_job_with_id("job-connected", "connected", &root)
        .await
        .unwrap();
    store
        .submit_job_with_id("job-detached", "detached", &detached_base)
        .await
        .unwrap();
    drop(store);

    let database_path = sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 24);
    connection.run_next_migration(STORE_MIGRATIONS).unwrap();
    drop(connection);

    let reopened = SqliteStore::open(&path).await.unwrap();
    assert_eq!(
        reopened.get_job("job-connected").await.unwrap().head,
        connected_head
    );
    assert_eq!(
        reopened.get_job("job-detached").await.unwrap().head,
        detached_base
    );
}
