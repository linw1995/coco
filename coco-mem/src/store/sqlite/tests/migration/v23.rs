use super::*;

#[tokio::test]
async fn open_upgrades_v22_without_node_item_backfill_marker() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    drop(store);
    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();

    connection
        .revert_last_migration(super::STORE_MIGRATIONS)
        .unwrap();
    let marker = diesel::RunQueryDsl::get_result::<String>(
        legacy_store_meta::table
            .filter(legacy_store_meta::key.eq(super::NODE_ITEM_ROWS_BACKFILL_META_KEY))
            .select(legacy_store_meta::value_json),
        &mut connection,
    )
    .unwrap();
    assert_eq!(marker, "true");
    diesel::RunQueryDsl::execute(
        diesel::delete(
            legacy_store_meta::table
                .filter(legacy_store_meta::key.eq(super::NODE_ITEM_ROWS_BACKFILL_META_KEY)),
        ),
        &mut connection,
    )
    .unwrap();
    drop(connection);

    let reopened = SqliteStore::open(&path).await.unwrap();

    assert_eq!(reopened.schema_version().await.unwrap(), 23);
    assert!(!table_exists(&reopened, "store_meta").await);
}

#[tokio::test]
async fn store_meta_migration_requires_matching_root_id() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    drop(store);
    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();

    connection
        .revert_last_migration(super::STORE_MIGRATIONS)
        .unwrap();
    diesel::RunQueryDsl::execute(
        diesel::update(legacy_store_meta::table.filter(legacy_store_meta::key.eq("root_id")))
            .set(legacy_store_meta::value_json.eq(r#""missing""#)),
        &mut connection,
    )
    .unwrap();

    let error = connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap_err();

    assert!(error.to_string().contains("CHECK constraint failed"));
    let root_id = diesel::RunQueryDsl::get_result::<String>(
        legacy_store_meta::table
            .filter(legacy_store_meta::key.eq("root_id"))
            .select(legacy_store_meta::value_json),
        &mut connection,
    )
    .unwrap();
    assert_eq!(root_id, r#""missing""#);
}

#[tokio::test]
async fn open_rejects_legacy_json_store_with_unmarked_sqlite_database() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    drop(store);
    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    connection
        .revert_last_migration(super::STORE_MIGRATIONS)
        .unwrap();
    diesel::RunQueryDsl::execute(
        diesel::delete(
            legacy_store_meta::table
                .filter(legacy_store_meta::key.eq(super::FS_MIGRATION_COMPLETE_META_KEY)),
        ),
        &mut connection,
    )
    .unwrap();
    drop(connection);
    std::fs::write(path.join("meta.json"), "{}").unwrap();
    std::fs::write(path.join("nodes.jsonl"), "").unwrap();

    let err = SqliteStore::open(&path).await.unwrap_err();

    assert!(matches!(err, crate::StoreError::LegacyJsonStore { path: legacy } if legacy == path));
}

#[tokio::test]
async fn open_accepts_legacy_json_store_after_completed_sqlite_migration() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    drop(store);
    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    connection
        .revert_last_migration(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);
    std::fs::write(path.join("meta.json"), "{}").unwrap();
    std::fs::write(path.join("nodes.jsonl"), "").unwrap();

    let reopened = SqliteStore::open(&path).await.unwrap();

    assert_eq!(reopened.schema_version().await.unwrap(), 23);
    assert!(!table_exists(&reopened, "store_meta").await);
}

#[tokio::test]
async fn open_accepts_legacy_json_files_with_current_sqlite_schema() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    drop(store);
    std::fs::write(path.join("meta.json"), "{}").unwrap();
    std::fs::write(path.join("nodes.jsonl"), "").unwrap();

    let reopened = SqliteStore::open(&path).await.unwrap();

    assert_eq!(reopened.schema_version().await.unwrap(), 23);
    assert!(!table_exists(&reopened, "store_meta").await);
}
