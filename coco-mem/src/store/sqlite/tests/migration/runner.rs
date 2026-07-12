use super::*;

#[tokio::test]
async fn open_creates_sqlite_database_and_schema() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");

    let store = SqliteStore::open(&path).await.unwrap();

    assert!(store.database_path().is_file());
    assert_eq!(store.schema_version().await.unwrap(), 23);
    assert!(!nodes_have_anchor_columns(&store).await);
    assert!(!node_has_kind_json_column(&store).await);
    assert!(!node_anchor_table_exists(&store).await);
    assert!(!table_exists(&store, "node_anchor_prompts").await);
    assert!(!job_has_payload_json_column(&store).await);
    assert!(!table_exists(&store, "store_meta").await);
}

#[tokio::test]
async fn open_read_only_accepts_current_schema() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    SqliteStore::open(&path).await.unwrap();

    let store = SqliteStore::open_read_only(&path).await.unwrap();

    assert_eq!(store.schema_version().await.unwrap(), 23);
}

#[tokio::test]
async fn open_read_only_rejects_missing_schema() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    std::fs::create_dir(&path).unwrap();

    let err = SqliteStore::open_read_only(&path).await.unwrap_err();

    assert!(err.to_string().contains("SQLite"));
    assert!(!super::sqlite_database_path(&path).exists());
}

#[tokio::test]
async fn open_read_only_or_upgrade_schema_rejects_old_diesel_schema_version() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    std::fs::create_dir(&path).unwrap();
    create_diesel_migration_metadata_for_test(&path, "00000000000005");

    let err = SqliteStore::open_read_only_or_upgrade_schema(&path)
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("unsupported SQLite schema version 5, expected 6..=23")
    );
}

#[tokio::test]
async fn open_rejects_old_diesel_schema_version() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    std::fs::create_dir(&path).unwrap();
    create_diesel_migration_metadata_for_test(&path, "00000000000005");

    let err = SqliteStore::open(&path).await.unwrap_err();

    assert!(
        err.to_string()
            .contains("unsupported SQLite schema version 5, expected 6..=23")
    );
}

#[tokio::test]
async fn open_rejects_legacy_json_store_without_creating_database() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("meta.json"), "{}").unwrap();
    std::fs::write(path.join("nodes.jsonl"), "").unwrap();

    let err = SqliteStore::open(&path).await.unwrap_err();

    assert!(matches!(err, crate::StoreError::LegacyJsonStore { path: legacy } if legacy == path));
    assert!(!super::sqlite_database_path(&path).exists());
}

#[tokio::test]
async fn open_read_only_rejects_legacy_json_store() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("nodes.jsonl"), "").unwrap();

    let err = SqliteStore::open_read_only(&path).await.unwrap_err();

    assert!(matches!(err, crate::StoreError::LegacyJsonStore { path: legacy } if legacy == path));
}
