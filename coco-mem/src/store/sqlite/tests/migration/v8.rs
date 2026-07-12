use super::*;

#[test]
fn contraction_migration_requires_completed_backfill() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    create_v6_store_with_legacy_data(&path);
    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();

    connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap();
    let error = connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap_err();

    assert!(error.to_string().contains("CHECK constraint failed"));
    let row = diesel::RunQueryDsl::get_result::<LegacyMetadataJson>(
        diesel::sql_query("SELECT metadata_json FROM nodes WHERE id = 'tool-use-node'"),
        &mut connection,
    )
    .unwrap();
    assert!(row.metadata_json.is_some());
}

#[tokio::test]
async fn contraction_migration_down_restores_metadata_json() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    create_v6_store_with_legacy_data(&path);
    let store = SqliteStore::open(&path).await.unwrap();
    drop(store);
    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();

    revert_store_migrations_to(&mut connection, 7);

    let row = diesel::RunQueryDsl::get_result::<LegacyMetadataJson>(
        diesel::sql_query("SELECT metadata_json FROM nodes WHERE id = 'tool-use-node'"),
        &mut connection,
    )
    .unwrap();
    assert_eq!(
        row.metadata_json
            .map(|value| serde_json::from_str::<serde_json::Value>(&value).unwrap()),
        Some(serde_json::json!([
            {
                "execution_id": "execution-1",
                "call_id": "call-1"
            },
            {
                "execution_id": "execution-1",
                "call_id": "call-1"
            }
        ]))
    );
}
