use super::*;

#[tokio::test]
async fn preset_migration_round_trips_records() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    store
        .set_preset("default", preset("gpt-5.4"))
        .await
        .unwrap();
    let expected = store
        .set_preset("default", rich_preset("gpt-5.5"))
        .await
        .unwrap();
    let mut nullable_config = preset("gpt-5.6");
    nullable_config.temperature = None;
    nullable_config.max_tokens = None;
    nullable_config.additional_params = Some(serde_json::Value::Null);
    let nullable_expected = store.set_preset("nullable", nullable_config).await.unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    diesel::connection::SimpleConnection::batch_execute(
        &mut connection,
        "PRAGMA foreign_keys = ON",
    )
    .unwrap();
    revert_store_migrations_to(&mut connection, 19);
    let legacy = diesel::RunQueryDsl::get_result::<LegacyPresetRecordJson>(
        diesel::sql_query("SELECT record_json FROM presets WHERE name = 'default'"),
        &mut connection,
    )
    .unwrap();
    assert_eq!(
        serde_json::from_str::<crate::PresetRecord>(&legacy.record_json).unwrap(),
        expected
    );

    connection
        .run_pending_migrations(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert!(!preset_has_record_json_column(&reopened).await);
    assert!(table_exists(&reopened, "preset_versions").await);
    assert!(table_exists(&reopened, "preset_version_tools").await);
    assert_eq!(
        reopened.get_preset_record("default").await.unwrap(),
        expected
    );
    assert_eq!(
        reopened.get_preset_record("nullable").await.unwrap(),
        nullable_expected
    );
    let mut connection = reopened.connect().await.unwrap();
    assert_eq!(
        preset_versions::table
            .filter(preset_versions::preset_name.eq("default"))
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        preset_version_tools::table
            .filter(preset_version_tools::preset_name.eq("default"))
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn preset_migration_rejects_mismatched_record_name() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    store
        .set_preset("default", preset("gpt-5.4"))
        .await
        .unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 19);
    diesel::RunQueryDsl::execute(
        diesel::sql_query(
            "UPDATE presets \
             SET record_json = json_set(record_json, '$.name', 'other') \
             WHERE name = 'default'",
        ),
        &mut connection,
    )
    .unwrap();

    let error = connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap_err();

    assert!(error.to_string().contains("CHECK constraint failed"));
}
