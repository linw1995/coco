use super::*;

#[tokio::test]
async fn skill_migration_round_trips_records() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    store
        .add_skill(
            SessionRole::Runner,
            "custom-runner",
            SkillVersionSpec {
                description: "first".to_owned(),
                body: "body-v1".to_owned(),
                scripts: vec![SkillScript {
                    path: "scripts/first.py".to_owned(),
                    content: "print('first')".to_owned(),
                }],
                enable_coco_shim: false,
            },
        )
        .await
        .unwrap();
    let expected = store
        .update_skill(
            SessionRole::Runner,
            "custom-runner",
            &SkillUpdatePatch {
                description: Some("second".to_owned()),
                body: Some("body-v2".to_owned()),
                scripts: Some(vec![
                    SkillScript {
                        path: "scripts/second.py".to_owned(),
                        content: "print('second')".to_owned(),
                    },
                    SkillScript {
                        path: "scripts/second.py.lock".to_owned(),
                        content: "lock".to_owned(),
                    },
                ]),
                enable_coco_shim: Some(true),
            },
        )
        .await
        .unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    diesel::connection::SimpleConnection::batch_execute(
        &mut connection,
        "PRAGMA foreign_keys = ON",
    )
    .unwrap();
    revert_store_migrations_to(&mut connection, 21);
    let legacy = diesel::RunQueryDsl::get_result::<LegacySkillRecordJson>(
        diesel::sql_query(
            "SELECT record_json FROM skills \
             WHERE role = 'runner' AND name = 'custom-runner'",
        ),
        &mut connection,
    )
    .unwrap();
    assert_eq!(
        serde_json::from_str::<crate::SkillRecord>(&legacy.record_json).unwrap(),
        expected
    );

    connection
        .run_pending_migrations(super::STORE_MIGRATIONS)
        .unwrap();
    drop(connection);

    let reopened = SqliteStore::open_read_only(&path).await.unwrap();
    assert!(!skill_has_record_json_column(&reopened).await);
    assert!(table_exists(&reopened, "skill_versions").await);
    assert!(table_exists(&reopened, "skill_version_scripts").await);
    assert_eq!(
        reopened
            .get_skill(SessionRole::Runner, "custom-runner")
            .await
            .unwrap(),
        expected
    );
    let mut connection = reopened.connect().await.unwrap();
    assert_eq!(
        skill_versions::table
            .filter(skill_versions::role.eq("runner"))
            .filter(skill_versions::skill_name.eq("custom-runner"))
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        skill_version_scripts::table
            .filter(skill_version_scripts::role.eq("runner"))
            .filter(skill_version_scripts::skill_name.eq("custom-runner"))
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .unwrap(),
        3
    );
}

#[tokio::test]
async fn skill_migration_rejects_mismatched_record_name() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("store");
    let store = SqliteStore::open(&path).await.unwrap();
    store
        .add_skill(
            SessionRole::Runner,
            "custom-runner",
            SkillVersionSpec {
                description: "custom".to_owned(),
                body: "body".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: false,
            },
        )
        .await
        .unwrap();
    drop(store);

    let database_path = super::sqlite_database_path(&path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    revert_store_migrations_to(&mut connection, 21);
    diesel::RunQueryDsl::execute(
        diesel::sql_query(
            "UPDATE skills \
             SET record_json = json_set(record_json, '$.name', 'other') \
             WHERE role = 'runner' AND name = 'custom-runner'",
        ),
        &mut connection,
    )
    .unwrap();

    let error = connection
        .run_next_migration(super::STORE_MIGRATIONS)
        .unwrap_err();

    assert!(error.to_string().contains("CHECK constraint failed"));
}
