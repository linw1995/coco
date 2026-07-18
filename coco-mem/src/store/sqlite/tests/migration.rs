use super::super::migration::{
    __diesel_schema_migrations, FS_MIGRATION_COMPLETE_META_KEY, NODE_ITEM_ROWS_BACKFILL_META_KEY,
    STORE_MIGRATIONS, legacy_store_meta, table_count,
};
use super::super::{node_storage_kind, sqlite_database_path};
use super::*;
use diesel_migrations::MigrationHarness;

mod runner;
mod v10;
mod v11;
mod v12;
mod v13;
mod v14;
mod v15;
mod v16;
mod v17;
mod v18;
mod v19;
mod v20;
mod v21;
mod v22;
mod v23;
mod v24;
mod v25;
mod v6;
mod v7;
mod v8;
mod v9;

#[derive(diesel::QueryableByName)]
struct ColumnCount {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(diesel::QueryableByName)]
struct LegacyMetadataJson {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    metadata_json: Option<String>,
}

#[derive(diesel::QueryableByName)]
struct LegacyKindJson {
    #[diesel(sql_type = diesel::sql_types::Text)]
    kind_json: String,
}

#[derive(diesel::QueryableByName)]
struct LegacyNodeAnchorRow {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    anchor_kind: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    anchor_session_role: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    anchor_provider_profile: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    anchor_provider: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    anchor_model: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    anchor_prompt: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    anchor_skill_name: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    anchor_skill_invocation_mode: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    kind_json: String,
}

#[derive(diesel::QueryableByName)]
struct LegacyJobPayloadJson {
    #[diesel(sql_type = diesel::sql_types::Text)]
    payload_json: String,
}

#[derive(diesel::QueryableByName)]
struct LegacySessionStateJson {
    #[diesel(sql_type = diesel::sql_types::Text)]
    branch_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    state_json: String,
}

#[derive(diesel::QueryableByName)]
struct LegacyPresetRecordJson {
    #[diesel(sql_type = diesel::sql_types::Text)]
    record_json: String,
}

#[derive(diesel::QueryableByName)]
struct LegacySkillRecordJson {
    #[diesel(sql_type = diesel::sql_types::Text)]
    record_json: String,
}

fn rich_preset(model: &str) -> Preset {
    Preset {
        role: SessionRole::Runner,
        provider_profile: "custom".to_owned(),
        model: model.to_owned(),
        tools: vec![
            Tool {
                name: "lookup".to_owned(),
                description: "Look up a value".to_owned(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {"key": {"type": "string"}}
                }),
            },
            Tool {
                name: "notify".to_owned(),
                description: "Send a notification".to_owned(),
                input_schema: serde_json::Value::Null,
            },
        ],
        system_prompt: "First line\nSecond line with \"quotes\"".to_owned(),
        prompt: "Run the preset".to_owned(),
        temperature: Some(0.25),
        max_tokens: Some(u64::MAX),
        additional_params: Some(serde_json::json!({
            "priority": ["high", null],
            "nested": {"enabled": true}
        })),
        enable_coco_shim: true,
    }
}

async fn node_has_metadata_json_column(store: &SqliteStore) -> bool {
    let mut connection = store.connect().await.unwrap();
    diesel::sql_query(
        "SELECT COUNT(*) AS count FROM pragma_table_info('nodes') WHERE name = 'metadata_json'",
    )
    .get_result::<ColumnCount>(&mut connection)
    .await
    .unwrap()
    .count
        != 0
}

async fn node_has_kind_json_column(store: &SqliteStore) -> bool {
    let mut connection = store.connect().await.unwrap();
    diesel::sql_query(
        "SELECT COUNT(*) AS count FROM pragma_table_info('nodes') WHERE name = 'kind_json'",
    )
    .get_result::<ColumnCount>(&mut connection)
    .await
    .unwrap()
    .count
        != 0
}

async fn table_exists(store: &SqliteStore, table_name: &str) -> bool {
    let mut connection = store.connect().await.unwrap();
    table_count(&mut connection, &store.database_path, table_name)
        .await
        .unwrap()
        != 0
}

async fn node_anchor_table_exists(store: &SqliteStore) -> bool {
    table_exists(store, "node_anchors").await
}

async fn nodes_have_anchor_columns(store: &SqliteStore) -> bool {
    let mut connection = store.connect().await.unwrap();
    diesel::sql_query(
        "SELECT COUNT(*) AS count FROM pragma_table_info('nodes') WHERE name LIKE 'anchor_%'",
    )
    .get_result::<ColumnCount>(&mut connection)
    .await
    .unwrap()
    .count
        != 0
}

async fn job_has_payload_json_column(store: &SqliteStore) -> bool {
    let mut connection = store.connect().await.unwrap();
    diesel::sql_query(
        "SELECT COUNT(*) AS count FROM pragma_table_info('jobs') WHERE name = 'payload_json'",
    )
    .get_result::<ColumnCount>(&mut connection)
    .await
    .unwrap()
    .count
        != 0
}

async fn session_has_state_json_column(store: &SqliteStore) -> bool {
    let mut connection = store.connect().await.unwrap();
    diesel::sql_query(
        "SELECT COUNT(*) AS count FROM pragma_table_info('sessions') WHERE name = 'state_json'",
    )
    .get_result::<ColumnCount>(&mut connection)
    .await
    .unwrap()
    .count
        != 0
}

async fn preset_has_record_json_column(store: &SqliteStore) -> bool {
    let mut connection = store.connect().await.unwrap();
    diesel::sql_query(
        "SELECT COUNT(*) AS count FROM pragma_table_info('presets') \
         WHERE name = 'record_json'",
    )
    .get_result::<ColumnCount>(&mut connection)
    .await
    .unwrap()
    .count
        != 0
}

async fn skill_has_record_json_column(store: &SqliteStore) -> bool {
    let mut connection = store.connect().await.unwrap();
    diesel::sql_query(
        "SELECT COUNT(*) AS count FROM pragma_table_info('skills') \
         WHERE name = 'record_json'",
    )
    .get_result::<ColumnCount>(&mut connection)
    .await
    .unwrap()
    .count
        != 0
}

fn create_diesel_migration_metadata_for_test(path: &std::path::Path, version: &str) {
    use diesel::connection::SimpleConnection;

    let database_path = sqlite_database_path(path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    connection
        .batch_execute(&format!(
            "CREATE TABLE __diesel_schema_migrations (
            version VARCHAR(50) PRIMARY KEY NOT NULL,
            run_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO __diesel_schema_migrations (version) VALUES ('{version}');"
        ))
        .unwrap();
}

fn revert_store_migrations_to(
    connection: &mut diesel::sqlite::SqliteConnection,
    target_version: i32,
) {
    loop {
        let current_version = diesel::RunQueryDsl::get_result::<Option<String>>(
            __diesel_schema_migrations::table
                .select(diesel::dsl::max(__diesel_schema_migrations::version)),
            connection,
        )
        .unwrap()
        .map(|version| version.trim_start_matches('0').parse::<i32>().unwrap())
        .unwrap_or_default();
        if current_version <= target_version {
            break;
        }
        connection.revert_last_migration(STORE_MIGRATIONS).unwrap();
    }
}

fn create_v6_store_with_legacy_data(path: &std::path::Path) {
    use diesel::connection::SimpleConnection;

    std::fs::create_dir(path).unwrap();
    let database_path = sqlite_database_path(path);
    let mut connection =
        diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
    connection
        .batch_execute(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/migrations/00000000000006_current_store_schema/up.sql"
        )))
        .unwrap();
    connection
        .batch_execute(
            r#"
            CREATE TABLE __diesel_schema_migrations (
                version VARCHAR(50) PRIMARY KEY NOT NULL,
                run_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO __diesel_schema_migrations (version) VALUES ('00000000000006');
            INSERT INTO store_meta (key, value_json) VALUES ('root_id', '"root"');
            INSERT INTO nodes (
                id,
                parent_id,
                created_at,
                role,
                kind,
                anchor_kind,
                metadata_json,
                kind_json
            ) VALUES
            (
                'root',
                '',
                '1970-01-01T00:00:00Z',
                'system',
                'text',
                NULL,
                NULL,
                '{"Text":"The Big Bang"}'
            ),
            (
                'tool-use-node',
                'root',
                '2026-03-25T09:10:11Z',
                'llm',
                'tool_use',
                NULL,
                '{"execution_id":"execution-1","call_id":"call-1"}',
                '{"ToolUse":[{"id":"tool-call-1","name":"exec_command","input":{"cmd":"pwd"}},{"id":"tool-call-2","name":"exec_command","input":{"cmd":"ls"}}]}'
            ),
            (
                'tool-result-node',
                'tool-use-node',
                '2026-03-25T09:10:12Z',
                'user',
                'tool_result',
                NULL,
                '[{"execution_id":"execution-2","call_id":"call-result"}]',
                '{"ToolResult":[{"id":"tool-call-1","output":"ok"},{"id":"tool-call-2","output":"done"}]}'
            );
            INSERT INTO node_relations (child_node_id, parent_node_id, kind, ordinal) VALUES
                ('tool-use-node', 'root', 'primary', 0),
                ('tool-result-node', 'tool-use-node', 'primary', 0);
            INSERT INTO jobs (
                job_id,
                created_at,
                finished_at,
                branch,
                work_branch,
                base,
                status,
                payload_json
            ) VALUES (
                'job-v6',
                '2026-03-25T09:10:13Z',
                NULL,
                'main',
                'main',
                'root',
                'running',
                '{"job_id":"job-v6","created_at":"2026-03-25T09:10:13Z","branch":"main","base":"root","status":"running"}'
            );
            "#,
        )
        .unwrap();
}
