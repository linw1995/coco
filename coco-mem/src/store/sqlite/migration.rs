use std::path::Path;

use diesel::prelude::*;
use diesel::result::OptionalExtension;
use diesel_async::RunQueryDsl;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use snafu::{ResultExt, ensure};

use super::{AsyncSqliteConnection, SqliteStore, StoreAccess, sqlite_database_path};
use crate::StoreResult as Result;
use crate::error::{
    CorruptedStoreSnafu, LegacyJsonStoreSnafu, ParseSqliteStoreValueSnafu, QuerySqliteStoreSnafu,
    StoreError,
};

mod v23;
mod v7;

#[cfg(test)]
pub use v7::NODE_ITEM_ROWS_BACKFILL_META_KEY;
#[cfg(test)]
pub use v23::FS_MIGRATION_COMPLETE_META_KEY;

pub const CURRENT_VERSION: i32 = v23::VERSION;
pub const STORE_MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

const MIN_SUPPORTED_VERSION: i32 = 6;
const DIESEL_MIGRATION_TABLE_NAME: &str = "__diesel_schema_migrations";
const LEGACY_JSON_STORE_MARKERS: &[&str] = &["meta.json", "nodes.jsonl"];

diesel::table! {
    __diesel_schema_migrations (version) {
        version -> Text,
        run_on -> Text,
    }
}

diesel::table! {
    #[sql_name = "store_meta"]
    legacy_store_meta (key) {
        key -> Text,
        value_json -> Text,
    }
}

diesel::table! {
    sqlite_master (name) {
        #[sql_name = "type"]
        object_type -> Text,
        name -> Text,
    }
}

pub async fn run_in_transaction(connection: &mut AsyncSqliteConnection, path: &Path) -> Result<()> {
    reject_unsupported_schema_version(connection, path).await?;
    let before_version = current_schema_version(connection, path).await?;
    let needs_migration = before_version != Some(CURRENT_VERSION);
    if needs_migration {
        tracing::info!(
            path = %path.display(),
            from_version = ?before_version,
            to_version = CURRENT_VERSION,
            "starting SQLite store migrations"
        );
    }

    run_embedded_migrations_through(connection, path, v7::VERSION).await?;
    v7::backfill_node_item_rows_in_transaction(connection, path).await?;
    run_embedded_migrations_through(connection, path, CURRENT_VERSION).await?;

    if needs_migration {
        tracing::info!(
            path = %path.display(),
            version = CURRENT_VERSION,
            "finished SQLite store migrations"
        );
    }
    Ok(())
}

pub async fn ensure_current_schema(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    let version = existing_schema_version(connection, path).await?;
    ensure!(
        version == CURRENT_VERSION,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "unsupported SQLite schema version {version}, expected {CURRENT_VERSION}"
            ),
        }
    );
    Ok(())
}

pub async fn requires_migration(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<bool> {
    let version = existing_schema_version(connection, path).await?;
    ensure_supported_schema_version(version, path)?;
    Ok(version < CURRENT_VERSION)
}

pub async fn existing_schema_version(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<i32> {
    if table_count(connection, path, DIESEL_MIGRATION_TABLE_NAME).await? == 1 {
        if let Some(version) = current_schema_version(connection, path).await? {
            return Ok(version);
        }
        return CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "missing SQLite schema version".to_owned(),
        }
        .fail();
    }

    CorruptedStoreSnafu {
        path: path.to_owned(),
        message: "missing SQLite schema migration table".to_owned(),
    }
    .fail()
}

pub async fn replaces_legacy_json_store(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<bool> {
    v23::replaces_legacy_json_store(connection, path).await
}

pub async fn reject_incomplete_legacy_json_store(path: &Path) -> Result<()> {
    let has_legacy_marker = LEGACY_JSON_STORE_MARKERS
        .iter()
        .any(|file_name| path.join(file_name).exists());
    if !has_legacy_marker {
        return Ok(());
    }

    let database_path = sqlite_database_path(path);
    if database_path.is_file() {
        let store = SqliteStore::new(path, StoreAccess::ReadOnly).await?;
        let mut connection = store.connect().await?;
        if replaces_legacy_json_store(&mut connection, &store.database_path).await? {
            return Ok(());
        }
    }

    LegacyJsonStoreSnafu {
        path: path.to_owned(),
    }
    .fail()
}

pub async fn table_count(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    table_name: &str,
) -> Result<i64> {
    sqlite_master::table
        .filter(sqlite_master::object_type.eq("table"))
        .filter(sqlite_master::name.eq(table_name))
        .count()
        .get_result(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

pub async fn current_schema_version(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<Option<i32>> {
    if table_count(connection, path, DIESEL_MIGRATION_TABLE_NAME).await? == 0 {
        return Ok(None);
    }

    __diesel_schema_migrations::table
        .select(diesel::dsl::max(__diesel_schema_migrations::version))
        .get_result::<Option<String>>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?
        .map(|version| migration_version_to_schema_version(&version, path))
        .transpose()
}

pub async fn load_store_meta_bool(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    key: &str,
) -> Result<bool> {
    if table_count(connection, path, "store_meta").await? == 0 {
        return Ok(false);
    }
    let Some(value) = legacy_store_meta::table
        .filter(legacy_store_meta::key.eq(key))
        .select(legacy_store_meta::value_json)
        .first::<String>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?
    else {
        return Ok(false);
    };
    serde_json::from_str(&value).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: format!("store_meta.{key}"),
    })
}

pub async fn persist_store_meta_bool(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    key: &str,
    value: bool,
) -> Result<()> {
    let value_json = serde_json::to_string(&value).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: format!("store_meta.{key}"),
    })?;
    diesel::insert_into(legacy_store_meta::table)
        .values((
            legacy_store_meta::key.eq(key),
            legacy_store_meta::value_json.eq(value_json),
        ))
        .on_conflict(legacy_store_meta::key)
        .do_update()
        .set(
            legacy_store_meta::value_json
                .eq(diesel::upsert::excluded(legacy_store_meta::value_json)),
        )
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn reject_unsupported_schema_version(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    let Some(version) = current_schema_version(connection, path).await? else {
        return Ok(());
    };
    ensure_supported_schema_version(version, path)
}

fn ensure_supported_schema_version(version: i32, path: &Path) -> Result<()> {
    ensure!(
        (MIN_SUPPORTED_VERSION..=CURRENT_VERSION).contains(&version),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "unsupported SQLite schema version {version}, expected {MIN_SUPPORTED_VERSION}..={CURRENT_VERSION}"
            ),
        }
    );
    Ok(())
}

async fn run_embedded_migrations_through(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    target_version: i32,
) -> Result<()> {
    let path = path.to_owned();
    let target_version = format!("{target_version:014}");
    let result = connection
        .spawn_blocking(move |connection| {
            Ok((|| -> diesel::migration::Result<()> {
                let pending = connection.pending_migrations(STORE_MIGRATIONS)?;
                for migration in pending {
                    if migration.name().version().to_string() > target_version {
                        break;
                    }
                    connection.run_migration(migration.as_ref())?;
                }
                Ok(())
            })())
        })
        .await
        .context(QuerySqliteStoreSnafu { path: path.clone() })?;
    result.map_err(|source| StoreError::MigrateSqliteStore { path, source })
}

fn migration_version_to_schema_version(version: &str, path: &Path) -> Result<i32> {
    let trimmed = version.trim_start_matches('0');
    if trimmed.is_empty() {
        return Ok(0);
    }
    trimmed
        .parse::<i32>()
        .map_err(|source| StoreError::CorruptedStore {
            path: path.to_owned(),
            message: format!("invalid SQLite migration version {version:?}: {source}"),
        })
}
