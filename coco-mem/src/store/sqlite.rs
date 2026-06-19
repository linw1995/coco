use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Integer, Nullable};
use diesel::sqlite::SqliteConnection;
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, RunQueryDsl, SimpleAsyncConnection};
use snafu::prelude::*;
use tokio::runtime::Runtime;

use crate::StoreResult as Result;
use crate::error::{
    ConnectSqliteStoreSnafu, CorruptedStoreSnafu, QuerySqliteStoreSnafu, StartSqliteRuntimeSnafu,
    StorePathIsNotDirectorySnafu, StoreReadOnlySnafu, WriteStoreDirectorySnafu,
};

const SQLITE_DATABASE_FILE_NAME: &str = "store.sqlite3";
const SQLITE_SCHEMA_VERSION: i32 = 1;

type AsyncSqliteConnection = SyncConnectionWrapper<SqliteConnection>;

#[derive(Clone)]
pub struct SqliteStore {
    dir: PathBuf,
    database_path: PathBuf,
    access: StoreAccess,
    runtime: Arc<Runtime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreAccess {
    ReadWrite,
    ReadOnly,
}

struct SqliteMigration {
    version: i32,
    name: &'static str,
    sql: &'static str,
}

#[derive(QueryableByName)]
struct TableCount {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(QueryableByName)]
struct CurrentSchemaVersion {
    #[diesel(sql_type = Nullable<Integer>)]
    version: Option<i32>,
}

const SQLITE_MIGRATIONS: &[SqliteMigration] = &[SqliteMigration {
    version: 1,
    name: "initial-store-schema",
    sql: r#"
CREATE TABLE store_meta (
    key TEXT PRIMARY KEY NOT NULL,
    value_json TEXT NOT NULL
);

CREATE TABLE nodes (
    id TEXT PRIMARY KEY NOT NULL,
    parent_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    role TEXT NOT NULL,
    metadata_json TEXT,
    kind_json TEXT NOT NULL
);

CREATE INDEX nodes_parent_idx ON nodes(parent_id);
CREATE INDEX nodes_created_at_id_idx ON nodes(created_at, id);

CREATE TABLE node_edges (
    parent_id TEXT NOT NULL,
    child_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    PRIMARY KEY (parent_id, child_id, kind),
    FOREIGN KEY (child_id) REFERENCES nodes(id)
);

CREATE INDEX node_edges_parent_idx ON node_edges(parent_id);

CREATE TABLE branches (
    name TEXT PRIMARY KEY NOT NULL,
    head_id TEXT NOT NULL,
    FOREIGN KEY (head_id) REFERENCES nodes(id)
);

CREATE TABLE sessions (
    branch_name TEXT PRIMARY KEY NOT NULL,
    state_json TEXT NOT NULL,
    FOREIGN KEY (branch_name) REFERENCES branches(name) ON DELETE CASCADE
);

CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE message_queue_items (
    queue TEXT NOT NULL,
    message_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (queue, message_id)
);

CREATE INDEX message_queue_items_dequeue_idx ON message_queue_items(queue, created_at, message_id);

CREATE TABLE presets (
    name TEXT PRIMARY KEY NOT NULL,
    record_json TEXT NOT NULL
);

CREATE TABLE skills (
    role TEXT NOT NULL,
    name TEXT NOT NULL,
    record_json TEXT NOT NULL,
    PRIMARY KEY (role, name)
);
"#,
}];

impl std::fmt::Debug for SqliteStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteStore")
            .field("dir", &self.dir)
            .field("database_path", &self.database_path)
            .field("access", &self.access)
            .finish_non_exhaustive()
    }
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        prepare_store_directory(path)?;
        let store = Self::new(path, StoreAccess::ReadWrite)?;
        store.run_migrations()?;
        Ok(store)
    }

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        ensure_existing_store_directory(path)?;
        let store = Self::new(path, StoreAccess::ReadOnly)?;
        store.ensure_current_schema()?;
        Ok(store)
    }

    pub fn store_path(&self) -> &Path {
        &self.dir
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn schema_version(&self) -> Result<i32> {
        self.block_on(async {
            let mut connection = self.connect().await?;
            current_schema_version(&mut connection, &self.database_path)
                .await?
                .context(CorruptedStoreSnafu {
                    path: self.database_path.clone(),
                    message: "missing SQLite schema version".to_owned(),
                })
        })
    }

    fn new(path: &Path, access: StoreAccess) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context(StartSqliteRuntimeSnafu)?;
        Ok(Self {
            dir: path.to_owned(),
            database_path: path.join(SQLITE_DATABASE_FILE_NAME),
            access,
            runtime: Arc::new(runtime),
        })
    }

    fn run_migrations(&self) -> Result<()> {
        self.ensure_writable()?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            configure_writable_connection(&mut connection, &self.database_path).await?;
            ensure_migration_table(&mut connection, &self.database_path).await?;
            reject_newer_schema_version(&mut connection, &self.database_path).await?;
            for migration in SQLITE_MIGRATIONS {
                apply_migration_if_needed(&mut connection, &self.database_path, migration).await?;
            }
            Ok(())
        })
    }

    fn ensure_current_schema(&self) -> Result<()> {
        self.block_on(async {
            let mut connection = self.connect().await?;
            ensure_migration_table_exists(&mut connection, &self.database_path).await?;
            let version = current_schema_version(&mut connection, &self.database_path)
                .await?
                .context(CorruptedStoreSnafu {
                    path: self.database_path.clone(),
                    message: "missing SQLite schema version".to_owned(),
                })?;
            ensure!(
                version == SQLITE_SCHEMA_VERSION,
                CorruptedStoreSnafu {
                    path: self.database_path.clone(),
                    message: format!(
                        "unsupported SQLite schema version {version}, expected {SQLITE_SCHEMA_VERSION}"
                    ),
                }
            );
            Ok(())
        })
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.access == StoreAccess::ReadWrite {
            return Ok(());
        }

        StoreReadOnlySnafu {
            path: self.dir.clone(),
        }
        .fail()
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    async fn connect(&self) -> Result<AsyncSqliteConnection> {
        let database_url = self.database_path.to_string_lossy().into_owned();
        let mut connection = AsyncSqliteConnection::establish(&database_url)
            .await
            .context(ConnectSqliteStoreSnafu {
                path: self.database_path.clone(),
            })?;
        configure_connection(&mut connection, &self.database_path).await?;
        Ok(connection)
    }
}

fn prepare_store_directory(path: &Path) -> Result<()> {
    if path.exists() {
        let metadata = fs::metadata(path).context(WriteStoreDirectorySnafu {
            path: path.to_owned(),
        })?;
        ensure!(
            metadata.is_dir(),
            StorePathIsNotDirectorySnafu {
                path: path.to_owned(),
            }
        );
    } else {
        fs::create_dir_all(path).context(WriteStoreDirectorySnafu {
            path: path.to_owned(),
        })?;
    }
    Ok(())
}

fn ensure_existing_store_directory(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path).context(WriteStoreDirectorySnafu {
        path: path.to_owned(),
    })?;
    ensure!(
        metadata.is_dir(),
        StorePathIsNotDirectorySnafu {
            path: path.to_owned(),
        }
    );
    Ok(())
}

async fn configure_connection(connection: &mut AsyncSqliteConnection, path: &Path) -> Result<()> {
    connection
        .batch_execute(
            r#"
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
"#,
        )
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

async fn configure_writable_connection(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    connection
        .batch_execute("PRAGMA journal_mode = WAL;")
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn ensure_migration_table(connection: &mut AsyncSqliteConnection, path: &Path) -> Result<()> {
    connection
        .batch_execute(
            r#"
CREATE TABLE IF NOT EXISTS store_schema_migrations (
    version INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"#,
        )
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

async fn ensure_migration_table_exists(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    let count = table_count(connection, path, "store_schema_migrations").await?;
    ensure!(
        count == 1,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "missing SQLite schema migration table".to_owned(),
        }
    );
    Ok(())
}

async fn table_count(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    table_name: &str,
) -> Result<i64> {
    sql_query("SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'table' AND name = ?")
        .bind::<diesel::sql_types::Text, _>(table_name)
        .get_result::<TableCount>(connection)
        .await
        .map(|row| row.count)
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

async fn current_schema_version(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<Option<i32>> {
    sql_query("SELECT MAX(version) AS version FROM store_schema_migrations")
        .get_result::<CurrentSchemaVersion>(connection)
        .await
        .map(|row| row.version)
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

async fn reject_newer_schema_version(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    let Some(version) = current_schema_version(connection, path).await? else {
        return Ok(());
    };
    ensure!(
        version <= SQLITE_SCHEMA_VERSION,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "unsupported SQLite schema version {version}, expected at most {SQLITE_SCHEMA_VERSION}"
            ),
        }
    );
    Ok(())
}

async fn apply_migration_if_needed(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    migration: &SqliteMigration,
) -> Result<()> {
    if migration_applied(connection, path, migration.version).await? {
        return Ok(());
    }

    connection
        .batch_execute(migration.sql)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    sql_query("INSERT INTO store_schema_migrations (version, name) VALUES (?, ?)")
        .bind::<Integer, _>(migration.version)
        .bind::<diesel::sql_types::Text, _>(migration.name)
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn migration_applied(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    version: i32,
) -> Result<bool> {
    sql_query("SELECT COUNT(*) AS count FROM store_schema_migrations WHERE version = ?")
        .bind::<Integer, _>(version)
        .get_result::<TableCount>(connection)
        .await
        .map(|row| row.count > 0)
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use super::SqliteStore;

    #[test]
    fn open_creates_sqlite_database_and_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");

        let store = SqliteStore::open(&path).unwrap();

        assert!(store.database_path().is_file());
        assert_eq!(store.schema_version().unwrap(), 1);
    }

    #[test]
    fn open_read_only_accepts_current_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        SqliteStore::open(&path).unwrap();

        let store = SqliteStore::open_read_only(&path).unwrap();

        assert_eq!(store.schema_version().unwrap(), 1);
    }

    #[test]
    fn open_read_only_rejects_missing_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();

        let err = SqliteStore::open_read_only(&path).unwrap_err();

        assert!(err.to_string().contains("SQLite"));
    }
}
