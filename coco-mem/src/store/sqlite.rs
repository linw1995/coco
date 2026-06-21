use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

use diesel::prelude::*;
use diesel::result::OptionalExtension;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Integer, Nullable, Text};
use diesel::sqlite::SqliteConnection;
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, RunQueryDsl, SimpleAsyncConnection};
use snafu::prelude::*;
use tokio::runtime::Runtime;

use super::fs::FsStore;
use super::state::StoreState;
use super::{
    BranchStore, JobStore, MessageQueueStore, NodeStore, PresetStore, ProcessShareableStore,
    SessionStore, SkillStore,
};
use crate::StoreResult as Result;
use crate::error::{
    AmbiguousNodePrefixSnafu, BranchNotFoundSnafu, ConnectSqliteStoreSnafu, CorruptedStoreSnafu,
    NotFoundSnafu, ParentNotFoundSnafu, ParseSqliteStoreValueSnafu, QuerySqliteStoreSnafu,
    RefsNotConnectedSnafu, StartSqliteRuntimeSnafu, StorePathIsNotDirectorySnafu,
    StoreReadOnlySnafu, WriteStoreDirectorySnafu,
};
use crate::{
    Job, JobStatus, Kind, MergeParent, MessageQueueItem, NewNode, Node, NodeMetadata, Preset,
    PresetRecord, Role, SessionAnchorPatch, SessionRole, SessionState, SkillGroups, SkillRecord,
    SkillUpdatePatch, SkillVersionSpec, default_skill_groups,
};

const SQLITE_DATABASE_FILE_NAME: &str = "store.sqlite3";
const SQLITE_MIGRATION_DATABASE_FILE_NAME: &str = "store.sqlite3.migrating";
const SQLITE_INCOMPLETE_DATABASE_FILE_NAME: &str = "store.sqlite3.incomplete";
const LEGACY_FS_META_FILE_NAME: &str = "meta.json";
const FS_MIGRATION_COMPLETE_META_KEY: &str = "fs_migration_complete";
const SQLITE_SCHEMA_VERSION: i32 = 2;

static SQLITE_RUNTIME: OnceLock<Runtime> = OnceLock::new();
static SQLITE_RUNTIME_INIT: Mutex<()> = Mutex::new(());
static SQLITE_DATABASES: OnceLock<Mutex<HashMap<PathBuf, Weak<SqliteDatabaseInner>>>> =
    OnceLock::new();

type AsyncSqliteConnection = SyncConnectionWrapper<SqliteConnection>;

type AsyncSqliteConnectionGuard<'a> = tokio::sync::MutexGuard<'a, AsyncSqliteConnection>;

#[derive(Clone)]
pub struct SqliteDatabase {
    inner: Arc<SqliteDatabaseInner>,
}

struct SqliteDatabaseInner {
    database_path: PathBuf,
    runtime: &'static Runtime,
    connection: Arc<tokio::sync::Mutex<AsyncSqliteConnection>>,
}

#[derive(Clone)]
pub struct SqliteStore {
    dir: PathBuf,
    database_path: PathBuf,
    database: SqliteDatabase,
    access: StoreAccess,
    inner: Arc<RwLock<StoreState>>,
    _lock_file: Option<Arc<std::fs::File>>,
}

#[derive(Clone, Debug)]
pub struct SqliteGraphStore {
    dir: PathBuf,
    database_path: PathBuf,
    database: SqliteDatabase,
    root_id: String,
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

#[derive(QueryableByName)]
struct StoreMetaValue {
    #[diesel(sql_type = Text)]
    value_json: String,
}

#[derive(QueryableByName)]
struct NodeIdRow {
    #[diesel(sql_type = Text)]
    id: String,
}

#[derive(QueryableByName)]
struct NodeRow {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Text)]
    parent_id: String,
    #[diesel(sql_type = Text)]
    created_at: String,
    #[diesel(sql_type = Text)]
    role: String,
    #[diesel(sql_type = Nullable<Text>)]
    metadata_json: Option<String>,
    #[diesel(sql_type = Text)]
    kind_json: String,
}

#[derive(QueryableByName)]
struct BranchRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    head_id: String,
}

#[derive(QueryableByName)]
struct SessionRow {
    #[diesel(sql_type = Text)]
    branch_name: String,
    #[diesel(sql_type = Text)]
    state_json: String,
}

#[derive(QueryableByName)]
struct JobRow {
    #[diesel(sql_type = Text)]
    payload_json: String,
}

#[derive(QueryableByName)]
struct MessageQueueItemRow {
    #[diesel(sql_type = BigInt)]
    row_id: i64,
    #[diesel(sql_type = Text)]
    item_json: String,
}

#[derive(QueryableByName)]
struct PresetRow {
    #[diesel(sql_type = Text)]
    record_json: String,
}

#[derive(QueryableByName)]
struct SkillRow {
    #[diesel(sql_type = Text)]
    role: String,
    #[diesel(sql_type = Text)]
    record_json: String,
}

const SQLITE_MIGRATIONS: &[SqliteMigration] = &[
    SqliteMigration {
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
    },
    SqliteMigration {
        version: 2,
        name: "node-relations",
        sql: r#"
CREATE TABLE node_relations (
    child_node_id TEXT NOT NULL,
    parent_node_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (child_node_id, kind, ordinal),
    FOREIGN KEY (child_node_id) REFERENCES nodes(id),
    FOREIGN KEY (parent_node_id) REFERENCES nodes(id)
);

INSERT INTO node_relations (child_node_id, parent_node_id, kind, ordinal)
SELECT
    child_id,
    parent_id,
    kind,
    ROW_NUMBER() OVER (PARTITION BY child_id, kind ORDER BY rowid) - 1
FROM node_edges;

CREATE INDEX node_relations_child_kind_idx ON node_relations(child_node_id, kind);
CREATE INDEX node_relations_parent_kind_idx ON node_relations(parent_node_id, kind);

DROP TABLE node_edges;
"#,
    },
];

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

impl std::fmt::Debug for SqliteDatabase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteDatabase")
            .field("database_path", &self.inner.database_path)
            .finish_non_exhaustive()
    }
}

impl SqliteDatabase {
    pub fn open_store_path(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(sqlite_database_path(path.as_ref()))
    }

    fn open(database_path: PathBuf) -> Result<Self> {
        let database_path = sqlite_database_registry_path(&database_path)?;
        let databases = SQLITE_DATABASES.get_or_init(|| Mutex::new(HashMap::new()));
        let mut databases = databases
            .lock()
            .expect("SQLite database registry lock poisoned");
        if let Some(inner) = databases
            .get(&database_path)
            .and_then(std::sync::Weak::upgrade)
        {
            return Ok(Self { inner });
        }

        let runtime = sqlite_runtime()?;
        let connection = block_on_sqlite_runtime_with(runtime, async {
            let mut connection = AsyncSqliteConnection::establish(&database_path.to_string_lossy())
                .await
                .context(ConnectSqliteStoreSnafu {
                    path: database_path.clone(),
                })?;
            configure_connection(&mut connection, &database_path).await?;
            Ok(connection)
        })?;
        let inner = Arc::new(SqliteDatabaseInner {
            database_path: database_path.clone(),
            runtime,
            connection: Arc::new(tokio::sync::Mutex::new(connection)),
        });
        databases.insert(database_path, Arc::downgrade(&inner));
        Ok(Self { inner })
    }

    fn connection(&self) -> &tokio::sync::Mutex<AsyncSqliteConnection> {
        &self.inner.connection
    }

    pub fn with_sync_connection<T, E, F, M>(
        &self,
        operation: F,
        map_connection_error: M,
    ) -> std::result::Result<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(&mut SqliteConnection) -> std::result::Result<T, E> + Send + 'static,
        M: FnOnce(diesel::result::Error) -> E,
    {
        let result = self.block_on(async {
            let mut connection = self.connection().lock().await;
            connection
                .spawn_blocking(move |connection| Ok(operation(connection)))
                .await
        });
        match result {
            Ok(result) => result,
            Err(error) => Err(map_connection_error(error)),
        }
    }

    #[cfg(test)]
    fn shared_connection(&self) -> &Arc<tokio::sync::Mutex<AsyncSqliteConnection>> {
        &self.inner.connection
    }

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        block_on_sqlite_runtime_with(self.inner.runtime, future)
    }
}

impl SqliteStore {
    pub fn open_or_migrate_fs(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !legacy_fs_store_exists(path) {
            return Self::open(path);
        }
        if sqlite_database_path(path).is_file() && fs_migration_complete_marker_exists(path)? {
            return Self::open(path);
        }

        prepare_store_directory(path)?;
        let legacy = FsStore::open(path)?;
        if sqlite_database_path(path).is_file() {
            if fs_migration_complete_marker_exists(path)? {
                drop(legacy);
                return Self::open(path);
            }
            quarantine_incomplete_sqlite_database(path)?;
        }
        remove_sqlite_database_files(&sqlite_migration_database_path(path))?;

        let state = legacy.snapshot_state();
        let store = Self::new_with_database_path(
            path,
            sqlite_migration_database_path(path),
            StoreAccess::ReadWrite,
        )?;
        store.run_migrations()?;
        store.persist_state_snapshot(&state)?;
        store.persist_fs_migration_complete_marker()?;
        store.prepare_database_for_atomic_rename()?;
        let migration_database_path = store.database_path.clone();
        drop(store);
        fs::rename(&migration_database_path, sqlite_database_path(path)).context(
            WriteStoreDirectorySnafu {
                path: migration_database_path.clone(),
            },
        )?;

        let migrated = Self::new(path, StoreAccess::ReadWrite)?;
        migrated.load_state()?;
        Ok(migrated)
    }

    pub fn open_read_only_or_migrate_fs(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if sqlite_database_path(path).is_file() {
            if Self::sqlite_schema_requires_migration(path)? {
                drop(Self::open(path)?);
            }
            return Self::open_read_only(path);
        }
        Self::open_read_only(path)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        prepare_store_directory(path)?;
        let store = Self::new(path, StoreAccess::ReadWrite)?;
        store.run_migrations()?;
        store.load_or_initialize_state()?;
        Ok(store)
    }

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        ensure_existing_store_directory(path)?;
        let store = Self::new(path, StoreAccess::ReadOnly)?;
        ensure_existing_database_file(&store.database_path)?;
        store.ensure_current_schema()?;
        store.load_state()?;
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
        Self::new_with_database_path(path, path.join(SQLITE_DATABASE_FILE_NAME), access)
    }

    fn new_with_database_path(
        path: &Path,
        database_path: PathBuf,
        access: StoreAccess,
    ) -> Result<Self> {
        let database = SqliteDatabase::open(database_path.clone())?;
        let lock_file = if access == StoreAccess::ReadWrite {
            Some(super::fs::open_store_lock(path)?)
        } else {
            None
        };
        Ok(Self {
            dir: path.to_owned(),
            database_path: database_path.clone(),
            database,
            access,
            inner: Arc::new(RwLock::new(StoreState::new())),
            _lock_file: lock_file,
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

    fn sqlite_schema_requires_migration(path: &Path) -> Result<bool> {
        ensure_existing_store_directory(path)?;
        let store = Self::new(path, StoreAccess::ReadOnly)?;
        ensure_existing_database_file(&store.database_path)?;
        let version = store.block_on(async {
            let mut connection = store.connect().await?;
            ensure_migration_table_exists(&mut connection, &store.database_path).await?;
            current_schema_version(&mut connection, &store.database_path)
                .await?
                .context(CorruptedStoreSnafu {
                    path: store.database_path.clone(),
                    message: "missing SQLite schema version".to_owned(),
                })
        })?;
        ensure!(
            version <= SQLITE_SCHEMA_VERSION,
            CorruptedStoreSnafu {
                path: store.database_path,
                message: format!(
                    "unsupported SQLite schema version {version}, expected at most {SQLITE_SCHEMA_VERSION}"
                ),
            }
        );
        Ok(version < SQLITE_SCHEMA_VERSION)
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

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: std::future::Future + Send,
        F::Output: Send,
    {
        self.database.block_on(future)
    }

    fn load_or_initialize_state(&self) -> Result<()> {
        let state = self.block_on(async {
            let mut connection = self.connect().await?;
            if node_count(&mut connection, &self.database_path).await? == 0 {
                self.ensure_writable()?;
                let state = StoreState::new();
                persist_root_metadata(&mut connection, &self.database_path, state.root_id())
                    .await?;
                persist_node(&mut connection, &self.database_path, state.root_node()).await?;
                return Ok(state);
            }
            load_state(&mut connection, &self.database_path).await
        })?;
        self.replace_state(state);
        Ok(())
    }

    fn load_state(&self) -> Result<()> {
        let state = self.block_on(async {
            let mut connection = self.connect().await?;
            load_state(&mut connection, &self.database_path).await
        })?;
        self.replace_state(state);
        Ok(())
    }

    fn replace_state(&self, state: StoreState) {
        *self.inner.write().expect("store lock poisoned") = state;
    }

    fn persist_state_snapshot(&self, state: &StoreState) -> Result<()> {
        self.ensure_writable()?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_state_snapshot(&mut connection, &self.database_path, state).await
        })
    }

    fn persist_fs_migration_complete_marker(&self) -> Result<()> {
        self.ensure_writable()?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_store_meta_bool(
                &mut connection,
                &self.database_path,
                FS_MIGRATION_COMPLETE_META_KEY,
                true,
            )
            .await
        })
    }

    fn prepare_database_for_atomic_rename(&self) -> Result<()> {
        self.ensure_writable()?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            connection
                .batch_execute(
                    r#"
PRAGMA wal_checkpoint(TRUNCATE);
PRAGMA journal_mode = DELETE;
"#,
                )
                .await
                .context(QuerySqliteStoreSnafu {
                    path: self.database_path.clone(),
                })
        })
    }

    async fn connect(&self) -> Result<AsyncSqliteConnectionGuard<'_>> {
        Ok(self.database.connection().lock().await)
    }

    #[cfg(test)]
    pub fn snapshot_state(&self) -> StoreState {
        self.inner.read().expect("store lock poisoned").clone()
    }
}

impl SqliteGraphStore {
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        ensure_existing_store_directory(path)?;
        let store = Self::new(path)?;
        ensure_existing_database_file(&store.database_path)?;
        store.ensure_current_schema()?;
        let root_id = store.block_on(async {
            let mut connection = store.connect().await?;
            load_root_id(&mut connection, &store.database_path).await
        })?;
        Ok(Self { root_id, ..store })
    }

    pub fn store_path(&self) -> &Path {
        &self.dir
    }

    fn new(path: &Path) -> Result<Self> {
        let database_path = sqlite_database_path(path);
        let database = SqliteDatabase::open(database_path.clone())?;
        Ok(Self {
            dir: path.to_owned(),
            database_path,
            database,
            root_id: String::new(),
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

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: std::future::Future + Send,
        F::Output: Send,
    {
        self.database.block_on(future)
    }

    async fn connect(&self) -> Result<AsyncSqliteConnectionGuard<'_>> {
        Ok(self.database.connection().lock().await)
    }

    fn ensure_read_only<T>(&self) -> Result<T> {
        StoreReadOnlySnafu {
            path: self.dir.clone(),
        }
        .fail()
    }

    fn get_node_by_exact_id(&self, id: &str) -> Result<Node> {
        self.block_on(async {
            let mut connection = self.connect().await?;
            let row = sql_query(
                r#"
SELECT id, parent_id, created_at, role, metadata_json, kind_json
FROM nodes
WHERE id = ?
"#,
            )
            .bind::<Text, _>(id)
            .get_result::<NodeRow>(&mut connection)
            .await
            .optional()
            .context(QuerySqliteStoreSnafu {
                path: self.database_path.clone(),
            })?
            .context(NotFoundSnafu { id: id.to_owned() })?;
            row.into_node(&self.database_path)
        })
    }

    fn resolve_ref_id(&self, reference: &str) -> Result<String> {
        if self.node_exists(reference)? {
            return Ok(reference.to_owned());
        }
        if let Some(head_id) = self.branch_head(reference)? {
            self.get_node_by_exact_id(&head_id)?;
            return Ok(head_id);
        }
        NotFoundSnafu {
            id: reference.to_owned(),
        }
        .fail()
    }

    fn branch_head(&self, name: &str) -> Result<Option<String>> {
        self.block_on(async {
            let mut connection = self.connect().await?;
            sql_query("SELECT head_id AS id FROM branches WHERE name = ?")
                .bind::<Text, _>(name)
                .get_result::<NodeIdRow>(&mut connection)
                .await
                .optional()
                .context(QuerySqliteStoreSnafu {
                    path: self.database_path.clone(),
                })
                .map(|row| row.map(|row| row.id))
        })
    }

    fn node_exists(&self, id: &str) -> Result<bool> {
        self.block_on(async {
            let mut connection = self.connect().await?;
            let count = sql_query("SELECT COUNT(*) AS count FROM nodes WHERE id = ?")
                .bind::<Text, _>(id)
                .get_result::<TableCount>(&mut connection)
                .await
                .context(QuerySqliteStoreSnafu {
                    path: self.database_path.clone(),
                })?
                .count;
            Ok(count > 0)
        })
    }

    fn get_node_by_prefix_or_branch(&self, reference: &str) -> Result<Node> {
        if let Some(head_id) = self.branch_head(reference)? {
            return self.get_node_by_exact_id(&head_id);
        }

        match self.get_node_by_exact_id(reference) {
            Ok(node) => Ok(node),
            Err(crate::StoreError::NotFound { .. }) => self.get_node_by_prefix(reference),
            Err(error) => Err(error),
        }
    }

    fn get_node_by_prefix(&self, prefix: &str) -> Result<Node> {
        match self.node_ids_by_prefix(prefix)?.as_slice() {
            [matched] => self.get_node_by_exact_id(matched),
            [] => NotFoundSnafu {
                id: prefix.to_owned(),
            }
            .fail(),
            matches => AmbiguousNodePrefixSnafu {
                prefix: prefix.to_owned(),
                matches: matches.to_vec(),
            }
            .fail(),
        }
    }

    fn node_ids_by_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        self.block_on(async {
            let mut connection = self.connect().await?;
            sql_query("SELECT id FROM nodes WHERE id LIKE ? ORDER BY id")
                .bind::<Text, _>(format!("{prefix}%"))
                .load::<NodeIdRow>(&mut connection)
                .await
                .context(QuerySqliteStoreSnafu {
                    path: self.database_path.clone(),
                })
                .map(|rows| rows.into_iter().map(|row| row.id).collect())
        })
    }
}

pub(super) fn sqlite_database_path(path: &Path) -> PathBuf {
    path.join(SQLITE_DATABASE_FILE_NAME)
}

fn sqlite_database_registry_path(database_path: &Path) -> Result<PathBuf> {
    let Some(parent) = database_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    else {
        return Ok(database_path.to_owned());
    };
    let Some(file_name) = database_path.file_name() else {
        return Ok(database_path.to_owned());
    };
    let parent = parent
        .canonicalize()
        .context(WriteStoreDirectorySnafu { path: parent })?;
    Ok(parent.join(file_name))
}

fn sqlite_migration_database_path(path: &Path) -> PathBuf {
    path.join(SQLITE_MIGRATION_DATABASE_FILE_NAME)
}

pub(super) fn legacy_fs_store_exists(path: &Path) -> bool {
    path.join(LEGACY_FS_META_FILE_NAME).is_file()
}

pub(super) fn fs_migration_complete_marker_exists(path: &Path) -> Result<bool> {
    let store = SqliteStore::new(path, StoreAccess::ReadOnly)?;
    store.block_on(async {
        let mut connection = store.connect().await?;
        load_store_meta_bool(
            &mut connection,
            &store.database_path,
            FS_MIGRATION_COMPLETE_META_KEY,
        )
        .await
    })
}

fn quarantine_incomplete_sqlite_database(path: &Path) -> Result<()> {
    let database_path = sqlite_database_path(path);
    let incomplete_path = path.join(SQLITE_INCOMPLETE_DATABASE_FILE_NAME);
    remove_sqlite_database_files(&incomplete_path)?;
    fs::rename(&database_path, &incomplete_path).context(WriteStoreDirectorySnafu {
        path: database_path.clone(),
    })?;
    for suffix in ["-wal", "-shm"] {
        let source = PathBuf::from(format!("{}{}", database_path.display(), suffix));
        if source.exists() {
            let target = PathBuf::from(format!("{}{}", incomplete_path.display(), suffix));
            fs::rename(&source, &target).context(WriteStoreDirectorySnafu { path: source })?;
        }
    }
    Ok(())
}

fn remove_sqlite_database_files(path: &Path) -> Result<()> {
    remove_file_if_exists(path)?;
    for suffix in ["-wal", "-shm"] {
        let sidecar = PathBuf::from(format!("{}{}", path.display(), suffix));
        remove_file_if_exists(&sidecar)?;
    }
    Ok(())
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(source).context(WriteStoreDirectorySnafu {
            path: path.to_owned(),
        }),
    }
}

fn sqlite_runtime() -> Result<&'static Runtime> {
    if let Some(runtime) = SQLITE_RUNTIME.get() {
        return Ok(runtime);
    }
    let _guard = SQLITE_RUNTIME_INIT
        .lock()
        .expect("SQLite runtime init lock poisoned");
    if let Some(runtime) = SQLITE_RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context(StartSqliteRuntimeSnafu)?;
    let _ = SQLITE_RUNTIME.set(runtime);
    Ok(SQLITE_RUNTIME
        .get()
        .expect("SQLite runtime should be initialized"))
}

fn block_on_sqlite_runtime_with<F>(runtime: &'static Runtime, future: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        return std::thread::scope(|scope| {
            scope
                .spawn(|| runtime.block_on(future))
                .join()
                .expect("SQLite store worker thread should not panic")
        });
    }
    runtime.block_on(future)
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

fn ensure_existing_database_file(path: &Path) -> Result<()> {
    ensure!(
        path.is_file(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "missing SQLite database file".to_owned(),
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
        .batch_execute("BEGIN IMMEDIATE")
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    let result = apply_migration_in_transaction(connection, path, migration).await;
    match result {
        Ok(()) => connection
            .batch_execute("COMMIT")
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            }),
        Err(error) => {
            let _ = connection.batch_execute("ROLLBACK").await;
            Err(error)
        }
    }
}

async fn apply_migration_in_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    migration: &SqliteMigration,
) -> Result<()> {
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

async fn node_count(connection: &mut AsyncSqliteConnection, path: &Path) -> Result<i64> {
    sql_query("SELECT COUNT(*) AS count FROM nodes")
        .get_result::<TableCount>(connection)
        .await
        .map(|row| row.count)
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

async fn persist_root_metadata(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    root_id: &str,
) -> Result<()> {
    let root_json = serde_json::to_string(root_id).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "store_meta.root_id".to_owned(),
    })?;
    sql_query(
        r#"
INSERT INTO store_meta (key, value_json)
VALUES ('root_id', ?)
ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json
"#,
    )
    .bind::<Text, _>(root_json)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

async fn persist_store_meta_bool(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    key: &str,
    value: bool,
) -> Result<()> {
    let value_json = serde_json::to_string(&value).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: format!("store_meta.{key}"),
    })?;
    sql_query(
        r#"
INSERT INTO store_meta (key, value_json)
VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json
"#,
    )
    .bind::<Text, _>(key)
    .bind::<Text, _>(value_json)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

async fn load_store_meta_bool(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    key: &str,
) -> Result<bool> {
    if table_count(connection, path, "store_meta").await? == 0 {
        return Ok(false);
    }
    let Some(value) = sql_query("SELECT value_json FROM store_meta WHERE key = ?")
        .bind::<Text, _>(key)
        .get_result::<StoreMetaValue>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?
    else {
        return Ok(false);
    };
    serde_json::from_str(&value.value_json).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: format!("store_meta.{key}"),
    })
}

async fn persist_state_snapshot(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    state: &StoreState,
) -> Result<()> {
    persist_root_metadata(connection, path, state.root_id()).await?;

    let mut nodes = state.nodes.values().collect::<Vec<_>>();
    nodes.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    for node in nodes {
        persist_node(connection, path, node).await?;
    }

    let mut branches = state.branches.iter().collect::<Vec<_>>();
    branches.sort_by(|(left, _), (right, _)| left.cmp(right));
    for (branch, head_id) in branches {
        persist_branch(connection, path, branch, head_id).await?;
    }

    let mut sessions = state.sessions.iter().collect::<Vec<_>>();
    sessions.sort_by(|(left, _), (right, _)| left.cmp(right));
    for (branch, session_state) in sessions {
        persist_session_state(connection, path, branch, session_state).await?;
    }

    let mut presets = state.presets.values().collect::<Vec<_>>();
    presets.sort_by(|left, right| left.name.cmp(&right.name));
    for preset in presets {
        persist_preset(connection, path, preset).await?;
    }

    let default_skills = default_skill_groups();
    for role in [SessionRole::Orchestrator, SessionRole::Runner] {
        for record in state.skill_groups.for_role(role).values() {
            if !should_persist_skill_record(&default_skills, role, record) {
                continue;
            }
            persist_skill(connection, path, role, record).await?;
        }
    }

    let mut jobs = state.jobs.values().collect::<Vec<_>>();
    jobs.sort_by(|left, right| left.job_id.cmp(&right.job_id));
    for job in jobs {
        persist_job(connection, path, job).await?;
    }

    let mut queues = state.message_queues.iter().collect::<Vec<_>>();
    queues.sort_by(|(left, _), (right, _)| left.cmp(right));
    for (_, items) in queues {
        for item in items {
            persist_message_queue_item(connection, path, item).await?;
        }
    }

    Ok(())
}

fn should_persist_skill_record(
    defaults: &SkillGroups,
    role: SessionRole,
    record: &SkillRecord,
) -> bool {
    let Some(default_record) = defaults.for_role(role).get(&record.name) else {
        return true;
    };
    !skill_record_matches_default(default_record, record)
}

fn skill_record_matches_default(default_record: &SkillRecord, record: &SkillRecord) -> bool {
    if record.current_version != default_record.current_version {
        return false;
    }
    if record.versions.len() != default_record.versions.len() {
        return false;
    }
    record
        .versions
        .iter()
        .zip(default_record.versions.iter())
        .all(
            |((version, record_version), (default_version, default_version_record))| {
                version == default_version && record_version.id == default_version_record.id
            },
        )
}

async fn load_root_id(connection: &mut AsyncSqliteConnection, path: &Path) -> Result<String> {
    let Some(value) = sql_query("SELECT value_json FROM store_meta WHERE key = 'root_id'")
        .get_result::<StoreMetaValue>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?
    else {
        return CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "missing SQLite root metadata".to_owned(),
        }
        .fail();
    };
    serde_json::from_str(&value.value_json).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "store_meta.root_id".to_owned(),
    })
}

async fn load_state(connection: &mut AsyncSqliteConnection, path: &Path) -> Result<StoreState> {
    let root_id = load_root_id(connection, path).await?;
    let mut rows = load_node_rows(connection, path).await?;
    let root_index =
        rows.iter()
            .position(|row| row.id == root_id)
            .context(CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("root node {root_id:?} is missing"),
            })?;
    let root = rows.remove(root_index).into_node(path)?;
    ensure!(
        root.is_root(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "root node must not have a parent".to_owned(),
        }
    );

    let mut state = StoreState::from_root(root);
    insert_node_rows(&mut state, rows, path)?;
    load_branches(connection, path, &mut state).await?;
    load_sessions(connection, path, &mut state).await?;
    load_presets(connection, path, &mut state).await?;
    load_skills(connection, path, &mut state).await?;
    load_jobs(connection, path, &mut state).await?;
    load_message_queue_items(connection, path, &mut state).await?;
    Ok(state)
}

async fn load_node_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<Vec<NodeRow>> {
    sql_query(
        r#"
SELECT id, parent_id, created_at, role, metadata_json, kind_json
FROM nodes
ORDER BY created_at, id
"#,
    )
    .load::<NodeRow>(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })
}

fn insert_node_rows(state: &mut StoreState, mut rows: Vec<NodeRow>, path: &Path) -> Result<()> {
    while !rows.is_empty() {
        let initial_len = rows.len();
        let mut pending = Vec::new();
        for row in rows {
            let node = row.into_node(path)?;
            if node_references_known_parents(state, &node) {
                state.insert_existing_node(node)?;
            } else {
                pending.push(NodeRow::from_node(node, path)?);
            }
        }

        ensure!(
            pending.len() < initial_len,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: "SQLite nodes contain missing or cyclic parents".to_owned(),
            }
        );
        rows = pending;
    }
    Ok(())
}

fn node_references_known_parents(state: &StoreState, node: &Node) -> bool {
    if !state.nodes.contains_key(&node.parent) {
        return false;
    }
    let Kind::Anchor(anchor) = &node.kind else {
        return true;
    };
    anchor
        .merge_parents()
        .iter()
        .all(|parent| state.nodes.contains_key(parent.node_id()))
}

async fn persist_node(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    let row = NodeRow::from_node(node.clone(), path)?;
    sql_query(
        r#"
INSERT INTO nodes (id, parent_id, created_at, role, metadata_json, kind_json)
VALUES (?, ?, ?, ?, ?, ?)
"#,
    )
    .bind::<Text, _>(row.id)
    .bind::<Text, _>(row.parent_id)
    .bind::<Text, _>(row.created_at)
    .bind::<Text, _>(row.role)
    .bind::<Nullable<Text>, _>(row.metadata_json)
    .bind::<Text, _>(row.kind_json)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;

    for relation in node_relations(node) {
        sql_query(
            r#"
INSERT INTO node_relations (child_node_id, parent_node_id, kind, ordinal)
VALUES (?, ?, ?, ?)
"#,
        )
        .bind::<Text, _>(relation.child_node_id)
        .bind::<Text, _>(relation.parent_node_id)
        .bind::<Text, _>(relation.kind)
        .bind::<Integer, _>(relation.ordinal)
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    }
    Ok(())
}

async fn upsert_node(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    let row = NodeRow::from_node(node.clone(), path)?;
    sql_query(
        r#"
INSERT INTO nodes (id, parent_id, created_at, role, metadata_json, kind_json)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    parent_id = excluded.parent_id,
    created_at = excluded.created_at,
    role = excluded.role,
    metadata_json = excluded.metadata_json,
    kind_json = excluded.kind_json
"#,
    )
    .bind::<Text, _>(row.id)
    .bind::<Text, _>(row.parent_id)
    .bind::<Text, _>(row.created_at)
    .bind::<Text, _>(row.role)
    .bind::<Nullable<Text>, _>(row.metadata_json)
    .bind::<Text, _>(row.kind_json)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;

    sql_query("DELETE FROM node_relations WHERE child_node_id = ?")
        .bind::<Text, _>(&node.id)
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    for relation in node_relations(node) {
        sql_query(
            r#"
INSERT INTO node_relations (child_node_id, parent_node_id, kind, ordinal)
VALUES (?, ?, ?, ?)
"#,
        )
        .bind::<Text, _>(relation.child_node_id)
        .bind::<Text, _>(relation.parent_node_id)
        .bind::<Text, _>(relation.kind)
        .bind::<Integer, _>(relation.ordinal)
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    }
    Ok(())
}

struct NodeRelation {
    child_node_id: String,
    parent_node_id: String,
    kind: String,
    ordinal: i32,
}

fn node_relations(node: &Node) -> Vec<NodeRelation> {
    let mut relations = Vec::new();
    if !node.parent.is_empty() {
        relations.push(NodeRelation {
            child_node_id: node.id.clone(),
            parent_node_id: node.parent.clone(),
            kind: "primary".to_owned(),
            ordinal: 0,
        });
    }
    if let Kind::Anchor(anchor) = &node.kind {
        relations.extend(
            anchor
                .merge_parents()
                .iter()
                .enumerate()
                .map(|(ordinal, parent)| NodeRelation {
                    child_node_id: node.id.clone(),
                    parent_node_id: parent.node_id().to_owned(),
                    kind: merge_parent_relation_kind(parent).to_owned(),
                    ordinal: ordinal as i32,
                }),
        );
    }
    relations
}

fn merge_parent_relation_kind(parent: &MergeParent) -> &'static str {
    if parent.is_shadow() {
        "shadow"
    } else {
        "merge"
    }
}

impl NodeRow {
    fn from_node(node: Node, path: &Path) -> Result<Self> {
        Ok(Self {
            id: node.id,
            parent_id: node.parent,
            created_at: node.created_at.to_string(),
            role: role_name(&node.role).to_owned(),
            metadata_json: node
                .metadata
                .map(|metadata| {
                    serde_json::to_string(&metadata).context(ParseSqliteStoreValueSnafu {
                        path: path.to_owned(),
                        column: "nodes.metadata_json".to_owned(),
                    })
                })
                .transpose()?,
            kind_json: serde_json::to_string(&node.kind).context(ParseSqliteStoreValueSnafu {
                path: path.to_owned(),
                column: "nodes.kind_json".to_owned(),
            })?,
        })
    }

    fn into_node(self, path: &Path) -> Result<Node> {
        Ok(Node {
            id: self.id,
            parent: self.parent_id,
            created_at: self.created_at.parse().map_err(|source| {
                crate::StoreError::CorruptedStore {
                    path: path.to_owned(),
                    message: format!("invalid SQLite node timestamp: {source}"),
                }
            })?,
            role: parse_role(&self.role, path)?,
            metadata: self
                .metadata_json
                .map(|metadata| {
                    serde_json::from_str::<NodeMetadata>(&metadata).context(
                        ParseSqliteStoreValueSnafu {
                            path: path.to_owned(),
                            column: "nodes.metadata_json".to_owned(),
                        },
                    )
                })
                .transpose()?,
            kind: serde_json::from_str(&self.kind_json).context(ParseSqliteStoreValueSnafu {
                path: path.to_owned(),
                column: "nodes.kind_json".to_owned(),
            })?,
        })
    }
}

fn role_name(role: &Role) -> &'static str {
    match role {
        Role::User => "user",
        Role::System => "system",
        Role::LLM => "llm",
    }
}

fn parse_role(role: &str, path: &Path) -> Result<Role> {
    match role {
        "user" => Ok(Role::User),
        "system" => Ok(Role::System),
        "llm" => Ok(Role::LLM),
        _ => CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("invalid SQLite node role {role:?}"),
        }
        .fail(),
    }
}

async fn load_branches(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    state: &mut StoreState,
) -> Result<()> {
    let branches = sql_query("SELECT name, head_id FROM branches ORDER BY name")
        .load::<BranchRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    for branch in branches {
        state.apply_fork(branch.name, branch.head_id)?;
    }
    Ok(())
}

async fn load_sessions(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    state: &mut StoreState,
) -> Result<()> {
    let sessions = sql_query("SELECT branch_name, state_json FROM sessions ORDER BY branch_name")
        .load::<SessionRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    state.sessions.clear();
    for session in sessions {
        let state_json = serde_json::from_str::<SessionState>(&session.state_json).context(
            ParseSqliteStoreValueSnafu {
                path: path.to_owned(),
                column: "sessions.state_json".to_owned(),
            },
        )?;
        state.sessions.insert(session.branch_name, state_json);
    }
    state.validate_session_records()
}

async fn persist_branch(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    head_id: &str,
) -> Result<()> {
    sql_query("INSERT INTO branches (name, head_id) VALUES (?, ?)")
        .bind::<Text, _>(branch)
        .bind::<Text, _>(head_id)
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn persist_branch_and_session_state(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    head_id: &str,
    session_state: &SessionState,
) -> Result<()> {
    connection
        .batch_execute("BEGIN IMMEDIATE")
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    let result = async {
        persist_branch(connection, path, branch, head_id).await?;
        persist_session_state(connection, path, branch, session_state).await
    }
    .await;
    match result {
        Ok(()) => connection
            .batch_execute("COMMIT")
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            }),
        Err(error) => {
            let _ = connection.batch_execute("ROLLBACK").await;
            Err(error)
        }
    }
}

async fn persist_session_state(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    state: &SessionState,
) -> Result<()> {
    let state_json = serde_json::to_string(state).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "sessions.state_json".to_owned(),
    })?;
    sql_query(
        r#"
INSERT INTO sessions (branch_name, state_json)
VALUES (?, ?)
ON CONFLICT(branch_name) DO UPDATE SET state_json = excluded.state_json
"#,
    )
    .bind::<Text, _>(branch)
    .bind::<Text, _>(state_json)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

async fn update_branch_head(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected_old_head: &str,
    new_head: &str,
) -> Result<usize> {
    sql_query("UPDATE branches SET head_id = ? WHERE name = ? AND head_id = ?")
        .bind::<Text, _>(new_head)
        .bind::<Text, _>(branch)
        .bind::<Text, _>(expected_old_head)
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

async fn delete_branch_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
) -> Result<()> {
    sql_query("DELETE FROM branches WHERE name = ?")
        .bind::<Text, _>(branch)
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn persist_session_nodes_and_branch_head(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected_old_head: &str,
    new_head: &str,
    nodes: &[Node],
) -> Result<()> {
    for node in nodes {
        upsert_node(connection, path, node).await?;
    }
    let updated = update_branch_head(connection, path, branch, expected_old_head, new_head).await?;
    ensure!(
        updated == 1,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite branch {branch:?} did not match expected head"),
        }
    );
    Ok(())
}

async fn load_jobs(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    state: &mut StoreState,
) -> Result<()> {
    let jobs = sql_query("SELECT payload_json FROM jobs ORDER BY job_id")
        .load::<JobRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    state.jobs.clear();
    for row in jobs {
        let job =
            serde_json::from_str::<Job>(&row.payload_json).context(ParseSqliteStoreValueSnafu {
                path: path.to_owned(),
                column: "jobs.payload_json".to_owned(),
            })?;
        state.jobs.insert(job.job_id.clone(), job);
    }
    Ok(())
}

async fn persist_job(connection: &mut AsyncSqliteConnection, path: &Path, job: &Job) -> Result<()> {
    let payload_json = serde_json::to_string(job).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "jobs.payload_json".to_owned(),
    })?;
    sql_query(
        r#"
INSERT INTO jobs (job_id, payload_json)
VALUES (?, ?)
ON CONFLICT(job_id) DO UPDATE SET payload_json = excluded.payload_json
"#,
    )
    .bind::<Text, _>(&job.job_id)
    .bind::<Text, _>(payload_json)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

async fn load_message_queue_items(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    state: &mut StoreState,
) -> Result<()> {
    let rows = sql_query(
        r#"
SELECT rowid AS row_id, payload_json AS item_json
FROM message_queue_items
ORDER BY rowid
"#,
    )
    .load::<MessageQueueItemRow>(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    state.message_queues.clear();
    let mut items = Vec::new();
    for row in rows {
        let item = serde_json::from_str::<MessageQueueItem>(&row.item_json).context(
            ParseSqliteStoreValueSnafu {
                path: path.to_owned(),
                column: "message_queue_items.payload_json".to_owned(),
            },
        )?;
        items.push((row.row_id, item));
    }
    items.sort_by(|(left_row_id, left), (right_row_id, right)| {
        left.queue
            .cmp(&right.queue)
            .then_with(|| left.created_at.cmp(&right.created_at))
            .then_with(|| left_row_id.cmp(right_row_id))
    });
    for (_, item) in items {
        state
            .message_queues
            .entry(item.queue.clone())
            .or_default()
            .push(item);
    }
    Ok(())
}

async fn persist_message_queue_item(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    item: &MessageQueueItem,
) -> Result<()> {
    let item_json = serde_json::to_string(item).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "message_queue_items.payload_json".to_owned(),
    })?;
    sql_query(
        r#"
INSERT INTO message_queue_items (queue, message_id, created_at, payload_json)
VALUES (?, ?, ?, ?)
"#,
    )
    .bind::<Text, _>(&item.queue)
    .bind::<Text, _>(&item.message_id)
    .bind::<Text, _>(item.created_at.to_string())
    .bind::<Text, _>(item_json)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

async fn delete_message_queue_item(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    item: &MessageQueueItem,
) -> Result<()> {
    sql_query("DELETE FROM message_queue_items WHERE queue = ? AND message_id = ?")
        .bind::<Text, _>(&item.queue)
        .bind::<Text, _>(&item.message_id)
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn load_presets(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    state: &mut StoreState,
) -> Result<()> {
    let rows = sql_query("SELECT record_json FROM presets ORDER BY name")
        .load::<PresetRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    state.presets.clear();
    for row in rows {
        let record = serde_json::from_str::<PresetRecord>(&row.record_json).context(
            ParseSqliteStoreValueSnafu {
                path: path.to_owned(),
                column: "presets.record_json".to_owned(),
            },
        )?;
        state.presets.insert(record.name.clone(), record);
    }
    Ok(())
}

async fn persist_preset(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    record: &PresetRecord,
) -> Result<()> {
    let record_json = serde_json::to_string(record).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "presets.record_json".to_owned(),
    })?;
    sql_query(
        r#"
INSERT INTO presets (name, record_json)
VALUES (?, ?)
ON CONFLICT(name) DO UPDATE SET record_json = excluded.record_json
"#,
    )
    .bind::<Text, _>(&record.name)
    .bind::<Text, _>(record_json)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

async fn delete_preset_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    name: &str,
) -> Result<()> {
    sql_query("DELETE FROM presets WHERE name = ?")
        .bind::<Text, _>(name)
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn load_skills(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    state: &mut StoreState,
) -> Result<()> {
    let rows = sql_query("SELECT role, record_json FROM skills ORDER BY role, name")
        .load::<SkillRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    for row in rows {
        let role = parse_session_role(&row.role, path)?;
        let record = serde_json::from_str::<SkillRecord>(&row.record_json).context(
            ParseSqliteStoreValueSnafu {
                path: path.to_owned(),
                column: "skills.record_json".to_owned(),
            },
        )?;
        state
            .skill_groups
            .for_role_mut(role)
            .insert(record.name.clone(), record);
    }
    Ok(())
}

async fn persist_skill(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    role: SessionRole,
    record: &SkillRecord,
) -> Result<()> {
    let record_json = serde_json::to_string(record).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "skills.record_json".to_owned(),
    })?;
    sql_query(
        r#"
INSERT INTO skills (role, name, record_json)
VALUES (?, ?, ?)
ON CONFLICT(role, name) DO UPDATE SET record_json = excluded.record_json
"#,
    )
    .bind::<Text, _>(role.as_str())
    .bind::<Text, _>(&record.name)
    .bind::<Text, _>(record_json)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

fn parse_session_role(role: &str, path: &Path) -> Result<SessionRole> {
    match role {
        "orchestrator" => Ok(SessionRole::Orchestrator),
        "runner" => Ok(SessionRole::Runner),
        _ => CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("invalid SQLite session role {role:?}"),
        }
        .fail(),
    }
}

impl NodeStore for SqliteGraphStore {
    fn root_id(&self) -> String {
        self.root_id.clone()
    }

    fn append(&self, _node: NewNode) -> Result<String> {
        self.ensure_read_only()
    }

    fn ancestry(&self, head_ref: &str) -> Result<Vec<Node>> {
        let head_id = self.resolve_ref_id(head_ref)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            let rows = sql_query(
                r#"
WITH RECURSIVE ancestry(id, depth) AS (
    SELECT ? AS id, 0 AS depth
    UNION ALL
    SELECT nodes.parent_id, ancestry.depth + 1
    FROM nodes
    JOIN ancestry ON nodes.id = ancestry.id
    WHERE nodes.parent_id != ''
)
SELECT nodes.id, nodes.parent_id, nodes.created_at, nodes.role, nodes.metadata_json, nodes.kind_json
FROM ancestry
JOIN nodes ON nodes.id = ancestry.id
ORDER BY ancestry.depth
"#,
            )
            .bind::<Text, _>(&head_id)
            .load::<NodeRow>(&mut connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: self.database_path.clone(),
            })?;

            let nodes = rows
                .into_iter()
                .map(|row| row.into_node(&self.database_path))
                .collect::<Result<Vec<_>>>()?;
            if let Some(last) = nodes.last()
                && !last.is_root()
            {
                return ParentNotFoundSnafu {
                    id: last.parent.clone(),
                }
                .fail();
            }
            Ok(nodes)
        })
    }

    fn log(&self, base_ref: &str, head_ref: &str) -> Result<Vec<Node>> {
        let base_id = self.resolve_ref_id(base_ref)?;
        let mut nodes = self.ancestry(head_ref)?;
        let Some(index) = nodes.iter().position(|node| node.id == base_id) else {
            return RefsNotConnectedSnafu {
                base_ref: base_ref.to_owned(),
                head_ref: head_ref.to_owned(),
            }
            .fail();
        };
        nodes.truncate(index + 1);
        Ok(nodes)
    }

    fn get_node(&self, id: &str) -> Result<Node> {
        self.get_node_by_prefix_or_branch(id)
    }

    fn list_children(&self, node_id: &str) -> Result<Vec<Node>> {
        self.get_node_by_exact_id(node_id)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            let rows = sql_query(
                r#"
SELECT nodes.id, nodes.parent_id, nodes.created_at, nodes.role, nodes.metadata_json, nodes.kind_json
FROM node_relations
JOIN nodes ON nodes.id = node_relations.child_node_id
WHERE node_relations.parent_node_id = ?
ORDER BY nodes.created_at, nodes.id
"#,
            )
            .bind::<Text, _>(node_id)
            .load::<NodeRow>(&mut connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: self.database_path.clone(),
            })?;
            rows.into_iter()
                .map(|row| row.into_node(&self.database_path))
                .collect()
        })
    }
}

impl BranchStore for SqliteGraphStore {
    fn fork(&self, _name: &str, _from_ref: &str) -> Result<String> {
        self.ensure_read_only()
    }

    fn get_branch_head(&self, name: &str) -> Result<String> {
        self.branch_head(name)?.context(BranchNotFoundSnafu {
            name: name.to_owned(),
        })
    }

    fn delete_branch(&self, _name: &str) -> Result<()> {
        self.ensure_read_only()
    }

    fn set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _new_head: &str,
    ) -> Result<()> {
        self.ensure_read_only()
    }
}

impl SessionStore for SqliteGraphStore {
    fn list_session_states(&self) -> Result<std::collections::HashMap<String, SessionState>> {
        self.block_on(async {
            let mut connection = self.connect().await?;
            let sessions =
                sql_query("SELECT branch_name, state_json FROM sessions ORDER BY branch_name")
                    .load::<SessionRow>(&mut connection)
                    .await
                    .context(QuerySqliteStoreSnafu {
                        path: self.database_path.clone(),
                    })?;
            sessions
                .into_iter()
                .map(|session| {
                    let state = serde_json::from_str::<SessionState>(&session.state_json).context(
                        ParseSqliteStoreValueSnafu {
                            path: self.database_path.clone(),
                            column: "sessions.state_json".to_owned(),
                        },
                    )?;
                    Ok((session.branch_name, state))
                })
                .collect()
        })
    }

    fn get_session_state(&self, name: &str) -> Result<SessionState> {
        self.list_session_states()?
            .remove(name)
            .context(BranchNotFoundSnafu {
                name: name.to_owned(),
            })
    }

    fn set_session_state(
        &self,
        _name: &str,
        _expected: Option<&SessionState>,
        _next: SessionState,
    ) -> Result<SessionState> {
        self.ensure_read_only()
    }

    fn rebase_session(&self, _name: &str, _patch: &SessionAnchorPatch) -> Result<String> {
        self.ensure_read_only()
    }

    fn handoff_session(
        &self,
        _name: &str,
        _patch: &SessionAnchorPatch,
        _prompt: &str,
    ) -> Result<String> {
        self.ensure_read_only()
    }
}

impl NodeStore for SqliteStore {
    fn root_id(&self) -> String {
        self.inner
            .read()
            .expect("store lock poisoned")
            .root_id()
            .to_owned()
    }

    fn append(&self, node: NewNode) -> Result<String> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let node = state.plan_append_node(node)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_node(&mut connection, &self.database_path, &node).await
        })?;
        state.insert_existing_node(node)
    }

    fn ancestry(&self, head_ref: &str) -> Result<Vec<Node>> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .ancestry(head_ref)
            .map(|nodes| nodes.into_iter().cloned().collect())
    }

    fn log(&self, base_ref: &str, head_ref: &str) -> Result<Vec<Node>> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .log(base_ref, head_ref)
            .map(|nodes| nodes.into_iter().cloned().collect())
    }

    fn get_node(&self, id: &str) -> Result<Node> {
        self.inner.read().expect("store lock poisoned").get_node(id)
    }

    fn list_children(&self, node_id: &str) -> Result<Vec<Node>> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .list_children(node_id)
    }
}

impl BranchStore for SqliteStore {
    fn fork(&self, name: &str, from_ref: &str) -> Result<String> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let plan = state.plan_fork(name, from_ref)?;
        let mut temp = state.clone();
        temp.apply_fork(name.to_owned(), plan.head_id.clone())?;
        let session_state = temp.get_session_state(name)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_branch_and_session_state(
                &mut connection,
                &self.database_path,
                name,
                &plan.head_id,
                &session_state,
            )
            .await
        })?;
        state.apply_fork(name.to_owned(), plan.head_id.clone())?;
        Ok(plan.head_id)
    }

    fn get_branch_head(&self, name: &str) -> Result<String> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_branch_head(name)
            .map(str::to_owned)
    }

    fn delete_branch(&self, name: &str) -> Result<()> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        temp.delete_branch(name)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            delete_branch_record(&mut connection, &self.database_path, name).await
        })?;
        state.delete_branch(name)
    }

    fn set_branch_head(&self, name: &str, expected_old_head: &str, new_head: &str) -> Result<()> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        temp.apply_set_branch_head(name.to_owned(), expected_old_head, new_head.to_owned())?;
        let updated = self.block_on(async {
            let mut connection = self.connect().await?;
            update_branch_head(
                &mut connection,
                &self.database_path,
                name,
                expected_old_head,
                new_head,
            )
            .await
        })?;
        ensure!(
            updated == 1,
            CorruptedStoreSnafu {
                path: self.database_path.clone(),
                message: format!("SQLite branch {name:?} did not match expected head"),
            }
        );
        state.apply_set_branch_head(name.to_owned(), expected_old_head, new_head.to_owned())
    }
}

impl SessionStore for SqliteStore {
    fn list_session_states(&self) -> Result<std::collections::HashMap<String, SessionState>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .list_session_states())
    }

    fn get_session_state(&self, name: &str) -> Result<SessionState> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_session_state(name)
    }

    fn set_session_state(
        &self,
        name: &str,
        expected: Option<&SessionState>,
        next: SessionState,
    ) -> Result<SessionState> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let updated = temp.set_session_state(name, expected, next)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_session_state(&mut connection, &self.database_path, name, &updated).await
        })?;
        state.set_session_state(name, expected, updated)
    }

    fn rebase_session(&self, name: &str, patch: &SessionAnchorPatch) -> Result<String> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let plan = state.plan_rebase_session(name, patch)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_session_nodes_and_branch_head(
                &mut connection,
                &self.database_path,
                &plan.branch,
                &plan.expected_old_head,
                &plan.new_head,
                &plan.nodes,
            )
            .await
        })?;
        for node in plan.nodes {
            state.insert_existing_node(node)?;
        }
        state.apply_set_branch_head(plan.branch, &plan.expected_old_head, plan.new_head.clone())?;
        Ok(plan.new_head)
    }

    fn handoff_session(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
        prompt: &str,
    ) -> Result<String> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let plan = state.plan_handoff_session(name, patch, prompt)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_session_nodes_and_branch_head(
                &mut connection,
                &self.database_path,
                &plan.branch,
                &plan.expected_old_head,
                &plan.new_head,
                std::slice::from_ref(&plan.node),
            )
            .await
        })?;
        state.insert_existing_node(plan.node)?;
        state.apply_set_branch_head(plan.branch, &plan.expected_old_head, plan.new_head.clone())?;
        Ok(plan.new_head)
    }
}

impl JobStore for SqliteStore {
    fn submit_job(&self, branch: &str, base: &str) -> Result<Job> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let created = temp.submit_job(branch, base)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_job(&mut connection, &self.database_path, &created).await
        })?;
        state.jobs = temp.jobs;
        Ok(created)
    }

    fn submit_job_with_id(&self, job_id: &str, branch: &str, base: &str) -> Result<Job> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let created = temp.submit_job_with_id(job_id, branch, base)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_job(&mut connection, &self.database_path, &created).await
        })?;
        state.jobs = temp.jobs;
        Ok(created)
    }

    fn get_job(&self, job_id: &str) -> Result<Job> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_job(job_id)
    }

    fn list_jobs(&self) -> Result<std::collections::HashMap<String, Job>> {
        Ok(self.inner.read().expect("store lock poisoned").list_jobs())
    }

    fn set_job_status(&self, job_id: &str, expected: JobStatus, next: JobStatus) -> Result<Job> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let updated = temp.set_job_status(job_id, expected, next)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_job(&mut connection, &self.database_path, &updated).await
        })?;
        state.jobs = temp.jobs;
        Ok(updated)
    }

    fn set_job_work_branch(
        &self,
        job_id: &str,
        expected_work_branch: &str,
        next_work_branch: &str,
    ) -> Result<Job> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let updated = temp.set_job_work_branch(job_id, expected_work_branch, next_work_branch)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_job(&mut connection, &self.database_path, &updated).await
        })?;
        state.jobs = temp.jobs;
        Ok(updated)
    }
}

impl MessageQueueStore for SqliteStore {
    fn enqueue_message(&self, queue: &str, payload: serde_json::Value) -> Result<MessageQueueItem> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let item = temp.enqueue_message(queue, payload);
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_message_queue_item(&mut connection, &self.database_path, &item).await
        })?;
        state.message_queues = temp.message_queues;
        Ok(item)
    }

    fn dequeue_message(&self, queue: &str) -> Result<Option<MessageQueueItem>> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let item = temp.dequeue_message(queue);
        let Some(item) = item else {
            return Ok(None);
        };
        self.block_on(async {
            let mut connection = self.connect().await?;
            delete_message_queue_item(&mut connection, &self.database_path, &item).await
        })?;
        state.message_queues = temp.message_queues;
        Ok(Some(item))
    }

    fn peek_message(&self, queue: &str) -> Result<Option<MessageQueueItem>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .peek_message(queue))
    }

    fn list_queue_messages(&self, queue: &str) -> Result<Vec<MessageQueueItem>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .list_queue_messages(queue))
    }

    fn list_message_queues(&self) -> Result<Vec<String>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .list_message_queues())
    }
}

impl PresetStore for SqliteStore {
    fn list_preset_records(&self) -> Result<std::collections::HashMap<String, PresetRecord>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .list_preset_records())
    }

    fn get_preset_record(&self, name: &str) -> Result<PresetRecord> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_preset_record(name)
    }

    fn set_preset(&self, name: &str, config: Preset) -> Result<PresetRecord> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let updated = temp.set_preset(name, config)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_preset(&mut connection, &self.database_path, &updated).await
        })?;
        state.presets = temp.presets;
        Ok(updated)
    }

    fn rollback_preset(&self, name: &str, target_version: u64) -> Result<PresetRecord> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let updated = temp.rollback_preset(name, target_version)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_preset(&mut connection, &self.database_path, &updated).await
        })?;
        state.presets = temp.presets;
        Ok(updated)
    }

    fn delete_preset(&self, name: &str) -> Result<()> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        temp.delete_preset(name)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            delete_preset_record(&mut connection, &self.database_path, name).await
        })?;
        state.presets = temp.presets;
        Ok(())
    }
}

impl SkillStore for SqliteStore {
    fn list_skills(&self, role: SessionRole) -> Result<Vec<SkillRecord>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .list_skills(role))
    }

    fn get_skill(&self, role: SessionRole, name: &str) -> Result<SkillRecord> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_skill(role, name)
    }

    fn add_skill(
        &self,
        role: SessionRole,
        name: &str,
        spec: SkillVersionSpec,
    ) -> Result<SkillRecord> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let created = temp.add_skill(role, name, spec)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_skill(&mut connection, &self.database_path, role, &created).await
        })?;
        state.skill_groups = temp.skill_groups;
        Ok(created)
    }

    fn update_skill(
        &self,
        role: SessionRole,
        name: &str,
        patch: &SkillUpdatePatch,
    ) -> Result<SkillRecord> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let updated = temp.update_skill(role, name, patch)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_skill(&mut connection, &self.database_path, role, &updated).await
        })?;
        state.skill_groups = temp.skill_groups;
        Ok(updated)
    }

    fn rollback_skill(
        &self,
        role: SessionRole,
        name: &str,
        target_version: u64,
    ) -> Result<SkillRecord> {
        self.ensure_writable()?;
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let updated = temp.rollback_skill(role, name, target_version)?;
        self.block_on(async {
            let mut connection = self.connect().await?;
            persist_skill(&mut connection, &self.database_path, role, &updated).await
        })?;
        state.skill_groups = temp.skill_groups;
        Ok(updated)
    }
}

impl ProcessShareableStore for SqliteStore {
    fn store_path(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AsyncSqliteConnection, FsStore, MessageQueueItem, NodeRow,
        SQLITE_INCOMPLETE_DATABASE_FILE_NAME, SQLITE_MIGRATION_DATABASE_FILE_NAME,
        SqliteGraphStore, SqliteMigration, SqliteStore, StoreAccess, persist_root_metadata,
    };
    use crate::{
        Anchor, BranchStore, JobStatus, JobStore, Kind, MergeParent, MessageQueueStore, NewNode,
        Node, NodeStore, PauseReason, Preset, PresetStore, Role, SessionAnchor, SessionAnchorPatch,
        SessionRole, SessionState, SessionStore, SkillStore, SkillUpdatePatch, SkillVersionSpec,
    };
    use diesel::sql_query;
    use diesel::sql_types::{Integer, Nullable, Text};
    use diesel_async::RunQueryDsl;
    use std::sync::Arc;

    #[derive(diesel::QueryableByName, Debug, PartialEq, Eq)]
    struct NodeRelationRow {
        #[diesel(sql_type = Text)]
        child_node_id: String,
        #[diesel(sql_type = Text)]
        parent_node_id: String,
        #[diesel(sql_type = Text)]
        kind: String,
        #[diesel(sql_type = Integer)]
        ordinal: i32,
    }

    fn session_anchor_node(parent: &str) -> NewNode {
        NewNode {
            parent: parent.to_owned(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(
                vec![],
                SessionAnchor {
                    role: SessionRole::Orchestrator,
                    provider_profile: None,
                    provider: Some("openai".to_owned()),
                    model: "gpt-5.4".to_owned(),
                    tools: vec![],
                    system_prompt: "system".to_owned(),
                    prompt: "prompt".to_owned(),
                    temperature: Some(0.1),
                    max_tokens: Some(64),
                    additional_params: None,
                    enable_coco_shim: false,
                    active_skill: None,
                },
            )),
        }
    }

    fn preset(model: &str) -> Preset {
        Preset {
            role: SessionRole::Orchestrator,
            provider_profile: "openai".to_owned(),
            model: model.to_owned(),
            tools: vec![],
            system_prompt: "system".to_owned(),
            prompt: "prompt".to_owned(),
            temperature: Some(0.1),
            max_tokens: Some(64),
            additional_params: None,
            enable_coco_shim: false,
        }
    }

    fn node_relation_rows(store: &SqliteStore, child_node_id: &str) -> Vec<NodeRelationRow> {
        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            sql_query(
                r#"
SELECT child_node_id, parent_node_id, kind, ordinal
FROM node_relations
WHERE child_node_id = ?
ORDER BY kind, ordinal, parent_node_id
"#,
            )
            .bind::<Text, _>(child_node_id)
            .load::<NodeRelationRow>(&mut connection)
            .await
            .unwrap()
        })
    }

    async fn insert_v1_node_row(
        connection: &mut AsyncSqliteConnection,
        store_path: &std::path::Path,
        node: Node,
    ) {
        let row = NodeRow::from_node(node, store_path).unwrap();
        sql_query(
            r#"
INSERT INTO nodes (id, parent_id, created_at, role, metadata_json, kind_json)
VALUES (?, ?, ?, ?, ?, ?)
"#,
        )
        .bind::<Text, _>(row.id)
        .bind::<Text, _>(row.parent_id)
        .bind::<Text, _>(row.created_at)
        .bind::<Text, _>(row.role)
        .bind::<Nullable<Text>, _>(row.metadata_json)
        .bind::<Text, _>(row.kind_json)
        .execute(connection)
        .await
        .unwrap();
    }

    #[test]
    fn open_creates_sqlite_database_and_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");

        let store = SqliteStore::open(&path).unwrap();

        assert!(store.database_path().is_file());
        assert_eq!(store.schema_version().unwrap(), 2);
    }

    #[test]
    fn cloned_sqlite_store_shares_database_instance() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");

        let store = SqliteStore::open(&path).unwrap();
        let cloned = store.clone();

        assert!(Arc::ptr_eq(
            store.database.shared_connection(),
            cloned.database.shared_connection()
        ));
    }

    #[test]
    fn reopened_sqlite_handles_share_database_instance() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");

        let store = SqliteStore::open(&path).unwrap();
        let read_only = SqliteStore::open_read_only(&path).unwrap();
        let graph = SqliteGraphStore::open_read_only(&path).unwrap();
        let lexical_read_only = SqliteStore::open_read_only(path.join(".")).unwrap();

        assert!(Arc::ptr_eq(
            store.database.shared_connection(),
            read_only.database.shared_connection()
        ));
        assert!(Arc::ptr_eq(
            store.database.shared_connection(),
            graph.database.shared_connection()
        ));
        assert!(Arc::ptr_eq(
            store.database.shared_connection(),
            lexical_read_only.database.shared_connection()
        ));
    }

    #[test]
    fn sqlite_store_serializes_concurrent_writes_on_shared_database() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let root_id = store.root_id();

        let handles = (0..8)
            .map(|index| {
                let store = store.clone();
                let root_id = root_id.clone();
                std::thread::spawn(move || {
                    store
                        .append(NewNode {
                            parent: root_id,
                            role: Role::User,
                            metadata: None,
                            kind: Kind::Text(format!("child-{index}")),
                        })
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();

        let mut node_ids = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        node_ids.sort();

        let mut children = store
            .list_children(&store.root_id())
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<Vec<_>>();
        children.sort();

        assert_eq!(children, node_ids);
        let reopened = SqliteStore::open_read_only(&path).unwrap();
        assert_eq!(
            reopened.list_children(&reopened.root_id()).unwrap().len(),
            8
        );
    }

    #[test]
    fn failed_schema_migration_rolls_back_ddl() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();
        let store = SqliteStore::new(&path, StoreAccess::ReadWrite).unwrap();
        let migration = SqliteMigration {
            version: 100,
            name: "failing-test-migration",
            sql: r#"
CREATE TABLE rollback_probe (
    id INTEGER PRIMARY KEY NOT NULL
);
THIS IS NOT SQL;
"#,
        };

        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            super::configure_writable_connection(&mut connection, &store.database_path)
                .await
                .unwrap();
            super::ensure_migration_table(&mut connection, &store.database_path)
                .await
                .unwrap();

            let err =
                super::apply_migration_if_needed(&mut connection, &store.database_path, &migration)
                    .await
                    .unwrap_err();

            assert!(err.to_string().contains("SQLite"));
            assert_eq!(
                super::table_count(&mut connection, &store.database_path, "rollback_probe")
                    .await
                    .unwrap(),
                0
            );
            assert!(
                !super::migration_applied(&mut connection, &store.database_path, migration.version)
                    .await
                    .unwrap()
            );
        });
    }

    #[test]
    fn open_read_only_accepts_current_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        SqliteStore::open(&path).unwrap();

        let store = SqliteStore::open_read_only(&path).unwrap();

        assert_eq!(store.schema_version().unwrap(), 2);
    }

    #[test]
    fn append_persists_node_relations() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let root_id = store.root_id();
        let primary_parent = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("primary parent".to_owned()),
            })
            .unwrap();
        let merge_parent = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("merge parent".to_owned()),
            })
            .unwrap();
        let shadow_parent = store
            .append(NewNode {
                parent: root_id,
                role: Role::User,
                metadata: None,
                kind: Kind::Text("shadow parent".to_owned()),
            })
            .unwrap();
        let child = store
            .append(NewNode {
                parent: primary_parent.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(
                    vec![
                        MergeParent::merge(merge_parent.clone()),
                        MergeParent::shadow(shadow_parent.clone()),
                    ],
                    SessionAnchor {
                        role: SessionRole::Orchestrator,
                        provider_profile: None,
                        provider: Some("openai".to_owned()),
                        model: "gpt-5.4".to_owned(),
                        tools: vec![],
                        system_prompt: "system".to_owned(),
                        prompt: "prompt".to_owned(),
                        temperature: Some(0.1),
                        max_tokens: Some(64),
                        additional_params: None,
                        enable_coco_shim: false,
                        active_skill: None,
                    },
                )),
            })
            .unwrap();

        let relations = node_relation_rows(&store, &child);

        assert_eq!(relations.len(), 3);
        assert!(relations.contains(&NodeRelationRow {
            child_node_id: child.clone(),
            parent_node_id: primary_parent,
            kind: "primary".to_owned(),
            ordinal: 0,
        }));
        assert!(relations.contains(&NodeRelationRow {
            child_node_id: child.clone(),
            parent_node_id: merge_parent,
            kind: "merge".to_owned(),
            ordinal: 0,
        }));
        assert!(relations.contains(&NodeRelationRow {
            child_node_id: child,
            parent_node_id: shadow_parent,
            kind: "shadow".to_owned(),
            ordinal: 1,
        }));
    }

    #[test]
    fn graph_store_reads_children_from_node_relations() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let writer = SqliteStore::open(&path).unwrap();
        let root_id = writer.root_id();
        let child_id = writer
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("graph child".to_owned()),
            })
            .unwrap();
        writer.fork("graph-child", &child_id).unwrap();
        drop(writer);

        let graph_store = SqliteGraphStore::open_read_only(&path).unwrap();

        assert_eq!(graph_store.root_id(), root_id);
        assert_eq!(
            graph_store.get_node(&child_id).unwrap().id,
            child_id.clone()
        );
        assert_eq!(
            graph_store.get_node(&child_id[..12]).unwrap().id,
            child_id.clone()
        );
        assert_eq!(
            graph_store.get_node("graph-child").unwrap().id,
            child_id.clone()
        );
        assert_eq!(graph_store.list_children(&root_id).unwrap()[0].id, child_id);
        assert_eq!(graph_store.ancestry(&child_id).unwrap().len(), 2);
        assert!(matches!(
            graph_store
                .append(NewNode {
                    parent: root_id,
                    role: Role::User,
                    metadata: None,
                    kind: Kind::Text("blocked".to_owned()),
                })
                .unwrap_err(),
            crate::StoreError::StoreReadOnly { .. }
        ));
    }

    #[test]
    fn schema_migration_backfills_node_relations_from_node_edges() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();
        let store = SqliteStore::new(&path, StoreAccess::ReadWrite).unwrap();
        let state = super::StoreState::new();
        let root = state.root_node().clone();
        let child = Node::new(
            root.id.clone(),
            Role::User,
            None,
            Kind::Text("legacy child".to_owned()),
            "1970-01-01T00:00:01Z".parse().unwrap(),
        );
        let child_id = child.id.clone();
        let root_id = root.id.clone();
        let merge_parent_a = Node::new(
            root_id.clone(),
            Role::LLM,
            None,
            Kind::Text("legacy merge parent a".to_owned()),
            "1970-01-01T00:00:02Z".parse().unwrap(),
        );
        let merge_parent_a_id = merge_parent_a.id.clone();
        let merge_parent_b = Node::new(
            root_id.clone(),
            Role::LLM,
            None,
            Kind::Text("legacy merge parent b".to_owned()),
            "1970-01-01T00:00:03Z".parse().unwrap(),
        );
        let merge_parent_b_id = merge_parent_b.id.clone();

        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            super::configure_writable_connection(&mut connection, &store.database_path)
                .await
                .unwrap();
            super::ensure_migration_table(&mut connection, &store.database_path)
                .await
                .unwrap();
            super::apply_migration_if_needed(
                &mut connection,
                &store.database_path,
                &super::SQLITE_MIGRATIONS[0],
            )
            .await
            .unwrap();
            persist_root_metadata(&mut connection, &store.database_path, &root_id)
                .await
                .unwrap();
            insert_v1_node_row(&mut connection, &store.database_path, root).await;
            insert_v1_node_row(&mut connection, &store.database_path, merge_parent_a).await;
            insert_v1_node_row(&mut connection, &store.database_path, merge_parent_b).await;
            insert_v1_node_row(&mut connection, &store.database_path, child).await;
            sql_query("INSERT INTO node_edges (parent_id, child_id, kind) VALUES (?, ?, ?)")
                .bind::<Text, _>(&root_id)
                .bind::<Text, _>(&child_id)
                .bind::<Text, _>("primary")
                .execute(&mut connection)
                .await
                .unwrap();
            sql_query("INSERT INTO node_edges (parent_id, child_id, kind) VALUES (?, ?, ?)")
                .bind::<Text, _>(&merge_parent_a_id)
                .bind::<Text, _>(&child_id)
                .bind::<Text, _>("merge")
                .execute(&mut connection)
                .await
                .unwrap();
            sql_query("INSERT INTO node_edges (parent_id, child_id, kind) VALUES (?, ?, ?)")
                .bind::<Text, _>(&merge_parent_b_id)
                .bind::<Text, _>(&child_id)
                .bind::<Text, _>("merge")
                .execute(&mut connection)
                .await
                .unwrap();
        });
        drop(store);

        let migrated = SqliteStore::open(&path).unwrap();

        assert_eq!(migrated.schema_version().unwrap(), 2);
        assert_eq!(
            node_relation_rows(&migrated, &child_id),
            vec![
                NodeRelationRow {
                    child_node_id: child_id.clone(),
                    parent_node_id: merge_parent_a_id,
                    kind: "merge".to_owned(),
                    ordinal: 0,
                },
                NodeRelationRow {
                    child_node_id: child_id.clone(),
                    parent_node_id: merge_parent_b_id,
                    kind: "merge".to_owned(),
                    ordinal: 1,
                },
                NodeRelationRow {
                    child_node_id: child_id,
                    parent_node_id: root_id,
                    kind: "primary".to_owned(),
                    ordinal: 0,
                },
            ]
        );
        migrated.block_on(async {
            let mut connection = migrated.connect().await.unwrap();
            assert_eq!(
                super::table_count(&mut connection, &migrated.database_path, "node_edges")
                    .await
                    .unwrap(),
                0
            );
        });
    }

    #[test]
    fn persistent_read_only_or_migrate_fs_upgrades_sqlite_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();
        let store = SqliteStore::new(&path, StoreAccess::ReadWrite).unwrap();
        let state = super::StoreState::new();
        let root = state.root_node().clone();
        let root_id = root.id.clone();

        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            super::configure_writable_connection(&mut connection, &store.database_path)
                .await
                .unwrap();
            super::ensure_migration_table(&mut connection, &store.database_path)
                .await
                .unwrap();
            super::apply_migration_if_needed(
                &mut connection,
                &store.database_path,
                &super::SQLITE_MIGRATIONS[0],
            )
            .await
            .unwrap();
            persist_root_metadata(&mut connection, &store.database_path, &root_id)
                .await
                .unwrap();
            insert_v1_node_row(&mut connection, &store.database_path, root).await;
        });
        drop(store);

        let crate::store::PersistentStore::Sqlite(migrated) =
            crate::store::PersistentStore::open_read_only_or_migrate_fs(&path).unwrap()
        else {
            panic!("expected SQLite store");
        };

        assert_eq!(migrated.schema_version().unwrap(), 2);
        assert_eq!(migrated.root_id(), root_id);
    }

    #[test]
    fn open_read_only_rejects_writes() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let writable = SqliteStore::open(&path).unwrap();
        let root_id = writable.root_id();
        drop(writable);

        let store = SqliteStore::open_read_only(&path).unwrap();
        let err = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("child".to_owned()),
            })
            .unwrap_err();

        assert!(matches!(err, crate::StoreError::StoreReadOnly { .. }));
        let reopened = SqliteStore::open_read_only(&path).unwrap();
        assert!(reopened.list_children(&root_id).unwrap().is_empty());
    }

    #[test]
    fn open_rejects_store_locked_by_another_owner() {
        use std::os::fd::AsRawFd;

        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();
        let lock_path = path.join("store.lock");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .unwrap();
        let result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(result, 0);

        let err = SqliteStore::open(&path).unwrap_err();

        assert!(matches!(err, crate::StoreError::StoreLocked { path: locked } if locked == path));
    }

    #[test]
    fn open_read_only_allows_store_locked_by_another_owner() {
        use std::os::fd::AsRawFd;

        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        SqliteStore::open(&path).unwrap();
        let lock_path = path.join("store.lock");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .unwrap();
        let result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(result, 0);

        let store = SqliteStore::open_read_only(&path).unwrap();

        assert_eq!(store.schema_version().unwrap(), 2);
    }

    #[test]
    fn open_read_only_rejects_missing_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();

        let err = SqliteStore::open_read_only(&path).unwrap_err();

        assert!(err.to_string().contains("SQLite"));
    }

    #[test]
    fn append_persists_node_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let root_id = store.root_id();
        let child_id = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("child".to_owned()),
            })
            .unwrap();
        assert_eq!(store.list_children(&root_id).unwrap()[0].id, child_id);

        let reopened = SqliteStore::open(&path).unwrap();
        let child = reopened.get_node(&child_id).unwrap();

        assert_eq!(child.parent, root_id);
        assert_eq!(reopened.list_children(&root_id).unwrap()[0].id, child_id);
    }

    #[test]
    fn reopened_store_supports_node_traversal() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let root_id = store.root_id();
        let first = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("first".to_owned()),
            })
            .unwrap();
        let second = store
            .append(NewNode {
                parent: first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("second".to_owned()),
            })
            .unwrap();

        let reopened = SqliteStore::open(&path).unwrap();

        let ancestry = reopened
            .ancestry(&second)
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<Vec<_>>();
        assert_eq!(
            ancestry,
            vec![second.clone(), first.clone(), root_id.clone()]
        );
        let log = reopened
            .log(&root_id, &second)
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<Vec<_>>();
        assert_eq!(log, vec![second.clone(), first, root_id]);
        assert_eq!(reopened.get_node(&second[..12]).unwrap().id, second);
    }

    #[test]
    fn branch_operations_persist_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let root_id = store.root_id();
        let first = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("first".to_owned()),
            })
            .unwrap();
        let second = store
            .append(NewNode {
                parent: first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("second".to_owned()),
            })
            .unwrap();

        assert_eq!(store.fork("main", &first).unwrap(), first);
        store.set_branch_head("main", &first, &second).unwrap();
        assert_eq!(store.get_branch_head("main").unwrap(), second);

        let reopened = SqliteStore::open(&path).unwrap();
        assert_eq!(reopened.get_branch_head("main").unwrap(), second);

        reopened.delete_branch("main").unwrap();
        let reopened = SqliteStore::open(&path).unwrap();
        assert!(reopened.get_branch_head("main").is_err());
    }

    #[test]
    fn fork_persistence_rolls_back_branch_when_session_insert_fails() {
        use diesel::sql_query;
        use diesel::sql_types::Text;
        use diesel_async::{RunQueryDsl, SimpleAsyncConnection};

        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let root_id = store.root_id();

        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            connection
                .batch_execute("DROP TABLE sessions")
                .await
                .unwrap();

            let err = super::persist_branch_and_session_state(
                &mut connection,
                &store.database_path,
                "main",
                &root_id,
                &SessionState::Active,
            )
            .await
            .unwrap_err();
            let count = sql_query("SELECT COUNT(*) AS count FROM branches WHERE name = ?")
                .bind::<Text, _>("main")
                .get_result::<super::TableCount>(&mut connection)
                .await
                .unwrap()
                .count;

            assert!(err.to_string().contains("SQLite"));
            assert_eq!(count, 0);
        });
    }

    #[test]
    fn session_operations_persist_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let root_id = store.root_id();
        let session = store.append(session_anchor_node(&root_id)).unwrap();
        store.fork("main", &session).unwrap();
        let text = store
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("text".to_owned()),
            })
            .unwrap();
        store.set_branch_head("main", &session, &text).unwrap();
        store
            .set_session_state(
                "main",
                Some(&SessionState::Active),
                SessionState::Paused {
                    target_branch: String::new(),
                    reason: PauseReason::Closed,
                },
            )
            .unwrap();

        let rebased = store
            .rebase_session(
                "main",
                &SessionAnchorPatch {
                    model: Some("gpt-5.5".to_owned()),
                    ..SessionAnchorPatch::default()
                },
            )
            .unwrap();
        let handoff = store
            .handoff_session("main", &SessionAnchorPatch::default(), "next prompt")
            .unwrap();

        let reopened = SqliteStore::open(&path).unwrap();

        assert_eq!(reopened.get_branch_head("main").unwrap(), handoff);
        assert_eq!(
            reopened.get_session_state("main").unwrap(),
            SessionState::Paused {
                target_branch: String::new(),
                reason: PauseReason::Closed,
            }
        );
        assert!(reopened.get_node(&rebased).is_ok());
        assert!(reopened.get_node(&handoff).is_ok());
    }

    #[test]
    fn job_operations_persist_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let root_id = store.root_id();
        let session = store.append(session_anchor_node(&root_id)).unwrap();
        store.fork("main", &session).unwrap();

        let job = store
            .submit_job_with_id("job-test", "main", &session)
            .unwrap();
        assert_eq!(job.status, JobStatus::Queued);
        let job = store
            .set_job_status("job-test", JobStatus::Queued, JobStatus::Running)
            .unwrap();
        assert_eq!(job.status, JobStatus::Running);

        let reopened = SqliteStore::open(&path).unwrap();
        let job = reopened.get_job("job-test").unwrap();

        assert_eq!(job.status, JobStatus::Running);
        assert_eq!(job.branch, "main");
        assert_eq!(job.base, session);
    }

    #[test]
    fn message_queue_operations_persist_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let first = store
            .enqueue_message("runner", serde_json::json!({"index": 1}))
            .unwrap();
        let second = store
            .enqueue_message("runner", serde_json::json!({"index": 2}))
            .unwrap();

        let reopened = SqliteStore::open(&path).unwrap();
        let messages = reopened.list_queue_messages("runner").unwrap();
        assert_eq!(messages[0].message_id, first.message_id);
        assert_eq!(messages[1].message_id, second.message_id);
        assert_eq!(
            reopened.peek_message("runner").unwrap().unwrap().payload["index"],
            1
        );

        let dequeued = reopened.dequeue_message("runner").unwrap().unwrap();
        assert_eq!(dequeued.message_id, first.message_id);
        let reopened = SqliteStore::open(&path).unwrap();
        let messages = reopened.list_queue_messages("runner").unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].message_id, second.message_id);
    }

    #[test]
    fn message_queue_preserves_insert_order_for_equal_timestamps() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let created_at = "2026-01-01T00:00:00Z".parse().unwrap();
        let first = MessageQueueItem {
            message_id: "z-first".to_owned(),
            queue: "runner".to_owned(),
            created_at,
            payload: serde_json::json!({"index": 1}),
        };
        let second = MessageQueueItem {
            message_id: "a-second".to_owned(),
            queue: "runner".to_owned(),
            created_at,
            payload: serde_json::json!({"index": 2}),
        };
        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            super::persist_message_queue_item(&mut connection, &store.database_path, &first)
                .await
                .unwrap();
            super::persist_message_queue_item(&mut connection, &store.database_path, &second)
                .await
                .unwrap();
        });

        let reopened = SqliteStore::open(&path).unwrap();
        let messages = reopened.list_queue_messages("runner").unwrap();

        assert_eq!(messages[0].message_id, first.message_id);
        assert_eq!(messages[1].message_id, second.message_id);
    }

    #[test]
    fn message_queue_sorts_by_parsed_timestamp() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let first = MessageQueueItem {
            message_id: "first".to_owned(),
            queue: "runner".to_owned(),
            created_at: "2026-01-01T00:00:00Z".parse().unwrap(),
            payload: serde_json::json!({"index": 1}),
        };
        let second = MessageQueueItem {
            message_id: "second".to_owned(),
            queue: "runner".to_owned(),
            created_at: "2026-01-01T00:00:00.001Z".parse().unwrap(),
            payload: serde_json::json!({"index": 2}),
        };
        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            super::persist_message_queue_item(&mut connection, &store.database_path, &first)
                .await
                .unwrap();
            super::persist_message_queue_item(&mut connection, &store.database_path, &second)
                .await
                .unwrap();
        });

        let reopened = SqliteStore::open(&path).unwrap();
        let messages = reopened.list_queue_messages("runner").unwrap();

        assert_eq!(messages[0].message_id, first.message_id);
        assert_eq!(messages[1].message_id, second.message_id);
    }

    #[test]
    fn preset_operations_persist_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();

        let first = store.set_preset("default", preset("gpt-5.4")).unwrap();
        assert_eq!(first.current_version, 1);
        let second = store.set_preset("default", preset("gpt-5.5")).unwrap();
        assert_eq!(second.current_version, 2);
        let rolled_back = store.rollback_preset("default", 1).unwrap();
        assert_eq!(rolled_back.current_version, 3);

        let reopened = SqliteStore::open(&path).unwrap();
        let record = reopened.get_preset_record("default").unwrap();

        assert_eq!(record.current_version, 3);
        assert_eq!(record.current_preset().unwrap().model, "gpt-5.4");
    }

    #[test]
    fn skill_operations_persist_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        assert!(
            store
                .get_skill(SessionRole::Orchestrator, "coco-orchestrator")
                .is_ok()
        );

        let created = store
            .add_skill(
                SessionRole::Runner,
                "custom-runner",
                SkillVersionSpec {
                    description: "custom".to_owned(),
                    body: "run".to_owned(),
                    scripts: vec![],
                    enable_coco_shim: false,
                },
            )
            .unwrap();
        assert_eq!(created.current_version, 1);
        let updated = store
            .update_skill(
                SessionRole::Runner,
                "custom-runner",
                &SkillUpdatePatch {
                    body: Some("run updated".to_owned()),
                    ..SkillUpdatePatch::default()
                },
            )
            .unwrap();
        assert_eq!(updated.current_version, 2);
        let rolled_back = store
            .rollback_skill(SessionRole::Runner, "custom-runner", 1)
            .unwrap();
        assert_eq!(rolled_back.current_version, 3);

        let reopened = SqliteStore::open(&path).unwrap();
        let record = reopened
            .get_skill(SessionRole::Runner, "custom-runner")
            .unwrap();

        assert_eq!(record.current_version, 3);
        assert_eq!(record.current().unwrap().body, "run");
    }

    #[test]
    fn open_or_migrate_fs_imports_legacy_store() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let legacy = FsStore::open(&path).unwrap();
        let root_id = legacy.root_id();
        let session = legacy.append(session_anchor_node(&root_id)).unwrap();
        legacy.fork("main", &session).unwrap();
        legacy.set_preset("default", preset("gpt-5.4")).unwrap();
        let queued = legacy
            .enqueue_message("runner", serde_json::json!({"source": "fs"}))
            .unwrap();
        drop(legacy);

        let migrated = SqliteStore::open_or_migrate_fs(&path).unwrap();

        assert!(migrated.database_path().is_file());
        assert_eq!(migrated.get_branch_head("main").unwrap(), session);
        assert_eq!(
            migrated
                .get_preset_record("default")
                .unwrap()
                .current_preset()
                .unwrap()
                .model,
            "gpt-5.4"
        );
        assert_eq!(
            migrated.peek_message("runner").unwrap().unwrap().message_id,
            queued.message_id
        );

        let reopened = SqliteStore::open_read_only_or_migrate_fs(&path).unwrap();
        assert_eq!(reopened.get_branch_head("main").unwrap(), session);
    }

    #[test]
    fn migrated_read_only_open_bypasses_legacy_lock() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let legacy = FsStore::open(&path).unwrap();
        let root_id = legacy.root_id();
        let session = legacy.append(session_anchor_node(&root_id)).unwrap();
        legacy.fork("main", &session).unwrap();
        drop(legacy);

        let migrated = SqliteStore::open_or_migrate_fs(&path).unwrap();

        let read_only = SqliteStore::open_read_only_or_migrate_fs(&path).unwrap();
        assert_eq!(migrated.get_branch_head("main").unwrap(), session);
        assert_eq!(read_only.get_branch_head("main").unwrap(), session);
    }

    #[test]
    fn migrated_writable_open_bypasses_legacy_replay() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let legacy = FsStore::open(&path).unwrap();
        let root_id = legacy.root_id();
        let session = legacy.append(session_anchor_node(&root_id)).unwrap();
        legacy.fork("main", &session).unwrap();
        drop(legacy);

        let migrated = SqliteStore::open_or_migrate_fs(&path).unwrap();
        drop(migrated);
        std::fs::write(path.join("meta.json"), "{").unwrap();

        let reopened = SqliteStore::open_or_migrate_fs(&path).unwrap();

        assert_eq!(reopened.get_branch_head("main").unwrap(), session);
    }

    #[test]
    fn migrated_store_does_not_persist_unmodified_builtin_skills() {
        use diesel::sql_query;
        use diesel_async::RunQueryDsl;

        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let legacy = FsStore::open(&path).unwrap();
        drop(legacy);

        let migrated = SqliteStore::open_or_migrate_fs(&path).unwrap();
        let default = crate::default_skill_groups()
            .for_role(SessionRole::Orchestrator)
            .get("coco-orchestrator")
            .unwrap()
            .current()
            .unwrap()
            .id
            .clone();
        let actual = migrated
            .get_skill(SessionRole::Orchestrator, "coco-orchestrator")
            .unwrap()
            .current()
            .unwrap()
            .id
            .clone();
        let count = migrated.block_on(async {
            let mut connection = migrated.connect().await.unwrap();
            sql_query("SELECT COUNT(*) AS count FROM skills")
                .get_result::<super::TableCount>(&mut connection)
                .await
                .unwrap()
                .count
        });

        assert_eq!(actual, default);
        assert_eq!(count, 0);
    }

    #[test]
    fn migrated_store_persists_modified_builtin_skills() {
        use diesel::sql_query;
        use diesel_async::RunQueryDsl;

        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let legacy = FsStore::open(&path).unwrap();
        legacy
            .update_skill(
                SessionRole::Orchestrator,
                "coco-orchestrator",
                &SkillUpdatePatch {
                    description: Some("custom builtin".to_owned()),
                    ..SkillUpdatePatch::default()
                },
            )
            .unwrap();
        drop(legacy);

        let migrated = SqliteStore::open_or_migrate_fs(&path).unwrap();
        let record = migrated
            .get_skill(SessionRole::Orchestrator, "coco-orchestrator")
            .unwrap();
        let count = migrated.block_on(async {
            let mut connection = migrated.connect().await.unwrap();
            sql_query("SELECT COUNT(*) AS count FROM skills")
                .get_result::<super::TableCount>(&mut connection)
                .await
                .unwrap()
                .count
        });

        assert_eq!(record.current().unwrap().description, "custom builtin");
        assert_eq!(count, 1);
    }

    #[test]
    fn migrated_store_preserves_rollback_history_for_builtin_skills() {
        use diesel::sql_query;
        use diesel_async::RunQueryDsl;

        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let legacy = FsStore::open(&path).unwrap();
        legacy
            .update_skill(
                SessionRole::Orchestrator,
                "coco-orchestrator",
                &SkillUpdatePatch {
                    description: Some("custom builtin".to_owned()),
                    ..SkillUpdatePatch::default()
                },
            )
            .unwrap();
        legacy
            .rollback_skill(SessionRole::Orchestrator, "coco-orchestrator", 1)
            .unwrap();
        drop(legacy);

        let migrated = SqliteStore::open_or_migrate_fs(&path).unwrap();
        let record = migrated
            .get_skill(SessionRole::Orchestrator, "coco-orchestrator")
            .unwrap();
        let count = migrated.block_on(async {
            let mut connection = migrated.connect().await.unwrap();
            sql_query("SELECT COUNT(*) AS count FROM skills")
                .get_result::<super::TableCount>(&mut connection)
                .await
                .unwrap()
                .count
        });

        assert_eq!(record.current_version, 3);
        assert_eq!(record.versions[&2].description, "custom builtin");
        assert_eq!(count, 1);
    }

    #[test]
    fn open_or_migrate_fs_retries_incomplete_sqlite_database() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let legacy = FsStore::open(&path).unwrap();
        let root_id = legacy.root_id();
        let session = legacy.append(session_anchor_node(&root_id)).unwrap();
        legacy.fork("main", &session).unwrap();
        drop(legacy);

        let incomplete = SqliteStore::open(&path).unwrap();
        assert!(incomplete.database_path().is_file());
        drop(incomplete);

        let migrated = SqliteStore::open_or_migrate_fs(&path).unwrap();

        assert!(path.join(SQLITE_INCOMPLETE_DATABASE_FILE_NAME).is_file());
        assert_eq!(migrated.get_branch_head("main").unwrap(), session);
    }

    #[test]
    fn open_or_migrate_fs_preserves_unreadable_completed_sqlite_database() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let legacy = FsStore::open(&path).unwrap();
        let root_id = legacy.root_id();
        legacy.fork("main", &root_id).unwrap();
        drop(legacy);
        SqliteStore::open_or_migrate_fs(&path).unwrap();
        std::fs::write(super::sqlite_database_path(&path), "not sqlite").unwrap();

        let err = SqliteStore::open_or_migrate_fs(&path).unwrap_err();

        assert!(err.to_string().contains("SQLite"));
        assert!(super::sqlite_database_path(&path).is_file());
        assert!(!path.join(SQLITE_INCOMPLETE_DATABASE_FILE_NAME).exists());
    }

    #[test]
    fn open_or_migrate_fs_removes_stale_temporary_database() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let legacy = FsStore::open(&path).unwrap();
        let root_id = legacy.root_id();
        let session = legacy.append(session_anchor_node(&root_id)).unwrap();
        legacy.fork("main", &session).unwrap();
        drop(legacy);
        std::fs::write(path.join(SQLITE_MIGRATION_DATABASE_FILE_NAME), "stale").unwrap();

        let migrated = SqliteStore::open_or_migrate_fs(&path).unwrap();

        assert!(!path.join(SQLITE_MIGRATION_DATABASE_FILE_NAME).exists());
        assert_eq!(migrated.get_branch_head("main").unwrap(), session);
    }
}
