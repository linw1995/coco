use std::collections::{HashMap, HashSet};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

use diesel::prelude::*;
use diesel::result::OptionalExtension;
use diesel::sql_types::{Nullable, Text};
use diesel::sqlite::SqliteConnection;
use diesel_async::pooled_connection::bb8::{
    Pool as AsyncSqlitePool, PooledConnection as AsyncSqlitePooledConnection,
};
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, RunQueryDsl, SimpleAsyncConnection, TransactionManager};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use serde_json::{Map, Value};
use snafu::{IntoError, prelude::*};
use tokio::runtime::Runtime;

use super::state::StoreState;
use super::{
    BranchStore, JobStore, MessageQueueStore, NodeStore, PresetStore, ProcessShareableStore,
    SessionStore, SkillStore,
};
use crate::StoreResult as Result;
use crate::error::{
    AcquireSqliteConnectionSnafu, AmbiguousNodePrefixSnafu, BranchNotFoundSnafu,
    CorruptedStoreSnafu, CreateSqlitePoolSnafu, LegacyJsonStoreSnafu, NotFoundSnafu,
    ParentNotFoundSnafu, ParseSqliteStoreValueSnafu, QuerySqliteStoreSnafu, RefsNotConnectedSnafu,
    StartSqliteRuntimeSnafu, StoreError, StorePathIsNotDirectorySnafu, StoreReadOnlySnafu,
    WriteStoreDirectorySnafu,
};
use crate::schema::{
    branches, jobs, message_queue_items, node_metadata, node_relations, nodes, presets, sessions,
    skills, store_meta,
};
use crate::{
    AnchorPayload, Job, JobStatus, Kind, MergeParent, MessageQueueItem, NewNode, Node,
    NodeMetadata, Preset, PresetRecord, Role, SessionAnchorPatch, SessionRole, SessionState,
    SkillInvocationMode, SkillRecord, SkillUpdatePatch, SkillVersionSpec,
};

const SQLITE_DATABASE_FILE_NAME: &str = "store.sqlite3";
const SQLITE_SCHEMA_VERSION: i32 = 6;
const DIESEL_MIGRATION_TABLE_NAME: &str = "__diesel_schema_migrations";
const LEGACY_MIGRATION_TABLE_NAME: &str = "store_schema_migrations";
const FS_MIGRATION_COMPLETE_META_KEY: &str = "fs_migration_complete";
const LEGACY_JSON_STORE_MARKERS: &[&str] = &["meta.json", "nodes.jsonl"];
const STORE_MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");
const SQLITE_POOL_MAX_SIZE: u32 = 4;

diesel::table! {
    __diesel_schema_migrations (version) {
        version -> Text,
        run_on -> Text,
    }
}

diesel::table! {
    store_schema_migrations (version) {
        version -> Integer,
        name -> Text,
        applied_at -> Text,
    }
}

diesel::table! {
    sqlite_master (name) {
        #[sql_name = "type"]
        object_type -> Text,
        name -> Text,
    }
}

diesel::table! {
    #[sql_name = "node_edges"]
    legacy_node_edges (parent_id, child_id, kind) {
        parent_id -> Text,
        child_id -> Text,
        kind -> Text,
    }
}

diesel::table! {
    #[sql_name = "message_queue_items"]
    message_queue_items_with_rowid (queue, message_id) {
        rowid -> BigInt,
        queue -> Text,
        message_id -> Text,
        created_at -> Text,
        payload_json -> Text,
    }
}

static SQLITE_RUNTIME: OnceLock<Runtime> = OnceLock::new();
static SQLITE_RUNTIME_INIT: Mutex<()> = Mutex::new(());
static SQLITE_DATABASES: OnceLock<Mutex<HashMap<PathBuf, Weak<SqliteDatabaseInner>>>> =
    OnceLock::new();

type AsyncSqliteConnection = SyncConnectionWrapper<SqliteConnection>;

type AsyncSqliteConnectionGuard<'a> = AsyncSqlitePooledConnection<'a, AsyncSqliteConnection>;
type SqliteGraphConnectionFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>>;

enum SqliteTransactionError {
    Query(diesel::result::Error),
    Operation(StoreError),
}

impl From<diesel::result::Error> for SqliteTransactionError {
    fn from(source: diesel::result::Error) -> Self {
        Self::Query(source)
    }
}

impl SqliteTransactionError {
    fn into_store_error(self, path: &Path) -> StoreError {
        match self {
            Self::Query(source) => QuerySqliteStoreSnafu {
                path: path.to_owned(),
            }
            .into_error(source),
            Self::Operation(error) => error,
        }
    }
}

#[derive(Clone)]
pub struct SqliteDatabase {
    inner: Arc<SqliteDatabaseInner>,
}

struct SqliteDatabaseInner {
    database_path: PathBuf,
    runtime: &'static Runtime,
    pool: AsyncSqlitePool<AsyncSqliteConnection>,
    ensure_wal: Arc<AtomicBool>,
    initialization: Mutex<()>,
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

#[derive(Clone)]
pub struct SqliteGraphStore {
    dir: PathBuf,
    database_path: PathBuf,
    database: SqliteDatabase,
    root_id: String,
    read_transaction: Arc<Mutex<Option<AsyncSqliteConnectionGuard<'static>>>>,
}

impl std::fmt::Debug for SqliteGraphStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteGraphStore")
            .field("dir", &self.dir)
            .field("database_path", &self.database_path)
            .field("root_id", &self.root_id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreAccess {
    ReadWrite,
    ReadOnly,
}

#[derive(Queryable, QueryableByName)]
struct NodeRow {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Text)]
    parent_id: String,
    #[diesel(sql_type = Text)]
    created_at: String,
    #[diesel(sql_type = Text)]
    role: String,
    #[diesel(sql_type = Text)]
    kind: String,
    #[diesel(sql_type = Nullable<Text>)]
    anchor_kind: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    anchor_session_role: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    anchor_provider_profile: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    anchor_provider: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    anchor_model: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    anchor_prompt: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    anchor_skill_name: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    anchor_skill_invocation_mode: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    metadata_json: Option<String>,
    #[diesel(sql_type = Text)]
    kind_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct NodeMetadataRow {
    node_id: String,
    ordinal: i32,
    execution_id: Option<String>,
    call_id: Option<String>,
}

#[derive(Queryable, QueryableByName)]
struct BranchRow {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    head_id: String,
}

#[derive(Queryable, QueryableByName)]
struct SessionRow {
    #[diesel(sql_type = Text)]
    branch_name: String,
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = Nullable<Text>)]
    target_branch: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    base_head_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pause_reason: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    merged_anchor_id: Option<String>,
    #[diesel(sql_type = Text)]
    state_json: String,
}

#[derive(Queryable)]
struct JobRow {
    job_id: String,
    created_at: String,
    finished_at: Option<String>,
    branch: String,
    work_branch: String,
    base: String,
    status: String,
    payload_json: String,
}

macro_rules! node_row_columns {
    () => {
        (
            nodes::id,
            nodes::parent_id,
            nodes::created_at,
            nodes::role,
            nodes::kind,
            nodes::anchor_kind,
            nodes::anchor_session_role,
            nodes::anchor_provider_profile,
            nodes::anchor_provider,
            nodes::anchor_model,
            nodes::anchor_prompt,
            nodes::anchor_skill_name,
            nodes::anchor_skill_invocation_mode,
            nodes::metadata_json,
            nodes::kind_json,
        )
    };
}

#[derive(Queryable)]
struct MessageQueueItemRow {
    row_id: i64,
    item_json: String,
}

#[derive(Queryable, QueryableByName)]
struct SkillRow {
    #[diesel(sql_type = Text)]
    role: String,
    #[diesel(sql_type = Text)]
    record_json: String,
}

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
        Self::open(sqlite_database_path(path.as_ref()), false)
    }

    pub fn open_writable_store_path(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(sqlite_database_path(path.as_ref()), true)
    }

    pub fn open_unshared_file_path(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_uncached(path.as_ref().to_owned(), true)
    }

    fn open_writable_file_path(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(path.as_ref().to_owned(), true)
    }

    fn open(database_path: PathBuf, ensure_wal: bool) -> Result<Self> {
        let database_path = sqlite_database_registry_path(&database_path)?;
        let databases = SQLITE_DATABASES.get_or_init(|| Mutex::new(HashMap::new()));
        let mut databases = databases
            .lock()
            .expect("SQLite database registry lock poisoned");
        if let Some(inner) = databases
            .get(&database_path)
            .and_then(std::sync::Weak::upgrade)
        {
            let database = Self { inner };
            drop(databases);
            if ensure_wal {
                database.request_wal_journal_mode()?;
            }
            return Ok(database);
        }

        let runtime = sqlite_runtime()?;
        let ensure_wal_flag = Arc::new(AtomicBool::new(ensure_wal));
        let pool = build_sqlite_pool(runtime, &database_path, ensure_wal_flag.clone())?;
        let inner = Arc::new(SqliteDatabaseInner {
            database_path: database_path.clone(),
            runtime,
            pool,
            ensure_wal: ensure_wal_flag,
            initialization: Mutex::new(()),
        });
        databases.insert(database_path, Arc::downgrade(&inner));
        let database = Self { inner };
        if ensure_wal {
            database.request_wal_journal_mode()?;
        }
        Ok(database)
    }

    fn open_uncached(database_path: PathBuf, ensure_wal: bool) -> Result<Self> {
        let database_path = sqlite_database_registry_path(&database_path)?;
        let runtime = sqlite_runtime()?;
        let ensure_wal_flag = Arc::new(AtomicBool::new(ensure_wal));
        let pool = build_sqlite_pool(runtime, &database_path, ensure_wal_flag.clone())?;
        let database = Self {
            inner: Arc::new(SqliteDatabaseInner {
                database_path: database_path.clone(),
                runtime,
                pool,
                ensure_wal: ensure_wal_flag,
                initialization: Mutex::new(()),
            }),
        };
        if ensure_wal {
            database.request_wal_journal_mode()?;
        }
        Ok(database)
    }

    async fn connection(&self) -> Result<AsyncSqliteConnectionGuard<'_>> {
        self.inner
            .pool
            .get()
            .await
            .context(AcquireSqliteConnectionSnafu {
                path: self.inner.database_path.clone(),
            })
    }

    pub fn with_sync_connection<T, E, F, P, M>(
        &self,
        operation: F,
        map_pool_error: P,
        map_connection_error: M,
    ) -> std::result::Result<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(&mut SqliteConnection) -> std::result::Result<T, E> + Send + 'static,
        P: FnOnce(StoreError) -> E + Send,
        M: FnOnce(diesel::result::Error) -> E + Send,
    {
        let result = self.block_on(async {
            let mut connection = match self.connection().await {
                Ok(connection) => connection,
                Err(error) => return Err(map_pool_error(error)),
            };
            match connection
                .spawn_blocking(move |connection| Ok(operation(connection)))
                .await
            {
                Ok(result) => Ok(result),
                Err(error) => Err(map_connection_error(error)),
            }
        });
        match result {
            Ok(result) => result,
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    fn shared_pool(&self) -> &AsyncSqlitePool<AsyncSqliteConnection> {
        &self.inner.pool
    }

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        block_on_sqlite_runtime_with(self.inner.runtime, future)
    }

    fn request_wal_journal_mode(&self) -> Result<()> {
        self.inner.ensure_wal.store(true, Ordering::SeqCst);
        self.block_on(async {
            let mut connection = self.connection().await?;
            ensure_wal_journal_mode(&mut connection, &self.inner.database_path).await
        })
    }

    fn with_initialization_lock<T, F>(&self, operation: F) -> Result<T>
    where
        F: FnOnce() -> Result<T>,
    {
        let _guard = self
            .inner
            .initialization
            .lock()
            .expect("SQLite database initialization lock poisoned");
        operation()
    }
}

impl SqliteStore {
    pub fn open_read_only_or_upgrade_schema(path: impl AsRef<Path>) -> Result<Self> {
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
        reject_incomplete_legacy_json_store(path)?;
        let store = Self::new(path, StoreAccess::ReadWrite)?;
        store.database.with_initialization_lock(|| {
            store.run_migrations()?;
            store.load_or_initialize_state()
        })?;
        Ok(store)
    }

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        ensure_existing_store_directory(path)?;
        reject_incomplete_legacy_json_store(path)?;
        ensure_existing_database_file(&sqlite_database_path(path))?;
        let store = Self::new(path, StoreAccess::ReadOnly)?;
        store.database.with_initialization_lock(|| {
            store.ensure_current_schema()?;
            store.load_state()
        })?;
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
            existing_schema_version(&mut connection, &self.database_path).await
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
        let database = match access {
            StoreAccess::ReadWrite => SqliteDatabase::open_writable_file_path(&database_path)?,
            StoreAccess::ReadOnly => SqliteDatabase::open(database_path.clone(), false)?,
        };
        let lock_file = if access == StoreAccess::ReadWrite {
            Some(super::lock::open_store_lock(path)?)
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
            bootstrap_diesel_migrations_from_legacy_table(&mut connection, &self.database_path)
                .await?;
            reject_newer_schema_version(&mut connection, &self.database_path).await?;
            run_embedded_migrations(&mut connection, &self.database_path).await?;
            Ok(())
        })
    }

    fn ensure_current_schema(&self) -> Result<()> {
        self.block_on(async {
            let mut connection = self.connect().await?;
            let version = existing_schema_version(&mut connection, &self.database_path).await?;
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
        let (version, has_diesel_migration_table) =
            store.database.with_initialization_lock(|| {
                store.block_on(async {
                    let mut connection = store.connect().await?;
                    let has_diesel_migration_table = table_count(
                        &mut connection,
                        &store.database_path,
                        DIESEL_MIGRATION_TABLE_NAME,
                    )
                    .await?
                        == 1;
                    let version = existing_schema_version_for_upgrade_check(
                        &mut connection,
                        &store.database_path,
                    )
                    .await?;
                    Ok((version, has_diesel_migration_table))
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
        if !has_diesel_migration_table {
            return Ok(true);
        }
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
                connection
                    .immediate_transaction::<(), SqliteTransactionError, _>(async |connection| {
                        persist_root_metadata(connection, &self.database_path, state.root_id())
                            .await
                            .map_err(SqliteTransactionError::Operation)?;
                        persist_node_without_transaction(
                            connection,
                            &self.database_path,
                            state.root_node(),
                        )
                        .await
                        .map_err(SqliteTransactionError::Operation)
                    })
                    .await
                    .map_err(|error| error.into_store_error(&self.database_path))?;
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

    async fn connect(&self) -> Result<AsyncSqliteConnectionGuard<'_>> {
        self.database.connection().await
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
        ensure_existing_database_file(&sqlite_database_path(path))?;
        let store = Self::new(path)?;
        let root_id = store.database.with_initialization_lock(|| {
            store.ensure_current_schema()?;
            store.block_on(async {
                let mut connection = store.connect().await?;
                load_root_id(&mut connection, &store.database_path).await
            })
        })?;
        Ok(Self { root_id, ..store })
    }

    pub fn store_path(&self) -> &Path {
        &self.dir
    }

    fn new(path: &Path) -> Result<Self> {
        let database_path = sqlite_database_path(path);
        let database = SqliteDatabase::open(database_path.clone(), false)?;
        Ok(Self {
            dir: path.to_owned(),
            database_path,
            database,
            root_id: String::new(),
            read_transaction: Arc::new(Mutex::new(None)),
        })
    }

    fn ensure_current_schema(&self) -> Result<()> {
        self.block_on(async {
            let mut connection = self.connect().await?;
            let version = existing_schema_version(&mut connection, &self.database_path).await?;
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
        self.database.connection().await
    }

    fn with_connection<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send,
        F: for<'a> FnOnce(&'a mut AsyncSqliteConnection) -> SqliteGraphConnectionFuture<'a, T>
            + Send,
    {
        let mut read_transaction = self
            .read_transaction
            .lock()
            .expect("graph store transaction lock poisoned");
        if let Some(connection) = read_transaction.as_mut() {
            return self.block_on(operation(&mut *connection));
        }
        drop(read_transaction);

        self.block_on(async {
            let mut connection = self.connect().await?;
            operation(&mut connection).await
        })
    }

    pub fn begin_read_transaction(&self) -> Result<()> {
        ensure!(
            self.read_transaction
                .lock()
                .expect("graph store transaction lock poisoned")
                .is_none(),
            CorruptedStoreSnafu {
                path: self.database_path.clone(),
                message: "SQLite graph read transaction already active".to_owned(),
            }
        );

        self.block_on(async {
            let mut connection = self.database.inner.pool.get_owned().await.context(
                AcquireSqliteConnectionSnafu {
                    path: self.database_path.clone(),
                },
            )?;
            begin_deferred_transaction(&mut connection, &self.database_path).await?;
            let mut connection = Some(connection);
            let transaction_already_active = {
                let mut read_transaction = self
                    .read_transaction
                    .lock()
                    .expect("graph store transaction lock poisoned");
                if read_transaction.is_some() {
                    true
                } else {
                    *read_transaction = connection.take();
                    false
                }
            };
            if transaction_already_active {
                let mut connection = connection.expect("pending graph read connection is missing");
                rollback_deferred_transaction(&mut connection, &self.database_path).await?;
                return CorruptedStoreSnafu {
                    path: self.database_path.clone(),
                    message: "SQLite graph read transaction already active".to_owned(),
                }
                .fail();
            }
            Ok(())
        })
    }

    pub fn commit_read_transaction(&self) -> Result<()> {
        let mut connection = self
            .read_transaction
            .lock()
            .expect("graph store transaction lock poisoned")
            .take()
            .context(CorruptedStoreSnafu {
                path: self.database_path.clone(),
                message: "SQLite graph read transaction is not active".to_owned(),
            })?;

        self.block_on(async {
            commit_deferred_transaction(&mut connection, &self.database_path).await
        })
    }

    pub fn rollback_read_transaction(&self) -> Result<()> {
        let Some(mut connection) = self
            .read_transaction
            .lock()
            .expect("graph store transaction lock poisoned")
            .take()
        else {
            return Ok(());
        };

        self.block_on(async {
            rollback_deferred_transaction(&mut connection, &self.database_path).await
        })
    }

    fn ensure_read_only<T>(&self) -> Result<T> {
        StoreReadOnlySnafu {
            path: self.dir.clone(),
        }
        .fail()
    }

    fn get_node_by_exact_id(&self, id: &str) -> Result<Node> {
        let id = id.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move {
                let row = nodes::table
                    .filter(nodes::id.eq(&id))
                    .select(node_row_columns!())
                    .get_result::<NodeRow>(connection)
                    .await
                    .optional()
                    .context(QuerySqliteStoreSnafu { path: path.clone() })?
                    .context(NotFoundSnafu { id })?;
                let node_id = row.id.clone();
                let metadata_rows = load_node_metadata_rows_for_ids(
                    connection,
                    &path,
                    Some(std::slice::from_ref(&node_id)),
                )
                .await?;
                row.into_node(&path, node_metadata_slice(&metadata_rows, &node_id))
            })
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
        let name = name.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move {
                branches::table
                    .filter(branches::name.eq(name))
                    .select(branches::head_id)
                    .get_result::<String>(connection)
                    .await
                    .optional()
                    .context(QuerySqliteStoreSnafu { path })
            })
        })
    }

    fn node_exists(&self, id: &str) -> Result<bool> {
        let id = id.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move {
                let count = nodes::table
                    .filter(nodes::id.eq(id))
                    .count()
                    .get_result::<i64>(connection)
                    .await
                    .context(QuerySqliteStoreSnafu { path })?;
                Ok(count > 0)
            })
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
        let prefix = prefix.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move {
                nodes::table
                    .filter(nodes::id.like(format!("{prefix}%")))
                    .select(nodes::id)
                    .order(nodes::id)
                    .load::<String>(connection)
                    .await
                    .context(QuerySqliteStoreSnafu { path })
            })
        })
    }
}

fn sqlite_database_path(path: &Path) -> PathBuf {
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

fn reject_incomplete_legacy_json_store(path: &Path) -> Result<()> {
    let has_legacy_marker = LEGACY_JSON_STORE_MARKERS
        .iter()
        .any(|file_name| path.join(file_name).exists());
    if !has_legacy_marker {
        return Ok(());
    }

    let database_path = sqlite_database_path(path);
    if database_path.is_file() && fs_migration_complete_marker_exists(path)? {
        return Ok(());
    }

    LegacyJsonStoreSnafu {
        path: path.to_owned(),
    }
    .fail()
}

fn fs_migration_complete_marker_exists(path: &Path) -> Result<bool> {
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

fn build_sqlite_pool(
    runtime: &'static Runtime,
    database_path: &Path,
    ensure_wal: Arc<AtomicBool>,
) -> Result<AsyncSqlitePool<AsyncSqliteConnection>> {
    let manager = AsyncDieselConnectionManager::<AsyncSqliteConnection>::new_with_config(
        database_path.to_string_lossy().into_owned(),
        sqlite_pool_manager_config(database_path.to_owned(), ensure_wal),
    );
    block_on_sqlite_runtime_with(runtime, async {
        AsyncSqlitePool::builder()
            .max_size(SQLITE_POOL_MAX_SIZE)
            .build(manager)
            .await
            .context(CreateSqlitePoolSnafu {
                path: database_path.to_owned(),
            })
    })
}

fn sqlite_pool_manager_config(
    database_path: PathBuf,
    ensure_wal: Arc<AtomicBool>,
) -> ManagerConfig<AsyncSqliteConnection> {
    let mut config = ManagerConfig::default();
    config.custom_setup = Box::new(move |url| {
        let url = url.to_owned();
        let database_path = database_path.clone();
        let ensure_wal = ensure_wal.clone();
        Box::pin(async move {
            let mut connection = AsyncSqliteConnection::establish(&url).await?;
            configure_connection(&mut connection, &database_path)
                .await
                .map_err(sqlite_connection_setup_error)?;
            if ensure_wal.as_ref().load(Ordering::SeqCst) {
                ensure_wal_journal_mode(&mut connection, &database_path)
                    .await
                    .map_err(sqlite_connection_setup_error)?;
            }
            Ok(connection)
        })
    });
    config
}

fn sqlite_connection_setup_error(error: crate::StoreError) -> diesel::ConnectionError {
    diesel::ConnectionError::BadConnection(error.to_string())
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
    ensure_wal_journal_mode(connection, path).await
}

async fn ensure_wal_journal_mode(
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
CREATE TABLE IF NOT EXISTS __diesel_schema_migrations (
    version VARCHAR(50) PRIMARY KEY NOT NULL,
    run_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"#,
        )
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

async fn table_count(
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

async fn current_schema_version(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<Option<i32>> {
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

async fn existing_schema_version_for_upgrade_check(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<i32> {
    existing_schema_version(connection, path).await
}

async fn existing_schema_version(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<i32> {
    if table_count(connection, path, DIESEL_MIGRATION_TABLE_NAME).await? == 1 {
        if let Some(version) = current_schema_version(connection, path).await? {
            return Ok(version);
        }
        if table_count(connection, path, LEGACY_MIGRATION_TABLE_NAME).await? == 0 {
            return CorruptedStoreSnafu {
                path: path.to_owned(),
                message: "missing SQLite schema version".to_owned(),
            }
            .fail();
        }
    }

    if table_count(connection, path, LEGACY_MIGRATION_TABLE_NAME).await? == 1 {
        return current_legacy_schema_version(connection, path)
            .await?
            .context(CorruptedStoreSnafu {
                path: path.to_owned(),
                message: "missing legacy SQLite schema version".to_owned(),
            });
    }

    CorruptedStoreSnafu {
        path: path.to_owned(),
        message: "missing SQLite schema migration table".to_owned(),
    }
    .fail()
}

async fn current_legacy_schema_version(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<Option<i32>> {
    store_schema_migrations::table
        .select(diesel::dsl::max(store_schema_migrations::version))
        .get_result::<Option<i32>>(connection)
        .await
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

async fn run_embedded_migrations(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    let path = path.to_owned();
    let result = connection
        .spawn_blocking(move |connection| {
            Ok(connection
                .run_pending_migrations(STORE_MIGRATIONS)
                .map(|_| ()))
        })
        .await
        .context(QuerySqliteStoreSnafu { path: path.clone() })?;
    result.map_err(|source| StoreError::MigrateSqliteStore { path, source })
}

async fn bootstrap_diesel_migrations_from_legacy_table(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    if table_count(connection, path, LEGACY_MIGRATION_TABLE_NAME).await? == 0 {
        return Ok(());
    }

    let rows = store_schema_migrations::table
        .select((
            store_schema_migrations::version,
            store_schema_migrations::applied_at,
        ))
        .load::<(i32, String)>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    for (version, applied_at) in rows {
        diesel::insert_into(__diesel_schema_migrations::table)
            .values((
                __diesel_schema_migrations::version.eq(format!("{version:014}")),
                __diesel_schema_migrations::run_on.eq(applied_at),
            ))
            .on_conflict(__diesel_schema_migrations::version)
            .do_nothing()
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    Ok(())
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

async fn node_count(connection: &mut AsyncSqliteConnection, path: &Path) -> Result<i64> {
    nodes::table
        .select(diesel::dsl::count_star())
        .first(connection)
        .await
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
    diesel::insert_into(store_meta::table)
        .values((
            store_meta::key.eq("root_id"),
            store_meta::value_json.eq(root_json),
        ))
        .on_conflict(store_meta::key)
        .do_update()
        .set(store_meta::value_json.eq(diesel::upsert::excluded(store_meta::value_json)))
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
    let Some(value) = store_meta::table
        .filter(store_meta::key.eq(key))
        .select(store_meta::value_json)
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

async fn load_root_id(connection: &mut AsyncSqliteConnection, path: &Path) -> Result<String> {
    let Some(value) = store_meta::table
        .filter(store_meta::key.eq("root_id"))
        .select(store_meta::value_json)
        .first::<String>(connection)
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
    serde_json::from_str(&value).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "store_meta.root_id".to_owned(),
    })
}

async fn load_state(connection: &mut AsyncSqliteConnection, path: &Path) -> Result<StoreState> {
    connection
        .transaction::<StoreState, SqliteTransactionError, _>(async |connection| {
            load_state_without_transaction(connection, path)
                .await
                .map_err(SqliteTransactionError::Operation)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn load_state_without_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<StoreState> {
    let root_id = load_root_id(connection, path).await?;
    let mut rows = load_node_rows(connection, path).await?;
    let metadata_rows = load_node_metadata_rows(connection, path).await?;
    let root_index =
        rows.iter()
            .position(|row| row.id == root_id)
            .context(CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("root node {root_id:?} is missing"),
            })?;
    let root = rows
        .remove(root_index)
        .into_node(path, node_metadata_slice(&metadata_rows, &root_id))?;
    ensure!(
        root.is_root(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "root node must not have a parent".to_owned(),
        }
    );

    let mut state = StoreState::from_root(root);
    insert_node_rows(&mut state, rows, path, &metadata_rows)?;
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
    nodes::table
        .select((
            nodes::id,
            nodes::parent_id,
            nodes::created_at,
            nodes::role,
            nodes::kind,
            nodes::anchor_kind,
            nodes::anchor_session_role,
            nodes::anchor_provider_profile,
            nodes::anchor_provider,
            nodes::anchor_model,
            nodes::anchor_prompt,
            nodes::anchor_skill_name,
            nodes::anchor_skill_invocation_mode,
            nodes::metadata_json,
            nodes::kind_json,
        ))
        .order((nodes::created_at, nodes::id))
        .load::<NodeRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

async fn load_node_metadata_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<HashMap<String, Vec<NodeMetadataRow>>> {
    load_node_metadata_rows_for_ids(connection, path, None).await
}

async fn load_node_metadata_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, Vec<NodeMetadataRow>>> {
    let mut query = node_metadata::table
        .select((
            node_metadata::node_id,
            node_metadata::ordinal,
            node_metadata::execution_id,
            node_metadata::call_id,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_metadata::node_id.eq_any(node_ids));
    }
    let rows = query
        .order((node_metadata::node_id, node_metadata::ordinal))
        .load::<NodeMetadataRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(group_node_metadata_rows(rows))
}

fn group_node_metadata_rows(rows: Vec<NodeMetadataRow>) -> HashMap<String, Vec<NodeMetadataRow>> {
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.node_id.clone())
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

fn node_metadata_slice<'a>(
    rows: &'a HashMap<String, Vec<NodeMetadataRow>>,
    node_id: &str,
) -> &'a [NodeMetadataRow] {
    rows.get(node_id).map(Vec::as_slice).unwrap_or_default()
}

async fn node_rows_into_nodes(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    rows: Vec<NodeRow>,
) -> Result<Vec<Node>> {
    let node_ids = rows.iter().map(|row| row.id.clone()).collect::<Vec<_>>();
    let metadata_rows = load_node_metadata_rows_for_ids(connection, path, Some(&node_ids)).await?;
    rows.into_iter()
        .map(|row| {
            let node_id = row.id.clone();
            row.into_node(path, node_metadata_slice(&metadata_rows, &node_id))
        })
        .collect()
}

fn insert_node_rows(
    state: &mut StoreState,
    mut rows: Vec<NodeRow>,
    path: &Path,
    metadata_rows: &HashMap<String, Vec<NodeMetadataRow>>,
) -> Result<()> {
    while !rows.is_empty() {
        let initial_len = rows.len();
        let mut pending = Vec::new();
        for row in rows {
            let node_id = row.id.clone();
            let node = row.into_node(path, node_metadata_slice(metadata_rows, &node_id))?;
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

async fn begin_deferred_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    <AsyncSqliteConnection as AsyncConnection>::TransactionManager::begin_transaction(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

async fn commit_deferred_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    <AsyncSqliteConnection as AsyncConnection>::TransactionManager::commit_transaction(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

async fn rollback_deferred_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    <AsyncSqliteConnection as AsyncConnection>::TransactionManager::rollback_transaction(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

async fn persist_node(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    connection
        .immediate_transaction::<(), SqliteTransactionError, _>(async |connection| {
            persist_node_without_transaction(connection, path, node)
                .await
                .map_err(SqliteTransactionError::Operation)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn persist_node_without_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    let row = NodeRow::from_node(node.clone(), path)?;
    diesel::insert_into(nodes::table)
        .values((
            nodes::id.eq(row.id),
            nodes::parent_id.eq(row.parent_id),
            nodes::created_at.eq(row.created_at),
            nodes::role.eq(row.role),
            nodes::kind.eq(row.kind),
            nodes::anchor_kind.eq(row.anchor_kind),
            nodes::anchor_session_role.eq(row.anchor_session_role),
            nodes::anchor_provider_profile.eq(row.anchor_provider_profile),
            nodes::anchor_provider.eq(row.anchor_provider),
            nodes::anchor_model.eq(row.anchor_model),
            nodes::anchor_prompt.eq(row.anchor_prompt),
            nodes::anchor_skill_name.eq(row.anchor_skill_name),
            nodes::anchor_skill_invocation_mode.eq(row.anchor_skill_invocation_mode),
            nodes::metadata_json.eq(row.metadata_json),
            nodes::kind_json.eq(row.kind_json),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    for relation in node_relations(node) {
        diesel::insert_into(node_relations::table)
            .values((
                node_relations::child_node_id.eq(relation.child_node_id),
                node_relations::parent_node_id.eq(relation.parent_node_id),
                node_relations::kind.eq(relation.kind),
                node_relations::ordinal.eq(relation.ordinal),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    persist_node_metadata_rows(connection, path, node).await?;
    Ok(())
}

async fn upsert_node_without_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    let row = NodeRow::from_node(node.clone(), path)?;
    diesel::insert_into(nodes::table)
        .values((
            nodes::id.eq(row.id),
            nodes::parent_id.eq(row.parent_id),
            nodes::created_at.eq(row.created_at),
            nodes::role.eq(row.role),
            nodes::kind.eq(row.kind),
            nodes::anchor_kind.eq(row.anchor_kind),
            nodes::anchor_session_role.eq(row.anchor_session_role),
            nodes::anchor_provider_profile.eq(row.anchor_provider_profile),
            nodes::anchor_provider.eq(row.anchor_provider),
            nodes::anchor_model.eq(row.anchor_model),
            nodes::anchor_prompt.eq(row.anchor_prompt),
            nodes::anchor_skill_name.eq(row.anchor_skill_name),
            nodes::anchor_skill_invocation_mode.eq(row.anchor_skill_invocation_mode),
            nodes::metadata_json.eq(row.metadata_json),
            nodes::kind_json.eq(row.kind_json),
        ))
        .on_conflict(nodes::id)
        .do_update()
        .set((
            nodes::parent_id.eq(diesel::upsert::excluded(nodes::parent_id)),
            nodes::created_at.eq(diesel::upsert::excluded(nodes::created_at)),
            nodes::role.eq(diesel::upsert::excluded(nodes::role)),
            nodes::kind.eq(diesel::upsert::excluded(nodes::kind)),
            nodes::anchor_kind.eq(diesel::upsert::excluded(nodes::anchor_kind)),
            nodes::anchor_session_role.eq(diesel::upsert::excluded(nodes::anchor_session_role)),
            nodes::anchor_provider_profile
                .eq(diesel::upsert::excluded(nodes::anchor_provider_profile)),
            nodes::anchor_provider.eq(diesel::upsert::excluded(nodes::anchor_provider)),
            nodes::anchor_model.eq(diesel::upsert::excluded(nodes::anchor_model)),
            nodes::anchor_prompt.eq(diesel::upsert::excluded(nodes::anchor_prompt)),
            nodes::anchor_skill_name.eq(diesel::upsert::excluded(nodes::anchor_skill_name)),
            nodes::anchor_skill_invocation_mode.eq(diesel::upsert::excluded(
                nodes::anchor_skill_invocation_mode,
            )),
            nodes::metadata_json.eq(diesel::upsert::excluded(nodes::metadata_json)),
            nodes::kind_json.eq(diesel::upsert::excluded(nodes::kind_json)),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    diesel::delete(node_relations::table.filter(node_relations::child_node_id.eq(&node.id)))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    diesel::delete(node_metadata::table.filter(node_metadata::node_id.eq(&node.id)))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    for relation in node_relations(node) {
        diesel::insert_into(node_relations::table)
            .values((
                node_relations::child_node_id.eq(relation.child_node_id),
                node_relations::parent_node_id.eq(relation.parent_node_id),
                node_relations::kind.eq(relation.kind),
                node_relations::ordinal.eq(relation.ordinal),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    persist_node_metadata_rows(connection, path, node).await?;
    Ok(())
}

async fn persist_node_metadata_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    for metadata_row in node_metadata_rows(node) {
        diesel::insert_into(node_metadata::table)
            .values((
                node_metadata::node_id.eq(metadata_row.node_id),
                node_metadata::ordinal.eq(metadata_row.ordinal),
                node_metadata::execution_id.eq(metadata_row.execution_id),
                node_metadata::call_id.eq(metadata_row.call_id),
            ))
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

fn node_metadata_rows(node: &Node) -> Vec<NodeMetadataRow> {
    expected_node_metadata_rows(&node.id, node.metadata.as_ref())
}

fn expected_node_metadata_rows(
    node_id: &str,
    metadata: Option<&NodeMetadata>,
) -> Vec<NodeMetadataRow> {
    metadata
        .into_iter()
        .flat_map(|metadata| metadata.iter())
        .enumerate()
        .map(|(ordinal, metadata)| NodeMetadataRow {
            node_id: node_id.to_owned(),
            ordinal: ordinal as i32,
            execution_id: metadata.execution_id.clone(),
            call_id: metadata.call_id.clone(),
        })
        .collect()
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
        let kind = node.kind.tag().as_str().to_owned();
        let anchor_kind = node
            .kind
            .anchor_payload_kind()
            .map(|kind| kind.as_str().to_owned());
        let anchor_summary = NodeAnchorSummary::from_kind(&node.kind);
        Ok(Self {
            id: node.id,
            parent_id: node.parent,
            created_at: node.created_at.to_string(),
            role: role_name(&node.role).to_owned(),
            kind,
            anchor_kind,
            anchor_session_role: anchor_summary.session_role,
            anchor_provider_profile: anchor_summary.provider_profile,
            anchor_provider: anchor_summary.provider,
            anchor_model: anchor_summary.model,
            anchor_prompt: anchor_summary.prompt,
            anchor_skill_name: anchor_summary.skill_name,
            anchor_skill_invocation_mode: anchor_summary.skill_invocation_mode,
            metadata_json: node
                .metadata
                .map(|metadata| {
                    serde_json::to_string(&metadata).context(ParseSqliteStoreValueSnafu {
                        path: path.to_owned(),
                        column: "nodes.metadata_json".to_owned(),
                    })
                })
                .transpose()?,
            kind_json: kind_residual_json(&node.kind, path)?,
        })
    }

    fn into_node(self, path: &Path, metadata_rows: &[NodeMetadataRow]) -> Result<Node> {
        let kind = self.kind_from_residual_json(path)?;
        ensure!(
            self.kind == kind.tag().as_str(),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "SQLite node kind column {:?} does not match kind_json",
                    self.kind
                ),
            }
        );
        ensure!(
            self.anchor_kind.as_deref() == kind.anchor_payload_kind().map(|kind| kind.as_str()),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "SQLite node anchor_kind column {:?} does not match kind_json",
                    self.anchor_kind
                ),
            }
        );
        validate_node_anchor_summary(path, &self, &kind)?;
        let metadata = self
            .metadata_json
            .map(|metadata| {
                serde_json::from_str::<NodeMetadata>(&metadata).context(
                    ParseSqliteStoreValueSnafu {
                        path: path.to_owned(),
                        column: "nodes.metadata_json".to_owned(),
                    },
                )
            })
            .transpose()?;
        validate_node_metadata_rows(path, &self.id, metadata.as_ref(), metadata_rows)?;
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
            metadata,
            kind,
        })
    }

    fn kind_from_residual_json(&self, path: &Path) -> Result<Kind> {
        let mut value: Value =
            serde_json::from_str(&self.kind_json).context(ParseSqliteStoreValueSnafu {
                path: path.to_owned(),
                column: "nodes.kind_json".to_owned(),
            })?;
        restore_kind_anchor_summary(self, &mut value, path)?;
        serde_json::from_value(value).context(ParseSqliteStoreValueSnafu {
            path: path.to_owned(),
            column: "nodes.kind_json".to_owned(),
        })
    }
}

fn kind_residual_json(kind: &Kind, path: &Path) -> Result<String> {
    let mut value = serde_json::to_value(kind).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "nodes.kind_json".to_owned(),
    })?;
    remove_kind_anchor_summary(&mut value);
    serde_json::to_string(&value).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "nodes.kind_json".to_owned(),
    })
}

fn remove_kind_anchor_summary(value: &mut Value) {
    if let Some(payload) = anchor_payload_object_mut(value, "Session") {
        payload.remove("role");
        payload.remove("provider_profile");
        payload.remove("provider");
        payload.remove("model");
        payload.remove("prompt");
    }
    if let Some(payload) = anchor_payload_object_mut(value, "Prompt") {
        payload.remove("prompt");
    }
    if let Some(payload) = anchor_payload_object_mut(value, "SkillInvocation") {
        payload.remove("skill_name");
        if let Some(mode) = payload.get_mut("mode").and_then(Value::as_object_mut) {
            mode.remove("kind");
            mode.remove("prompt");
        }
    }
    if let Some(payload) = anchor_payload_object_mut(value, "SkillResult") {
        payload.remove("skill_name");
    }
}

fn restore_kind_anchor_summary(row: &NodeRow, value: &mut Value, path: &Path) -> Result<()> {
    if let Some(payload) = anchor_payload_object_mut(value, "Session") {
        ensure_absent(path, "nodes.kind_json", payload, "role")?;
        ensure_absent(path, "nodes.kind_json", payload, "provider_profile")?;
        ensure_absent(path, "nodes.kind_json", payload, "provider")?;
        ensure_absent(path, "nodes.kind_json", payload, "model")?;
        ensure_absent(path, "nodes.kind_json", payload, "prompt")?;
        payload.insert(
            "role".to_owned(),
            session_role_json_value(
                row.anchor_session_role
                    .as_deref()
                    .context(CorruptedStoreSnafu {
                        path: path.to_owned(),
                        message: "missing SQLite node anchor_session_role".to_owned(),
                    })?,
                path,
            )?,
        );
        insert_optional_string(
            payload,
            "provider_profile",
            row.anchor_provider_profile.as_deref(),
        );
        insert_optional_string(payload, "provider", row.anchor_provider.as_deref());
        payload.insert(
            "model".to_owned(),
            Value::String(required_anchor_summary(
                path,
                "anchor_model",
                row.anchor_model.as_deref(),
            )?),
        );
        payload.insert(
            "prompt".to_owned(),
            Value::String(required_anchor_summary(
                path,
                "anchor_prompt",
                row.anchor_prompt.as_deref(),
            )?),
        );
    }
    if let Some(payload) = anchor_payload_object_mut(value, "Prompt") {
        ensure_absent(path, "nodes.kind_json", payload, "prompt")?;
        payload.insert(
            "prompt".to_owned(),
            Value::String(required_anchor_summary(
                path,
                "anchor_prompt",
                row.anchor_prompt.as_deref(),
            )?),
        );
    }
    if let Some(payload) = anchor_payload_object_mut(value, "SkillInvocation") {
        ensure_absent(path, "nodes.kind_json", payload, "skill_name")?;
        payload.insert(
            "skill_name".to_owned(),
            Value::String(required_anchor_summary(
                path,
                "anchor_skill_name",
                row.anchor_skill_name.as_deref(),
            )?),
        );
        let mode = payload
            .get_mut("mode")
            .and_then(Value::as_object_mut)
            .context(CorruptedStoreSnafu {
                path: path.to_owned(),
                message: "missing SQLite node skill invocation mode".to_owned(),
            })?;
        ensure_absent(path, "nodes.kind_json", mode, "kind")?;
        ensure_absent(path, "nodes.kind_json", mode, "prompt")?;
        let mode_kind = required_anchor_summary(
            path,
            "anchor_skill_invocation_mode",
            row.anchor_skill_invocation_mode.as_deref(),
        )?;
        mode.insert("kind".to_owned(), Value::String(mode_kind.clone()));
        if mode_kind == "handoff" {
            mode.insert(
                "prompt".to_owned(),
                Value::String(required_anchor_summary(
                    path,
                    "anchor_prompt",
                    row.anchor_prompt.as_deref(),
                )?),
            );
        }
    }
    if let Some(payload) = anchor_payload_object_mut(value, "SkillResult") {
        ensure_absent(path, "nodes.kind_json", payload, "skill_name")?;
        payload.insert(
            "skill_name".to_owned(),
            Value::String(required_anchor_summary(
                path,
                "anchor_skill_name",
                row.anchor_skill_name.as_deref(),
            )?),
        );
    }
    Ok(())
}

fn anchor_payload_object_mut<'a>(
    value: &'a mut Value,
    payload_kind: &str,
) -> Option<&'a mut Map<String, Value>> {
    value
        .get_mut("Anchor")?
        .get_mut("payload")?
        .get_mut(payload_kind)?
        .as_object_mut()
}

fn ensure_absent(path: &Path, column: &str, object: &Map<String, Value>, key: &str) -> Result<()> {
    ensure!(
        !object.contains_key(key),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite {column} duplicates column-owned field {key:?}"),
        }
    );
    Ok(())
}

fn insert_optional_string(object: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        object.insert(key.to_owned(), Value::String(value.to_owned()));
    }
}

fn required_anchor_summary(path: &Path, column: &str, value: Option<&str>) -> Result<String> {
    value.map(str::to_owned).context(CorruptedStoreSnafu {
        path: path.to_owned(),
        message: format!("missing SQLite node {column}"),
    })
}

fn session_role_json_value(role: &str, path: &Path) -> Result<Value> {
    Ok(Value::String(match parse_session_role(role, path)? {
        SessionRole::Orchestrator => "orchestrator".to_owned(),
        SessionRole::Runner => "runner".to_owned(),
    }))
}

#[derive(Default)]
struct NodeAnchorSummary {
    session_role: Option<String>,
    provider_profile: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    prompt: Option<String>,
    skill_name: Option<String>,
    skill_invocation_mode: Option<String>,
}

impl NodeAnchorSummary {
    fn from_kind(kind: &Kind) -> Self {
        let Kind::Anchor(anchor) = kind else {
            return Self::default();
        };
        match &anchor.payload {
            AnchorPayload::Session(anchor) => Self {
                session_role: Some(anchor.role.as_str().to_owned()),
                provider_profile: anchor.provider_profile.clone(),
                provider: anchor.provider.clone(),
                model: Some(anchor.model.clone()),
                prompt: Some(anchor.prompt.clone()),
                ..Self::default()
            },
            AnchorPayload::SessionPatch(_) => Self::default(),
            AnchorPayload::Prompt(anchor) => Self {
                prompt: Some(anchor.prompt.clone()),
                ..Self::default()
            },
            AnchorPayload::SkillInvocation(anchor) => {
                let mut summary = Self {
                    skill_name: Some(anchor.skill_name.clone()),
                    ..Self::default()
                };
                match &anchor.mode {
                    SkillInvocationMode::InheritContext => {
                        summary.skill_invocation_mode = Some("inherit_context".to_owned());
                    }
                    SkillInvocationMode::Handoff { prompt } => {
                        summary.skill_invocation_mode = Some("handoff".to_owned());
                        summary.prompt = Some(prompt.clone());
                    }
                }
                summary
            }
            AnchorPayload::SkillResult(anchor) => Self {
                skill_name: Some(anchor.skill_name.clone()),
                ..Self::default()
            },
        }
    }
}

fn validate_node_anchor_summary(path: &Path, row: &NodeRow, kind: &Kind) -> Result<()> {
    let expected = NodeAnchorSummary::from_kind(kind);
    validate_optional_text_summary(
        path,
        "nodes.anchor_session_role",
        row.anchor_session_role.as_deref(),
        expected.session_role.as_deref(),
    )?;
    validate_optional_text_summary(
        path,
        "nodes.anchor_provider_profile",
        row.anchor_provider_profile.as_deref(),
        expected.provider_profile.as_deref(),
    )?;
    validate_optional_text_summary(
        path,
        "nodes.anchor_provider",
        row.anchor_provider.as_deref(),
        expected.provider.as_deref(),
    )?;
    validate_optional_text_summary(
        path,
        "nodes.anchor_model",
        row.anchor_model.as_deref(),
        expected.model.as_deref(),
    )?;
    validate_optional_text_summary(
        path,
        "nodes.anchor_prompt",
        row.anchor_prompt.as_deref(),
        expected.prompt.as_deref(),
    )?;
    validate_optional_text_summary(
        path,
        "nodes.anchor_skill_name",
        row.anchor_skill_name.as_deref(),
        expected.skill_name.as_deref(),
    )?;
    validate_optional_text_summary(
        path,
        "nodes.anchor_skill_invocation_mode",
        row.anchor_skill_invocation_mode.as_deref(),
        expected.skill_invocation_mode.as_deref(),
    )
}

fn validate_node_metadata_rows(
    path: &Path,
    node_id: &str,
    metadata: Option<&NodeMetadata>,
    metadata_rows: &[NodeMetadataRow],
) -> Result<()> {
    let expected = expected_node_metadata_rows(node_id, metadata);
    ensure!(
        expected == metadata_rows,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "SQLite node metadata rows for {node_id:?} do not match metadata_json"
            ),
        }
    );
    Ok(())
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
    let branches = branches::table
        .select((branches::name, branches::head_id))
        .order(branches::name)
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
    let sessions = sessions::table
        .select((
            sessions::branch_name,
            sessions::state,
            sessions::target_branch,
            sessions::base_head_id,
            sessions::pause_reason,
            sessions::merged_anchor_id,
            sessions::state_json,
        ))
        .order(sessions::branch_name)
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
        validate_session_row_summary(&session, &state_json, path)?;
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
    diesel::insert_into(branches::table)
        .values((branches::name.eq(branch), branches::head_id.eq(head_id)))
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
        .immediate_transaction::<(), SqliteTransactionError, _>(async |connection| {
            persist_branch(connection, path, branch, head_id)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            persist_session_state(connection, path, branch, session_state)
                .await
                .map_err(SqliteTransactionError::Operation)
        })
        .await
        .map_err(|error| error.into_store_error(path))
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
    let pause_reason = state.pause_reason();
    diesel::insert_into(sessions::table)
        .values((
            sessions::branch_name.eq(branch),
            sessions::state.eq(state.as_str()),
            sessions::target_branch.eq(state.target_branch()),
            sessions::base_head_id.eq(state.base_head_id()),
            sessions::pause_reason.eq(pause_reason.map(|reason| reason.as_str())),
            sessions::merged_anchor_id
                .eq(pause_reason.and_then(|reason| reason.merged_anchor_id())),
            sessions::state_json.eq(state_json),
        ))
        .on_conflict(sessions::branch_name)
        .do_update()
        .set((
            sessions::state.eq(diesel::upsert::excluded(sessions::state)),
            sessions::target_branch.eq(diesel::upsert::excluded(sessions::target_branch)),
            sessions::base_head_id.eq(diesel::upsert::excluded(sessions::base_head_id)),
            sessions::pause_reason.eq(diesel::upsert::excluded(sessions::pause_reason)),
            sessions::merged_anchor_id.eq(diesel::upsert::excluded(sessions::merged_anchor_id)),
            sessions::state_json.eq(diesel::upsert::excluded(sessions::state_json)),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

fn validate_session_row_summary(row: &SessionRow, state: &SessionState, path: &Path) -> Result<()> {
    validate_text_summary(path, "sessions.state", &row.state, state.as_str())?;
    validate_optional_text_summary(
        path,
        "sessions.target_branch",
        row.target_branch.as_deref(),
        state.target_branch(),
    )?;
    validate_optional_text_summary(
        path,
        "sessions.base_head_id",
        row.base_head_id.as_deref(),
        state.base_head_id(),
    )?;

    let pause_reason = state.pause_reason();
    validate_optional_text_summary(
        path,
        "sessions.pause_reason",
        row.pause_reason.as_deref(),
        pause_reason.map(|reason| reason.as_str()),
    )?;
    validate_optional_text_summary(
        path,
        "sessions.merged_anchor_id",
        row.merged_anchor_id.as_deref(),
        pause_reason.and_then(|reason| reason.merged_anchor_id()),
    )
}

async fn update_branch_head(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected_old_head: &str,
    new_head: &str,
) -> Result<usize> {
    diesel::update(
        branches::table
            .filter(branches::name.eq(branch))
            .filter(branches::head_id.eq(expected_old_head)),
    )
    .set(branches::head_id.eq(new_head))
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
    diesel::delete(branches::table.filter(branches::name.eq(branch)))
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
    connection
        .immediate_transaction::<(), SqliteTransactionError, _>(async |connection| {
            persist_session_nodes_and_branch_head_in_transaction(
                connection,
                path,
                branch,
                expected_old_head,
                new_head,
                nodes,
            )
            .await
            .map_err(SqliteTransactionError::Operation)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn persist_session_nodes_and_branch_head_in_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected_old_head: &str,
    new_head: &str,
    nodes: &[Node],
) -> Result<()> {
    for node in nodes {
        upsert_node_without_transaction(connection, path, node).await?;
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
    let jobs = jobs::table
        .select((
            jobs::job_id,
            jobs::created_at,
            jobs::finished_at,
            jobs::branch,
            jobs::work_branch,
            jobs::base,
            jobs::status,
            jobs::payload_json,
        ))
        .order(jobs::job_id)
        .load::<JobRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    state.jobs.clear();
    for row in jobs {
        let mut job =
            serde_json::from_str::<Job>(&row.payload_json).context(ParseSqliteStoreValueSnafu {
                path: path.to_owned(),
                column: "jobs.payload_json".to_owned(),
            })?;
        job.normalize_work_branch();
        validate_job_row_summary(&row, &job, path)?;
        state.jobs.insert(job.job_id.clone(), job);
    }
    Ok(())
}

async fn persist_job(connection: &mut AsyncSqliteConnection, path: &Path, job: &Job) -> Result<()> {
    let mut summary = job.clone();
    summary.normalize_work_branch();
    let finished_at = summary
        .finished_at
        .as_ref()
        .map(std::string::ToString::to_string);
    let payload_json = serde_json::to_string(job).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "jobs.payload_json".to_owned(),
    })?;
    diesel::insert_into(jobs::table)
        .values((
            jobs::job_id.eq(&job.job_id),
            jobs::created_at.eq(summary.created_at.to_string()),
            jobs::finished_at.eq(finished_at),
            jobs::branch.eq(&summary.branch),
            jobs::work_branch.eq(&summary.work_branch),
            jobs::base.eq(&summary.base),
            jobs::status.eq(summary.status.as_str()),
            jobs::payload_json.eq(payload_json),
        ))
        .on_conflict(jobs::job_id)
        .do_update()
        .set((
            jobs::created_at.eq(diesel::upsert::excluded(jobs::created_at)),
            jobs::finished_at.eq(diesel::upsert::excluded(jobs::finished_at)),
            jobs::branch.eq(diesel::upsert::excluded(jobs::branch)),
            jobs::work_branch.eq(diesel::upsert::excluded(jobs::work_branch)),
            jobs::base.eq(diesel::upsert::excluded(jobs::base)),
            jobs::status.eq(diesel::upsert::excluded(jobs::status)),
            jobs::payload_json.eq(diesel::upsert::excluded(jobs::payload_json)),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

fn validate_job_row_summary(row: &JobRow, job: &Job, path: &Path) -> Result<()> {
    let finished_at = job
        .finished_at
        .as_ref()
        .map(std::string::ToString::to_string);

    validate_text_summary(path, "jobs.job_id", &row.job_id, &job.job_id)?;
    validate_text_summary(
        path,
        "jobs.created_at",
        &row.created_at,
        &job.created_at.to_string(),
    )?;
    validate_optional_text_summary(
        path,
        "jobs.finished_at",
        row.finished_at.as_deref(),
        finished_at.as_deref(),
    )?;
    validate_text_summary(path, "jobs.branch", &row.branch, &job.branch)?;
    validate_text_summary(path, "jobs.work_branch", &row.work_branch, &job.work_branch)?;
    validate_text_summary(path, "jobs.base", &row.base, &job.base)?;
    validate_text_summary(path, "jobs.status", &row.status, job.status.as_str())
}

fn validate_text_summary(
    path: &Path,
    column: &'static str,
    actual: &str,
    expected: &str,
) -> Result<()> {
    ensure!(
        actual == expected,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("{column} value {actual:?} does not match JSON value {expected:?}"),
        }
    );
    Ok(())
}

fn validate_optional_text_summary(
    path: &Path,
    column: &'static str,
    actual: Option<&str>,
    expected: Option<&str>,
) -> Result<()> {
    ensure!(
        actual == expected,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("{column} value {actual:?} does not match JSON value {expected:?}"),
        }
    );
    Ok(())
}

async fn load_message_queue_items(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    state: &mut StoreState,
) -> Result<()> {
    let rows = message_queue_items_with_rowid::table
        .select((
            message_queue_items_with_rowid::rowid,
            message_queue_items_with_rowid::payload_json,
        ))
        .order(message_queue_items_with_rowid::rowid)
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
    diesel::insert_into(message_queue_items::table)
        .values((
            message_queue_items::queue.eq(&item.queue),
            message_queue_items::message_id.eq(&item.message_id),
            message_queue_items::created_at.eq(item.created_at.to_string()),
            message_queue_items::payload_json.eq(item_json),
        ))
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
    diesel::delete(
        message_queue_items::table
            .filter(message_queue_items::queue.eq(&item.queue))
            .filter(message_queue_items::message_id.eq(&item.message_id)),
    )
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
    let rows = presets::table
        .select(presets::record_json)
        .order(presets::name)
        .load::<String>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    state.presets.clear();
    for record_json in rows {
        let record = serde_json::from_str::<PresetRecord>(&record_json).context(
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
    diesel::insert_into(presets::table)
        .values((
            presets::name.eq(&record.name),
            presets::record_json.eq(record_json),
        ))
        .on_conflict(presets::name)
        .do_update()
        .set(presets::record_json.eq(diesel::upsert::excluded(presets::record_json)))
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
    diesel::delete(presets::table.filter(presets::name.eq(name)))
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
    let rows = skills::table
        .select((skills::role, skills::record_json))
        .order((skills::role, skills::name))
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
    diesel::insert_into(skills::table)
        .values((
            skills::role.eq(role.as_str()),
            skills::name.eq(&record.name),
            skills::record_json.eq(record_json),
        ))
        .on_conflict((skills::role, skills::name))
        .do_update()
        .set(skills::record_json.eq(diesel::upsert::excluded(skills::record_json)))
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
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move {
                let mut rows = Vec::new();
                let mut seen = HashSet::new();
                let mut current_id = head_id;
                loop {
                    ensure!(
                        seen.insert(current_id.clone()),
                        CorruptedStoreSnafu {
                            path: path.clone(),
                            message: "SQLite nodes contain cyclic parents".to_owned(),
                        }
                    );
                    let row = nodes::table
                        .filter(nodes::id.eq(&current_id))
                        .select(node_row_columns!())
                        .get_result::<NodeRow>(connection)
                        .await
                        .optional()
                        .context(QuerySqliteStoreSnafu { path: path.clone() })?
                        .context(ParentNotFoundSnafu {
                            id: current_id.clone(),
                        })?;
                    let parent_id = row.parent_id.clone();
                    let is_root = parent_id.is_empty();
                    rows.push(row);
                    if is_root {
                        break;
                    }
                    current_id = parent_id;
                }

                let nodes = node_rows_into_nodes(connection, &path, rows).await?;
                Ok(nodes)
            })
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
        let node_id = node_id.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move {
                let rows = node_relations::table
                    .inner_join(nodes::table.on(nodes::id.eq(node_relations::child_node_id)))
                    .filter(node_relations::parent_node_id.eq(node_id))
                    .select(node_row_columns!())
                    .order((nodes::created_at, nodes::id))
                    .load::<NodeRow>(connection)
                    .await
                    .context(QuerySqliteStoreSnafu { path: path.clone() })?;
                node_rows_into_nodes(connection, &path, rows).await
            })
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
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move {
                let sessions = sessions::table
                    .select((
                        sessions::branch_name,
                        sessions::state,
                        sessions::target_branch,
                        sessions::base_head_id,
                        sessions::pause_reason,
                        sessions::merged_anchor_id,
                        sessions::state_json,
                    ))
                    .order(sessions::branch_name)
                    .load::<SessionRow>(connection)
                    .await
                    .context(QuerySqliteStoreSnafu { path: path.clone() })?;
                sessions
                    .into_iter()
                    .map(|session| {
                        let state = serde_json::from_str::<SessionState>(&session.state_json)
                            .context(ParseSqliteStoreValueSnafu {
                                path: path.clone(),
                                column: "sessions.state_json".to_owned(),
                            })?;
                        validate_session_row_summary(&session, &state, &path)?;
                        Ok((session.branch_name, state))
                    })
                    .collect()
            })
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
        AsyncSqliteConnection, MessageQueueItem, NodeMetadataRow, NodeRow, SqliteGraphStore,
        SqliteStore, StoreAccess, persist_root_metadata,
    };
    use crate::schema::{
        branches, jobs, node_metadata, node_relations, nodes, sessions, store_meta,
    };
    use crate::{
        Anchor, BackendMetadata, BranchStore, JobStatus, JobStore, Kind, MergeParent,
        MessageQueueStore, NewNode, Node, NodeMetadata, NodeStore, PauseReason, Preset,
        PresetStore, Role, SessionAnchor, SessionAnchorPatch, SessionRole, SessionState,
        SessionStore, SkillStore, SkillUpdatePatch, SkillVersionSpec,
    };
    use diesel::prelude::*;
    use diesel_async::{AsyncConnection, RunQueryDsl, SimpleAsyncConnection};
    use std::sync::mpsc;
    use std::time::Duration;

    #[derive(diesel::Queryable, Debug, PartialEq, Eq)]
    struct NodeRelationRow {
        child_node_id: String,
        parent_node_id: String,
        kind: String,
        ordinal: i32,
    }

    #[derive(diesel::Queryable, Debug, PartialEq, Eq)]
    struct NodeKindRow {
        kind: String,
        anchor_kind: Option<String>,
    }

    #[derive(diesel::Queryable, Debug, PartialEq, Eq)]
    struct NodeAnchorSummaryRow {
        anchor_session_role: Option<String>,
        anchor_provider_profile: Option<String>,
        anchor_provider: Option<String>,
        anchor_model: Option<String>,
        anchor_prompt: Option<String>,
        anchor_skill_name: Option<String>,
        anchor_skill_invocation_mode: Option<String>,
    }

    #[derive(diesel::Queryable, Debug, PartialEq, Eq)]
    struct SessionSummaryRow {
        state: String,
        target_branch: Option<String>,
        base_head_id: Option<String>,
        pause_reason: Option<String>,
        merged_anchor_id: Option<String>,
    }

    #[derive(diesel::Queryable, Debug, PartialEq, Eq)]
    struct JobSummaryRow {
        created_at: String,
        finished_at: Option<String>,
        branch: String,
        work_branch: String,
        base: String,
        status: String,
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
            node_relations::table
                .filter(node_relations::child_node_id.eq(child_node_id))
                .select((
                    node_relations::child_node_id,
                    node_relations::parent_node_id,
                    node_relations::kind,
                    node_relations::ordinal,
                ))
                .order((
                    node_relations::kind,
                    node_relations::ordinal,
                    node_relations::parent_node_id,
                ))
                .load::<NodeRelationRow>(&mut connection)
                .await
                .unwrap()
        })
    }

    fn node_metadata_rows(store: &SqliteStore, node_id: &str) -> Vec<NodeMetadataRow> {
        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            node_metadata::table
                .filter(node_metadata::node_id.eq(node_id))
                .select((
                    node_metadata::node_id,
                    node_metadata::ordinal,
                    node_metadata::execution_id,
                    node_metadata::call_id,
                ))
                .order(node_metadata::ordinal)
                .load::<NodeMetadataRow>(&mut connection)
                .await
                .unwrap()
        })
    }

    fn node_kinds(store: &SqliteStore, node_id: &str) -> (String, Option<String>) {
        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            let row = nodes::table
                .filter(nodes::id.eq(node_id))
                .select((nodes::kind, nodes::anchor_kind))
                .get_result::<NodeKindRow>(&mut connection)
                .await
                .unwrap();
            (row.kind, row.anchor_kind)
        })
    }

    fn node_kind_json(store: &SqliteStore, node_id: &str) -> serde_json::Value {
        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            let kind_json = nodes::table
                .filter(nodes::id.eq(node_id))
                .select(nodes::kind_json)
                .get_result::<String>(&mut connection)
                .await
                .unwrap();
            serde_json::from_str(&kind_json).unwrap()
        })
    }

    fn node_anchor_summary(store: &SqliteStore, node_id: &str) -> NodeAnchorSummaryRow {
        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            nodes::table
                .filter(nodes::id.eq(node_id))
                .select((
                    nodes::anchor_session_role,
                    nodes::anchor_provider_profile,
                    nodes::anchor_provider,
                    nodes::anchor_model,
                    nodes::anchor_prompt,
                    nodes::anchor_skill_name,
                    nodes::anchor_skill_invocation_mode,
                ))
                .get_result::<NodeAnchorSummaryRow>(&mut connection)
                .await
                .unwrap()
        })
    }

    fn session_summary(store: &SqliteStore, branch: &str) -> SessionSummaryRow {
        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            sessions::table
                .filter(sessions::branch_name.eq(branch))
                .select((
                    sessions::state,
                    sessions::target_branch,
                    sessions::base_head_id,
                    sessions::pause_reason,
                    sessions::merged_anchor_id,
                ))
                .get_result::<SessionSummaryRow>(&mut connection)
                .await
                .unwrap()
        })
    }

    fn job_summary(store: &SqliteStore, job_id: &str) -> JobSummaryRow {
        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            jobs::table
                .filter(jobs::job_id.eq(job_id))
                .select((
                    jobs::created_at,
                    jobs::finished_at,
                    jobs::branch,
                    jobs::work_branch,
                    jobs::base,
                    jobs::status,
                ))
                .get_result::<JobSummaryRow>(&mut connection)
                .await
                .unwrap()
        })
    }

    fn persist_store_meta_bool_for_test(store: &SqliteStore, key: &str, value: bool) {
        let value_json = serde_json::to_string(&value).unwrap();
        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            diesel::insert_into(store_meta::table)
                .values((
                    store_meta::key.eq(key),
                    store_meta::value_json.eq(value_json),
                ))
                .on_conflict(store_meta::key)
                .do_update()
                .set(store_meta::value_json.eq(diesel::upsert::excluded(store_meta::value_json)))
                .execute(&mut connection)
                .await
                .unwrap();
        });
    }

    async fn apply_v1_schema_for_test(
        connection: &mut AsyncSqliteConnection,
        store_path: &std::path::Path,
    ) {
        super::configure_writable_connection(connection, store_path)
            .await
            .unwrap();
        super::ensure_migration_table(connection, store_path)
            .await
            .unwrap();
        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000001_initial_store_schema/up.sql"
            ))
            .await
            .unwrap();
        diesel::insert_into(super::__diesel_schema_migrations::table)
            .values(super::__diesel_schema_migrations::version.eq("00000000000001"))
            .execute(connection)
            .await
            .unwrap();
    }

    async fn apply_legacy_v1_schema_for_test(
        connection: &mut AsyncSqliteConnection,
        store_path: &std::path::Path,
    ) {
        super::configure_writable_connection(connection, store_path)
            .await
            .unwrap();
        connection
            .batch_execute(
                r#"
CREATE TABLE store_schema_migrations (
    version INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"#,
            )
            .await
            .unwrap();
        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000001_initial_store_schema/up.sql"
            ))
            .await
            .unwrap();
        diesel::insert_into(super::store_schema_migrations::table)
            .values((
                super::store_schema_migrations::version.eq(1),
                super::store_schema_migrations::name.eq("initial-store-schema"),
            ))
            .execute(connection)
            .await
            .unwrap();
    }

    async fn insert_v1_node_row(
        connection: &mut AsyncSqliteConnection,
        store_path: &std::path::Path,
        node: Node,
    ) {
        let kind_json = serde_json::to_string(&node.kind).unwrap();
        let row = NodeRow::from_node(node, store_path).unwrap();
        diesel::insert_into(nodes::table)
            .values((
                nodes::id.eq(row.id),
                nodes::parent_id.eq(row.parent_id),
                nodes::created_at.eq(row.created_at),
                nodes::role.eq(row.role),
                nodes::metadata_json.eq(row.metadata_json),
                nodes::kind_json.eq(kind_json),
            ))
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
        assert_eq!(store.schema_version().unwrap(), 6);
    }

    #[test]
    fn cloned_sqlite_store_shares_database_instance() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");

        let store = SqliteStore::open(&path).unwrap();
        let cloned = store.clone();

        assert!(std::ptr::eq(
            store.database.shared_pool(),
            cloned.database.shared_pool()
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

        assert!(std::ptr::eq(
            store.database.shared_pool(),
            read_only.database.shared_pool()
        ));
        assert!(std::ptr::eq(
            store.database.shared_pool(),
            graph.database.shared_pool()
        ));
        assert!(std::ptr::eq(
            store.database.shared_pool(),
            lexical_read_only.database.shared_pool()
        ));
    }

    #[test]
    fn graph_store_connection_contention_does_not_block_writer() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let graph = SqliteGraphStore::open_read_only(&path).unwrap();
        let graph_database = graph.database.clone();
        let graph_connection_database = graph_database.clone();
        let (graph_locked_tx, graph_locked_rx) = mpsc::channel();
        let (release_graph_tx, release_graph_rx) = mpsc::channel();
        let graph_lock = std::thread::spawn(move || {
            graph_database.block_on(async move {
                let _connection = graph_connection_database.connection().await.unwrap();
                graph_locked_tx.send(()).unwrap();
                release_graph_rx.recv().unwrap();
            });
        });
        graph_locked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("graph connection lock should be held");

        let writer = store.clone();
        let root = store.root_id();
        let (write_tx, write_rx) = mpsc::channel();
        let write = std::thread::spawn(move || {
            let node = writer
                .append(NewNode {
                    parent: root,
                    role: Role::User,
                    metadata: None,
                    kind: Kind::Text("write while graph rebuild holds its connection".to_owned()),
                })
                .unwrap();
            write_tx.send(node).unwrap();
        });

        let written = write_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("writer should not wait for graph connection release");
        release_graph_tx.send(()).unwrap();
        graph_lock.join().unwrap();
        write.join().unwrap();
        assert_eq!(store.get_node(&written).unwrap().id, written);
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
    fn load_state_reads_from_one_snapshot() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let root_id = store.root_id();

        let (snapshot, child_id) = store.block_on(async {
            let mut reader = store.connect().await.unwrap();
            reader
                .transaction::<_, super::SqliteTransactionError, _>(async |reader| {
                    assert_eq!(
                        super::load_root_id(reader, &store.database_path)
                            .await
                            .map_err(super::SqliteTransactionError::Operation)?,
                        root_id
                    );

                    let writer = store.clone();
                    let child_parent = root_id.clone();
                    let handle = std::thread::spawn(move || {
                        writer
                            .append(NewNode {
                                parent: child_parent,
                                role: Role::User,
                                metadata: None,
                                kind: Kind::Text("concurrent child".to_owned()),
                            })
                            .unwrap()
                    });
                    let child_id = handle.join().unwrap();

                    let snapshot =
                        super::load_state_without_transaction(reader, &store.database_path)
                            .await
                            .map_err(super::SqliteTransactionError::Operation)?;
                    Ok((snapshot, child_id))
                })
                .await
                .map_err(|error| error.into_store_error(&store.database_path))
                .unwrap()
        });

        assert!(snapshot.get_node(&child_id).is_err());
        assert!(store.get_node(&child_id).is_ok());
    }

    #[test]
    fn open_read_only_accepts_current_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        SqliteStore::open(&path).unwrap();

        let store = SqliteStore::open_read_only(&path).unwrap();

        assert_eq!(store.schema_version().unwrap(), 6);
    }

    #[test]
    fn open_read_only_accepts_legacy_current_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let root_id = store.root_id();
        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            connection
                .batch_execute(
                    r#"
CREATE TABLE store_schema_migrations (
    version INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
INSERT INTO store_schema_migrations (version, name)
VALUES
    (1, 'initial-store-schema'),
    (2, 'node-relations'),
    (3, 'node-kind'),
    (4, 'session-job-summary'),
    (5, 'node-metadata'),
    (6, 'node-anchor-summary');
DROP TABLE __diesel_schema_migrations;
"#,
                )
                .await
                .unwrap();
        });
        drop(store);

        let store = SqliteStore::open_read_only(&path).unwrap();
        let graph = SqliteGraphStore::open_read_only(&path).unwrap();

        assert_eq!(store.schema_version().unwrap(), 6);
        assert_eq!(store.root_id(), root_id);
        assert_eq!(graph.root_id, root_id);
    }

    #[test]
    fn open_read_only_accepts_legacy_current_schema_with_empty_diesel_table() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let root_id = store.root_id();
        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            connection
                .batch_execute(
                    r#"
CREATE TABLE store_schema_migrations (
    version INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
INSERT INTO store_schema_migrations (version, name)
VALUES
    (1, 'initial-store-schema'),
    (2, 'node-relations'),
    (3, 'node-kind'),
    (4, 'session-job-summary'),
    (5, 'node-metadata'),
    (6, 'node-anchor-summary');
DELETE FROM __diesel_schema_migrations;
"#,
                )
                .await
                .unwrap();
        });
        drop(store);

        let store = SqliteStore::open_read_only(&path).unwrap();
        let graph = SqliteGraphStore::open_read_only(&path).unwrap();

        assert_eq!(store.schema_version().unwrap(), 6);
        assert_eq!(store.root_id(), root_id);
        assert_eq!(graph.root_id, root_id);
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
        let child_kind = Kind::Anchor(Anchor::session(
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
        ));
        let expected_node_kinds = (
            child_kind.tag().as_str().to_owned(),
            child_kind
                .anchor_payload_kind()
                .map(|kind| kind.as_str().to_owned()),
        );
        let child = store
            .append(NewNode {
                parent: primary_parent.clone(),
                role: Role::System,
                metadata: None,
                kind: child_kind,
            })
            .unwrap();

        let relations = node_relation_rows(&store, &child);

        assert_eq!(node_kinds(&store, &child), expected_node_kinds);
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
    fn append_persists_node_metadata_rows() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let root_id = store.root_id();
        let single_metadata = BackendMetadata {
            execution_id: Some("execution-single".to_owned()),
            call_id: Some("call-single".to_owned()),
        };
        let single = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: Some(NodeMetadata::one(single_metadata)),
                kind: Kind::Text("single metadata".to_owned()),
            })
            .unwrap();
        let many = store
            .append(NewNode {
                parent: root_id,
                role: Role::LLM,
                metadata: Some(NodeMetadata::many(vec![
                    BackendMetadata {
                        execution_id: Some("execution-many".to_owned()),
                        call_id: Some("call-a".to_owned()),
                    },
                    BackendMetadata {
                        execution_id: Some("execution-many".to_owned()),
                        call_id: Some("call-b".to_owned()),
                    },
                ])),
                kind: Kind::Text("many metadata".to_owned()),
            })
            .unwrap();

        assert_eq!(
            node_metadata_rows(&store, &single),
            vec![NodeMetadataRow {
                node_id: single,
                ordinal: 0,
                execution_id: Some("execution-single".to_owned()),
                call_id: Some("call-single".to_owned()),
            }]
        );
        assert_eq!(
            node_metadata_rows(&store, &many),
            vec![
                NodeMetadataRow {
                    node_id: many.clone(),
                    ordinal: 0,
                    execution_id: Some("execution-many".to_owned()),
                    call_id: Some("call-a".to_owned()),
                },
                NodeMetadataRow {
                    node_id: many,
                    ordinal: 1,
                    execution_id: Some("execution-many".to_owned()),
                    call_id: Some("call-b".to_owned()),
                },
            ]
        );
    }

    #[test]
    fn append_persists_node_anchor_summary() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let root_id = store.root_id();
        let session = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(
                    vec![],
                    SessionAnchor {
                        role: SessionRole::Runner,
                        provider_profile: Some("runner-profile".to_owned()),
                        provider: Some("openai".to_owned()),
                        model: "gpt-5.4".to_owned(),
                        tools: vec![],
                        system_prompt: "system".to_owned(),
                        prompt: "session prompt".to_owned(),
                        temperature: Some(0.1),
                        max_tokens: Some(64),
                        additional_params: None,
                        enable_coco_shim: false,
                        active_skill: None,
                    },
                )),
            })
            .unwrap();
        let prompt = store
            .append(NewNode {
                parent: root_id,
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![],
                    crate::PromptAnchor {
                        prompt: "detached prompt".to_owned(),
                        attachments: vec![],
                    },
                )),
            })
            .unwrap();

        assert_eq!(
            node_anchor_summary(&store, &session),
            NodeAnchorSummaryRow {
                anchor_session_role: Some("runner".to_owned()),
                anchor_provider_profile: Some("runner-profile".to_owned()),
                anchor_provider: Some("openai".to_owned()),
                anchor_model: Some("gpt-5.4".to_owned()),
                anchor_prompt: Some("session prompt".to_owned()),
                anchor_skill_name: None,
                anchor_skill_invocation_mode: None,
            }
        );
        let session_kind_json = node_kind_json(&store, &session);
        assert_eq!(
            session_kind_json.pointer("/Anchor/payload/Session/role"),
            None
        );
        assert_eq!(
            session_kind_json.pointer("/Anchor/payload/Session/provider_profile"),
            None
        );
        assert_eq!(
            session_kind_json.pointer("/Anchor/payload/Session/provider"),
            None
        );
        assert_eq!(
            session_kind_json.pointer("/Anchor/payload/Session/model"),
            None
        );
        assert_eq!(
            session_kind_json.pointer("/Anchor/payload/Session/prompt"),
            None
        );
        assert_eq!(
            node_anchor_summary(&store, &prompt),
            NodeAnchorSummaryRow {
                anchor_session_role: None,
                anchor_provider_profile: None,
                anchor_provider: None,
                anchor_model: None,
                anchor_prompt: Some("detached prompt".to_owned()),
                anchor_skill_name: None,
                anchor_skill_invocation_mode: None,
            }
        );
        let prompt_kind_json = node_kind_json(&store, &prompt);
        assert_eq!(
            prompt_kind_json.pointer("/Anchor/payload/Prompt/prompt"),
            None
        );
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
    fn schema_migration_backfills_node_relations_and_node_kind() {
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
        let expected_child_kinds = (
            child.kind.tag().as_str().to_owned(),
            child
                .kind
                .anchor_payload_kind()
                .map(|kind| kind.as_str().to_owned()),
        );
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
            apply_v1_schema_for_test(&mut connection, &store.database_path).await;
            persist_root_metadata(&mut connection, &store.database_path, &root_id)
                .await
                .unwrap();
            insert_v1_node_row(&mut connection, &store.database_path, root).await;
            insert_v1_node_row(&mut connection, &store.database_path, merge_parent_a).await;
            insert_v1_node_row(&mut connection, &store.database_path, merge_parent_b).await;
            insert_v1_node_row(&mut connection, &store.database_path, child).await;
            diesel::insert_into(super::legacy_node_edges::table)
                .values((
                    super::legacy_node_edges::parent_id.eq(&root_id),
                    super::legacy_node_edges::child_id.eq(&child_id),
                    super::legacy_node_edges::kind.eq("primary"),
                ))
                .execute(&mut connection)
                .await
                .unwrap();
            diesel::insert_into(super::legacy_node_edges::table)
                .values((
                    super::legacy_node_edges::parent_id.eq(&merge_parent_a_id),
                    super::legacy_node_edges::child_id.eq(&child_id),
                    super::legacy_node_edges::kind.eq("merge"),
                ))
                .execute(&mut connection)
                .await
                .unwrap();
            diesel::insert_into(super::legacy_node_edges::table)
                .values((
                    super::legacy_node_edges::parent_id.eq(&merge_parent_b_id),
                    super::legacy_node_edges::child_id.eq(&child_id),
                    super::legacy_node_edges::kind.eq("merge"),
                ))
                .execute(&mut connection)
                .await
                .unwrap();
        });
        drop(store);

        let crate::store::PersistentStore::Sqlite(migrated) =
            crate::store::PersistentStore::open_read_only_or_upgrade_schema(&path).unwrap();

        assert_eq!(migrated.schema_version().unwrap(), 6);
        assert_eq!(node_kinds(&migrated, &child_id), expected_child_kinds);
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
    fn schema_migration_backfills_session_and_job_summary() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();
        let store = SqliteStore::new(&path, StoreAccess::ReadWrite).unwrap();
        let state = super::StoreState::new();
        let root = state.root_node().clone();
        let root_id = root.id.clone();
        let session_state = SessionState::Attached {
            target_branch: "main".to_owned(),
            base_head_id: root_id.clone(),
        };
        let session_state_json = serde_json::to_string(&session_state).unwrap();
        let job_json = serde_json::json!({
            "job_id": "job-test",
            "created_at": "2026-01-01T00:00:00Z",
            "finished_at": null,
            "branch": "main",
            "work_branch": "",
            "base": root_id,
            "status": "queued"
        })
        .to_string();

        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            apply_v1_schema_for_test(&mut connection, &store.database_path).await;
            persist_root_metadata(&mut connection, &store.database_path, state.root_id())
                .await
                .unwrap();
            insert_v1_node_row(&mut connection, &store.database_path, root).await;
            diesel::insert_into(branches::table)
                .values((
                    branches::name.eq("main"),
                    branches::head_id.eq(state.root_id()),
                ))
                .execute(&mut connection)
                .await
                .unwrap();
            diesel::insert_into(sessions::table)
                .values((
                    sessions::branch_name.eq("main"),
                    sessions::state_json.eq(session_state_json),
                ))
                .execute(&mut connection)
                .await
                .unwrap();
            diesel::insert_into(jobs::table)
                .values((jobs::job_id.eq("job-test"), jobs::payload_json.eq(job_json)))
                .execute(&mut connection)
                .await
                .unwrap();
        });
        drop(store);

        let crate::store::PersistentStore::Sqlite(migrated) =
            crate::store::PersistentStore::open_read_only_or_upgrade_schema(&path).unwrap();

        assert_eq!(migrated.schema_version().unwrap(), 6);
        assert_eq!(
            session_summary(&migrated, "main"),
            SessionSummaryRow {
                state: "attached".to_owned(),
                target_branch: Some("main".to_owned()),
                base_head_id: Some(state.root_id().to_owned()),
                pause_reason: None,
                merged_anchor_id: None,
            }
        );
        assert_eq!(
            job_summary(&migrated, "job-test"),
            JobSummaryRow {
                created_at: "2026-01-01T00:00:00Z".to_owned(),
                finished_at: None,
                branch: "main".to_owned(),
                work_branch: "main".to_owned(),
                base: state.root_id().to_owned(),
                status: "queued".to_owned(),
            }
        );
    }

    #[test]
    fn schema_migration_backfills_node_metadata_rows() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();
        let store = SqliteStore::new(&path, StoreAccess::ReadWrite).unwrap();
        let state = super::StoreState::new();
        let root = state.root_node().clone();
        let root_id = root.id.clone();
        let child = Node::new(
            root_id.clone(),
            Role::LLM,
            Some(NodeMetadata::many(vec![
                BackendMetadata {
                    execution_id: Some("execution-migration".to_owned()),
                    call_id: Some("call-a".to_owned()),
                },
                BackendMetadata {
                    execution_id: Some("execution-migration".to_owned()),
                    call_id: Some("call-b".to_owned()),
                },
            ])),
            Kind::Text("legacy metadata".to_owned()),
            "1970-01-01T00:00:01Z".parse().unwrap(),
        );
        let child_id = child.id.clone();

        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            apply_v1_schema_for_test(&mut connection, &store.database_path).await;
            persist_root_metadata(&mut connection, &store.database_path, &root_id)
                .await
                .unwrap();
            insert_v1_node_row(&mut connection, &store.database_path, root).await;
            insert_v1_node_row(&mut connection, &store.database_path, child).await;
        });
        drop(store);

        let crate::store::PersistentStore::Sqlite(migrated) =
            crate::store::PersistentStore::open_read_only_or_upgrade_schema(&path).unwrap();

        assert_eq!(migrated.schema_version().unwrap(), 6);
        assert_eq!(
            node_metadata_rows(&migrated, &child_id),
            vec![
                NodeMetadataRow {
                    node_id: child_id.clone(),
                    ordinal: 0,
                    execution_id: Some("execution-migration".to_owned()),
                    call_id: Some("call-a".to_owned()),
                },
                NodeMetadataRow {
                    node_id: child_id,
                    ordinal: 1,
                    execution_id: Some("execution-migration".to_owned()),
                    call_id: Some("call-b".to_owned()),
                },
            ]
        );
    }

    #[test]
    fn schema_migration_backfills_node_anchor_summary() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();
        let store = SqliteStore::new(&path, StoreAccess::ReadWrite).unwrap();
        let state = super::StoreState::new();
        let root = state.root_node().clone();
        let root_id = root.id.clone();
        let session = Node::new(
            root_id.clone(),
            Role::System,
            None,
            Kind::Anchor(Anchor::session(
                vec![],
                SessionAnchor {
                    role: SessionRole::Orchestrator,
                    provider_profile: Some("migration-profile".to_owned()),
                    provider: Some("anthropic".to_owned()),
                    model: "claude-sonnet-4".to_owned(),
                    tools: vec![],
                    system_prompt: "migration system".to_owned(),
                    prompt: "migration prompt".to_owned(),
                    temperature: Some(0.3),
                    max_tokens: Some(256),
                    additional_params: None,
                    enable_coco_shim: true,
                    active_skill: None,
                },
            )),
            "1970-01-01T00:00:01Z".parse().unwrap(),
        );
        let session_id = session.id.clone();
        let prompt = Node::new(
            root_id.clone(),
            Role::User,
            None,
            Kind::Anchor(Anchor::prompt(
                vec![],
                crate::PromptAnchor {
                    prompt: "migration detached prompt".to_owned(),
                    attachments: vec![],
                },
            )),
            "1970-01-01T00:00:02Z".parse().unwrap(),
        );
        let prompt_id = prompt.id.clone();
        let skill_invocation = Node::new(
            root_id.clone(),
            Role::System,
            None,
            Kind::Anchor(Anchor::skill_invocation(
                vec![],
                crate::SkillInvocationAnchor {
                    skill_name: "migration-review".to_owned(),
                    mode: crate::SkillInvocationMode::Handoff {
                        prompt: "migration handoff prompt".to_owned(),
                    },
                },
            )),
            "1970-01-01T00:00:03Z".parse().unwrap(),
        );
        let skill_invocation_id = skill_invocation.id.clone();
        let skill_result = Node::new(
            root_id.clone(),
            Role::System,
            None,
            Kind::Anchor(Anchor::skill_result(
                vec![],
                crate::SkillResultAnchor {
                    skill_name: "migration-review".to_owned(),
                    output: "migration output".to_owned(),
                },
            )),
            "1970-01-01T00:00:04Z".parse().unwrap(),
        );
        let skill_result_id = skill_result.id.clone();

        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            apply_v1_schema_for_test(&mut connection, &store.database_path).await;
            persist_root_metadata(&mut connection, &store.database_path, &root_id)
                .await
                .unwrap();
            insert_v1_node_row(&mut connection, &store.database_path, root).await;
            insert_v1_node_row(&mut connection, &store.database_path, session).await;
            insert_v1_node_row(&mut connection, &store.database_path, prompt).await;
            insert_v1_node_row(&mut connection, &store.database_path, skill_invocation).await;
            insert_v1_node_row(&mut connection, &store.database_path, skill_result).await;
        });
        drop(store);

        let crate::store::PersistentStore::Sqlite(migrated) =
            crate::store::PersistentStore::open_read_only_or_upgrade_schema(&path).unwrap();

        assert_eq!(migrated.schema_version().unwrap(), 6);
        assert_eq!(
            node_anchor_summary(&migrated, &session_id),
            NodeAnchorSummaryRow {
                anchor_session_role: Some("orchestrator".to_owned()),
                anchor_provider_profile: Some("migration-profile".to_owned()),
                anchor_provider: Some("anthropic".to_owned()),
                anchor_model: Some("claude-sonnet-4".to_owned()),
                anchor_prompt: Some("migration prompt".to_owned()),
                anchor_skill_name: None,
                anchor_skill_invocation_mode: None,
            }
        );
        let session_kind_json = node_kind_json(&migrated, &session_id);
        assert_eq!(
            session_kind_json.pointer("/Anchor/payload/Session/role"),
            None
        );
        assert_eq!(
            session_kind_json.pointer("/Anchor/payload/Session/provider_profile"),
            None
        );
        assert_eq!(
            session_kind_json.pointer("/Anchor/payload/Session/provider"),
            None
        );
        assert_eq!(
            session_kind_json.pointer("/Anchor/payload/Session/model"),
            None
        );
        assert_eq!(
            session_kind_json.pointer("/Anchor/payload/Session/prompt"),
            None
        );
        assert_eq!(
            node_anchor_summary(&migrated, &prompt_id),
            NodeAnchorSummaryRow {
                anchor_session_role: None,
                anchor_provider_profile: None,
                anchor_provider: None,
                anchor_model: None,
                anchor_prompt: Some("migration detached prompt".to_owned()),
                anchor_skill_name: None,
                anchor_skill_invocation_mode: None,
            }
        );
        let prompt_kind_json = node_kind_json(&migrated, &prompt_id);
        assert_eq!(
            prompt_kind_json.pointer("/Anchor/payload/Prompt/prompt"),
            None
        );
        assert_eq!(
            node_anchor_summary(&migrated, &skill_invocation_id),
            NodeAnchorSummaryRow {
                anchor_session_role: None,
                anchor_provider_profile: None,
                anchor_provider: None,
                anchor_model: None,
                anchor_prompt: Some("migration handoff prompt".to_owned()),
                anchor_skill_name: Some("migration-review".to_owned()),
                anchor_skill_invocation_mode: Some("handoff".to_owned()),
            }
        );
        let skill_invocation_kind_json = node_kind_json(&migrated, &skill_invocation_id);
        assert_eq!(
            skill_invocation_kind_json.pointer("/Anchor/payload/SkillInvocation/skill_name"),
            None
        );
        assert_eq!(
            skill_invocation_kind_json.pointer("/Anchor/payload/SkillInvocation/mode/kind"),
            None
        );
        assert_eq!(
            skill_invocation_kind_json.pointer("/Anchor/payload/SkillInvocation/mode/prompt"),
            None
        );
        assert_eq!(
            node_anchor_summary(&migrated, &skill_result_id),
            NodeAnchorSummaryRow {
                anchor_session_role: None,
                anchor_provider_profile: None,
                anchor_provider: None,
                anchor_model: None,
                anchor_prompt: None,
                anchor_skill_name: Some("migration-review".to_owned()),
                anchor_skill_invocation_mode: None,
            }
        );
        let skill_result_kind_json = node_kind_json(&migrated, &skill_result_id);
        assert_eq!(
            skill_result_kind_json.pointer("/Anchor/payload/SkillResult/skill_name"),
            None
        );
    }

    #[test]
    fn persistent_read_only_open_upgrades_sqlite_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();
        let store = SqliteStore::new(&path, StoreAccess::ReadWrite).unwrap();
        let state = super::StoreState::new();
        let root = state.root_node().clone();
        let root_id = root.id.clone();

        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            apply_v1_schema_for_test(&mut connection, &store.database_path).await;
            persist_root_metadata(&mut connection, &store.database_path, &root_id)
                .await
                .unwrap();
            insert_v1_node_row(&mut connection, &store.database_path, root).await;
        });
        drop(store);

        let crate::store::PersistentStore::Sqlite(migrated) =
            crate::store::PersistentStore::open_read_only_or_upgrade_schema(&path).unwrap();

        assert_eq!(migrated.schema_version().unwrap(), 6);
        assert_eq!(migrated.root_id(), root_id);
    }

    #[test]
    fn open_imports_legacy_schema_migration_records() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();
        let store = SqliteStore::new(&path, StoreAccess::ReadWrite).unwrap();
        let state = super::StoreState::new();
        let root = state.root_node().clone();
        let root_id = root.id.clone();

        store.block_on(async {
            let mut connection = store.connect().await.unwrap();
            apply_legacy_v1_schema_for_test(&mut connection, &store.database_path).await;
            persist_root_metadata(&mut connection, &store.database_path, &root_id)
                .await
                .unwrap();
            insert_v1_node_row(&mut connection, &store.database_path, root).await;
        });
        drop(store);

        let migrated = SqliteStore::open(&path).unwrap();

        assert_eq!(migrated.schema_version().unwrap(), 6);
        assert_eq!(migrated.root_id(), root_id);
        migrated.block_on(async {
            let mut connection = migrated.connect().await.unwrap();
            assert_eq!(
                super::table_count(
                    &mut connection,
                    &migrated.database_path,
                    "__diesel_schema_migrations",
                )
                .await
                .unwrap(),
                1
            );
            assert_eq!(
                super::table_count(&mut connection, &migrated.database_path, "node_relations")
                    .await
                    .unwrap(),
                1
            );
        });
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

        assert_eq!(store.schema_version().unwrap(), 6);
    }

    #[test]
    fn open_read_only_rejects_missing_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();

        let err = SqliteStore::open_read_only(&path).unwrap_err();

        assert!(err.to_string().contains("SQLite"));
        assert!(!super::sqlite_database_path(&path).exists());
    }

    #[test]
    fn open_rejects_legacy_json_store_without_creating_database() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("meta.json"), "{}").unwrap();
        std::fs::write(path.join("nodes.jsonl"), "").unwrap();

        let err = SqliteStore::open(&path).unwrap_err();

        assert!(
            matches!(err, crate::StoreError::LegacyJsonStore { path: legacy } if legacy == path)
        );
        assert!(!super::sqlite_database_path(&path).exists());
    }

    #[test]
    fn open_rejects_legacy_json_store_with_unmarked_sqlite_database() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        SqliteStore::open(&path).unwrap();
        std::fs::write(path.join("meta.json"), "{}").unwrap();
        std::fs::write(path.join("nodes.jsonl"), "").unwrap();

        let err = SqliteStore::open(&path).unwrap_err();

        assert!(
            matches!(err, crate::StoreError::LegacyJsonStore { path: legacy } if legacy == path)
        );
    }

    #[test]
    fn open_accepts_legacy_json_store_after_completed_sqlite_migration() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        persist_store_meta_bool_for_test(&store, super::FS_MIGRATION_COMPLETE_META_KEY, true);
        drop(store);
        std::fs::write(path.join("meta.json"), "{}").unwrap();
        std::fs::write(path.join("nodes.jsonl"), "").unwrap();

        let reopened = SqliteStore::open(&path).unwrap();

        assert_eq!(reopened.schema_version().unwrap(), 6);
    }

    #[test]
    fn open_read_only_rejects_legacy_json_store() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("nodes.jsonl"), "").unwrap();

        let err = SqliteStore::open_read_only(&path).unwrap_err();

        assert!(
            matches!(err, crate::StoreError::LegacyJsonStore { path: legacy } if legacy == path)
        );
    }

    #[test]
    fn graph_open_read_only_rejects_missing_schema_without_creating_database() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();

        let err = SqliteGraphStore::open_read_only(&path).unwrap_err();

        assert!(err.to_string().contains("SQLite"));
        assert!(!super::sqlite_database_path(&path).exists());
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
            let count = branches::table
                .filter(branches::name.eq("main"))
                .count()
                .get_result::<i64>(&mut connection)
                .await
                .unwrap();

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
        assert_eq!(
            session_summary(&store, "main"),
            SessionSummaryRow {
                state: "paused".to_owned(),
                target_branch: Some(String::new()),
                base_head_id: None,
                pause_reason: Some("closed".to_owned()),
                merged_anchor_id: None,
            }
        );

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
        assert_eq!(
            job_summary(&store, "job-test"),
            JobSummaryRow {
                created_at: job.created_at.to_string(),
                finished_at: None,
                branch: "main".to_owned(),
                work_branch: "main".to_owned(),
                base: session.clone(),
                status: "running".to_owned(),
            }
        );

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
}
