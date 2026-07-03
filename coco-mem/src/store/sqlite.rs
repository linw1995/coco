use std::collections::{HashMap, HashSet};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use diesel::prelude::*;
use diesel::result::OptionalExtension;
use diesel::sql_types::{Nullable, Text};
use diesel::sqlite::SqliteConnection;
use diesel_async::pooled_connection::bb8::{
    Pool as AsyncSqlitePool, PooledConnection as AsyncSqlitePooledConnection,
};
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, RunQueryDsl, TransactionManager};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use serde_json::{Map, Value};
use snafu::{IntoError, prelude::*};
use tokio::runtime::Runtime;

use super::{
    BranchStore, JobStore, MessageQueueStore, NodeStore, PresetStore, ProcessShareableStore,
    SessionStore, SkillStore,
};
use crate::StoreResult as Result;
use crate::error::{
    AcquireSqliteConnectionSnafu, AmbiguousNodePrefixSnafu, BranchExistsSnafu,
    BranchHeadMovedSnafu, BranchNotFoundSnafu, CorruptedStoreSnafu, CreateSqlitePoolSnafu,
    DuplicateMergeParentSnafu, InvalidAnchorSnafu, InvalidSessionHandoffPromptSnafu,
    InvalidSkillNameSnafu, LegacyJsonStoreSnafu, MergeParentMatchesParentSnafu,
    MissingSessionAnchorSnafu, MultipleShadowParentsSnafu, NotFoundSnafu, ParentNotFoundSnafu,
    ParseSqliteStoreValueSnafu, PresetNotFoundSnafu, PresetVersionNotFoundSnafu,
    PromptJobActiveOnBranchSnafu, PromptJobAlreadyExistsSnafu,
    PromptJobInvalidStatusTransitionSnafu, PromptJobMovedSnafu, PromptJobNotFoundSnafu,
    QuerySqliteStoreSnafu, RefsNotConnectedSnafu, SessionStateMovedSnafu, SkillAlreadyExistsSnafu,
    SkillNotFoundSnafu, SkillUpdateEmptySnafu, SkillVersionNotFoundSnafu, StartSqliteRuntimeSnafu,
    StoreError, StorePathIsNotDirectorySnafu, StoreReadOnlySnafu, WriteStoreDirectorySnafu,
};
use crate::schema::{
    branches, jobs, message_queue_items, node_metadata, node_relations, nodes, presets, sessions,
    skills, store_meta,
};
use crate::{
    Anchor, AnchorPayload, Job, JobStatus, Kind, MergeParent, MessageQueueItem, NewNode, Node,
    NodeMetadata, PauseReason, Preset, PresetRecord, Role, SessionAnchorPatch, SessionRole,
    SessionState, SkillGroups, SkillInvocationMode, SkillRecord, SkillUpdatePatch,
    SkillVersionSpec, default_skill_groups,
};

const SQLITE_DATABASE_FILE_NAME: &str = "store.sqlite3";
const SQLITE_SCHEMA_VERSION: i32 = 6;
const DIESEL_MIGRATION_TABLE_NAME: &str = "__diesel_schema_migrations";
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
    sqlite_master (name) {
        #[sql_name = "type"]
        object_type -> Text,
        name -> Text,
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
    initialization: tokio::sync::Mutex<()>,
}

#[derive(Clone)]
pub struct SqliteStore {
    dir: PathBuf,
    database_path: PathBuf,
    database: SqliteDatabase,
    root_id: String,
    access: StoreAccess,
    _lock_file: Option<Arc<std::fs::File>>,
    _owned_directory: Option<Arc<OwnedStoreDirectory>>,
}

#[derive(Clone)]
pub struct SqliteGraphStore {
    dir: PathBuf,
    database_path: PathBuf,
    database: SqliteDatabase,
    root_id: String,
    read_transaction: Arc<tokio::sync::Mutex<Option<AsyncSqliteConnectionGuard<'static>>>>,
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

struct OwnedStoreDirectory {
    path: PathBuf,
}

impl Drop for OwnedStoreDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
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
    created_at: String,
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
    pub async fn open_store_path(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(sqlite_database_path(path.as_ref()), false).await
    }

    pub async fn open_writable_store_path(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(sqlite_database_path(path.as_ref()), true).await
    }

    pub fn open_unshared_file_path(path: impl AsRef<Path>) -> Result<Self> {
        block_on_sqlite_runtime_with(
            sqlite_runtime()?,
            Self::open_uncached(path.as_ref().to_owned(), true),
        )
    }

    async fn open(database_path: PathBuf, ensure_wal: bool) -> Result<Self> {
        let database_path = sqlite_database_registry_path(&database_path)?;
        let databases = SQLITE_DATABASES.get_or_init(|| Mutex::new(HashMap::new()));
        let existing = {
            let databases = databases
                .lock()
                .expect("SQLite database registry lock poisoned");
            databases
                .get(&database_path)
                .and_then(std::sync::Weak::upgrade)
                .map(|inner| Self { inner })
        };
        if let Some(database) = existing {
            if ensure_wal {
                database.request_wal_journal_mode().await?;
            }
            return Ok(database);
        }

        let runtime = sqlite_runtime()?;
        let ensure_wal_flag = Arc::new(AtomicBool::new(ensure_wal));
        let pool = build_sqlite_pool(&database_path, ensure_wal_flag.clone()).await?;
        let inner = Arc::new(SqliteDatabaseInner {
            database_path: database_path.clone(),
            runtime,
            pool,
            ensure_wal: ensure_wal_flag,
            initialization: tokio::sync::Mutex::new(()),
        });
        let database = {
            let mut databases = databases
                .lock()
                .expect("SQLite database registry lock poisoned");
            if let Some(inner) = databases
                .get(&database_path)
                .and_then(std::sync::Weak::upgrade)
            {
                Self { inner }
            } else {
                databases.insert(database_path, Arc::downgrade(&inner));
                Self { inner }
            }
        };
        if ensure_wal {
            database.request_wal_journal_mode().await?;
        }
        Ok(database)
    }

    async fn open_uncached(database_path: PathBuf, ensure_wal: bool) -> Result<Self> {
        let database_path = sqlite_database_registry_path(&database_path)?;
        let runtime = sqlite_runtime()?;
        let ensure_wal_flag = Arc::new(AtomicBool::new(ensure_wal));
        let pool = build_sqlite_pool(&database_path, ensure_wal_flag.clone()).await?;
        let database = Self {
            inner: Arc::new(SqliteDatabaseInner {
                database_path: database_path.clone(),
                runtime,
                pool,
                ensure_wal: ensure_wal_flag,
                initialization: tokio::sync::Mutex::new(()),
            }),
        };
        if ensure_wal {
            database.request_wal_journal_mode().await?;
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
        let result = self.block_on(self.with_sync_connection_in_sqlite(
            operation,
            map_pool_error,
            map_connection_error,
        ));
        match result {
            Ok(result) => result,
            Err(error) => Err(error),
        }
    }

    async fn with_sync_connection_in_sqlite<T, E, F, P, M>(
        &self,
        operation: F,
        map_pool_error: P,
        map_connection_error: M,
    ) -> std::result::Result<std::result::Result<T, E>, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(&mut SqliteConnection) -> std::result::Result<T, E> + Send + 'static,
        P: FnOnce(StoreError) -> E + Send,
        M: FnOnce(diesel::result::Error) -> E + Send,
    {
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

    async fn request_wal_journal_mode(&self) -> Result<()> {
        self.inner.ensure_wal.store(true, Ordering::SeqCst);
        let mut connection = self.connection().await?;
        ensure_wal_journal_mode(&mut connection, &self.inner.database_path).await
    }

    async fn with_initialization_lock<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let _guard = self.inner.initialization.lock().await;
        operation().await
    }
}

impl SqliteStore {
    pub async fn open_read_only_or_upgrade_schema(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_read_only_or_upgrade_schema_in_sqlite(path.as_ref()).await
    }

    async fn open_read_only_or_upgrade_schema_in_sqlite(path: &Path) -> Result<Self> {
        if sqlite_database_path(path).is_file() {
            if Self::sqlite_schema_requires_migration(path).await? {
                drop(Self::open_in_sqlite(path).await?);
            }
            return Self::open_read_only_in_sqlite(path).await;
        }
        Self::open_read_only_in_sqlite(path).await
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        block_on_sqlite_runtime_with(sqlite_runtime()?, Self::open_in_sqlite(path))
    }

    async fn open_in_sqlite(path: &Path) -> Result<Self> {
        prepare_store_directory(path)?;
        reject_incomplete_legacy_json_store(path).await?;
        let mut store = Self::new(path, StoreAccess::ReadWrite).await?;
        let root_id = store
            .database
            .with_initialization_lock(|| async {
                store.run_migrations().await?;
                store.load_or_initialize_state().await
            })
            .await?;
        store.root_id = root_id;
        Ok(store)
    }

    pub fn open_temporary() -> Result<Self> {
        let directory = Arc::new(create_temporary_store_directory());
        let mut store = Self::open(&directory.path)?;
        store._owned_directory = Some(directory);
        Ok(store)
    }

    pub async fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_read_only_in_sqlite(path.as_ref()).await
    }

    async fn open_read_only_in_sqlite(path: &Path) -> Result<Self> {
        ensure_existing_store_directory(path)?;
        reject_incomplete_legacy_json_store(path).await?;
        ensure_existing_database_file(&sqlite_database_path(path))?;
        let mut store = Self::new(path, StoreAccess::ReadOnly).await?;
        let root_id = store
            .database
            .with_initialization_lock(|| async {
                store.ensure_current_schema().await?;
                store.ensure_root_exists().await
            })
            .await?;
        store.root_id = root_id;
        Ok(store)
    }

    pub fn store_path(&self) -> &Path {
        &self.dir
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub async fn schema_version(&self) -> Result<i32> {
        let mut connection = self.connect().await?;
        existing_schema_version(&mut connection, &self.database_path).await
    }

    async fn new(path: &Path, access: StoreAccess) -> Result<Self> {
        Self::new_with_database_path(path, path.join(SQLITE_DATABASE_FILE_NAME), access).await
    }

    async fn new_with_database_path(
        path: &Path,
        database_path: PathBuf,
        access: StoreAccess,
    ) -> Result<Self> {
        let database = match access {
            StoreAccess::ReadWrite => SqliteDatabase::open(database_path.clone(), true).await?,
            StoreAccess::ReadOnly => SqliteDatabase::open(database_path.clone(), false).await?,
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
            root_id: String::new(),
            access,
            _lock_file: lock_file,
            _owned_directory: None,
        })
    }

    async fn run_migrations(&self) -> Result<()> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        configure_writable_connection(&mut connection, &self.database_path).await?;
        reject_unsupported_schema_version(&mut connection, &self.database_path).await?;
        run_embedded_migrations(&mut connection, &self.database_path).await?;
        Ok(())
    }

    async fn ensure_current_schema(&self) -> Result<()> {
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
    }

    async fn sqlite_schema_requires_migration(path: &Path) -> Result<bool> {
        ensure_existing_store_directory(path)?;
        let store = Self::new(path, StoreAccess::ReadOnly).await?;
        ensure_existing_database_file(&store.database_path)?;
        let version = store
            .database
            .with_initialization_lock(|| async {
                let mut connection = store.connect().await?;
                existing_schema_version(&mut connection, &store.database_path).await
            })
            .await?;
        ensure!(
            version == SQLITE_SCHEMA_VERSION,
            CorruptedStoreSnafu {
                path: store.database_path,
                message: format!(
                    "unsupported SQLite schema version {version}, expected {SQLITE_SCHEMA_VERSION}"
                ),
            }
        );
        Ok(false)
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

    async fn load_or_initialize_state(&self) -> Result<String> {
        let mut connection = self.connect().await?;
        if node_count(&mut connection, &self.database_path).await? == 0 {
            self.ensure_writable()?;
            let root = initial_root_node();
            let root_id = root.id.clone();
            connection
                .immediate_transaction::<(), SqliteTransactionError, _>(async |connection| {
                    persist_root_metadata(connection, &self.database_path, &root_id)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    persist_node_without_transaction(connection, &self.database_path, &root)
                        .await
                        .map_err(SqliteTransactionError::Operation)
                })
                .await
                .map_err(|error| error.into_store_error(&self.database_path))?;
            return Ok(root_id);
        }
        load_root_id(&mut connection, &self.database_path).await
    }

    async fn ensure_root_exists(&self) -> Result<String> {
        let mut connection = self.connect().await?;
        let root_id = load_root_id(&mut connection, &self.database_path).await?;
        load_node_by_exact_id(&mut connection, &self.database_path, &root_id).await?;
        Ok(root_id)
    }

    async fn connect(&self) -> Result<AsyncSqliteConnectionGuard<'_>> {
        self.database.connection().await
    }

    async fn append_in_sqlite(&self, node: NewNode) -> Result<String> {
        self.ensure_writable()?;
        let node = Node::new(
            node.parent,
            node.role,
            node.metadata,
            node.kind,
            jiff::Timestamp::now(),
        );
        let mut connection = self.connect().await?;
        validate_new_node(&mut connection, &self.database_path, &node).await?;
        persist_node(&mut connection, &self.database_path, &node).await?;
        Ok(node.id)
    }

    async fn ancestry_in_sqlite(&self, head_ref: &str) -> Result<Vec<Node>> {
        let mut connection = self.connect().await?;
        load_ancestry_nodes(&mut connection, &self.database_path, head_ref).await
    }

    async fn log_in_sqlite(&self, base_ref: &str, head_ref: &str) -> Result<Vec<Node>> {
        let mut connection = self.connect().await?;
        load_log_nodes(&mut connection, &self.database_path, base_ref, head_ref).await
    }

    async fn get_node_in_sqlite(&self, id: &str) -> Result<Node> {
        let mut connection = self.connect().await?;
        load_node_by_prefix_or_branch(&mut connection, &self.database_path, id).await
    }

    async fn list_children_in_sqlite(&self, node_id: &str) -> Result<Vec<Node>> {
        let mut connection = self.connect().await?;
        load_child_nodes(&mut connection, &self.database_path, node_id).await
    }

    async fn fork_in_sqlite(&self, name: &str, from_ref: &str) -> Result<String> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        create_branch(&mut connection, &self.database_path, name, from_ref).await
    }

    async fn get_branch_head_in_sqlite(&self, name: &str) -> Result<String> {
        let mut connection = self.connect().await?;
        load_branch_head(&mut connection, &self.database_path, name).await
    }

    async fn delete_branch_in_sqlite(&self, name: &str) -> Result<()> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        delete_branch_checked(&mut connection, &self.database_path, name).await
    }

    async fn set_branch_head_in_sqlite(
        &self,
        name: &str,
        expected_old_head: &str,
        new_head: &str,
    ) -> Result<()> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        update_branch_head_checked(
            &mut connection,
            &self.database_path,
            name,
            expected_old_head,
            new_head,
        )
        .await
    }

    async fn list_session_states_in_sqlite(
        &self,
    ) -> Result<std::collections::HashMap<String, SessionState>> {
        let mut connection = self.connect().await?;
        load_session_states(&mut connection, &self.database_path).await
    }

    async fn get_session_state_in_sqlite(&self, name: &str) -> Result<SessionState> {
        let mut connection = self.connect().await?;
        load_session_state(&mut connection, &self.database_path, name).await
    }

    async fn set_session_state_in_sqlite(
        &self,
        name: &str,
        expected: Option<&SessionState>,
        next: SessionState,
    ) -> Result<SessionState> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        update_session_state(&mut connection, &self.database_path, name, expected, next).await
    }

    async fn rebase_session_in_sqlite(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
    ) -> Result<String> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        let (new_head, _) =
            rebase_session_in_sqlite(&mut connection, &self.database_path, name, patch).await?;
        Ok(new_head)
    }

    async fn handoff_session_in_sqlite(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
        prompt: &str,
    ) -> Result<String> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        let (new_head, _) =
            handoff_session_in_sqlite(&mut connection, &self.database_path, name, patch, prompt)
                .await?;
        Ok(new_head)
    }

    async fn submit_job_in_sqlite(&self, branch: &str, base: &str) -> Result<Job> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        submit_job_in_sqlite(&mut connection, &self.database_path, branch, base).await
    }

    async fn submit_job_with_id_in_sqlite(
        &self,
        job_id: &str,
        branch: &str,
        base: &str,
    ) -> Result<Job> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        submit_job_with_id_in_sqlite(&mut connection, &self.database_path, job_id, branch, base)
            .await
    }

    async fn get_job_in_sqlite(&self, job_id: &str) -> Result<Job> {
        let mut connection = self.connect().await?;
        load_job(&mut connection, &self.database_path, job_id).await
    }

    async fn list_jobs_in_sqlite(&self) -> Result<std::collections::HashMap<String, Job>> {
        let mut connection = self.connect().await?;
        load_job_map(&mut connection, &self.database_path).await
    }

    async fn set_job_status_in_sqlite(
        &self,
        job_id: &str,
        expected: JobStatus,
        next: JobStatus,
    ) -> Result<Job> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        update_job_status_in_sqlite(&mut connection, &self.database_path, job_id, expected, next)
            .await
    }

    async fn set_job_work_branch_in_sqlite(
        &self,
        job_id: &str,
        expected_work_branch: &str,
        next_work_branch: &str,
    ) -> Result<Job> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        update_job_work_branch_in_sqlite(
            &mut connection,
            &self.database_path,
            job_id,
            expected_work_branch,
            next_work_branch,
        )
        .await
    }

    async fn enqueue_message_in_sqlite(
        &self,
        queue: &str,
        payload: serde_json::Value,
    ) -> Result<MessageQueueItem> {
        self.ensure_writable()?;
        let item = MessageQueueItem::new(queue, payload, jiff::Timestamp::now());
        let mut connection = self.connect().await?;
        persist_message_queue_item(&mut connection, &self.database_path, &item).await?;
        Ok(item)
    }

    async fn dequeue_message_in_sqlite(&self, queue: &str) -> Result<Option<MessageQueueItem>> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        dequeue_message_queue_item(&mut connection, &self.database_path, queue).await
    }

    async fn peek_message_in_sqlite(&self, queue: &str) -> Result<Option<MessageQueueItem>> {
        let mut connection = self.connect().await?;
        Ok(
            load_queue_messages(&mut connection, &self.database_path, queue)
                .await?
                .into_iter()
                .next(),
        )
    }

    async fn list_queue_messages_in_sqlite(&self, queue: &str) -> Result<Vec<MessageQueueItem>> {
        let mut connection = self.connect().await?;
        load_queue_messages(&mut connection, &self.database_path, queue).await
    }

    async fn list_message_queues_in_sqlite(&self) -> Result<Vec<String>> {
        let mut connection = self.connect().await?;
        load_message_queue_names(&mut connection, &self.database_path).await
    }

    async fn list_preset_records_in_sqlite(
        &self,
    ) -> Result<std::collections::HashMap<String, PresetRecord>> {
        let mut connection = self.connect().await?;
        load_preset_records(&mut connection, &self.database_path).await
    }

    async fn get_preset_record_in_sqlite(&self, name: &str) -> Result<PresetRecord> {
        let mut connection = self.connect().await?;
        load_preset_record(&mut connection, &self.database_path, name).await
    }

    async fn set_preset_in_sqlite(&self, name: &str, config: Preset) -> Result<PresetRecord> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        set_preset_record(&mut connection, &self.database_path, name, config).await
    }

    async fn rollback_preset_in_sqlite(
        &self,
        name: &str,
        target_version: u64,
    ) -> Result<PresetRecord> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        rollback_preset_record(&mut connection, &self.database_path, name, target_version).await
    }

    async fn delete_preset_in_sqlite(&self, name: &str) -> Result<()> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        delete_preset_record_checked(&mut connection, &self.database_path, name).await
    }

    async fn list_skills_in_sqlite(&self, role: SessionRole) -> Result<Vec<SkillRecord>> {
        let mut connection = self.connect().await?;
        Ok(load_skill_groups(&mut connection, &self.database_path)
            .await?
            .for_role(role)
            .values()
            .cloned()
            .collect())
    }

    async fn get_skill_in_sqlite(&self, role: SessionRole, name: &str) -> Result<SkillRecord> {
        let mut connection = self.connect().await?;
        load_skill_record(&mut connection, &self.database_path, role, name).await
    }

    async fn add_skill_in_sqlite(
        &self,
        role: SessionRole,
        name: &str,
        spec: SkillVersionSpec,
    ) -> Result<SkillRecord> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        add_skill_record(&mut connection, &self.database_path, role, name, spec).await
    }

    async fn update_skill_in_sqlite(
        &self,
        role: SessionRole,
        name: &str,
        patch: &SkillUpdatePatch,
    ) -> Result<SkillRecord> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        update_skill_record(&mut connection, &self.database_path, role, name, patch).await
    }

    async fn rollback_skill_in_sqlite(
        &self,
        role: SessionRole,
        name: &str,
        target_version: u64,
    ) -> Result<SkillRecord> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        rollback_skill_record(
            &mut connection,
            &self.database_path,
            role,
            name,
            target_version,
        )
        .await
    }
}

impl SqliteGraphStore {
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        block_on_sqlite_runtime_with(sqlite_runtime()?, Self::open_read_only_in_sqlite(path))
    }

    async fn open_read_only_in_sqlite(path: &Path) -> Result<Self> {
        ensure_existing_store_directory(path)?;
        ensure_existing_database_file(&sqlite_database_path(path))?;
        let store = Self::new(path).await?;
        let root_id = store
            .database
            .with_initialization_lock(|| async {
                store.ensure_current_schema().await?;
                let mut connection = store.connect().await?;
                load_root_id(&mut connection, &store.database_path).await
            })
            .await?;
        Ok(Self { root_id, ..store })
    }

    pub fn store_path(&self) -> &Path {
        &self.dir
    }

    async fn new(path: &Path) -> Result<Self> {
        let database_path = sqlite_database_path(path);
        let database = SqliteDatabase::open(database_path.clone(), false).await?;
        Ok(Self {
            dir: path.to_owned(),
            database_path,
            database,
            root_id: String::new(),
            read_transaction: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    async fn ensure_current_schema(&self) -> Result<()> {
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

    async fn with_connection<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send,
        F: for<'a> FnOnce(&'a mut AsyncSqliteConnection) -> SqliteGraphConnectionFuture<'a, T>
            + Send,
    {
        let mut read_transaction = self.read_transaction.lock().await;
        if let Some(connection) = read_transaction.as_mut() {
            return operation(&mut *connection).await;
        }
        drop(read_transaction);

        let mut connection = self.connect().await?;
        operation(&mut connection).await
    }

    pub fn begin_read_transaction(&self) -> Result<()> {
        self.block_on(self.begin_read_transaction_in_sqlite())
    }

    async fn begin_read_transaction_in_sqlite(&self) -> Result<()> {
        let transaction_is_inactive = self.read_transaction.lock().await.is_none();
        ensure!(
            transaction_is_inactive,
            CorruptedStoreSnafu {
                path: self.database_path.clone(),
                message: "SQLite graph read transaction already active".to_owned(),
            }
        );

        let mut connection =
            self.database
                .inner
                .pool
                .get_owned()
                .await
                .context(AcquireSqliteConnectionSnafu {
                    path: self.database_path.clone(),
                })?;
        begin_deferred_transaction(&mut connection, &self.database_path).await?;
        let mut connection = Some(connection);
        let transaction_already_active = {
            let mut read_transaction = self.read_transaction.lock().await;
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
    }

    pub fn commit_read_transaction(&self) -> Result<()> {
        self.block_on(self.commit_read_transaction_in_sqlite())
    }

    async fn commit_read_transaction_in_sqlite(&self) -> Result<()> {
        let mut connection =
            self.read_transaction
                .lock()
                .await
                .take()
                .context(CorruptedStoreSnafu {
                    path: self.database_path.clone(),
                    message: "SQLite graph read transaction is not active".to_owned(),
                })?;

        commit_deferred_transaction(&mut connection, &self.database_path).await
    }

    pub fn rollback_read_transaction(&self) -> Result<()> {
        self.block_on(self.rollback_read_transaction_in_sqlite())
    }

    async fn rollback_read_transaction_in_sqlite(&self) -> Result<()> {
        let Some(mut connection) = self.read_transaction.lock().await.take() else {
            return Ok(());
        };

        rollback_deferred_transaction(&mut connection, &self.database_path).await
    }

    fn ensure_read_only<T>(&self) -> Result<T> {
        StoreReadOnlySnafu {
            path: self.dir.clone(),
        }
        .fail()
    }

    async fn branch_head(&self, name: &str) -> Result<Option<String>> {
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
        .await
    }

    async fn get_branch_head_in_sqlite(&self, name: &str) -> Result<String> {
        self.branch_head(name).await?.context(BranchNotFoundSnafu {
            name: name.to_owned(),
        })
    }

    async fn get_node_by_prefix_or_branch(&self, reference: &str) -> Result<Node> {
        let reference = reference.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(
                async move { load_node_by_prefix_or_branch(connection, &path, &reference).await },
            )
        })
        .await
    }

    async fn ancestry_in_sqlite(&self, head_ref: &str) -> Result<Vec<Node>> {
        let head_ref = head_ref.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move { load_ancestry_nodes(connection, &path, &head_ref).await })
        })
        .await
    }

    async fn log_in_sqlite(&self, base_ref: &str, head_ref: &str) -> Result<Vec<Node>> {
        let base_ref = base_ref.to_owned();
        let head_ref = head_ref.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move { load_log_nodes(connection, &path, &base_ref, &head_ref).await })
        })
        .await
    }

    async fn list_children_in_sqlite(&self, node_id: &str) -> Result<Vec<Node>> {
        let node_id = node_id.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move { load_child_nodes(connection, &path, &node_id).await })
        })
        .await
    }

    async fn list_session_states_in_sqlite(
        &self,
    ) -> Result<std::collections::HashMap<String, SessionState>> {
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move { load_session_states(connection, &path).await })
        })
        .await
    }

    async fn get_session_state_in_sqlite(&self, name: &str) -> Result<SessionState> {
        let name = name.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move { load_session_state(connection, &path, &name).await })
        })
        .await
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

fn initial_root_node() -> Node {
    Node::new(
        String::new(),
        Role::System,
        None,
        Kind::Text("The Big Bang".to_owned()),
        "1970-01-01T00:00:00Z"
            .parse()
            .expect("root timestamp should parse"),
    )
}

fn create_temporary_store_directory() -> OwnedStoreDirectory {
    let base = std::env::temp_dir();
    loop {
        let path = base.join(format!("coco-mem-{}", nanoid::nanoid!()));
        match std::fs::create_dir(&path) {
            Ok(()) => return OwnedStoreDirectory { path },
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => panic!(
                "failed to create temporary SQLite store at {:?}: {error}",
                path
            ),
        }
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

async fn reject_incomplete_legacy_json_store(path: &Path) -> Result<()> {
    let has_legacy_marker = LEGACY_JSON_STORE_MARKERS
        .iter()
        .any(|file_name| path.join(file_name).exists());
    if !has_legacy_marker {
        return Ok(());
    }

    let database_path = sqlite_database_path(path);
    if database_path.is_file() && fs_migration_complete_marker_exists(path).await? {
        return Ok(());
    }

    LegacyJsonStoreSnafu {
        path: path.to_owned(),
    }
    .fail()
}

async fn fs_migration_complete_marker_exists(path: &Path) -> Result<bool> {
    let store = SqliteStore::new(path, StoreAccess::ReadOnly).await?;
    let mut connection = store.connect().await?;
    load_store_meta_bool(
        &mut connection,
        &store.database_path,
        FS_MIGRATION_COMPLETE_META_KEY,
    )
    .await
}

async fn build_sqlite_pool(
    database_path: &Path,
    ensure_wal: Arc<AtomicBool>,
) -> Result<AsyncSqlitePool<AsyncSqliteConnection>> {
    let manager = AsyncDieselConnectionManager::<AsyncSqliteConnection>::new_with_config(
        database_path.to_string_lossy().into_owned(),
        sqlite_pool_manager_config(database_path.to_owned(), ensure_wal),
    );
    AsyncSqlitePool::builder()
        .max_size(SQLITE_POOL_MAX_SIZE)
        .build(manager)
        .await
        .context(CreateSqlitePoolSnafu {
            path: database_path.to_owned(),
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
    SqliteConnectionPragma::ForeignKeysOn
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    SqliteConnectionPragma::BusyTimeout5000
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
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
    SqliteConnectionPragma::JournalModeWal
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum SqliteConnectionPragma {
    ForeignKeysOn,
    BusyTimeout5000,
    JournalModeWal,
}

impl diesel::query_builder::QueryId for SqliteConnectionPragma {
    type QueryId = Self;

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl diesel::query_builder::QueryFragment<diesel::sqlite::Sqlite> for SqliteConnectionPragma {
    fn walk_ast<'b>(
        &'b self,
        mut out: diesel::query_builder::AstPass<'_, 'b, diesel::sqlite::Sqlite>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();
        match self {
            Self::ForeignKeysOn => out.push_sql("PRAGMA foreign_keys = ON"),
            Self::BusyTimeout5000 => out.push_sql("PRAGMA busy_timeout = 5000"),
            Self::JournalModeWal => out.push_sql("PRAGMA journal_mode = WAL"),
        }
        Ok(())
    }
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

async fn existing_schema_version(
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

async fn reject_unsupported_schema_version(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    let Some(version) = current_schema_version(connection, path).await? else {
        return Ok(());
    };
    ensure!(
        version == SQLITE_SCHEMA_VERSION,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "unsupported SQLite schema version {version}, expected {SQLITE_SCHEMA_VERSION}"
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

async fn load_node_by_exact_id(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    id: &str,
) -> Result<Node> {
    let row = nodes::table
        .filter(nodes::id.eq(id))
        .select(node_row_columns!())
        .get_result::<NodeRow>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?
        .context(NotFoundSnafu { id: id.to_owned() })?;
    let node_id = row.id.clone();
    let metadata_rows =
        load_node_metadata_rows_for_ids(connection, path, Some(std::slice::from_ref(&node_id)))
            .await?;
    row.into_node(path, node_metadata_slice(&metadata_rows, &node_id))
}

async fn node_exists_by_id(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    id: &str,
) -> Result<bool> {
    let count = nodes::table
        .filter(nodes::id.eq(id))
        .count()
        .get_result::<i64>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(count > 0)
}

async fn load_node_by_prefix_or_branch(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    reference: &str,
) -> Result<Node> {
    if let Some(head_id) = maybe_load_branch_head(connection, path, reference).await? {
        return load_node_by_exact_id(connection, path, &head_id).await;
    }

    match load_node_by_exact_id(connection, path, reference).await {
        Ok(node) => Ok(node),
        Err(crate::StoreError::NotFound { .. }) => {
            load_node_by_prefix(connection, path, reference).await
        }
        Err(error) => Err(error),
    }
}

async fn load_node_by_prefix(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    prefix: &str,
) -> Result<Node> {
    match load_node_ids_by_prefix(connection, path, prefix)
        .await?
        .as_slice()
    {
        [matched] => load_node_by_exact_id(connection, path, matched).await,
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

async fn load_node_ids_by_prefix(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    prefix: &str,
) -> Result<Vec<String>> {
    nodes::table
        .filter(nodes::id.like(format!("{prefix}%")))
        .select(nodes::id)
        .order(nodes::id)
        .load::<String>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

async fn resolve_ref_id(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    reference: &str,
) -> Result<String> {
    if node_exists_by_id(connection, path, reference).await? {
        return Ok(reference.to_owned());
    }
    if let Some(head_id) = maybe_load_branch_head(connection, path, reference).await? {
        load_node_by_exact_id(connection, path, &head_id).await?;
        return Ok(head_id);
    }

    if is_node_id(reference) {
        return NotFoundSnafu {
            id: reference.to_owned(),
        }
        .fail();
    }
    BranchNotFoundSnafu {
        name: reference.to_owned(),
    }
    .fail()
}

async fn load_ancestry_nodes(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    head_ref: &str,
) -> Result<Vec<Node>> {
    let mut current_id = resolve_ref_id(connection, path, head_ref).await?;
    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    loop {
        ensure!(
            seen.insert(current_id.clone()),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: "SQLite nodes contain cyclic parents".to_owned(),
            }
        );
        let row = nodes::table
            .filter(nodes::id.eq(&current_id))
            .select(node_row_columns!())
            .get_result::<NodeRow>(connection)
            .await
            .optional()
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?
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
    node_rows_into_nodes(connection, path, rows).await
}

async fn load_log_nodes(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    base_ref: &str,
    head_ref: &str,
) -> Result<Vec<Node>> {
    let base_id = resolve_ref_id(connection, path, base_ref).await?;
    let mut nodes = load_ancestry_nodes(connection, path, head_ref).await?;
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

async fn load_child_nodes(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_id: &str,
) -> Result<Vec<Node>> {
    load_node_by_exact_id(connection, path, node_id).await?;
    let rows = node_relations::table
        .inner_join(nodes::table.on(nodes::id.eq(node_relations::child_node_id)))
        .filter(node_relations::parent_node_id.eq(node_id))
        .select(node_row_columns!())
        .order((nodes::created_at, nodes::id))
        .load::<NodeRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    node_rows_into_nodes(connection, path, rows).await
}

async fn validate_new_node(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    ensure!(
        node_exists_by_id(connection, path, &node.parent).await?,
        ParentNotFoundSnafu {
            id: node.parent.clone(),
        }
    );
    validate_anchor_merge_parents(connection, path, &node.parent, &node.kind).await
}

async fn validate_anchor_merge_parents(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    parent: &str,
    kind: &Kind,
) -> Result<()> {
    let Kind::Anchor(anchor) = kind else {
        return Ok(());
    };

    let mut seen = HashSet::new();
    let mut shadow_parents = Vec::new();
    for merge_parent in anchor.merge_parents() {
        let node_id = merge_parent.node_id();
        ensure!(
            node_id != parent,
            MergeParentMatchesParentSnafu {
                id: node_id.to_owned(),
            }
        );
        ensure!(
            seen.insert(node_id),
            DuplicateMergeParentSnafu {
                id: node_id.to_owned(),
            }
        );
        ensure!(
            node_exists_by_id(connection, path, node_id).await?,
            ParentNotFoundSnafu {
                id: node_id.to_owned(),
            }
        );
        if merge_parent.is_shadow() {
            shadow_parents.push(node_id.to_owned());
        }
    }
    ensure!(
        shadow_parents.len() <= 1,
        MultipleShadowParentsSnafu {
            ids: shadow_parents,
        }
    );

    Ok(())
}

fn is_node_id(reference: &str) -> bool {
    reference.len() == 64 && reference.bytes().all(|byte| byte.is_ascii_hexdigit())
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

async fn load_session_states(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<HashMap<String, SessionState>> {
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
    sessions
        .into_iter()
        .map(|session| session_row_into_state(path, session))
        .collect()
}

async fn load_session_state(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    name: &str,
) -> Result<SessionState> {
    let session = sessions::table
        .filter(sessions::branch_name.eq(name))
        .select((
            sessions::branch_name,
            sessions::state,
            sessions::target_branch,
            sessions::base_head_id,
            sessions::pause_reason,
            sessions::merged_anchor_id,
            sessions::state_json,
        ))
        .get_result::<SessionRow>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?
        .context(BranchNotFoundSnafu {
            name: name.to_owned(),
        })?;
    let (_, state) = session_row_into_state(path, session)?;
    Ok(state)
}

fn session_row_into_state(path: &Path, session: SessionRow) -> Result<(String, SessionState)> {
    let state = serde_json::from_str::<SessionState>(&session.state_json).context(
        ParseSqliteStoreValueSnafu {
            path: path.to_owned(),
            column: "sessions.state_json".to_owned(),
        },
    )?;
    validate_session_row_summary(&session, &state, path)?;
    Ok((session.branch_name, state))
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

async fn create_branch(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    from_ref: &str,
) -> Result<String> {
    connection
        .immediate_transaction::<String, SqliteTransactionError, _>(async |connection| {
            if maybe_load_branch_head(connection, path, branch)
                .await
                .map_err(SqliteTransactionError::Operation)?
                .is_some()
            {
                return Err(SqliteTransactionError::Operation(
                    BranchExistsSnafu {
                        name: branch.to_owned(),
                    }
                    .build(),
                ));
            }
            let head_id = resolve_ref_id(connection, path, from_ref)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            persist_branch(connection, path, branch, &head_id)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            persist_session_state(connection, path, branch, &SessionState::Active)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(head_id)
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

async fn update_session_state(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected: Option<&SessionState>,
    next: SessionState,
) -> Result<SessionState> {
    connection
        .immediate_transaction::<SessionState, SqliteTransactionError, _>(async |connection| {
            let current = load_session_state(connection, path, branch)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            if let Some(expected) = expected
                && current != *expected
            {
                return Err(SqliteTransactionError::Operation(
                    SessionStateMovedSnafu {
                        name: branch.to_owned(),
                        expected: format!("{expected:?}"),
                        actual: format!("{current:?}"),
                    }
                    .build(),
                ));
            }
            validate_session_state(connection, path, &next)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            persist_session_state(connection, path, branch, &next)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(next)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn rebase_session_in_sqlite(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    patch: &SessionAnchorPatch,
) -> Result<(String, Vec<Node>)> {
    connection
        .immediate_transaction::<(String, Vec<Node>), SqliteTransactionError, _>(
            async |connection| {
                let expected_old_head = load_branch_head(connection, path, branch)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                let mut chain = load_session_chain(connection, path, branch)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                chain.reverse();
                let session_node = chain
                    .as_slice()
                    .first()
                    .expect("session chain should not be empty");
                let session_anchor = session_anchor_from_node(path, session_node)
                    .map_err(SqliteTransactionError::Operation)?;
                let rebased_session_anchor = session_anchor.apply_patch(patch);

                let mut previous_new_id = None;
                let mut new_head = String::new();
                let mut nodes = Vec::with_capacity(chain.len());
                for (index, node) in chain.into_iter().enumerate() {
                    let parent = previous_new_id
                        .clone()
                        .unwrap_or_else(|| node.parent.clone());
                    let kind = if index == 0 {
                        let Kind::Anchor(anchor) = &node.kind else {
                            unreachable!("session chain should start with anchor");
                        };
                        Kind::Anchor(Anchor::session(
                            anchor.merge_parents().to_vec(),
                            rebased_session_anchor.clone(),
                        ))
                    } else {
                        node.kind.clone()
                    };
                    let new_node =
                        Node::new(parent, node.role, node.metadata, kind, node.created_at);
                    upsert_node_without_transaction(connection, path, &new_node)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    previous_new_id = Some(new_node.id.clone());
                    new_head = new_node.id.clone();
                    nodes.push(new_node);
                }

                update_branch_head_after_session_write(
                    connection,
                    path,
                    branch,
                    &expected_old_head,
                    &new_head,
                )
                .await
                .map_err(SqliteTransactionError::Operation)?;
                Ok((new_head, nodes))
            },
        )
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn handoff_session_in_sqlite(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    patch: &SessionAnchorPatch,
    prompt: &str,
) -> Result<(String, Node)> {
    let prompt = prompt.trim().to_owned();
    ensure!(!prompt.is_empty(), InvalidSessionHandoffPromptSnafu);
    connection
        .immediate_transaction::<(String, Node), SqliteTransactionError, _>(async |connection| {
            let expected_old_head = load_branch_head(connection, path, branch)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            let chain = load_session_chain(connection, path, branch)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            let session_node = chain.last().expect("session chain should not be empty");
            let session_anchor = session_anchor_from_node(path, session_node)
                .map_err(SqliteTransactionError::Operation)?;
            let mut handoff_session_anchor = session_anchor.apply_patch(patch);
            handoff_session_anchor.prompt = prompt;

            let node = Node::new(
                expected_old_head.clone(),
                Role::System,
                None,
                Kind::Anchor(Anchor::session(vec![], handoff_session_anchor)),
                jiff::Timestamp::now(),
            );
            validate_new_node(connection, path, &node)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            persist_node_without_transaction(connection, path, &node)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            update_branch_head_after_session_write(
                connection,
                path,
                branch,
                &expected_old_head,
                &node.id,
            )
            .await
            .map_err(SqliteTransactionError::Operation)?;
            Ok((node.id.clone(), node))
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn load_session_chain(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
) -> Result<Vec<Node>> {
    let ancestry = load_ancestry_nodes(connection, path, branch).await?;
    let mut chain = Vec::new();
    for node in ancestry {
        let is_session_anchor = matches!(
            node.kind,
            Kind::Anchor(Anchor {
                payload: AnchorPayload::Session(_),
                ..
            })
        );
        chain.push(node);
        if is_session_anchor {
            return Ok(chain);
        }
    }
    MissingSessionAnchorSnafu {
        branch: branch.to_owned(),
    }
    .fail()
}

fn session_anchor_from_node(path: &Path, node: &Node) -> Result<crate::SessionAnchor> {
    match &node.kind {
        Kind::Anchor(anchor) => anchor.as_session().cloned().context(CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "session chain should end with session anchor".to_owned(),
        }),
        _ => CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "session chain should end with anchor".to_owned(),
        }
        .fail(),
    }
}

async fn update_branch_head_after_session_write(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected_old_head: &str,
    new_head: &str,
) -> Result<()> {
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

async fn validate_session_state(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    state: &SessionState,
) -> Result<()> {
    match state {
        SessionState::Active => Ok(()),
        SessionState::Attached {
            target_branch,
            base_head_id,
        } => validate_ref_on_branch(connection, path, target_branch, base_head_id).await,
        SessionState::Paused {
            target_branch,
            reason,
        } => match reason {
            PauseReason::Merged { merged_anchor_id } => {
                validate_anchor_on_branch(connection, path, target_branch, merged_anchor_id).await
            }
            PauseReason::Closed => {
                if target_branch.is_empty() {
                    return Ok(());
                }
                load_branch_head(connection, path, target_branch).await?;
                Ok(())
            }
        },
    }
}

async fn validate_ref_on_branch(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    node_id: &str,
) -> Result<()> {
    let head_id = load_branch_head(connection, path, branch).await?;
    load_node_by_exact_id(connection, path, node_id).await?;
    let visible = node_reachable_from_head(connection, path, &head_id, node_id).await?;
    ensure!(
        visible,
        RefsNotConnectedSnafu {
            base_ref: node_id.to_owned(),
            head_ref: branch.to_owned(),
        }
    );
    Ok(())
}

async fn validate_anchor_on_branch(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    node_id: &str,
) -> Result<()> {
    let node = load_node_by_exact_id(connection, path, node_id).await?;
    ensure!(
        matches!(node.kind, Kind::Anchor(_)),
        InvalidAnchorSnafu {
            id: node_id.to_owned(),
        }
    );
    validate_ref_on_branch(connection, path, branch, node_id).await
}

async fn load_branch_head(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
) -> Result<String> {
    maybe_load_branch_head(connection, path, branch)
        .await?
        .context(BranchNotFoundSnafu {
            name: branch.to_owned(),
        })
}

async fn maybe_load_branch_head(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
) -> Result<Option<String>> {
    branches::table
        .filter(branches::name.eq(branch))
        .select(branches::head_id)
        .get_result::<String>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

async fn node_reachable_from_head(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    head_id: &str,
    node_id: &str,
) -> Result<bool> {
    let mut current_id = head_id.to_owned();
    let mut seen = HashSet::new();
    loop {
        if current_id == node_id {
            return Ok(true);
        }
        ensure!(
            seen.insert(current_id.clone()),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: "SQLite nodes contain cyclic parents".to_owned(),
            }
        );
        let parent_id = nodes::table
            .filter(nodes::id.eq(&current_id))
            .select(nodes::parent_id)
            .get_result::<String>(connection)
            .await
            .optional()
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?
            .context(ParentNotFoundSnafu {
                id: current_id.clone(),
            })?;
        if parent_id.is_empty() {
            return Ok(false);
        }
        current_id = parent_id;
    }
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

async fn update_branch_head_checked(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected_old_head: &str,
    new_head: &str,
) -> Result<()> {
    connection
        .immediate_transaction::<(), SqliteTransactionError, _>(async |connection| {
            let actual = load_branch_head(connection, path, branch)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            if actual != expected_old_head {
                return Err(SqliteTransactionError::Operation(
                    BranchHeadMovedSnafu {
                        name: branch.to_owned(),
                        expected: expected_old_head.to_owned(),
                        actual,
                    }
                    .build(),
                ));
            }
            load_node_by_exact_id(connection, path, new_head)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            let updated = update_branch_head(connection, path, branch, expected_old_head, new_head)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            if updated != 1 {
                return Err(SqliteTransactionError::Operation(
                    CorruptedStoreSnafu {
                        path: path.to_owned(),
                        message: format!("SQLite branch {branch:?} did not match expected head"),
                    }
                    .build(),
                ));
            }
            Ok(())
        })
        .await
        .map_err(|error| error.into_store_error(path))
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

async fn delete_branch_checked(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
) -> Result<()> {
    load_branch_head(connection, path, branch).await?;
    delete_branch_record(connection, path, branch).await
}

#[cfg(test)]
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

#[cfg(test)]
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

async fn load_job_map(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<HashMap<String, Job>> {
    let rows = jobs::table
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
    rows.into_iter()
        .map(|row| {
            let job = job_row_into_job(path, row)?;
            Ok((job.job_id.clone(), job))
        })
        .collect()
}

async fn load_job(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    job_id: &str,
) -> Result<Job> {
    let row = jobs::table
        .filter(jobs::job_id.eq(job_id))
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
        .get_result::<JobRow>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?
        .context(PromptJobNotFoundSnafu {
            job_id: job_id.to_owned(),
        })?;
    job_row_into_job(path, row)
}

fn job_row_into_job(path: &Path, row: JobRow) -> Result<Job> {
    let mut job =
        serde_json::from_str::<Job>(&row.payload_json).context(ParseSqliteStoreValueSnafu {
            path: path.to_owned(),
            column: "jobs.payload_json".to_owned(),
        })?;
    job.normalize_work_branch();
    validate_job_row_summary(&row, &job, path)?;
    Ok(job)
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

async fn submit_job_in_sqlite(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    base: &str,
) -> Result<Job> {
    connection
        .immediate_transaction::<Job, SqliteTransactionError, _>(async |connection| {
            loop {
                let job_id = format!("job-{}", nanoid::nanoid!());
                if jobs::table
                    .filter(jobs::job_id.eq(&job_id))
                    .count()
                    .get_result::<i64>(connection)
                    .await
                    .context(QuerySqliteStoreSnafu {
                        path: path.to_owned(),
                    })
                    .map_err(SqliteTransactionError::Operation)?
                    == 0
                {
                    return submit_job_with_id_in_transaction(
                        connection, path, &job_id, branch, base,
                    )
                    .await;
                }
            }
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn submit_job_with_id_in_sqlite(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    job_id: &str,
    branch: &str,
    base: &str,
) -> Result<Job> {
    connection
        .immediate_transaction::<Job, SqliteTransactionError, _>(async |connection| {
            submit_job_with_id_in_transaction(connection, path, job_id, branch, base).await
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn submit_job_with_id_in_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    job_id: &str,
    branch: &str,
    base: &str,
) -> std::result::Result<Job, SqliteTransactionError> {
    load_branch_head(connection, path, branch)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    load_node_by_exact_id(connection, path, base)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    if jobs::table
        .filter(jobs::job_id.eq(job_id))
        .count()
        .get_result::<i64>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
        .map_err(SqliteTransactionError::Operation)?
        != 0
    {
        return Err(SqliteTransactionError::Operation(
            PromptJobAlreadyExistsSnafu {
                job_id: job_id.to_owned(),
            }
            .build(),
        ));
    }
    if let Some(active_job) = load_job_map(connection, path)
        .await
        .map_err(SqliteTransactionError::Operation)?
        .values()
        .find(|job| job_uses_active_branch(job, branch))
    {
        return Err(SqliteTransactionError::Operation(
            PromptJobActiveOnBranchSnafu {
                branch: branch.to_owned(),
                job_id: active_job.job_id.clone(),
            }
            .build(),
        ));
    }
    let job = Job::new(job_id, branch, base);
    persist_job(connection, path, &job)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    Ok(job)
}

async fn update_job_status_in_sqlite(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    job_id: &str,
    expected: JobStatus,
    next: JobStatus,
) -> Result<Job> {
    connection
        .immediate_transaction::<Job, SqliteTransactionError, _>(async |connection| {
            let mut job = load_job(connection, path, job_id)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            if job.status != expected {
                return Err(SqliteTransactionError::Operation(
                    PromptJobMovedSnafu {
                        job_id: job_id.to_owned(),
                        expected: format!("{expected:?}"),
                        actual: format!("{:?}", job.status),
                    }
                    .build(),
                ));
            }
            if !job.status.can_transition_to(next) {
                return Err(SqliteTransactionError::Operation(
                    PromptJobInvalidStatusTransitionSnafu {
                        job_id: job_id.to_owned(),
                        current: format!("{:?}", job.status),
                        next: format!("{next:?}"),
                    }
                    .build(),
                ));
            }
            job.status = next;
            job.finished_at = match next {
                JobStatus::Finished => Some(jiff::Timestamp::now()),
                _ => None,
            };
            persist_job(connection, path, &job)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(job)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn update_job_work_branch_in_sqlite(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    job_id: &str,
    expected_work_branch: &str,
    next_work_branch: &str,
) -> Result<Job> {
    connection
        .immediate_transaction::<Job, SqliteTransactionError, _>(async |connection| {
            load_branch_head(connection, path, next_work_branch)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            let jobs = load_job_map(connection, path)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            if let Some(active_job) = jobs
                .values()
                .find(|job| job.job_id != job_id && job_uses_active_branch(job, next_work_branch))
            {
                return Err(SqliteTransactionError::Operation(
                    PromptJobActiveOnBranchSnafu {
                        branch: next_work_branch.to_owned(),
                        job_id: active_job.job_id.clone(),
                    }
                    .build(),
                ));
            }
            let mut job = jobs.get(job_id).cloned().ok_or_else(|| {
                SqliteTransactionError::Operation(
                    PromptJobNotFoundSnafu {
                        job_id: job_id.to_owned(),
                    }
                    .build(),
                )
            })?;
            job.normalize_work_branch();
            if matches!(job.status, JobStatus::Finished) {
                return Err(SqliteTransactionError::Operation(
                    PromptJobInvalidStatusTransitionSnafu {
                        job_id: job_id.to_owned(),
                        current: format!("{:?}", job.status),
                        next: "work_branch_changed".to_owned(),
                    }
                    .build(),
                ));
            }
            if job.work_branch != expected_work_branch {
                return Err(SqliteTransactionError::Operation(
                    PromptJobMovedSnafu {
                        job_id: job_id.to_owned(),
                        expected: expected_work_branch.to_owned(),
                        actual: job.work_branch.clone(),
                    }
                    .build(),
                ));
            }
            job.work_branch = next_work_branch.to_owned();
            persist_job(connection, path, &job)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(job)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

fn job_uses_active_branch(job: &Job, branch: &str) -> bool {
    if matches!(job.status, JobStatus::Finished) {
        return false;
    }
    let work_branch = if job.work_branch.is_empty() {
        job.branch.as_str()
    } else {
        job.work_branch.as_str()
    };
    job.branch == branch || work_branch == branch
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

async fn load_queue_messages(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    queue: &str,
) -> Result<Vec<MessageQueueItem>> {
    let rows = message_queue_items_with_rowid::table
        .filter(message_queue_items_with_rowid::queue.eq(queue))
        .select((
            message_queue_items_with_rowid::rowid,
            message_queue_items_with_rowid::created_at,
            message_queue_items_with_rowid::payload_json,
        ))
        .order(message_queue_items_with_rowid::rowid)
        .load::<MessageQueueItemRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    message_queue_rows_into_sorted_items(path, rows)
}

async fn load_message_queue_names(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<Vec<String>> {
    message_queue_items::table
        .select(message_queue_items::queue)
        .distinct()
        .order(message_queue_items::queue)
        .load::<String>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

fn message_queue_item_row_into_item(
    path: &Path,
    row: MessageQueueItemRow,
) -> Result<MessageQueueItem> {
    let item = serde_json::from_str::<MessageQueueItem>(&row.item_json).context(
        ParseSqliteStoreValueSnafu {
            path: path.to_owned(),
            column: "message_queue_items.payload_json".to_owned(),
        },
    )?;
    validate_text_summary(
        path,
        "message_queue_items.created_at",
        &row.created_at,
        &item.created_at.to_string(),
    )?;
    Ok(item)
}

fn message_queue_rows_into_sorted_items(
    path: &Path,
    rows: Vec<MessageQueueItemRow>,
) -> Result<Vec<MessageQueueItem>> {
    let mut items = Vec::new();
    for row in rows {
        let row_id = row.row_id;
        let item = message_queue_item_row_into_item(path, row)?;
        items.push((row_id, item));
    }
    items.sort_by(|(left_row_id, left), (right_row_id, right)| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left_row_id.cmp(right_row_id))
    });
    Ok(items.into_iter().map(|(_, item)| item).collect())
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

async fn dequeue_message_queue_item(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    queue: &str,
) -> Result<Option<MessageQueueItem>> {
    connection
        .immediate_transaction::<Option<MessageQueueItem>, SqliteTransactionError, _>(
            async |connection| {
                let rows = message_queue_items_with_rowid::table
                    .filter(message_queue_items_with_rowid::queue.eq(queue))
                    .select((
                        message_queue_items_with_rowid::rowid,
                        message_queue_items_with_rowid::created_at,
                        message_queue_items_with_rowid::payload_json,
                    ))
                    .order(message_queue_items_with_rowid::rowid)
                    .load::<MessageQueueItemRow>(connection)
                    .await
                    .context(QuerySqliteStoreSnafu {
                        path: path.to_owned(),
                    })
                    .map_err(SqliteTransactionError::Operation)?;
                let Some(item) = message_queue_rows_into_sorted_items(path, rows)
                    .map_err(SqliteTransactionError::Operation)?
                    .into_iter()
                    .next()
                else {
                    return Ok(None);
                };
                delete_message_queue_item(connection, path, &item)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                Ok(Some(item))
            },
        )
        .await
        .map_err(|error| error.into_store_error(path))
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

async fn load_preset_records(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<HashMap<String, PresetRecord>> {
    let rows = presets::table
        .select(presets::record_json)
        .order(presets::name)
        .load::<String>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    rows.into_iter()
        .map(|record_json| {
            let record = preset_record_from_json(path, &record_json)?;
            Ok((record.name.clone(), record))
        })
        .collect()
}

async fn load_preset_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    name: &str,
) -> Result<PresetRecord> {
    let record_json = presets::table
        .filter(presets::name.eq(name))
        .select(presets::record_json)
        .get_result::<String>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?
        .context(PresetNotFoundSnafu {
            name: name.to_owned(),
        })?;
    preset_record_from_json(path, &record_json)
}

fn preset_record_from_json(path: &Path, record_json: &str) -> Result<PresetRecord> {
    serde_json::from_str::<PresetRecord>(record_json).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "presets.record_json".to_owned(),
    })
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

async fn set_preset_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    name: &str,
    config: Preset,
) -> Result<PresetRecord> {
    connection
        .immediate_transaction::<PresetRecord, SqliteTransactionError, _>(async |connection| {
            let record_json = presets::table
                .filter(presets::name.eq(name))
                .select(presets::record_json)
                .get_result::<String>(connection)
                .await
                .optional()
                .context(QuerySqliteStoreSnafu {
                    path: path.to_owned(),
                })
                .map_err(SqliteTransactionError::Operation)?;
            let record = if let Some(record_json) = record_json {
                let mut record = preset_record_from_json(path, &record_json)
                    .map_err(SqliteTransactionError::Operation)?;
                let current_version = record.current_version;
                record.update(config).ok_or_else(|| {
                    SqliteTransactionError::Operation(
                        PresetVersionNotFoundSnafu {
                            name: name.to_owned(),
                            version: current_version,
                        }
                        .build(),
                    )
                })?;
                record
            } else {
                PresetRecord::new(name.to_owned(), config)
            };
            persist_preset(connection, path, &record)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(record)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn rollback_preset_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    name: &str,
    target_version: u64,
) -> Result<PresetRecord> {
    connection
        .immediate_transaction::<PresetRecord, SqliteTransactionError, _>(async |connection| {
            let mut record = load_preset_record(connection, path, name)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            record.rollback(target_version).ok_or_else(|| {
                SqliteTransactionError::Operation(
                    PresetVersionNotFoundSnafu {
                        name: name.to_owned(),
                        version: target_version,
                    }
                    .build(),
                )
            })?;
            persist_preset(connection, path, &record)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(record)
        })
        .await
        .map_err(|error| error.into_store_error(path))
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

async fn delete_preset_record_checked(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    name: &str,
) -> Result<()> {
    load_preset_record(connection, path, name).await?;
    delete_preset_record(connection, path, name).await
}

async fn load_skill_groups(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<SkillGroups> {
    let mut groups = default_skill_groups();
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
        let record = skill_record_from_json(path, &row.record_json)?;
        groups
            .for_role_mut(role)
            .insert(record.name.clone(), record);
    }
    Ok(groups)
}

async fn load_skill_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    role: SessionRole,
    name: &str,
) -> Result<SkillRecord> {
    load_skill_groups(connection, path)
        .await?
        .for_role(role)
        .get(name)
        .cloned()
        .context(SkillNotFoundSnafu {
            role: role.as_str().to_owned(),
            name: name.to_owned(),
        })
}

fn skill_record_from_json(path: &Path, record_json: &str) -> Result<SkillRecord> {
    serde_json::from_str::<SkillRecord>(record_json).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "skills.record_json".to_owned(),
    })
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

async fn add_skill_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    role: SessionRole,
    name: &str,
    spec: SkillVersionSpec,
) -> Result<SkillRecord> {
    validate_skill_name(name)?;
    connection
        .immediate_transaction::<SkillRecord, SqliteTransactionError, _>(async |connection| {
            let groups = load_skill_groups(connection, path)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            if groups.for_role(role).contains_key(name) {
                return Err(SqliteTransactionError::Operation(
                    SkillAlreadyExistsSnafu {
                        role: role.as_str().to_owned(),
                        name: name.to_owned(),
                    }
                    .build(),
                ));
            }
            let record = SkillRecord::new(name.to_owned(), spec);
            persist_skill(connection, path, role, &record)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(record)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn update_skill_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    role: SessionRole,
    name: &str,
    patch: &SkillUpdatePatch,
) -> Result<SkillRecord> {
    ensure!(
        !patch.is_empty(),
        SkillUpdateEmptySnafu {
            role: role.as_str().to_owned(),
            name: name.to_owned(),
        }
    );
    connection
        .immediate_transaction::<SkillRecord, SqliteTransactionError, _>(async |connection| {
            let mut record = load_skill_record(connection, path, role, name)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            let current_version = record.current_version;
            record.update(patch).ok_or_else(|| {
                SqliteTransactionError::Operation(
                    SkillVersionNotFoundSnafu {
                        role: role.as_str().to_owned(),
                        name: name.to_owned(),
                        version: current_version,
                    }
                    .build(),
                )
            })?;
            persist_skill(connection, path, role, &record)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(record)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn rollback_skill_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    role: SessionRole,
    name: &str,
    target_version: u64,
) -> Result<SkillRecord> {
    connection
        .immediate_transaction::<SkillRecord, SqliteTransactionError, _>(async |connection| {
            let mut record = load_skill_record(connection, path, role, name)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            record.rollback(target_version).ok_or_else(|| {
                SqliteTransactionError::Operation(
                    SkillVersionNotFoundSnafu {
                        role: role.as_str().to_owned(),
                        name: name.to_owned(),
                        version: target_version,
                    }
                    .build(),
                )
            })?;
            persist_skill(connection, path, role, &record)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(record)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

fn validate_skill_name(name: &str) -> Result<()> {
    let trimmed = name.trim();
    ensure!(
        !trimmed.is_empty(),
        InvalidSkillNameSnafu {
            name: name.to_owned(),
            message: "name must not be empty".to_owned(),
        }
    );
    ensure!(
        trimmed == name,
        InvalidSkillNameSnafu {
            name: name.to_owned(),
            message: "name must not have leading or trailing whitespace".to_owned(),
        }
    );
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
        self.block_on(self.ancestry_in_sqlite(head_ref))
    }

    fn log(&self, base_ref: &str, head_ref: &str) -> Result<Vec<Node>> {
        self.block_on(self.log_in_sqlite(base_ref, head_ref))
    }

    fn get_node(&self, id: &str) -> Result<Node> {
        self.block_on(self.get_node_by_prefix_or_branch(id))
    }

    fn list_children(&self, node_id: &str) -> Result<Vec<Node>> {
        self.block_on(self.list_children_in_sqlite(node_id))
    }
}

impl BranchStore for SqliteGraphStore {
    fn fork(&self, _name: &str, _from_ref: &str) -> Result<String> {
        self.ensure_read_only()
    }

    fn get_branch_head(&self, name: &str) -> Result<String> {
        self.block_on(self.get_branch_head_in_sqlite(name))
    }

    fn delete_branch<'a>(&'a self, _name: &'a str) -> impl Future<Output = Result<()>> + Send + 'a {
        std::future::ready(self.ensure_read_only())
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
        self.block_on(self.list_session_states_in_sqlite())
    }

    async fn get_session_state<'a>(&'a self, name: &'a str) -> Result<SessionState> {
        let store = self.clone();
        let name = name.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move { store.get_session_state_in_sqlite(&name).await })
            .await
            .expect("SQLite store task should not panic")
    }

    fn set_session_state<'a>(
        &'a self,
        _name: &'a str,
        _expected: Option<&'a SessionState>,
        _next: SessionState,
    ) -> impl Future<Output = Result<SessionState>> + Send + 'a {
        std::future::ready(self.ensure_read_only())
    }

    fn rebase_session<'a>(
        &'a self,
        _name: &'a str,
        _patch: &'a SessionAnchorPatch,
    ) -> impl Future<Output = Result<String>> + Send + 'a {
        std::future::ready(self.ensure_read_only())
    }

    async fn handoff_session<'a>(
        &'a self,
        _name: &'a str,
        _patch: &'a SessionAnchorPatch,
        _prompt: &'a str,
    ) -> Result<String> {
        self.ensure_read_only()
    }
}

impl NodeStore for SqliteStore {
    fn root_id(&self) -> String {
        self.root_id.clone()
    }

    fn append(&self, node: NewNode) -> Result<String> {
        self.block_on(self.append_in_sqlite(node))
    }

    fn ancestry(&self, head_ref: &str) -> Result<Vec<Node>> {
        self.block_on(self.ancestry_in_sqlite(head_ref))
    }

    fn log(&self, base_ref: &str, head_ref: &str) -> Result<Vec<Node>> {
        self.block_on(self.log_in_sqlite(base_ref, head_ref))
    }

    fn get_node(&self, id: &str) -> Result<Node> {
        self.block_on(self.get_node_in_sqlite(id))
    }

    fn list_children(&self, node_id: &str) -> Result<Vec<Node>> {
        self.block_on(self.list_children_in_sqlite(node_id))
    }
}

impl BranchStore for SqliteStore {
    fn fork(&self, name: &str, from_ref: &str) -> Result<String> {
        self.block_on(self.fork_in_sqlite(name, from_ref))
    }

    fn get_branch_head(&self, name: &str) -> Result<String> {
        self.block_on(self.get_branch_head_in_sqlite(name))
    }

    async fn delete_branch<'a>(&'a self, name: &'a str) -> Result<()> {
        let store = self.clone();
        let name = name.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move { store.delete_branch_in_sqlite(&name).await })
            .await
            .expect("SQLite store task should not panic")
    }

    fn set_branch_head(&self, name: &str, expected_old_head: &str, new_head: &str) -> Result<()> {
        self.block_on(self.set_branch_head_in_sqlite(name, expected_old_head, new_head))
    }
}

impl SessionStore for SqliteStore {
    fn list_session_states(&self) -> Result<std::collections::HashMap<String, SessionState>> {
        self.block_on(self.list_session_states_in_sqlite())
    }

    async fn get_session_state<'a>(&'a self, name: &'a str) -> Result<SessionState> {
        let store = self.clone();
        let name = name.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move { store.get_session_state_in_sqlite(&name).await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn set_session_state<'a>(
        &'a self,
        name: &'a str,
        expected: Option<&'a SessionState>,
        next: SessionState,
    ) -> Result<SessionState> {
        let store = self.clone();
        let name = name.to_owned();
        let expected = expected.cloned();
        self.database
            .inner
            .runtime
            .spawn(async move {
                store
                    .set_session_state_in_sqlite(&name, expected.as_ref(), next)
                    .await
            })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn rebase_session<'a>(
        &'a self,
        name: &'a str,
        patch: &'a SessionAnchorPatch,
    ) -> Result<String> {
        let store = self.clone();
        let name = name.to_owned();
        let patch = patch.clone();
        self.database
            .inner
            .runtime
            .spawn(async move { store.rebase_session_in_sqlite(&name, &patch).await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn handoff_session<'a>(
        &'a self,
        name: &'a str,
        patch: &'a SessionAnchorPatch,
        prompt: &'a str,
    ) -> Result<String> {
        let store = self.clone();
        let name = name.to_owned();
        let patch = patch.clone();
        let prompt = prompt.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move {
                store
                    .handoff_session_in_sqlite(&name, &patch, &prompt)
                    .await
            })
            .await
            .expect("SQLite store task should not panic")
    }
}

impl JobStore for SqliteStore {
    async fn submit_job<'a>(&'a self, branch: &'a str, base: &'a str) -> Result<Job> {
        let store = self.clone();
        let branch = branch.to_owned();
        let base = base.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move { store.submit_job_in_sqlite(&branch, &base).await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn submit_job_with_id<'a>(
        &'a self,
        job_id: &'a str,
        branch: &'a str,
        base: &'a str,
    ) -> Result<Job> {
        let store = self.clone();
        let job_id = job_id.to_owned();
        let branch = branch.to_owned();
        let base = base.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move {
                store
                    .submit_job_with_id_in_sqlite(&job_id, &branch, &base)
                    .await
            })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn get_job<'a>(&'a self, job_id: &'a str) -> Result<Job> {
        let store = self.clone();
        let job_id = job_id.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move { store.get_job_in_sqlite(&job_id).await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn list_jobs(&self) -> Result<std::collections::HashMap<String, Job>> {
        let store = self.clone();
        self.database
            .inner
            .runtime
            .spawn(async move { store.list_jobs_in_sqlite().await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn set_job_status<'a>(
        &'a self,
        job_id: &'a str,
        expected: JobStatus,
        next: JobStatus,
    ) -> Result<Job> {
        let store = self.clone();
        let job_id = job_id.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move {
                store
                    .set_job_status_in_sqlite(&job_id, expected, next)
                    .await
            })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn set_job_work_branch<'a>(
        &'a self,
        job_id: &'a str,
        expected_work_branch: &'a str,
        next_work_branch: &'a str,
    ) -> Result<Job> {
        let store = self.clone();
        let job_id = job_id.to_owned();
        let expected_work_branch = expected_work_branch.to_owned();
        let next_work_branch = next_work_branch.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move {
                store
                    .set_job_work_branch_in_sqlite(
                        &job_id,
                        &expected_work_branch,
                        &next_work_branch,
                    )
                    .await
            })
            .await
            .expect("SQLite store task should not panic")
    }
}

impl MessageQueueStore for SqliteStore {
    async fn enqueue_message<'a>(
        &'a self,
        queue: &'a str,
        payload: serde_json::Value,
    ) -> Result<MessageQueueItem> {
        let store = self.clone();
        let queue = queue.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move { store.enqueue_message_in_sqlite(&queue, payload).await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn dequeue_message<'a>(&'a self, queue: &'a str) -> Result<Option<MessageQueueItem>> {
        let store = self.clone();
        let queue = queue.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move { store.dequeue_message_in_sqlite(&queue).await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn peek_message<'a>(&'a self, queue: &'a str) -> Result<Option<MessageQueueItem>> {
        let store = self.clone();
        let queue = queue.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move { store.peek_message_in_sqlite(&queue).await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn list_queue_messages<'a>(&'a self, queue: &'a str) -> Result<Vec<MessageQueueItem>> {
        let store = self.clone();
        let queue = queue.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move { store.list_queue_messages_in_sqlite(&queue).await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn list_message_queues(&self) -> Result<Vec<String>> {
        let store = self.clone();
        self.database
            .inner
            .runtime
            .spawn(async move { store.list_message_queues_in_sqlite().await })
            .await
            .expect("SQLite store task should not panic")
    }
}

impl PresetStore for SqliteStore {
    async fn list_preset_records(&self) -> Result<std::collections::HashMap<String, PresetRecord>> {
        let store = self.clone();
        self.database
            .inner
            .runtime
            .spawn(async move { store.list_preset_records_in_sqlite().await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn get_preset_record<'a>(&'a self, name: &'a str) -> Result<PresetRecord> {
        let store = self.clone();
        let name = name.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move { store.get_preset_record_in_sqlite(&name).await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn set_preset<'a>(&'a self, name: &'a str, config: Preset) -> Result<PresetRecord> {
        let store = self.clone();
        let name = name.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move { store.set_preset_in_sqlite(&name, config).await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn rollback_preset<'a>(
        &'a self,
        name: &'a str,
        target_version: u64,
    ) -> Result<PresetRecord> {
        let store = self.clone();
        let name = name.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move { store.rollback_preset_in_sqlite(&name, target_version).await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn delete_preset<'a>(&'a self, name: &'a str) -> Result<()> {
        let store = self.clone();
        let name = name.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move { store.delete_preset_in_sqlite(&name).await })
            .await
            .expect("SQLite store task should not panic")
    }
}

impl SkillStore for SqliteStore {
    async fn list_skills(&self, role: SessionRole) -> Result<Vec<SkillRecord>> {
        let store = self.clone();
        self.database
            .inner
            .runtime
            .spawn(async move { store.list_skills_in_sqlite(role).await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn get_skill<'a>(&'a self, role: SessionRole, name: &'a str) -> Result<SkillRecord> {
        let store = self.clone();
        let name = name.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move { store.get_skill_in_sqlite(role, &name).await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn add_skill<'a>(
        &'a self,
        role: SessionRole,
        name: &'a str,
        spec: SkillVersionSpec,
    ) -> Result<SkillRecord> {
        let store = self.clone();
        let name = name.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move { store.add_skill_in_sqlite(role, &name, spec).await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn update_skill<'a>(
        &'a self,
        role: SessionRole,
        name: &'a str,
        patch: &'a SkillUpdatePatch,
    ) -> Result<SkillRecord> {
        let store = self.clone();
        let name = name.to_owned();
        let patch = patch.clone();
        self.database
            .inner
            .runtime
            .spawn(async move { store.update_skill_in_sqlite(role, &name, &patch).await })
            .await
            .expect("SQLite store task should not panic")
    }

    async fn rollback_skill<'a>(
        &'a self,
        role: SessionRole,
        name: &'a str,
        target_version: u64,
    ) -> Result<SkillRecord> {
        let store = self.clone();
        let name = name.to_owned();
        self.database
            .inner
            .runtime
            .spawn(async move {
                store
                    .rollback_skill_in_sqlite(role, &name, target_version)
                    .await
            })
            .await
            .expect("SQLite store task should not panic")
    }
}

impl ProcessShareableStore for SqliteStore {
    fn store_path(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use super::{MessageQueueItem, NodeMetadataRow, SqliteGraphStore, SqliteStore};
    use crate::schema::{jobs, node_metadata, node_relations, nodes, sessions, store_meta};
    use crate::{
        Anchor, BackendMetadata, BranchStore, JobStatus, JobStore, Kind, MergeParent,
        MessageQueueStore, NewNode, Node, NodeMetadata, NodeStore, PauseReason, Preset,
        PresetStore, Role, SessionAnchor, SessionAnchorPatch, SessionRole, SessionState,
        SessionStore, SkillStore, SkillUpdatePatch, SkillVersionSpec,
    };
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;
    use std::sync::mpsc;
    use std::time::Duration;
    use tokio::sync::oneshot;

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

    async fn node_relation_rows(store: &SqliteStore, child_node_id: &str) -> Vec<NodeRelationRow> {
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
    }

    async fn node_metadata_rows(store: &SqliteStore, node_id: &str) -> Vec<NodeMetadataRow> {
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
    }

    async fn node_kinds(store: &SqliteStore, node_id: &str) -> (String, Option<String>) {
        let mut connection = store.connect().await.unwrap();
        let row = nodes::table
            .filter(nodes::id.eq(node_id))
            .select((nodes::kind, nodes::anchor_kind))
            .get_result::<NodeKindRow>(&mut connection)
            .await
            .unwrap();
        (row.kind, row.anchor_kind)
    }

    async fn node_kind_json(store: &SqliteStore, node_id: &str) -> serde_json::Value {
        let mut connection = store.connect().await.unwrap();
        let kind_json = nodes::table
            .filter(nodes::id.eq(node_id))
            .select(nodes::kind_json)
            .get_result::<String>(&mut connection)
            .await
            .unwrap();
        serde_json::from_str(&kind_json).unwrap()
    }

    async fn node_anchor_summary(store: &SqliteStore, node_id: &str) -> NodeAnchorSummaryRow {
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
    }

    async fn session_summary(store: &SqliteStore, branch: &str) -> SessionSummaryRow {
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
    }

    async fn job_summary(store: &SqliteStore, job_id: &str) -> JobSummaryRow {
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
    }

    async fn persist_store_meta_bool_for_test(store: &SqliteStore, key: &str, value: bool) {
        let value_json = serde_json::to_string(&value).unwrap();
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
    }

    fn create_diesel_migration_metadata_for_test(path: &std::path::Path, version: &str) {
        use diesel::connection::SimpleConnection;

        let database_path = super::sqlite_database_path(path);
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

    #[tokio::test]
    async fn open_creates_sqlite_database_and_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");

        let store = SqliteStore::open(&path).unwrap();

        assert!(store.database_path().is_file());
        assert_eq!(store.schema_version().await.unwrap(), 6);
    }

    #[test]
    fn open_temporary_removes_directory_after_last_store_drop() {
        let store = SqliteStore::open_temporary().unwrap();
        let path = store.store_path().to_owned();
        let clone = store.clone();

        assert!(path.exists());
        drop(store);
        assert!(path.exists());
        drop(clone);
        assert!(!path.exists());
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

    #[tokio::test]
    async fn reopened_sqlite_handles_share_database_instance() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");

        let store = SqliteStore::open(&path).unwrap();
        let read_only = SqliteStore::open_read_only(&path).await.unwrap();
        let graph = SqliteGraphStore::open_read_only(&path).unwrap();
        let lexical_read_only = SqliteStore::open_read_only(path.join(".")).await.unwrap();

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

    #[tokio::test(flavor = "multi_thread")]
    async fn graph_store_connection_contention_does_not_block_writer() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let graph = SqliteGraphStore::open_read_only(&path).unwrap();
        let graph_connection_database = graph.database.clone();
        let (graph_locked_tx, graph_locked_rx) = mpsc::channel();
        let (release_graph_tx, release_graph_rx) = oneshot::channel();
        let graph_lock = tokio::spawn(async move {
            let _connection = graph_connection_database.connection().await.unwrap();
            graph_locked_tx.send(()).unwrap();
            release_graph_rx.await.unwrap();
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
        graph_lock.await.unwrap();
        write.join().unwrap();
        assert_eq!(store.get_node(&written).unwrap().id, written);
    }

    #[tokio::test]
    async fn sqlite_store_serializes_concurrent_writes_on_shared_database() {
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
        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert_eq!(
            reopened.list_children(&reopened.root_id()).unwrap().len(),
            8
        );
    }

    #[tokio::test]
    async fn open_read_only_accepts_current_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        SqliteStore::open(&path).unwrap();

        let store = SqliteStore::open_read_only(&path).await.unwrap();

        assert_eq!(store.schema_version().await.unwrap(), 6);
    }

    #[tokio::test]
    async fn append_persists_node_relations() {
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

        let relations = node_relation_rows(&store, &child).await;

        assert_eq!(node_kinds(&store, &child).await, expected_node_kinds);
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

    #[tokio::test]
    async fn append_persists_node_metadata_rows() {
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
            node_metadata_rows(&store, &single).await,
            vec![NodeMetadataRow {
                node_id: single,
                ordinal: 0,
                execution_id: Some("execution-single".to_owned()),
                call_id: Some("call-single".to_owned()),
            }]
        );
        assert_eq!(
            node_metadata_rows(&store, &many).await,
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

    #[tokio::test]
    async fn append_persists_node_anchor_summary() {
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
            node_anchor_summary(&store, &session).await,
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
        let session_kind_json = node_kind_json(&store, &session).await;
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
            node_anchor_summary(&store, &prompt).await,
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
        let prompt_kind_json = node_kind_json(&store, &prompt).await;
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

    #[tokio::test]
    async fn open_read_only_rejects_writes() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let writable = SqliteStore::open(&path).unwrap();
        let root_id = writable.root_id();
        drop(writable);

        let store = SqliteStore::open_read_only(&path).await.unwrap();
        let err = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("child".to_owned()),
            })
            .unwrap_err();

        assert!(matches!(err, crate::StoreError::StoreReadOnly { .. }));
        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
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

    #[tokio::test]
    async fn open_read_only_allows_store_locked_by_another_owner() {
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

        let store = SqliteStore::open_read_only(&path).await.unwrap();

        assert_eq!(store.schema_version().await.unwrap(), 6);
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
                .contains("unsupported SQLite schema version 5, expected 6")
        );
    }

    #[test]
    fn open_rejects_old_diesel_schema_version() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();
        create_diesel_migration_metadata_for_test(&path, "00000000000005");

        let err = SqliteStore::open(&path).unwrap_err();

        assert!(
            err.to_string()
                .contains("unsupported SQLite schema version 5, expected 6")
        );
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

    #[tokio::test]
    async fn open_accepts_legacy_json_store_after_completed_sqlite_migration() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        persist_store_meta_bool_for_test(&store, super::FS_MIGRATION_COMPLETE_META_KEY, true).await;
        drop(store);
        std::fs::write(path.join("meta.json"), "{}").unwrap();
        std::fs::write(path.join("nodes.jsonl"), "").unwrap();

        let reopened = SqliteStore::open(&path).unwrap();

        assert_eq!(reopened.schema_version().await.unwrap(), 6);
    }

    #[tokio::test]
    async fn open_read_only_rejects_legacy_json_store() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("nodes.jsonl"), "").unwrap();

        let err = SqliteStore::open_read_only(&path).await.unwrap_err();

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

    #[tokio::test]
    async fn branch_operations_persist_across_reopen() {
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

        reopened.delete_branch("main").await.unwrap();
        let reopened = SqliteStore::open(&path).unwrap();
        assert!(reopened.get_branch_head("main").is_err());
    }

    #[tokio::test]
    async fn persist_session_nodes_rolls_back_node_when_branch_head_mismatch() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let root_id = store.root_id();
        store.fork("main", &root_id).unwrap();
        let node = Node::new(
            root_id.clone(),
            Role::User,
            None,
            Kind::Text("rolled back node".to_owned()),
            "1970-01-01T00:00:01Z".parse().unwrap(),
        );
        let node_id = node.id.clone();

        let mut connection = store.connect().await.unwrap();
        let err = super::persist_session_nodes_and_branch_head(
            &mut connection,
            &store.database_path,
            "main",
            "stale-head",
            &node_id,
            std::slice::from_ref(&node),
        )
        .await
        .unwrap_err();
        let count = nodes::table
            .filter(nodes::id.eq(node_id))
            .count()
            .get_result::<i64>(&mut connection)
            .await
            .unwrap();

        assert!(err.to_string().contains("did not match expected head"));
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn session_operations_persist_across_reopen() {
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
            .await
            .unwrap();
        assert_eq!(
            session_summary(&store, "main").await,
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
            .await
            .unwrap();
        let handoff = store
            .handoff_session("main", &SessionAnchorPatch::default(), "next prompt")
            .await
            .unwrap();

        let reopened = SqliteStore::open(&path).unwrap();

        assert_eq!(reopened.get_branch_head("main").unwrap(), handoff);
        assert_eq!(
            reopened.get_session_state("main").await.unwrap(),
            SessionState::Paused {
                target_branch: String::new(),
                reason: PauseReason::Closed,
            }
        );
        assert!(reopened.get_node(&rebased).is_ok());
        assert!(reopened.get_node(&handoff).is_ok());
    }

    #[tokio::test]
    async fn job_operations_persist_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let root_id = store.root_id();
        let session = store.append(session_anchor_node(&root_id)).unwrap();
        store.fork("main", &session).unwrap();

        let job = store
            .submit_job_with_id("job-test", "main", &session)
            .await
            .unwrap();
        assert_eq!(job.status, JobStatus::Queued);
        let job = store
            .set_job_status("job-test", JobStatus::Queued, JobStatus::Running)
            .await
            .unwrap();
        assert_eq!(job.status, JobStatus::Running);
        assert_eq!(
            job_summary(&store, "job-test").await,
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
        let job = reopened.get_job("job-test").await.unwrap();

        assert_eq!(job.status, JobStatus::Running);
        assert_eq!(job.branch, "main");
        assert_eq!(job.base, session);
    }

    #[tokio::test]
    async fn message_queue_operations_persist_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        let first = store
            .enqueue_message("runner", serde_json::json!({"index": 1}))
            .await
            .unwrap();
        let second = store
            .enqueue_message("runner", serde_json::json!({"index": 2}))
            .await
            .unwrap();

        let reopened = SqliteStore::open(&path).unwrap();
        let messages = reopened.list_queue_messages("runner").await.unwrap();
        assert_eq!(messages[0].message_id, first.message_id);
        assert_eq!(messages[1].message_id, second.message_id);
        assert_eq!(
            reopened
                .peek_message("runner")
                .await
                .unwrap()
                .unwrap()
                .payload["index"],
            1
        );

        let dequeued = reopened.dequeue_message("runner").await.unwrap().unwrap();
        assert_eq!(dequeued.message_id, first.message_id);
        let reopened = SqliteStore::open(&path).unwrap();
        let messages = reopened.list_queue_messages("runner").await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].message_id, second.message_id);
    }

    #[tokio::test]
    async fn message_queue_preserves_insert_order_for_equal_timestamps() {
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
        let mut connection = store.connect().await.unwrap();
        super::persist_message_queue_item(&mut connection, &store.database_path, &first)
            .await
            .unwrap();
        super::persist_message_queue_item(&mut connection, &store.database_path, &second)
            .await
            .unwrap();

        let reopened = SqliteStore::open(&path).unwrap();
        let messages = reopened.list_queue_messages("runner").await.unwrap();

        assert_eq!(messages[0].message_id, first.message_id);
        assert_eq!(messages[1].message_id, second.message_id);
    }

    #[tokio::test]
    async fn message_queue_sorts_by_parsed_timestamp() {
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
        let mut connection = store.connect().await.unwrap();
        super::persist_message_queue_item(&mut connection, &store.database_path, &first)
            .await
            .unwrap();
        super::persist_message_queue_item(&mut connection, &store.database_path, &second)
            .await
            .unwrap();

        let reopened = SqliteStore::open(&path).unwrap();
        let messages = reopened.list_queue_messages("runner").await.unwrap();

        assert_eq!(messages[0].message_id, first.message_id);
        assert_eq!(messages[1].message_id, second.message_id);
    }

    #[tokio::test]
    async fn preset_operations_persist_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();

        let first = store
            .set_preset("default", preset("gpt-5.4"))
            .await
            .unwrap();
        assert_eq!(first.current_version, 1);
        let second = store
            .set_preset("default", preset("gpt-5.5"))
            .await
            .unwrap();
        assert_eq!(second.current_version, 2);
        let rolled_back = store.rollback_preset("default", 1).await.unwrap();
        assert_eq!(rolled_back.current_version, 3);

        let reopened = SqliteStore::open(&path).unwrap();
        let record = reopened.get_preset_record("default").await.unwrap();

        assert_eq!(record.current_version, 3);
        assert_eq!(record.current_preset().unwrap().model, "gpt-5.4");
    }

    #[tokio::test]
    async fn skill_operations_persist_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).unwrap();
        assert!(
            store
                .get_skill(SessionRole::Orchestrator, "coco-orchestrator")
                .await
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
            .await
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
            .await
            .unwrap();
        assert_eq!(updated.current_version, 2);
        let rolled_back = store
            .rollback_skill(SessionRole::Runner, "custom-runner", 1)
            .await
            .unwrap();
        assert_eq!(rolled_back.current_version, 3);

        let reopened = SqliteStore::open(&path).unwrap();
        let record = reopened
            .get_skill(SessionRole::Runner, "custom-runner")
            .await
            .unwrap();

        assert_eq!(record.current_version, 3);
        assert_eq!(record.current().unwrap().body, "run");
    }
}
