use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use async_trait::async_trait;
use diesel::prelude::*;
use diesel::result::OptionalExtension;
use diesel::sql_types::{Bool, Nullable, Text};
use diesel::sqlite::SqliteConnection;
use diesel_async::pooled_connection::bb8::{
    Pool as AsyncSqlitePool, PooledConnection as AsyncSqlitePooledConnection,
};
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, RunQueryDsl, TransactionManager};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use serde::de::DeserializeOwned;
use serde_json::Value;
use snafu::{IntoError, prelude::*};
use tokio::runtime::Runtime;

use super::{
    BranchAppendSessionState, BranchStore, JobStore, MessageQueueStore, NodeStore, PresetStore,
    ProcessShareableStore, SessionStore, SkillStore,
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
    branches, jobs, message_queue_items, node_anchor_prompt_attachments,
    node_anchor_session_patch_tools, node_anchor_session_patches, node_anchor_session_tools,
    node_anchor_sessions, node_anchor_skill_invocations, node_anchor_skill_results, node_metadata,
    node_relations, node_tool_results, node_tool_uses, nodes, preset_version_tools,
    preset_versions, presets, sessions, skill_version_scripts, skill_versions, skills, store_meta,
};
use crate::{
    Anchor, AnchorPayload, AnchorPayloadKind, BackendMetadata, Job, JobStatus, Kind, MergeParent,
    MessageQueueItem, NewNode, NewNodeContent, Node, NodeMetadata, PauseReason, Preset,
    PresetRecord, PresetVersion, PromptAnchor, PromptAttachment, PromptImageAttachment, Role,
    SessionAnchor, SessionAnchorPatch, SessionRole, SessionState, SkillGroups,
    SkillInvocationAnchor, SkillInvocationMode, SkillRecord, SkillResultAnchor,
    SkillRuntimeContext, SkillScript, SkillUpdatePatch, SkillVersion, SkillVersionSpec, Tool,
    ToolResult, ToolUse, default_skill_groups,
};

const SQLITE_DATABASE_FILE_NAME: &str = "store.sqlite3";
const SQLITE_SCHEMA_VERSION: i32 = 22;
const MIN_SUPPORTED_SQLITE_SCHEMA_VERSION: i32 = 6;
const NODE_ITEM_EXPANSION_SCHEMA_VERSION: i32 = 7;
const DIESEL_MIGRATION_TABLE_NAME: &str = "__diesel_schema_migrations";
const FS_MIGRATION_COMPLETE_META_KEY: &str = "fs_migration_complete";
const NODE_ITEM_ROWS_BACKFILL_META_KEY: &str = "node_items_backfilled";
const LEGACY_JSON_STORE_MARKERS: &[&str] = &["meta.json", "nodes.jsonl"];
const STORE_MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");
const SQLITE_POOL_MAX_SIZE: u32 = 4;
const NODE_KIND_ANCHOR_SESSION: &str = "anchor_session";
const NODE_KIND_ANCHOR_SESSION_PATCH: &str = "anchor_session_patch";
const NODE_KIND_ANCHOR_PROMPT: &str = "anchor_prompt";
const NODE_KIND_ANCHOR_SKILL_INVOCATION: &str = "anchor_skill_invocation";
const NODE_KIND_ANCHOR_SKILL_RESULT: &str = "anchor_skill_result";

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
    write: tokio::sync::Mutex<()>,
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
    #[diesel(sql_type = Bool)]
    metadata_present: bool,
    #[diesel(sql_type = Nullable<Text>)]
    content: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Queryable)]
struct NodeAnchorSessionRow {
    node_id: String,
    role: String,
    provider_profile: Option<String>,
    provider: Option<String>,
    model: String,
    system_prompt: String,
    prompt: String,
    temperature: Option<f64>,
    max_tokens: Option<String>,
    additional_params_json: Option<String>,
    enable_coco_shim: bool,
    active_skill_name: Option<String>,
    active_skill_handoff: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct NodeAnchorSessionToolRow {
    node_id: String,
    ordinal: i32,
    name: String,
    description: String,
    input_schema_json: String,
}

#[derive(Clone, Debug, PartialEq, Queryable)]
struct NodeAnchorSessionPatchRow {
    node_id: String,
    role: Option<String>,
    provider_profile_present: bool,
    provider_profile: Option<String>,
    provider_present: bool,
    provider: Option<String>,
    model: Option<String>,
    tools_present: bool,
    system_prompt: Option<String>,
    temperature_present: bool,
    temperature: Option<f64>,
    max_tokens_present: bool,
    max_tokens: Option<String>,
    additional_params_present: bool,
    additional_params_json: Option<String>,
    enable_coco_shim: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct NodeAnchorSessionPatchToolRow {
    node_id: String,
    ordinal: i32,
    name: String,
    description: String,
    input_schema_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct NodeAnchorPromptAttachmentRow {
    node_id: String,
    ordinal: i32,
    kind: String,
    attachment_id: String,
    width: Option<i64>,
    height: Option<i64>,
    file_size: Option<String>,
    media_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct NodeAnchorSkillInvocationRow {
    node_id: String,
    skill_name: String,
    mode: String,
    prompt: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct NodeAnchorSkillResultRow {
    node_id: String,
    skill_name: String,
    output: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct NodeRelationRow {
    child_node_id: String,
    parent_node_id: String,
    kind: String,
    ordinal: i32,
}

struct NodeAnchorStorageRows<'a> {
    session: Option<&'a NodeAnchorSessionRow>,
    session_tools: &'a [NodeAnchorSessionToolRow],
    session_patch: Option<&'a NodeAnchorSessionPatchRow>,
    session_patch_tools: &'a [NodeAnchorSessionPatchToolRow],
    prompt_attachments: &'a [NodeAnchorPromptAttachmentRow],
    skill_invocation: Option<&'a NodeAnchorSkillInvocationRow>,
    skill_result: Option<&'a NodeAnchorSkillResultRow>,
    relations: &'a [NodeRelationRow],
}

struct NodeStorageRows<'a> {
    anchor: NodeAnchorStorageRows<'a>,
    metadata: &'a [NodeMetadataRow],
    tool_uses: &'a [NodeToolUseRow],
    tool_results: &'a [NodeToolResultRow],
}

#[derive(Default)]
struct NodeStorageIds {
    anchor_sessions: Vec<String>,
    anchor_session_patches: Vec<String>,
    anchor_prompts: Vec<String>,
    anchor_skill_invocations: Vec<String>,
    anchor_skill_results: Vec<String>,
    anchors: Vec<String>,
    metadata: Vec<String>,
    tool_uses: Vec<String>,
    tool_results: Vec<String>,
}

impl NodeStorageIds {
    fn from_rows(rows: &[NodeRow]) -> Self {
        let mut ids = Self::default();
        for row in rows {
            if row.metadata_present {
                ids.metadata.push(row.id.clone());
            }
            match row.kind.as_str() {
                NODE_KIND_ANCHOR_SESSION => {
                    ids.anchor_sessions.push(row.id.clone());
                    ids.anchors.push(row.id.clone());
                }
                NODE_KIND_ANCHOR_SESSION_PATCH => {
                    ids.anchor_session_patches.push(row.id.clone());
                    ids.anchors.push(row.id.clone());
                }
                NODE_KIND_ANCHOR_PROMPT => {
                    ids.anchor_prompts.push(row.id.clone());
                    ids.anchors.push(row.id.clone());
                }
                NODE_KIND_ANCHOR_SKILL_INVOCATION => {
                    ids.anchor_skill_invocations.push(row.id.clone());
                    ids.anchors.push(row.id.clone());
                }
                NODE_KIND_ANCHOR_SKILL_RESULT => {
                    ids.anchor_skill_results.push(row.id.clone());
                    ids.anchors.push(row.id.clone());
                }
                "tool_use" => ids.tool_uses.push(row.id.clone()),
                "tool_result" => ids.tool_results.push(row.id.clone()),
                "text" | "failure" => {}
                _ => {}
            }
        }
        ids
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct NodeMetadataRow {
    node_id: String,
    ordinal: i32,
    execution_id: Option<String>,
    call_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct NodeToolUseRow {
    node_id: String,
    ordinal: i32,
    tool_use_id: String,
    name: String,
    input_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct NodeToolResultRow {
    node_id: String,
    ordinal: i32,
    tool_result_id: String,
    output: String,
}

#[derive(QueryableByName)]
struct NodeItemBackfillRow {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Nullable<Text>)]
    metadata_json: Option<String>,
    #[diesel(sql_type = Text)]
    kind_json: String,
}

enum LegacyNodeMetadata {
    Missing,
    One(BackendMetadata),
    Many(NodeMetadata),
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
}

macro_rules! node_row_columns {
    () => {
        (
            nodes::id,
            nodes::parent_id,
            nodes::created_at,
            nodes::role,
            nodes::kind,
            nodes::metadata_present,
            nodes::content,
        )
    };
}

#[derive(Queryable)]
struct MessageQueueItemRow {
    row_id: i64,
    created_at: String,
    item_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct PresetRow {
    name: String,
    current_version: String,
}

#[derive(Clone, Debug, PartialEq, Queryable)]
struct PresetVersionRow {
    preset_name: String,
    version: String,
    created_at: String,
    role: String,
    provider_profile: String,
    model: String,
    system_prompt: String,
    prompt: String,
    temperature: Option<f64>,
    max_tokens: Option<String>,
    additional_params_json: Option<String>,
    enable_coco_shim: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct PresetVersionToolRow {
    preset_name: String,
    version: String,
    ordinal: i32,
    name: String,
    description: String,
    input_schema_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct SkillRow {
    role: String,
    name: String,
    current_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct SkillVersionRow {
    role: String,
    skill_name: String,
    version: String,
    id: String,
    created_at: String,
    description: String,
    body: String,
    enable_coco_shim: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct SkillVersionScriptRow {
    role: String,
    skill_name: String,
    version: String,
    ordinal: i32,
    path: String,
    content: String,
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

    pub async fn open_unshared_file_path(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_uncached(path.as_ref().to_owned(), true).await
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
            write: tokio::sync::Mutex::new(()),
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
                write: tokio::sync::Mutex::new(()),
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
        P: FnOnce(StoreError) -> E + Send + 'static,
        M: FnOnce(diesel::result::Error) -> E + Send + 'static,
    {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let database = self.clone();
        self.inner.runtime.spawn(async move {
            let result = async {
                let mut connection = match database.connection().await {
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
            .await;
            let _ = sender.send(result);
        });
        let result = receiver
            .recv()
            .expect("SQLite store worker task should not panic");
        match result {
            Ok(result) => result,
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    fn shared_pool(&self) -> &AsyncSqlitePool<AsyncSqliteConnection> {
        &self.inner.pool
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
        let path = path.as_ref();
        if sqlite_database_path(path).is_file() {
            if Self::sqlite_schema_requires_migration(path).await? {
                drop(Self::open(path).await?);
            }
            return Self::open_read_only(path).await;
        }
        Self::open_read_only(path).await
    }

    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
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

    pub async fn open_temporary() -> Result<Self> {
        let directory = Arc::new(create_temporary_store_directory());
        let mut store = Self::open(&directory.path).await?;
        store._owned_directory = Some(directory);
        Ok(store)
    }

    pub async fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
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
        let before_version = current_schema_version(&mut connection, &self.database_path).await?;
        let backfill_complete = if before_version
            .is_some_and(|version| version >= NODE_ITEM_EXPANSION_SCHEMA_VERSION)
        {
            node_item_rows_backfill_complete(&mut connection, &self.database_path).await?
        } else {
            false
        };
        let needs_migration = before_version != Some(SQLITE_SCHEMA_VERSION) || !backfill_complete;
        if needs_migration {
            tracing::info!(
                path = %self.database_path.display(),
                from_version = ?before_version,
                to_version = SQLITE_SCHEMA_VERSION,
                "starting SQLite store migrations"
            );
        }
        run_embedded_migrations_through(
            &mut connection,
            &self.database_path,
            NODE_ITEM_EXPANSION_SCHEMA_VERSION,
        )
        .await?;
        backfill_node_item_rows_with_progress(&mut connection, &self.database_path).await?;
        run_embedded_migrations_through(
            &mut connection,
            &self.database_path,
            SQLITE_SCHEMA_VERSION,
        )
        .await?;
        if needs_migration {
            tracing::info!(
                path = %self.database_path.display(),
                version = SQLITE_SCHEMA_VERSION,
                "finished SQLite store migrations"
            );
        }
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
        ensure!(
            node_item_rows_backfill_complete(&mut connection, &self.database_path).await?,
            CorruptedStoreSnafu {
                path: self.database_path.clone(),
                message: "SQLite node item row migration is incomplete; open the store writable to finish migration".to_owned(),
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
        ensure_supported_schema_version(version, &store.database_path)?;
        if version < SQLITE_SCHEMA_VERSION {
            return Ok(true);
        }
        let mut connection = store.connect().await?;
        Ok(!node_item_rows_backfill_complete(&mut connection, &store.database_path).await?)
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
}

impl SqliteGraphStore {
    pub async fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
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
        ensure!(
            node_item_rows_backfill_complete(&mut connection, &self.database_path).await?,
            CorruptedStoreSnafu {
                path: self.database_path.clone(),
                message: "SQLite node item row migration is incomplete; open the store writable to finish migration".to_owned(),
            }
        );
        Ok(())
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

    pub async fn begin_read_transaction(&self) -> Result<()> {
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

    pub async fn commit_read_transaction(&self) -> Result<()> {
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

    pub async fn rollback_read_transaction(&self) -> Result<()> {
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
    ensure_supported_schema_version(version, path)
}

fn ensure_supported_schema_version(version: i32, path: &Path) -> Result<()> {
    ensure!(
        (MIN_SUPPORTED_SQLITE_SCHEMA_VERSION..=SQLITE_SCHEMA_VERSION).contains(&version),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "unsupported SQLite schema version {version}, expected {MIN_SUPPORTED_SQLITE_SCHEMA_VERSION}..={SQLITE_SCHEMA_VERSION}"
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

async fn backfill_node_item_rows_with_progress(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<()> {
    if node_item_rows_backfill_complete(connection, path).await? {
        return Ok(());
    }
    if current_schema_version(connection, path).await? > Some(NODE_ITEM_EXPANSION_SCHEMA_VERSION) {
        return CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "missing node item backfill marker after legacy columns were removed"
                .to_owned(),
        }
        .fail();
    }

    let total = nodes::table
        .count()
        .get_result::<i64>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    tracing::info!(
        path = %path.display(),
        total_nodes = total,
        "starting SQLite node item row backfill"
    );

    connection
        .immediate_transaction::<(), SqliteTransactionError, _>(async |connection| {
            diesel::delete(node_tool_uses::table)
                .execute(connection)
                .await
                .map_err(SqliteTransactionError::Query)?;
            diesel::delete(node_tool_results::table)
                .execute(connection)
                .await
                .map_err(SqliteTransactionError::Query)?;
            diesel::delete(node_metadata::table)
                .execute(connection)
                .await
                .map_err(SqliteTransactionError::Query)?;

            let rows =
                diesel::sql_query("SELECT id, metadata_json, kind_json FROM nodes ORDER BY id")
                    .load::<NodeItemBackfillRow>(connection)
                    .await
                    .map_err(SqliteTransactionError::Query)?;

            let mut processed = 0usize;
            for row in rows {
                backfill_node_item_row(connection, path, row)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                processed += 1;
                if processed.is_multiple_of(1000) {
                    tracing::info!(
                        path = %path.display(),
                        processed_nodes = processed,
                        total_nodes = total,
                        "backfilled SQLite node item rows"
                    );
                }
            }

            persist_store_meta_bool(connection, path, NODE_ITEM_ROWS_BACKFILL_META_KEY, true)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(())
        })
        .await
        .map_err(|error| error.into_store_error(path))?;

    tracing::info!(
        path = %path.display(),
        total_nodes = total,
        "finished SQLite node item row backfill"
    );
    Ok(())
}

async fn backfill_node_item_row(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    row: NodeItemBackfillRow,
) -> Result<()> {
    let metadata = legacy_node_metadata(path, row.metadata_json.as_deref())?;
    let (kind_json, tool_use_rows, tool_result_rows, tool_item_count) =
        canonical_kind_json_and_tool_rows(path, &row.id, &row.kind_json)?;
    let metadata = metadata.into_node_metadata(tool_item_count);

    diesel::sql_query("UPDATE nodes SET kind_json = ? WHERE id = ?")
        .bind::<Text, _>(kind_json)
        .bind::<Text, _>(&row.id)
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    for metadata_row in expected_node_metadata_rows(&row.id, metadata.as_ref()) {
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

    for tool_use_row in tool_use_rows {
        diesel::insert_into(node_tool_uses::table)
            .values((
                node_tool_uses::node_id.eq(tool_use_row.node_id),
                node_tool_uses::ordinal.eq(tool_use_row.ordinal),
                node_tool_uses::tool_use_id.eq(tool_use_row.tool_use_id),
                node_tool_uses::name.eq(tool_use_row.name),
                node_tool_uses::input_json.eq(tool_use_row.input_json),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }

    for tool_result_row in tool_result_rows {
        diesel::insert_into(node_tool_results::table)
            .values((
                node_tool_results::node_id.eq(tool_result_row.node_id),
                node_tool_results::ordinal.eq(tool_result_row.ordinal),
                node_tool_results::tool_result_id.eq(tool_result_row.tool_result_id),
                node_tool_results::output.eq(tool_result_row.output),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }

    Ok(())
}

fn legacy_node_metadata(path: &Path, metadata_json: Option<&str>) -> Result<LegacyNodeMetadata> {
    let Some(metadata_json) = metadata_json else {
        return Ok(LegacyNodeMetadata::Missing);
    };
    let value =
        serde_json::from_str::<Value>(metadata_json).context(ParseSqliteStoreValueSnafu {
            path: path.to_owned(),
            column: "nodes.metadata_json".to_owned(),
        })?;
    match value {
        Value::Object(_) => serde_json::from_value(value)
            .map(LegacyNodeMetadata::One)
            .context(ParseSqliteStoreValueSnafu {
                path: path.to_owned(),
                column: "nodes.metadata_json".to_owned(),
            }),
        Value::Array(items) => items
            .into_iter()
            .map(|value| {
                serde_json::from_value(value).context(ParseSqliteStoreValueSnafu {
                    path: path.to_owned(),
                    column: "nodes.metadata_json".to_owned(),
                })
            })
            .collect::<Result<_>>()
            .map(LegacyNodeMetadata::Many),
        _ => CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "SQLite nodes.metadata_json must be an object or array for migration"
                .to_owned(),
        }
        .fail(),
    }
}

impl LegacyNodeMetadata {
    fn into_node_metadata(self, tool_item_count: usize) -> Option<NodeMetadata> {
        match self {
            Self::Missing => None,
            Self::One(metadata) => Some(vec![metadata; tool_item_count.max(1)]),
            Self::Many(metadata) => Some(metadata),
        }
    }
}

fn canonical_kind_json_and_tool_rows(
    path: &Path,
    node_id: &str,
    kind_json: &str,
) -> Result<(String, Vec<NodeToolUseRow>, Vec<NodeToolResultRow>, usize)> {
    let value = serde_json::from_str::<Value>(kind_json).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "nodes.kind_json".to_owned(),
    })?;

    if let Some(payload) = value.get("ToolUse") {
        let tool_uses =
            legacy_one_or_many_items::<ToolUse>(path, "nodes.kind_json.ToolUse", payload.clone())?;
        let kind = Kind::tool_use_items(tool_uses);
        let kind_json = legacy_node_kind_residual_json(&kind, path)?;
        let rows = expected_node_tool_use_rows(node_id, &kind, path)?;
        let item_count = rows.len();
        return Ok((kind_json, rows, Vec::new(), item_count));
    }

    if let Some(payload) = value.get("ToolResult") {
        let tool_results = legacy_one_or_many_items::<ToolResult>(
            path,
            "nodes.kind_json.ToolResult",
            payload.clone(),
        )?;
        let kind = Kind::tool_result_items(tool_results);
        let kind_json = legacy_node_kind_residual_json(&kind, path)?;
        let rows = expected_node_tool_result_rows(node_id, &kind);
        let item_count = rows.len();
        return Ok((kind_json, Vec::new(), rows, item_count));
    }

    Ok((kind_json.to_owned(), Vec::new(), Vec::new(), 1))
}

fn legacy_one_or_many_items<T>(path: &Path, column: &str, value: Value) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    match value {
        Value::Array(items) => items
            .into_iter()
            .map(|value| {
                serde_json::from_value(value).context(ParseSqliteStoreValueSnafu {
                    path: path.to_owned(),
                    column: column.to_owned(),
                })
            })
            .collect(),
        Value::Object(_) => serde_json::from_value(value)
            .map(|item| vec![item])
            .context(ParseSqliteStoreValueSnafu {
                path: path.to_owned(),
                column: column.to_owned(),
            }),
        _ => CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite {column} must be an object or array for migration"),
        }
        .fail(),
    }
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
    diesel::insert_into(store_meta::table)
        .values((
            store_meta::key.eq(key),
            store_meta::value_json.eq(value_json),
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

async fn node_item_rows_backfill_complete(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<bool> {
    load_store_meta_bool(connection, path, NODE_ITEM_ROWS_BACKFILL_META_KEY).await
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

async fn load_node_anchor_session_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, NodeAnchorSessionRow>> {
    let mut query = node_anchor_sessions::table
        .select((
            node_anchor_sessions::node_id,
            node_anchor_sessions::role,
            node_anchor_sessions::provider_profile,
            node_anchor_sessions::provider,
            node_anchor_sessions::model,
            node_anchor_sessions::system_prompt,
            node_anchor_sessions::prompt,
            node_anchor_sessions::temperature,
            node_anchor_sessions::max_tokens,
            node_anchor_sessions::additional_params_json,
            node_anchor_sessions::enable_coco_shim,
            node_anchor_sessions::active_skill_name,
            node_anchor_sessions::active_skill_handoff,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_anchor_sessions::node_id.eq_any(node_ids));
    }
    query
        .load::<NodeAnchorSessionRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.node_id.clone(), row))
                .collect()
        })
}

async fn load_node_anchor_session_tool_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, Vec<NodeAnchorSessionToolRow>>> {
    let mut query = node_anchor_session_tools::table
        .select((
            node_anchor_session_tools::node_id,
            node_anchor_session_tools::ordinal,
            node_anchor_session_tools::name,
            node_anchor_session_tools::description,
            node_anchor_session_tools::input_schema_json,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_anchor_session_tools::node_id.eq_any(node_ids));
    }
    let rows = query
        .order((
            node_anchor_session_tools::node_id,
            node_anchor_session_tools::ordinal,
        ))
        .load::<NodeAnchorSessionToolRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(group_node_anchor_session_tool_rows(rows))
}

async fn load_node_anchor_session_patch_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, NodeAnchorSessionPatchRow>> {
    let mut query = node_anchor_session_patches::table
        .select((
            node_anchor_session_patches::node_id,
            node_anchor_session_patches::role,
            node_anchor_session_patches::provider_profile_present,
            node_anchor_session_patches::provider_profile,
            node_anchor_session_patches::provider_present,
            node_anchor_session_patches::provider,
            node_anchor_session_patches::model,
            node_anchor_session_patches::tools_present,
            node_anchor_session_patches::system_prompt,
            node_anchor_session_patches::temperature_present,
            node_anchor_session_patches::temperature,
            node_anchor_session_patches::max_tokens_present,
            node_anchor_session_patches::max_tokens,
            node_anchor_session_patches::additional_params_present,
            node_anchor_session_patches::additional_params_json,
            node_anchor_session_patches::enable_coco_shim,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_anchor_session_patches::node_id.eq_any(node_ids));
    }
    query
        .load::<NodeAnchorSessionPatchRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.node_id.clone(), row))
                .collect()
        })
}

async fn load_node_anchor_session_patch_tool_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, Vec<NodeAnchorSessionPatchToolRow>>> {
    let mut query = node_anchor_session_patch_tools::table
        .select((
            node_anchor_session_patch_tools::node_id,
            node_anchor_session_patch_tools::ordinal,
            node_anchor_session_patch_tools::name,
            node_anchor_session_patch_tools::description,
            node_anchor_session_patch_tools::input_schema_json,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_anchor_session_patch_tools::node_id.eq_any(node_ids));
    }
    let rows = query
        .order((
            node_anchor_session_patch_tools::node_id,
            node_anchor_session_patch_tools::ordinal,
        ))
        .load::<NodeAnchorSessionPatchToolRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(group_node_anchor_session_patch_tool_rows(rows))
}

async fn load_node_anchor_prompt_attachment_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, Vec<NodeAnchorPromptAttachmentRow>>> {
    let mut query = node_anchor_prompt_attachments::table
        .select((
            node_anchor_prompt_attachments::node_id,
            node_anchor_prompt_attachments::ordinal,
            node_anchor_prompt_attachments::kind,
            node_anchor_prompt_attachments::attachment_id,
            node_anchor_prompt_attachments::width,
            node_anchor_prompt_attachments::height,
            node_anchor_prompt_attachments::file_size,
            node_anchor_prompt_attachments::media_type,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_anchor_prompt_attachments::node_id.eq_any(node_ids));
    }
    let rows = query
        .order((
            node_anchor_prompt_attachments::node_id,
            node_anchor_prompt_attachments::ordinal,
        ))
        .load::<NodeAnchorPromptAttachmentRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(group_node_anchor_prompt_attachment_rows(rows))
}

async fn load_node_anchor_skill_invocation_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, NodeAnchorSkillInvocationRow>> {
    let mut query = node_anchor_skill_invocations::table
        .select((
            node_anchor_skill_invocations::node_id,
            node_anchor_skill_invocations::skill_name,
            node_anchor_skill_invocations::mode,
            node_anchor_skill_invocations::prompt,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_anchor_skill_invocations::node_id.eq_any(node_ids));
    }
    query
        .load::<NodeAnchorSkillInvocationRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.node_id.clone(), row))
                .collect()
        })
}

async fn load_node_anchor_skill_result_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, NodeAnchorSkillResultRow>> {
    let mut query = node_anchor_skill_results::table
        .select((
            node_anchor_skill_results::node_id,
            node_anchor_skill_results::skill_name,
            node_anchor_skill_results::output,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_anchor_skill_results::node_id.eq_any(node_ids));
    }
    query
        .load::<NodeAnchorSkillResultRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.node_id.clone(), row))
                .collect()
        })
}

async fn load_node_relation_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, Vec<NodeRelationRow>>> {
    let mut query = node_relations::table
        .select((
            node_relations::child_node_id,
            node_relations::parent_node_id,
            node_relations::kind,
            node_relations::ordinal,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_relations::child_node_id.eq_any(node_ids));
    }
    let rows = query
        .order((node_relations::child_node_id, node_relations::ordinal))
        .load::<NodeRelationRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(group_node_relation_rows(rows))
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

fn group_node_anchor_session_tool_rows(
    rows: Vec<NodeAnchorSessionToolRow>,
) -> HashMap<String, Vec<NodeAnchorSessionToolRow>> {
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.node_id.clone())
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

fn group_node_anchor_session_patch_tool_rows(
    rows: Vec<NodeAnchorSessionPatchToolRow>,
) -> HashMap<String, Vec<NodeAnchorSessionPatchToolRow>> {
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.node_id.clone())
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

fn group_node_anchor_prompt_attachment_rows(
    rows: Vec<NodeAnchorPromptAttachmentRow>,
) -> HashMap<String, Vec<NodeAnchorPromptAttachmentRow>> {
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.node_id.clone())
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

fn group_node_relation_rows(rows: Vec<NodeRelationRow>) -> HashMap<String, Vec<NodeRelationRow>> {
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.child_node_id.clone())
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

async fn load_node_tool_use_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, Vec<NodeToolUseRow>>> {
    let mut query = node_tool_uses::table
        .select((
            node_tool_uses::node_id,
            node_tool_uses::ordinal,
            node_tool_uses::tool_use_id,
            node_tool_uses::name,
            node_tool_uses::input_json,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_tool_uses::node_id.eq_any(node_ids));
    }
    let rows = query
        .order((node_tool_uses::node_id, node_tool_uses::ordinal))
        .load::<NodeToolUseRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(group_node_tool_use_rows(rows))
}

fn group_node_tool_use_rows(rows: Vec<NodeToolUseRow>) -> HashMap<String, Vec<NodeToolUseRow>> {
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.node_id.clone())
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

async fn load_node_tool_result_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, Vec<NodeToolResultRow>>> {
    let mut query = node_tool_results::table
        .select((
            node_tool_results::node_id,
            node_tool_results::ordinal,
            node_tool_results::tool_result_id,
            node_tool_results::output,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_tool_results::node_id.eq_any(node_ids));
    }
    let rows = query
        .order((node_tool_results::node_id, node_tool_results::ordinal))
        .load::<NodeToolResultRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(group_node_tool_result_rows(rows))
}

fn group_node_tool_result_rows(
    rows: Vec<NodeToolResultRow>,
) -> HashMap<String, Vec<NodeToolResultRow>> {
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

fn node_anchor_session_tool_slice<'a>(
    rows: &'a HashMap<String, Vec<NodeAnchorSessionToolRow>>,
    node_id: &str,
) -> &'a [NodeAnchorSessionToolRow] {
    rows.get(node_id).map(Vec::as_slice).unwrap_or_default()
}

fn node_anchor_session_patch_tool_slice<'a>(
    rows: &'a HashMap<String, Vec<NodeAnchorSessionPatchToolRow>>,
    node_id: &str,
) -> &'a [NodeAnchorSessionPatchToolRow] {
    rows.get(node_id).map(Vec::as_slice).unwrap_or_default()
}

fn node_anchor_prompt_attachment_slice<'a>(
    rows: &'a HashMap<String, Vec<NodeAnchorPromptAttachmentRow>>,
    node_id: &str,
) -> &'a [NodeAnchorPromptAttachmentRow] {
    rows.get(node_id).map(Vec::as_slice).unwrap_or_default()
}

fn node_relation_slice<'a>(
    rows: &'a HashMap<String, Vec<NodeRelationRow>>,
    node_id: &str,
) -> &'a [NodeRelationRow] {
    rows.get(node_id).map(Vec::as_slice).unwrap_or_default()
}

fn node_tool_use_slice<'a>(
    rows: &'a HashMap<String, Vec<NodeToolUseRow>>,
    node_id: &str,
) -> &'a [NodeToolUseRow] {
    rows.get(node_id).map(Vec::as_slice).unwrap_or_default()
}

fn node_tool_result_slice<'a>(
    rows: &'a HashMap<String, Vec<NodeToolResultRow>>,
    node_id: &str,
) -> &'a [NodeToolResultRow] {
    rows.get(node_id).map(Vec::as_slice).unwrap_or_default()
}

async fn node_rows_into_nodes(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    rows: Vec<NodeRow>,
) -> Result<Vec<Node>> {
    let ids = NodeStorageIds::from_rows(&rows);
    let anchor_session_rows =
        load_node_anchor_session_rows_for_ids(connection, path, Some(&ids.anchor_sessions)).await?;
    let anchor_session_tool_rows =
        load_node_anchor_session_tool_rows_for_ids(connection, path, Some(&ids.anchor_sessions))
            .await?;
    let anchor_session_patch_rows = load_node_anchor_session_patch_rows_for_ids(
        connection,
        path,
        Some(&ids.anchor_session_patches),
    )
    .await?;
    let anchor_session_patch_tool_rows = load_node_anchor_session_patch_tool_rows_for_ids(
        connection,
        path,
        Some(&ids.anchor_session_patches),
    )
    .await?;
    let anchor_prompt_attachment_rows = load_node_anchor_prompt_attachment_rows_for_ids(
        connection,
        path,
        Some(&ids.anchor_prompts),
    )
    .await?;
    let anchor_skill_invocation_rows = load_node_anchor_skill_invocation_rows_for_ids(
        connection,
        path,
        Some(&ids.anchor_skill_invocations),
    )
    .await?;
    let anchor_skill_result_rows = load_node_anchor_skill_result_rows_for_ids(
        connection,
        path,
        Some(&ids.anchor_skill_results),
    )
    .await?;
    let relation_rows =
        load_node_relation_rows_for_ids(connection, path, Some(&ids.anchors)).await?;
    let metadata_rows =
        load_node_metadata_rows_for_ids(connection, path, Some(&ids.metadata)).await?;
    let tool_use_rows =
        load_node_tool_use_rows_for_ids(connection, path, Some(&ids.tool_uses)).await?;
    let tool_result_rows =
        load_node_tool_result_rows_for_ids(connection, path, Some(&ids.tool_results)).await?;
    rows.into_iter()
        .map(|row| {
            let node_id = row.id.clone();
            row.into_node(
                path,
                NodeStorageRows {
                    anchor: NodeAnchorStorageRows {
                        session: anchor_session_rows.get(&node_id),
                        session_tools: node_anchor_session_tool_slice(
                            &anchor_session_tool_rows,
                            &node_id,
                        ),
                        session_patch: anchor_session_patch_rows.get(&node_id),
                        session_patch_tools: node_anchor_session_patch_tool_slice(
                            &anchor_session_patch_tool_rows,
                            &node_id,
                        ),
                        prompt_attachments: node_anchor_prompt_attachment_slice(
                            &anchor_prompt_attachment_rows,
                            &node_id,
                        ),
                        skill_invocation: anchor_skill_invocation_rows.get(&node_id),
                        skill_result: anchor_skill_result_rows.get(&node_id),
                        relations: node_relation_slice(&relation_rows, &node_id),
                    },
                    metadata: node_metadata_slice(&metadata_rows, &node_id),
                    tool_uses: node_tool_use_slice(&tool_use_rows, &node_id),
                    tool_results: node_tool_result_slice(&tool_result_rows, &node_id),
                },
            )
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
    node_rows_into_nodes(connection, path, vec![row])
        .await?
        .pop()
        .context(CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite node query for {id:?} returned no rows"),
        })
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
    let row = NodeRow::from_node(node);
    diesel::insert_into(nodes::table)
        .values((
            nodes::id.eq(row.id),
            nodes::parent_id.eq(row.parent_id),
            nodes::created_at.eq(row.created_at),
            nodes::role.eq(row.role),
            nodes::kind.eq(row.kind),
            nodes::metadata_present.eq(row.metadata_present),
            nodes::content.eq(row.content),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    persist_node_anchor_session_row(connection, path, node).await?;
    persist_node_anchor_session_tool_rows(connection, path, node).await?;
    persist_node_anchor_session_patch_row(connection, path, node).await?;
    persist_node_anchor_session_patch_tool_rows(connection, path, node).await?;
    persist_node_anchor_prompt_attachment_rows(connection, path, node).await?;
    persist_node_anchor_skill_invocation_row(connection, path, node).await?;
    persist_node_anchor_skill_result_row(connection, path, node).await?;
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
    persist_node_tool_use_rows(connection, path, node).await?;
    persist_node_tool_result_rows(connection, path, node).await?;
    Ok(())
}

async fn upsert_node_without_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    let row = NodeRow::from_node(node);
    diesel::insert_into(nodes::table)
        .values((
            nodes::id.eq(row.id),
            nodes::parent_id.eq(row.parent_id),
            nodes::created_at.eq(row.created_at),
            nodes::role.eq(row.role),
            nodes::kind.eq(row.kind),
            nodes::metadata_present.eq(row.metadata_present),
            nodes::content.eq(row.content),
        ))
        .on_conflict(nodes::id)
        .do_update()
        .set((
            nodes::parent_id.eq(diesel::upsert::excluded(nodes::parent_id)),
            nodes::created_at.eq(diesel::upsert::excluded(nodes::created_at)),
            nodes::role.eq(diesel::upsert::excluded(nodes::role)),
            nodes::kind.eq(diesel::upsert::excluded(nodes::kind)),
            nodes::metadata_present.eq(diesel::upsert::excluded(nodes::metadata_present)),
            nodes::content.eq(diesel::upsert::excluded(nodes::content)),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    delete_node_anchor_payload_rows(connection, path, &node.id).await?;
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
    diesel::delete(node_tool_uses::table.filter(node_tool_uses::node_id.eq(&node.id)))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    diesel::delete(node_tool_results::table.filter(node_tool_results::node_id.eq(&node.id)))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    persist_node_anchor_session_row(connection, path, node).await?;
    persist_node_anchor_session_tool_rows(connection, path, node).await?;
    persist_node_anchor_session_patch_row(connection, path, node).await?;
    persist_node_anchor_session_patch_tool_rows(connection, path, node).await?;
    persist_node_anchor_prompt_attachment_rows(connection, path, node).await?;
    persist_node_anchor_skill_invocation_row(connection, path, node).await?;
    persist_node_anchor_skill_result_row(connection, path, node).await?;
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
    persist_node_tool_use_rows(connection, path, node).await?;
    persist_node_tool_result_rows(connection, path, node).await?;
    Ok(())
}

async fn delete_node_anchor_payload_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_id: &str,
) -> Result<()> {
    diesel::delete(
        node_anchor_prompt_attachments::table
            .filter(node_anchor_prompt_attachments::node_id.eq(node_id)),
    )
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    diesel::delete(node_anchor_sessions::table.filter(node_anchor_sessions::node_id.eq(node_id)))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    diesel::delete(
        node_anchor_session_patches::table.filter(node_anchor_session_patches::node_id.eq(node_id)),
    )
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    diesel::delete(
        node_anchor_skill_invocations::table
            .filter(node_anchor_skill_invocations::node_id.eq(node_id)),
    )
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    diesel::delete(
        node_anchor_skill_results::table.filter(node_anchor_skill_results::node_id.eq(node_id)),
    )
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

async fn persist_node_anchor_session_row(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    let Some(row) = NodeAnchorSessionRow::from_node(node, path)? else {
        return Ok(());
    };
    diesel::insert_into(node_anchor_sessions::table)
        .values((
            node_anchor_sessions::node_id.eq(row.node_id),
            node_anchor_sessions::role.eq(row.role),
            node_anchor_sessions::provider_profile.eq(row.provider_profile),
            node_anchor_sessions::provider.eq(row.provider),
            node_anchor_sessions::model.eq(row.model),
            node_anchor_sessions::system_prompt.eq(row.system_prompt),
            node_anchor_sessions::prompt.eq(row.prompt),
            node_anchor_sessions::temperature.eq(row.temperature),
            node_anchor_sessions::max_tokens.eq(row.max_tokens),
            node_anchor_sessions::additional_params_json.eq(row.additional_params_json),
            node_anchor_sessions::enable_coco_shim.eq(row.enable_coco_shim),
            node_anchor_sessions::active_skill_name.eq(row.active_skill_name),
            node_anchor_sessions::active_skill_handoff.eq(row.active_skill_handoff),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn persist_node_anchor_session_tool_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    for row in node_anchor_session_tool_rows(node, path)? {
        diesel::insert_into(node_anchor_session_tools::table)
            .values((
                node_anchor_session_tools::node_id.eq(row.node_id),
                node_anchor_session_tools::ordinal.eq(row.ordinal),
                node_anchor_session_tools::name.eq(row.name),
                node_anchor_session_tools::description.eq(row.description),
                node_anchor_session_tools::input_schema_json.eq(row.input_schema_json),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    Ok(())
}

async fn persist_node_anchor_session_patch_row(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    let Some(row) = NodeAnchorSessionPatchRow::from_node(node, path)? else {
        return Ok(());
    };
    diesel::insert_into(node_anchor_session_patches::table)
        .values((
            node_anchor_session_patches::node_id.eq(row.node_id),
            node_anchor_session_patches::role.eq(row.role),
            node_anchor_session_patches::provider_profile_present.eq(row.provider_profile_present),
            node_anchor_session_patches::provider_profile.eq(row.provider_profile),
            node_anchor_session_patches::provider_present.eq(row.provider_present),
            node_anchor_session_patches::provider.eq(row.provider),
            node_anchor_session_patches::model.eq(row.model),
            node_anchor_session_patches::tools_present.eq(row.tools_present),
            node_anchor_session_patches::system_prompt.eq(row.system_prompt),
            node_anchor_session_patches::temperature_present.eq(row.temperature_present),
            node_anchor_session_patches::temperature.eq(row.temperature),
            node_anchor_session_patches::max_tokens_present.eq(row.max_tokens_present),
            node_anchor_session_patches::max_tokens.eq(row.max_tokens),
            node_anchor_session_patches::additional_params_present
                .eq(row.additional_params_present),
            node_anchor_session_patches::additional_params_json.eq(row.additional_params_json),
            node_anchor_session_patches::enable_coco_shim.eq(row.enable_coco_shim),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn persist_node_anchor_session_patch_tool_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    for row in node_anchor_session_patch_tool_rows(node, path)? {
        diesel::insert_into(node_anchor_session_patch_tools::table)
            .values((
                node_anchor_session_patch_tools::node_id.eq(row.node_id),
                node_anchor_session_patch_tools::ordinal.eq(row.ordinal),
                node_anchor_session_patch_tools::name.eq(row.name),
                node_anchor_session_patch_tools::description.eq(row.description),
                node_anchor_session_patch_tools::input_schema_json.eq(row.input_schema_json),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    Ok(())
}

async fn persist_node_anchor_prompt_attachment_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    for row in node_anchor_prompt_attachment_rows(node) {
        diesel::insert_into(node_anchor_prompt_attachments::table)
            .values((
                node_anchor_prompt_attachments::node_id.eq(row.node_id),
                node_anchor_prompt_attachments::ordinal.eq(row.ordinal),
                node_anchor_prompt_attachments::kind.eq(row.kind),
                node_anchor_prompt_attachments::attachment_id.eq(row.attachment_id),
                node_anchor_prompt_attachments::width.eq(row.width),
                node_anchor_prompt_attachments::height.eq(row.height),
                node_anchor_prompt_attachments::file_size.eq(row.file_size),
                node_anchor_prompt_attachments::media_type.eq(row.media_type),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    Ok(())
}

async fn persist_node_anchor_skill_invocation_row(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    let Some(row) = NodeAnchorSkillInvocationRow::from_node(node) else {
        return Ok(());
    };
    diesel::insert_into(node_anchor_skill_invocations::table)
        .values((
            node_anchor_skill_invocations::node_id.eq(row.node_id),
            node_anchor_skill_invocations::skill_name.eq(row.skill_name),
            node_anchor_skill_invocations::mode.eq(row.mode),
            node_anchor_skill_invocations::prompt.eq(row.prompt),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn persist_node_anchor_skill_result_row(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    let Some(row) = NodeAnchorSkillResultRow::from_node(node) else {
        return Ok(());
    };
    diesel::insert_into(node_anchor_skill_results::table)
        .values((
            node_anchor_skill_results::node_id.eq(row.node_id),
            node_anchor_skill_results::skill_name.eq(row.skill_name),
            node_anchor_skill_results::output.eq(row.output),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
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

async fn persist_node_tool_use_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    for row in node_tool_use_rows(node, path)? {
        diesel::insert_into(node_tool_uses::table)
            .values((
                node_tool_uses::node_id.eq(row.node_id),
                node_tool_uses::ordinal.eq(row.ordinal),
                node_tool_uses::tool_use_id.eq(row.tool_use_id),
                node_tool_uses::name.eq(row.name),
                node_tool_uses::input_json.eq(row.input_json),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    Ok(())
}

async fn persist_node_tool_result_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    for row in node_tool_result_rows(node) {
        diesel::insert_into(node_tool_results::table)
            .values((
                node_tool_results::node_id.eq(row.node_id),
                node_tool_results::ordinal.eq(row.ordinal),
                node_tool_results::tool_result_id.eq(row.tool_result_id),
                node_tool_results::output.eq(row.output),
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

fn node_anchor_session_tool_rows(
    node: &Node,
    path: &Path,
) -> Result<Vec<NodeAnchorSessionToolRow>> {
    let Kind::Anchor(Anchor {
        payload: AnchorPayload::Session(anchor),
        ..
    }) = &node.kind
    else {
        return Ok(Vec::new());
    };
    anchor
        .tools
        .iter()
        .enumerate()
        .map(|(ordinal, tool)| {
            Ok(NodeAnchorSessionToolRow {
                node_id: node.id.clone(),
                ordinal: ordinal as i32,
                name: tool.name.clone(),
                description: tool.description.clone(),
                input_schema_json: serde_json::to_string(&tool.input_schema).context(
                    ParseSqliteStoreValueSnafu {
                        path: path.to_owned(),
                        column: "node_anchor_session_tools.input_schema_json".to_owned(),
                    },
                )?,
            })
        })
        .collect()
}

fn node_anchor_session_patch_tool_rows(
    node: &Node,
    path: &Path,
) -> Result<Vec<NodeAnchorSessionPatchToolRow>> {
    let Kind::Anchor(Anchor {
        payload: AnchorPayload::SessionPatch(patch),
        ..
    }) = &node.kind
    else {
        return Ok(Vec::new());
    };
    patch
        .tools
        .iter()
        .flatten()
        .enumerate()
        .map(|(ordinal, tool)| {
            Ok(NodeAnchorSessionPatchToolRow {
                node_id: node.id.clone(),
                ordinal: ordinal as i32,
                name: tool.name.clone(),
                description: tool.description.clone(),
                input_schema_json: serde_json::to_string(&tool.input_schema).context(
                    ParseSqliteStoreValueSnafu {
                        path: path.to_owned(),
                        column: "node_anchor_session_patch_tools.input_schema_json".to_owned(),
                    },
                )?,
            })
        })
        .collect()
}

fn node_anchor_prompt_attachment_rows(node: &Node) -> Vec<NodeAnchorPromptAttachmentRow> {
    let Kind::Anchor(Anchor {
        payload: AnchorPayload::Prompt(prompt),
        ..
    }) = &node.kind
    else {
        return Vec::new();
    };
    prompt
        .attachments
        .iter()
        .enumerate()
        .map(|(ordinal, attachment)| match attachment {
            PromptAttachment::Image(image) => NodeAnchorPromptAttachmentRow {
                node_id: node.id.clone(),
                ordinal: ordinal as i32,
                kind: "image".to_owned(),
                attachment_id: image.id.clone(),
                width: image.width.map(i64::from),
                height: image.height.map(i64::from),
                file_size: image.file_size.map(|value| value.to_string()),
                media_type: image.media_type.clone(),
            },
        })
        .collect()
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

fn node_tool_use_rows(node: &Node, path: &Path) -> Result<Vec<NodeToolUseRow>> {
    let Kind::ToolUse(tool_uses) = &node.kind else {
        return Ok(Vec::new());
    };

    tool_uses
        .iter()
        .enumerate()
        .map(|(ordinal, tool_use)| {
            Ok(NodeToolUseRow {
                node_id: node.id.clone(),
                ordinal: ordinal as i32,
                tool_use_id: tool_use.id.clone(),
                name: tool_use.name.clone(),
                input_json: serde_json::to_string(&tool_use.input).context(
                    ParseSqliteStoreValueSnafu {
                        path: path.to_owned(),
                        column: "node_tool_uses.input_json".to_owned(),
                    },
                )?,
            })
        })
        .collect()
}

fn node_tool_result_rows(node: &Node) -> Vec<NodeToolResultRow> {
    let Kind::ToolResult(tool_results) = &node.kind else {
        return Vec::new();
    };

    tool_results
        .iter()
        .enumerate()
        .map(|(ordinal, tool_result)| NodeToolResultRow {
            node_id: node.id.clone(),
            ordinal: ordinal as i32,
            tool_result_id: tool_result.id.clone(),
            output: tool_result.output.clone(),
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

fn node_storage_kind(kind: &Kind) -> &'static str {
    match kind {
        Kind::Anchor(Anchor {
            payload: AnchorPayload::Session(_),
            ..
        }) => NODE_KIND_ANCHOR_SESSION,
        Kind::Anchor(Anchor {
            payload: AnchorPayload::SessionPatch(_),
            ..
        }) => NODE_KIND_ANCHOR_SESSION_PATCH,
        Kind::Anchor(Anchor {
            payload: AnchorPayload::Prompt(_),
            ..
        }) => NODE_KIND_ANCHOR_PROMPT,
        Kind::Anchor(Anchor {
            payload: AnchorPayload::SkillInvocation(_),
            ..
        }) => NODE_KIND_ANCHOR_SKILL_INVOCATION,
        Kind::Anchor(Anchor {
            payload: AnchorPayload::SkillResult(_),
            ..
        }) => NODE_KIND_ANCHOR_SKILL_RESULT,
        Kind::ToolUse(_) => "tool_use",
        Kind::ToolResult(_) => "tool_result",
        Kind::Text(_) => "text",
        Kind::Failure(_) => "failure",
    }
}

impl NodeRow {
    fn from_node(node: &Node) -> Self {
        let kind = node_storage_kind(&node.kind).to_owned();
        let metadata_present = node.metadata.is_some();
        Self {
            id: node.id.clone(),
            parent_id: node.parent.clone(),
            created_at: node.created_at.to_string(),
            role: role_name(&node.role).to_owned(),
            kind,
            content: match &node.kind {
                Kind::Text(content) | Kind::Failure(content) => Some(content.clone()),
                Kind::Anchor(Anchor {
                    payload: AnchorPayload::Prompt(prompt),
                    ..
                }) => Some(prompt.prompt.clone()),
                Kind::Anchor(_) | Kind::ToolUse(_) | Kind::ToolResult(_) => None,
            },
            metadata_present,
        }
    }

    fn into_node(self, path: &Path, rows: NodeStorageRows<'_>) -> Result<Node> {
        let kind = self.kind_from_storage(path, &rows.anchor, rows.tool_uses, rows.tool_results)?;
        ensure!(
            self.kind == node_storage_kind(&kind),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "SQLite node kind column {:?} does not match stored payload",
                    self.kind
                ),
            }
        );
        let metadata =
            node_metadata_from_rows(path, &self.id, self.metadata_present, rows.metadata)?;
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

    fn kind_from_storage(
        &self,
        path: &Path,
        anchor_rows: &NodeAnchorStorageRows<'_>,
        tool_use_rows: &[NodeToolUseRow],
        tool_result_rows: &[NodeToolResultRow],
    ) -> Result<Kind> {
        match self.kind.as_str() {
            NODE_KIND_ANCHOR_SESSION => self.anchor_kind_from_storage(
                path,
                AnchorPayloadKind::Session,
                anchor_rows,
                tool_use_rows,
                tool_result_rows,
            ),
            NODE_KIND_ANCHOR_SESSION_PATCH => self.anchor_kind_from_storage(
                path,
                AnchorPayloadKind::SessionPatch,
                anchor_rows,
                tool_use_rows,
                tool_result_rows,
            ),
            NODE_KIND_ANCHOR_PROMPT => self.anchor_kind_from_storage(
                path,
                AnchorPayloadKind::Prompt,
                anchor_rows,
                tool_use_rows,
                tool_result_rows,
            ),
            NODE_KIND_ANCHOR_SKILL_INVOCATION => self.anchor_kind_from_storage(
                path,
                AnchorPayloadKind::SkillInvocation,
                anchor_rows,
                tool_use_rows,
                tool_result_rows,
            ),
            NODE_KIND_ANCHOR_SKILL_RESULT => self.anchor_kind_from_storage(
                path,
                AnchorPayloadKind::SkillResult,
                anchor_rows,
                tool_use_rows,
                tool_result_rows,
            ),
            "tool_use" => {
                self.ensure_no_content(path)?;
                ensure_no_tool_result_rows(path, &self.id, tool_result_rows)?;
                node_tool_uses_from_rows(path, &self.id, tool_use_rows).map(Kind::tool_use_items)
            }
            "tool_result" => {
                self.ensure_no_content(path)?;
                ensure_no_tool_use_rows(path, &self.id, tool_use_rows)?;
                node_tool_results_from_rows(path, &self.id, tool_result_rows)
                    .map(Kind::tool_result_items)
            }
            "text" => {
                ensure_no_tool_use_rows(path, &self.id, tool_use_rows)?;
                ensure_no_tool_result_rows(path, &self.id, tool_result_rows)?;
                self.required_content(path).map(Kind::Text)
            }
            "failure" => {
                ensure_no_tool_use_rows(path, &self.id, tool_use_rows)?;
                ensure_no_tool_result_rows(path, &self.id, tool_result_rows)?;
                self.required_content(path).map(Kind::Failure)
            }
            _ => CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("invalid SQLite node kind {:?}", self.kind),
            }
            .fail(),
        }
    }

    fn anchor_kind_from_storage(
        &self,
        path: &Path,
        payload_kind: AnchorPayloadKind,
        anchor_rows: &NodeAnchorStorageRows<'_>,
        tool_use_rows: &[NodeToolUseRow],
        tool_result_rows: &[NodeToolResultRow],
    ) -> Result<Kind> {
        let prompt = match payload_kind {
            AnchorPayloadKind::Prompt => Some(self.required_content(path)?),
            AnchorPayloadKind::Session
            | AnchorPayloadKind::SessionPatch
            | AnchorPayloadKind::SkillInvocation
            | AnchorPayloadKind::SkillResult => {
                self.ensure_no_content(path)?;
                None
            }
        };
        ensure_no_tool_use_rows(path, &self.id, tool_use_rows)?;
        ensure_no_tool_result_rows(path, &self.id, tool_result_rows)?;
        node_anchor_kind_from_storage(
            path,
            &self.id,
            &self.parent_id,
            payload_kind,
            prompt,
            anchor_rows,
        )
    }

    fn ensure_no_content(&self, path: &Path) -> Result<()> {
        ensure!(
            self.content.is_none(),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "SQLite node {:?} of kind {:?} unexpectedly has content",
                    self.id, self.kind
                ),
            }
        );
        Ok(())
    }

    fn required_content(&self, path: &Path) -> Result<String> {
        self.content.clone().context(CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "missing SQLite node content for {:?} node {:?}",
                self.kind, self.id
            ),
        })
    }
}

fn node_anchor_kind_from_storage(
    path: &Path,
    node_id: &str,
    parent_id: &str,
    payload_kind: AnchorPayloadKind,
    prompt: Option<String>,
    rows: &NodeAnchorStorageRows<'_>,
) -> Result<Kind> {
    match payload_kind {
        AnchorPayloadKind::Session => {
            let session_row = rows.session.context(CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("missing SQLite node anchor session row for {node_id:?}"),
            })?;
            session_row.kind_from_storage(
                path,
                node_id,
                parent_id,
                rows.session_tools,
                rows.relations,
            )
        }
        AnchorPayloadKind::SessionPatch => {
            let patch_row = rows.session_patch.context(CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("missing SQLite node anchor session patch row for {node_id:?}"),
            })?;
            patch_row.kind_from_storage(
                path,
                node_id,
                parent_id,
                rows.session_patch_tools,
                rows.relations,
            )
        }
        AnchorPayloadKind::Prompt => {
            let prompt = prompt.context(CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("missing SQLite node anchor prompt content for {node_id:?}"),
            })?;
            let attachments = prompt_attachments_from_rows(path, node_id, rows.prompt_attachments)?;
            let merge_parents =
                merge_parents_from_relation_rows(path, node_id, parent_id, rows.relations)?;
            Ok(Kind::Anchor(Anchor::prompt(
                merge_parents,
                PromptAnchor {
                    prompt,
                    attachments,
                },
            )))
        }
        AnchorPayloadKind::SkillInvocation => {
            let invocation_row = rows.skill_invocation.context(CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("missing SQLite node anchor skill invocation row for {node_id:?}"),
            })?;
            invocation_row.kind_from_storage(path, node_id, parent_id, rows.relations)
        }
        AnchorPayloadKind::SkillResult => {
            let result_row = rows.skill_result.context(CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("missing SQLite node anchor skill result row for {node_id:?}"),
            })?;
            result_row.kind_from_storage(path, node_id, parent_id, rows.relations)
        }
    }
}

impl NodeAnchorSessionRow {
    fn from_node(node: &Node, path: &Path) -> Result<Option<Self>> {
        let Kind::Anchor(Anchor {
            payload: AnchorPayload::Session(session),
            ..
        }) = &node.kind
        else {
            return Ok(None);
        };
        let additional_params_json = session
            .additional_params
            .as_ref()
            .map(|value| {
                serde_json::to_string(value).context(ParseSqliteStoreValueSnafu {
                    path: path.to_owned(),
                    column: "node_anchor_sessions.additional_params_json".to_owned(),
                })
            })
            .transpose()?;
        Ok(Some(Self {
            node_id: node.id.clone(),
            role: session.role.as_str().to_owned(),
            provider_profile: session.provider_profile.clone(),
            provider: session.provider.clone(),
            model: session.model.clone(),
            system_prompt: session.system_prompt.clone(),
            prompt: session.prompt.clone(),
            temperature: session.temperature,
            max_tokens: session.max_tokens.map(|value| value.to_string()),
            additional_params_json,
            enable_coco_shim: session.enable_coco_shim,
            active_skill_name: session
                .active_skill
                .as_ref()
                .map(|skill| skill.name.clone()),
            active_skill_handoff: session
                .active_skill
                .as_ref()
                .and_then(|skill| skill.handoff.clone()),
        }))
    }

    fn kind_from_storage(
        &self,
        path: &Path,
        node_id: &str,
        parent_id: &str,
        tool_rows: &[NodeAnchorSessionToolRow],
        relation_rows: &[NodeRelationRow],
    ) -> Result<Kind> {
        ensure!(
            self.node_id == node_id,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "SQLite session row {:?} does not belong to node {node_id:?}",
                    self.node_id
                ),
            }
        );
        let role = parse_session_role(&self.role, path)?;
        let max_tokens = self
            .max_tokens
            .as_deref()
            .map(|value| parse_u64_column(path, "node_anchor_sessions.max_tokens", value))
            .transpose()?;
        let additional_params = self
            .additional_params_json
            .as_deref()
            .map(|value| {
                parse_json_column(path, "node_anchor_sessions.additional_params_json", value)
            })
            .transpose()?;
        let active_skill = match self.active_skill_name.as_deref() {
            Some(name) => Some(SkillRuntimeContext {
                name: name.to_owned(),
                handoff: self.active_skill_handoff.clone(),
            }),
            None => {
                ensure!(
                    self.active_skill_handoff.is_none(),
                    CorruptedStoreSnafu {
                        path: path.to_owned(),
                        message: "SQLite session active skill handoff is present without a name"
                            .to_owned(),
                    }
                );
                None
            }
        };
        let merge_parents =
            merge_parents_from_relation_rows(path, node_id, parent_id, relation_rows)?;
        let tools = session_tools_from_rows(path, node_id, tool_rows)?;
        Ok(Kind::Anchor(Anchor::session(
            merge_parents,
            SessionAnchor {
                role,
                provider_profile: self.provider_profile.clone(),
                provider: self.provider.clone(),
                model: self.model.clone(),
                tools,
                system_prompt: self.system_prompt.clone(),
                prompt: self.prompt.clone(),
                temperature: self.temperature,
                max_tokens,
                additional_params,
                enable_coco_shim: self.enable_coco_shim,
                active_skill,
            },
        )))
    }
}

impl NodeAnchorSkillInvocationRow {
    fn from_node(node: &Node) -> Option<Self> {
        let Kind::Anchor(Anchor {
            payload: AnchorPayload::SkillInvocation(invocation),
            ..
        }) = &node.kind
        else {
            return None;
        };
        let (mode, prompt) = match &invocation.mode {
            SkillInvocationMode::InheritContext => ("inherit_context".to_owned(), None),
            SkillInvocationMode::Handoff { prompt } => ("handoff".to_owned(), Some(prompt.clone())),
        };
        Some(Self {
            node_id: node.id.clone(),
            skill_name: invocation.skill_name.clone(),
            mode,
            prompt,
        })
    }

    fn kind_from_storage(
        &self,
        path: &Path,
        node_id: &str,
        parent_id: &str,
        relation_rows: &[NodeRelationRow],
    ) -> Result<Kind> {
        ensure!(
            self.node_id == node_id,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "SQLite skill invocation row {:?} does not belong to node {node_id:?}",
                    self.node_id
                ),
            }
        );
        let mode = match self.mode.as_str() {
            "inherit_context" => {
                ensure!(
                    self.prompt.is_none(),
                    CorruptedStoreSnafu {
                        path: path.to_owned(),
                        message: "SQLite inherit-context skill invocation has a prompt".to_owned(),
                    }
                );
                SkillInvocationMode::InheritContext
            }
            "handoff" => SkillInvocationMode::Handoff {
                prompt: required_anchor_summary(
                    path,
                    "node_anchor_skill_invocations.prompt",
                    self.prompt.as_deref(),
                )?,
            },
            mode => {
                return CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!("invalid SQLite skill invocation mode {mode:?}"),
                }
                .fail();
            }
        };
        let merge_parents =
            merge_parents_from_relation_rows(path, node_id, parent_id, relation_rows)?;
        Ok(Kind::Anchor(Anchor::skill_invocation(
            merge_parents,
            SkillInvocationAnchor {
                skill_name: self.skill_name.clone(),
                mode,
            },
        )))
    }
}

impl NodeAnchorSkillResultRow {
    fn from_node(node: &Node) -> Option<Self> {
        let Kind::Anchor(Anchor {
            payload: AnchorPayload::SkillResult(result),
            ..
        }) = &node.kind
        else {
            return None;
        };
        Some(Self {
            node_id: node.id.clone(),
            skill_name: result.skill_name.clone(),
            output: result.output.clone(),
        })
    }

    fn kind_from_storage(
        &self,
        path: &Path,
        node_id: &str,
        parent_id: &str,
        relation_rows: &[NodeRelationRow],
    ) -> Result<Kind> {
        ensure!(
            self.node_id == node_id,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "SQLite skill result row {:?} does not belong to node {node_id:?}",
                    self.node_id
                ),
            }
        );
        let merge_parents =
            merge_parents_from_relation_rows(path, node_id, parent_id, relation_rows)?;
        Ok(Kind::Anchor(Anchor::skill_result(
            merge_parents,
            SkillResultAnchor {
                skill_name: self.skill_name.clone(),
                output: self.output.clone(),
            },
        )))
    }
}

impl NodeAnchorSessionPatchRow {
    fn from_node(node: &Node, path: &Path) -> Result<Option<Self>> {
        let Kind::Anchor(Anchor {
            payload: AnchorPayload::SessionPatch(patch),
            ..
        }) = &node.kind
        else {
            return Ok(None);
        };
        let additional_params_json = patch
            .additional_params
            .as_ref()
            .and_then(Option::as_ref)
            .map(|value| {
                serde_json::to_string(value).context(ParseSqliteStoreValueSnafu {
                    path: path.to_owned(),
                    column: "node_anchor_session_patches.additional_params_json".to_owned(),
                })
            })
            .transpose()?;
        Ok(Some(Self {
            node_id: node.id.clone(),
            role: patch.role.map(|role| role.as_str().to_owned()),
            provider_profile_present: patch.provider_profile.is_some(),
            provider_profile: patch.provider_profile.clone().flatten(),
            provider_present: patch.provider.is_some(),
            provider: patch.provider.clone().flatten(),
            model: patch.model.clone(),
            tools_present: patch.tools.is_some(),
            system_prompt: patch.system_prompt.clone(),
            temperature_present: patch.temperature.is_some(),
            temperature: patch.temperature.flatten(),
            max_tokens_present: patch.max_tokens.is_some(),
            max_tokens: patch.max_tokens.flatten().map(|value| value.to_string()),
            additional_params_present: patch.additional_params.is_some(),
            additional_params_json,
            enable_coco_shim: patch.enable_coco_shim,
        }))
    }

    fn kind_from_storage(
        &self,
        path: &Path,
        node_id: &str,
        parent_id: &str,
        tool_rows: &[NodeAnchorSessionPatchToolRow],
        relation_rows: &[NodeRelationRow],
    ) -> Result<Kind> {
        ensure!(
            self.node_id == node_id,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "SQLite session patch row {:?} does not belong to node {node_id:?}",
                    self.node_id
                ),
            }
        );
        let role = self
            .role
            .as_deref()
            .map(|role| parse_session_role(role, path))
            .transpose()?;
        let tools = if self.tools_present {
            Some(session_patch_tools_from_rows(path, node_id, tool_rows)?)
        } else {
            ensure!(
                tool_rows.is_empty(),
                CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!(
                        "SQLite session patch {node_id:?} has tools without tools_present"
                    ),
                }
            );
            None
        };
        let temperature = nested_optional_column(
            path,
            "node_anchor_session_patches.temperature",
            self.temperature_present,
            self.temperature,
        )?;
        let max_tokens_value = self
            .max_tokens
            .as_deref()
            .map(|value| parse_u64_column(path, "node_anchor_session_patches.max_tokens", value))
            .transpose()?;
        let max_tokens = nested_optional_column(
            path,
            "node_anchor_session_patches.max_tokens",
            self.max_tokens_present,
            max_tokens_value,
        )?;
        let additional_params_value = self
            .additional_params_json
            .as_deref()
            .map(|value| {
                parse_json_column(
                    path,
                    "node_anchor_session_patches.additional_params_json",
                    value,
                )
            })
            .transpose()?;
        let additional_params = nested_optional_column(
            path,
            "node_anchor_session_patches.additional_params_json",
            self.additional_params_present,
            additional_params_value,
        )?;
        let patch = SessionAnchorPatch {
            role,
            provider_profile: nested_optional_column(
                path,
                "node_anchor_session_patches.provider_profile",
                self.provider_profile_present,
                self.provider_profile.clone(),
            )?,
            provider: nested_optional_column(
                path,
                "node_anchor_session_patches.provider",
                self.provider_present,
                self.provider.clone(),
            )?,
            model: self.model.clone(),
            tools,
            system_prompt: self.system_prompt.clone(),
            temperature,
            max_tokens,
            additional_params,
            enable_coco_shim: self.enable_coco_shim,
        };
        let merge_parents =
            merge_parents_from_relation_rows(path, node_id, parent_id, relation_rows)?;
        Ok(Kind::Anchor(Anchor::session_patch(merge_parents, patch)))
    }
}

fn nested_optional_column<T>(
    path: &Path,
    column: &str,
    present: bool,
    value: Option<T>,
) -> Result<Option<Option<T>>> {
    if present {
        return Ok(Some(value));
    }
    ensure!(
        value.is_none(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite {column} is set without its presence flag"),
        }
    );
    Ok(None)
}

fn parse_json_column(path: &Path, column: &str, value: &str) -> Result<Value> {
    serde_json::from_str(value).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: column.to_owned(),
    })
}

fn parse_u64_column(path: &Path, column: &str, value: &str) -> Result<u64> {
    value.parse().map_err(|source| StoreError::CorruptedStore {
        path: path.to_owned(),
        message: format!("invalid SQLite {column}: {source}"),
    })
}

fn session_tools_from_rows(
    path: &Path,
    node_id: &str,
    rows: &[NodeAnchorSessionToolRow],
) -> Result<Vec<Tool>> {
    rows.iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure!(
                row.node_id == node_id && row.ordinal == ordinal as i32,
                CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!("invalid SQLite session tool ordinal for node {node_id:?}"),
                }
            );
            Ok(Tool {
                name: row.name.clone(),
                description: row.description.clone(),
                input_schema: parse_json_column(
                    path,
                    "node_anchor_session_tools.input_schema_json",
                    &row.input_schema_json,
                )?,
            })
        })
        .collect()
}

fn session_patch_tools_from_rows(
    path: &Path,
    node_id: &str,
    rows: &[NodeAnchorSessionPatchToolRow],
) -> Result<Vec<Tool>> {
    rows.iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure!(
                row.node_id == node_id && row.ordinal == ordinal as i32,
                CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!(
                        "invalid SQLite session patch tool ordinal for node {node_id:?}"
                    ),
                }
            );
            Ok(Tool {
                name: row.name.clone(),
                description: row.description.clone(),
                input_schema: parse_json_column(
                    path,
                    "node_anchor_session_patch_tools.input_schema_json",
                    &row.input_schema_json,
                )?,
            })
        })
        .collect()
}

fn prompt_attachments_from_rows(
    path: &Path,
    node_id: &str,
    rows: &[NodeAnchorPromptAttachmentRow],
) -> Result<Vec<PromptAttachment>> {
    rows.iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure!(
                row.node_id == node_id && row.ordinal == ordinal as i32,
                CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!(
                        "invalid SQLite prompt attachment ordinal for node {node_id:?}"
                    ),
                }
            );
            ensure!(
                row.kind == "image",
                CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!("invalid SQLite prompt attachment kind {:?}", row.kind),
                }
            );
            let width = row
                .width
                .map(|value| parse_u32_column(path, "node_anchor_prompt_attachments.width", value))
                .transpose()?;
            let height = row
                .height
                .map(|value| parse_u32_column(path, "node_anchor_prompt_attachments.height", value))
                .transpose()?;
            let file_size = row
                .file_size
                .as_deref()
                .map(|value| {
                    parse_u64_column(path, "node_anchor_prompt_attachments.file_size", value)
                })
                .transpose()?;
            Ok(PromptAttachment::Image(PromptImageAttachment {
                id: row.attachment_id.clone(),
                width,
                height,
                file_size,
                media_type: row.media_type.clone(),
            }))
        })
        .collect()
}

fn parse_u32_column(path: &Path, column: &str, value: i64) -> Result<u32> {
    value
        .try_into()
        .map_err(|source| StoreError::CorruptedStore {
            path: path.to_owned(),
            message: format!("invalid SQLite {column}: {source}"),
        })
}

fn merge_parents_from_relation_rows(
    path: &Path,
    node_id: &str,
    parent_id: &str,
    rows: &[NodeRelationRow],
) -> Result<Vec<MergeParent>> {
    let primary_rows = rows
        .iter()
        .filter(|row| row.kind == "primary")
        .collect::<Vec<_>>();
    ensure!(
        primary_rows.len() == 1
            && primary_rows[0].parent_node_id == parent_id
            && primary_rows[0].ordinal == 0,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("invalid SQLite primary relation for anchor node {node_id:?}"),
        }
    );
    let mut merge_parents = Vec::new();
    for row in rows.iter().filter(|row| row.kind != "primary") {
        ensure!(
            row.child_node_id == node_id && row.ordinal == merge_parents.len() as i32,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("invalid SQLite merge relation for anchor node {node_id:?}"),
            }
        );
        let merge_parent = match row.kind.as_str() {
            "merge" => MergeParent::merge(row.parent_node_id.clone()),
            "shadow" => MergeParent::shadow(row.parent_node_id.clone()),
            _ => {
                return CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!(
                        "invalid SQLite relation kind {:?} for anchor node {node_id:?}",
                        row.kind
                    ),
                }
                .fail();
            }
        };
        merge_parents.push(merge_parent);
    }
    Ok(merge_parents)
}

fn legacy_node_kind_residual_json(kind: &Kind, path: &Path) -> Result<String> {
    let residual = match kind {
        Kind::ToolUse(_) => Kind::tool_use_items(Vec::new()),
        Kind::ToolResult(_) => Kind::tool_result_items(Vec::new()),
        _ => kind.clone(),
    };
    serde_json::to_string(&residual).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "nodes.kind_json".to_owned(),
    })
}

fn required_anchor_summary(path: &Path, column: &str, value: Option<&str>) -> Result<String> {
    value.map(str::to_owned).context(CorruptedStoreSnafu {
        path: path.to_owned(),
        message: format!("missing SQLite {column}"),
    })
}

fn node_metadata_from_rows(
    path: &Path,
    node_id: &str,
    metadata_present: bool,
    metadata_rows: &[NodeMetadataRow],
) -> Result<Option<NodeMetadata>> {
    ensure!(
        metadata_present || metadata_rows.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite node {node_id:?} has metadata rows without metadata_present"),
        }
    );
    if !metadata_present {
        return Ok(None);
    }
    metadata_rows
        .iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure_node_item_row(
                path,
                node_id,
                ordinal,
                &row.node_id,
                row.ordinal,
                "metadata",
            )?;
            Ok(BackendMetadata {
                execution_id: row.execution_id.clone(),
                call_id: row.call_id.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn node_tool_uses_from_rows(
    path: &Path,
    node_id: &str,
    rows: &[NodeToolUseRow],
) -> Result<Vec<ToolUse>> {
    rows.iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure_node_item_row(
                path,
                node_id,
                ordinal,
                &row.node_id,
                row.ordinal,
                "tool use",
            )?;
            Ok(ToolUse {
                id: row.tool_use_id.clone(),
                name: row.name.clone(),
                input: serde_json::from_str(&row.input_json).context(
                    ParseSqliteStoreValueSnafu {
                        path: path.to_owned(),
                        column: "node_tool_uses.input_json".to_owned(),
                    },
                )?,
            })
        })
        .collect()
}

fn node_tool_results_from_rows(
    path: &Path,
    node_id: &str,
    rows: &[NodeToolResultRow],
) -> Result<Vec<ToolResult>> {
    rows.iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure_node_item_row(
                path,
                node_id,
                ordinal,
                &row.node_id,
                row.ordinal,
                "tool result",
            )?;
            Ok(ToolResult {
                id: row.tool_result_id.clone(),
                output: row.output.clone(),
            })
        })
        .collect()
}

fn ensure_no_tool_use_rows(path: &Path, node_id: &str, rows: &[NodeToolUseRow]) -> Result<()> {
    ensure!(
        rows.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite node {node_id:?} has unexpected tool use rows"),
        }
    );
    Ok(())
}

fn ensure_no_tool_result_rows(
    path: &Path,
    node_id: &str,
    rows: &[NodeToolResultRow],
) -> Result<()> {
    ensure!(
        rows.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite node {node_id:?} has unexpected tool result rows"),
        }
    );
    Ok(())
}

fn ensure_node_item_row(
    path: &Path,
    node_id: &str,
    expected_ordinal: usize,
    row_node_id: &str,
    row_ordinal: i32,
    item_kind: &str,
) -> Result<()> {
    ensure!(
        row_node_id == node_id && row_ordinal == expected_ordinal as i32,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "invalid SQLite node {item_kind} row for {node_id:?} at ordinal {expected_ordinal}"
            ),
        }
    );
    Ok(())
}

fn expected_node_tool_use_rows(
    node_id: &str,
    kind: &Kind,
    path: &Path,
) -> Result<Vec<NodeToolUseRow>> {
    let Kind::ToolUse(tool_uses) = kind else {
        return Ok(Vec::new());
    };

    tool_uses
        .iter()
        .enumerate()
        .map(|(ordinal, tool_use)| {
            Ok(NodeToolUseRow {
                node_id: node_id.to_owned(),
                ordinal: ordinal as i32,
                tool_use_id: tool_use.id.clone(),
                name: tool_use.name.clone(),
                input_json: serde_json::to_string(&tool_use.input).context(
                    ParseSqliteStoreValueSnafu {
                        path: path.to_owned(),
                        column: "node_tool_uses.input_json".to_owned(),
                    },
                )?,
            })
        })
        .collect()
}

fn expected_node_tool_result_rows(node_id: &str, kind: &Kind) -> Vec<NodeToolResultRow> {
    let Kind::ToolResult(tool_results) = kind else {
        return Vec::new();
    };

    tool_results
        .iter()
        .enumerate()
        .map(|(ordinal, tool_result)| NodeToolResultRow {
            node_id: node_id.to_owned(),
            ordinal: ordinal as i32,
            tool_result_id: tool_result.id.clone(),
            output: tool_result.output.clone(),
        })
        .collect()
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
    let SessionRow {
        branch_name,
        state,
        target_branch,
        base_head_id,
        pause_reason,
        merged_anchor_id,
    } = session;
    let parsed = match (
        state.as_str(),
        target_branch,
        base_head_id,
        pause_reason.as_deref(),
        merged_anchor_id,
    ) {
        ("active", None, None, None, None) => SessionState::Active,
        ("attached", Some(target_branch), Some(base_head_id), None, None) => {
            SessionState::Attached {
                target_branch,
                base_head_id,
            }
        }
        ("paused", Some(target_branch), None, Some("closed"), None) => SessionState::Paused {
            target_branch,
            reason: PauseReason::Closed,
        },
        ("paused", Some(target_branch), None, Some("merged"), Some(merged_anchor_id)) => {
            SessionState::Paused {
                target_branch,
                reason: PauseReason::Merged { merged_anchor_id },
            }
        }
        (state, target_branch, base_head_id, pause_reason, merged_anchor_id) => {
            return CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "invalid SQLite session row for branch {branch_name:?}: \
                     state={state:?}, target_branch={target_branch:?}, \
                     base_head_id={base_head_id:?}, pause_reason={pause_reason:?}, \
                     merged_anchor_id={merged_anchor_id:?}"
                ),
            }
            .fail();
        }
    };
    Ok((branch_name, parsed))
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
        ))
        .on_conflict(sessions::branch_name)
        .do_update()
        .set((
            sessions::state.eq(diesel::upsert::excluded(sessions::state)),
            sessions::target_branch.eq(diesel::upsert::excluded(sessions::target_branch)),
            sessions::base_head_id.eq(diesel::upsert::excluded(sessions::base_head_id)),
            sessions::pause_reason.eq(diesel::upsert::excluded(sessions::pause_reason)),
            sessions::merged_anchor_id.eq(diesel::upsert::excluded(sessions::merged_anchor_id)),
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
            update_session_state_in_transaction(connection, path, branch, expected, next).await
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn update_session_state_in_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected: Option<&SessionState>,
    next: SessionState,
) -> std::result::Result<SessionState, SqliteTransactionError> {
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
            update_branch_head_checked_in_transaction(
                connection,
                path,
                branch,
                expected_old_head,
                new_head,
            )
            .await
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn update_branch_head_checked_in_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected_old_head: &str,
    new_head: &str,
) -> std::result::Result<(), SqliteTransactionError> {
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
}

async fn append_nodes_and_set_branch_head_in_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected_old_head: &str,
    parent: &str,
    new_head: Option<&str>,
    nodes: Vec<NewNodeContent>,
) -> std::result::Result<String, SqliteTransactionError> {
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
    load_node_by_exact_id(connection, path, parent)
        .await
        .map_err(SqliteTransactionError::Operation)?;

    let mut head = parent.to_owned();
    for content in nodes {
        let node = Node::new(
            head,
            content.role,
            content.metadata,
            content.kind,
            jiff::Timestamp::now(),
        );
        validate_new_node(connection, path, &node)
            .await
            .map_err(SqliteTransactionError::Operation)?;
        persist_node_without_transaction(connection, path, &node)
            .await
            .map_err(SqliteTransactionError::Operation)?;
        head = node.id;
    }

    update_branch_head_checked_in_transaction(
        connection,
        path,
        branch,
        expected_old_head,
        new_head.unwrap_or(&head),
    )
    .await?;
    Ok(head)
}

async fn append_nodes_and_set_branch_head_with_session_state_in_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    update: BranchAppendSessionState,
) -> std::result::Result<String, SqliteTransactionError> {
    let head = append_nodes_and_set_branch_head_in_transaction(
        connection,
        path,
        &update.branch,
        &update.expected_old_head,
        &update.parent,
        update.new_head.as_deref(),
        update.nodes,
    )
    .await?;
    let next_session = update.next_session.into_session_state(&head);
    update_session_state_in_transaction(
        connection,
        path,
        &update.session_branch,
        update.expected_session.as_ref(),
        next_session,
    )
    .await?;
    Ok(head)
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
    ensure!(
        !row.work_branch.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite job {:?} has an empty work branch", row.job_id),
        }
    );
    let created_at = parse_job_timestamp(path, "jobs.created_at", &row.created_at)?;
    let finished_at = row
        .finished_at
        .as_deref()
        .map(|value| parse_job_timestamp(path, "jobs.finished_at", value))
        .transpose()?;
    let status = parse_job_status(path, &row.status)?;

    Ok(Job {
        job_id: row.job_id,
        created_at,
        finished_at,
        branch: row.branch,
        work_branch: row.work_branch,
        base: row.base,
        status,
    })
}

fn parse_job_timestamp(path: &Path, column: &str, value: &str) -> Result<jiff::Timestamp> {
    value
        .parse()
        .map_err(|source| crate::StoreError::CorruptedStore {
            path: path.to_owned(),
            message: format!("invalid SQLite job timestamp in {column}: {source}"),
        })
}

fn parse_job_status(path: &Path, status: &str) -> Result<JobStatus> {
    match status {
        "queued" => Ok(JobStatus::Queued),
        "running" => Ok(JobStatus::Running),
        "finished" => Ok(JobStatus::Finished),
        _ => CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("invalid SQLite job status {status:?}"),
        }
        .fail(),
    }
}

async fn persist_job(connection: &mut AsyncSqliteConnection, path: &Path, job: &Job) -> Result<()> {
    let mut summary = job.clone();
    summary.normalize_work_branch();
    let finished_at = summary
        .finished_at
        .as_ref()
        .map(std::string::ToString::to_string);
    diesel::insert_into(jobs::table)
        .values((
            jobs::job_id.eq(&job.job_id),
            jobs::created_at.eq(summary.created_at.to_string()),
            jobs::finished_at.eq(finished_at),
            jobs::branch.eq(&summary.branch),
            jobs::work_branch.eq(&summary.work_branch),
            jobs::base.eq(&summary.base),
            jobs::status.eq(summary.status.as_str()),
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
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
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

async fn append_prompt_job_base_in_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    prompt: PromptAnchor,
    merge_parents: Vec<MergeParent>,
    session_patch: Option<SessionAnchorPatch>,
) -> std::result::Result<String, SqliteTransactionError> {
    let parent_id = load_branch_head(connection, path, branch)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    let prompt_parent_id = if let Some(patch) = session_patch {
        load_session_chain(connection, path, &parent_id)
            .await
            .map_err(SqliteTransactionError::Operation)?;
        let node = Node::new(
            parent_id,
            Role::System,
            None,
            Kind::Anchor(Anchor::session_patch(vec![], patch)),
            jiff::Timestamp::now(),
        );
        validate_new_node(connection, path, &node)
            .await
            .map_err(SqliteTransactionError::Operation)?;
        persist_node_without_transaction(connection, path, &node)
            .await
            .map_err(SqliteTransactionError::Operation)?;
        node.id
    } else {
        parent_id
    };
    let normalized_parents = normalize_prompt_merge_parents(&prompt_parent_id, merge_parents);
    let node = Node::new(
        prompt_parent_id,
        Role::System,
        None,
        Kind::Anchor(Anchor::prompt(normalized_parents, prompt)),
        jiff::Timestamp::now(),
    );
    validate_new_node(connection, path, &node)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    persist_node_without_transaction(connection, path, &node)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    Ok(node.id)
}

fn normalize_prompt_merge_parents(
    parent_id: &str,
    merge_parents: Vec<MergeParent>,
) -> Vec<MergeParent> {
    let mut normalized_parents = Vec::new();
    for merge_parent in merge_parents {
        let node_id = merge_parent.node_id();
        if node_id != parent_id
            && !normalized_parents
                .iter()
                .any(|parent: &MergeParent| parent.node_id() == node_id)
        {
            normalized_parents.push(merge_parent);
        }
    }
    normalized_parents
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
    query_preset_records(connection, path, None).await
}

async fn query_preset_records(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    name: Option<&str>,
) -> Result<HashMap<String, PresetRecord>> {
    let mut preset_query = presets::table.into_boxed();
    if let Some(name) = name {
        preset_query = preset_query.filter(presets::name.eq(name));
    }
    let preset_rows = preset_query
        .select((presets::name, presets::current_version))
        .order(presets::name)
        .load::<PresetRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    let mut version_query = preset_versions::table.into_boxed();
    if let Some(name) = name {
        version_query = version_query.filter(preset_versions::preset_name.eq(name));
    }
    let version_rows = version_query
        .select((
            preset_versions::preset_name,
            preset_versions::version,
            preset_versions::created_at,
            preset_versions::role,
            preset_versions::provider_profile,
            preset_versions::model,
            preset_versions::system_prompt,
            preset_versions::prompt,
            preset_versions::temperature,
            preset_versions::max_tokens,
            preset_versions::additional_params_json,
            preset_versions::enable_coco_shim,
        ))
        .order((preset_versions::preset_name, preset_versions::version))
        .load::<PresetVersionRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    let mut tool_query = preset_version_tools::table.into_boxed();
    if let Some(name) = name {
        tool_query = tool_query.filter(preset_version_tools::preset_name.eq(name));
    }
    let tool_rows = tool_query
        .select((
            preset_version_tools::preset_name,
            preset_version_tools::version,
            preset_version_tools::ordinal,
            preset_version_tools::name,
            preset_version_tools::description,
            preset_version_tools::input_schema_json,
        ))
        .order((
            preset_version_tools::preset_name,
            preset_version_tools::version,
            preset_version_tools::ordinal,
        ))
        .load::<PresetVersionToolRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    preset_records_from_rows(path, preset_rows, version_rows, tool_rows)
}

async fn load_preset_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    name: &str,
) -> Result<PresetRecord> {
    query_preset_records(connection, path, Some(name))
        .await?
        .remove(name)
        .context(PresetNotFoundSnafu {
            name: name.to_owned(),
        })
}

fn preset_records_from_rows(
    path: &Path,
    preset_rows: Vec<PresetRow>,
    version_rows: Vec<PresetVersionRow>,
    tool_rows: Vec<PresetVersionToolRow>,
) -> Result<HashMap<String, PresetRecord>> {
    let mut tools_by_version = HashMap::<(String, String), Vec<_>>::new();
    for row in tool_rows {
        tools_by_version
            .entry((row.preset_name.clone(), row.version.clone()))
            .or_default()
            .push(row);
    }

    let mut versions_by_preset = HashMap::<String, BTreeMap<u64, PresetVersion>>::new();
    for row in version_rows {
        let preset_name = row.preset_name.clone();
        let version_text = row.version.clone();
        let tool_rows = tools_by_version
            .remove(&(preset_name.clone(), version_text))
            .unwrap_or_default();
        let version = row.into_version(path, &tool_rows)?;
        let version_number = version.version;
        let previous = versions_by_preset
            .entry(preset_name.clone())
            .or_default()
            .insert(version_number, version);
        ensure!(
            previous.is_none(),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "duplicate SQLite preset version {version_number} for {preset_name:?}"
                ),
            }
        );
    }
    ensure!(
        tools_by_version.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "SQLite preset tool rows have no matching version".to_owned(),
        }
    );

    let mut records = HashMap::with_capacity(preset_rows.len());
    for row in preset_rows {
        let current_version =
            parse_u64_column(path, "presets.current_version", &row.current_version)?;
        let versions = versions_by_preset.remove(&row.name).unwrap_or_default();
        ensure!(
            versions.contains_key(&current_version),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "missing current SQLite preset version {current_version} for {:?}",
                    row.name
                ),
            }
        );
        let name = row.name;
        let previous = records.insert(
            name.clone(),
            PresetRecord {
                name: name.clone(),
                current_version,
                versions,
            },
        );
        ensure!(
            previous.is_none(),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("duplicate SQLite preset row for {name:?}"),
            }
        );
    }
    ensure!(
        versions_by_preset.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "SQLite preset versions have no matching preset".to_owned(),
        }
    );
    Ok(records)
}

impl PresetVersionRow {
    fn from_version(preset_name: &str, version: &PresetVersion, path: &Path) -> Result<Self> {
        let additional_params_json = version
            .config
            .additional_params
            .as_ref()
            .map(|value| {
                serde_json::to_string(value).context(ParseSqliteStoreValueSnafu {
                    path: path.to_owned(),
                    column: "preset_versions.additional_params_json".to_owned(),
                })
            })
            .transpose()?;
        Ok(Self {
            preset_name: preset_name.to_owned(),
            version: version.version.to_string(),
            created_at: version.created_at.to_string(),
            role: version.config.role.as_str().to_owned(),
            provider_profile: version.config.provider_profile.clone(),
            model: version.config.model.clone(),
            system_prompt: version.config.system_prompt.clone(),
            prompt: version.config.prompt.clone(),
            temperature: version.config.temperature,
            max_tokens: version.config.max_tokens.map(|value| value.to_string()),
            additional_params_json,
            enable_coco_shim: version.config.enable_coco_shim,
        })
    }

    fn into_version(
        self,
        path: &Path,
        tool_rows: &[PresetVersionToolRow],
    ) -> Result<PresetVersion> {
        let version = parse_u64_column(path, "preset_versions.version", &self.version)?;
        let created_at = self
            .created_at
            .parse()
            .map_err(|source| StoreError::CorruptedStore {
                path: path.to_owned(),
                message: format!(
                    "invalid SQLite preset timestamp in preset_versions.created_at: {source}"
                ),
            })?;
        let max_tokens = self
            .max_tokens
            .as_deref()
            .map(|value| parse_u64_column(path, "preset_versions.max_tokens", value))
            .transpose()?;
        let additional_params = self
            .additional_params_json
            .as_deref()
            .map(|value| parse_json_column(path, "preset_versions.additional_params_json", value))
            .transpose()?;
        let tools = preset_tools_from_rows(path, &self.preset_name, &self.version, tool_rows)?;
        Ok(PresetVersion {
            version,
            created_at,
            config: Preset {
                role: parse_session_role(&self.role, path)?,
                provider_profile: self.provider_profile,
                model: self.model,
                tools,
                system_prompt: self.system_prompt,
                prompt: self.prompt,
                temperature: self.temperature,
                max_tokens,
                additional_params,
                enable_coco_shim: self.enable_coco_shim,
            },
        })
    }
}

fn preset_version_tool_rows(
    preset_name: &str,
    version: &PresetVersion,
    path: &Path,
) -> Result<Vec<PresetVersionToolRow>> {
    version
        .config
        .tools
        .iter()
        .enumerate()
        .map(|(ordinal, tool)| {
            Ok(PresetVersionToolRow {
                preset_name: preset_name.to_owned(),
                version: version.version.to_string(),
                ordinal: ordinal as i32,
                name: tool.name.clone(),
                description: tool.description.clone(),
                input_schema_json: serde_json::to_string(&tool.input_schema).context(
                    ParseSqliteStoreValueSnafu {
                        path: path.to_owned(),
                        column: "preset_version_tools.input_schema_json".to_owned(),
                    },
                )?,
            })
        })
        .collect()
}

fn preset_tools_from_rows(
    path: &Path,
    preset_name: &str,
    version: &str,
    rows: &[PresetVersionToolRow],
) -> Result<Vec<Tool>> {
    rows.iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure!(
                row.preset_name == preset_name
                    && row.version == version
                    && row.ordinal == ordinal as i32,
                CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!(
                        "invalid SQLite preset tool ordinal for {preset_name:?} version {version}"
                    ),
                }
            );
            Ok(Tool {
                name: row.name.clone(),
                description: row.description.clone(),
                input_schema: parse_json_column(
                    path,
                    "preset_version_tools.input_schema_json",
                    &row.input_schema_json,
                )?,
            })
        })
        .collect()
}

async fn persist_preset(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    record: &PresetRecord,
) -> Result<()> {
    let version = record.current().context(CorruptedStoreSnafu {
        path: path.to_owned(),
        message: format!(
            "missing current preset version {} for {:?}",
            record.current_version, record.name
        ),
    })?;
    ensure!(
        version.version == record.current_version,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "preset version {} does not match current version {} for {:?}",
                version.version, record.current_version, record.name
            ),
        }
    );
    let version_row = PresetVersionRow::from_version(&record.name, version, path)?;
    let tool_rows = preset_version_tool_rows(&record.name, version, path)?;

    diesel::insert_into(presets::table)
        .values((
            presets::name.eq(&record.name),
            presets::current_version.eq(record.current_version.to_string()),
        ))
        .on_conflict(presets::name)
        .do_update()
        .set(presets::current_version.eq(diesel::upsert::excluded(presets::current_version)))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    diesel::insert_into(preset_versions::table)
        .values((
            preset_versions::preset_name.eq(version_row.preset_name),
            preset_versions::version.eq(version_row.version),
            preset_versions::created_at.eq(version_row.created_at),
            preset_versions::role.eq(version_row.role),
            preset_versions::provider_profile.eq(version_row.provider_profile),
            preset_versions::model.eq(version_row.model),
            preset_versions::system_prompt.eq(version_row.system_prompt),
            preset_versions::prompt.eq(version_row.prompt),
            preset_versions::temperature.eq(version_row.temperature),
            preset_versions::max_tokens.eq(version_row.max_tokens),
            preset_versions::additional_params_json.eq(version_row.additional_params_json),
            preset_versions::enable_coco_shim.eq(version_row.enable_coco_shim),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    for row in tool_rows {
        diesel::insert_into(preset_version_tools::table)
            .values((
                preset_version_tools::preset_name.eq(row.preset_name),
                preset_version_tools::version.eq(row.version),
                preset_version_tools::ordinal.eq(row.ordinal),
                preset_version_tools::name.eq(row.name),
                preset_version_tools::description.eq(row.description),
                preset_version_tools::input_schema_json.eq(row.input_schema_json),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
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
            let mut records = query_preset_records(connection, path, Some(name))
                .await
                .map_err(SqliteTransactionError::Operation)?;
            let record = if let Some(mut record) = records.remove(name) {
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
    for (role, record) in query_skill_records(connection, path, None, None).await? {
        groups
            .for_role_mut(role)
            .insert(record.name.clone(), record);
    }
    Ok(groups)
}

async fn query_skill_records(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    role: Option<SessionRole>,
    name: Option<&str>,
) -> Result<Vec<(SessionRole, SkillRecord)>> {
    let mut skill_query = skills::table.into_boxed();
    if let Some(role) = role {
        skill_query = skill_query.filter(skills::role.eq(role.as_str()));
    }
    if let Some(name) = name {
        skill_query = skill_query.filter(skills::name.eq(name));
    }
    let skill_rows = skill_query
        .select((skills::role, skills::name, skills::current_version))
        .order((skills::role, skills::name))
        .load::<SkillRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    let mut version_query = skill_versions::table.into_boxed();
    if let Some(role) = role {
        version_query = version_query.filter(skill_versions::role.eq(role.as_str()));
    }
    if let Some(name) = name {
        version_query = version_query.filter(skill_versions::skill_name.eq(name));
    }
    let version_rows = version_query
        .select((
            skill_versions::role,
            skill_versions::skill_name,
            skill_versions::version,
            skill_versions::id,
            skill_versions::created_at,
            skill_versions::description,
            skill_versions::body,
            skill_versions::enable_coco_shim,
        ))
        .order((
            skill_versions::role,
            skill_versions::skill_name,
            skill_versions::version,
        ))
        .load::<SkillVersionRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    let mut script_query = skill_version_scripts::table.into_boxed();
    if let Some(role) = role {
        script_query = script_query.filter(skill_version_scripts::role.eq(role.as_str()));
    }
    if let Some(name) = name {
        script_query = script_query.filter(skill_version_scripts::skill_name.eq(name));
    }
    let script_rows = script_query
        .select((
            skill_version_scripts::role,
            skill_version_scripts::skill_name,
            skill_version_scripts::version,
            skill_version_scripts::ordinal,
            skill_version_scripts::path,
            skill_version_scripts::content,
        ))
        .order((
            skill_version_scripts::role,
            skill_version_scripts::skill_name,
            skill_version_scripts::version,
            skill_version_scripts::ordinal,
        ))
        .load::<SkillVersionScriptRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    skill_records_from_rows(path, skill_rows, version_rows, script_rows)
}

async fn load_skill_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    role: SessionRole,
    name: &str,
) -> Result<SkillRecord> {
    if let Some((_, record)) = query_skill_records(connection, path, Some(role), Some(name))
        .await?
        .pop()
    {
        return Ok(record);
    }
    default_skill_groups()
        .for_role(role)
        .get(name)
        .cloned()
        .context(SkillNotFoundSnafu {
            role: role.as_str().to_owned(),
            name: name.to_owned(),
        })
}

fn skill_records_from_rows(
    path: &Path,
    skill_rows: Vec<SkillRow>,
    version_rows: Vec<SkillVersionRow>,
    script_rows: Vec<SkillVersionScriptRow>,
) -> Result<Vec<(SessionRole, SkillRecord)>> {
    let mut scripts_by_version =
        HashMap::<(String, String, String), Vec<SkillVersionScriptRow>>::new();
    for row in script_rows {
        scripts_by_version
            .entry((
                row.role.clone(),
                row.skill_name.clone(),
                row.version.clone(),
            ))
            .or_default()
            .push(row);
    }

    let mut versions_by_skill = HashMap::<(String, String), BTreeMap<u64, SkillVersion>>::new();
    for row in version_rows {
        let role = row.role.clone();
        let skill_name = row.skill_name.clone();
        let version_text = row.version.clone();
        let scripts = scripts_by_version
            .remove(&(role.clone(), skill_name.clone(), version_text))
            .unwrap_or_default();
        let version = row.into_version(path, scripts)?;
        let version_number = version.version;
        let previous = versions_by_skill
            .entry((role.clone(), skill_name.clone()))
            .or_default()
            .insert(version_number, version);
        ensure!(
            previous.is_none(),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "duplicate SQLite skill version {version_number} for {role:?}/{skill_name:?}"
                ),
            }
        );
    }
    ensure!(
        scripts_by_version.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "SQLite skill script rows have no matching version".to_owned(),
        }
    );

    let mut records = Vec::with_capacity(skill_rows.len());
    for row in skill_rows {
        let role = parse_session_role(&row.role, path)?;
        let current_version =
            parse_u64_column(path, "skills.current_version", &row.current_version)?;
        let versions = versions_by_skill
            .remove(&(row.role.clone(), row.name.clone()))
            .unwrap_or_default();
        ensure!(
            versions.contains_key(&current_version),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "missing current SQLite skill version {current_version} for {:?}/{:?}",
                    row.role, row.name
                ),
            }
        );
        records.push((
            role,
            SkillRecord {
                name: row.name,
                current_version,
                versions,
            },
        ));
    }
    ensure!(
        versions_by_skill.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "SQLite skill versions have no matching skill".to_owned(),
        }
    );
    Ok(records)
}

impl SkillVersionRow {
    fn from_version(role: SessionRole, skill_name: &str, version: &SkillVersion) -> Self {
        Self {
            role: role.as_str().to_owned(),
            skill_name: skill_name.to_owned(),
            version: version.version.to_string(),
            id: version.id.clone(),
            created_at: version.created_at.to_string(),
            description: version.description.clone(),
            body: version.body.clone(),
            enable_coco_shim: version.enable_coco_shim,
        }
    }

    fn into_version(
        self,
        path: &Path,
        script_rows: Vec<SkillVersionScriptRow>,
    ) -> Result<SkillVersion> {
        let version = parse_u64_column(path, "skill_versions.version", &self.version)?;
        let created_at = self
            .created_at
            .parse()
            .map_err(|source| StoreError::CorruptedStore {
                path: path.to_owned(),
                message: format!(
                    "invalid SQLite skill timestamp in skill_versions.created_at: {source}"
                ),
            })?;
        let scripts = skill_scripts_from_rows(
            path,
            &self.role,
            &self.skill_name,
            &self.version,
            script_rows,
        )?;
        Ok(SkillVersion {
            id: self.id,
            version,
            created_at,
            description: self.description,
            body: self.body,
            scripts,
            enable_coco_shim: self.enable_coco_shim,
        })
    }
}

fn skill_version_script_rows(
    role: SessionRole,
    skill_name: &str,
    version: &SkillVersion,
) -> Vec<SkillVersionScriptRow> {
    version
        .scripts
        .iter()
        .enumerate()
        .map(|(ordinal, script)| SkillVersionScriptRow {
            role: role.as_str().to_owned(),
            skill_name: skill_name.to_owned(),
            version: version.version.to_string(),
            ordinal: ordinal as i32,
            path: script.path.clone(),
            content: script.content.clone(),
        })
        .collect()
}

fn skill_scripts_from_rows(
    path: &Path,
    role: &str,
    skill_name: &str,
    version: &str,
    rows: Vec<SkillVersionScriptRow>,
) -> Result<Vec<SkillScript>> {
    rows.into_iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure!(
                row.role == role
                    && row.skill_name == skill_name
                    && row.version == version
                    && row.ordinal == ordinal as i32,
                CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!(
                        "invalid SQLite skill script ordinal for {role:?}/{skill_name:?} version {version}"
                    ),
                }
            );
            Ok(SkillScript {
                path: row.path,
                content: row.content,
            })
        })
        .collect()
}

async fn persist_skill(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    role: SessionRole,
    record: &SkillRecord,
) -> Result<()> {
    let version = record.current().context(CorruptedStoreSnafu {
        path: path.to_owned(),
        message: format!(
            "missing current skill version {} for {:?}/{:?}",
            record.current_version,
            role.as_str(),
            record.name
        ),
    })?;
    ensure!(
        version.version == record.current_version,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "skill version {} does not match current version {} for {:?}/{:?}",
                version.version,
                record.current_version,
                role.as_str(),
                record.name
            ),
        }
    );
    let skill_is_persisted = skills::table
        .filter(skills::role.eq(role.as_str()))
        .filter(skills::name.eq(&record.name))
        .select(skills::name)
        .first::<String>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?
        .is_some();
    // Built-in skills live in memory until their first update, so materialize their history too.
    let versions = if skill_is_persisted {
        vec![(record.current_version, version)]
    } else {
        record
            .versions
            .iter()
            .map(|(version_number, version)| (*version_number, version))
            .collect()
    };
    let mut rows = Vec::with_capacity(versions.len());
    for (version_number, version) in versions {
        ensure!(
            version.version == version_number,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "skill version {} does not match version key {version_number} for {:?}/{:?}",
                    version.version,
                    role.as_str(),
                    record.name
                ),
            }
        );
        rows.push((
            SkillVersionRow::from_version(role, &record.name, version),
            skill_version_script_rows(role, &record.name, version),
        ));
    }

    diesel::insert_into(skills::table)
        .values((
            skills::role.eq(role.as_str()),
            skills::name.eq(&record.name),
            skills::current_version.eq(record.current_version.to_string()),
        ))
        .on_conflict((skills::role, skills::name))
        .do_update()
        .set(skills::current_version.eq(diesel::upsert::excluded(skills::current_version)))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    for (version_row, script_rows) in rows {
        diesel::insert_into(skill_versions::table)
            .values((
                skill_versions::role.eq(version_row.role),
                skill_versions::skill_name.eq(version_row.skill_name),
                skill_versions::version.eq(version_row.version),
                skill_versions::id.eq(version_row.id),
                skill_versions::created_at.eq(version_row.created_at),
                skill_versions::description.eq(version_row.description),
                skill_versions::body.eq(version_row.body),
                skill_versions::enable_coco_shim.eq(version_row.enable_coco_shim),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;

        for row in script_rows {
            diesel::insert_into(skill_version_scripts::table)
                .values((
                    skill_version_scripts::role.eq(row.role),
                    skill_version_scripts::skill_name.eq(row.skill_name),
                    skill_version_scripts::version.eq(row.version),
                    skill_version_scripts::ordinal.eq(row.ordinal),
                    skill_version_scripts::path.eq(row.path),
                    skill_version_scripts::content.eq(row.content),
                ))
                .execute(connection)
                .await
                .context(QuerySqliteStoreSnafu {
                    path: path.to_owned(),
                })?;
        }
    }
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

#[async_trait]
impl NodeStore for SqliteGraphStore {
    fn root_id(&self) -> String {
        self.root_id.clone()
    }

    async fn append(&self, _node: NewNode) -> Result<String> {
        self.ensure_read_only()
    }

    async fn ancestry(&self, head_ref: &str) -> Result<Vec<Node>> {
        let head_ref = head_ref.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move { load_ancestry_nodes(connection, &path, &head_ref).await })
        })
        .await
    }

    async fn log(&self, base_ref: &str, head_ref: &str) -> Result<Vec<Node>> {
        let base_ref = base_ref.to_owned();
        let head_ref = head_ref.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move { load_log_nodes(connection, &path, &base_ref, &head_ref).await })
        })
        .await
    }

    async fn get_node(&self, id: &str) -> Result<Node> {
        self.get_node_by_prefix_or_branch(id).await
    }

    async fn list_children(&self, node_id: &str) -> Result<Vec<Node>> {
        let node_id = node_id.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move { load_child_nodes(connection, &path, &node_id).await })
        })
        .await
    }
}

#[async_trait]
impl BranchStore for SqliteGraphStore {
    async fn fork(&self, _name: &str, _from_ref: &str) -> Result<String> {
        self.ensure_read_only()
    }

    async fn get_branch_head(&self, name: &str) -> Result<String> {
        self.branch_head(name).await?.context(BranchNotFoundSnafu {
            name: name.to_owned(),
        })
    }

    async fn delete_branch(&self, _name: &str) -> Result<()> {
        self.ensure_read_only()
    }

    async fn set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _new_head: &str,
    ) -> Result<()> {
        self.ensure_read_only()
    }

    async fn append_nodes_and_set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _parent: &str,
        _nodes: Vec<NewNodeContent>,
    ) -> Result<String> {
        self.ensure_read_only()
    }

    async fn append_nodes_and_set_branch_head_to(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _parent: &str,
        _new_head: &str,
        _nodes: Vec<NewNodeContent>,
    ) -> Result<String> {
        self.ensure_read_only()
    }

    async fn append_nodes_and_set_branch_head_with_session_state(
        &self,
        _update: BranchAppendSessionState,
    ) -> Result<String> {
        self.ensure_read_only()
    }
}

#[async_trait]
impl SessionStore for SqliteGraphStore {
    async fn list_session_states(&self) -> Result<std::collections::HashMap<String, SessionState>> {
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move { load_session_states(connection, &path).await })
        })
        .await
    }

    async fn get_session_state(&self, name: &str) -> Result<SessionState> {
        let name = name.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move { load_session_state(connection, &path, &name).await })
        })
        .await
    }

    async fn set_session_state(
        &self,
        _name: &str,
        _expected: Option<&SessionState>,
        _next: SessionState,
    ) -> Result<SessionState> {
        self.ensure_read_only()
    }

    async fn rebase_session(&self, _name: &str, _patch: &SessionAnchorPatch) -> Result<String> {
        self.ensure_read_only()
    }

    async fn handoff_session(
        &self,
        _name: &str,
        _patch: &SessionAnchorPatch,
        _prompt: &str,
    ) -> Result<String> {
        self.ensure_read_only()
    }
}

#[async_trait]
impl NodeStore for SqliteStore {
    fn root_id(&self) -> String {
        self.root_id.clone()
    }

    async fn append(&self, node: NewNode) -> Result<String> {
        self.ensure_writable()?;
        let node = Node::new(
            node.parent,
            node.role,
            node.metadata,
            node.kind,
            jiff::Timestamp::now(),
        );
        let _write = self.database.inner.write.lock().await;
        let mut connection = self.connect().await?;
        validate_new_node(&mut connection, &self.database_path, &node).await?;
        persist_node(&mut connection, &self.database_path, &node).await?;
        Ok(node.id)
    }

    async fn ancestry(&self, head_ref: &str) -> Result<Vec<Node>> {
        let mut connection = self.connect().await?;
        load_ancestry_nodes(&mut connection, &self.database_path, head_ref).await
    }

    async fn log(&self, base_ref: &str, head_ref: &str) -> Result<Vec<Node>> {
        let mut connection = self.connect().await?;
        load_log_nodes(&mut connection, &self.database_path, base_ref, head_ref).await
    }

    async fn get_node(&self, id: &str) -> Result<Node> {
        let mut connection = self.connect().await?;
        load_node_by_prefix_or_branch(&mut connection, &self.database_path, id).await
    }

    async fn list_children(&self, node_id: &str) -> Result<Vec<Node>> {
        let mut connection = self.connect().await?;
        load_child_nodes(&mut connection, &self.database_path, node_id).await
    }
}

#[async_trait]
impl BranchStore for SqliteStore {
    async fn fork(&self, name: &str, from_ref: &str) -> Result<String> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        create_branch(&mut connection, &self.database_path, name, from_ref).await
    }

    async fn get_branch_head(&self, name: &str) -> Result<String> {
        let mut connection = self.connect().await?;
        load_branch_head(&mut connection, &self.database_path, name).await
    }

    async fn delete_branch(&self, name: &str) -> Result<()> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        delete_branch_checked(&mut connection, &self.database_path, name).await
    }

    async fn set_branch_head(
        &self,
        name: &str,
        expected_old_head: &str,
        new_head: &str,
    ) -> Result<()> {
        self.ensure_writable()?;
        let _write = self.database.inner.write.lock().await;
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

    async fn append_nodes_and_set_branch_head(
        &self,
        name: &str,
        expected_old_head: &str,
        parent: &str,
        nodes: Vec<NewNodeContent>,
    ) -> Result<String> {
        self.ensure_writable()?;
        let _write = self.database.inner.write.lock().await;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<String, SqliteTransactionError, _>(async |connection| {
                append_nodes_and_set_branch_head_in_transaction(
                    connection,
                    &self.database_path,
                    name,
                    expected_old_head,
                    parent,
                    None,
                    nodes,
                )
                .await
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }

    async fn append_nodes_and_set_branch_head_to(
        &self,
        name: &str,
        expected_old_head: &str,
        parent: &str,
        new_head: &str,
        nodes: Vec<NewNodeContent>,
    ) -> Result<String> {
        self.ensure_writable()?;
        let _write = self.database.inner.write.lock().await;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<String, SqliteTransactionError, _>(async |connection| {
                append_nodes_and_set_branch_head_in_transaction(
                    connection,
                    &self.database_path,
                    name,
                    expected_old_head,
                    parent,
                    Some(new_head),
                    nodes,
                )
                .await
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }

    async fn append_nodes_and_set_branch_head_with_session_state(
        &self,
        update: BranchAppendSessionState,
    ) -> Result<String> {
        self.ensure_writable()?;
        let _write = self.database.inner.write.lock().await;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<String, SqliteTransactionError, _>(async |connection| {
                append_nodes_and_set_branch_head_with_session_state_in_transaction(
                    connection,
                    &self.database_path,
                    update,
                )
                .await
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }
}

#[async_trait]
impl SessionStore for SqliteStore {
    async fn list_session_states(&self) -> Result<std::collections::HashMap<String, SessionState>> {
        let mut connection = self.connect().await?;
        load_session_states(&mut connection, &self.database_path).await
    }

    async fn get_session_state(&self, name: &str) -> Result<SessionState> {
        let mut connection = self.connect().await?;
        load_session_state(&mut connection, &self.database_path, name).await
    }

    async fn set_session_state(
        &self,
        name: &str,
        expected: Option<&SessionState>,
        next: SessionState,
    ) -> Result<SessionState> {
        let expected = expected.cloned();
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        update_session_state(
            &mut connection,
            &self.database_path,
            name,
            expected.as_ref(),
            next,
        )
        .await
    }

    async fn rebase_session(&self, name: &str, patch: &SessionAnchorPatch) -> Result<String> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        let (new_head, _) = connection
            .immediate_transaction::<(String, Vec<Node>), SqliteTransactionError, _>(
                async |connection| {
                    let expected_old_head = load_branch_head(connection, &self.database_path, name)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    let mut chain = load_session_chain(connection, &self.database_path, name)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    chain.reverse();
                    let session_node = chain
                        .as_slice()
                        .first()
                        .expect("session chain should not be empty");
                    let session_anchor =
                        session_anchor_from_node(&self.database_path, session_node)
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
                        upsert_node_without_transaction(connection, &self.database_path, &new_node)
                            .await
                            .map_err(SqliteTransactionError::Operation)?;
                        previous_new_id = Some(new_node.id.clone());
                        new_head = new_node.id.clone();
                        nodes.push(new_node);
                    }

                    update_branch_head_after_session_write(
                        connection,
                        &self.database_path,
                        name,
                        &expected_old_head,
                        &new_head,
                    )
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                    Ok((new_head, nodes))
                },
            )
            .await
            .map_err(|error| error.into_store_error(&self.database_path))?;
        Ok(new_head)
    }

    async fn handoff_session(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
        prompt: &str,
    ) -> Result<String> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        let prompt = prompt.trim().to_owned();
        ensure!(!prompt.is_empty(), InvalidSessionHandoffPromptSnafu);
        let (new_head, _) = connection
            .immediate_transaction::<(String, Node), SqliteTransactionError, _>(
                async |connection| {
                    let expected_old_head = load_branch_head(connection, &self.database_path, name)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    let chain = load_session_chain(connection, &self.database_path, name)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    let session_node = chain.last().expect("session chain should not be empty");
                    let session_anchor =
                        session_anchor_from_node(&self.database_path, session_node)
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
                    validate_new_node(connection, &self.database_path, &node)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    persist_node_without_transaction(connection, &self.database_path, &node)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    update_branch_head_after_session_write(
                        connection,
                        &self.database_path,
                        name,
                        &expected_old_head,
                        &node.id,
                    )
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                    Ok((node.id.clone(), node))
                },
            )
            .await
            .map_err(|error| error.into_store_error(&self.database_path))?;
        Ok(new_head)
    }
}

#[async_trait]
impl JobStore for SqliteStore {
    async fn submit_job(&self, branch: &str, base: &str) -> Result<Job> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
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
                            path: self.database_path.clone(),
                        })
                        .map_err(SqliteTransactionError::Operation)?
                        == 0
                    {
                        return submit_job_with_id_in_transaction(
                            connection,
                            &self.database_path,
                            &job_id,
                            branch,
                            base,
                        )
                        .await;
                    }
                }
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }

    async fn submit_job_with_prompt_base(
        &self,
        branch: &str,
        prompt: PromptAnchor,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionAnchorPatch>,
    ) -> Result<Job> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<Job, SqliteTransactionError, _>(async |connection| {
                let job_id = loop {
                    let job_id = format!("job-{}", nanoid::nanoid!());
                    if jobs::table
                        .filter(jobs::job_id.eq(&job_id))
                        .count()
                        .get_result::<i64>(connection)
                        .await
                        .context(QuerySqliteStoreSnafu {
                            path: self.database_path.clone(),
                        })
                        .map_err(SqliteTransactionError::Operation)?
                        == 0
                    {
                        break job_id;
                    }
                };
                let base = append_prompt_job_base_in_transaction(
                    connection,
                    &self.database_path,
                    branch,
                    prompt,
                    merge_parents,
                    session_patch,
                )
                .await?;
                submit_job_with_id_in_transaction(
                    connection,
                    &self.database_path,
                    &job_id,
                    branch,
                    &base,
                )
                .await
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }

    async fn submit_job_with_id(&self, job_id: &str, branch: &str, base: &str) -> Result<Job> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<Job, SqliteTransactionError, _>(async |connection| {
                submit_job_with_id_in_transaction(
                    connection,
                    &self.database_path,
                    job_id,
                    branch,
                    base,
                )
                .await
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }

    async fn submit_job_with_id_and_prompt_base(
        &self,
        job_id: &str,
        branch: &str,
        prompt: PromptAnchor,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionAnchorPatch>,
    ) -> Result<Job> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<Job, SqliteTransactionError, _>(async |connection| {
                let base = append_prompt_job_base_in_transaction(
                    connection,
                    &self.database_path,
                    branch,
                    prompt,
                    merge_parents,
                    session_patch,
                )
                .await?;
                submit_job_with_id_in_transaction(
                    connection,
                    &self.database_path,
                    job_id,
                    branch,
                    &base,
                )
                .await
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }

    async fn get_job(&self, job_id: &str) -> Result<Job> {
        let mut connection = self.connect().await?;
        load_job(&mut connection, &self.database_path, job_id).await
    }

    async fn list_jobs(&self) -> Result<std::collections::HashMap<String, Job>> {
        let mut connection = self.connect().await?;
        load_job_map(&mut connection, &self.database_path).await
    }

    async fn set_job_status(
        &self,
        job_id: &str,
        expected: JobStatus,
        next: JobStatus,
    ) -> Result<Job> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<Job, SqliteTransactionError, _>(async |connection| {
                let mut job = load_job(connection, &self.database_path, job_id)
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
                persist_job(connection, &self.database_path, &job)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                Ok(job)
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }

    async fn set_job_work_branch(
        &self,
        job_id: &str,
        expected_work_branch: &str,
        next_work_branch: &str,
    ) -> Result<Job> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<Job, SqliteTransactionError, _>(async |connection| {
                load_branch_head(connection, &self.database_path, next_work_branch)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                let jobs = load_job_map(connection, &self.database_path)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                if let Some(active_job) = jobs.values().find(|job| {
                    job.job_id != job_id && job_uses_active_branch(job, next_work_branch)
                }) {
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
                persist_job(connection, &self.database_path, &job)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                Ok(job)
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }
}

#[async_trait]
impl MessageQueueStore for SqliteStore {
    async fn enqueue_message(
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

    async fn dequeue_message(&self, queue: &str) -> Result<Option<MessageQueueItem>> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        dequeue_message_queue_item(&mut connection, &self.database_path, queue).await
    }

    async fn peek_message(&self, queue: &str) -> Result<Option<MessageQueueItem>> {
        let mut connection = self.connect().await?;
        Ok(
            load_queue_messages(&mut connection, &self.database_path, queue)
                .await?
                .into_iter()
                .next(),
        )
    }

    async fn list_queue_messages(&self, queue: &str) -> Result<Vec<MessageQueueItem>> {
        let mut connection = self.connect().await?;
        load_queue_messages(&mut connection, &self.database_path, queue).await
    }

    async fn list_message_queues(&self) -> Result<Vec<String>> {
        let mut connection = self.connect().await?;
        load_message_queue_names(&mut connection, &self.database_path).await
    }
}

#[async_trait]
impl PresetStore for SqliteStore {
    async fn list_preset_records(&self) -> Result<std::collections::HashMap<String, PresetRecord>> {
        let mut connection = self.connect().await?;
        load_preset_records(&mut connection, &self.database_path).await
    }

    async fn get_preset_record(&self, name: &str) -> Result<PresetRecord> {
        let mut connection = self.connect().await?;
        load_preset_record(&mut connection, &self.database_path, name).await
    }

    async fn set_preset(&self, name: &str, config: Preset) -> Result<PresetRecord> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        set_preset_record(&mut connection, &self.database_path, name, config).await
    }

    async fn rollback_preset(&self, name: &str, target_version: u64) -> Result<PresetRecord> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        rollback_preset_record(&mut connection, &self.database_path, name, target_version).await
    }

    async fn delete_preset(&self, name: &str) -> Result<()> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        delete_preset_record_checked(&mut connection, &self.database_path, name).await
    }
}

#[async_trait]
impl SkillStore for SqliteStore {
    async fn list_skills(&self, role: SessionRole) -> Result<Vec<SkillRecord>> {
        let mut connection = self.connect().await?;
        Ok(load_skill_groups(&mut connection, &self.database_path)
            .await?
            .for_role(role)
            .values()
            .cloned()
            .collect())
    }

    async fn get_skill(&self, role: SessionRole, name: &str) -> Result<SkillRecord> {
        let mut connection = self.connect().await?;
        load_skill_record(&mut connection, &self.database_path, role, name).await
    }

    async fn add_skill(
        &self,
        role: SessionRole,
        name: &str,
        spec: SkillVersionSpec,
    ) -> Result<SkillRecord> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        add_skill_record(&mut connection, &self.database_path, role, name, spec).await
    }

    async fn update_skill(
        &self,
        role: SessionRole,
        name: &str,
        patch: &SkillUpdatePatch,
    ) -> Result<SkillRecord> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        update_skill_record(&mut connection, &self.database_path, role, name, patch).await
    }

    async fn rollback_skill(
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

impl ProcessShareableStore for SqliteStore {
    fn store_path(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MessageQueueItem, NodeAnchorPromptAttachmentRow, NodeAnchorSessionPatchRow,
        NodeAnchorSessionPatchToolRow, NodeAnchorSessionRow, NodeAnchorSessionToolRow,
        NodeAnchorSkillInvocationRow, NodeAnchorSkillResultRow, NodeMetadataRow, NodeToolResultRow,
        NodeToolUseRow, SqliteGraphStore, SqliteStore,
    };
    use crate::schema::{
        jobs, node_anchor_prompt_attachments, node_anchor_session_patch_tools,
        node_anchor_session_patches, node_anchor_session_tools, node_anchor_sessions,
        node_anchor_skill_invocations, node_anchor_skill_results, node_metadata, node_relations,
        node_tool_results, node_tool_uses, nodes, preset_version_tools, preset_versions, sessions,
        skill_version_scripts, skill_versions, store_meta,
    };
    use crate::{
        Anchor, BackendMetadata, BranchStore, Job, JobStatus, JobStore, Kind, MergeParent,
        MessageQueueStore, NewNode, Node, NodeStore, PauseReason, Preset, PresetStore,
        PromptAnchor, PromptAttachment, PromptImageAttachment, Role, SessionAnchor,
        SessionAnchorPatch, SessionRole, SessionState, SessionStore, SkillInvocationAnchor,
        SkillInvocationMode, SkillResultAnchor, SkillRuntimeContext, SkillScript, SkillStore,
        SkillUpdatePatch, SkillVersionSpec, Tool, ToolResult, ToolUse,
    };
    use diesel::connection::InstrumentationEvent;
    use diesel::prelude::*;
    use diesel_async::{AsyncConnection, RunQueryDsl};
    use diesel_migrations::MigrationHarness;
    use std::sync::{Arc, Mutex, mpsc};
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

    fn rich_session_anchor_node(parent: &str, merge_parents: Vec<MergeParent>) -> NewNode {
        NewNode {
            parent: parent.to_owned(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(
                merge_parents,
                SessionAnchor {
                    role: SessionRole::Runner,
                    provider_profile: Some("runner-profile".to_owned()),
                    provider: Some("openai".to_owned()),
                    model: "gpt-5.4".to_owned(),
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
                            name: "finish".to_owned(),
                            description: "Finish the task".to_owned(),
                            input_schema: serde_json::json!({"type": "object"}),
                        },
                    ],
                    system_prompt: "system".to_owned(),
                    prompt: "session prompt".to_owned(),
                    temperature: Some(0.25),
                    max_tokens: Some(u64::MAX),
                    additional_params: Some(serde_json::json!({
                        "reasoning": {"effort": "high"}
                    })),
                    enable_coco_shim: true,
                    active_skill: Some(SkillRuntimeContext {
                        name: "compact".to_owned(),
                        handoff: Some("Preserve the decisions".to_owned()),
                    }),
                },
            )),
        }
    }

    fn rich_session_patch_anchor_node(parent: &str, merge_parents: Vec<MergeParent>) -> NewNode {
        NewNode {
            parent: parent.to_owned(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session_patch(
                merge_parents,
                SessionAnchorPatch {
                    role: Some(SessionRole::Runner),
                    provider_profile: Some(Some("runner-profile".to_owned())),
                    provider: Some(Some("openai".to_owned())),
                    model: Some("gpt-5.4".to_owned()),
                    tools: Some(vec![Tool {
                        name: "lookup".to_owned(),
                        description: "Look up a value".to_owned(),
                        input_schema: serde_json::json!({"type": "object"}),
                    }]),
                    system_prompt: Some("patched system".to_owned()),
                    temperature: Some(Some(0.5)),
                    max_tokens: Some(Some(u64::MAX)),
                    additional_params: Some(Some(serde_json::json!("priority"))),
                    enable_coco_shim: Some(true),
                },
            )),
        }
    }

    fn rich_prompt_anchor_node(parent: &str, merge_parents: Vec<MergeParent>) -> NewNode {
        NewNode {
            parent: parent.to_owned(),
            role: Role::User,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                merge_parents,
                PromptAnchor {
                    prompt: "Inspect these images".to_owned(),
                    attachments: vec![
                        PromptAttachment::Image(PromptImageAttachment {
                            id: "image-a".to_owned(),
                            width: Some(u32::MAX),
                            height: Some(1080),
                            file_size: Some(u64::MAX),
                            media_type: Some("image/png".to_owned()),
                        }),
                        PromptAttachment::Image(PromptImageAttachment {
                            id: "image-b".to_owned(),
                            width: None,
                            height: None,
                            file_size: None,
                            media_type: None,
                        }),
                    ],
                },
            )),
        }
    }

    fn skill_invocation_anchor_node(parent: &str, merge_parents: Vec<MergeParent>) -> NewNode {
        NewNode {
            parent: parent.to_owned(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::skill_invocation(
                merge_parents,
                SkillInvocationAnchor {
                    skill_name: "compact".to_owned(),
                    mode: SkillInvocationMode::Handoff {
                        prompt: "Compact this branch".to_owned(),
                    },
                },
            )),
        }
    }

    fn skill_result_anchor_node(parent: &str, merge_parents: Vec<MergeParent>) -> NewNode {
        NewNode {
            parent: parent.to_owned(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::skill_result(
                merge_parents,
                SkillResultAnchor {
                    skill_name: "compact".to_owned(),
                    output: "First line\nSecond line with \"quotes\"".to_owned(),
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

    async fn node_tool_use_rows(store: &SqliteStore, node_id: &str) -> Vec<NodeToolUseRow> {
        let mut connection = store.connect().await.unwrap();
        node_tool_uses::table
            .filter(node_tool_uses::node_id.eq(node_id))
            .select((
                node_tool_uses::node_id,
                node_tool_uses::ordinal,
                node_tool_uses::tool_use_id,
                node_tool_uses::name,
                node_tool_uses::input_json,
            ))
            .order(node_tool_uses::ordinal)
            .load::<NodeToolUseRow>(&mut connection)
            .await
            .unwrap()
    }

    async fn node_tool_result_rows(store: &SqliteStore, node_id: &str) -> Vec<NodeToolResultRow> {
        let mut connection = store.connect().await.unwrap();
        node_tool_results::table
            .filter(node_tool_results::node_id.eq(node_id))
            .select((
                node_tool_results::node_id,
                node_tool_results::ordinal,
                node_tool_results::tool_result_id,
                node_tool_results::output,
            ))
            .order(node_tool_results::ordinal)
            .load::<NodeToolResultRow>(&mut connection)
            .await
            .unwrap()
    }

    async fn node_anchor_session_row(store: &SqliteStore, node_id: &str) -> NodeAnchorSessionRow {
        let mut connection = store.connect().await.unwrap();
        node_anchor_sessions::table
            .filter(node_anchor_sessions::node_id.eq(node_id))
            .select((
                node_anchor_sessions::node_id,
                node_anchor_sessions::role,
                node_anchor_sessions::provider_profile,
                node_anchor_sessions::provider,
                node_anchor_sessions::model,
                node_anchor_sessions::system_prompt,
                node_anchor_sessions::prompt,
                node_anchor_sessions::temperature,
                node_anchor_sessions::max_tokens,
                node_anchor_sessions::additional_params_json,
                node_anchor_sessions::enable_coco_shim,
                node_anchor_sessions::active_skill_name,
                node_anchor_sessions::active_skill_handoff,
            ))
            .get_result::<NodeAnchorSessionRow>(&mut connection)
            .await
            .unwrap()
    }

    async fn node_anchor_session_tool_rows(
        store: &SqliteStore,
        node_id: &str,
    ) -> Vec<NodeAnchorSessionToolRow> {
        let mut connection = store.connect().await.unwrap();
        node_anchor_session_tools::table
            .filter(node_anchor_session_tools::node_id.eq(node_id))
            .select((
                node_anchor_session_tools::node_id,
                node_anchor_session_tools::ordinal,
                node_anchor_session_tools::name,
                node_anchor_session_tools::description,
                node_anchor_session_tools::input_schema_json,
            ))
            .order(node_anchor_session_tools::ordinal)
            .load::<NodeAnchorSessionToolRow>(&mut connection)
            .await
            .unwrap()
    }

    async fn node_anchor_session_patch_row(
        store: &SqliteStore,
        node_id: &str,
    ) -> NodeAnchorSessionPatchRow {
        let mut connection = store.connect().await.unwrap();
        node_anchor_session_patches::table
            .filter(node_anchor_session_patches::node_id.eq(node_id))
            .select((
                node_anchor_session_patches::node_id,
                node_anchor_session_patches::role,
                node_anchor_session_patches::provider_profile_present,
                node_anchor_session_patches::provider_profile,
                node_anchor_session_patches::provider_present,
                node_anchor_session_patches::provider,
                node_anchor_session_patches::model,
                node_anchor_session_patches::tools_present,
                node_anchor_session_patches::system_prompt,
                node_anchor_session_patches::temperature_present,
                node_anchor_session_patches::temperature,
                node_anchor_session_patches::max_tokens_present,
                node_anchor_session_patches::max_tokens,
                node_anchor_session_patches::additional_params_present,
                node_anchor_session_patches::additional_params_json,
                node_anchor_session_patches::enable_coco_shim,
            ))
            .get_result::<NodeAnchorSessionPatchRow>(&mut connection)
            .await
            .unwrap()
    }

    async fn node_anchor_session_patch_tool_rows(
        store: &SqliteStore,
        node_id: &str,
    ) -> Vec<NodeAnchorSessionPatchToolRow> {
        let mut connection = store.connect().await.unwrap();
        node_anchor_session_patch_tools::table
            .filter(node_anchor_session_patch_tools::node_id.eq(node_id))
            .select((
                node_anchor_session_patch_tools::node_id,
                node_anchor_session_patch_tools::ordinal,
                node_anchor_session_patch_tools::name,
                node_anchor_session_patch_tools::description,
                node_anchor_session_patch_tools::input_schema_json,
            ))
            .order(node_anchor_session_patch_tools::ordinal)
            .load::<NodeAnchorSessionPatchToolRow>(&mut connection)
            .await
            .unwrap()
    }

    async fn node_anchor_prompt_attachment_rows(
        store: &SqliteStore,
        node_id: &str,
    ) -> Vec<NodeAnchorPromptAttachmentRow> {
        let mut connection = store.connect().await.unwrap();
        node_anchor_prompt_attachments::table
            .filter(node_anchor_prompt_attachments::node_id.eq(node_id))
            .select((
                node_anchor_prompt_attachments::node_id,
                node_anchor_prompt_attachments::ordinal,
                node_anchor_prompt_attachments::kind,
                node_anchor_prompt_attachments::attachment_id,
                node_anchor_prompt_attachments::width,
                node_anchor_prompt_attachments::height,
                node_anchor_prompt_attachments::file_size,
                node_anchor_prompt_attachments::media_type,
            ))
            .order(node_anchor_prompt_attachments::ordinal)
            .load::<NodeAnchorPromptAttachmentRow>(&mut connection)
            .await
            .unwrap()
    }

    async fn node_anchor_skill_invocation_row(
        store: &SqliteStore,
        node_id: &str,
    ) -> NodeAnchorSkillInvocationRow {
        let mut connection = store.connect().await.unwrap();
        node_anchor_skill_invocations::table
            .filter(node_anchor_skill_invocations::node_id.eq(node_id))
            .select((
                node_anchor_skill_invocations::node_id,
                node_anchor_skill_invocations::skill_name,
                node_anchor_skill_invocations::mode,
                node_anchor_skill_invocations::prompt,
            ))
            .get_result::<NodeAnchorSkillInvocationRow>(&mut connection)
            .await
            .unwrap()
    }

    async fn node_anchor_skill_result_row(
        store: &SqliteStore,
        node_id: &str,
    ) -> NodeAnchorSkillResultRow {
        let mut connection = store.connect().await.unwrap();
        node_anchor_skill_results::table
            .filter(node_anchor_skill_results::node_id.eq(node_id))
            .select((
                node_anchor_skill_results::node_id,
                node_anchor_skill_results::skill_name,
                node_anchor_skill_results::output,
            ))
            .get_result::<NodeAnchorSkillResultRow>(&mut connection)
            .await
            .unwrap()
    }

    async fn node_kind(store: &SqliteStore, node_id: &str) -> String {
        let mut connection = store.connect().await.unwrap();
        nodes::table
            .filter(nodes::id.eq(node_id))
            .select(nodes::kind)
            .get_result::<String>(&mut connection)
            .await
            .unwrap()
    }

    async fn node_content(store: &SqliteStore, node_id: &str) -> Option<String> {
        let mut connection = store.connect().await.unwrap();
        nodes::table
            .filter(nodes::id.eq(node_id))
            .select(nodes::content)
            .get_result::<Option<String>>(&mut connection)
            .await
            .unwrap()
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
        super::table_count(&mut connection, &store.database_path, table_name)
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

    fn valid_job_row() -> super::JobRow {
        super::JobRow {
            job_id: "job-test".to_owned(),
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            finished_at: None,
            branch: "main".to_owned(),
            work_branch: "main".to_owned(),
            base: "base".to_owned(),
            status: "queued".to_owned(),
        }
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

    fn revert_store_migrations_to(
        connection: &mut diesel::sqlite::SqliteConnection,
        target_version: i32,
    ) {
        loop {
            let current_version = diesel::RunQueryDsl::get_result::<Option<String>>(
                super::__diesel_schema_migrations::table
                    .select(diesel::dsl::max(super::__diesel_schema_migrations::version)),
                connection,
            )
            .unwrap()
            .map(|version| version.trim_start_matches('0').parse::<i32>().unwrap())
            .unwrap_or_default();
            if current_version <= target_version {
                break;
            }
            connection
                .revert_last_migration(super::STORE_MIGRATIONS)
                .unwrap();
        }
    }

    fn create_v6_store_with_legacy_data(path: &std::path::Path) {
        use diesel::connection::SimpleConnection;

        std::fs::create_dir(path).unwrap();
        let database_path = super::sqlite_database_path(path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        connection
            .batch_execute(include_str!(
                "../../migrations/00000000000006_current_store_schema/up.sql"
            ))
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

    #[tokio::test]
    async fn open_creates_sqlite_database_and_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");

        let store = SqliteStore::open(&path).await.unwrap();

        assert!(store.database_path().is_file());
        assert_eq!(store.schema_version().await.unwrap(), 22);
        assert!(!nodes_have_anchor_columns(&store).await);
        assert!(!node_has_kind_json_column(&store).await);
        assert!(!node_anchor_table_exists(&store).await);
        assert!(!table_exists(&store, "node_anchor_prompts").await);
        assert!(!job_has_payload_json_column(&store).await);
    }

    #[tokio::test]
    async fn open_migrates_v6_data_to_current_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        create_v6_store_with_legacy_data(&path);

        let store = SqliteStore::open(&path).await.unwrap();

        assert_eq!(store.schema_version().await.unwrap(), 22);
        assert!(!nodes_have_anchor_columns(&store).await);
        assert_eq!(
            node_tool_use_rows(&store, "tool-use-node").await,
            vec![
                NodeToolUseRow {
                    node_id: "tool-use-node".to_owned(),
                    ordinal: 0,
                    tool_use_id: "tool-call-1".to_owned(),
                    name: "exec_command".to_owned(),
                    input_json: r#"{"cmd":"pwd"}"#.to_owned(),
                },
                NodeToolUseRow {
                    node_id: "tool-use-node".to_owned(),
                    ordinal: 1,
                    tool_use_id: "tool-call-2".to_owned(),
                    name: "exec_command".to_owned(),
                    input_json: r#"{"cmd":"ls"}"#.to_owned(),
                },
            ]
        );
        assert_eq!(
            node_tool_result_rows(&store, "tool-result-node").await,
            vec![
                NodeToolResultRow {
                    node_id: "tool-result-node".to_owned(),
                    ordinal: 0,
                    tool_result_id: "tool-call-1".to_owned(),
                    output: "ok".to_owned(),
                },
                NodeToolResultRow {
                    node_id: "tool-result-node".to_owned(),
                    ordinal: 1,
                    tool_result_id: "tool-call-2".to_owned(),
                    output: "done".to_owned(),
                },
            ]
        );
        assert_eq!(
            node_content(&store, "root").await.as_deref(),
            Some("The Big Bang")
        );
        assert_eq!(node_content(&store, "tool-use-node").await, None);
        assert!(!node_has_kind_json_column(&store).await);
        assert!(!node_has_metadata_json_column(&store).await);
        assert!(!job_has_payload_json_column(&store).await);
        assert_eq!(
            store.get_job("job-v6").await.unwrap(),
            Job {
                job_id: "job-v6".to_owned(),
                created_at: "2026-03-25T09:10:13Z".parse().unwrap(),
                finished_at: None,
                branch: "main".to_owned(),
                work_branch: "main".to_owned(),
                base: "root".to_owned(),
                status: JobStatus::Running,
            }
        );
        let tool_use = store.get_node("tool-use-node").await.unwrap();
        assert_eq!(tool_use.kind.as_tool_uses().unwrap().len(), 2);
        assert_eq!(
            tool_use.metadata,
            Some(vec![
                BackendMetadata {
                    execution_id: Some("execution-1".to_owned()),
                    call_id: Some("call-1".to_owned()),
                },
                BackendMetadata {
                    execution_id: Some("execution-1".to_owned()),
                    call_id: Some("call-1".to_owned()),
                },
            ])
        );
        let tool_result = store.get_node("tool-result-node").await.unwrap();
        assert_eq!(tool_result.kind.as_tool_results().unwrap().len(), 2);
        assert_eq!(
            tool_result.metadata,
            Some(vec![BackendMetadata {
                execution_id: Some("execution-2".to_owned()),
                call_id: Some("call-result".to_owned()),
            }])
        );
    }

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

    #[tokio::test]
    async fn node_item_migration_down_restores_kind_json() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        create_v6_store_with_legacy_data(&path);
        let store = SqliteStore::open(&path).await.unwrap();
        drop(store);
        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();

        revert_store_migrations_to(&mut connection, 6);

        let tool_use = diesel::RunQueryDsl::get_result::<LegacyKindJson>(
            diesel::sql_query("SELECT kind_json FROM nodes WHERE id = 'tool-use-node'"),
            &mut connection,
        )
        .unwrap();
        let tool_result = diesel::RunQueryDsl::get_result::<LegacyKindJson>(
            diesel::sql_query("SELECT kind_json FROM nodes WHERE id = 'tool-result-node'"),
            &mut connection,
        )
        .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&tool_use.kind_json).unwrap(),
            serde_json::json!({
                "ToolUse": [
                    {
                        "id": "tool-call-1",
                        "name": "exec_command",
                        "input": { "cmd": "pwd" }
                    },
                    {
                        "id": "tool-call-2",
                        "name": "exec_command",
                        "input": { "cmd": "ls" }
                    }
                ]
            })
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&tool_result.kind_json).unwrap(),
            serde_json::json!({
                "ToolResult": [
                    { "id": "tool-call-1", "output": "ok" },
                    { "id": "tool-call-2", "output": "done" }
                ]
            })
        );
        drop(connection);

        let reopened = SqliteStore::open(&path).await.unwrap();
        assert_eq!(
            reopened
                .get_node("tool-use-node")
                .await
                .unwrap()
                .kind
                .as_tool_uses()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            reopened
                .get_node("tool-result-node")
                .await
                .unwrap()
                .kind
                .as_tool_results()
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn node_content_migration_down_restores_kind_json() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        let failure_id = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Failure("boom".to_owned()),
            })
            .await
            .unwrap();
        let tool_use_id = store
            .append(NewNode {
                parent: failure_id.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_uses(Vec::new()),
            })
            .await
            .unwrap();
        let tool_result_id = store
            .append(NewNode {
                parent: tool_use_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::tool_results(Vec::new()),
            })
            .await
            .unwrap();
        let anchor_id = store
            .append(session_anchor_node(&tool_result_id))
            .await
            .unwrap();
        let node_ids = [
            root_id.clone(),
            failure_id.clone(),
            tool_use_id.clone(),
            tool_result_id.clone(),
            anchor_id.clone(),
        ];
        let mut expected = Vec::new();
        for node_id in &node_ids {
            expected.push(store.get_node(node_id).await.unwrap());
        }
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 10);

        for (node_id, expected_json) in [
            (&root_id, serde_json::json!({ "Text": "The Big Bang" })),
            (&failure_id, serde_json::json!({ "Failure": "boom" })),
            (&tool_use_id, serde_json::json!({ "ToolUse": [] })),
            (&tool_result_id, serde_json::json!({ "ToolResult": [] })),
            (&anchor_id, serde_json::json!({ "Anchor": null })),
        ] {
            let row = diesel::RunQueryDsl::get_result::<LegacyKindJson>(
                diesel::sql_query("SELECT kind_json FROM nodes WHERE id = ?")
                    .bind::<diesel::sql_types::Text, _>(node_id),
                &mut connection,
            )
            .unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&row.kind_json).unwrap(),
                expected_json
            );
        }

        connection
            .run_pending_migrations(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert!(!node_has_kind_json_column(&reopened).await);
        for (node_id, expected) in node_ids.iter().zip(expected) {
            assert_eq!(reopened.get_node(node_id).await.unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn node_content_migration_rejects_mismatched_kind_json() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 10);
        diesel::RunQueryDsl::execute(
            diesel::sql_query("UPDATE nodes SET kind_json = ? WHERE id = ?")
                .bind::<diesel::sql_types::Text, _>(r#"{"Failure":"The Big Bang"}"#)
                .bind::<diesel::sql_types::Text, _>(&root_id),
            &mut connection,
        )
        .unwrap();

        let error = connection
            .run_next_migration(super::STORE_MIGRATIONS)
            .unwrap_err();

        assert!(error.to_string().contains("CHECK constraint failed"));
    }

    #[tokio::test]
    async fn node_anchor_session_migration_round_trips_relational_fields() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let merge_parent = store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("merge parent".to_owned()),
            })
            .await
            .unwrap();
        let anchor_id = store
            .append(rich_session_anchor_node(
                &store.root_id(),
                vec![MergeParent::merge(merge_parent)],
            ))
            .await
            .unwrap();
        let expected = store.get_node(&anchor_id).await.unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 11);
        connection
            .run_pending_migrations(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
        assert_eq!(
            node_anchor_session_row(&reopened, &anchor_id).await,
            NodeAnchorSessionRow {
                node_id: anchor_id.clone(),
                role: "runner".to_owned(),
                provider_profile: Some("runner-profile".to_owned()),
                provider: Some("openai".to_owned()),
                model: "gpt-5.4".to_owned(),
                system_prompt: "system".to_owned(),
                prompt: "session prompt".to_owned(),
                temperature: Some(0.25),
                max_tokens: Some(u64::MAX.to_string()),
                additional_params_json: Some(r#"{"reasoning":{"effort":"high"}}"#.to_owned()),
                enable_coco_shim: true,
                active_skill_name: Some("compact".to_owned()),
                active_skill_handoff: Some("Preserve the decisions".to_owned()),
            }
        );
        assert_eq!(
            node_anchor_session_tool_rows(&reopened, &anchor_id).await,
            vec![
                NodeAnchorSessionToolRow {
                    node_id: anchor_id.clone(),
                    ordinal: 0,
                    name: "lookup".to_owned(),
                    description: "Look up a value".to_owned(),
                    input_schema_json:
                        r#"{"properties":{"key":{"type":"string"}},"type":"object"}"#.to_owned(),
                },
                NodeAnchorSessionToolRow {
                    node_id: anchor_id,
                    ordinal: 1,
                    name: "finish".to_owned(),
                    description: "Finish the task".to_owned(),
                    input_schema_json: r#"{"type":"object"}"#.to_owned(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn node_anchor_session_migration_defaults_missing_coco_shim_to_false() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(session_anchor_node(&store.root_id()))
            .await
            .unwrap();
        let expected = store.get_node(&anchor_id).await.unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 11);
        diesel::RunQueryDsl::execute(
            diesel::sql_query(
                "UPDATE node_anchors SET kind_json = json_remove(\
                 kind_json, '$.Anchor.payload.Session.enable_coco_shim') WHERE node_id = ?",
            )
            .bind::<diesel::sql_types::Text, _>(&anchor_id),
            &mut connection,
        )
        .unwrap();
        connection
            .run_pending_migrations(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
    }

    #[tokio::test]
    async fn node_anchor_session_migration_accepts_missing_additional_params() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(session_anchor_node(&store.root_id()))
            .await
            .unwrap();
        let expected = store.get_node(&anchor_id).await.unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 11);
        diesel::RunQueryDsl::execute(
            diesel::sql_query(
                "UPDATE node_anchors SET kind_json = json_remove(\
                 kind_json, '$.Anchor.payload.Session.additional_params') WHERE node_id = ?",
            )
            .bind::<diesel::sql_types::Text, _>(&anchor_id),
            &mut connection,
        )
        .unwrap();
        connection
            .run_pending_migrations(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
    }

    #[tokio::test]
    async fn node_anchor_session_migration_rejects_invalid_payload() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(session_anchor_node(&store.root_id()))
            .await
            .unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 11);
        diesel::RunQueryDsl::execute(
            diesel::sql_query(
                "UPDATE node_anchors SET kind_json = json_set(\
                 kind_json, '$.Anchor.payload.Session.system_prompt', 1) WHERE node_id = ?",
            )
            .bind::<diesel::sql_types::Text, _>(&anchor_id),
            &mut connection,
        )
        .unwrap();

        let error = connection
            .run_next_migration(super::STORE_MIGRATIONS)
            .unwrap_err();

        assert!(error.to_string().contains("CHECK constraint failed"));
    }

    #[tokio::test]
    async fn node_anchor_session_patch_migration_round_trips_relational_fields() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let merge_parent = store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("merge parent".to_owned()),
            })
            .await
            .unwrap();
        let anchor_id = store
            .append(rich_session_patch_anchor_node(
                &store.root_id(),
                vec![MergeParent::shadow(merge_parent)],
            ))
            .await
            .unwrap();
        let expected = store.get_node(&anchor_id).await.unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 12);
        connection
            .run_pending_migrations(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
        assert_eq!(
            node_anchor_session_patch_row(&reopened, &anchor_id).await,
            NodeAnchorSessionPatchRow {
                node_id: anchor_id.clone(),
                role: Some("runner".to_owned()),
                provider_profile_present: true,
                provider_profile: Some("runner-profile".to_owned()),
                provider_present: true,
                provider: Some("openai".to_owned()),
                model: Some("gpt-5.4".to_owned()),
                tools_present: true,
                system_prompt: Some("patched system".to_owned()),
                temperature_present: true,
                temperature: Some(0.5),
                max_tokens_present: true,
                max_tokens: Some(u64::MAX.to_string()),
                additional_params_present: true,
                additional_params_json: Some(r#""priority""#.to_owned()),
                enable_coco_shim: Some(true),
            }
        );
        assert_eq!(
            node_anchor_session_patch_tool_rows(&reopened, &anchor_id).await,
            vec![NodeAnchorSessionPatchToolRow {
                node_id: anchor_id,
                ordinal: 0,
                name: "lookup".to_owned(),
                description: "Look up a value".to_owned(),
                input_schema_json: r#"{"type":"object"}"#.to_owned(),
            }]
        );
    }

    #[tokio::test]
    async fn node_anchor_session_patch_migration_accepts_absent_tools() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session_patch(vec![], SessionAnchorPatch::default())),
            })
            .await
            .unwrap();
        let expected = store.get_node(&anchor_id).await.unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 12);
        connection
            .run_pending_migrations(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
        assert!(
            !node_anchor_session_patch_row(&reopened, &anchor_id)
                .await
                .tools_present
        );
        assert!(
            node_anchor_session_patch_tool_rows(&reopened, &anchor_id)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn node_anchor_session_patch_migration_accepts_missing_optional_fields() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session_patch(vec![], SessionAnchorPatch::default())),
            })
            .await
            .unwrap();
        let expected = store.get_node(&anchor_id).await.unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 12);
        diesel::RunQueryDsl::execute(
            diesel::sql_query(
                "UPDATE node_anchors SET kind_json = json_remove(\
                 kind_json, \
                 '$.Anchor.payload.SessionPatch.role', \
                 '$.Anchor.payload.SessionPatch.provider_profile', \
                 '$.Anchor.payload.SessionPatch.provider', \
                 '$.Anchor.payload.SessionPatch.model', \
                 '$.Anchor.payload.SessionPatch.tools', \
                 '$.Anchor.payload.SessionPatch.system_prompt', \
                 '$.Anchor.payload.SessionPatch.temperature', \
                 '$.Anchor.payload.SessionPatch.max_tokens', \
                 '$.Anchor.payload.SessionPatch.additional_params', \
                 '$.Anchor.payload.SessionPatch.enable_coco_shim' \
                 ) WHERE node_id = ?",
            )
            .bind::<diesel::sql_types::Text, _>(&anchor_id),
            &mut connection,
        )
        .unwrap();
        connection
            .run_pending_migrations(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
        assert_eq!(
            node_anchor_session_patch_row(&reopened, &anchor_id).await,
            NodeAnchorSessionPatchRow {
                node_id: anchor_id,
                role: None,
                provider_profile_present: false,
                provider_profile: None,
                provider_present: false,
                provider: None,
                model: None,
                tools_present: false,
                system_prompt: None,
                temperature_present: false,
                temperature: None,
                max_tokens_present: false,
                max_tokens: None,
                additional_params_present: false,
                additional_params_json: None,
                enable_coco_shim: None,
            }
        );
    }

    #[tokio::test]
    async fn node_anchor_session_patch_migration_rejects_invalid_payload() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(rich_session_patch_anchor_node(&store.root_id(), vec![]))
            .await
            .unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 12);
        diesel::RunQueryDsl::execute(
            diesel::sql_query(
                "UPDATE node_anchors SET kind_json = json_set(\
                 kind_json, '$.Anchor.payload.SessionPatch.role', 'invalid') WHERE node_id = ?",
            )
            .bind::<diesel::sql_types::Text, _>(&anchor_id),
            &mut connection,
        )
        .unwrap();

        let error = connection
            .run_next_migration(super::STORE_MIGRATIONS)
            .unwrap_err();

        assert!(error.to_string().contains("CHECK constraint failed"));
    }

    #[tokio::test]
    async fn node_anchor_prompt_migration_round_trips_relational_fields() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let merge_parent = store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("merge parent".to_owned()),
            })
            .await
            .unwrap();
        let anchor_id = store
            .append(rich_prompt_anchor_node(
                &store.root_id(),
                vec![MergeParent::merge(merge_parent)],
            ))
            .await
            .unwrap();
        let expected = store.get_node(&anchor_id).await.unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 13);
        connection
            .run_pending_migrations(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
        assert_eq!(
            node_content(&reopened, &anchor_id).await.as_deref(),
            Some("Inspect these images")
        );
        assert_eq!(
            node_anchor_prompt_attachment_rows(&reopened, &anchor_id).await,
            vec![
                NodeAnchorPromptAttachmentRow {
                    node_id: anchor_id.clone(),
                    ordinal: 0,
                    kind: "image".to_owned(),
                    attachment_id: "image-a".to_owned(),
                    width: Some(i64::from(u32::MAX)),
                    height: Some(1080),
                    file_size: Some(u64::MAX.to_string()),
                    media_type: Some("image/png".to_owned()),
                },
                NodeAnchorPromptAttachmentRow {
                    node_id: anchor_id,
                    ordinal: 1,
                    kind: "image".to_owned(),
                    attachment_id: "image-b".to_owned(),
                    width: None,
                    height: None,
                    file_size: None,
                    media_type: None,
                },
            ]
        );
    }

    #[tokio::test]
    async fn node_anchor_prompt_migration_rejects_invalid_attachment() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(rich_prompt_anchor_node(&store.root_id(), vec![]))
            .await
            .unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 13);
        diesel::RunQueryDsl::execute(
            diesel::sql_query(
                "UPDATE node_anchors SET kind_json = json_set(\
                 kind_json, '$.Anchor.payload.Prompt.attachments[0].kind', 'video') \
                 WHERE node_id = ?",
            )
            .bind::<diesel::sql_types::Text, _>(&anchor_id),
            &mut connection,
        )
        .unwrap();

        let error = connection
            .run_next_migration(super::STORE_MIGRATIONS)
            .unwrap_err();

        assert!(error.to_string().contains("CHECK constraint failed"));
    }

    #[tokio::test]
    async fn node_anchor_skill_invocation_migration_round_trips_relational_fields() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let merge_parent = store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("merge parent".to_owned()),
            })
            .await
            .unwrap();
        let anchor_id = store
            .append(skill_invocation_anchor_node(
                &store.root_id(),
                vec![MergeParent::shadow(merge_parent)],
            ))
            .await
            .unwrap();
        let expected = store.get_node(&anchor_id).await.unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 14);
        connection
            .run_pending_migrations(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
        assert_eq!(
            node_anchor_skill_invocation_row(&reopened, &anchor_id).await,
            NodeAnchorSkillInvocationRow {
                node_id: anchor_id.clone(),
                skill_name: "compact".to_owned(),
                mode: "handoff".to_owned(),
                prompt: Some("Compact this branch".to_owned()),
            }
        );
    }

    #[tokio::test]
    async fn node_anchor_skill_invocation_migration_rejects_invalid_mode() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(skill_invocation_anchor_node(&store.root_id(), vec![]))
            .await
            .unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 14);
        diesel::RunQueryDsl::execute(
            diesel::sql_query(
                "UPDATE node_anchors SET skill_invocation_mode = 'invalid' WHERE node_id = ?",
            )
            .bind::<diesel::sql_types::Text, _>(&anchor_id),
            &mut connection,
        )
        .unwrap();

        let error = connection
            .run_next_migration(super::STORE_MIGRATIONS)
            .unwrap_err();

        assert!(error.to_string().contains("CHECK constraint failed"));
    }

    #[tokio::test]
    async fn node_anchor_skill_result_migration_round_trips_relational_fields() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let merge_parent = store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("merge parent".to_owned()),
            })
            .await
            .unwrap();
        let anchor_id = store
            .append(skill_result_anchor_node(
                &store.root_id(),
                vec![MergeParent::merge(merge_parent)],
            ))
            .await
            .unwrap();
        let expected = store.get_node(&anchor_id).await.unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 15);
        connection
            .run_pending_migrations(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
        assert_eq!(
            node_anchor_skill_result_row(&reopened, &anchor_id).await,
            NodeAnchorSkillResultRow {
                node_id: anchor_id,
                skill_name: "compact".to_owned(),
                output: "First line\nSecond line with \"quotes\"".to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn node_anchor_skill_result_migration_rejects_invalid_output() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(skill_result_anchor_node(&store.root_id(), vec![]))
            .await
            .unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 15);
        diesel::RunQueryDsl::execute(
            diesel::sql_query(
                "UPDATE node_anchors SET kind_json = json_set(\
                 kind_json, '$.Anchor.payload.SkillResult.output', json('{}')) WHERE node_id = ?",
            )
            .bind::<diesel::sql_types::Text, _>(&anchor_id),
            &mut connection,
        )
        .unwrap();

        let error = connection
            .run_next_migration(super::STORE_MIGRATIONS)
            .unwrap_err();

        assert!(error.to_string().contains("CHECK constraint failed"));
    }

    #[tokio::test]
    async fn node_anchor_kind_json_migration_round_trips_all_payloads() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let merge_parent = store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("merge parent".to_owned()),
            })
            .await
            .unwrap();
        let new_nodes = [
            rich_session_anchor_node(
                &store.root_id(),
                vec![MergeParent::merge(merge_parent.clone())],
            ),
            rich_session_patch_anchor_node(
                &store.root_id(),
                vec![MergeParent::shadow(merge_parent.clone())],
            ),
            rich_prompt_anchor_node(
                &store.root_id(),
                vec![MergeParent::merge(merge_parent.clone())],
            ),
            skill_invocation_anchor_node(
                &store.root_id(),
                vec![MergeParent::shadow(merge_parent.clone())],
            ),
            skill_result_anchor_node(&store.root_id(), vec![MergeParent::merge(merge_parent)]),
        ];
        let mut expected = Vec::new();
        for new_node in new_nodes {
            let node_id = store.append(new_node).await.unwrap();
            expected.push(store.get_node(&node_id).await.unwrap());
        }
        for node in &expected {
            assert_eq!(
                node_kind(&store, &node.id).await,
                super::node_storage_kind(&node.kind)
            );
        }
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 16);
        for node in &expected {
            let row = diesel::RunQueryDsl::get_result::<LegacyKindJson>(
                diesel::sql_query("SELECT kind_json FROM node_anchors WHERE node_id = ?")
                    .bind::<diesel::sql_types::Text, _>(&node.id),
                &mut connection,
            )
            .unwrap();
            assert_eq!(
                serde_json::from_str::<Kind>(&row.kind_json).unwrap(),
                node.kind
            );
        }
        revert_store_migrations_to(&mut connection, 10);
        for node in &expected {
            let row = diesel::RunQueryDsl::get_result::<LegacyKindJson>(
                diesel::sql_query("SELECT kind_json FROM node_anchors WHERE node_id = ?")
                    .bind::<diesel::sql_types::Text, _>(&node.id),
                &mut connection,
            )
            .unwrap();
            let value = serde_json::from_str::<serde_json::Value>(&row.kind_json).unwrap();
            match &node.kind {
                Kind::Anchor(Anchor {
                    payload: crate::AnchorPayload::Session(_),
                    ..
                }) => {
                    assert_eq!(value.pointer("/Anchor/payload/Session/role"), None);
                    assert_eq!(value.pointer("/Anchor/payload/Session/prompt"), None);
                    assert!(
                        value
                            .pointer("/Anchor/payload/Session/system_prompt")
                            .is_some()
                    );
                }
                Kind::Anchor(Anchor {
                    payload: crate::AnchorPayload::SessionPatch(_),
                    ..
                }) => {
                    assert!(value.pointer("/Anchor/payload/SessionPatch").is_some());
                }
                Kind::Anchor(Anchor {
                    payload: crate::AnchorPayload::Prompt(_),
                    ..
                }) => {
                    assert_eq!(value.pointer("/Anchor/payload/Prompt/prompt"), None);
                    assert!(
                        value
                            .pointer("/Anchor/payload/Prompt/attachments")
                            .is_some()
                    );
                }
                Kind::Anchor(Anchor {
                    payload: crate::AnchorPayload::SkillInvocation(_),
                    ..
                }) => {
                    assert_eq!(
                        value.pointer("/Anchor/payload/SkillInvocation/skill_name"),
                        None
                    );
                    assert_eq!(
                        value.pointer("/Anchor/payload/SkillInvocation/mode/kind"),
                        None
                    );
                    assert_eq!(
                        value.pointer("/Anchor/payload/SkillInvocation/mode/prompt"),
                        None
                    );
                }
                Kind::Anchor(Anchor {
                    payload: crate::AnchorPayload::SkillResult(_),
                    ..
                }) => {
                    assert_eq!(
                        value.pointer("/Anchor/payload/SkillResult/skill_name"),
                        None
                    );
                    assert!(
                        value
                            .pointer("/Anchor/payload/SkillResult/output")
                            .is_some()
                    );
                }
                Kind::ToolUse(_) | Kind::ToolResult(_) | Kind::Text(_) | Kind::Failure(_) => {
                    panic!("expected anchor node")
                }
            }
        }
        connection
            .run_pending_migrations(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert!(!node_anchor_table_exists(&reopened).await);
        for node in expected {
            assert_eq!(
                node_kind(&reopened, &node.id).await,
                super::node_storage_kind(&node.kind)
            );
            assert_eq!(reopened.get_node(&node.id).await.unwrap(), node);
        }
    }

    #[tokio::test]
    async fn node_kind_discriminator_migration_round_trips_all_anchor_kinds() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let new_nodes = [
            session_anchor_node(&store.root_id()),
            rich_session_patch_anchor_node(&store.root_id(), vec![]),
            rich_prompt_anchor_node(&store.root_id(), vec![]),
            skill_invocation_anchor_node(&store.root_id(), vec![]),
            skill_result_anchor_node(&store.root_id(), vec![]),
        ];
        let mut expected = Vec::new();
        for new_node in new_nodes {
            let node_id = store.append(new_node).await.unwrap();
            expected.push(store.get_node(&node_id).await.unwrap());
        }
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 17);
        for node in &expected {
            let kind = diesel::RunQueryDsl::get_result::<String>(
                nodes::table
                    .filter(nodes::id.eq(&node.id))
                    .select(nodes::kind),
                &mut connection,
            )
            .unwrap();
            assert_eq!(kind, "anchor");
        }

        connection
            .run_next_migration(super::STORE_MIGRATIONS)
            .unwrap();
        for node in &expected {
            let kind = diesel::RunQueryDsl::get_result::<String>(
                nodes::table
                    .filter(nodes::id.eq(&node.id))
                    .select(nodes::kind),
                &mut connection,
            )
            .unwrap();
            assert_eq!(kind, super::node_storage_kind(&node.kind));
        }

        connection
            .revert_last_migration(super::STORE_MIGRATIONS)
            .unwrap();
        for node in &expected {
            let kind = diesel::RunQueryDsl::get_result::<String>(
                nodes::table
                    .filter(nodes::id.eq(&node.id))
                    .select(nodes::kind),
                &mut connection,
            )
            .unwrap();
            assert_eq!(kind, "anchor");
        }
        connection
            .run_pending_migrations(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        for node in expected {
            assert_eq!(reopened.get_node(&node.id).await.unwrap(), node);
        }
    }

    #[tokio::test]
    async fn node_kind_discriminator_migration_rejects_mismatched_node_kind() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(session_anchor_node(&store.root_id()))
            .await
            .unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 17);
        diesel::RunQueryDsl::execute(
            diesel::update(nodes::table.filter(nodes::id.eq(&anchor_id)))
                .set(nodes::kind.eq("text")),
            &mut connection,
        )
        .unwrap();

        let error = connection
            .run_next_migration(super::STORE_MIGRATIONS)
            .unwrap_err();

        assert!(error.to_string().contains("CHECK constraint failed"));
    }

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

    #[tokio::test]
    async fn node_anchor_prompt_content_migration_round_trips_payload() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(rich_prompt_anchor_node(&store.root_id(), vec![]))
            .await
            .unwrap();
        let expected = store.get_node(&anchor_id).await.unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        diesel::connection::SimpleConnection::batch_execute(
            &mut connection,
            "PRAGMA foreign_keys = ON",
        )
        .unwrap();
        revert_store_migrations_to(&mut connection, 18);
        let content = diesel::RunQueryDsl::get_result::<Option<String>>(
            nodes::table
                .filter(nodes::id.eq(&anchor_id))
                .select(nodes::content),
            &mut connection,
        )
        .unwrap();
        assert_eq!(content, None);

        connection
            .run_next_migration(super::STORE_MIGRATIONS)
            .unwrap();
        let content = diesel::RunQueryDsl::get_result::<Option<String>>(
            nodes::table
                .filter(nodes::id.eq(&anchor_id))
                .select(nodes::content),
            &mut connection,
        )
        .unwrap();
        assert_eq!(content.as_deref(), Some("Inspect these images"));
        let prompt_table_count = diesel::RunQueryDsl::get_result::<ColumnCount>(
            diesel::sql_query(
                "SELECT COUNT(*) AS count FROM sqlite_master \
                 WHERE type = 'table' AND name = 'node_anchor_prompts'",
            ),
            &mut connection,
        )
        .unwrap();
        assert_eq!(prompt_table_count.count, 0);

        connection
            .revert_last_migration(super::STORE_MIGRATIONS)
            .unwrap();
        connection
            .run_pending_migrations(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
        assert_eq!(
            node_anchor_prompt_attachment_rows(&reopened, &anchor_id).await,
            vec![
                NodeAnchorPromptAttachmentRow {
                    node_id: anchor_id.clone(),
                    ordinal: 0,
                    kind: "image".to_owned(),
                    attachment_id: "image-a".to_owned(),
                    width: Some(i64::from(u32::MAX)),
                    height: Some(1080),
                    file_size: Some(u64::MAX.to_string()),
                    media_type: Some("image/png".to_owned()),
                },
                NodeAnchorPromptAttachmentRow {
                    node_id: anchor_id,
                    ordinal: 1,
                    kind: "image".to_owned(),
                    attachment_id: "image-b".to_owned(),
                    width: None,
                    height: None,
                    file_size: None,
                    media_type: None,
                },
            ]
        );
    }

    #[tokio::test]
    async fn node_anchor_prompt_content_migration_rejects_missing_prompt() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![],
                    PromptAnchor {
                        prompt: "prompt".to_owned(),
                        attachments: vec![],
                    },
                )),
            })
            .await
            .unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 18);
        diesel::RunQueryDsl::execute(
            diesel::sql_query("DELETE FROM node_anchor_prompts WHERE node_id = ?")
                .bind::<diesel::sql_types::Text, _>(&anchor_id),
            &mut connection,
        )
        .unwrap();

        let error = connection
            .run_next_migration(super::STORE_MIGRATIONS)
            .unwrap_err();

        assert!(error.to_string().contains("CHECK constraint failed"));
    }

    #[tokio::test]
    async fn node_anchor_kind_json_migration_rejects_incomplete_relational_payload() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(session_anchor_node(&store.root_id()))
            .await
            .unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 16);
        diesel::RunQueryDsl::execute(
            diesel::sql_query("DELETE FROM node_anchor_sessions WHERE node_id = ?")
                .bind::<diesel::sql_types::Text, _>(&anchor_id),
            &mut connection,
        )
        .unwrap();

        let error = connection
            .run_next_migration(super::STORE_MIGRATIONS)
            .unwrap_err();

        assert!(error.to_string().contains("CHECK constraint failed"));
    }

    #[tokio::test]
    async fn node_anchor_migration_down_restores_node_columns() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(session_anchor_node(&store.root_id()))
            .await
            .unwrap();
        let expected = store.get_node(&anchor_id).await.unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 9);

        let legacy = diesel::RunQueryDsl::get_result::<LegacyNodeAnchorRow>(
            diesel::sql_query(
                "SELECT anchor_kind, anchor_session_role, anchor_provider_profile, \
                 anchor_provider, anchor_model, anchor_prompt, anchor_skill_name, \
                 anchor_skill_invocation_mode, kind_json \
                 FROM nodes WHERE id = ?",
            )
            .bind::<diesel::sql_types::Text, _>(&anchor_id),
            &mut connection,
        )
        .unwrap();
        assert_eq!(legacy.anchor_kind.as_deref(), Some("session"));
        assert_eq!(legacy.anchor_session_role.as_deref(), Some("orchestrator"));
        assert_eq!(legacy.anchor_provider_profile, None);
        assert_eq!(legacy.anchor_provider.as_deref(), Some("openai"));
        assert_eq!(legacy.anchor_model.as_deref(), Some("gpt-5.4"));
        assert_eq!(legacy.anchor_prompt.as_deref(), Some("prompt"));
        assert_eq!(legacy.anchor_skill_name, None);
        assert_eq!(legacy.anchor_skill_invocation_mode, None);
        let legacy_kind_json =
            serde_json::from_str::<serde_json::Value>(&legacy.kind_json).unwrap();
        assert_eq!(
            legacy_kind_json.pointer("/Anchor/payload/Session/system_prompt"),
            Some(&serde_json::Value::String("system".to_owned()))
        );
        assert_eq!(
            legacy_kind_json.pointer("/Anchor/payload/Session/prompt"),
            None
        );

        connection
            .run_pending_migrations(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert!(!nodes_have_anchor_columns(&reopened).await);
        assert_eq!(reopened.get_node(&anchor_id).await.unwrap(), expected);
    }

    #[tokio::test]
    async fn job_payload_migration_down_restores_payload_json() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        let session = store.append(session_anchor_node(&root_id)).await.unwrap();
        store.fork("main", &session).await.unwrap();
        store
            .submit_job_with_id("job-test", "main", &session)
            .await
            .unwrap();
        store
            .set_job_status("job-test", JobStatus::Queued, JobStatus::Running)
            .await
            .unwrap();
        let job = store
            .set_job_status("job-test", JobStatus::Running, JobStatus::Finished)
            .await
            .unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 8);
        let row = diesel::RunQueryDsl::get_result::<LegacyJobPayloadJson>(
            diesel::sql_query("SELECT payload_json FROM jobs WHERE job_id = 'job-test'"),
            &mut connection,
        )
        .unwrap();

        assert_eq!(serde_json::from_str::<Job>(&row.payload_json).unwrap(), job);

        connection
            .run_next_migration(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open(&path).await.unwrap();
        assert!(!job_has_payload_json_column(&reopened).await);
        assert_eq!(reopened.get_job("job-test").await.unwrap(), job);
    }

    #[tokio::test]
    async fn session_state_json_migration_round_trips_all_states() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        let session = store.append(session_anchor_node(&root_id)).await.unwrap();

        let states = std::collections::HashMap::from([
            ("main".to_owned(), SessionState::Active),
            (
                "attached".to_owned(),
                SessionState::Attached {
                    target_branch: "main".to_owned(),
                    base_head_id: session.clone(),
                },
            ),
            (
                "closed".to_owned(),
                SessionState::Paused {
                    target_branch: "main".to_owned(),
                    reason: PauseReason::Closed,
                },
            ),
            (
                "merged".to_owned(),
                SessionState::Paused {
                    target_branch: "main".to_owned(),
                    reason: PauseReason::Merged {
                        merged_anchor_id: session.clone(),
                    },
                },
            ),
        ]);
        for branch in states.keys() {
            store.fork(branch, &session).await.unwrap();
        }
        for (branch, state) in &states {
            if state != &SessionState::Active {
                store
                    .set_session_state(branch, Some(&SessionState::Active), state.clone())
                    .await
                    .unwrap();
            }
        }
        assert!(!session_has_state_json_column(&store).await);
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 20);
        let legacy_rows = diesel::RunQueryDsl::load::<LegacySessionStateJson>(
            diesel::sql_query("SELECT branch_name, state_json FROM sessions ORDER BY branch_name"),
            &mut connection,
        )
        .unwrap();
        let restored = legacy_rows
            .into_iter()
            .map(|row| {
                (
                    row.branch_name,
                    serde_json::from_str::<SessionState>(&row.state_json).unwrap(),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();

        assert_eq!(restored, states);

        connection
            .run_pending_migrations(super::STORE_MIGRATIONS)
            .unwrap();
        drop(connection);

        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert!(!session_has_state_json_column(&reopened).await);
        assert_eq!(reopened.list_session_states().await.unwrap(), states);
    }

    #[tokio::test]
    async fn session_state_json_migration_rejects_mismatched_relational_state() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        let session = store.append(session_anchor_node(&root_id)).await.unwrap();
        store.fork("main", &session).await.unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 20);
        let mismatched = serde_json::to_string(&SessionState::Attached {
            target_branch: "main".to_owned(),
            base_head_id: session,
        })
        .unwrap();
        diesel::RunQueryDsl::execute(
            diesel::sql_query("UPDATE sessions SET state_json = ? WHERE branch_name = 'main'")
                .bind::<diesel::sql_types::Text, _>(mismatched),
            &mut connection,
        )
        .unwrap();

        let error = connection
            .run_next_migration(super::STORE_MIGRATIONS)
            .unwrap_err();

        assert!(error.to_string().contains("CHECK constraint failed"));
    }

    #[tokio::test]
    async fn job_payload_migration_rejects_mismatched_summary() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        let session = store.append(session_anchor_node(&root_id)).await.unwrap();
        store.fork("main", &session).await.unwrap();
        store
            .submit_job_with_id("job-test", "main", &session)
            .await
            .unwrap();
        drop(store);

        let database_path = super::sqlite_database_path(&path);
        let mut connection =
            diesel::sqlite::SqliteConnection::establish(database_path.to_str().unwrap()).unwrap();
        revert_store_migrations_to(&mut connection, 8);
        diesel::RunQueryDsl::execute(
            diesel::sql_query(
                "UPDATE jobs SET payload_json = json_set(payload_json, '$.status', 'finished')",
            ),
            &mut connection,
        )
        .unwrap();

        let error = connection
            .run_next_migration(super::STORE_MIGRATIONS)
            .unwrap_err();

        assert!(error.to_string().contains("CHECK constraint failed"));
    }

    #[test]
    fn job_row_rejects_empty_work_branch() {
        let mut row = valid_job_row();
        row.work_branch.clear();

        let error = super::job_row_into_job(std::path::Path::new("store.sqlite3"), row)
            .expect_err("empty work branch must fail");

        assert!(error.to_string().contains("empty work branch"));
    }

    #[test]
    fn session_row_rejects_inconsistent_relational_state() {
        let row = super::SessionRow {
            branch_name: "main".to_owned(),
            state: "attached".to_owned(),
            target_branch: Some("base".to_owned()),
            base_head_id: None,
            pause_reason: None,
            merged_anchor_id: None,
        };

        let error = super::session_row_into_state(std::path::Path::new("store.sqlite3"), row)
            .expect_err("inconsistent session row must fail");

        assert!(error.to_string().contains("invalid SQLite session row"));
    }

    #[test]
    fn job_row_rejects_invalid_timestamp() {
        let mut row = valid_job_row();
        row.created_at = "invalid".to_owned();

        let error = super::job_row_into_job(std::path::Path::new("store.sqlite3"), row)
            .expect_err("invalid timestamp must fail");

        assert!(error.to_string().contains("invalid SQLite job timestamp"));
    }

    #[test]
    fn job_row_rejects_invalid_status() {
        let mut row = valid_job_row();
        row.status = "invalid".to_owned();

        let error = super::job_row_into_job(std::path::Path::new("store.sqlite3"), row)
            .expect_err("invalid status must fail");

        assert!(error.to_string().contains("invalid SQLite job status"));
    }

    #[tokio::test]
    async fn open_temporary_removes_directory_after_last_store_drop() {
        let store = SqliteStore::open_temporary().await.unwrap();
        let path = store.store_path().to_owned();
        let clone = store.clone();

        assert!(path.exists());
        drop(store);
        assert!(path.exists());
        drop(clone);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn cloned_sqlite_store_shares_database_instance() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");

        let store = SqliteStore::open(&path).await.unwrap();
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

        let store = SqliteStore::open(&path).await.unwrap();
        let read_only = SqliteStore::open_read_only(&path).await.unwrap();
        let graph = SqliteGraphStore::open_read_only(&path).await.unwrap();
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
        let store = SqliteStore::open(&path).await.unwrap();
        let graph = SqliteGraphStore::open_read_only(&path).await.unwrap();
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
        let (write_tx, write_rx) = oneshot::channel();
        let write = tokio::spawn(async move {
            let node = writer
                .append(NewNode {
                    parent: root,
                    role: Role::User,
                    metadata: None,
                    kind: Kind::Text("write while graph rebuild holds its connection".to_owned()),
                })
                .await
                .unwrap();
            write_tx.send(node).unwrap();
        });

        let written = tokio::time::timeout(Duration::from_secs(1), write_rx)
            .await
            .expect("writer should not wait for graph connection release")
            .unwrap();
        release_graph_tx.send(()).unwrap();
        graph_lock.await.unwrap();
        write.await.unwrap();
        assert_eq!(store.get_node(&written).await.unwrap().id, written);
    }

    #[tokio::test]
    async fn sqlite_store_serializes_concurrent_writes_on_shared_database() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();

        let handles = (0..8)
            .map(|index| {
                let store = store.clone();
                let root_id = root_id.clone();
                tokio::spawn(async move {
                    store
                        .append(NewNode {
                            parent: root_id,
                            role: Role::User,
                            metadata: None,
                            kind: Kind::Text(format!("child-{index}")),
                        })
                        .await
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();

        let mut node_ids = Vec::new();
        for handle in handles {
            node_ids.push(handle.await.unwrap());
        }
        node_ids.sort();

        let mut children = store
            .list_children(&store.root_id())
            .await
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<Vec<_>>();
        children.sort();

        assert_eq!(children, node_ids);
        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert_eq!(
            reopened
                .list_children(&reopened.root_id())
                .await
                .unwrap()
                .len(),
            8
        );
    }

    #[tokio::test]
    async fn open_read_only_accepts_current_schema() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        SqliteStore::open(&path).await.unwrap();

        let store = SqliteStore::open_read_only(&path).await.unwrap();

        assert_eq!(store.schema_version().await.unwrap(), 22);
    }

    #[tokio::test]
    async fn append_persists_node_relations() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        let primary_parent = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("primary parent".to_owned()),
            })
            .await
            .unwrap();
        let merge_parent = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("merge parent".to_owned()),
            })
            .await
            .unwrap();
        let shadow_parent = store
            .append(NewNode {
                parent: root_id,
                role: Role::User,
                metadata: None,
                kind: Kind::Text("shadow parent".to_owned()),
            })
            .await
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
        let expected_node_kind = super::node_storage_kind(&child_kind).to_owned();
        let child = store
            .append(NewNode {
                parent: primary_parent.clone(),
                role: Role::System,
                metadata: None,
                kind: child_kind,
            })
            .await
            .unwrap();

        let relations = node_relation_rows(&store, &child).await;

        assert_eq!(node_kind(&store, &child).await, expected_node_kind);
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
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        let single_metadata = BackendMetadata {
            execution_id: Some("execution-single".to_owned()),
            call_id: Some("call-single".to_owned()),
        };
        let single = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: Some(vec![single_metadata]),
                kind: Kind::Text("single metadata".to_owned()),
            })
            .await
            .unwrap();
        let many = store
            .append(NewNode {
                parent: root_id,
                role: Role::LLM,
                metadata: Some(vec![
                    BackendMetadata {
                        execution_id: Some("execution-many".to_owned()),
                        call_id: Some("call-a".to_owned()),
                    },
                    BackendMetadata {
                        execution_id: Some("execution-many".to_owned()),
                        call_id: Some("call-b".to_owned()),
                    },
                ]),
                kind: Kind::Text("many metadata".to_owned()),
            })
            .await
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
    async fn append_round_trips_present_empty_metadata() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let node_id = store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::User,
                metadata: Some(Vec::new()),
                kind: Kind::Text("empty metadata".to_owned()),
            })
            .await
            .unwrap();

        assert_eq!(
            store.get_node(&node_id).await.unwrap().metadata,
            Some(Vec::new())
        );
        drop(store);
        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert_eq!(
            reopened.get_node(&node_id).await.unwrap().metadata,
            Some(Vec::new())
        );
    }

    #[tokio::test]
    async fn append_persists_text_and_failure_content() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        let text_id = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("line one\n\"line two\"".to_owned()),
            })
            .await
            .unwrap();
        let failure_id = store
            .append(NewNode {
                parent: text_id.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Failure(String::new()),
            })
            .await
            .unwrap();

        assert_eq!(
            node_content(&store, &root_id).await.as_deref(),
            Some("The Big Bang")
        );
        assert_eq!(
            node_content(&store, &text_id).await.as_deref(),
            Some("line one\n\"line two\"")
        );
        assert_eq!(node_content(&store, &failure_id).await.as_deref(), Some(""));
        assert_eq!(
            store.get_node(&text_id).await.unwrap().kind,
            Kind::Text("line one\n\"line two\"".to_owned())
        );
        assert_eq!(
            store.get_node(&failure_id).await.unwrap().kind,
            Kind::Failure(String::new())
        );
    }

    #[tokio::test]
    async fn append_persists_node_tool_item_rows() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        let tool_use_node = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_uses(vec![
                    ToolUse {
                        id: "tool-call-a".to_owned(),
                        name: "exec_command".to_owned(),
                        input: serde_json::json!({"cmd": "pwd"}),
                    },
                    ToolUse {
                        id: "tool-call-b".to_owned(),
                        name: "exec_command".to_owned(),
                        input: serde_json::json!({"cmd": "ls"}),
                    },
                ]),
            })
            .await
            .unwrap();
        let tool_result_node = store
            .append(NewNode {
                parent: tool_use_node.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::tool_results(vec![
                    ToolResult {
                        id: "tool-call-a".to_owned(),
                        output: "left".to_owned(),
                    },
                    ToolResult {
                        id: "tool-call-b".to_owned(),
                        output: "right".to_owned(),
                    },
                ]),
            })
            .await
            .unwrap();

        assert_eq!(
            node_tool_use_rows(&store, &tool_use_node).await,
            vec![
                NodeToolUseRow {
                    node_id: tool_use_node.clone(),
                    ordinal: 0,
                    tool_use_id: "tool-call-a".to_owned(),
                    name: "exec_command".to_owned(),
                    input_json: r#"{"cmd":"pwd"}"#.to_owned(),
                },
                NodeToolUseRow {
                    node_id: tool_use_node.clone(),
                    ordinal: 1,
                    tool_use_id: "tool-call-b".to_owned(),
                    name: "exec_command".to_owned(),
                    input_json: r#"{"cmd":"ls"}"#.to_owned(),
                },
            ]
        );
        assert_eq!(
            node_tool_result_rows(&store, &tool_result_node).await,
            vec![
                NodeToolResultRow {
                    node_id: tool_result_node.clone(),
                    ordinal: 0,
                    tool_result_id: "tool-call-a".to_owned(),
                    output: "left".to_owned(),
                },
                NodeToolResultRow {
                    node_id: tool_result_node.clone(),
                    ordinal: 1,
                    tool_result_id: "tool-call-b".to_owned(),
                    output: "right".to_owned(),
                },
            ]
        );
        assert_eq!(node_content(&store, &tool_use_node).await, None);
        assert_eq!(node_content(&store, &tool_result_node).await, None);
    }

    #[tokio::test]
    async fn append_persists_node_anchor_payload_rows() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
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
            .await
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
            .await
            .unwrap();

        assert_eq!(
            node_anchor_session_row(&store, &session).await,
            NodeAnchorSessionRow {
                node_id: session.clone(),
                role: "runner".to_owned(),
                provider_profile: Some("runner-profile".to_owned()),
                provider: Some("openai".to_owned()),
                model: "gpt-5.4".to_owned(),
                system_prompt: "system".to_owned(),
                prompt: "session prompt".to_owned(),
                temperature: Some(0.1),
                max_tokens: Some("64".to_owned()),
                additional_params_json: None,
                enable_coco_shim: false,
                active_skill_name: None,
                active_skill_handoff: None,
            }
        );
        assert_eq!(node_content(&store, &session).await, None);
        assert_eq!(
            node_content(&store, &prompt).await.as_deref(),
            Some("detached prompt")
        );
    }

    #[tokio::test]
    async fn reading_session_anchor_uses_relational_payload() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(rich_session_anchor_node(&store.root_id(), vec![]))
            .await
            .unwrap();
        let expected = store.get_node(&anchor_id).await.unwrap();

        assert_eq!(store.get_node(&anchor_id).await.unwrap(), expected);
    }

    #[tokio::test]
    async fn reading_session_patch_anchor_preserves_explicit_clears() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let kind = Kind::Anchor(Anchor::session_patch(
            vec![],
            SessionAnchorPatch {
                provider_profile: Some(None),
                provider: Some(None),
                tools: Some(vec![]),
                temperature: Some(None),
                max_tokens: Some(None),
                additional_params: Some(None),
                ..SessionAnchorPatch::default()
            },
        ));
        let anchor_id = store
            .append(NewNode {
                parent: store.root_id(),
                role: Role::System,
                metadata: None,
                kind: kind.clone(),
            })
            .await
            .unwrap();

        assert_eq!(store.get_node(&anchor_id).await.unwrap().kind, kind);
        let row = node_anchor_session_patch_row(&store, &anchor_id).await;
        assert!(row.provider_profile_present);
        assert!(row.provider_profile.is_none());
        assert!(row.provider_present);
        assert!(row.provider.is_none());
        assert!(row.tools_present);
        assert!(row.temperature_present);
        assert!(row.temperature.is_none());
        assert!(row.max_tokens_present);
        assert!(row.max_tokens.is_none());
        assert!(row.additional_params_present);
        assert!(row.additional_params_json.is_none());
    }

    #[tokio::test]
    async fn reading_prompt_anchor_uses_content_and_relational_attachments() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(rich_prompt_anchor_node(&store.root_id(), vec![]))
            .await
            .unwrap();
        let expected = store.get_node(&anchor_id).await.unwrap();

        assert_eq!(store.get_node(&anchor_id).await.unwrap(), expected);
    }

    #[tokio::test]
    async fn reading_skill_invocation_anchor_uses_relational_payload() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(skill_invocation_anchor_node(&store.root_id(), vec![]))
            .await
            .unwrap();
        let expected = store.get_node(&anchor_id).await.unwrap();

        assert_eq!(store.get_node(&anchor_id).await.unwrap(), expected);
        assert_eq!(
            node_anchor_skill_invocation_row(&store, &anchor_id).await,
            NodeAnchorSkillInvocationRow {
                node_id: anchor_id,
                skill_name: "compact".to_owned(),
                mode: "handoff".to_owned(),
                prompt: Some("Compact this branch".to_owned()),
            }
        );
    }

    #[tokio::test]
    async fn reading_skill_result_anchor_uses_relational_payload() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(skill_result_anchor_node(&store.root_id(), vec![]))
            .await
            .unwrap();
        let expected = store.get_node(&anchor_id).await.unwrap();

        assert_eq!(store.get_node(&anchor_id).await.unwrap(), expected);
        assert_eq!(
            node_anchor_skill_result_row(&store, &anchor_id).await,
            NodeAnchorSkillResultRow {
                node_id: anchor_id,
                skill_name: "compact".to_owned(),
                output: "First line\nSecond line with \"quotes\"".to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn reading_anchor_node_requires_matching_payload_row() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(session_anchor_node(&store.root_id()))
            .await
            .unwrap();
        let mut connection = store.connect().await.unwrap();
        diesel::delete(
            node_anchor_sessions::table.filter(node_anchor_sessions::node_id.eq(&anchor_id)),
        )
        .execute(&mut connection)
        .await
        .unwrap();
        drop(connection);

        let error = store.get_node(&anchor_id).await.unwrap_err();

        assert!(matches!(
            error,
            crate::StoreError::CorruptedStore { message, .. }
                if message.contains("missing SQLite node anchor session row")
        ));
    }

    #[tokio::test]
    async fn reading_anchor_node_queries_only_selected_payload_tables() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let anchor_id = store
            .append(rich_session_anchor_node(&store.root_id(), vec![]))
            .await
            .unwrap();
        let mut connection = store.connect().await.unwrap();
        let queries = Arc::new(Mutex::new(Vec::new()));
        let captured_queries = Arc::clone(&queries);
        connection.set_instrumentation(move |event: InstrumentationEvent<'_>| {
            if let InstrumentationEvent::StartQuery { query, .. } = event {
                captured_queries.lock().unwrap().push(query.to_string());
            }
        });

        super::load_node_by_exact_id(&mut connection, &store.database_path, &anchor_id)
            .await
            .unwrap();

        let queries = queries.lock().unwrap();
        assert_eq!(queries.len(), 4, "unexpected queries: {queries:#?}");
        assert!(
            queries
                .iter()
                .any(|query| query.contains("node_anchor_sessions"))
        );
        assert!(
            queries
                .iter()
                .any(|query| query.contains("node_anchor_session_tools"))
        );
        assert!(queries.iter().any(|query| query.contains("node_relations")));
        for table in [
            "node_anchor_session_patches",
            "node_anchor_session_patch_tools",
            "node_anchor_prompt_attachments",
            "node_anchor_skill_invocations",
            "node_anchor_skill_results",
            "node_metadata",
            "node_tool_uses",
            "node_tool_results",
        ] {
            assert!(
                queries.iter().all(|query| !query.contains(table)),
                "unexpected query for {table}: {queries:#?}"
            );
        }
    }

    #[tokio::test]
    async fn reading_nodes_validates_content_presence() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        let tool_use_id = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::LLM,
                metadata: None,
                kind: Kind::tool_uses(Vec::new()),
            })
            .await
            .unwrap();
        let prompt_id = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    vec![],
                    PromptAnchor {
                        prompt: "prompt".to_owned(),
                        attachments: vec![],
                    },
                )),
            })
            .await
            .unwrap();
        let mut connection = store.connect().await.unwrap();
        diesel::update(nodes::table.filter(nodes::id.eq(&root_id)))
            .set(nodes::content.eq(None::<String>))
            .execute(&mut connection)
            .await
            .unwrap();
        drop(connection);

        let error = store.get_node(&root_id).await.unwrap_err();
        assert!(error.to_string().contains("missing SQLite node content"));

        let mut connection = store.connect().await.unwrap();
        diesel::update(nodes::table.filter(nodes::id.eq(&prompt_id)))
            .set(nodes::content.eq(None::<String>))
            .execute(&mut connection)
            .await
            .unwrap();
        drop(connection);

        let error = store.get_node(&prompt_id).await.unwrap_err();
        assert!(error.to_string().contains("missing SQLite node content"));

        let mut connection = store.connect().await.unwrap();
        diesel::update(nodes::table.filter(nodes::id.eq(&tool_use_id)))
            .set(nodes::content.eq(Some("unexpected")))
            .execute(&mut connection)
            .await
            .unwrap();
        drop(connection);

        let error = store.get_node(&tool_use_id).await.unwrap_err();
        assert!(error.to_string().contains("unexpectedly has content"));
    }

    #[tokio::test]
    async fn graph_store_reads_children_from_node_relations() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let writer = SqliteStore::open(&path).await.unwrap();
        let root_id = writer.root_id();
        let child_id = writer
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("graph child".to_owned()),
            })
            .await
            .unwrap();
        writer.fork("graph-child", &child_id).await.unwrap();
        drop(writer);

        let graph_store = SqliteGraphStore::open_read_only(&path).await.unwrap();

        assert_eq!(graph_store.root_id(), root_id);
        assert_eq!(
            graph_store.get_node(&child_id).await.unwrap().id,
            child_id.clone()
        );
        assert_eq!(
            graph_store.get_node(&child_id[..12]).await.unwrap().id,
            child_id.clone()
        );
        assert_eq!(
            graph_store.get_node("graph-child").await.unwrap().id,
            child_id.clone()
        );
        assert_eq!(
            graph_store.list_children(&root_id).await.unwrap()[0].id,
            child_id
        );
        assert_eq!(graph_store.ancestry(&child_id).await.unwrap().len(), 2);
        assert!(matches!(
            graph_store
                .append(NewNode {
                    parent: root_id,
                    role: Role::User,
                    metadata: None,
                    kind: Kind::Text("blocked".to_owned()),
                })
                .await
                .unwrap_err(),
            crate::StoreError::StoreReadOnly { .. }
        ));
    }

    #[tokio::test]
    async fn open_read_only_rejects_writes() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let writable = SqliteStore::open(&path).await.unwrap();
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
            .await
            .unwrap_err();

        assert!(matches!(err, crate::StoreError::StoreReadOnly { .. }));
        let reopened = SqliteStore::open_read_only(&path).await.unwrap();
        assert!(reopened.list_children(&root_id).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn open_rejects_store_locked_by_another_owner() {
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

        let err = SqliteStore::open(&path).await.unwrap_err();

        assert!(matches!(err, crate::StoreError::StoreLocked { path: locked } if locked == path));
    }

    #[tokio::test]
    async fn open_read_only_allows_store_locked_by_another_owner() {
        use std::os::fd::AsRawFd;

        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        SqliteStore::open(&path).await.unwrap();
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

        assert_eq!(store.schema_version().await.unwrap(), 22);
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
                .contains("unsupported SQLite schema version 5, expected 6..=22")
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
                .contains("unsupported SQLite schema version 5, expected 6..=22")
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

        assert!(
            matches!(err, crate::StoreError::LegacyJsonStore { path: legacy } if legacy == path)
        );
        assert!(!super::sqlite_database_path(&path).exists());
    }

    #[tokio::test]
    async fn open_rejects_legacy_json_store_with_unmarked_sqlite_database() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        SqliteStore::open(&path).await.unwrap();
        std::fs::write(path.join("meta.json"), "{}").unwrap();
        std::fs::write(path.join("nodes.jsonl"), "").unwrap();

        let err = SqliteStore::open(&path).await.unwrap_err();

        assert!(
            matches!(err, crate::StoreError::LegacyJsonStore { path: legacy } if legacy == path)
        );
    }

    #[tokio::test]
    async fn open_accepts_legacy_json_store_after_completed_sqlite_migration() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        persist_store_meta_bool_for_test(&store, super::FS_MIGRATION_COMPLETE_META_KEY, true).await;
        drop(store);
        std::fs::write(path.join("meta.json"), "{}").unwrap();
        std::fs::write(path.join("nodes.jsonl"), "").unwrap();

        let reopened = SqliteStore::open(&path).await.unwrap();

        assert_eq!(reopened.schema_version().await.unwrap(), 22);
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

    #[tokio::test]
    async fn graph_open_read_only_rejects_missing_schema_without_creating_database() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        std::fs::create_dir(&path).unwrap();

        let err = SqliteGraphStore::open_read_only(&path).await.unwrap_err();

        assert!(err.to_string().contains("SQLite"));
        assert!(!super::sqlite_database_path(&path).exists());
    }

    #[tokio::test]
    async fn append_persists_node_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        let child_id = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("child".to_owned()),
            })
            .await
            .unwrap();
        assert_eq!(store.list_children(&root_id).await.unwrap()[0].id, child_id);

        let reopened = SqliteStore::open(&path).await.unwrap();
        let child = reopened.get_node(&child_id).await.unwrap();

        assert_eq!(child.parent, root_id);
        assert_eq!(
            reopened.list_children(&root_id).await.unwrap()[0].id,
            child_id
        );
    }

    #[tokio::test]
    async fn reopened_store_supports_node_traversal() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        let first = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("first".to_owned()),
            })
            .await
            .unwrap();
        let second = store
            .append(NewNode {
                parent: first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("second".to_owned()),
            })
            .await
            .unwrap();

        let reopened = SqliteStore::open(&path).await.unwrap();

        let ancestry = reopened
            .ancestry(&second)
            .await
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
            .await
            .unwrap()
            .into_iter()
            .map(|node| node.id)
            .collect::<Vec<_>>();
        assert_eq!(log, vec![second.clone(), first, root_id]);
        assert_eq!(reopened.get_node(&second[..12]).await.unwrap().id, second);
    }

    #[tokio::test]
    async fn branch_operations_persist_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        let first = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("first".to_owned()),
            })
            .await
            .unwrap();
        let second = store
            .append(NewNode {
                parent: first.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("second".to_owned()),
            })
            .await
            .unwrap();

        assert_eq!(store.fork("main", &first).await.unwrap(), first);
        store
            .set_branch_head("main", &first, &second)
            .await
            .unwrap();
        assert_eq!(store.get_branch_head("main").await.unwrap(), second);

        let reopened = SqliteStore::open(&path).await.unwrap();
        assert_eq!(reopened.get_branch_head("main").await.unwrap(), second);

        reopened.delete_branch("main").await.unwrap();
        let reopened = SqliteStore::open(&path).await.unwrap();
        assert!(reopened.get_branch_head("main").await.is_err());
    }

    #[tokio::test]
    async fn persist_session_nodes_rolls_back_node_when_branch_head_mismatch() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        store.fork("main", &root_id).await.unwrap();
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
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        let session = store.append(session_anchor_node(&root_id)).await.unwrap();
        store.fork("main", &session).await.unwrap();
        let text = store
            .append(NewNode {
                parent: session.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("text".to_owned()),
            })
            .await
            .unwrap();
        store
            .set_branch_head("main", &session, &text)
            .await
            .unwrap();
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

        let reopened = SqliteStore::open(&path).await.unwrap();

        assert_eq!(reopened.get_branch_head("main").await.unwrap(), handoff);
        assert_eq!(
            reopened.get_session_state("main").await.unwrap(),
            SessionState::Paused {
                target_branch: String::new(),
                reason: PauseReason::Closed,
            }
        );
        assert!(reopened.get_node(&rebased).await.is_ok());
        assert!(reopened.get_node(&handoff).await.is_ok());
    }

    #[tokio::test]
    async fn job_operations_persist_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let root_id = store.root_id();
        let session = store.append(session_anchor_node(&root_id)).await.unwrap();
        store.fork("main", &session).await.unwrap();

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

        let reopened = SqliteStore::open(&path).await.unwrap();
        let job = reopened.get_job("job-test").await.unwrap();

        assert_eq!(job.status, JobStatus::Running);
        assert_eq!(job.branch, "main");
        assert_eq!(job.base, session);
    }

    #[tokio::test]
    async fn message_queue_operations_persist_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let first = store
            .enqueue_message("runner", serde_json::json!({"index": 1}))
            .await
            .unwrap();
        let second = store
            .enqueue_message("runner", serde_json::json!({"index": 2}))
            .await
            .unwrap();

        let reopened = SqliteStore::open(&path).await.unwrap();
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
        let reopened = SqliteStore::open(&path).await.unwrap();
        let messages = reopened.list_queue_messages("runner").await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].message_id, second.message_id);
    }

    #[tokio::test]
    async fn message_queue_preserves_insert_order_for_equal_timestamps() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
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

        let reopened = SqliteStore::open(&path).await.unwrap();
        let messages = reopened.list_queue_messages("runner").await.unwrap();

        assert_eq!(messages[0].message_id, first.message_id);
        assert_eq!(messages[1].message_id, second.message_id);
    }

    #[tokio::test]
    async fn message_queue_sorts_by_parsed_timestamp() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
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

        let reopened = SqliteStore::open(&path).await.unwrap();
        let messages = reopened.list_queue_messages("runner").await.unwrap();

        assert_eq!(messages[0].message_id, first.message_id);
        assert_eq!(messages[1].message_id, second.message_id);
    }

    #[tokio::test]
    async fn preset_operations_persist_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();

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

        let reopened = SqliteStore::open(&path).await.unwrap();
        let record = reopened.get_preset_record("default").await.unwrap();

        assert_eq!(record.current_version, 3);
        assert_eq!(record.current_preset().unwrap().model, "gpt-5.4");
    }

    #[tokio::test]
    async fn skill_operations_persist_across_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
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

        let reopened = SqliteStore::open(&path).await.unwrap();
        let record = reopened
            .get_skill(SessionRole::Runner, "custom-runner")
            .await
            .unwrap();

        assert_eq!(record.current_version, 3);
        assert_eq!(record.current().unwrap().body, "run");
    }

    #[tokio::test]
    async fn updating_builtin_skill_materializes_existing_history() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("store");
        let store = SqliteStore::open(&path).await.unwrap();
        let original = store
            .get_skill(SessionRole::Orchestrator, "cronjob")
            .await
            .unwrap();

        let updated = store
            .update_skill(
                SessionRole::Orchestrator,
                "cronjob",
                &SkillUpdatePatch {
                    body: Some("updated body".to_owned()),
                    ..SkillUpdatePatch::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            updated.versions.keys().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );
        drop(store);

        let reopened = SqliteStore::open(&path).await.unwrap();
        let record = reopened
            .get_skill(SessionRole::Orchestrator, "cronjob")
            .await
            .unwrap();
        assert_eq!(
            record.versions.keys().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            record.versions.get(&1).unwrap().id,
            original.versions.get(&1).unwrap().id
        );
        assert_eq!(record.current().unwrap().body, "updated body");

        let rolled_back = reopened
            .rollback_skill(SessionRole::Orchestrator, "cronjob", 1)
            .await
            .unwrap();
        assert_eq!(rolled_back.current_version, 3);
        assert_eq!(
            rolled_back.current().unwrap().body,
            original.current().unwrap().body
        );
        assert_eq!(
            rolled_back.current().unwrap().scripts,
            original.current().unwrap().scripts
        );
    }
}
