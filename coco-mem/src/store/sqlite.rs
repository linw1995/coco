use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use diesel::prelude::*;
use diesel::result::OptionalExtension;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Integer, Nullable, Text};
use diesel::sqlite::SqliteConnection;
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, RunQueryDsl, SimpleAsyncConnection};
use snafu::prelude::*;
use tokio::runtime::Runtime;

use super::NodeStore;
use super::state::StoreState;
use crate::StoreResult as Result;
use crate::error::{
    ConnectSqliteStoreSnafu, CorruptedStoreSnafu, ParseSqliteStoreValueSnafu,
    QuerySqliteStoreSnafu, StartSqliteRuntimeSnafu, StorePathIsNotDirectorySnafu,
    StoreReadOnlySnafu, WriteStoreDirectorySnafu,
};
use crate::{Kind, MergeParent, NewNode, Node, NodeMetadata, Role};

const SQLITE_DATABASE_FILE_NAME: &str = "store.sqlite3";
const SQLITE_SCHEMA_VERSION: i32 = 1;

type AsyncSqliteConnection = SyncConnectionWrapper<SqliteConnection>;

#[derive(Clone)]
pub struct SqliteStore {
    dir: PathBuf,
    database_path: PathBuf,
    access: StoreAccess,
    runtime: Arc<Runtime>,
    inner: Arc<RwLock<StoreState>>,
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
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context(StartSqliteRuntimeSnafu)?;
        Ok(Self {
            dir: path.to_owned(),
            database_path: path.join(SQLITE_DATABASE_FILE_NAME),
            access,
            runtime: Arc::new(runtime),
            inner: Arc::new(RwLock::new(StoreState::new())),
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

    #[cfg(test)]
    pub fn snapshot_state(&self) -> StoreState {
        self.inner.read().expect("store lock poisoned").clone()
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

    for edge in node_edges(node) {
        sql_query("INSERT INTO node_edges (parent_id, child_id, kind) VALUES (?, ?, ?)")
            .bind::<Text, _>(edge.parent_id)
            .bind::<Text, _>(edge.child_id)
            .bind::<Text, _>(edge.kind)
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    Ok(())
}

struct NodeEdge {
    parent_id: String,
    child_id: String,
    kind: String,
}

fn node_edges(node: &Node) -> Vec<NodeEdge> {
    let mut edges = Vec::new();
    if !node.parent.is_empty() {
        edges.push(NodeEdge {
            parent_id: node.parent.clone(),
            child_id: node.id.clone(),
            kind: "primary".to_owned(),
        });
    }
    if let Kind::Anchor(anchor) = &node.kind {
        edges.extend(anchor.merge_parents().iter().map(|parent| NodeEdge {
            parent_id: parent.node_id().to_owned(),
            child_id: node.id.clone(),
            kind: merge_parent_edge_kind(parent).to_owned(),
        }));
    }
    edges
}

fn merge_parent_edge_kind(parent: &MergeParent) -> &'static str {
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

impl NodeStore for SqliteStore {
    fn root_id(&self) -> String {
        self.inner
            .read()
            .expect("store lock poisoned")
            .root_id()
            .to_owned()
    }

    fn append(&self, node: NewNode) -> Result<String> {
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

#[cfg(test)]
mod tests {
    use super::SqliteStore;
    use crate::{Kind, NewNode, NodeStore, Role};

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
}
