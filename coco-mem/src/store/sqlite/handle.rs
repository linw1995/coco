use std::fs;
use std::path::{Path, PathBuf};
#[cfg(any(test, feature = "test-utils"))]
use std::sync::Arc;

use diesel::prelude::*;
use diesel::result::OptionalExtension;
use diesel_async::{AsyncConnection, RunQueryDsl};
use snafu::prelude::*;

#[cfg(any(test, feature = "test-utils"))]
use super::OwnedStoreDirectory;
use super::database::sqlite_database_path;
use super::node::{
    load_child_ids_by_parent_ids, load_node_by_exact_id, load_node_by_prefix_or_branch,
    load_nodes_by_exact_ids, load_root_id, node_count, persist_node_without_transaction,
};
use super::{
    AsyncSqliteConnection, AsyncSqliteConnectionGuard, GRAPH_READ_BATCH_SIZE, GraphBranchRecord,
    SqliteDatabase, SqliteGraphConnectionFuture, SqliteGraphStore, SqliteStore,
    SqliteTransactionError, StoreAccess, migration,
};
use crate::StoreResult as Result;
use crate::error::{
    CorruptedStoreSnafu, GraphReadBatchTooLargeSnafu, QuerySqliteStoreSnafu,
    StorePathIsNotDirectorySnafu, StoreReadOnlySnafu, WriteStoreDirectorySnafu,
};
use crate::schema::branches;
use crate::store::ProcessShareableStore;
use crate::{Kind, Node, Role};
use std::collections::HashMap;

impl std::fmt::Debug for SqliteGraphStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteGraphStore")
            .field("dir", &self.dir)
            .field("database_path", &self.database_path)
            .field("root_id", &self.root_id)
            .finish_non_exhaustive()
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl Drop for OwnedStoreDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
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

impl SqliteStore {
    pub async fn open_read_only_or_upgrade_schema(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if sqlite_database_path(path).is_file()
            && Self::sqlite_schema_requires_migration(path).await?
        {
            drop(Self::open(path).await?);
        }
        Self::open_read_only(path).await
    }

    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        prepare_store_directory(path)?;
        migration::reject_incomplete_legacy_json_store(path).await?;
        let mut store = Self::new(path, StoreAccess::ReadWrite).await?;
        let root_id = store
            .database
            .initialized_root_id(|| store.initialize_writable())
            .await?;
        store.root_id = root_id;
        Ok(store)
    }

    /// Opens a test store that is removed after its last cloned handle is dropped.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn open_temporary() -> Result<Self> {
        let directory = Arc::new(create_temporary_store_directory());
        let mut store = Self::open(&directory.path).await?;
        store._owned_directory = Some(directory);
        Ok(store)
    }

    pub async fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        ensure_existing_store_directory(path)?;
        migration::reject_incomplete_legacy_json_store(path).await?;
        ensure_existing_database_file(&sqlite_database_path(path))?;
        let mut store = Self::new(path, StoreAccess::ReadOnly).await?;
        let root_id = store
            .database
            .initialized_root_id(|| store.initialize_read_only())
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
        migration::existing_schema_version(&mut connection, &self.database_path).await
    }

    pub(super) async fn new(path: &Path, access: StoreAccess) -> Result<Self> {
        Self::new_with_database_path(path, sqlite_database_path(path), access).await
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
        Ok(Self {
            dir: path.to_owned(),
            database_path: database_path.clone(),
            database,
            root_id: String::new(),
            access,
            #[cfg(any(test, feature = "test-utils"))]
            _owned_directory: None,
        })
    }

    async fn initialize_writable(&self) -> Result<String> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<String, SqliteTransactionError, _>(async |connection| {
                migration::run_in_transaction(connection, &self.database_path)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                if node_count(connection, &self.database_path)
                    .await
                    .map_err(SqliteTransactionError::Operation)?
                    == 0
                {
                    let root = initial_root_node();
                    persist_node_without_transaction(connection, &self.database_path, &root)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                }
                load_root_id(connection, &self.database_path)
                    .await
                    .map_err(SqliteTransactionError::Operation)
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }

    async fn sqlite_schema_requires_migration(path: &Path) -> Result<bool> {
        ensure_existing_store_directory(path)?;
        let store = Self::new(path, StoreAccess::ReadOnly).await?;
        ensure_existing_database_file(&store.database_path)?;
        let mut connection = store.connect().await?;
        migration::requires_migration(&mut connection, &store.database_path).await
    }

    pub(super) fn ensure_writable(&self) -> Result<()> {
        if self.access == StoreAccess::ReadWrite {
            return Ok(());
        }

        StoreReadOnlySnafu {
            path: self.dir.clone(),
        }
        .fail()
    }

    async fn initialize_read_only(&self) -> Result<String> {
        let mut connection = self.connect().await?;
        connection
            .transaction::<String, SqliteTransactionError, _>(async |connection| {
                migration::ensure_current_schema(connection, &self.database_path)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                let root_id = load_root_id(connection, &self.database_path)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                load_node_by_exact_id(connection, &self.database_path, &root_id)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                Ok(root_id)
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }

    pub(super) async fn connect(&self) -> Result<AsyncSqliteConnectionGuard<'_>> {
        self.database.acquire().await
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
            .initialized_root_id(|| store.initialize_read_only())
            .await?;
        Ok(Self { root_id, ..store })
    }

    pub fn store_path(&self) -> &Path {
        &self.dir
    }

    pub async fn graph_branches(&self) -> Result<Vec<GraphBranchRecord>> {
        let mut connection = self.connect().await?;
        super::branch::load_graph_branch_records(&mut connection, &self.database_path, None).await
    }

    pub async fn graph_branches_by_names(
        &self,
        names: &[String],
    ) -> Result<Vec<GraphBranchRecord>> {
        ensure_graph_read_batch_size(names.len())?;
        let mut connection = self.connect().await?;
        super::branch::load_graph_branch_records(&mut connection, &self.database_path, Some(names))
            .await
    }

    pub async fn graph_nodes_by_ids(&self, ids: &[String]) -> Result<Vec<Node>> {
        ensure_graph_read_batch_size(ids.len())?;
        let mut connection = self.connect().await?;
        load_nodes_by_exact_ids(&mut connection, &self.database_path, ids).await
    }

    pub async fn graph_child_ids(
        &self,
        parent_ids: &[String],
    ) -> Result<HashMap<String, Vec<String>>> {
        ensure_graph_read_batch_size(parent_ids.len())?;
        let mut connection = self.connect().await?;
        load_child_ids_by_parent_ids(&mut connection, &self.database_path, parent_ids).await
    }

    async fn new(path: &Path) -> Result<Self> {
        let database_path = sqlite_database_path(path);
        let database = SqliteDatabase::open(database_path.clone(), false).await?;
        Ok(Self {
            dir: path.to_owned(),
            database_path,
            database,
            root_id: String::new(),
        })
    }

    async fn initialize_read_only(&self) -> Result<String> {
        let mut connection = self.connect().await?;
        connection
            .transaction::<String, SqliteTransactionError, _>(async |connection| {
                migration::ensure_current_schema(connection, &self.database_path)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                let root_id = load_root_id(connection, &self.database_path)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                load_node_by_exact_id(connection, &self.database_path, &root_id)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                Ok(root_id)
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }

    pub(super) async fn connect(&self) -> Result<AsyncSqliteConnectionGuard<'_>> {
        self.database.acquire().await
    }

    pub(super) async fn with_connection<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send,
        F: for<'a> FnOnce(&'a mut AsyncSqliteConnection) -> SqliteGraphConnectionFuture<'a, T>
            + Send,
    {
        let mut connection = self.connect().await?;
        operation(&mut connection).await
    }

    pub(super) fn ensure_read_only<T>(&self) -> Result<T> {
        StoreReadOnlySnafu {
            path: self.dir.clone(),
        }
        .fail()
    }

    pub(super) async fn branch_head(&self, name: &str) -> Result<Option<String>> {
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

    pub(super) async fn get_node_by_prefix_or_branch(&self, reference: &str) -> Result<Node> {
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

fn ensure_graph_read_batch_size(actual: usize) -> Result<()> {
    ensure!(
        actual <= GRAPH_READ_BATCH_SIZE,
        GraphReadBatchTooLargeSnafu {
            actual,
            maximum: GRAPH_READ_BATCH_SIZE,
        }
    );
    Ok(())
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

#[cfg(any(test, feature = "test-utils"))]
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

impl ProcessShareableStore for SqliteStore {
    fn store_path(&self) -> &Path {
        &self.dir
    }
}
