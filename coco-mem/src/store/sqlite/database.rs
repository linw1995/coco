use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use diesel::sqlite::SqliteConnection;
use diesel_async::pooled_connection::bb8::Pool as AsyncSqlitePool;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::{AsyncConnection, RunQueryDsl};
use snafu::prelude::*;
use tokio::runtime::Runtime;

use super::{
    AsyncSqliteConnection, AsyncSqliteConnectionGuard, SqliteDatabase, SqliteDatabaseInner,
};
use crate::StoreResult as Result;
use crate::error::{
    AcquireSqliteConnectionSnafu, CreateSqlitePoolSnafu, QuerySqliteStoreSnafu,
    StartSqliteRuntimeSnafu, StoreError, WriteStoreDirectorySnafu,
};

const SQLITE_DATABASE_FILE_NAME: &str = "store.sqlite3";
const SQLITE_POOL_MAX_SIZE: u32 = 4;
static SQLITE_RUNTIME: OnceLock<Runtime> = OnceLock::new();
static SQLITE_RUNTIME_INIT: Mutex<()> = Mutex::new(());
static SQLITE_DATABASES: OnceLock<Mutex<HashMap<PathBuf, Weak<SqliteDatabaseInner>>>> =
    OnceLock::new();

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

    pub(super) async fn open(database_path: PathBuf, ensure_wal: bool) -> Result<Self> {
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

    pub(super) async fn connection(&self) -> Result<AsyncSqliteConnectionGuard<'_>> {
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
    pub(super) fn shared_pool(&self) -> &AsyncSqlitePool<AsyncSqliteConnection> {
        &self.inner.pool
    }

    async fn request_wal_journal_mode(&self) -> Result<()> {
        self.inner.ensure_wal.store(true, Ordering::SeqCst);
        let mut connection = self.connection().await?;
        ensure_wal_journal_mode(&mut connection, &self.inner.database_path).await
    }

    pub(super) async fn with_initialization_lock<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let _guard = self.inner.initialization.lock().await;
        operation().await
    }
}

pub fn sqlite_database_path(path: &Path) -> PathBuf {
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

pub async fn configure_writable_connection(
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
