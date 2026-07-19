use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::sql_types::Text;
use diesel::sqlite::{Sqlite, SqliteConnection};
use diesel_async::pooled_connection::bb8::{
    Pool as AsyncSqlitePool, PooledConnection as AsyncSqlitePooledConnection,
};
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, RunQueryDsl};
use diesel_migrations::MigrationHarness;
use snafu::prelude::*;

use super::{ConfigureSnafu, Error, MIGRATIONS, MigrateSnafu, QuerySnafu, Result};

const SQLITE_POOL_MAX_SIZE: u32 = 4;
static DATABASES: OnceLock<Mutex<HashMap<PathBuf, Weak<DatabaseInner>>>> = OnceLock::new();

pub type AsyncSqliteConnection = SyncConnectionWrapper<SqliteConnection>;
pub type AsyncSqliteConnectionGuard<'a> = AsyncSqlitePooledConnection<'a, AsyncSqliteConnection>;
type ConnectionPool = AsyncSqlitePool<AsyncSqliteConnection>;

#[derive(Clone)]
pub struct Database {
    inner: Arc<DatabaseInner>,
}

struct DatabaseInner {
    path: PathBuf,
    pool: ConnectionPool,
    wal_journal_mode_enabled: tokio::sync::OnceCell<()>,
    schema_initialized: tokio::sync::OnceCell<()>,
}

impl Database {
    pub async fn open(path: PathBuf) -> Result<Self> {
        let path = registry_path(&path)?;
        let databases = DATABASES.get_or_init(|| Mutex::new(HashMap::new()));
        let existing = {
            let databases = databases
                .lock()
                .expect("SQLite database registry lock poisoned");
            databases
                .get(&path)
                .and_then(Weak::upgrade)
                .map(|inner| Self { inner })
        };
        if let Some(database) = existing {
            database.initialize().await?;
            return Ok(database);
        }

        let pool = build_pool(&path).await?;
        let inner = Arc::new(DatabaseInner {
            path: path.clone(),
            pool,
            wal_journal_mode_enabled: tokio::sync::OnceCell::new(),
            schema_initialized: tokio::sync::OnceCell::new(),
        });
        let database = {
            let mut databases = databases
                .lock()
                .expect("SQLite database registry lock poisoned");
            if let Some(inner) = databases.get(&path).and_then(Weak::upgrade) {
                Self { inner }
            } else {
                databases.insert(path, Arc::downgrade(&inner));
                Self { inner }
            }
        };
        database.initialize().await?;
        Ok(database)
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub async fn acquire(&self) -> Result<AsyncSqliteConnectionGuard<'_>> {
        self.inner
            .pool
            .get()
            .await
            .context(super::AcquireConnectionSnafu {
                path: self.inner.path.clone(),
            })
    }

    #[cfg(test)]
    pub fn shared_pool(&self) -> &ConnectionPool {
        &self.inner.pool
    }

    async fn initialize(&self) -> Result<()> {
        self.enable_wal_journal_mode().await?;
        self.ensure_schema().await
    }

    async fn enable_wal_journal_mode(&self) -> Result<()> {
        self.inner
            .wal_journal_mode_enabled
            .get_or_try_init(|| async {
                let mut connection = self.acquire().await?;
                let journal_mode = SqliteJournalModeWal
                    .get_result::<String>(&mut connection)
                    .await
                    .context(QuerySnafu {
                        path: self.inner.path.clone(),
                    })?;
                ensure!(
                    journal_mode.eq_ignore_ascii_case("wal"),
                    ConfigureSnafu {
                        path: self.inner.path.clone(),
                        message: format!(
                            "SQLite refused WAL journal mode and remained in {journal_mode:?}"
                        ),
                    }
                );
                Ok(())
            })
            .await
            .copied()
    }

    async fn ensure_schema(&self) -> Result<()> {
        self.inner
            .schema_initialized
            .get_or_try_init(|| async {
                let mut connection = self.acquire().await?;
                let path = self.inner.path.clone();
                let result = connection
                    .spawn_blocking(|connection| {
                        Ok(connection.run_pending_migrations(MIGRATIONS).map(|_| ()))
                    })
                    .await
                    .context(QuerySnafu { path: path.clone() })?;
                result.context(MigrateSnafu { path })
            })
            .await
            .copied()
    }
}

fn registry_path(path: &Path) -> Result<PathBuf> {
    let Some(parent) = path.parent().filter(|path| !path.as_os_str().is_empty()) else {
        return Ok(path.to_owned());
    };
    let Some(file_name) = path.file_name() else {
        return Ok(path.to_owned());
    };
    let parent = parent.canonicalize().context(super::ResolvePathSnafu {
        path: parent.to_owned(),
    })?;
    Ok(parent.join(file_name))
}

async fn build_pool(path: &Path) -> Result<ConnectionPool> {
    let manager = AsyncDieselConnectionManager::<AsyncSqliteConnection>::new_with_config(
        path.to_string_lossy().into_owned(),
        pool_manager_config(path.to_owned()),
    );
    AsyncSqlitePool::builder()
        .max_size(SQLITE_POOL_MAX_SIZE)
        .build(manager)
        .await
        .context(super::CreatePoolSnafu {
            path: path.to_owned(),
        })
}

fn pool_manager_config(path: PathBuf) -> ManagerConfig<AsyncSqliteConnection> {
    let mut config = ManagerConfig::default();
    config.custom_setup = Box::new(move |url| {
        let url = url.to_owned();
        let path = path.clone();
        Box::pin(async move {
            let mut connection = AsyncSqliteConnection::establish(&url).await?;
            configure_connection(&mut connection, &path)
                .await
                .map_err(connection_setup_error)?;
            Ok(connection)
        })
    });
    config
}

fn connection_setup_error(error: Error) -> diesel::ConnectionError {
    diesel::ConnectionError::BadConnection(error.to_string())
}

async fn configure_connection(connection: &mut AsyncSqliteConnection, path: &Path) -> Result<()> {
    SqliteConnectionPragma::ForeignKeysOn
        .execute(connection)
        .await
        .context(QuerySnafu {
            path: path.to_owned(),
        })?;
    SqliteConnectionPragma::BusyTimeout5000
        .execute(connection)
        .await
        .context(QuerySnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum SqliteConnectionPragma {
    ForeignKeysOn,
    BusyTimeout5000,
}

impl QueryId for SqliteConnectionPragma {
    type QueryId = Self;

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Sqlite> for SqliteConnectionPragma {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Sqlite>) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();
        match self {
            Self::ForeignKeysOn => out.push_sql("PRAGMA foreign_keys = ON"),
            Self::BusyTimeout5000 => out.push_sql("PRAGMA busy_timeout = 5000"),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)]
struct SqliteJournalModeWal;

impl Query for SqliteJournalModeWal {
    type SqlType = Text;
}

impl QueryFragment<Sqlite> for SqliteJournalModeWal {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Sqlite>) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();
        out.push_sql("PRAGMA journal_mode = WAL");
        Ok(())
    }
}
