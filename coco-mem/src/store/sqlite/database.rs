use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use diesel::sql_types::Text;
use diesel_async::pooled_connection::bb8::Pool as AsyncSqlitePool;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::{AsyncConnection, RunQueryDsl};
use snafu::prelude::*;

use super::{
    AsyncSqliteConnection, AsyncSqliteConnectionGuard, SqliteDatabase, SqliteDatabaseInner,
};
use crate::StoreResult as Result;
use crate::error::{
    AcquireSqliteConnectionSnafu, CorruptedStoreSnafu, CreateSqlitePoolSnafu,
    QuerySqliteStoreSnafu, WriteStoreDirectorySnafu,
};

const SQLITE_DATABASE_FILE_NAME: &str = "store.sqlite3";
const SQLITE_POOL_MAX_SIZE: u32 = 4;
static SQLITE_DATABASES: OnceLock<Mutex<HashMap<PathBuf, Weak<SqliteDatabaseInner>>>> =
    OnceLock::new();

#[derive(diesel::QueryableByName)]
struct SqliteJournalModeRow {
    #[diesel(sql_type = Text)]
    journal_mode: String,
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
    pub(super) async fn open(database_path: PathBuf, writable: bool) -> Result<Self> {
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
            if writable {
                database.enable_wal_journal_mode().await?;
            }
            return Ok(database);
        }

        let pool = build_sqlite_pool(&database_path).await?;
        let inner = Arc::new(SqliteDatabaseInner {
            database_path: database_path.clone(),
            pool,
            wal_journal_mode_enabled: tokio::sync::OnceCell::new(),
            initialized_root_id: tokio::sync::OnceCell::new(),
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
        if writable {
            database.enable_wal_journal_mode().await?;
        }
        Ok(database)
    }

    pub(super) async fn acquire(&self) -> Result<AsyncSqliteConnectionGuard<'_>> {
        self.inner
            .pool
            .get()
            .await
            .context(AcquireSqliteConnectionSnafu {
                path: self.inner.database_path.clone(),
            })
    }

    #[cfg(test)]
    pub(super) fn shared_pool(&self) -> &AsyncSqlitePool<AsyncSqliteConnection> {
        &self.inner.pool
    }

    async fn enable_wal_journal_mode(&self) -> Result<()> {
        self.inner
            .wal_journal_mode_enabled
            .get_or_try_init(|| async {
                let mut connection = self.acquire().await?;
                configure_writable_connection(&mut connection, &self.inner.database_path).await
            })
            .await
            .copied()
    }

    pub(super) async fn initialized_root_id<F, Fut>(&self, operation: F) -> Result<String>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<String>>,
    {
        self.inner
            .initialized_root_id
            .get_or_try_init(operation)
            .await
            .cloned()
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

async fn build_sqlite_pool(database_path: &Path) -> Result<AsyncSqlitePool<AsyncSqliteConnection>> {
    let manager = AsyncDieselConnectionManager::<AsyncSqliteConnection>::new_with_config(
        database_path.to_string_lossy().into_owned(),
        sqlite_pool_manager_config(database_path.to_owned()),
    );
    AsyncSqlitePool::builder()
        .max_size(SQLITE_POOL_MAX_SIZE)
        .build(manager)
        .await
        .context(CreateSqlitePoolSnafu {
            path: database_path.to_owned(),
        })
}

fn sqlite_pool_manager_config(database_path: PathBuf) -> ManagerConfig<AsyncSqliteConnection> {
    let mut config = ManagerConfig::default();
    config.custom_setup = Box::new(move |url| {
        let url = url.to_owned();
        let database_path = database_path.clone();
        Box::pin(async move {
            let mut connection = AsyncSqliteConnection::establish(&url).await?;
            configure_connection(&mut connection, &database_path)
                .await
                .map_err(sqlite_connection_setup_error)?;
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
    let row = diesel::sql_query("PRAGMA journal_mode = WAL")
        .get_result::<SqliteJournalModeRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    ensure!(
        row.journal_mode.eq_ignore_ascii_case("wal"),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "SQLite refused WAL journal mode and remained in {:?}",
                row.journal_mode
            ),
        }
    );
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum SqliteConnectionPragma {
    ForeignKeysOn,
    BusyTimeout5000,
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
        }
        Ok(())
    }
}
