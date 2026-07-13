use super::*;

#[derive(diesel::QueryableByName)]
struct SnapshotJournalModeRow {
    #[diesel(sql_type = Text)]
    journal_mode: String,
}

#[derive(Clone)]
pub struct SnapshotDatabase {
    path: Arc<PathBuf>,
    pool: AsyncSnapshotPool,
}

impl std::fmt::Debug for SnapshotDatabase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotDatabase")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl SnapshotDatabase {
    pub async fn open(path: impl AsRef<Path>) -> crate::Result<Self> {
        let path = path.as_ref().to_owned();
        let manager = AsyncDieselConnectionManager::<AsyncSnapshotConnection>::new_with_config(
            path.to_string_lossy().into_owned(),
            snapshot_pool_manager_config(),
        );
        let pool = AsyncSqlitePool::builder()
            .max_size(SQLITE_POOL_MAX_SIZE)
            .build(manager)
            .await
            .context(CreateGraphSnapshotPoolSnafu { path: path.clone() })?;
        let database = Self {
            path: Arc::new(path.clone()),
            pool,
        };
        database
            .with_connection(move |connection| {
                let mode = diesel::sql_query("PRAGMA journal_mode = WAL")
                    .get_result::<SnapshotJournalModeRow>(connection)
                    .context(QueryGraphSnapshotStoreSnafu { path: path.clone() })?;
                ensure!(
                    mode.journal_mode.eq_ignore_ascii_case("wal"),
                    ConfigureGraphSnapshotStoreSnafu {
                        path: path.clone(),
                        message: format!(
                            "SQLite refused WAL journal mode and remained in {:?}",
                            mode.journal_mode
                        ),
                    }
                );
                Ok(())
            })
            .await?;
        Ok(database)
    }

    pub async fn with_connection<T, F>(&self, operation: F) -> crate::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut SqliteConnection) -> crate::Result<T> + Send + 'static,
    {
        let mut connection =
            self.pool
                .get()
                .await
                .context(AcquireGraphSnapshotConnectionSnafu {
                    path: self.path.as_ref().clone(),
                })?;
        match connection
            .spawn_blocking(move |connection| Ok(operation(connection)))
            .await
        {
            Ok(result) => result,
            Err(source) => Err(crate::Error::QueryGraphSnapshotStore {
                path: self.path.as_ref().clone(),
                source,
            }),
        }
    }
}

fn snapshot_pool_manager_config() -> ManagerConfig<AsyncSnapshotConnection> {
    let mut config = ManagerConfig::default();
    config.custom_setup = Box::new(|url| {
        let url = url.to_owned();
        Box::pin(async move {
            let mut connection = AsyncSnapshotConnection::establish(&url).await?;
            connection
                .batch_execute("PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000")
                .await
                .map_err(|error| diesel::ConnectionError::BadConnection(error.to_string()))?;
            Ok(connection)
        })
    });
    config
}

impl ConsoleGraphSnapshotStore {
    pub async fn open(dir: impl AsRef<Path>) -> crate::Result<Self> {
        let dir = dir.as_ref();
        let path = database_path(dir);
        let database = SnapshotDatabase::open(&path).await?;
        let store = Self {
            path: Arc::new(path),
            database,
        };
        store.ensure_schema().await?;
        Ok(store)
    }

    pub async fn ensure_schema(&self) -> crate::Result<()> {
        let this = self.clone();
        self.with_connection(move |connection| {
            connection
                .run_pending_migrations(CONSOLE_GRAPH_MIGRATIONS)
                .map(|_| ())
                .context(MigrateGraphSnapshotStoreSnafu {
                    path: this.path.as_ref().clone(),
                })?;
            Ok(())
        })
        .await
    }

    pub async fn with_connection<T, F>(&self, operation: F) -> crate::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut SqliteConnection) -> crate::Result<T> + Send + 'static,
    {
        self.database.with_connection(operation).await
    }

    #[cfg(test)]
    pub(crate) async fn with_connection_for_tests<T, F>(&self, operation: F) -> crate::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut SqliteConnection) -> crate::Result<T> + Send + 'static,
    {
        self.with_connection(operation).await
    }
}

pub fn database_path(dir: impl AsRef<Path>) -> PathBuf {
    dir.as_ref().join(SQLITE_DATABASE_FILE_NAME)
}
