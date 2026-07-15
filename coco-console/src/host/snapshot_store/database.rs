use super::*;
use diesel::sql_types::Text;

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
            .context(crate::error::CreateGraphSnapshotPoolSnafu { path: path.clone() })?;
        let database = Self {
            path: Arc::new(path.clone()),
            pool,
        };
        database
            .with_connection(move |connection| {
                let mode = diesel::sql_query("PRAGMA journal_mode = WAL")
                    .get_result::<SnapshotJournalModeRow>(connection)
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path: path.clone() })?;
                ensure!(
                    mode.journal_mode.eq_ignore_ascii_case("wal"),
                    crate::error::ConfigureGraphSnapshotStoreSnafu {
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
                .context(crate::error::AcquireGraphSnapshotConnectionSnafu {
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
