use super::*;
use diesel::sql_types::Text;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, Weak};

type SnapshotWriterGate = tokio::sync::Mutex<()>;

static SNAPSHOT_WRITER_GATES: OnceLock<Mutex<HashMap<PathBuf, Weak<SnapshotWriterGate>>>> =
    OnceLock::new();

#[derive(diesel::QueryableByName)]
struct SnapshotJournalModeRow {
    #[diesel(sql_type = Text)]
    journal_mode: String,
}

#[derive(Clone)]
pub struct SnapshotDatabase {
    path: Arc<PathBuf>,
    pool: AsyncSnapshotPool,
    writer_gate: Arc<SnapshotWriterGate>,
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
            writer_gate: snapshot_writer_gate(&path),
        };
        database
            .with_write_connection("configure WAL journal mode", move |connection| {
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

    pub async fn with_write_connection<T, F>(
        &self,
        operation_name: impl Into<String>,
        operation: F,
    ) -> crate::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut SqliteConnection) -> crate::Result<T> + Send + 'static,
    {
        let operation_name = operation_name.into();
        let guard = self.writer_gate.clone().lock_owned().await;
        let result = self
            .with_connection(move |connection| {
                let _guard = guard;
                operation(connection)
            })
            .await;
        if let Err(error) = &result {
            tracing::warn!(
                database_operation = %operation_name,
                database_path = %self.path.display(),
                error = %error,
                "console graph database write failed",
            );
        }
        result
    }

    pub fn path(&self) -> &Path {
        self.path.as_ref()
    }
}

fn snapshot_writer_gate(path: &Path) -> Arc<SnapshotWriterGate> {
    let path = std::fs::canonicalize(path).unwrap_or_else(|_| {
        path.parent()
            .and_then(|parent| std::fs::canonicalize(parent).ok())
            .and_then(|parent| path.file_name().map(|file_name| parent.join(file_name)))
            .unwrap_or_else(|| path.to_owned())
    });
    let gates = SNAPSHOT_WRITER_GATES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut gates = gates
        .lock()
        .expect("console graph writer gate registry lock poisoned");
    if let Some(gate) = gates.get(&path).and_then(Weak::upgrade) {
        return gate;
    }
    let gate = Arc::new(SnapshotWriterGate::new(()));
    gates.insert(path, Arc::downgrade(&gate));
    gate
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::*;

    static TEST_DATABASE_NONCE: AtomicU64 = AtomicU64::new(0);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writer_gate_is_shared_by_database_instances_for_the_same_path() {
        let path = test_database_path();
        let first = SnapshotDatabase::open(&path).await.unwrap();
        let second = SnapshotDatabase::open(&path).await.unwrap();
        assert!(Arc::ptr_eq(&first.writer_gate, &second.writer_gate));
        let setup_path = path.clone();
        first
            .with_write_connection("create writer gate test table", move |connection| {
                diesel::sql_query("CREATE TABLE writer_gate_test (id INTEGER PRIMARY KEY NOT NULL)")
                    .execute(connection)
                    .map(|_| ())
                    .context(crate::error::QueryGraphSnapshotStoreSnafu { path: setup_path })
            })
            .await
            .unwrap();

        let (first_entered_tx, first_entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_first_tx, release_first_rx) = std::sync::mpsc::sync_channel(1);
        let first_task = tokio::spawn(async move {
            let first_path = first.path().to_owned();
            first
                .with_write_connection("hold writer gate", move |connection| {
                    connection
                        .immediate_transaction::<_, diesel::result::Error, _>(|connection| {
                            diesel::sql_query("INSERT INTO writer_gate_test (id) VALUES (1)")
                                .execute(connection)?;
                            first_entered_tx.send(()).unwrap();
                            release_first_rx.recv().unwrap();
                            Ok(())
                        })
                        .context(crate::error::QueryGraphSnapshotStoreSnafu { path: first_path })
                })
                .await
        });
        tokio::task::spawn_blocking(move || first_entered_rx.recv_timeout(Duration::from_secs(2)))
            .await
            .unwrap()
            .unwrap();

        let (second_started_tx, second_started_rx) = std::sync::mpsc::sync_channel(1);
        let (second_entered_tx, second_entered_rx) = std::sync::mpsc::sync_channel(1);
        let second_task = tokio::spawn(async move {
            let second_path = second.path().to_owned();
            second_started_tx.send(()).unwrap();
            second
                .with_write_connection("wait for writer gate", move |connection| {
                    second_entered_tx.send(()).unwrap();
                    diesel::sql_query("INSERT INTO writer_gate_test (id) VALUES (2)")
                        .execute(connection)
                        .map(|_| ())
                        .context(crate::error::QueryGraphSnapshotStoreSnafu { path: second_path })
                })
                .await
        });
        tokio::task::spawn_blocking(move || second_started_rx.recv_timeout(Duration::from_secs(2)))
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(second_entered_rx.try_recv().is_err());

        release_first_tx.send(()).unwrap();
        first_task.await.unwrap().unwrap();
        second_task.await.unwrap().unwrap();
        second_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    fn test_database_path() -> PathBuf {
        let nonce = TEST_DATABASE_NONCE.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "coco-console-writer-gate-{}-{timestamp}-{nonce}",
            std::process::id(),
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("console-graph.sqlite3")
    }
}
