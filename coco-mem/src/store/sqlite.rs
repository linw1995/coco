use std::path::PathBuf;
use std::sync::Arc;

use diesel::sqlite::SqliteConnection;
use diesel_async::pooled_connection::bb8::{
    Pool as AsyncSqlitePool, PooledConnection as AsyncSqlitePooledConnection,
};
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;

use crate::SessionState;
use crate::StoreResult as Result;
use crate::error::StoreError;

pub const GRAPH_READ_BATCH_SIZE: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphBranchRecord {
    pub name: String,
    pub head_id: String,
    pub state: SessionState,
}

mod branch;
mod codec;
mod database;
mod handle;
mod job;
mod message_queue;
mod migration;
mod node;
mod preset;
mod skill;
mod transaction;

use database::sqlite_database_path;

#[cfg(test)]
use crate::MessageQueueItem;
#[cfg(test)]
use branch::{SessionRow, persist_session_nodes_and_branch_head, session_row_into_state};
#[cfg(test)]
use job::{JobRow, job_row_into_job};
#[cfg(test)]
use message_queue::persist_message_queue_item;
#[cfg(test)]
use node::{
    NodeAnchorPromptAttachmentRow, NodeAnchorSessionPatchRow, NodeAnchorSessionPatchToolRow,
    NodeAnchorSessionRow, NodeAnchorSessionToolRow, NodeAnchorSkillInvocationRow,
    NodeAnchorSkillResultRow, NodeMetadataRow, NodeToolResultRow, NodeToolUseRow,
    load_node_by_exact_id, node_storage_kind,
};

type AsyncSqliteConnection = SyncConnectionWrapper<SqliteConnection>;

type AsyncSqliteConnectionGuard<'a> = AsyncSqlitePooledConnection<'a, AsyncSqliteConnection>;
type SqliteGraphConnectionFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>>;

enum SqliteTransactionError {
    Query(diesel::result::Error),
    Operation(StoreError),
}

#[derive(Clone)]
struct SqliteDatabase {
    inner: Arc<SqliteDatabaseInner>,
}

struct SqliteDatabaseInner {
    database_path: PathBuf,
    pool: AsyncSqlitePool<AsyncSqliteConnection>,
    initialized_root_id: tokio::sync::OnceCell<String>,
}

#[derive(Clone)]
pub struct SqliteStore {
    dir: PathBuf,
    database_path: PathBuf,
    database: SqliteDatabase,
    root_id: String,
    access: StoreAccess,
    #[cfg(any(test, feature = "test-utils"))]
    // Keeps a temporary store alive until the last cloned handle is dropped.
    _owned_directory: Option<Arc<OwnedStoreDirectory>>,
}

#[derive(Clone)]
pub struct SqliteGraphStore {
    dir: PathBuf,
    database_path: PathBuf,
    database: SqliteDatabase,
    root_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreAccess {
    ReadWrite,
    ReadOnly,
}

#[cfg(any(test, feature = "test-utils"))]
struct OwnedStoreDirectory {
    path: PathBuf,
}

#[cfg(test)]
mod tests;
