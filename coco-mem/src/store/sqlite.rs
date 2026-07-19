use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::Arc;

use diesel::sqlite::SqliteConnection;
use diesel_async::pooled_connection::bb8::{
    Pool as AsyncSqlitePool, PooledConnection as AsyncSqlitePooledConnection,
};
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;

use crate::StoreResult as Result;
use crate::error::StoreError;
use crate::{MergeParent, SessionState};

pub const GRAPH_READ_BATCH_SIZE: usize = 128;
// Graph reads are auxiliary and must not exhaust the pool shared with primary store operations.
const GRAPH_CONNECTION_LIMIT: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphBranchRecord {
    pub name: String,
    pub head_id: String,
    pub state: SessionState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphBranchPageCursor {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphBranchPage {
    pub branches: Vec<GraphBranchRecord>,
    pub next_cursor: Option<GraphBranchPageCursor>,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphChildPageCursor {
    pub created_at: String,
    pub node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphChildPage {
    pub child_ids: Vec<String>,
    pub next_cursor: Option<GraphChildPageCursor>,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphNodeCursor {
    pub row_id: i64,
    pub node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphNodePage {
    pub entries: Vec<GraphNodeCursor>,
    pub complete: bool,
}

/// Topology-only node data for incremental graph construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphNodeRecord {
    pub id: String,
    pub parent: String,
    pub is_anchor: bool,
    pub merge_parents: Vec<MergeParent>,
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
    load_graph_node_records_by_exact_ids, load_node_by_exact_id, node_storage_kind,
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
    graph_connection_gate: Arc<tokio::sync::Semaphore>,
    wal_journal_mode_enabled: tokio::sync::OnceCell<()>,
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

struct SqliteGraphConnectionGuard<'a> {
    connection: AsyncSqliteConnectionGuard<'a>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Deref for SqliteGraphConnectionGuard<'_> {
    type Target = AsyncSqliteConnection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl DerefMut for SqliteGraphConnectionGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
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
