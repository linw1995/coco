use std::collections::HashMap;
use std::path::Path;

use async_trait::async_trait;

mod lock;
mod sqlite;

#[cfg(test)]
mod tests;

pub use sqlite::{SqliteDatabase, SqliteGraphStore, SqliteStore};

use crate::{
    Job, JobStatus, MessageQueueItem, NewNode, Node, Preset, PresetRecord, SessionAnchorPatch,
    SessionRole, SessionState, SkillRecord, SkillUpdatePatch, SkillVersionSpec, StoreResult,
};

/// Node graph storage API used by CoCo services.
#[async_trait]
pub trait NodeStore {
    /// Returns the global root node identifier.
    fn root_id(&self) -> String;

    /// Appends a new node and returns the persisted node identifier.
    async fn append(&self, node: NewNode) -> StoreResult<String>;

    /// Returns the chain from a node id or branch reference back to the root.
    async fn ancestry(&self, head_ref: &str) -> StoreResult<Vec<Node>>;

    /// Returns the main-parent chain from `head_ref` back to `base_ref`, inclusive.
    async fn log(&self, base_ref: &str, head_ref: &str) -> StoreResult<Vec<Node>>;

    /// Returns a single node by branch name, full node ID, or node ID prefix.
    async fn get_node(&self, id: &str) -> StoreResult<Node>;

    /// Returns all direct children for a node, including merge-parent edges.
    async fn list_children(&self, node_id: &str) -> StoreResult<Vec<Node>>;
}

/// Branch reference storage API.
#[async_trait]
pub trait BranchStore {
    /// Creates a branch from a node id or branch reference and returns its head id.
    async fn fork(&self, name: &str, from_ref: &str) -> StoreResult<String>;

    /// Returns the current head node identifier for a branch.
    async fn get_branch_head(&self, name: &str) -> StoreResult<String>;

    /// Deletes a branch head and its session state.
    async fn delete_branch(&self, name: &str) -> StoreResult<()>;

    /// Moves a branch head when the expected current head matches.
    async fn set_branch_head(
        &self,
        name: &str,
        expected_old_head: &str,
        new_head: &str,
    ) -> StoreResult<()>;
}

/// Branch workflow session state storage API.
#[async_trait]
pub trait SessionStore {
    /// Returns all persisted branch workflow states keyed by branch.
    async fn list_session_states(&self) -> StoreResult<HashMap<String, SessionState>>;

    /// Returns the workflow state for a branch.
    async fn get_session_state(&self, name: &str) -> StoreResult<SessionState>;

    /// Updates the persisted session workflow state.
    async fn set_session_state(
        &self,
        name: &str,
        expected: Option<&SessionState>,
        next: SessionState,
    ) -> StoreResult<SessionState>;

    /// Rewrites the visible session chain for a branch and returns the new head id.
    async fn rebase_session(&self, name: &str, patch: &SessionAnchorPatch) -> StoreResult<String>;

    /// Appends a new full session anchor to reset provider context for a branch.
    async fn handoff_session(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
        prompt: &str,
    ) -> StoreResult<String>;
}

/// Preset storage API.
#[async_trait]
pub trait PresetStore {
    /// Returns all persisted preset records keyed by preset name.
    async fn list_preset_records(&self) -> StoreResult<HashMap<String, PresetRecord>>;

    /// Returns one preset record by preset name.
    async fn get_preset_record(&self, name: &str) -> StoreResult<PresetRecord>;

    /// Creates a new version for a preset under a preset name.
    async fn set_preset(&self, name: &str, preset: Preset) -> StoreResult<PresetRecord>;

    /// Creates a new version cloned from a previous preset version.
    async fn rollback_preset(&self, name: &str, target_version: u64) -> StoreResult<PresetRecord>;

    /// Deletes one preset by preset name.
    async fn delete_preset(&self, name: &str) -> StoreResult<()>;
}

/// Persisted skill storage API.
#[async_trait]
pub trait SkillStore {
    /// Returns all persisted skills for the given role.
    async fn list_skills(&self, role: SessionRole) -> StoreResult<Vec<SkillRecord>>;

    /// Returns one persisted skill for the given role and name.
    async fn get_skill(&self, role: SessionRole, name: &str) -> StoreResult<SkillRecord>;

    /// Creates a new persisted skill for the given role.
    async fn add_skill(
        &self,
        role: SessionRole,
        name: &str,
        spec: SkillVersionSpec,
    ) -> StoreResult<SkillRecord>;

    /// Creates a new version for an existing skill by patching the current version.
    async fn update_skill(
        &self,
        role: SessionRole,
        name: &str,
        patch: &SkillUpdatePatch,
    ) -> StoreResult<SkillRecord>;

    /// Creates a new version cloned from a previous version and makes it current.
    async fn rollback_skill(
        &self,
        role: SessionRole,
        name: &str,
        target_version: u64,
    ) -> StoreResult<SkillRecord>;
}

/// Prompt job storage API.
#[async_trait]
pub trait JobStore {
    /// Creates a new single-task prompt job record with a generated id.
    ///
    /// Rejects the request when the branch already has an unfinished prompt job.
    async fn submit_job(&self, branch: &str, base: &str) -> StoreResult<Job>;

    /// Creates a new single-task prompt job record with a caller-provided id.
    ///
    /// Rejects the request when the branch already has an unfinished prompt job.
    async fn submit_job_with_id(&self, job_id: &str, branch: &str, base: &str) -> StoreResult<Job>;

    /// Returns a persisted prompt job.
    async fn get_job(&self, job_id: &str) -> StoreResult<Job>;

    /// Returns all persisted prompt jobs keyed by job id.
    async fn list_jobs(&self) -> StoreResult<HashMap<String, Job>>;

    /// Updates a prompt job lifecycle state when the current state matches.
    async fn set_job_status(
        &self,
        job_id: &str,
        expected: JobStatus,
        next: JobStatus,
    ) -> StoreResult<Job>;

    /// Moves the current work branch for an unfinished prompt job.
    async fn set_job_work_branch(
        &self,
        job_id: &str,
        expected_work_branch: &str,
        next_work_branch: &str,
    ) -> StoreResult<Job>;
}

/// Generic persistent message queue storage API.
#[async_trait]
pub trait MessageQueueStore {
    /// Enqueues one message in a named queue.
    async fn enqueue_message(
        &self,
        queue: &str,
        payload: serde_json::Value,
    ) -> StoreResult<MessageQueueItem>;

    /// Removes and returns the oldest message in a named queue.
    async fn dequeue_message(&self, queue: &str) -> StoreResult<Option<MessageQueueItem>>;

    /// Returns the oldest message in a named queue without removing it.
    async fn peek_message(&self, queue: &str) -> StoreResult<Option<MessageQueueItem>>;

    /// Returns all persisted messages for a named queue in dequeue order.
    async fn list_queue_messages(&self, queue: &str) -> StoreResult<Vec<MessageQueueItem>>;

    /// Returns all queue names that currently contain at least one message.
    async fn list_message_queues(&self) -> StoreResult<Vec<String>>;
}

/// Capability for stores with a process-shareable backing path.
pub trait ProcessShareableStore {
    /// Returns the backing store directory that another process can reopen.
    fn store_path(&self) -> &Path;
}

/// Complete storage API used by CoCo application services.
pub trait Store:
    NodeStore + BranchStore + SessionStore + PresetStore + SkillStore + JobStore + MessageQueueStore
{
}

impl<T> Store for T where
    T: NodeStore
        + BranchStore
        + SessionStore
        + PresetStore
        + SkillStore
        + JobStore
        + MessageQueueStore
{
}

#[derive(Clone, Debug)]
pub enum PersistentStore {
    Sqlite(SqliteStore),
}

impl PersistentStore {
    pub async fn open(path: impl AsRef<Path>) -> StoreResult<Self> {
        SqliteStore::open(path).await.map(Self::Sqlite)
    }

    pub async fn open_read_only_or_upgrade_schema(path: impl AsRef<Path>) -> StoreResult<Self> {
        SqliteStore::open_read_only_or_upgrade_schema(path)
            .await
            .map(Self::Sqlite)
    }

    pub fn store_path(&self) -> &Path {
        match self {
            Self::Sqlite(store) => store.store_path(),
        }
    }
}

macro_rules! delegate_persistent_store {
    ($self:expr, $store:ident, $body:expr) => {
        match $self {
            PersistentStore::Sqlite($store) => $body,
        }
    };
}

#[async_trait]
impl NodeStore for PersistentStore {
    fn root_id(&self) -> String {
        delegate_persistent_store!(self, store, store.root_id())
    }

    async fn append(&self, node: NewNode) -> StoreResult<String> {
        match self {
            Self::Sqlite(store) => store.append(node).await,
        }
    }

    async fn ancestry(&self, head_ref: &str) -> StoreResult<Vec<Node>> {
        match self {
            Self::Sqlite(store) => store.ancestry(head_ref).await,
        }
    }

    async fn log(&self, base_ref: &str, head_ref: &str) -> StoreResult<Vec<Node>> {
        match self {
            Self::Sqlite(store) => store.log(base_ref, head_ref).await,
        }
    }

    async fn get_node(&self, id: &str) -> StoreResult<Node> {
        match self {
            Self::Sqlite(store) => store.get_node(id).await,
        }
    }

    async fn list_children(&self, node_id: &str) -> StoreResult<Vec<Node>> {
        match self {
            Self::Sqlite(store) => store.list_children(node_id).await,
        }
    }
}

#[async_trait]
impl BranchStore for PersistentStore {
    async fn fork(&self, name: &str, from_ref: &str) -> StoreResult<String> {
        match self {
            Self::Sqlite(store) => store.fork(name, from_ref).await,
        }
    }

    async fn get_branch_head(&self, name: &str) -> StoreResult<String> {
        match self {
            Self::Sqlite(store) => store.get_branch_head(name).await,
        }
    }

    async fn delete_branch(&self, name: &str) -> StoreResult<()> {
        match self {
            Self::Sqlite(store) => store.delete_branch(name).await,
        }
    }

    async fn set_branch_head(
        &self,
        name: &str,
        expected_old_head: &str,
        new_head: &str,
    ) -> StoreResult<()> {
        match self {
            Self::Sqlite(store) => {
                store
                    .set_branch_head(name, expected_old_head, new_head)
                    .await
            }
        }
    }
}

#[async_trait]
impl SessionStore for PersistentStore {
    async fn list_session_states(&self) -> StoreResult<HashMap<String, SessionState>> {
        match self {
            Self::Sqlite(store) => store.list_session_states().await,
        }
    }

    async fn get_session_state(&self, name: &str) -> StoreResult<SessionState> {
        match self {
            Self::Sqlite(store) => store.get_session_state(name).await,
        }
    }

    async fn set_session_state(
        &self,
        name: &str,
        expected: Option<&SessionState>,
        next: SessionState,
    ) -> StoreResult<SessionState> {
        match self {
            Self::Sqlite(store) => store.set_session_state(name, expected, next).await,
        }
    }

    async fn rebase_session(&self, name: &str, patch: &SessionAnchorPatch) -> StoreResult<String> {
        match self {
            Self::Sqlite(store) => store.rebase_session(name, patch).await,
        }
    }

    async fn handoff_session(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
        prompt: &str,
    ) -> StoreResult<String> {
        match self {
            Self::Sqlite(store) => store.handoff_session(name, patch, prompt).await,
        }
    }
}

#[async_trait]
impl PresetStore for PersistentStore {
    async fn list_preset_records(&self) -> StoreResult<HashMap<String, PresetRecord>> {
        match self {
            Self::Sqlite(store) => store.list_preset_records().await,
        }
    }

    async fn get_preset_record(&self, name: &str) -> StoreResult<PresetRecord> {
        match self {
            Self::Sqlite(store) => store.get_preset_record(name).await,
        }
    }

    async fn set_preset(&self, name: &str, preset: Preset) -> StoreResult<PresetRecord> {
        match self {
            Self::Sqlite(store) => store.set_preset(name, preset).await,
        }
    }

    async fn rollback_preset(&self, name: &str, target_version: u64) -> StoreResult<PresetRecord> {
        match self {
            Self::Sqlite(store) => store.rollback_preset(name, target_version).await,
        }
    }

    async fn delete_preset(&self, name: &str) -> StoreResult<()> {
        match self {
            Self::Sqlite(store) => store.delete_preset(name).await,
        }
    }
}

#[async_trait]
impl SkillStore for PersistentStore {
    async fn list_skills(&self, role: SessionRole) -> StoreResult<Vec<SkillRecord>> {
        match self {
            Self::Sqlite(store) => store.list_skills(role).await,
        }
    }

    async fn get_skill(&self, role: SessionRole, name: &str) -> StoreResult<SkillRecord> {
        match self {
            Self::Sqlite(store) => store.get_skill(role, name).await,
        }
    }

    async fn add_skill(
        &self,
        role: SessionRole,
        name: &str,
        spec: SkillVersionSpec,
    ) -> StoreResult<SkillRecord> {
        match self {
            Self::Sqlite(store) => store.add_skill(role, name, spec).await,
        }
    }

    async fn update_skill(
        &self,
        role: SessionRole,
        name: &str,
        patch: &SkillUpdatePatch,
    ) -> StoreResult<SkillRecord> {
        match self {
            Self::Sqlite(store) => store.update_skill(role, name, patch).await,
        }
    }

    async fn rollback_skill(
        &self,
        role: SessionRole,
        name: &str,
        target_version: u64,
    ) -> StoreResult<SkillRecord> {
        match self {
            Self::Sqlite(store) => store.rollback_skill(role, name, target_version).await,
        }
    }
}

#[async_trait]
impl JobStore for PersistentStore {
    async fn submit_job(&self, branch: &str, base: &str) -> StoreResult<Job> {
        match self {
            Self::Sqlite(store) => store.submit_job(branch, base).await,
        }
    }

    async fn submit_job_with_id(&self, job_id: &str, branch: &str, base: &str) -> StoreResult<Job> {
        match self {
            Self::Sqlite(store) => store.submit_job_with_id(job_id, branch, base).await,
        }
    }

    async fn get_job(&self, job_id: &str) -> StoreResult<Job> {
        match self {
            Self::Sqlite(store) => store.get_job(job_id).await,
        }
    }

    async fn list_jobs(&self) -> StoreResult<HashMap<String, Job>> {
        match self {
            Self::Sqlite(store) => store.list_jobs().await,
        }
    }

    async fn set_job_status(
        &self,
        job_id: &str,
        expected: JobStatus,
        next: JobStatus,
    ) -> StoreResult<Job> {
        match self {
            Self::Sqlite(store) => store.set_job_status(job_id, expected, next).await,
        }
    }

    async fn set_job_work_branch(
        &self,
        job_id: &str,
        expected_work_branch: &str,
        next_work_branch: &str,
    ) -> StoreResult<Job> {
        match self {
            Self::Sqlite(store) => {
                store
                    .set_job_work_branch(job_id, expected_work_branch, next_work_branch)
                    .await
            }
        }
    }
}

#[async_trait]
impl MessageQueueStore for PersistentStore {
    async fn enqueue_message(
        &self,
        queue: &str,
        payload: serde_json::Value,
    ) -> StoreResult<MessageQueueItem> {
        match self {
            PersistentStore::Sqlite(store) => store.enqueue_message(queue, payload).await,
        }
    }

    async fn dequeue_message(&self, queue: &str) -> StoreResult<Option<MessageQueueItem>> {
        match self {
            PersistentStore::Sqlite(store) => store.dequeue_message(queue).await,
        }
    }

    async fn peek_message(&self, queue: &str) -> StoreResult<Option<MessageQueueItem>> {
        match self {
            PersistentStore::Sqlite(store) => store.peek_message(queue).await,
        }
    }

    async fn list_queue_messages(&self, queue: &str) -> StoreResult<Vec<MessageQueueItem>> {
        match self {
            PersistentStore::Sqlite(store) => store.list_queue_messages(queue).await,
        }
    }

    async fn list_message_queues(&self) -> StoreResult<Vec<String>> {
        match self {
            PersistentStore::Sqlite(store) => store.list_message_queues().await,
        }
    }
}

impl ProcessShareableStore for PersistentStore {
    fn store_path(&self) -> &Path {
        delegate_persistent_store!(self, store, store.store_path())
    }
}
