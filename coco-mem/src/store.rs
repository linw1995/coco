use std::collections::HashMap;
use std::future::Future;
use std::path::Path;

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
pub trait NodeStore {
    /// Returns the global root node identifier.
    fn root_id(&self) -> String;

    /// Appends a new node and returns the persisted node identifier.
    fn append(&self, node: NewNode) -> StoreResult<String>;

    /// Returns the chain from a node id or branch reference back to the root.
    fn ancestry(&self, head_ref: &str) -> StoreResult<Vec<Node>>;

    /// Returns the main-parent chain from `head_ref` back to `base_ref`, inclusive.
    fn log(&self, base_ref: &str, head_ref: &str) -> StoreResult<Vec<Node>>;

    /// Returns a single node by branch name, full node ID, or node ID prefix.
    fn get_node(&self, id: &str) -> StoreResult<Node>;

    /// Returns all direct children for a node, including merge-parent edges.
    fn list_children(&self, node_id: &str) -> StoreResult<Vec<Node>>;
}

/// Branch reference storage API.
pub trait BranchStore {
    /// Creates a branch from a node id or branch reference and returns its head id.
    fn fork(&self, name: &str, from_ref: &str) -> StoreResult<String>;

    /// Returns the current head node identifier for a branch.
    fn get_branch_head(&self, name: &str) -> StoreResult<String>;

    /// Deletes a branch head and its session state.
    fn delete_branch(&self, name: &str) -> StoreResult<()>;

    /// Moves a branch head when the expected current head matches.
    fn set_branch_head(
        &self,
        name: &str,
        expected_old_head: &str,
        new_head: &str,
    ) -> StoreResult<()>;
}

/// Branch workflow session state storage API.
pub trait SessionStore {
    /// Returns all persisted branch workflow states keyed by branch.
    fn list_session_states(&self) -> StoreResult<HashMap<String, SessionState>>;

    /// Returns the workflow state for a branch.
    fn get_session_state(&self, name: &str) -> StoreResult<SessionState>;

    /// Updates the persisted session workflow state.
    fn set_session_state(
        &self,
        name: &str,
        expected: Option<&SessionState>,
        next: SessionState,
    ) -> StoreResult<SessionState>;

    /// Rewrites the visible session chain for a branch and returns the new head id.
    fn rebase_session(&self, name: &str, patch: &SessionAnchorPatch) -> StoreResult<String>;

    /// Appends a new full session anchor to reset provider context for a branch.
    fn handoff_session<'a>(
        &'a self,
        name: &'a str,
        patch: &'a SessionAnchorPatch,
        prompt: &'a str,
    ) -> impl Future<Output = StoreResult<String>> + Send + 'a;
}

/// Preset storage API.
pub trait PresetStore {
    /// Returns all persisted preset records keyed by preset name.
    fn list_preset_records(
        &self,
    ) -> impl Future<Output = StoreResult<HashMap<String, PresetRecord>>> + Send + '_;

    /// Returns one preset record by preset name.
    fn get_preset_record<'a>(
        &'a self,
        name: &'a str,
    ) -> impl Future<Output = StoreResult<PresetRecord>> + Send + 'a;

    /// Creates a new version for a preset under a preset name.
    fn set_preset<'a>(
        &'a self,
        name: &'a str,
        preset: Preset,
    ) -> impl Future<Output = StoreResult<PresetRecord>> + Send + 'a;

    /// Creates a new version cloned from a previous preset version.
    fn rollback_preset<'a>(
        &'a self,
        name: &'a str,
        target_version: u64,
    ) -> impl Future<Output = StoreResult<PresetRecord>> + Send + 'a;

    /// Deletes one preset by preset name.
    fn delete_preset<'a>(
        &'a self,
        name: &'a str,
    ) -> impl Future<Output = StoreResult<()>> + Send + 'a;
}

/// Persisted skill storage API.
pub trait SkillStore {
    /// Returns all persisted skills for the given role.
    fn list_skills(
        &self,
        role: SessionRole,
    ) -> impl Future<Output = StoreResult<Vec<SkillRecord>>> + Send + '_;

    /// Returns one persisted skill for the given role and name.
    fn get_skill<'a>(
        &'a self,
        role: SessionRole,
        name: &'a str,
    ) -> impl Future<Output = StoreResult<SkillRecord>> + Send + 'a;

    /// Creates a new persisted skill for the given role.
    fn add_skill<'a>(
        &'a self,
        role: SessionRole,
        name: &'a str,
        spec: SkillVersionSpec,
    ) -> impl Future<Output = StoreResult<SkillRecord>> + Send + 'a;

    /// Creates a new version for an existing skill by patching the current version.
    fn update_skill<'a>(
        &'a self,
        role: SessionRole,
        name: &'a str,
        patch: &'a SkillUpdatePatch,
    ) -> impl Future<Output = StoreResult<SkillRecord>> + Send + 'a;

    /// Creates a new version cloned from a previous version and makes it current.
    fn rollback_skill<'a>(
        &'a self,
        role: SessionRole,
        name: &'a str,
        target_version: u64,
    ) -> impl Future<Output = StoreResult<SkillRecord>> + Send + 'a;
}

/// Prompt job storage API.
pub trait JobStore {
    /// Creates a new single-task prompt job record with a generated id.
    ///
    /// Rejects the request when the branch already has an unfinished prompt job.
    fn submit_job<'a>(
        &'a self,
        branch: &'a str,
        base: &'a str,
    ) -> impl Future<Output = StoreResult<Job>> + Send + 'a;

    /// Creates a new single-task prompt job record with a caller-provided id.
    ///
    /// Rejects the request when the branch already has an unfinished prompt job.
    fn submit_job_with_id<'a>(
        &'a self,
        job_id: &'a str,
        branch: &'a str,
        base: &'a str,
    ) -> impl Future<Output = StoreResult<Job>> + Send + 'a;

    /// Returns a persisted prompt job.
    fn get_job<'a>(&'a self, job_id: &'a str)
    -> impl Future<Output = StoreResult<Job>> + Send + 'a;

    /// Returns all persisted prompt jobs keyed by job id.
    fn list_jobs(&self) -> impl Future<Output = StoreResult<HashMap<String, Job>>> + Send + '_;

    /// Updates a prompt job lifecycle state when the current state matches.
    fn set_job_status<'a>(
        &'a self,
        job_id: &'a str,
        expected: JobStatus,
        next: JobStatus,
    ) -> impl Future<Output = StoreResult<Job>> + Send + 'a;

    /// Moves the current work branch for an unfinished prompt job.
    fn set_job_work_branch<'a>(
        &'a self,
        job_id: &'a str,
        expected_work_branch: &'a str,
        next_work_branch: &'a str,
    ) -> impl Future<Output = StoreResult<Job>> + Send + 'a;
}

/// Generic persistent message queue storage API.
pub trait MessageQueueStore {
    /// Enqueues one message in a named queue.
    fn enqueue_message<'a>(
        &'a self,
        queue: &'a str,
        payload: serde_json::Value,
    ) -> impl Future<Output = StoreResult<MessageQueueItem>> + Send + 'a;

    /// Removes and returns the oldest message in a named queue.
    fn dequeue_message<'a>(
        &'a self,
        queue: &'a str,
    ) -> impl Future<Output = StoreResult<Option<MessageQueueItem>>> + Send + 'a;

    /// Returns the oldest message in a named queue without removing it.
    fn peek_message<'a>(
        &'a self,
        queue: &'a str,
    ) -> impl Future<Output = StoreResult<Option<MessageQueueItem>>> + Send + 'a;

    /// Returns all persisted messages for a named queue in dequeue order.
    fn list_queue_messages<'a>(
        &'a self,
        queue: &'a str,
    ) -> impl Future<Output = StoreResult<Vec<MessageQueueItem>>> + Send + 'a;

    /// Returns all queue names that currently contain at least one message.
    fn list_message_queues(&self) -> impl Future<Output = StoreResult<Vec<String>>> + Send + '_;
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
    pub fn open(path: impl AsRef<Path>) -> StoreResult<Self> {
        SqliteStore::open(path).map(Self::Sqlite)
    }

    pub fn open_read_only_or_upgrade_schema(path: impl AsRef<Path>) -> StoreResult<Self> {
        SqliteStore::open_read_only_or_upgrade_schema(path).map(Self::Sqlite)
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

impl NodeStore for PersistentStore {
    fn root_id(&self) -> String {
        delegate_persistent_store!(self, store, store.root_id())
    }

    fn append(&self, node: NewNode) -> StoreResult<String> {
        delegate_persistent_store!(self, store, store.append(node))
    }

    fn ancestry(&self, head_ref: &str) -> StoreResult<Vec<Node>> {
        delegate_persistent_store!(self, store, store.ancestry(head_ref))
    }

    fn log(&self, base_ref: &str, head_ref: &str) -> StoreResult<Vec<Node>> {
        delegate_persistent_store!(self, store, store.log(base_ref, head_ref))
    }

    fn get_node(&self, id: &str) -> StoreResult<Node> {
        delegate_persistent_store!(self, store, store.get_node(id))
    }

    fn list_children(&self, node_id: &str) -> StoreResult<Vec<Node>> {
        delegate_persistent_store!(self, store, store.list_children(node_id))
    }
}

impl BranchStore for PersistentStore {
    fn fork(&self, name: &str, from_ref: &str) -> StoreResult<String> {
        delegate_persistent_store!(self, store, store.fork(name, from_ref))
    }

    fn get_branch_head(&self, name: &str) -> StoreResult<String> {
        delegate_persistent_store!(self, store, store.get_branch_head(name))
    }

    fn delete_branch(&self, name: &str) -> StoreResult<()> {
        delegate_persistent_store!(self, store, store.delete_branch(name))
    }

    fn set_branch_head(
        &self,
        name: &str,
        expected_old_head: &str,
        new_head: &str,
    ) -> StoreResult<()> {
        delegate_persistent_store!(
            self,
            store,
            store.set_branch_head(name, expected_old_head, new_head)
        )
    }
}

impl SessionStore for PersistentStore {
    fn list_session_states(&self) -> StoreResult<HashMap<String, SessionState>> {
        delegate_persistent_store!(self, store, store.list_session_states())
    }

    fn get_session_state(&self, name: &str) -> StoreResult<SessionState> {
        delegate_persistent_store!(self, store, store.get_session_state(name))
    }

    fn set_session_state(
        &self,
        name: &str,
        expected: Option<&SessionState>,
        next: SessionState,
    ) -> StoreResult<SessionState> {
        delegate_persistent_store!(self, store, store.set_session_state(name, expected, next))
    }

    fn rebase_session(&self, name: &str, patch: &SessionAnchorPatch) -> StoreResult<String> {
        delegate_persistent_store!(self, store, store.rebase_session(name, patch))
    }

    async fn handoff_session<'a>(
        &'a self,
        name: &'a str,
        patch: &'a SessionAnchorPatch,
        prompt: &'a str,
    ) -> StoreResult<String> {
        match self {
            Self::Sqlite(store) => store.handoff_session(name, patch, prompt).await,
        }
    }
}

impl PresetStore for PersistentStore {
    async fn list_preset_records(&self) -> StoreResult<HashMap<String, PresetRecord>> {
        match self {
            Self::Sqlite(store) => store.list_preset_records().await,
        }
    }

    async fn get_preset_record<'a>(&'a self, name: &'a str) -> StoreResult<PresetRecord> {
        match self {
            Self::Sqlite(store) => store.get_preset_record(name).await,
        }
    }

    async fn set_preset<'a>(&'a self, name: &'a str, preset: Preset) -> StoreResult<PresetRecord> {
        match self {
            Self::Sqlite(store) => store.set_preset(name, preset).await,
        }
    }

    async fn rollback_preset<'a>(
        &'a self,
        name: &'a str,
        target_version: u64,
    ) -> StoreResult<PresetRecord> {
        match self {
            Self::Sqlite(store) => store.rollback_preset(name, target_version).await,
        }
    }

    async fn delete_preset<'a>(&'a self, name: &'a str) -> StoreResult<()> {
        match self {
            Self::Sqlite(store) => store.delete_preset(name).await,
        }
    }
}

impl SkillStore for PersistentStore {
    async fn list_skills(&self, role: SessionRole) -> StoreResult<Vec<SkillRecord>> {
        match self {
            Self::Sqlite(store) => store.list_skills(role).await,
        }
    }

    async fn get_skill<'a>(&'a self, role: SessionRole, name: &'a str) -> StoreResult<SkillRecord> {
        match self {
            Self::Sqlite(store) => store.get_skill(role, name).await,
        }
    }

    async fn add_skill<'a>(
        &'a self,
        role: SessionRole,
        name: &'a str,
        spec: SkillVersionSpec,
    ) -> StoreResult<SkillRecord> {
        match self {
            Self::Sqlite(store) => store.add_skill(role, name, spec).await,
        }
    }

    async fn update_skill<'a>(
        &'a self,
        role: SessionRole,
        name: &'a str,
        patch: &'a SkillUpdatePatch,
    ) -> StoreResult<SkillRecord> {
        match self {
            Self::Sqlite(store) => store.update_skill(role, name, patch).await,
        }
    }

    async fn rollback_skill<'a>(
        &'a self,
        role: SessionRole,
        name: &'a str,
        target_version: u64,
    ) -> StoreResult<SkillRecord> {
        match self {
            Self::Sqlite(store) => store.rollback_skill(role, name, target_version).await,
        }
    }
}

impl JobStore for PersistentStore {
    async fn submit_job<'a>(&'a self, branch: &'a str, base: &'a str) -> StoreResult<Job> {
        match self {
            Self::Sqlite(store) => store.submit_job(branch, base).await,
        }
    }

    async fn submit_job_with_id<'a>(
        &'a self,
        job_id: &'a str,
        branch: &'a str,
        base: &'a str,
    ) -> StoreResult<Job> {
        match self {
            Self::Sqlite(store) => store.submit_job_with_id(job_id, branch, base).await,
        }
    }

    async fn get_job<'a>(&'a self, job_id: &'a str) -> StoreResult<Job> {
        match self {
            Self::Sqlite(store) => store.get_job(job_id).await,
        }
    }

    async fn list_jobs(&self) -> StoreResult<HashMap<String, Job>> {
        match self {
            Self::Sqlite(store) => store.list_jobs().await,
        }
    }

    async fn set_job_status<'a>(
        &'a self,
        job_id: &'a str,
        expected: JobStatus,
        next: JobStatus,
    ) -> StoreResult<Job> {
        match self {
            Self::Sqlite(store) => store.set_job_status(job_id, expected, next).await,
        }
    }

    async fn set_job_work_branch<'a>(
        &'a self,
        job_id: &'a str,
        expected_work_branch: &'a str,
        next_work_branch: &'a str,
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

impl MessageQueueStore for PersistentStore {
    async fn enqueue_message<'a>(
        &'a self,
        queue: &'a str,
        payload: serde_json::Value,
    ) -> StoreResult<MessageQueueItem> {
        match self {
            PersistentStore::Sqlite(store) => store.enqueue_message(queue, payload).await,
        }
    }

    async fn dequeue_message<'a>(
        &'a self,
        queue: &'a str,
    ) -> StoreResult<Option<MessageQueueItem>> {
        match self {
            PersistentStore::Sqlite(store) => store.dequeue_message(queue).await,
        }
    }

    async fn peek_message<'a>(&'a self, queue: &'a str) -> StoreResult<Option<MessageQueueItem>> {
        match self {
            PersistentStore::Sqlite(store) => store.peek_message(queue).await,
        }
    }

    async fn list_queue_messages<'a>(
        &'a self,
        queue: &'a str,
    ) -> StoreResult<Vec<MessageQueueItem>> {
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
