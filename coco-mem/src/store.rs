use std::collections::HashMap;
use std::path::Path;

mod fs;
mod memory;
mod sqlite;
mod state;

#[cfg(test)]
mod tests;

pub use fs::FsStore;
pub use memory::MemoryStore;
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
    fn handoff_session(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
        prompt: &str,
    ) -> StoreResult<String>;
}

/// Preset storage API.
pub trait PresetStore {
    /// Returns all persisted preset records keyed by preset name.
    fn list_preset_records(&self) -> StoreResult<HashMap<String, PresetRecord>>;

    /// Returns one preset record by preset name.
    fn get_preset_record(&self, name: &str) -> StoreResult<PresetRecord>;

    /// Creates a new version for a preset under a preset name.
    fn set_preset(&self, name: &str, preset: Preset) -> StoreResult<PresetRecord>;

    /// Creates a new version cloned from a previous preset version.
    fn rollback_preset(&self, name: &str, target_version: u64) -> StoreResult<PresetRecord>;

    /// Deletes one preset by preset name.
    fn delete_preset(&self, name: &str) -> StoreResult<()>;
}

/// Persisted skill storage API.
pub trait SkillStore {
    /// Returns all persisted skills for the given role.
    fn list_skills(&self, role: SessionRole) -> StoreResult<Vec<SkillRecord>>;

    /// Returns one persisted skill for the given role and name.
    fn get_skill(&self, role: SessionRole, name: &str) -> StoreResult<SkillRecord>;

    /// Creates a new persisted skill for the given role.
    fn add_skill(
        &self,
        role: SessionRole,
        name: &str,
        spec: SkillVersionSpec,
    ) -> StoreResult<SkillRecord>;

    /// Creates a new version for an existing skill by patching the current version.
    fn update_skill(
        &self,
        role: SessionRole,
        name: &str,
        patch: &SkillUpdatePatch,
    ) -> StoreResult<SkillRecord>;

    /// Creates a new version cloned from a previous version and makes it current.
    fn rollback_skill(
        &self,
        role: SessionRole,
        name: &str,
        target_version: u64,
    ) -> StoreResult<SkillRecord>;
}

/// Prompt job storage API.
pub trait JobStore {
    /// Creates a new single-task prompt job record with a generated id.
    ///
    /// Rejects the request when the branch already has an unfinished prompt job.
    fn submit_job(&self, branch: &str, base: &str) -> StoreResult<Job>;

    /// Creates a new single-task prompt job record with a caller-provided id.
    ///
    /// Rejects the request when the branch already has an unfinished prompt job.
    fn submit_job_with_id(&self, job_id: &str, branch: &str, base: &str) -> StoreResult<Job>;

    /// Returns a persisted prompt job.
    fn get_job(&self, job_id: &str) -> StoreResult<Job>;

    /// Returns all persisted prompt jobs keyed by job id.
    fn list_jobs(&self) -> StoreResult<HashMap<String, Job>>;

    /// Updates a prompt job lifecycle state when the current state matches.
    fn set_job_status(
        &self,
        job_id: &str,
        expected: JobStatus,
        next: JobStatus,
    ) -> StoreResult<Job>;

    /// Moves the current work branch for an unfinished prompt job.
    fn set_job_work_branch(
        &self,
        job_id: &str,
        expected_work_branch: &str,
        next_work_branch: &str,
    ) -> StoreResult<Job>;
}

/// Generic persistent message queue storage API.
pub trait MessageQueueStore {
    /// Enqueues one message in a named queue.
    fn enqueue_message(
        &self,
        queue: &str,
        payload: serde_json::Value,
    ) -> StoreResult<MessageQueueItem>;

    /// Removes and returns the oldest message in a named queue.
    fn dequeue_message(&self, queue: &str) -> StoreResult<Option<MessageQueueItem>>;

    /// Returns the oldest message in a named queue without removing it.
    fn peek_message(&self, queue: &str) -> StoreResult<Option<MessageQueueItem>>;

    /// Returns all persisted messages for a named queue in dequeue order.
    fn list_queue_messages(&self, queue: &str) -> StoreResult<Vec<MessageQueueItem>>;

    /// Returns all queue names that currently contain at least one message.
    fn list_message_queues(&self) -> StoreResult<Vec<String>>;
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
    Fs(FsStore),
    Sqlite(SqliteStore),
}

impl PersistentStore {
    pub fn open_or_migrate_fs(path: impl AsRef<Path>) -> StoreResult<Self> {
        SqliteStore::open_or_migrate_fs(path).map(Self::Sqlite)
    }

    pub fn open_read_only_or_migrate_fs(path: impl AsRef<Path>) -> StoreResult<Self> {
        let path = path.as_ref();
        if sqlite::sqlite_database_path(path).is_file()
            && sqlite::fs_migration_complete_marker_exists(path)?
        {
            return SqliteStore::open_read_only_or_migrate_fs(path).map(Self::Sqlite);
        }
        if sqlite::sqlite_database_path(path).is_file() && !sqlite::legacy_fs_store_exists(path) {
            return SqliteStore::open_read_only_or_migrate_fs(path).map(Self::Sqlite);
        }
        if sqlite::legacy_fs_store_exists(path) {
            return FsStore::open_read_only(path).map(Self::Fs);
        }
        SqliteStore::open_read_only_or_migrate_fs(path).map(Self::Sqlite)
    }

    pub fn store_path(&self) -> &Path {
        match self {
            Self::Fs(store) => store.store_path(),
            Self::Sqlite(store) => store.store_path(),
        }
    }
}

macro_rules! delegate_persistent_store {
    ($self:expr, $store:ident, $body:expr) => {
        match $self {
            PersistentStore::Fs($store) => $body,
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

    fn handoff_session(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
        prompt: &str,
    ) -> StoreResult<String> {
        delegate_persistent_store!(self, store, store.handoff_session(name, patch, prompt))
    }
}

impl PresetStore for PersistentStore {
    fn list_preset_records(&self) -> StoreResult<HashMap<String, PresetRecord>> {
        delegate_persistent_store!(self, store, store.list_preset_records())
    }

    fn get_preset_record(&self, name: &str) -> StoreResult<PresetRecord> {
        delegate_persistent_store!(self, store, store.get_preset_record(name))
    }

    fn set_preset(&self, name: &str, preset: Preset) -> StoreResult<PresetRecord> {
        delegate_persistent_store!(self, store, store.set_preset(name, preset))
    }

    fn rollback_preset(&self, name: &str, target_version: u64) -> StoreResult<PresetRecord> {
        delegate_persistent_store!(self, store, store.rollback_preset(name, target_version))
    }

    fn delete_preset(&self, name: &str) -> StoreResult<()> {
        delegate_persistent_store!(self, store, store.delete_preset(name))
    }
}

impl SkillStore for PersistentStore {
    fn list_skills(&self, role: SessionRole) -> StoreResult<Vec<SkillRecord>> {
        delegate_persistent_store!(self, store, store.list_skills(role))
    }

    fn get_skill(&self, role: SessionRole, name: &str) -> StoreResult<SkillRecord> {
        delegate_persistent_store!(self, store, store.get_skill(role, name))
    }

    fn add_skill(
        &self,
        role: SessionRole,
        name: &str,
        spec: SkillVersionSpec,
    ) -> StoreResult<SkillRecord> {
        delegate_persistent_store!(self, store, store.add_skill(role, name, spec))
    }

    fn update_skill(
        &self,
        role: SessionRole,
        name: &str,
        patch: &SkillUpdatePatch,
    ) -> StoreResult<SkillRecord> {
        delegate_persistent_store!(self, store, store.update_skill(role, name, patch))
    }

    fn rollback_skill(
        &self,
        role: SessionRole,
        name: &str,
        target_version: u64,
    ) -> StoreResult<SkillRecord> {
        delegate_persistent_store!(
            self,
            store,
            store.rollback_skill(role, name, target_version)
        )
    }
}

impl JobStore for PersistentStore {
    fn submit_job(&self, branch: &str, base: &str) -> StoreResult<Job> {
        delegate_persistent_store!(self, store, store.submit_job(branch, base))
    }

    fn submit_job_with_id(&self, job_id: &str, branch: &str, base: &str) -> StoreResult<Job> {
        delegate_persistent_store!(self, store, store.submit_job_with_id(job_id, branch, base))
    }

    fn get_job(&self, job_id: &str) -> StoreResult<Job> {
        delegate_persistent_store!(self, store, store.get_job(job_id))
    }

    fn list_jobs(&self) -> StoreResult<HashMap<String, Job>> {
        delegate_persistent_store!(self, store, store.list_jobs())
    }

    fn set_job_status(
        &self,
        job_id: &str,
        expected: JobStatus,
        next: JobStatus,
    ) -> StoreResult<Job> {
        delegate_persistent_store!(self, store, store.set_job_status(job_id, expected, next))
    }

    fn set_job_work_branch(
        &self,
        job_id: &str,
        expected_work_branch: &str,
        next_work_branch: &str,
    ) -> StoreResult<Job> {
        delegate_persistent_store!(
            self,
            store,
            store.set_job_work_branch(job_id, expected_work_branch, next_work_branch)
        )
    }
}

impl MessageQueueStore for PersistentStore {
    fn enqueue_message(
        &self,
        queue: &str,
        payload: serde_json::Value,
    ) -> StoreResult<MessageQueueItem> {
        delegate_persistent_store!(self, store, store.enqueue_message(queue, payload))
    }

    fn dequeue_message(&self, queue: &str) -> StoreResult<Option<MessageQueueItem>> {
        delegate_persistent_store!(self, store, store.dequeue_message(queue))
    }

    fn peek_message(&self, queue: &str) -> StoreResult<Option<MessageQueueItem>> {
        delegate_persistent_store!(self, store, store.peek_message(queue))
    }

    fn list_queue_messages(&self, queue: &str) -> StoreResult<Vec<MessageQueueItem>> {
        delegate_persistent_store!(self, store, store.list_queue_messages(queue))
    }

    fn list_message_queues(&self) -> StoreResult<Vec<String>> {
        delegate_persistent_store!(self, store, store.list_message_queues())
    }
}

impl ProcessShareableStore for PersistentStore {
    fn store_path(&self) -> &Path {
        delegate_persistent_store!(self, store, store.store_path())
    }
}
