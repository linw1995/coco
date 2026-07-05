use std::collections::HashMap;
use std::path::Path;

use async_trait::async_trait;

mod lock;
mod sqlite;

#[cfg(test)]
mod tests;

pub use sqlite::{SqliteDatabase, SqliteGraphStore, SqliteStore};

use crate::{
    Job, JobStatus, MergeParent, MessageQueueItem, NewNode, NewNodeContent, Node, Preset,
    PresetRecord, PromptAnchor, SessionAnchorPatch, SessionRole, SessionState, SkillRecord,
    SkillUpdatePatch, SkillVersionSpec, StoreResult,
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

    /// Appends nodes after `parent` and moves a branch head in the same operation.
    async fn append_nodes_and_set_branch_head(
        &self,
        name: &str,
        expected_old_head: &str,
        parent: &str,
        nodes: Vec<NewNodeContent>,
    ) -> StoreResult<String>;
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

    /// Appends the prompt job base and creates a prompt job record atomically.
    ///
    /// Rejects the request when the branch already has an unfinished prompt job.
    async fn submit_job_with_prompt_base(
        &self,
        branch: &str,
        prompt: PromptAnchor,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionAnchorPatch>,
    ) -> StoreResult<Job>;

    /// Creates a new single-task prompt job record with a caller-provided id.
    ///
    /// Rejects the request when the branch already has an unfinished prompt job.
    async fn submit_job_with_id(&self, job_id: &str, branch: &str, base: &str) -> StoreResult<Job>;

    /// Appends the prompt job base and creates a prompt job record with a caller-provided id atomically.
    ///
    /// Rejects the request when the branch already has an unfinished prompt job.
    async fn submit_job_with_id_and_prompt_base(
        &self,
        job_id: &str,
        branch: &str,
        prompt: PromptAnchor,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionAnchorPatch>,
    ) -> StoreResult<Job>;

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

pub type PersistentStore = SqliteStore;
