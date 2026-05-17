use std::collections::HashMap;
use std::path::Path;

pub(crate) mod fs;
pub mod memory;
pub(crate) mod state;

#[cfg(test)]
mod tests;

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
