use std::collections::HashMap;
use std::path::PathBuf;

pub(crate) mod collection;
pub(crate) mod fs;
pub(crate) mod log;
pub mod memory;
pub(crate) mod projection;
pub(crate) mod snapshot;
pub(crate) mod state;
pub(crate) mod versioned;

#[cfg(test)]
mod tests;

use crate::{
    BranchConfig, BranchConfigRecord, Job, JobStatus, NewNode, Node, SessionAnchorPatch,
    SessionRole, SessionState, SkillGroups, SkillRecord, SkillUpdatePatch, SkillVersionSpec,
    StoreResult,
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

/// Branch preset config storage API.
pub trait BranchConfigStore {
    /// Returns all persisted branch preset configs keyed by preset name.
    fn list_branch_configs(&self) -> StoreResult<HashMap<String, BranchConfig>>;

    /// Returns all persisted branch preset config records keyed by preset name.
    fn list_branch_config_records(&self) -> StoreResult<HashMap<String, BranchConfigRecord>>;

    /// Returns one branch preset config by preset name.
    fn get_branch_config(&self, name: &str) -> StoreResult<BranchConfig>;

    /// Returns one branch preset config record by preset name.
    fn get_branch_config_record(&self, name: &str) -> StoreResult<BranchConfigRecord>;

    /// Creates a new version for a branch preset config under a preset name.
    fn set_branch_config(
        &self,
        name: &str,
        config: BranchConfig,
    ) -> StoreResult<BranchConfigRecord>;

    /// Creates a new version cloned from a previous branch preset config version.
    fn rollback_branch_config(
        &self,
        name: &str,
        target_version: u64,
    ) -> StoreResult<BranchConfigRecord>;

    /// Deletes one branch preset config by preset name.
    fn delete_branch_config(&self, name: &str) -> StoreResult<()>;
}

/// Persisted skill storage API.
pub trait SkillStore {
    /// Returns the persisted skill groups.
    fn skill_groups(&self) -> StoreResult<SkillGroups>;

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
    /// Creates a new single-task prompt job record.
    ///
    /// Rejects the request when the branch already has an unfinished prompt job.
    fn submit_job(&self, branch: &str, base: &str) -> StoreResult<Job>;

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

/// Optional runtime metadata for stores with a process-shareable backing path.
pub trait RuntimeStore {
    /// Returns the backing store directory when the store is process-shareable.
    fn runtime_store_path(&self) -> Option<PathBuf> {
        None
    }
}
