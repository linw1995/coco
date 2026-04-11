use std::collections::HashMap;
use std::path::PathBuf;

pub(crate) mod fs;
pub mod memory;
pub(crate) mod state;

#[cfg(test)]
mod tests;

use crate::{Job, JobStatus, NewNode, Node, SessionAnchorPatch, SessionState, StoreResult};

/// Thread-safe node graph storage used by CoCo services.
pub trait Store: Clone + Send + Sync + 'static {
    /// Returns the global root node identifier.
    fn root_id(&self) -> String;

    /// Appends a new node and returns the persisted node identifier.
    fn append(&self, node: NewNode) -> StoreResult<String>;

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

    /// Returns the chain from a node id or branch reference back to the root.
    fn ancestry(&self, head_ref: &str) -> StoreResult<Vec<Node>>;

    /// Returns the main-parent chain from `head_ref` back to `base_ref`, inclusive.
    fn log(&self, base_ref: &str, head_ref: &str) -> StoreResult<Vec<Node>>;

    /// Returns a single node by branch name, full node ID, or node ID prefix.
    fn get_node(&self, id: &str) -> StoreResult<Node>;

    /// Returns all direct children for a node, including merge-parent edges.
    fn list_children(&self, node_id: &str) -> StoreResult<Vec<Node>>;

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

    /// Creates a new single-task prompt job record.
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

    /// Returns the backing store directory when the store is process-shareable.
    fn runtime_store_path(&self) -> Option<PathBuf> {
        None
    }
}
