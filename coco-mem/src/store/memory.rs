use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::Store;
use super::state::StoreState;
use crate::StoreResult as Result;
use crate::{
    BranchConfig, Job, JobStatus, NewNode, Node, SessionAnchorPatch, SessionRole, SessionState,
    SkillGroups, SkillRecord, SkillUpdatePatch, SkillVersionSpec,
};

#[derive(Clone, Debug)]
pub struct MemoryStore {
    inner: Arc<RwLock<StoreState>>,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(StoreState::new())),
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot_state(&self) -> StoreState {
        self.inner.read().expect("store lock poisoned").clone()
    }
}

impl Store for MemoryStore {
    fn root_id(&self) -> String {
        self.inner
            .read()
            .expect("store lock poisoned")
            .root_id()
            .to_owned()
    }

    fn append(&self, node: NewNode) -> Result<String> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let node = state.plan_append_node(node)?;
        state.insert_existing_node(node)
    }

    fn fork(&self, name: &str, from_ref: &str) -> Result<String> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let plan = state.plan_fork(name, from_ref)?;
        state.apply_fork(name.to_owned(), plan.head_id.clone())?;
        Ok(plan.head_id)
    }

    fn get_branch_head(&self, name: &str) -> Result<String> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_branch_head(name)
            .map(str::to_owned)
    }

    fn delete_branch(&self, name: &str) -> Result<()> {
        self.inner
            .write()
            .expect("store lock poisoned")
            .delete_branch(name)
    }

    fn set_branch_head(&self, name: &str, expected_old_head: &str, new_head: &str) -> Result<()> {
        self.inner
            .write()
            .expect("store lock poisoned")
            .apply_set_branch_head(name.to_owned(), expected_old_head, new_head.to_owned())
    }

    fn ancestry(&self, head_ref: &str) -> Result<Vec<Node>> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .ancestry(head_ref)
            .map(|nodes| nodes.into_iter().cloned().collect())
    }

    fn log(&self, base_ref: &str, head_ref: &str) -> Result<Vec<Node>> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .log(base_ref, head_ref)
            .map(|nodes| nodes.into_iter().cloned().collect())
    }

    fn get_node(&self, id: &str) -> Result<Node> {
        self.inner.read().expect("store lock poisoned").get_node(id)
    }

    fn list_children(&self, node_id: &str) -> Result<Vec<Node>> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .list_children(node_id)
    }

    fn list_session_states(&self) -> Result<HashMap<String, SessionState>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .list_session_states())
    }

    fn get_session_state(&self, name: &str) -> Result<SessionState> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_session_state(name)
    }

    fn set_session_state(
        &self,
        name: &str,
        expected: Option<&SessionState>,
        next: SessionState,
    ) -> Result<SessionState> {
        self.inner
            .write()
            .expect("store lock poisoned")
            .set_session_state(name, expected, next)
    }

    fn rebase_session(&self, name: &str, patch: &SessionAnchorPatch) -> Result<String> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let plan = state.plan_rebase_session(name, patch)?;

        for node in plan.nodes {
            state.insert_existing_node(node)?;
        }
        state.apply_set_branch_head(plan.branch, &plan.expected_old_head, plan.new_head.clone())?;
        Ok(plan.new_head)
    }

    fn list_branch_configs(&self) -> Result<HashMap<String, BranchConfig>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .list_branch_configs())
    }

    fn get_branch_config(&self, name: &str) -> Result<BranchConfig> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_branch_config(name)
    }

    fn set_branch_config(&self, name: &str, config: BranchConfig) -> Result<BranchConfig> {
        Ok(self
            .inner
            .write()
            .expect("store lock poisoned")
            .set_branch_config(name, config))
    }

    fn delete_branch_config(&self, name: &str) -> Result<()> {
        self.inner
            .write()
            .expect("store lock poisoned")
            .delete_branch_config(name)
    }

    fn skill_groups(&self) -> Result<SkillGroups> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .skill_groups())
    }

    fn list_skills(&self, role: SessionRole) -> Result<Vec<SkillRecord>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .list_skills(role))
    }

    fn get_skill(&self, role: SessionRole, name: &str) -> Result<SkillRecord> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_skill(role, name)
    }

    fn add_skill(
        &self,
        role: SessionRole,
        name: &str,
        spec: SkillVersionSpec,
    ) -> Result<SkillRecord> {
        self.inner
            .write()
            .expect("store lock poisoned")
            .add_skill(role, name, spec)
    }

    fn update_skill(
        &self,
        role: SessionRole,
        name: &str,
        patch: &SkillUpdatePatch,
    ) -> Result<SkillRecord> {
        self.inner
            .write()
            .expect("store lock poisoned")
            .update_skill(role, name, patch)
    }

    fn rollback_skill(
        &self,
        role: SessionRole,
        name: &str,
        target_version: u64,
    ) -> Result<SkillRecord> {
        self.inner
            .write()
            .expect("store lock poisoned")
            .rollback_skill(role, name, target_version)
    }

    fn submit_job(&self, branch: &str, base: &str) -> Result<Job> {
        self.inner
            .write()
            .expect("store lock poisoned")
            .submit_job(branch, base)
    }

    fn get_job(&self, job_id: &str) -> Result<Job> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_job(job_id)
    }

    fn list_jobs(&self) -> Result<HashMap<String, Job>> {
        Ok(self.inner.read().expect("store lock poisoned").list_jobs())
    }

    fn set_job_status(&self, job_id: &str, expected: JobStatus, next: JobStatus) -> Result<Job> {
        self.inner
            .write()
            .expect("store lock poisoned")
            .set_job_status(job_id, expected, next)
    }
}
