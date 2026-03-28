use std::sync::{Arc, RwLock};

use super::Store;
use super::state::StoreState;
use crate::StoreResult as Result;
use crate::{NewNode, Node, SessionAnchorPatch};

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
        let head_id = state.plan_fork(name, from_ref)?;
        state.apply_fork(name.to_owned(), head_id.clone())?;
        Ok(head_id)
    }

    fn get_branch_head(&self, name: &str) -> Result<String> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_branch_head(name)
            .map(str::to_owned)
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

    fn rebase_session(&self, name: &str, patch: &SessionAnchorPatch) -> Result<String> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let plan = state.plan_rebase_session(name, patch)?;

        for node in plan.nodes {
            state.insert_existing_node(node)?;
        }
        state.apply_set_branch_head(plan.branch, &plan.expected_old_head, plan.new_head.clone())?;
        Ok(plan.new_head)
    }
}
