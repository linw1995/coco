use std::path::PathBuf;
use std::sync::Arc;

use super::sqlite::SqliteStore;
use super::{
    BranchStore, JobStore, MessageQueueStore, NodeStore, PresetStore, SessionStore, SkillStore,
};
use crate::StoreResult as Result;
use crate::{
    Job, JobStatus, MessageQueueItem, NewNode, Node, Preset, PresetRecord, SessionAnchorPatch,
    SessionRole, SessionState, SkillRecord, SkillUpdatePatch, SkillVersionSpec,
};

#[derive(Clone, Debug)]
pub struct MemoryStore {
    store: SqliteStore,
    _dir: Arc<MemoryStoreDir>,
}

#[derive(Debug)]
struct MemoryStoreDir {
    path: PathBuf,
}

impl Drop for MemoryStoreDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        let dir = create_memory_store_dir();
        let store =
            SqliteStore::open(&dir.path).expect("temporary SQLite memory store should open");
        Self {
            store,
            _dir: Arc::new(dir),
        }
    }
}

fn create_memory_store_dir() -> MemoryStoreDir {
    let base = std::env::temp_dir();
    loop {
        let path = base.join(format!("coco-mem-{}", nanoid::nanoid!()));
        match std::fs::create_dir(&path) {
            Ok(()) => return MemoryStoreDir { path },
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => panic!(
                "failed to create temporary SQLite memory store at {:?}: {error}",
                path
            ),
        }
    }
}

impl NodeStore for MemoryStore {
    fn root_id(&self) -> String {
        self.store.root_id()
    }

    fn append(&self, node: NewNode) -> Result<String> {
        self.store.append(node)
    }

    fn ancestry(&self, head_ref: &str) -> Result<Vec<Node>> {
        self.store.ancestry(head_ref)
    }

    fn log(&self, base_ref: &str, head_ref: &str) -> Result<Vec<Node>> {
        self.store.log(base_ref, head_ref)
    }

    fn get_node(&self, id: &str) -> Result<Node> {
        self.store.get_node(id)
    }

    fn list_children(&self, node_id: &str) -> Result<Vec<Node>> {
        self.store.list_children(node_id)
    }
}

impl BranchStore for MemoryStore {
    fn fork(&self, name: &str, from_ref: &str) -> Result<String> {
        self.store.fork(name, from_ref)
    }

    fn get_branch_head(&self, name: &str) -> Result<String> {
        self.store.get_branch_head(name)
    }

    fn delete_branch(&self, name: &str) -> Result<()> {
        self.store.delete_branch(name)
    }

    fn set_branch_head(&self, name: &str, expected_old_head: &str, new_head: &str) -> Result<()> {
        self.store
            .set_branch_head(name, expected_old_head, new_head)
    }
}

impl SessionStore for MemoryStore {
    fn list_session_states(&self) -> Result<std::collections::HashMap<String, SessionState>> {
        self.store.list_session_states()
    }

    fn get_session_state(&self, name: &str) -> Result<SessionState> {
        self.store.get_session_state(name)
    }

    fn set_session_state(
        &self,
        name: &str,
        expected: Option<&SessionState>,
        next: SessionState,
    ) -> Result<SessionState> {
        self.store.set_session_state(name, expected, next)
    }

    fn rebase_session(&self, name: &str, patch: &SessionAnchorPatch) -> Result<String> {
        self.store.rebase_session(name, patch)
    }

    fn handoff_session(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
        prompt: &str,
    ) -> Result<String> {
        self.store.handoff_session(name, patch, prompt)
    }
}

impl PresetStore for MemoryStore {
    fn list_preset_records(&self) -> Result<std::collections::HashMap<String, PresetRecord>> {
        self.store.list_preset_records()
    }

    fn get_preset_record(&self, name: &str) -> Result<PresetRecord> {
        self.store.get_preset_record(name)
    }

    fn set_preset(&self, name: &str, config: Preset) -> Result<PresetRecord> {
        self.store.set_preset(name, config)
    }

    fn rollback_preset(&self, name: &str, target_version: u64) -> Result<PresetRecord> {
        self.store.rollback_preset(name, target_version)
    }

    fn delete_preset(&self, name: &str) -> Result<()> {
        self.store.delete_preset(name)
    }
}

impl SkillStore for MemoryStore {
    fn list_skills(&self, role: SessionRole) -> Result<Vec<SkillRecord>> {
        self.store.list_skills(role)
    }

    fn get_skill(&self, role: SessionRole, name: &str) -> Result<SkillRecord> {
        self.store.get_skill(role, name)
    }

    fn add_skill(
        &self,
        role: SessionRole,
        name: &str,
        spec: SkillVersionSpec,
    ) -> Result<SkillRecord> {
        self.store.add_skill(role, name, spec)
    }

    fn update_skill(
        &self,
        role: SessionRole,
        name: &str,
        patch: &SkillUpdatePatch,
    ) -> Result<SkillRecord> {
        self.store.update_skill(role, name, patch)
    }

    fn rollback_skill(
        &self,
        role: SessionRole,
        name: &str,
        target_version: u64,
    ) -> Result<SkillRecord> {
        self.store.rollback_skill(role, name, target_version)
    }
}

impl JobStore for MemoryStore {
    fn submit_job(&self, branch: &str, base: &str) -> Result<Job> {
        self.store.submit_job(branch, base)
    }

    fn submit_job_with_id(&self, job_id: &str, branch: &str, base: &str) -> Result<Job> {
        self.store.submit_job_with_id(job_id, branch, base)
    }

    fn get_job(&self, job_id: &str) -> Result<Job> {
        self.store.get_job(job_id)
    }

    fn list_jobs(&self) -> Result<std::collections::HashMap<String, Job>> {
        self.store.list_jobs()
    }

    fn set_job_status(&self, job_id: &str, expected: JobStatus, next: JobStatus) -> Result<Job> {
        self.store.set_job_status(job_id, expected, next)
    }

    fn set_job_work_branch(
        &self,
        job_id: &str,
        expected_work_branch: &str,
        next_work_branch: &str,
    ) -> Result<Job> {
        self.store
            .set_job_work_branch(job_id, expected_work_branch, next_work_branch)
    }
}

impl MessageQueueStore for MemoryStore {
    fn enqueue_message(&self, queue: &str, payload: serde_json::Value) -> Result<MessageQueueItem> {
        self.store.enqueue_message(queue, payload)
    }

    fn dequeue_message(&self, queue: &str) -> Result<Option<MessageQueueItem>> {
        self.store.dequeue_message(queue)
    }

    fn peek_message(&self, queue: &str) -> Result<Option<MessageQueueItem>> {
        self.store.peek_message(queue)
    }

    fn list_queue_messages(&self, queue: &str) -> Result<Vec<MessageQueueItem>> {
        self.store.list_queue_messages(queue)
    }

    fn list_message_queues(&self) -> Result<Vec<String>> {
        self.store.list_message_queues()
    }
}
