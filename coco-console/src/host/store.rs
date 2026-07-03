use std::collections::HashMap;
use std::path::Path;

use coco_mem::{
    BranchStore, Job, JobStatus, JobStore, MessageQueueItem, MessageQueueStore, NewNode, Node,
    NodeStore, Preset, PresetRecord, PresetStore, ProcessShareableStore, SessionAnchorPatch,
    SessionRole, SessionState, SessionStore, SkillRecord, SkillStore, SkillUpdatePatch,
    SkillVersionSpec, StoreResult,
};

use crate::ConsolePublisher;

#[derive(Clone)]
pub struct ConsoleStore<S> {
    inner: S,
    publisher: ConsolePublisher,
}

impl<S> ConsoleStore<S> {
    pub fn new(inner: S, publisher: ConsolePublisher) -> Self {
        Self { inner, publisher }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub fn publisher(&self) -> &ConsolePublisher {
        &self.publisher
    }

    fn notify_if_ok<T>(&self, result: StoreResult<T>) -> StoreResult<T> {
        if result.is_ok() {
            self.publisher.mark_changed();
        }
        result
    }
}

impl<S> NodeStore for ConsoleStore<S>
where
    S: NodeStore,
{
    fn root_id(&self) -> String {
        self.inner.root_id()
    }

    fn append(&self, node: NewNode) -> StoreResult<String> {
        self.notify_if_ok(self.inner.append(node))
    }

    fn ancestry(&self, head_ref: &str) -> StoreResult<Vec<Node>> {
        self.inner.ancestry(head_ref)
    }

    fn log(&self, base_ref: &str, head_ref: &str) -> StoreResult<Vec<Node>> {
        self.inner.log(base_ref, head_ref)
    }

    fn get_node(&self, id: &str) -> StoreResult<Node> {
        self.inner.get_node(id)
    }

    fn list_children(&self, node_id: &str) -> StoreResult<Vec<Node>> {
        self.inner.list_children(node_id)
    }
}

impl<S> BranchStore for ConsoleStore<S>
where
    S: BranchStore,
{
    fn fork(&self, name: &str, from_ref: &str) -> StoreResult<String> {
        self.notify_if_ok(self.inner.fork(name, from_ref))
    }

    fn get_branch_head(&self, name: &str) -> StoreResult<String> {
        self.inner.get_branch_head(name)
    }

    fn delete_branch(&self, name: &str) -> StoreResult<()> {
        self.notify_if_ok(self.inner.delete_branch(name))
    }

    fn set_branch_head(
        &self,
        name: &str,
        expected_old_head: &str,
        new_head: &str,
    ) -> StoreResult<()> {
        self.notify_if_ok(
            self.inner
                .set_branch_head(name, expected_old_head, new_head),
        )
    }
}

impl<S> SessionStore for ConsoleStore<S>
where
    S: SessionStore + Sync,
{
    fn list_session_states(&self) -> StoreResult<HashMap<String, SessionState>> {
        self.inner.list_session_states()
    }

    fn get_session_state(&self, name: &str) -> StoreResult<SessionState> {
        self.inner.get_session_state(name)
    }

    fn set_session_state(
        &self,
        name: &str,
        expected: Option<&SessionState>,
        next: SessionState,
    ) -> StoreResult<SessionState> {
        self.notify_if_ok(self.inner.set_session_state(name, expected, next))
    }

    async fn rebase_session<'a>(
        &'a self,
        name: &'a str,
        patch: &'a SessionAnchorPatch,
    ) -> StoreResult<String> {
        self.notify_if_ok(self.inner.rebase_session(name, patch).await)
    }

    async fn handoff_session<'a>(
        &'a self,
        name: &'a str,
        patch: &'a SessionAnchorPatch,
        prompt: &'a str,
    ) -> StoreResult<String> {
        self.notify_if_ok(self.inner.handoff_session(name, patch, prompt).await)
    }
}

impl<S> PresetStore for ConsoleStore<S>
where
    S: PresetStore + Sync,
{
    async fn list_preset_records(&self) -> StoreResult<HashMap<String, PresetRecord>> {
        self.inner.list_preset_records().await
    }

    async fn get_preset_record<'a>(&'a self, name: &'a str) -> StoreResult<PresetRecord> {
        self.inner.get_preset_record(name).await
    }

    async fn set_preset<'a>(&'a self, name: &'a str, config: Preset) -> StoreResult<PresetRecord> {
        self.notify_if_ok(self.inner.set_preset(name, config).await)
    }

    async fn rollback_preset<'a>(
        &'a self,
        name: &'a str,
        target_version: u64,
    ) -> StoreResult<PresetRecord> {
        self.notify_if_ok(self.inner.rollback_preset(name, target_version).await)
    }

    async fn delete_preset<'a>(&'a self, name: &'a str) -> StoreResult<()> {
        self.notify_if_ok(self.inner.delete_preset(name).await)
    }
}

impl<S> SkillStore for ConsoleStore<S>
where
    S: SkillStore + Sync,
{
    async fn list_skills(&self, role: SessionRole) -> StoreResult<Vec<SkillRecord>> {
        self.inner.list_skills(role).await
    }

    async fn get_skill<'a>(&'a self, role: SessionRole, name: &'a str) -> StoreResult<SkillRecord> {
        self.inner.get_skill(role, name).await
    }

    async fn add_skill<'a>(
        &'a self,
        role: SessionRole,
        name: &'a str,
        spec: SkillVersionSpec,
    ) -> StoreResult<SkillRecord> {
        self.notify_if_ok(self.inner.add_skill(role, name, spec).await)
    }

    async fn update_skill<'a>(
        &'a self,
        role: SessionRole,
        name: &'a str,
        patch: &'a SkillUpdatePatch,
    ) -> StoreResult<SkillRecord> {
        self.notify_if_ok(self.inner.update_skill(role, name, patch).await)
    }

    async fn rollback_skill<'a>(
        &'a self,
        role: SessionRole,
        name: &'a str,
        target_version: u64,
    ) -> StoreResult<SkillRecord> {
        self.notify_if_ok(self.inner.rollback_skill(role, name, target_version).await)
    }
}

impl<S> JobStore for ConsoleStore<S>
where
    S: JobStore + Sync,
{
    async fn submit_job<'a>(&'a self, branch: &'a str, base: &'a str) -> StoreResult<Job> {
        self.notify_if_ok(self.inner.submit_job(branch, base).await)
    }

    async fn submit_job_with_id<'a>(
        &'a self,
        job_id: &'a str,
        branch: &'a str,
        base: &'a str,
    ) -> StoreResult<Job> {
        self.notify_if_ok(self.inner.submit_job_with_id(job_id, branch, base).await)
    }

    async fn get_job<'a>(&'a self, job_id: &'a str) -> StoreResult<Job> {
        self.inner.get_job(job_id).await
    }

    async fn list_jobs(&self) -> StoreResult<HashMap<String, Job>> {
        self.inner.list_jobs().await
    }

    async fn set_job_status<'a>(
        &'a self,
        job_id: &'a str,
        expected: JobStatus,
        next: JobStatus,
    ) -> StoreResult<Job> {
        self.notify_if_ok(self.inner.set_job_status(job_id, expected, next).await)
    }

    async fn set_job_work_branch<'a>(
        &'a self,
        job_id: &'a str,
        expected_work_branch: &'a str,
        next_work_branch: &'a str,
    ) -> StoreResult<Job> {
        self.notify_if_ok(
            self.inner
                .set_job_work_branch(job_id, expected_work_branch, next_work_branch)
                .await,
        )
    }
}

impl<S> MessageQueueStore for ConsoleStore<S>
where
    S: MessageQueueStore + Sync,
{
    async fn enqueue_message<'a>(
        &'a self,
        queue: &'a str,
        payload: serde_json::Value,
    ) -> StoreResult<MessageQueueItem> {
        let item = self.inner.enqueue_message(queue, payload).await?;
        self.publisher.mark_changed();
        Ok(item)
    }

    async fn dequeue_message<'a>(
        &'a self,
        queue: &'a str,
    ) -> StoreResult<Option<MessageQueueItem>> {
        let item = self.inner.dequeue_message(queue).await?;
        if item.is_some() {
            self.publisher.mark_changed();
        }
        Ok(item)
    }

    async fn peek_message<'a>(&'a self, queue: &'a str) -> StoreResult<Option<MessageQueueItem>> {
        self.inner.peek_message(queue).await
    }

    async fn list_queue_messages<'a>(
        &'a self,
        queue: &'a str,
    ) -> StoreResult<Vec<MessageQueueItem>> {
        self.inner.list_queue_messages(queue).await
    }

    async fn list_message_queues(&self) -> StoreResult<Vec<String>> {
        self.inner.list_message_queues().await
    }
}

impl<S> ProcessShareableStore for ConsoleStore<S>
where
    S: ProcessShareableStore,
{
    fn store_path(&self) -> &Path {
        self.inner.store_path()
    }
}
