use std::collections::HashMap;
use std::path::Path;

use async_trait::async_trait;
use coco_mem::{
    BranchStore, Job, JobStatus, JobStore, MergeParent, MessageQueueItem, MessageQueueStore,
    NewNode, Node, NodeStore, Preset, PresetRecord, PresetStore, ProcessShareableStore,
    PromptAnchor, SessionAnchorPatch, SessionRole, SessionState, SessionStore, SkillRecord,
    SkillStore, SkillUpdatePatch, SkillVersionSpec, StoreResult,
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

#[async_trait]
impl<S> NodeStore for ConsoleStore<S>
where
    S: NodeStore + Sync,
{
    fn root_id(&self) -> String {
        self.inner.root_id()
    }

    async fn append(&self, node: NewNode) -> StoreResult<String> {
        let result = self.inner.append(node).await;
        self.notify_if_ok(result)
    }

    async fn ancestry(&self, head_ref: &str) -> StoreResult<Vec<Node>> {
        self.inner.ancestry(head_ref).await
    }

    async fn log(&self, base_ref: &str, head_ref: &str) -> StoreResult<Vec<Node>> {
        self.inner.log(base_ref, head_ref).await
    }

    async fn get_node(&self, id: &str) -> StoreResult<Node> {
        self.inner.get_node(id).await
    }

    async fn list_children(&self, node_id: &str) -> StoreResult<Vec<Node>> {
        self.inner.list_children(node_id).await
    }
}

#[async_trait]
impl<S> BranchStore for ConsoleStore<S>
where
    S: BranchStore + Sync,
{
    async fn fork(&self, name: &str, from_ref: &str) -> StoreResult<String> {
        self.notify_if_ok(self.inner.fork(name, from_ref).await)
    }

    async fn get_branch_head(&self, name: &str) -> StoreResult<String> {
        self.inner.get_branch_head(name).await
    }

    async fn delete_branch(&self, name: &str) -> StoreResult<()> {
        self.notify_if_ok(self.inner.delete_branch(name).await)
    }

    async fn set_branch_head(
        &self,
        name: &str,
        expected_old_head: &str,
        new_head: &str,
    ) -> StoreResult<()> {
        self.notify_if_ok(
            self.inner
                .set_branch_head(name, expected_old_head, new_head)
                .await,
        )
    }
}

#[async_trait]
impl<S> SessionStore for ConsoleStore<S>
where
    S: SessionStore + Sync,
{
    async fn list_session_states(&self) -> StoreResult<HashMap<String, SessionState>> {
        self.inner.list_session_states().await
    }

    async fn get_session_state(&self, name: &str) -> StoreResult<SessionState> {
        self.inner.get_session_state(name).await
    }

    async fn set_session_state(
        &self,
        name: &str,
        expected: Option<&SessionState>,
        next: SessionState,
    ) -> StoreResult<SessionState> {
        self.notify_if_ok(self.inner.set_session_state(name, expected, next).await)
    }

    async fn rebase_session(&self, name: &str, patch: &SessionAnchorPatch) -> StoreResult<String> {
        self.notify_if_ok(self.inner.rebase_session(name, patch).await)
    }

    async fn handoff_session(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
        prompt: &str,
    ) -> StoreResult<String> {
        self.notify_if_ok(self.inner.handoff_session(name, patch, prompt).await)
    }
}

#[async_trait]
impl<S> PresetStore for ConsoleStore<S>
where
    S: PresetStore + Sync,
{
    async fn list_preset_records(&self) -> StoreResult<HashMap<String, PresetRecord>> {
        self.inner.list_preset_records().await
    }

    async fn get_preset_record(&self, name: &str) -> StoreResult<PresetRecord> {
        self.inner.get_preset_record(name).await
    }

    async fn set_preset(&self, name: &str, config: Preset) -> StoreResult<PresetRecord> {
        self.notify_if_ok(self.inner.set_preset(name, config).await)
    }

    async fn rollback_preset(&self, name: &str, target_version: u64) -> StoreResult<PresetRecord> {
        self.notify_if_ok(self.inner.rollback_preset(name, target_version).await)
    }

    async fn delete_preset(&self, name: &str) -> StoreResult<()> {
        self.notify_if_ok(self.inner.delete_preset(name).await)
    }
}

#[async_trait]
impl<S> SkillStore for ConsoleStore<S>
where
    S: SkillStore + Sync,
{
    async fn list_skills(&self, role: SessionRole) -> StoreResult<Vec<SkillRecord>> {
        self.inner.list_skills(role).await
    }

    async fn get_skill(&self, role: SessionRole, name: &str) -> StoreResult<SkillRecord> {
        self.inner.get_skill(role, name).await
    }

    async fn add_skill(
        &self,
        role: SessionRole,
        name: &str,
        spec: SkillVersionSpec,
    ) -> StoreResult<SkillRecord> {
        self.notify_if_ok(self.inner.add_skill(role, name, spec).await)
    }

    async fn update_skill(
        &self,
        role: SessionRole,
        name: &str,
        patch: &SkillUpdatePatch,
    ) -> StoreResult<SkillRecord> {
        self.notify_if_ok(self.inner.update_skill(role, name, patch).await)
    }

    async fn rollback_skill(
        &self,
        role: SessionRole,
        name: &str,
        target_version: u64,
    ) -> StoreResult<SkillRecord> {
        self.notify_if_ok(self.inner.rollback_skill(role, name, target_version).await)
    }
}

#[async_trait]
impl<S> JobStore for ConsoleStore<S>
where
    S: JobStore + Sync,
{
    async fn submit_job(&self, branch: &str, base: &str) -> StoreResult<Job> {
        self.notify_if_ok(self.inner.submit_job(branch, base).await)
    }

    async fn submit_job_with_prompt_base(
        &self,
        branch: &str,
        prompt: PromptAnchor,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionAnchorPatch>,
    ) -> StoreResult<Job> {
        self.notify_if_ok(
            self.inner
                .submit_job_with_prompt_base(branch, prompt, merge_parents, session_patch)
                .await,
        )
    }

    async fn submit_job_with_id(&self, job_id: &str, branch: &str, base: &str) -> StoreResult<Job> {
        self.notify_if_ok(self.inner.submit_job_with_id(job_id, branch, base).await)
    }

    async fn submit_job_with_id_and_prompt_base(
        &self,
        job_id: &str,
        branch: &str,
        prompt: PromptAnchor,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionAnchorPatch>,
    ) -> StoreResult<Job> {
        self.notify_if_ok(
            self.inner
                .submit_job_with_id_and_prompt_base(
                    job_id,
                    branch,
                    prompt,
                    merge_parents,
                    session_patch,
                )
                .await,
        )
    }

    async fn get_job(&self, job_id: &str) -> StoreResult<Job> {
        self.inner.get_job(job_id).await
    }

    async fn list_jobs(&self) -> StoreResult<HashMap<String, Job>> {
        self.inner.list_jobs().await
    }

    async fn set_job_status(
        &self,
        job_id: &str,
        expected: JobStatus,
        next: JobStatus,
    ) -> StoreResult<Job> {
        self.notify_if_ok(self.inner.set_job_status(job_id, expected, next).await)
    }

    async fn set_job_work_branch(
        &self,
        job_id: &str,
        expected_work_branch: &str,
        next_work_branch: &str,
    ) -> StoreResult<Job> {
        self.notify_if_ok(
            self.inner
                .set_job_work_branch(job_id, expected_work_branch, next_work_branch)
                .await,
        )
    }
}

#[async_trait]
impl<S> MessageQueueStore for ConsoleStore<S>
where
    S: MessageQueueStore + Sync,
{
    async fn enqueue_message(
        &self,
        queue: &str,
        payload: serde_json::Value,
    ) -> StoreResult<MessageQueueItem> {
        let item = self.inner.enqueue_message(queue, payload).await?;
        self.publisher.mark_changed();
        Ok(item)
    }

    async fn dequeue_message(&self, queue: &str) -> StoreResult<Option<MessageQueueItem>> {
        let item = self.inner.dequeue_message(queue).await?;
        if item.is_some() {
            self.publisher.mark_changed();
        }
        Ok(item)
    }

    async fn peek_message(&self, queue: &str) -> StoreResult<Option<MessageQueueItem>> {
        self.inner.peek_message(queue).await
    }

    async fn list_queue_messages(&self, queue: &str) -> StoreResult<Vec<MessageQueueItem>> {
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
