use std::{collections::HashMap, path::Path, sync::Arc};

use coco_llm::coco_mem::{
    BranchStore, Job, JobStatus, JobStore, Kind, MergeParent, MessageQueueStore, Node, NodeStore,
    PromptAttachment, SessionStore, SkillStore, SqliteStore,
};
use coco_llm::{
    BackendError, BackendFailureContext, CompletionBackend, CompletionInput, CompletionOrigin,
    CompletionOverrides, CompletionRequest, Error as LlmError, LlmService, RigBackend,
    SessionConfigPatch,
};
use futures::future::{BoxFuture, FutureExt, Shared};
use jiff::Timestamp;
use serde::Serialize;
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tokio::task::JoinSet;

use crate::{BatchPromptResult, BranchPromptOutcome, BranchPromptRequest, EngineError};

type JobResult = std::result::Result<String, EngineError>;
type InflightJob = Shared<BoxFuture<'static, JobResult>>;
type InflightJobTable = Arc<AsyncMutex<HashMap<String, InflightJob>>>;
pub type BranchLockGuard = coco_llm::BranchLockGuard;
pub const SYSTEM_EVENT_QUEUE: &str = "system";
const LLM_BACKEND_FAILURE_RECOVERY_REQUESTED: &str = "llm.backend_failure.recovery_requested";
const SYSTEM_EVENT_VERSION: u64 = 1;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JobStatusSnapshot {
    pub job_id: String,
    pub created_at: Timestamp,
    pub finished_at: Option<Timestamp>,
    pub branch: String,
    pub work_branch: String,
    pub base: String,
    pub status: JobStatus,
    pub head: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PromptReply {
    execution_id: Option<String>,
    response_node_id: String,
    branch_head: String,
    text: String,
}

pub struct ConversationEngine<B = RigBackend, S = SqliteStore> {
    service: Arc<LlmService<B, S>>,
    inflight_jobs: InflightJobTable,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct SystemEventEnvelope<T> {
    #[serde(rename = "type")]
    event_type: &'static str,
    version: u64,
    dedupe_key: String,
    data: T,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct LlmBackendFailureRecoveryRequested {
    job_id: String,
    root_branch: String,
    work_branch: String,
    failed_branch: String,
    base_node_id: String,
    execution_id: String,
    error_node_id: String,
    retry_from_node_id: String,
    message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum JobRunOutcome {
    Completed,
    RecoveryQueued,
}

impl<B, S> Clone for ConversationEngine<B, S> {
    fn clone(&self) -> Self {
        Self {
            service: self.service.clone(),
            inflight_jobs: self.inflight_jobs.clone(),
        }
    }
}

impl ConversationEngine<RigBackend, SqliteStore> {
    pub fn with_service(service: Arc<LlmService<RigBackend, SqliteStore>>) -> Self {
        Self::new(service)
    }
}

impl<B, S> ConversationEngine<B, S> {
    pub fn new(service: Arc<LlmService<B, S>>) -> Self {
        Self {
            service,
            inflight_jobs: Arc::new(AsyncMutex::new(HashMap::new())),
        }
    }

    pub fn service(&self) -> &Arc<LlmService<B, S>> {
        &self.service
    }

    pub fn runtime_store_path(&self) -> Option<&Path> {
        self.service.runtime_store_path()
    }

    pub async fn lock_branch(&self, branch: &str) -> BranchLockGuard {
        self.service.lock_branch_scope(branch).await
    }
}

impl<B, S> ConversationEngine<B, S>
where
    S: NodeStore,
{
    pub fn session_supports_tool(
        &self,
        branch: &str,
        tool_name: &str,
    ) -> std::result::Result<bool, LlmError> {
        self.service.session_supports_tool(branch, tool_name)
    }
}

impl<B, S> ConversationEngine<B, S>
where
    B: CompletionBackend + 'static,
    S: NodeStore
        + BranchStore
        + SessionStore
        + JobStore
        + SkillStore
        + MessageQueueStore
        + Clone
        + Send
        + Sync
        + 'static,
{
    pub async fn reply(
        &self,
        branch: &str,
        prompt: &str,
    ) -> std::result::Result<String, EngineError> {
        self.reply_with_merge_parents(branch, prompt, vec![]).await
    }

    pub async fn reply_with_attachments(
        &self,
        branch: &str,
        prompt: &str,
        attachments: Vec<PromptAttachment>,
    ) -> std::result::Result<String, EngineError> {
        self.reply_with_prompt_options(branch, prompt, attachments, vec![], None)
            .await
    }

    pub async fn reply_with_merge_parents(
        &self,
        branch: &str,
        prompt: &str,
        merge_parents: Vec<MergeParent>,
    ) -> std::result::Result<String, EngineError> {
        self.reply_with_prompt_options(branch, prompt, vec![], merge_parents, None)
            .await
    }

    pub async fn reply_with_session_patch(
        &self,
        branch: &str,
        prompt: &str,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionConfigPatch>,
    ) -> std::result::Result<String, EngineError> {
        self.reply_with_prompt_options(branch, prompt, vec![], merge_parents, session_patch)
            .await
    }

    async fn reply_with_prompt_options(
        &self,
        branch: &str,
        prompt: &str,
        attachments: Vec<PromptAttachment>,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionConfigPatch>,
    ) -> std::result::Result<String, EngineError> {
        Ok(self
            .run_prompt_job(branch, prompt, attachments, merge_parents, session_patch)
            .await?
            .text)
    }

    pub async fn reply_many(
        &self,
        items: Vec<BranchPromptRequest>,
        max_concurrency: usize,
    ) -> BatchPromptResult {
        let limit = max_concurrency.max(1);
        tracing::info!(
            item_count = items.len(),
            max_concurrency = limit,
            "starting batch prompt"
        );
        let semaphore = Arc::new(Semaphore::new(limit));
        let mut tasks = JoinSet::new();

        for (index, item) in items.into_iter().enumerate() {
            let engine = self.clone();
            let semaphore = semaphore.clone();
            tasks.spawn(async move {
                let _permit = semaphore
                    .acquire_owned()
                    .await
                    .expect("batch prompt semaphore should stay open");
                let branch = item.branch;
                match engine
                    .run_prompt_job(
                        &branch,
                        &item.prompt,
                        item.attachments,
                        item.merge_parents,
                        None,
                    )
                    .await
                {
                    Ok(result) => (
                        index,
                        BranchPromptOutcome::succeeded(
                            branch,
                            result.execution_id,
                            result.response_node_id,
                            result.branch_head,
                            result.text,
                        ),
                    ),
                    Err(error) => (
                        index,
                        BranchPromptOutcome::failed(branch, error.to_string()),
                    ),
                }
            });
        }

        let mut outcomes = Vec::with_capacity(tasks.len());
        while let Some(result) = tasks.join_next().await {
            let (index, outcome) = result.expect("batch prompt task should not panic");
            outcomes.push((index, outcome));
        }
        outcomes.sort_by_key(|(index, _)| *index);

        BatchPromptResult {
            outcomes: outcomes.into_iter().map(|(_, outcome)| outcome).collect(),
        }
    }

    async fn run_prompt_job(
        &self,
        branch: &str,
        prompt: &str,
        attachments: Vec<PromptAttachment>,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionConfigPatch>,
    ) -> std::result::Result<PromptReply, EngineError> {
        let job = self
            .submit_job_with_attachments_and_session_patch(
                branch,
                prompt,
                attachments,
                merge_parents,
                session_patch,
            )
            .await?;
        let snapshot = self.drive_job_for_prompt(&job.job_id).await?;
        let job = self.service.store().get_job(&job.job_id)?;
        if !matches!(snapshot.status, JobStatus::Finished) {
            self.finish_job(&job).await?;
            return Err(EngineError::EngineFailed {
                message: format!("prompt job {:?} is waiting for recovery", job.job_id),
            });
        }
        build_prompt_reply(self.service.store(), &job, &snapshot)
    }

    pub async fn submit_job(
        &self,
        branch: &str,
        prompt: &str,
        merge_parents: Vec<MergeParent>,
    ) -> std::result::Result<Job, EngineError> {
        self.submit_job_with_session_patch(branch, prompt, merge_parents, None)
            .await
    }

    pub async fn submit_job_with_session_patch(
        &self,
        branch: &str,
        prompt: &str,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionConfigPatch>,
    ) -> std::result::Result<Job, EngineError> {
        self.submit_job_with_optional_id_attachments_and_session_patch(
            None,
            branch,
            prompt,
            vec![],
            merge_parents,
            session_patch,
        )
        .await
    }

    pub async fn submit_job_with_attachments_and_session_patch(
        &self,
        branch: &str,
        prompt: &str,
        attachments: Vec<PromptAttachment>,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionConfigPatch>,
    ) -> std::result::Result<Job, EngineError> {
        self.submit_job_with_optional_id_attachments_and_session_patch(
            None,
            branch,
            prompt,
            attachments,
            merge_parents,
            session_patch,
        )
        .await
    }

    pub async fn submit_job_with_id_and_session_patch(
        &self,
        job_id: &str,
        branch: &str,
        prompt: &str,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionConfigPatch>,
    ) -> std::result::Result<Job, EngineError> {
        self.submit_job_with_optional_id_attachments_and_session_patch(
            Some(job_id),
            branch,
            prompt,
            vec![],
            merge_parents,
            session_patch,
        )
        .await
    }

    async fn submit_job_with_optional_id_attachments_and_session_patch(
        &self,
        job_id: Option<&str>,
        branch: &str,
        prompt: &str,
        attachments: Vec<PromptAttachment>,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionConfigPatch>,
    ) -> std::result::Result<Job, EngineError> {
        self.ensure_prompt_job_can_submit(job_id, branch)?;
        let merge_parent_count = merge_parents.len();
        let has_session_patch = session_patch.is_some();
        let base = self.service.append_prompt_job_base(
            branch,
            prompt,
            &attachments,
            &merge_parents,
            session_patch.as_ref(),
        )?;
        let job = match job_id {
            Some(job_id) => self
                .service
                .store()
                .submit_job_with_id(job_id, branch, &base)?,
            None => self.service.store().submit_job(branch, &base)?,
        };
        tracing::info!(
            job_id = %job.job_id,
            branch = %job.branch,
            base = %job.base,
            merge_parent_count,
            has_session_patch,
            "submitted prompt job"
        );
        Ok(job)
    }

    fn ensure_prompt_job_can_submit(
        &self,
        job_id: Option<&str>,
        branch: &str,
    ) -> std::result::Result<(), EngineError> {
        if let Some(job_id) = job_id {
            match self.service.store().get_job(job_id) {
                Ok(_) => {
                    return Err(EngineError::EngineFailed {
                        message: format!("Prompt job {job_id:?} already exists"),
                    });
                }
                Err(coco_llm::coco_mem::StoreError::PromptJobNotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        if let Some(active_job) = self.active_branch_prompt_job(branch)? {
            return Err(EngineError::EngineFailed {
                message: format!(
                    "Branch {branch:?} already has an active prompt job {:?}",
                    active_job.job_id
                ),
            });
        }
        Ok(())
    }

    pub fn get_job(&self, job_id: &str) -> std::result::Result<JobStatusSnapshot, EngineError> {
        let job = self.service.store().get_job(job_id)?;
        self.build_job_status_snapshot(&job)
    }

    pub fn set_job_work_branch(
        &self,
        job_id: &str,
        expected_work_branch: &str,
        next_work_branch: &str,
    ) -> std::result::Result<JobStatusSnapshot, EngineError> {
        let job = self.service.store().set_job_work_branch(
            job_id,
            expected_work_branch,
            next_work_branch,
        )?;
        self.service.notify_job_status_changed(job_id);
        self.build_job_status_snapshot(&job)
    }

    pub fn active_branch_prompt_job(
        &self,
        branch: &str,
    ) -> std::result::Result<Option<Job>, EngineError> {
        self.active_branch_prompt_job_excluding(branch, None)
    }

    pub fn list_jobs(
        &self,
    ) -> std::result::Result<std::collections::HashMap<String, Job>, EngineError> {
        Ok(self.service.store().list_jobs()?)
    }

    pub async fn resume_incomplete_jobs(&self) -> std::result::Result<(), EngineError> {
        let jobs = self.list_jobs()?;
        let incomplete_count = jobs
            .values()
            .filter(|job| !matches!(job.status, JobStatus::Finished))
            .count();
        tracing::info!(incomplete_count, "resuming incomplete prompt jobs");
        for (job_id, job) in jobs {
            if matches!(job.status, JobStatus::Finished) {
                continue;
            }
            self.drive_job(&job_id).await?;
        }
        Ok(())
    }

    pub async fn drive_job(
        &self,
        job_id: &str,
    ) -> std::result::Result<JobStatusSnapshot, EngineError> {
        let _ = self.drive_job_singleflight(job_id, vec![]).await;
        self.get_job(job_id)
    }

    pub async fn join_job(
        &self,
        job_id: &str,
    ) -> std::result::Result<JobStatusSnapshot, EngineError> {
        let mut job_status = self.service.subscribe_job_status(job_id);
        loop {
            let snapshot = self.get_job(job_id)?;
            if matches!(snapshot.status, JobStatus::Finished) {
                self.service.clear_job_status_watch(job_id);
                return Ok(snapshot);
            }

            tracing::debug!(
                job_id = %job_id,
                status = ?snapshot.status,
                "waiting for prompt job status notification"
            );
            if job_status.changed().await.is_err() {
                return self.get_job(job_id);
            }
        }
    }

    pub async fn has_inflight_job(&self, job_id: &str) -> bool {
        self.inflight_jobs.lock().await.contains_key(job_id)
    }

    pub async fn drive_job_with_merge_parents(
        &self,
        job_id: &str,
        merge_parents: Vec<MergeParent>,
    ) -> std::result::Result<JobStatusSnapshot, EngineError> {
        let _ = self.drive_job_singleflight(job_id, merge_parents).await;
        self.get_job(job_id)
    }

    async fn drive_job_for_prompt(
        &self,
        job_id: &str,
    ) -> std::result::Result<JobStatusSnapshot, EngineError> {
        self.drive_job_singleflight(job_id, vec![]).await?;
        self.get_job(job_id)
    }

    async fn drive_job_singleflight(
        &self,
        job_id: &str,
        merge_parents: Vec<MergeParent>,
    ) -> std::result::Result<String, EngineError> {
        let job_id = job_id.to_owned();
        let merge_parent_count = merge_parents.len();
        tracing::debug!(
            job_id = %job_id,
            merge_parent_count,
            "joining prompt job singleflight"
        );
        let inflight_job = self.get_or_start_inflight_job(&job_id, merge_parents).await;
        let result = inflight_job.clone().await;

        self.inflight_jobs.lock().await.remove(&job_id);
        self.service.notify_job_status_changed(&job_id);

        result
    }

    async fn drive_job_once(
        &self,
        job_id: &str,
        merge_parents: Vec<MergeParent>,
    ) -> std::result::Result<String, EngineError> {
        let mut job = self.service.store().get_job(job_id)?;
        tracing::info!(
            job_id = %job.job_id,
            branch = %job.branch,
            work_branch = %job.work_branch,
            base = %job.base,
            status = ?job.status,
            merge_parent_count = merge_parents.len(),
            "driving prompt job"
        );

        if matches!(job.status, JobStatus::Queued) {
            job = self.service.store().set_job_status(
                job_id,
                JobStatus::Queued,
                JobStatus::Running,
            )?;
            self.service.notify_job_status_changed(job_id);
            tracing::info!(
                job_id = %job.job_id,
                branch = %job.branch,
                work_branch = %job.work_branch,
                "prompt job started"
            );
        }

        if matches!(job.status, JobStatus::Running) {
            self.ensure_job_has_exclusive_branch_access(&job)?;
            match self.resume_or_run_job(&job, merge_parents).await {
                Ok(JobRunOutcome::Completed) => {
                    job = match self.restore_root_branch_after_recovery(&job) {
                        Ok(job) => job,
                        Err(error) => {
                            self.finish_job(&job).await?;
                            return Err(error);
                        }
                    };
                    self.finish_job(&job).await?;
                }
                Ok(JobRunOutcome::RecoveryQueued) => {
                    tracing::info!(
                        job_id = %job.job_id,
                        branch = %job.branch,
                        work_branch = %job.work_branch,
                        "prompt job is waiting for recovery"
                    );
                }
                Err(error) => {
                    self.finish_job(&job).await?;
                    return Err(error);
                }
            }
        }

        let head = self.job_head(&job)?;
        tracing::info!(
            job_id = %job.job_id,
            branch = %job.branch,
            work_branch = %job.work_branch,
            head = %head,
            "prompt job drive completed"
        );
        Ok(head)
    }

    async fn resume_or_run_job(
        &self,
        job: &Job,
        merge_parents: Vec<MergeParent>,
    ) -> std::result::Result<JobRunOutcome, EngineError> {
        let store = self.service.store();
        let last_node = find_job_last_node(store, job)?;

        let retry_from_failure = if let Some(last_node) = last_node.as_ref() {
            match &last_node.kind {
                Kind::Text(_) => {
                    tracing::info!(
                        job_id = %job.job_id,
                        branch = %job.branch,
                        work_branch = %job.work_branch,
                        head = %last_node.id,
                        "prompt job already has terminal response node"
                    );
                    return Ok(JobRunOutcome::Completed);
                }
                Kind::Failure(_) => {
                    tracing::info!(
                        job_id = %job.job_id,
                        branch = %job.branch,
                        work_branch = %job.work_branch,
                        head = %last_node.id,
                        retry_from_node_id = %last_node.parent,
                        "retrying prompt job from before terminal failure node"
                    );
                    Some(last_node.parent.clone())
                }
                _ => None,
            }
        } else {
            None
        };

        let is_retrying_failure = retry_from_failure.is_some();
        let (origin_node_id, use_branch_session) = match retry_from_failure {
            Some(retry_from_failure) => (retry_from_failure, true),
            None => match last_node {
                Some(last_node) => (last_node.id, false),
                None => (job.base.clone(), true),
            },
        };
        let origin = if use_branch_session {
            CompletionOrigin::ReferenceWithBranchSession(origin_node_id)
        } else {
            CompletionOrigin::Reference(origin_node_id)
        };
        let origin_node = match &origin {
            CompletionOrigin::BranchHead => "<branch-head>",
            CompletionOrigin::Reference(node_id) => node_id.as_str(),
            CompletionOrigin::ReferenceWithBranchSession(node_id) => node_id.as_str(),
        };
        let input = if merge_parents.is_empty() {
            CompletionInput::Continue
        } else {
            CompletionInput::Prompt {
                text: String::new(),
                attachments: vec![],
                merge_parents,
                session_patch: None,
            }
        };
        tracing::info!(
            job_id = %job.job_id,
            branch = %job.branch,
            work_branch = %job.work_branch,
            origin = %origin_node,
            input = completion_input_kind(&input),
            "running prompt job completion"
        );
        let request = CompletionRequest {
            branch: job.work_branch.clone(),
            origin,
            input,
            overrides: CompletionOverrides::default(),
            active_skill_runtime: None,
        };

        match self.service.run(request).await {
            Ok(_) => Ok(JobRunOutcome::Completed),
            Err(source) => self.handle_completion_error(job, source, !is_retrying_failure),
        }
    }

    fn handle_completion_error(
        &self,
        job: &Job,
        source: LlmError,
        queue_recovery_event: bool,
    ) -> std::result::Result<JobRunOutcome, EngineError> {
        if let LlmError::Backend {
            source: backend_source,
            context,
        } = &source
        {
            if queue_recovery_event {
                self.enqueue_backend_failure_recovery_event(job, backend_source, context)?;
            } else {
                tracing::warn!(
                    job_id = %job.job_id,
                    branch = %job.branch,
                    work_branch = %job.work_branch,
                    execution_id = %context.execution_id,
                    error_node_id = %context.error_node_id,
                    retry_from_node_id = %context.retry_from_node_id,
                    error = %backend_source,
                    "suppressed duplicate backend failure recovery request while retrying failed job"
                );
            }
            return Ok(JobRunOutcome::RecoveryQueued);
        }
        Err(source.into())
    }

    fn enqueue_backend_failure_recovery_event(
        &self,
        job: &Job,
        source: &BackendError,
        context: &BackendFailureContext,
    ) -> std::result::Result<(), EngineError> {
        let event = backend_failure_recovery_event(job, source, context);
        let item = self.service.store().enqueue_message(
            SYSTEM_EVENT_QUEUE,
            serde_json::to_value(&event).expect("system event payload should serialize"),
        )?;
        tracing::warn!(
            job_id = %job.job_id,
            branch = %job.branch,
            work_branch = %job.work_branch,
            message_id = %item.message_id,
            queue = SYSTEM_EVENT_QUEUE,
            dedupe_key = %event.dedupe_key,
            execution_id = %context.execution_id,
            error_node_id = %context.error_node_id,
            retry_from_node_id = %context.retry_from_node_id,
            error = %source,
            "queued backend failure recovery request"
        );
        Ok(())
    }

    fn restore_root_branch_after_recovery(
        &self,
        job: &Job,
    ) -> std::result::Result<Job, EngineError> {
        if job.work_branch == job.branch {
            return Ok(job.clone());
        }

        let restored_from_work_branch = job.work_branch.clone();
        let response_head = self.job_head(job)?;
        let root_head = self.service.store().get_branch_head(&job.branch)?;
        self.service
            .store()
            .set_branch_head(&job.branch, &root_head, &response_head)?;
        let job =
            self.service
                .store()
                .set_job_work_branch(&job.job_id, &job.work_branch, &job.branch)?;
        self.service.notify_job_status_changed(&job.job_id);
        tracing::info!(
            job_id = %job.job_id,
            branch = %job.branch,
            restored_from_work_branch = %restored_from_work_branch,
            head = %response_head,
            "restored prompt job root branch from recovery branch"
        );
        Ok(job)
    }

    async fn finish_job(&self, job: &Job) -> std::result::Result<(), EngineError> {
        if matches!(job.status, JobStatus::Finished) {
            return Ok(());
        }
        self.service.store().set_job_status(
            &job.job_id,
            JobStatus::Running,
            JobStatus::Finished,
        )?;
        self.service.notify_job_status_changed(&job.job_id);
        tracing::info!(
            job_id = %job.job_id,
            branch = %job.branch,
            "prompt job finished"
        );
        Ok(())
    }

    fn build_job_status_snapshot(
        &self,
        job: &Job,
    ) -> std::result::Result<JobStatusSnapshot, EngineError> {
        let head = self.job_head(job)?;

        Ok(JobStatusSnapshot {
            job_id: job.job_id.clone(),
            created_at: job.created_at,
            finished_at: job.finished_at,
            branch: job.branch.clone(),
            work_branch: job.work_branch.clone(),
            base: job.base.clone(),
            status: job.status,
            head,
        })
    }

    fn job_head(&self, job: &Job) -> std::result::Result<String, EngineError> {
        find_job_last_node(self.service.store(), job)?
            .map_or_else(|| Ok(job.base.clone()), |last_node| Ok(last_node.id))
    }

    fn ensure_job_has_exclusive_branch_access(
        &self,
        job: &Job,
    ) -> std::result::Result<(), EngineError> {
        if let Some(active_job) =
            self.active_branch_prompt_job_excluding(&job.work_branch, Some(&job.job_id))?
        {
            tracing::warn!(
                branch = %job.branch,
                work_branch = %job.work_branch,
                active_job_id = %active_job.job_id,
                job_id = %job.job_id,
                "prompt job conflicts with another active job"
            );
            return Err(EngineError::EngineFailed {
                message: format!(
                    "branch {:?} already has conflicting active prompt jobs {:?} and {:?}",
                    job.work_branch, active_job.job_id, job.job_id
                ),
            });
        }
        Ok(())
    }

    fn active_branch_prompt_job_excluding(
        &self,
        branch: &str,
        excluded_job_id: Option<&str>,
    ) -> std::result::Result<Option<Job>, EngineError> {
        Ok(self
            .service
            .store()
            .list_jobs()?
            .into_values()
            .filter(|job| {
                (job.branch == branch || job.work_branch == branch)
                    && !matches!(job.status, JobStatus::Finished)
                    && excluded_job_id != Some(job.job_id.as_str())
            })
            .min_by_key(|job| job.created_at))
    }

    async fn get_or_start_inflight_job(
        &self,
        job_id: &str,
        merge_parents: Vec<MergeParent>,
    ) -> InflightJob {
        let mut inflight_jobs = self.inflight_jobs.lock().await;
        let exists = inflight_jobs.contains_key(job_id);
        if exists {
            tracing::debug!(job_id = %job_id, "reusing inflight prompt job");
        } else {
            tracing::debug!(job_id = %job_id, "starting inflight prompt job");
        }
        inflight_jobs
            .entry(job_id.to_owned())
            .or_insert_with(|| {
                let engine = self.clone();
                let job_id = job_id.to_owned();
                async move { engine.drive_job_once(&job_id, merge_parents).await }
                    .boxed()
                    .shared()
            })
            .clone()
    }
}

fn completion_input_kind(input: &CompletionInput) -> &'static str {
    match input {
        CompletionInput::Continue => "continue",
        CompletionInput::Prompt { .. } => "prompt",
    }
}

fn backend_failure_recovery_event(
    job: &Job,
    source: &BackendError,
    context: &BackendFailureContext,
) -> SystemEventEnvelope<LlmBackendFailureRecoveryRequested> {
    SystemEventEnvelope {
        event_type: LLM_BACKEND_FAILURE_RECOVERY_REQUESTED,
        version: SYSTEM_EVENT_VERSION,
        dedupe_key: format!(
            "llm.backend_failure:{}:{}:{}",
            job.job_id, job.work_branch, context.retry_from_node_id
        ),
        data: LlmBackendFailureRecoveryRequested {
            job_id: job.job_id.clone(),
            root_branch: job.branch.clone(),
            work_branch: job.work_branch.clone(),
            failed_branch: job.work_branch.clone(),
            base_node_id: job.base.clone(),
            execution_id: context.execution_id.clone(),
            error_node_id: context.error_node_id.clone(),
            retry_from_node_id: context.retry_from_node_id.clone(),
            message: source.to_string(),
        },
    }
}

fn find_job_last_node<S>(store: &S, job: &Job) -> std::result::Result<Option<Node>, EngineError>
where
    S: NodeStore,
{
    let path = match store.log(&job.base, &job.work_branch) {
        Ok(path) => path,
        Err(coco_llm::coco_mem::StoreError::RefsNotConnected { .. }) => return Ok(None),
        Err(source) => return Err(source.into()),
    };
    let mut ordered = path.into_iter().rev();
    let prompt_anchor = ordered
        .next()
        .expect("log should include the prompt anchor node");
    let mut last_node = prompt_anchor.clone();
    for node in ordered {
        last_node = node;
    }

    Ok(Some(last_node))
}

fn build_prompt_reply<S>(
    store: &S,
    job: &Job,
    snapshot: &JobStatusSnapshot,
) -> std::result::Result<PromptReply, EngineError>
where
    S: NodeStore,
{
    if !matches!(snapshot.status, JobStatus::Finished) {
        return Err(EngineError::EngineFailed {
            message: format!("prompt job {:?} is waiting for recovery", job.job_id),
        });
    }

    let response_node = store.get_node(&snapshot.head)?;
    let execution_id = response_node
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.first())
        .and_then(|metadata| metadata.execution_id.clone());

    match response_node.kind {
        Kind::Text(text) => Ok(PromptReply {
            execution_id,
            response_node_id: response_node.id,
            branch_head: snapshot.head.clone(),
            text,
        }),
        Kind::Failure(message) => Err(EngineError::EngineFailed { message }),
        _ => Err(EngineError::EngineFailed {
            message: format!(
                "prompt job {:?} finished on non-terminal node {:?}",
                job.job_id, snapshot.head
            ),
        }),
    }
}
