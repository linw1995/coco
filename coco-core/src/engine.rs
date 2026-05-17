use std::{collections::HashMap, path::Path, sync::Arc};

use coco_llm::coco_mem::{
    BranchStore, Job, JobStatus, JobStore, Kind, MemoryStore, MergeParent, Node, NodeStore,
    SessionStore, SkillStore,
};
use coco_llm::{
    CompletionBackend, CompletionInput, CompletionOrigin, CompletionOverrides, CompletionRequest,
    LlmService, RigBackend, SessionConfigPatch,
};
use futures::future::{BoxFuture, FutureExt, Shared};
use jiff::Timestamp;
use serde::Serialize;
use tokio::sync::{Mutex, OwnedMutexGuard, Semaphore};
use tokio::task::JoinSet;

use crate::{BatchPromptResult, BranchPromptOutcome, BranchPromptRequest, EngineError};

type JobResult = std::result::Result<String, EngineError>;
type InflightJob = Shared<BoxFuture<'static, JobResult>>;
type InflightJobTable = Arc<Mutex<HashMap<String, InflightJob>>>;
type BranchLockTable = Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JobStatusSnapshot {
    pub job_id: String,
    pub created_at: Timestamp,
    pub finished_at: Option<Timestamp>,
    pub branch: String,
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

pub struct ConversationEngine<B = RigBackend, S = MemoryStore> {
    service: Arc<LlmService<B, S>>,
    inflight_jobs: InflightJobTable,
    branch_locks: BranchLockTable,
}

impl<B, S> Clone for ConversationEngine<B, S> {
    fn clone(&self) -> Self {
        Self {
            service: self.service.clone(),
            inflight_jobs: self.inflight_jobs.clone(),
            branch_locks: self.branch_locks.clone(),
        }
    }
}

pub struct BranchLockGuard {
    _guard: OwnedMutexGuard<()>,
}

impl ConversationEngine<RigBackend, MemoryStore> {
    pub fn with_service(service: Arc<LlmService<RigBackend, MemoryStore>>) -> Self {
        Self::new(service)
    }
}

impl<B, S> ConversationEngine<B, S> {
    pub fn new(service: Arc<LlmService<B, S>>) -> Self {
        Self {
            service,
            inflight_jobs: Arc::new(Mutex::new(HashMap::new())),
            branch_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn service(&self) -> &Arc<LlmService<B, S>> {
        &self.service
    }

    pub fn runtime_store_path(&self) -> Option<&Path> {
        self.service.runtime_store_path()
    }

    pub async fn lock_branch(&self, branch: &str) -> BranchLockGuard {
        let lock = {
            let mut locks = self.branch_locks.lock().await;
            locks
                .entry(branch.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        BranchLockGuard {
            _guard: lock.lock_owned().await,
        }
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

    pub async fn reply_with_merge_parents(
        &self,
        branch: &str,
        prompt: &str,
        merge_parents: Vec<MergeParent>,
    ) -> std::result::Result<String, EngineError> {
        self.reply_with_session_patch(branch, prompt, merge_parents, None)
            .await
    }

    pub async fn reply_with_session_patch(
        &self,
        branch: &str,
        prompt: &str,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionConfigPatch>,
    ) -> std::result::Result<String, EngineError> {
        Ok(self
            .run_prompt_job(branch, prompt, merge_parents, session_patch)
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
                    .run_prompt_job(&branch, &item.prompt, item.merge_parents, None)
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
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionConfigPatch>,
    ) -> std::result::Result<PromptReply, EngineError> {
        let job = self
            .submit_job_with_session_patch(branch, prompt, merge_parents, session_patch)
            .await?;
        let snapshot = self.drive_job_for_prompt(&job.job_id).await?;
        let job = self.service.store().get_job(&job.job_id)?;
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
        self.submit_job_with_optional_id_and_session_patch(
            None,
            branch,
            prompt,
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
        self.submit_job_with_optional_id_and_session_patch(
            Some(job_id),
            branch,
            prompt,
            merge_parents,
            session_patch,
        )
        .await
    }

    async fn submit_job_with_optional_id_and_session_patch(
        &self,
        job_id: Option<&str>,
        branch: &str,
        prompt: &str,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionConfigPatch>,
    ) -> std::result::Result<Job, EngineError> {
        self.ensure_prompt_job_can_submit(job_id, branch)?;
        let merge_parent_count = merge_parents.len();
        let has_session_patch = session_patch.is_some();
        let base = self.service.append_prompt_job_base(
            branch,
            prompt,
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
        if let Some(job_id) = job_id
            && self.prompt_job_exists(job_id)?
        {
            return Err(EngineError::EngineFailed {
                message: format!("Prompt job {job_id:?} already exists"),
            });
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

    pub fn prompt_job_exists(&self, job_id: &str) -> std::result::Result<bool, EngineError> {
        match self.service.store().get_job(job_id) {
            Ok(_) => Ok(true),
            Err(coco_llm::coco_mem::StoreError::PromptJobNotFound { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
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
            tracing::info!(
                job_id = %job.job_id,
                branch = %job.branch,
                "prompt job started"
            );
        }

        if matches!(job.status, JobStatus::Running) {
            self.ensure_job_has_exclusive_branch_access(&job)?;
            // Prompt jobs must reach a terminal finished state even when execution fails so they
            // remain recoverable and auditable. The caller decides whether to surface the
            // execution error or only inspect the persisted snapshot afterwards.
            let run_result = self.resume_or_run_job(&job, merge_parents).await;
            self.finish_job(&job).await?;
            run_result?;
        }

        let head = self.job_head(&job)?;
        tracing::info!(
            job_id = %job.job_id,
            branch = %job.branch,
            head = %head,
            "prompt job drive completed"
        );
        Ok(head)
    }

    async fn resume_or_run_job(
        &self,
        job: &Job,
        merge_parents: Vec<MergeParent>,
    ) -> std::result::Result<(), EngineError> {
        let store = self.service.store();
        let last_node = find_job_last_node(store, job)?;

        if let Some(last_node) = last_node.as_ref()
            && is_terminal_job_last_node(last_node)
        {
            tracing::info!(
                job_id = %job.job_id,
                branch = %job.branch,
                head = %last_node.id,
                "prompt job already has terminal node"
            );
            return Ok(());
        }

        let origin = CompletionOrigin::Reference(match last_node {
            Some(last_node) => last_node.id,
            None => job.base.clone(),
        });
        let origin_node = match &origin {
            CompletionOrigin::BranchHead => "<branch-head>",
            CompletionOrigin::Reference(node_id) => node_id.as_str(),
        };
        let input = if merge_parents.is_empty() {
            CompletionInput::Continue
        } else {
            CompletionInput::Prompt {
                text: String::new(),
                merge_parents,
                session_patch: None,
            }
        };
        tracing::info!(
            job_id = %job.job_id,
            branch = %job.branch,
            origin = %origin_node,
            input = completion_input_kind(&input),
            "running prompt job completion"
        );
        let request = CompletionRequest {
            branch: job.branch.clone(),
            origin,
            input,
            overrides: CompletionOverrides::default(),
            active_skill_runtime: None,
        };

        let _ = self.service.run(request).await?;
        Ok(())
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
            self.active_branch_prompt_job_excluding(&job.branch, Some(&job.job_id))?
        {
            tracing::warn!(
                branch = %job.branch,
                active_job_id = %active_job.job_id,
                job_id = %job.job_id,
                "prompt job conflicts with another active job"
            );
            return Err(EngineError::EngineFailed {
                message: format!(
                    "branch {:?} already has conflicting active prompt jobs {:?} and {:?}",
                    job.branch, active_job.job_id, job.job_id
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
                job.branch == branch
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

fn find_job_last_node<S>(store: &S, job: &Job) -> std::result::Result<Option<Node>, EngineError>
where
    S: NodeStore,
{
    let path = match store.log(&job.base, &job.branch) {
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

fn is_terminal_job_last_node(node: &Node) -> bool {
    matches!(&node.kind, Kind::Text(_) | Kind::Failure(_))
}

fn build_prompt_reply<S>(
    store: &S,
    job: &Job,
    snapshot: &JobStatusSnapshot,
) -> std::result::Result<PromptReply, EngineError>
where
    S: NodeStore,
{
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
