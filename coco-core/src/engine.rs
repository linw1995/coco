use std::{collections::HashMap, sync::Arc};

use coco_llm::coco_mem::{
    Anchor, Job, JobStatus, Kind, MemoryStore, NewNode, Node, PromptAnchor, Role, Store,
};
use coco_llm::{
    CompletionBackend, CompletionInput, CompletionOrigin, CompletionOverrides, CompletionRequest,
    LlmService, RigBackend,
};
use futures::future::{BoxFuture, FutureExt, Shared};
use jiff::Timestamp;
use serde::Serialize;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;

use crate::{BatchPromptResult, BranchPromptOutcome, BranchPromptRequest, EngineError};

type JobResult = std::result::Result<String, EngineError>;
type InflightJob = Shared<BoxFuture<'static, JobResult>>;
type InflightJobTable = Arc<Mutex<HashMap<String, InflightJob>>>;

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

pub struct ConversationEngine<B = RigBackend, S = MemoryStore>
where
    S: Store,
{
    service: Arc<LlmService<B, S>>,
    inflight_jobs: InflightJobTable,
}

impl<B, S> Clone for ConversationEngine<B, S>
where
    S: Store,
{
    fn clone(&self) -> Self {
        Self {
            service: self.service.clone(),
            inflight_jobs: self.inflight_jobs.clone(),
        }
    }
}

impl ConversationEngine<RigBackend, MemoryStore> {
    pub fn with_service(service: Arc<LlmService<RigBackend, MemoryStore>>) -> Self {
        Self::new(service)
    }
}

impl<B, S> ConversationEngine<B, S>
where
    B: CompletionBackend + 'static,
    S: Store,
{
    pub fn new(service: Arc<LlmService<B, S>>) -> Self {
        Self {
            service,
            inflight_jobs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn service(&self) -> &Arc<LlmService<B, S>> {
        &self.service
    }

    pub async fn reply(
        &self,
        branch: &str,
        prompt: &str,
    ) -> std::result::Result<String, EngineError> {
        Ok(self.run_prompt_job(branch, prompt, vec![]).await?.text)
    }

    pub async fn reply_many(
        &self,
        items: Vec<BranchPromptRequest>,
        max_concurrency: usize,
    ) -> BatchPromptResult {
        let limit = max_concurrency.max(1);
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
                    .run_prompt_job(&branch, &item.prompt, item.merge_parents)
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
        merge_parents: Vec<String>,
    ) -> std::result::Result<PromptReply, EngineError> {
        let job = self.submit_job(branch, prompt, merge_parents).await?;
        let snapshot = self.drive_job_for_prompt(&job.job_id).await?;
        let job = self.service.store().get_job(&job.job_id)?;
        build_prompt_reply(self.service.store(), &job, &snapshot)
    }

    pub async fn submit_job(
        &self,
        branch: &str,
        prompt: &str,
        merge_parents: Vec<String>,
    ) -> std::result::Result<Job, EngineError> {
        let merge_parent_ids = resolve_reference_ids(self.service.store(), &merge_parents)?;
        let base = self.service.store().get_branch_head(branch)?;
        let base = self.service.store().append(NewNode {
            parent: base,
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::prompt(
                merge_parent_ids,
                PromptAnchor {
                    prompt: prompt.to_owned(),
                },
            )),
        })?;
        Ok(self.service.store().submit_job(branch, &base)?)
    }

    pub fn get_job(&self, job_id: &str) -> std::result::Result<JobStatusSnapshot, EngineError> {
        let job = self.service.store().get_job(job_id)?;
        self.build_job_status_snapshot(&job)
    }

    pub fn list_jobs(
        &self,
    ) -> std::result::Result<std::collections::HashMap<String, Job>, EngineError> {
        Ok(self.service.store().list_jobs()?)
    }

    pub async fn resume_incomplete_jobs(&self) -> std::result::Result<(), EngineError> {
        let jobs = self.list_jobs()?;
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
        let _ = self.drive_job_singleflight(job_id).await;
        self.get_job(job_id)
    }

    async fn drive_job_for_prompt(
        &self,
        job_id: &str,
    ) -> std::result::Result<JobStatusSnapshot, EngineError> {
        self.drive_job_singleflight(job_id).await?;
        self.get_job(job_id)
    }

    async fn drive_job_singleflight(
        &self,
        job_id: &str,
    ) -> std::result::Result<String, EngineError> {
        let job_id = job_id.to_owned();
        let inflight_job = self.get_or_start_inflight_job(&job_id).await;
        let result = inflight_job.clone().await;

        self.inflight_jobs.lock().await.remove(&job_id);

        result
    }

    async fn drive_job_once(&self, job_id: &str) -> std::result::Result<String, EngineError> {
        let mut job = self.service.store().get_job(job_id)?;

        if matches!(job.status, JobStatus::Queued) {
            job = self.service.store().set_job_status(
                job_id,
                JobStatus::Queued,
                JobStatus::Running,
            )?;
        }

        if matches!(job.status, JobStatus::Running) {
            self.ensure_job_has_exclusive_branch_access(&job)?;
            // Prompt jobs must reach a terminal finished state even when execution fails so they
            // remain recoverable and auditable. The caller decides whether to surface the
            // execution error or only inspect the persisted snapshot afterwards.
            let run_result = self.resume_or_run_job(&job).await;
            self.finish_job(&job).await?;
            run_result?;
        }

        self.job_head(&job)
    }

    async fn resume_or_run_job(&self, job: &Job) -> std::result::Result<(), EngineError> {
        let store = self.service.store();
        let last_node = find_job_last_node(store, job)?;

        if let Some(last_node) = last_node.as_ref()
            && is_terminal_job_last_node(last_node)
        {
            return Ok(());
        }

        let request = match last_node {
            Some(last_node) => CompletionRequest {
                branch: job.branch.clone(),
                origin: CompletionOrigin::Reference(last_node.id),
                input: CompletionInput::Continue,
                overrides: CompletionOverrides::default(),
            },
            None => CompletionRequest {
                branch: job.branch.clone(),
                origin: CompletionOrigin::Reference(job.base.clone()),
                input: CompletionInput::Continue,
                overrides: CompletionOverrides::default(),
            },
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
        let jobs = self.service.store().list_jobs()?;
        if let Some(active_job) = jobs.values().find(|other| {
            other.job_id != job.job_id
                && other.branch == job.branch
                && !matches!(other.status, JobStatus::Finished)
        }) {
            return Err(EngineError::EngineFailed {
                message: format!(
                    "branch {:?} already has conflicting active prompt jobs {:?} and {:?}",
                    job.branch, active_job.job_id, job.job_id
                ),
            });
        }
        Ok(())
    }

    async fn get_or_start_inflight_job(&self, job_id: &str) -> InflightJob {
        let mut inflight_jobs = self.inflight_jobs.lock().await;
        inflight_jobs
            .entry(job_id.to_owned())
            .or_insert_with(|| {
                let engine = self.clone();
                let job_id = job_id.to_owned();
                async move { engine.drive_job_once(&job_id).await }
                    .boxed()
                    .shared()
            })
            .clone()
    }
}

fn find_job_last_node<S: Store>(
    store: &S,
    job: &Job,
) -> std::result::Result<Option<Node>, EngineError> {
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

fn build_prompt_reply<S: Store>(
    store: &S,
    job: &Job,
    snapshot: &JobStatusSnapshot,
) -> std::result::Result<PromptReply, EngineError> {
    let response_node = store.get_node(&snapshot.head)?;
    let execution_id = response_node
        .metadata
        .as_ref()
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

fn resolve_reference_ids<S: Store>(
    store: &S,
    references: &[String],
) -> std::result::Result<Vec<String>, EngineError> {
    references
        .iter()
        .map(|reference| {
            Ok(store
                .ancestry(reference)?
                .into_iter()
                .next()
                .expect("ancestry should always include the head node")
                .id)
        })
        .collect()
}
