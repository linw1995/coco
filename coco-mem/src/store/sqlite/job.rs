use std::collections::HashMap;
use std::path::Path;

use async_trait::async_trait;
use diesel::prelude::*;
use diesel::result::OptionalExtension;
use diesel_async::RunQueryDsl;
use snafu::prelude::*;

use super::branch::{load_branch_head, load_session_chain};
use super::graph_mutation::{begin_graph_mutation, dirty_parent_ids, finish_graph_mutation};
use super::node::{load_node_by_exact_id, persist_node_without_transaction, validate_new_node};
use super::{AsyncSqliteConnection, SqliteStore, SqliteTransactionError};
use crate::error::{
    CorruptedStoreSnafu, PromptJobActiveOnBranchSnafu, PromptJobAlreadyExistsSnafu,
    PromptJobInvalidStatusTransitionSnafu, PromptJobMovedSnafu, PromptJobNotFoundSnafu,
    QuerySqliteStoreSnafu,
};
use crate::schema::jobs;
use crate::store::{GraphMutationReceipt, JobStore, inserted_parent_ids};
use crate::{
    Anchor, Job, JobStatus, Kind, MergeParent, Node, PromptAnchor, Role, SessionAnchorPatch,
    StoreResult as Result,
};

#[derive(Queryable)]
pub struct JobRow {
    pub job_id: String,
    pub created_at: String,
    pub finished_at: Option<String>,
    pub branch: String,
    pub work_branch: String,
    pub base: String,
    pub status: String,
}

async fn load_job_map(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<HashMap<String, Job>> {
    let rows = jobs::table
        .select((
            jobs::job_id,
            jobs::created_at,
            jobs::finished_at,
            jobs::branch,
            jobs::work_branch,
            jobs::base,
            jobs::status,
        ))
        .order(jobs::job_id)
        .load::<JobRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    rows.into_iter()
        .map(|row| {
            let job = job_row_into_job(path, row)?;
            Ok((job.job_id.clone(), job))
        })
        .collect()
}

async fn load_job(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    job_id: &str,
) -> Result<Job> {
    let row = jobs::table
        .filter(jobs::job_id.eq(job_id))
        .select((
            jobs::job_id,
            jobs::created_at,
            jobs::finished_at,
            jobs::branch,
            jobs::work_branch,
            jobs::base,
            jobs::status,
        ))
        .get_result::<JobRow>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?
        .context(PromptJobNotFoundSnafu {
            job_id: job_id.to_owned(),
        })?;
    job_row_into_job(path, row)
}

pub fn job_row_into_job(path: &Path, row: JobRow) -> Result<Job> {
    ensure!(
        !row.work_branch.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite job {:?} has an empty work branch", row.job_id),
        }
    );
    let created_at = parse_job_timestamp(path, "jobs.created_at", &row.created_at)?;
    let finished_at = row
        .finished_at
        .as_deref()
        .map(|value| parse_job_timestamp(path, "jobs.finished_at", value))
        .transpose()?;
    let status = parse_job_status(path, &row.status)?;

    Ok(Job {
        job_id: row.job_id,
        created_at,
        finished_at,
        branch: row.branch,
        work_branch: row.work_branch,
        base: row.base,
        status,
    })
}

fn parse_job_timestamp(path: &Path, column: &str, value: &str) -> Result<jiff::Timestamp> {
    value
        .parse()
        .map_err(|source| crate::StoreError::CorruptedStore {
            path: path.to_owned(),
            message: format!("invalid SQLite job timestamp in {column}: {source}"),
        })
}

fn parse_job_status(path: &Path, status: &str) -> Result<JobStatus> {
    match status {
        "queued" => Ok(JobStatus::Queued),
        "running" => Ok(JobStatus::Running),
        "finished" => Ok(JobStatus::Finished),
        _ => CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("invalid SQLite job status {status:?}"),
        }
        .fail(),
    }
}

async fn persist_job(connection: &mut AsyncSqliteConnection, path: &Path, job: &Job) -> Result<()> {
    let mut summary = job.clone();
    summary.normalize_work_branch();
    let finished_at = summary
        .finished_at
        .as_ref()
        .map(std::string::ToString::to_string);
    diesel::insert_into(jobs::table)
        .values((
            jobs::job_id.eq(&job.job_id),
            jobs::created_at.eq(summary.created_at.to_string()),
            jobs::finished_at.eq(finished_at),
            jobs::branch.eq(&summary.branch),
            jobs::work_branch.eq(&summary.work_branch),
            jobs::base.eq(&summary.base),
            jobs::status.eq(summary.status.as_str()),
        ))
        .on_conflict(jobs::job_id)
        .do_update()
        .set((
            jobs::created_at.eq(diesel::upsert::excluded(jobs::created_at)),
            jobs::finished_at.eq(diesel::upsert::excluded(jobs::finished_at)),
            jobs::branch.eq(diesel::upsert::excluded(jobs::branch)),
            jobs::work_branch.eq(diesel::upsert::excluded(jobs::work_branch)),
            jobs::base.eq(diesel::upsert::excluded(jobs::base)),
            jobs::status.eq(diesel::upsert::excluded(jobs::status)),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn submit_job_with_id_in_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    job_id: &str,
    branch: &str,
    base: &str,
) -> std::result::Result<Job, SqliteTransactionError> {
    load_branch_head(connection, path, branch)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    load_node_by_exact_id(connection, path, base)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    if jobs::table
        .filter(jobs::job_id.eq(job_id))
        .count()
        .get_result::<i64>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
        .map_err(SqliteTransactionError::Operation)?
        != 0
    {
        return Err(SqliteTransactionError::Operation(
            PromptJobAlreadyExistsSnafu {
                job_id: job_id.to_owned(),
            }
            .build(),
        ));
    }
    if let Some(active_job) = load_job_map(connection, path)
        .await
        .map_err(SqliteTransactionError::Operation)?
        .values()
        .find(|job| job_uses_active_branch(job, branch))
    {
        return Err(SqliteTransactionError::Operation(
            PromptJobActiveOnBranchSnafu {
                branch: branch.to_owned(),
                job_id: active_job.job_id.clone(),
            }
            .build(),
        ));
    }
    let job = Job::new(job_id, branch, base);
    persist_job(connection, path, &job)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    Ok(job)
}

async fn append_prompt_job_base_in_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    prompt: PromptAnchor,
    merge_parents: Vec<MergeParent>,
    session_patch: Option<SessionAnchorPatch>,
    graph_mutation_revision: i64,
) -> std::result::Result<(String, Vec<Node>), SqliteTransactionError> {
    let parent_id = load_branch_head(connection, path, branch)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    let mut nodes = Vec::with_capacity(2);
    let prompt_parent_id = if let Some(patch) = session_patch {
        load_session_chain(connection, path, &parent_id)
            .await
            .map_err(SqliteTransactionError::Operation)?;
        let node = Node::new(
            parent_id,
            Role::System,
            None,
            Kind::Anchor(Anchor::session_patch(vec![], patch)),
            jiff::Timestamp::now(),
        );
        validate_new_node(connection, path, &node)
            .await
            .map_err(SqliteTransactionError::Operation)?;
        persist_node_without_transaction(connection, path, &node, graph_mutation_revision)
            .await
            .map_err(SqliteTransactionError::Operation)?;
        let node_id = node.id.clone();
        nodes.push(node);
        node_id
    } else {
        parent_id
    };
    let normalized_parents = normalize_prompt_merge_parents(&prompt_parent_id, merge_parents);
    let node = Node::new(
        prompt_parent_id,
        Role::System,
        None,
        Kind::Anchor(Anchor::prompt(normalized_parents, prompt)),
        jiff::Timestamp::now(),
    );
    validate_new_node(connection, path, &node)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    persist_node_without_transaction(connection, path, &node, graph_mutation_revision)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    let node_id = node.id.clone();
    nodes.push(node);
    Ok((node_id, nodes))
}

fn normalize_prompt_merge_parents(
    parent_id: &str,
    merge_parents: Vec<MergeParent>,
) -> Vec<MergeParent> {
    let mut normalized_parents = Vec::new();
    for merge_parent in merge_parents {
        let node_id = merge_parent.node_id();
        if node_id != parent_id
            && !normalized_parents
                .iter()
                .any(|parent: &MergeParent| parent.node_id() == node_id)
        {
            normalized_parents.push(merge_parent);
        }
    }
    normalized_parents
}

fn job_uses_active_branch(job: &Job, branch: &str) -> bool {
    if matches!(job.status, JobStatus::Finished) {
        return false;
    }
    let work_branch = if job.work_branch.is_empty() {
        job.branch.as_str()
    } else {
        job.work_branch.as_str()
    };
    job.branch == branch || work_branch == branch
}

#[async_trait]
impl JobStore for SqliteStore {
    async fn submit_job(&self, branch: &str, base: &str) -> Result<Job> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<Job, SqliteTransactionError, _>(async |connection| {
                loop {
                    let job_id = format!("job-{}", nanoid::nanoid!());
                    if jobs::table
                        .filter(jobs::job_id.eq(&job_id))
                        .count()
                        .get_result::<i64>(connection)
                        .await
                        .context(QuerySqliteStoreSnafu {
                            path: self.database_path.clone(),
                        })
                        .map_err(SqliteTransactionError::Operation)?
                        == 0
                    {
                        return submit_job_with_id_in_transaction(
                            connection,
                            &self.database_path,
                            &job_id,
                            branch,
                            base,
                        )
                        .await;
                    }
                }
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }

    async fn submit_job_with_prompt_base(
        &self,
        branch: &str,
        prompt: PromptAnchor,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionAnchorPatch>,
    ) -> Result<Job> {
        self.submit_job_with_prompt_base_and_graph_mutation(
            branch,
            prompt,
            merge_parents,
            session_patch,
        )
        .await
        .map(|receipt| receipt.value)
    }

    async fn submit_job_with_prompt_base_and_graph_mutation(
        &self,
        branch: &str,
        prompt: PromptAnchor,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionAnchorPatch>,
    ) -> Result<GraphMutationReceipt<Job>> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        let (job, nodes) = connection
            .immediate_transaction::<(Job, Vec<Node>), SqliteTransactionError, _>(
                async |connection| {
                    let job_id = loop {
                        let job_id = format!("job-{}", nanoid::nanoid!());
                        if jobs::table
                            .filter(jobs::job_id.eq(&job_id))
                            .count()
                            .get_result::<i64>(connection)
                            .await
                            .context(QuerySqliteStoreSnafu {
                                path: self.database_path.clone(),
                            })
                            .map_err(SqliteTransactionError::Operation)?
                            == 0
                        {
                            break job_id;
                        }
                    };
                    let revision = begin_graph_mutation(connection, &self.database_path)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    let (base, nodes) = append_prompt_job_base_in_transaction(
                        connection,
                        &self.database_path,
                        branch,
                        prompt,
                        merge_parents,
                        session_patch,
                        revision,
                    )
                    .await?;
                    let job = submit_job_with_id_in_transaction(
                        connection,
                        &self.database_path,
                        &job_id,
                        branch,
                        &base,
                    )
                    .await?;
                    finish_graph_mutation(
                        connection,
                        &self.database_path,
                        revision,
                        &dirty_parent_ids(&nodes),
                        &[],
                    )
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                    Ok((job, nodes))
                },
            )
            .await
            .map_err(|error| error.into_store_error(&self.database_path))?;
        let inserted_parent_ids = inserted_parent_ids(
            nodes.iter().map(|node| node.parent.as_str()),
            nodes.iter().map(|node| &node.kind),
        );
        Ok(GraphMutationReceipt {
            value: job,
            branch_changes: Vec::new(),
            inserted_parent_ids,
            exact: true,
        })
    }

    async fn submit_job_with_id(&self, job_id: &str, branch: &str, base: &str) -> Result<Job> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<Job, SqliteTransactionError, _>(async |connection| {
                submit_job_with_id_in_transaction(
                    connection,
                    &self.database_path,
                    job_id,
                    branch,
                    base,
                )
                .await
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }

    async fn submit_job_with_id_and_prompt_base(
        &self,
        job_id: &str,
        branch: &str,
        prompt: PromptAnchor,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionAnchorPatch>,
    ) -> Result<Job> {
        self.submit_job_with_id_and_prompt_base_and_graph_mutation(
            job_id,
            branch,
            prompt,
            merge_parents,
            session_patch,
        )
        .await
        .map(|receipt| receipt.value)
    }

    async fn submit_job_with_id_and_prompt_base_and_graph_mutation(
        &self,
        job_id: &str,
        branch: &str,
        prompt: PromptAnchor,
        merge_parents: Vec<MergeParent>,
        session_patch: Option<SessionAnchorPatch>,
    ) -> Result<GraphMutationReceipt<Job>> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        let (job, nodes) = connection
            .immediate_transaction::<(Job, Vec<Node>), SqliteTransactionError, _>(
                async |connection| {
                    let revision = begin_graph_mutation(connection, &self.database_path)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    let (base, nodes) = append_prompt_job_base_in_transaction(
                        connection,
                        &self.database_path,
                        branch,
                        prompt,
                        merge_parents,
                        session_patch,
                        revision,
                    )
                    .await?;
                    let job = submit_job_with_id_in_transaction(
                        connection,
                        &self.database_path,
                        job_id,
                        branch,
                        &base,
                    )
                    .await?;
                    finish_graph_mutation(
                        connection,
                        &self.database_path,
                        revision,
                        &dirty_parent_ids(&nodes),
                        &[],
                    )
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                    Ok((job, nodes))
                },
            )
            .await
            .map_err(|error| error.into_store_error(&self.database_path))?;
        let inserted_parent_ids = inserted_parent_ids(
            nodes.iter().map(|node| node.parent.as_str()),
            nodes.iter().map(|node| &node.kind),
        );
        Ok(GraphMutationReceipt {
            value: job,
            branch_changes: Vec::new(),
            inserted_parent_ids,
            exact: true,
        })
    }

    async fn get_job(&self, job_id: &str) -> Result<Job> {
        let mut connection = self.connect().await?;
        load_job(&mut connection, &self.database_path, job_id).await
    }

    async fn list_jobs(&self) -> Result<std::collections::HashMap<String, Job>> {
        let mut connection = self.connect().await?;
        load_job_map(&mut connection, &self.database_path).await
    }

    async fn set_job_status(
        &self,
        job_id: &str,
        expected: JobStatus,
        next: JobStatus,
    ) -> Result<Job> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<Job, SqliteTransactionError, _>(async |connection| {
                let mut job = load_job(connection, &self.database_path, job_id)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                if job.status != expected {
                    return Err(SqliteTransactionError::Operation(
                        PromptJobMovedSnafu {
                            job_id: job_id.to_owned(),
                            expected: format!("{expected:?}"),
                            actual: format!("{:?}", job.status),
                        }
                        .build(),
                    ));
                }
                if !job.status.can_transition_to(next) {
                    return Err(SqliteTransactionError::Operation(
                        PromptJobInvalidStatusTransitionSnafu {
                            job_id: job_id.to_owned(),
                            current: format!("{:?}", job.status),
                            next: format!("{next:?}"),
                        }
                        .build(),
                    ));
                }
                job.status = next;
                job.finished_at = match next {
                    JobStatus::Finished => Some(jiff::Timestamp::now()),
                    _ => None,
                };
                persist_job(connection, &self.database_path, &job)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                Ok(job)
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }

    async fn set_job_work_branch(
        &self,
        job_id: &str,
        expected_work_branch: &str,
        next_work_branch: &str,
    ) -> Result<Job> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<Job, SqliteTransactionError, _>(async |connection| {
                load_branch_head(connection, &self.database_path, next_work_branch)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                let jobs = load_job_map(connection, &self.database_path)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                if let Some(active_job) = jobs.values().find(|job| {
                    job.job_id != job_id && job_uses_active_branch(job, next_work_branch)
                }) {
                    return Err(SqliteTransactionError::Operation(
                        PromptJobActiveOnBranchSnafu {
                            branch: next_work_branch.to_owned(),
                            job_id: active_job.job_id.clone(),
                        }
                        .build(),
                    ));
                }
                let mut job = jobs.get(job_id).cloned().ok_or_else(|| {
                    SqliteTransactionError::Operation(
                        PromptJobNotFoundSnafu {
                            job_id: job_id.to_owned(),
                        }
                        .build(),
                    )
                })?;
                job.normalize_work_branch();
                if matches!(job.status, JobStatus::Finished) {
                    return Err(SqliteTransactionError::Operation(
                        PromptJobInvalidStatusTransitionSnafu {
                            job_id: job_id.to_owned(),
                            current: format!("{:?}", job.status),
                            next: "work_branch_changed".to_owned(),
                        }
                        .build(),
                    ));
                }
                if job.work_branch != expected_work_branch {
                    return Err(SqliteTransactionError::Operation(
                        PromptJobMovedSnafu {
                            job_id: job_id.to_owned(),
                            expected: expected_work_branch.to_owned(),
                            actual: job.work_branch.clone(),
                        }
                        .build(),
                    ));
                }
                job.work_branch = next_work_branch.to_owned();
                persist_job(connection, &self.database_path, &job)
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                Ok(job)
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }
}
