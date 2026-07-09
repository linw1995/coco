use jiff::Timestamp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Finished,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Finished => "finished",
        }
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Queued, Self::Running) | (Self::Running, Self::Finished)
        )
    }
}

/// A persisted execution job bound to a branch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Job {
    pub job_id: String,
    pub created_at: Timestamp,
    #[serde(default)]
    pub finished_at: Option<Timestamp>,
    pub branch: String,
    #[serde(default)]
    pub work_branch: String,
    /// The node where this job starts execution.
    ///
    /// For prompt-based jobs this is the detached prompt anchor. For resume-style
    /// jobs this can be any existing node that should continue execution.
    pub base: String,
    pub status: JobStatus,
}

impl Job {
    pub fn new(
        job_id: impl Into<String>,
        branch: impl Into<String>,
        base: impl Into<String>,
    ) -> Self {
        let branch = branch.into();
        Self {
            job_id: job_id.into(),
            created_at: Timestamp::now(),
            finished_at: None,
            work_branch: branch.clone(),
            branch,
            base: base.into(),
            status: JobStatus::Queued,
        }
    }

    pub fn normalize_work_branch(&mut self) {
        if self.work_branch.is_empty() {
            self.work_branch = self.branch.clone();
        }
    }
}
