CREATE TABLE jobs_without_head (
    job_id TEXT PRIMARY KEY NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    branch TEXT NOT NULL,
    work_branch TEXT NOT NULL,
    base TEXT NOT NULL,
    status TEXT NOT NULL
);

INSERT INTO jobs_without_head (
    job_id,
    created_at,
    finished_at,
    branch,
    work_branch,
    base,
    status
)
SELECT
    job_id,
    created_at,
    finished_at,
    branch,
    work_branch,
    base,
    status
FROM jobs;

DROP TABLE jobs;
ALTER TABLE jobs_without_head RENAME TO jobs;
