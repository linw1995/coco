CREATE TABLE jobs_with_head (
    job_id TEXT PRIMARY KEY NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    branch TEXT NOT NULL,
    work_branch TEXT NOT NULL,
    base TEXT NOT NULL,
    head TEXT NOT NULL,
    status TEXT NOT NULL
);

WITH RECURSIVE job_paths(job_id, node_id, base) AS (
    SELECT
        jobs.job_id,
        COALESCE(branches.head_id, jobs.base),
        jobs.base
    FROM jobs
    LEFT JOIN branches ON branches.name = jobs.work_branch

    UNION ALL

    SELECT job_paths.job_id, nodes.parent_id, job_paths.base
    FROM job_paths
    JOIN nodes ON nodes.id = job_paths.node_id
    WHERE job_paths.node_id <> job_paths.base
      AND nodes.parent_id <> ''
),
connected_jobs AS (
    SELECT job_id, MAX(node_id = base) AS connected
    FROM job_paths
    GROUP BY job_id
)
INSERT INTO jobs_with_head (
    job_id,
    created_at,
    finished_at,
    branch,
    work_branch,
    base,
    head,
    status
)
SELECT
    jobs.job_id,
    jobs.created_at,
    jobs.finished_at,
    jobs.branch,
    jobs.work_branch,
    jobs.base,
    CASE
        WHEN jobs.status = 'finished' THEN jobs.base
        WHEN connected_jobs.connected = 1 THEN COALESCE(branches.head_id, jobs.base)
        ELSE jobs.base
    END,
    jobs.status
FROM jobs
LEFT JOIN branches ON branches.name = jobs.work_branch
JOIN connected_jobs ON connected_jobs.job_id = jobs.job_id;

DROP TABLE jobs;
ALTER TABLE jobs_with_head RENAME TO jobs;
