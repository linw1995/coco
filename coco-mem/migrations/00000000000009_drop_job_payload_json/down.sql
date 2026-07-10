CREATE TABLE jobs_with_payload_json (
    job_id TEXT PRIMARY KEY NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    branch TEXT NOT NULL,
    work_branch TEXT NOT NULL,
    base TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

INSERT INTO jobs_with_payload_json (
    job_id,
    created_at,
    finished_at,
    branch,
    work_branch,
    base,
    status,
    payload_json
)
SELECT
    job_id,
    created_at,
    finished_at,
    branch,
    work_branch,
    base,
    status,
    json_object(
        'job_id', job_id,
        'created_at', created_at,
        'finished_at', finished_at,
        'branch', branch,
        'work_branch', work_branch,
        'base', base,
        'status', status
    )
FROM jobs;

DROP TABLE jobs;
ALTER TABLE jobs_with_payload_json RENAME TO jobs;
