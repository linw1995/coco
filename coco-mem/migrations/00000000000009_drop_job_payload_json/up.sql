CREATE TEMP TABLE job_payload_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO job_payload_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM jobs
        WHERE NOT json_valid(payload_json)
    ) THEN 1
    ELSE 0
END;

DELETE FROM job_payload_migration_guard;

INSERT INTO job_payload_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM jobs
        WHERE json_extract(payload_json, '$.job_id') IS NOT job_id
           OR json_extract(payload_json, '$.created_at') IS NOT created_at
           OR json_extract(payload_json, '$.finished_at') IS NOT finished_at
           OR json_extract(payload_json, '$.branch') IS NOT branch
           OR COALESCE(
                NULLIF(json_extract(payload_json, '$.work_branch'), ''),
                json_extract(payload_json, '$.branch'),
                ''
              ) IS NOT work_branch
           OR json_extract(payload_json, '$.base') IS NOT base
           OR json_extract(payload_json, '$.status') IS NOT status
    ) THEN 1
    ELSE 0
END;

DROP TABLE job_payload_migration_guard;

ALTER TABLE jobs DROP COLUMN payload_json;
