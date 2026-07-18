UPDATE console_graph_build_runs
SET status = 'abandoned', lease_expires_at_ms = 0
WHERE status IN ('building', 'paused');

DROP TABLE IF EXISTS console_graph_build_shell_projections;
DROP INDEX IF EXISTS console_graph_build_runs_resume_idx;
ALTER TABLE console_graph_build_runs DROP COLUMN lease_epoch;
