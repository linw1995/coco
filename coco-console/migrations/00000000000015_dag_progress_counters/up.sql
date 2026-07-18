ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_discovered_node_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_processed_node_count BIGINT NOT NULL DEFAULT 0;

-- Existing resumable runs predate the counters and cannot distinguish an
-- untouched run from one with already discovered or completed nodes.
UPDATE console_graph_build_runs
SET status = 'abandoned', lease_expires_at_ms = 0
WHERE status IN ('building', 'paused');
