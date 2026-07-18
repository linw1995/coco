UPDATE console_graph_build_runs
SET status = 'abandoned', lease_expires_at_ms = 0
WHERE status IN ('building', 'paused');

ALTER TABLE console_graph_build_runs DROP COLUMN dag_processed_node_count;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_discovered_node_count;
