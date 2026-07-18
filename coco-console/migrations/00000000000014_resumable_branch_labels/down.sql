UPDATE console_graph_build_runs
SET status = 'abandoned', lease_expires_at_ms = 0
WHERE status IN ('building', 'paused');

DROP TABLE IF EXISTS console_graph_build_branch_label_visited;
DROP TABLE IF EXISTS console_graph_build_branch_label_resolutions;
