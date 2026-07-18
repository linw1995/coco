UPDATE console_graph_build_runs
SET status = 'abandoned', lease_expires_at_ms = 0
WHERE status IN ('building', 'paused');

DROP INDEX IF EXISTS console_graph_build_nodes_parent_requirements_idx;
ALTER TABLE console_graph_build_nodes DROP COLUMN parent_requirements_complete;
ALTER TABLE console_graph_build_nodes DROP COLUMN parent_requirement_cursor;
