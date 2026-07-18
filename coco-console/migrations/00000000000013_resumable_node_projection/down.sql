UPDATE console_graph_build_runs
SET status = 'abandoned', lease_expires_at_ms = 0
WHERE status IN ('building', 'paused');

DROP TABLE IF EXISTS console_graph_build_anchor_ancestor_visits;
DROP INDEX IF EXISTS console_graph_build_anchor_raw_edges_pending_idx;
DROP TABLE IF EXISTS console_graph_build_anchor_raw_edges;
DROP TABLE IF EXISTS console_graph_build_anchor_projection_state;
DROP INDEX IF EXISTS console_graph_build_projection_edges_page_idx;
DROP TABLE IF EXISTS console_graph_build_projection_edges;

ALTER TABLE console_graph_build_anchor_edges DROP COLUMN ancestor_depth;

ALTER TABLE console_graph_build_nodes DROP COLUMN projection_required_rank;
ALTER TABLE console_graph_build_nodes DROP COLUMN projection_edge_source_cursor;
ALTER TABLE console_graph_build_nodes DROP COLUMN projection_edge_order_cursor;
ALTER TABLE console_graph_build_nodes DROP COLUMN projection_raw_cursor;
ALTER TABLE console_graph_build_nodes DROP COLUMN projection_phase;
