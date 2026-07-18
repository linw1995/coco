UPDATE console_graph_build_runs
SET status = 'abandoned', lease_expires_at_ms = 0
WHERE status IN ('building', 'paused');

ALTER TABLE console_graph_build_runs
    DROP COLUMN dag_build_classification_reason;
ALTER TABLE console_graph_build_runs
    DROP COLUMN dag_baseline_publication_epoch;
ALTER TABLE console_graph_build_runs
    DROP COLUMN dag_baseline_source_revision;
ALTER TABLE console_graph_build_runs
    DROP COLUMN dag_published_graph_generation;

DROP TABLE console_graph_build_shell_tick_candidates;

DROP TRIGGER console_graph_edge_ports_invalidate_delta_capability_on_update;
DROP TRIGGER console_graph_edge_ports_invalidate_delta_capability_on_delete;

ALTER TABLE console_graph_materialization_time_ticks
    DROP COLUMN node_id;

ALTER TABLE console_graph_build_shell_projections
    DROP COLUMN baseline_cursor_key;
ALTER TABLE console_graph_build_shell_projections
    DROP COLUMN baseline_endpoint;
ALTER TABLE console_graph_build_shell_projections
    DROP COLUMN baseline_cursor_node_id;
ALTER TABLE console_graph_build_shell_projections
    DROP COLUMN baseline_work_kind;
ALTER TABLE console_graph_build_shell_projections
    DROP COLUMN baseline_phase;
ALTER TABLE console_graph_build_shell_projections
    DROP COLUMN baseline_initialized;
ALTER TABLE console_graph_build_shell_projections
    DROP COLUMN baseline_generation;
ALTER TABLE console_graph_build_shell_projections
    DROP COLUMN build_kind;

DROP TABLE console_graph_build_publications;
DROP TABLE console_graph_build_label_affected_nodes;
DROP TABLE console_graph_branch_label_assignments;
DROP INDEX console_graph_edge_routes_source_idx;
DROP INDEX console_graph_edge_routes_extent_y_idx;
DROP INDEX console_graph_edge_routes_extent_x_idx;
DROP INDEX console_graph_node_locations_extent_y_idx;
DROP INDEX console_graph_node_locations_extent_x_idx;
DROP TABLE console_graph_build_anchor_node_tombstones;
DROP TABLE console_graph_build_scope_tombstones;
DROP TABLE console_graph_build_node_tombstones;
DROP TABLE console_graph_build_delta_nodes;
DROP TABLE console_graph_build_branch_deltas;
DROP TABLE console_graph_build_changed_branches;
DROP TABLE console_graph_generation_delta_capabilities;
