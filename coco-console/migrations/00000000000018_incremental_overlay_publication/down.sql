-- Overlay generations are derived staging data. Once the overlay descriptor is
-- removed, the previous implementation cannot resume or interpret them.
UPDATE console_graph_build_runs
SET status = 'abandoned', lease_expires_at_ms = 0
WHERE run_id IN (
    SELECT run_id
    FROM console_graph_overlay_runs
    WHERE status IN ('preparing', 'compacting')
);

ALTER TABLE console_graph_generation_state
    DROP COLUMN active_overlay_run_id;

DELETE FROM console_graph_build_publications
WHERE build_run_id IN (
    SELECT run_id FROM console_graph_overlay_runs
    WHERE status IN ('preparing', 'compacting')
);

DELETE FROM console_graph_materialization_time_ticks
WHERE generation IN (
    SELECT run_id FROM console_graph_overlay_runs
    WHERE status IN ('preparing', 'compacting')
);
DELETE FROM console_graph_materialization_shells
WHERE generation IN (
    SELECT run_id FROM console_graph_overlay_runs
    WHERE status IN ('preparing', 'compacting')
);
DELETE FROM console_graph_materializations
WHERE generation IN (
    SELECT run_id FROM console_graph_overlay_runs
    WHERE status IN ('preparing', 'compacting')
);
DELETE FROM console_graph_node_locations
WHERE generation IN (
    SELECT run_id FROM console_graph_overlay_runs
    WHERE status IN ('preparing', 'compacting')
);
DELETE FROM console_graph_edge_routes
WHERE generation IN (
    SELECT run_id FROM console_graph_overlay_runs
    WHERE status IN ('preparing', 'compacting')
);
DELETE FROM console_graph_edge_ports
WHERE generation IN (
    SELECT run_id FROM console_graph_overlay_runs
    WHERE status IN ('preparing', 'compacting')
);
DELETE FROM console_graph_anchor_scopes
WHERE generation IN (
    SELECT run_id FROM console_graph_overlay_runs
    WHERE status IN ('preparing', 'compacting')
);
DELETE FROM console_graph_anchor_scope_manifests
WHERE generation IN (
    SELECT run_id FROM console_graph_overlay_runs
    WHERE status IN ('preparing', 'compacting')
);
DELETE FROM console_graph_materialization_branches
WHERE generation IN (
    SELECT run_id FROM console_graph_overlay_runs
    WHERE status IN ('preparing', 'compacting')
);
DELETE FROM console_graph_generation_source_revisions
WHERE generation IN (
    SELECT run_id FROM console_graph_overlay_runs
    WHERE status IN ('preparing', 'compacting')
);
DELETE FROM console_graph_generation_delta_capabilities
WHERE generation IN (
    SELECT run_id FROM console_graph_overlay_runs
    WHERE status IN ('preparing', 'compacting')
);
DELETE FROM console_graph_branch_label_assignments
WHERE generation IN (
    SELECT run_id FROM console_graph_overlay_runs
    WHERE status IN ('preparing', 'compacting')
);

DROP INDEX console_graph_edge_ports_target_keyset_idx;
DROP INDEX console_graph_edge_ports_source_keyset_idx;
DROP INDEX console_graph_edge_routes_target_keyset_idx;

DROP TABLE console_graph_build_edge_port_tombstones;
DROP TABLE console_graph_build_edge_route_tombstones;

DROP INDEX console_graph_overlay_runs_base_idx;
DROP INDEX console_graph_overlay_runs_resume_idx;
DROP TABLE console_graph_overlay_runs;
