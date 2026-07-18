UPDATE console_graph_branch_build_state
SET inflight_head_id = NULL, build_generation = NULL
WHERE build_generation IN (
    SELECT run_id FROM console_graph_build_runs WHERE status <> 'completed'
);

UPDATE console_graph_build_runs
SET status = 'abandoned', lease_expires_at_ms = 0
WHERE status IN ('building', 'paused');

DROP INDEX IF EXISTS console_graph_build_nodes_ready_idx;
DROP INDEX IF EXISTS console_graph_build_parent_satisfactions_child_idx;
DROP TABLE IF EXISTS console_graph_build_parent_satisfactions;
DROP TABLE IF EXISTS console_graph_build_parent_expansions;
DROP INDEX IF EXISTS console_graph_build_scope_queue_pending_idx;
DROP TABLE IF EXISTS console_graph_build_scope_queue;
DROP INDEX IF EXISTS console_graph_build_source_manifest_gc_idx;
DROP INDEX IF EXISTS console_graph_build_source_manifest_contribution_idx;
DROP TABLE IF EXISTS console_graph_build_source_manifest;
DROP TRIGGER IF EXISTS console_graph_source_branches_identity_delete;
DROP TRIGGER IF EXISTS console_graph_source_branches_identity_update;
DROP TRIGGER IF EXISTS console_graph_source_branches_identity_insert;
DROP TABLE IF EXISTS console_graph_source_identity;

ALTER TABLE console_graph_build_nodes DROP COLUMN projection_complete;
ALTER TABLE console_graph_build_nodes DROP COLUMN frontier_enqueued;
ALTER TABLE console_graph_materialization_branches DROP COLUMN contribution_generation;

ALTER TABLE console_graph_build_runs DROP COLUMN dag_finalize_cursor;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_frontier_pending_count;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_finalize_mode;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_finalize_phase;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_init_row_cursor;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_init_text_cursor;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_init_text_cursor_secondary;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_init_counter;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_source_revision;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_scope_count;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_init_phase;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_seed_complete;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_seed_cursor;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_build_kind;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_root_id;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_baseline_generation;
ALTER TABLE console_graph_build_runs DROP COLUMN dag_initialized;
