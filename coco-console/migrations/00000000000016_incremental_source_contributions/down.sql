DROP INDEX IF EXISTS console_graph_source_refresh_runs_branch_delta_idx;
DROP INDEX IF EXISTS console_graph_source_refresh_runs_target_delta_idx;
DROP INDEX IF EXISTS console_graph_source_refresh_runs_invalidation_branch_idx;
DROP INDEX IF EXISTS console_graph_source_refresh_queue_pending_idx;
DROP INDEX IF EXISTS console_graph_source_child_rechecks_branch_order_idx;
DROP VIEW IF EXISTS console_graph_build_effective_source_branch_nodes;
DROP VIEW IF EXISTS console_graph_source_published_delta_nodes;
DROP VIEW IF EXISTS console_graph_source_current_child_rechecks;
DROP VIEW IF EXISTS console_graph_source_current_completed_work;
DROP VIEW IF EXISTS console_graph_source_current_branch_nodes;
DROP INDEX IF EXISTS console_graph_build_source_refresh_manifest_refresh_idx;
DROP TABLE IF EXISTS console_graph_build_source_refresh_manifest;
DROP INDEX IF EXISTS console_graph_source_branch_change_journal_branch_idx;
DROP TABLE IF EXISTS console_graph_source_invalidation_receipt_seeds;
DROP INDEX IF EXISTS console_graph_source_invalidation_receipts_token_idx;
DROP TABLE IF EXISTS console_graph_source_invalidation_receipts;
DROP INDEX IF EXISTS console_graph_source_invalidation_boundaries_gc_idx;
DROP INDEX IF EXISTS console_graph_source_invalidation_boundaries_resume_idx;
DROP TABLE IF EXISTS console_graph_source_invalidation_boundaries;
DROP INDEX IF EXISTS console_graph_source_sweep_runs_gc_idx;
DROP INDEX IF EXISTS console_graph_source_sweep_runs_resume_idx;
DROP TABLE IF EXISTS console_graph_source_sweep_runs;
DROP TABLE IF EXISTS console_graph_source_refresh_dirty_seeds;
DROP TABLE IF EXISTS console_graph_source_branch_change_journal;
DROP TABLE IF EXISTS console_graph_source_branch_publications;

-- A target contribution can be assembled from multiple refresh ids. The older
-- schema has no representation for that relationship, so retaining these rows
-- would make branch pointers refer to nonexistent contribution data. This cache
-- is derived; clearing it makes the older implementation rebuild it safely.
DELETE FROM console_graph_source_branches;
DELETE FROM console_graph_source_child_rechecks;
DELETE FROM console_graph_source_branch_nodes;
DELETE FROM console_graph_source_refresh_queue;
DELETE FROM console_graph_source_refresh_runs;

ALTER TABLE console_graph_source_nodes DROP COLUMN relation_ingest_complete;
ALTER TABLE console_graph_source_nodes DROP COLUMN relation_cursor_offset;

ALTER TABLE console_graph_source_refresh_queue DROP COLUMN child_scan_required;
ALTER TABLE console_graph_source_refresh_queue DROP COLUMN child_high_watermark_node_id;
ALTER TABLE console_graph_source_refresh_queue DROP COLUMN child_high_watermark_relation_revision;
ALTER TABLE console_graph_source_refresh_queue DROP COLUMN child_high_watermark_frozen;
ALTER TABLE console_graph_source_refresh_queue DROP COLUMN parent_traversal_complete;
ALTER TABLE console_graph_source_refresh_queue DROP COLUMN parent_cursor_offset;
ALTER TABLE console_graph_source_refresh_queue
    RENAME COLUMN refresh_id TO contribution_generation;

CREATE INDEX console_graph_source_refresh_queue_pending_idx
    ON console_graph_source_refresh_queue(
        contribution_generation,
        processed,
        node_committed,
        node_id,
        traversal_kind
    );

ALTER TABLE console_graph_source_refresh_runs DROP COLUMN lease_epoch;
ALTER TABLE console_graph_source_refresh_runs DROP COLUMN lease_expires_at_ms;
ALTER TABLE console_graph_source_refresh_runs DROP COLUMN target_invalidation_version;
ALTER TABLE console_graph_source_refresh_runs DROP COLUMN target_invalidation_incarnation;
ALTER TABLE console_graph_source_refresh_runs DROP COLUMN target_invalidation_kind;
ALTER TABLE console_graph_source_refresh_runs DROP COLUMN relation_revision;
ALTER TABLE console_graph_source_refresh_runs DROP COLUMN expected_branch_absent;
ALTER TABLE console_graph_source_refresh_runs DROP COLUMN expected_branch_contribution_generation;
ALTER TABLE console_graph_source_refresh_runs DROP COLUMN expected_branch_source_revision;
ALTER TABLE console_graph_source_refresh_runs DROP COLUMN expected_branch_state_json;
ALTER TABLE console_graph_source_refresh_runs DROP COLUMN expected_branch_head_id;
ALTER TABLE console_graph_source_refresh_runs DROP COLUMN owner_id;
ALTER TABLE console_graph_source_refresh_runs DROP COLUMN published_source_revision;
ALTER TABLE console_graph_source_refresh_runs DROP COLUMN refresh_kind;
ALTER TABLE console_graph_source_refresh_runs DROP COLUMN target_contribution_generation;

ALTER TABLE console_graph_source_refresh_runs
    RENAME COLUMN refresh_id TO contribution_generation;

CREATE TRIGGER console_graph_source_branches_identity_insert
AFTER INSERT ON console_graph_source_branches
BEGIN
    UPDATE console_graph_source_identity
    SET revision = revision + 1
    WHERE id = 1;
END;

CREATE TRIGGER console_graph_source_branches_identity_update
AFTER UPDATE ON console_graph_source_branches
BEGIN
    UPDATE console_graph_source_identity
    SET revision = revision + 1
    WHERE id = 1;
END;

CREATE TRIGGER console_graph_source_branches_identity_delete
AFTER DELETE ON console_graph_source_branches
BEGIN
    UPDATE console_graph_source_identity
    SET revision = revision + 1
    WHERE id = 1;
END;
