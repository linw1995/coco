DROP TABLE IF EXISTS console_graph_source_dynamic_branch_scan_results;
DROP TABLE IF EXISTS console_graph_source_dynamic_branch_scan_origins;
DROP INDEX IF EXISTS console_graph_source_dynamic_branch_scans_mutation_idx;
DROP INDEX IF EXISTS console_graph_source_dynamic_branch_scans_active_retention_idx;
DROP INDEX IF EXISTS console_graph_source_dynamic_branch_scans_retention_idx;
DROP INDEX IF EXISTS console_graph_source_dynamic_branch_scans_resume_idx;
DROP TABLE IF EXISTS console_graph_source_dynamic_branch_scans;

ALTER TABLE console_graph_source_mutation_event_runs
    RENAME TO console_graph_source_mutation_event_runs_v22;

CREATE TABLE console_graph_source_mutation_event_runs (
    revision BIGINT PRIMARY KEY NOT NULL CHECK (revision > 0),
    phase TEXT NOT NULL CHECK (phase IN ('branch_changes', 'dirty_parents')),
    branch_cursor TEXT,
    dirty_parent_cursor TEXT,
    active_dirty_parent_id TEXT,
    peer_branch_cursor TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (
        (active_dirty_parent_id IS NULL AND peer_branch_cursor IS NULL)
        OR active_dirty_parent_id IS NOT NULL
    )
);

INSERT INTO console_graph_source_mutation_event_runs (
    revision,
    phase,
    branch_cursor,
    dirty_parent_cursor,
    active_dirty_parent_id,
    peer_branch_cursor,
    created_at,
    updated_at
)
SELECT
    revision,
    CASE WHEN phase = 'discarding' THEN 'dirty_parents' ELSE phase END,
    branch_cursor,
    dirty_parent_cursor,
    active_dirty_parent_id,
    peer_branch_cursor,
    created_at,
    updated_at
FROM console_graph_source_mutation_event_runs_v22;

DROP TABLE console_graph_source_mutation_event_runs_v22;

DROP INDEX IF EXISTS console_graph_source_refresh_runs_published_raw_upper_idx;
DROP INDEX IF EXISTS console_graph_source_child_rechecks_node_raw_idx;
