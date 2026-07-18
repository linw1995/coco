CREATE TABLE console_graph_source_refresh_runs (
    contribution_generation BIGINT PRIMARY KEY NOT NULL,
    branch_name TEXT NOT NULL,
    target_head_id TEXT NOT NULL,
    target_state_json TEXT NOT NULL,
    base_generation BIGINT,
    status TEXT NOT NULL CHECK (status IN ('building', 'published', 'superseded')),
    copied_nodes INTEGER NOT NULL DEFAULT 0,
    copied_work INTEGER NOT NULL DEFAULT 0,
    copied_rechecks INTEGER NOT NULL DEFAULT 0,
    node_copy_cursor TEXT,
    work_copy_node_cursor TEXT,
    work_copy_traversal_cursor TEXT,
    recheck_copy_node_cursor TEXT,
    recheck_copy_traversal_cursor TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE UNIQUE INDEX console_graph_source_refresh_runs_active_branch_idx
    ON console_graph_source_refresh_runs(branch_name)
    WHERE status = 'building';

CREATE INDEX console_graph_source_refresh_runs_gc_idx
    ON console_graph_source_refresh_runs(status, contribution_generation);

CREATE INDEX console_graph_source_refresh_runs_active_base_idx
    ON console_graph_source_refresh_runs(base_generation)
    WHERE status = 'building' AND base_generation IS NOT NULL;

CREATE INDEX console_graph_source_branch_nodes_generation_idx
    ON console_graph_source_branch_nodes(
        contribution_generation,
        branch_name,
        node_id
    );

CREATE INDEX console_graph_source_child_rechecks_generation_idx
    ON console_graph_source_child_rechecks(
        contribution_generation,
        branch_name,
        node_id,
        traversal_kind
    );

ALTER TABLE console_graph_source_refresh_queue
    ADD COLUMN node_committed INTEGER NOT NULL DEFAULT 0;

ALTER TABLE console_graph_source_refresh_queue
    ADD COLUMN force_child_scan INTEGER NOT NULL DEFAULT 0;

ALTER TABLE console_graph_source_refresh_queue
    ADD COLUMN child_cursor_relation_revision BIGINT;

ALTER TABLE console_graph_source_refresh_queue
    ADD COLUMN child_cursor_node_id TEXT;

DROP INDEX console_graph_source_refresh_queue_pending_idx;

CREATE INDEX console_graph_source_refresh_queue_pending_idx
    ON console_graph_source_refresh_queue(
        contribution_generation,
        processed,
        node_committed,
        node_id,
        traversal_kind
    );

CREATE INDEX console_graph_source_refresh_queue_node_idx
    ON console_graph_source_refresh_queue(
        node_id,
        contribution_generation,
        branch_name,
        traversal_kind
    );

CREATE TABLE console_graph_source_orphan_gc_state (
    id INTEGER PRIMARY KEY NOT NULL CHECK (id = 1),
    scan_cursor TEXT,
    scan_upper_bound TEXT
);

INSERT INTO console_graph_source_orphan_gc_state (
    id,
    scan_cursor,
    scan_upper_bound
) VALUES (1, NULL, NULL);

CREATE TABLE console_graph_source_orphan_gc_queue (
    node_id TEXT PRIMARY KEY NOT NULL,
    FOREIGN KEY (node_id)
        REFERENCES console_graph_source_nodes(node_id)
        ON DELETE CASCADE
);

-- Published queue rows are now the durable traversal-completion index. Legacy
-- generations did not retain those rows, so detach their branch pointers and let
-- the bounded stale-refresh collector reclaim their large work tables while a
-- new contribution is traversed from source.
INSERT OR IGNORE INTO console_graph_source_refresh_runs (
    contribution_generation,
    branch_name,
    target_head_id,
    target_state_json,
    status
)
SELECT
    contribution_generation,
    name,
    head_id,
    state_json,
    'superseded'
FROM console_graph_source_branches;

DELETE FROM console_graph_source_branches;
