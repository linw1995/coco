CREATE INDEX console_graph_source_child_rechecks_node_raw_idx
    ON console_graph_source_child_rechecks(
        node_id,
        branch_name,
        contribution_generation,
        traversal_kind
    );

CREATE INDEX console_graph_source_refresh_runs_published_raw_upper_idx
    ON console_graph_source_refresh_runs(
        status,
        refresh_id DESC,
        published_source_revision
    );

ALTER TABLE console_graph_source_mutation_event_runs
    RENAME TO console_graph_source_mutation_event_runs_v21;

CREATE TABLE console_graph_source_mutation_event_runs (
    revision BIGINT PRIMARY KEY NOT NULL CHECK (revision > 0),
    phase TEXT NOT NULL
        CHECK (phase IN ('branch_changes', 'dirty_parents', 'discarding')),
    branch_cursor TEXT,
    dirty_parent_cursor TEXT,
    active_dirty_parent_id TEXT,
    peer_branch_cursor TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (
        (active_dirty_parent_id IS NULL AND peer_branch_cursor IS NULL)
        OR active_dirty_parent_id IS NOT NULL
    ),
    CHECK (
        phase <> 'discarding'
        OR (
            branch_cursor IS NULL
            AND dirty_parent_cursor IS NULL
            AND active_dirty_parent_id IS NULL
            AND peer_branch_cursor IS NULL
        )
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
    phase,
    branch_cursor,
    dirty_parent_cursor,
    active_dirty_parent_id,
    peer_branch_cursor,
    created_at,
    updated_at
FROM console_graph_source_mutation_event_runs_v21;

DROP TABLE console_graph_source_mutation_event_runs_v21;

CREATE INDEX console_graph_source_mutation_event_runs_cleanup_idx
    ON console_graph_source_mutation_event_runs(phase, revision);

CREATE TABLE console_graph_source_dynamic_branch_scans (
    scan_id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_kind TEXT NOT NULL CHECK (scan_kind IN ('dirty_parent', 'affected')),
    request_key TEXT NOT NULL,
    mutation_revision BIGINT,
    source_revision BIGINT NOT NULL CHECK (source_revision >= 0),
    raw_refresh_id_upper_bound BIGINT NOT NULL
        CHECK (raw_refresh_id_upper_bound >= -1),
    dirty_node_id TEXT,
    targeted_limit BIGINT CHECK (targeted_limit IS NULL OR targeted_limit >= 0),
    status TEXT NOT NULL CHECK (status IN ('building', 'completed', 'discarding')),
    origin_branch_cursor TEXT,
    active_origin_branch_name TEXT,
    origin_raw_node_cursor TEXT,
    origin_raw_traversal_cursor TEXT,
    origin_raw_refresh_id_cursor BIGINT,
    completed_origin_node_id TEXT,
    active_origin_node_id TEXT,
    candidate_raw_branch_cursor TEXT,
    candidate_raw_refresh_id_cursor BIGINT,
    candidate_raw_traversal_cursor TEXT,
    result_count BIGINT NOT NULL DEFAULT 0 CHECK (result_count >= 0),
    exceeded_limit INTEGER NOT NULL DEFAULT 0 CHECK (exceeded_limit IN (0, 1)),
    owner_id TEXT NOT NULL DEFAULT '',
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_expires_at_ms BIGINT NOT NULL DEFAULT 0 CHECK (lease_expires_at_ms >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (scan_kind, request_key),
    FOREIGN KEY (mutation_revision)
        REFERENCES console_graph_source_mutation_event_runs(revision),
    CHECK (
        (origin_raw_node_cursor IS NULL) =
            (origin_raw_traversal_cursor IS NULL) AND
        (origin_raw_node_cursor IS NULL) =
            (origin_raw_refresh_id_cursor IS NULL)
    ),
    CHECK (
        (candidate_raw_branch_cursor IS NULL) =
            (candidate_raw_refresh_id_cursor IS NULL) AND
        (candidate_raw_branch_cursor IS NULL) =
            (candidate_raw_traversal_cursor IS NULL)
    ),
    CHECK (active_origin_node_id IS NULL OR active_origin_branch_name IS NOT NULL),
    CHECK (
        (scan_kind = 'dirty_parent'
            AND mutation_revision IS NOT NULL
            AND dirty_node_id IS NOT NULL
            AND targeted_limit IS NULL)
        OR
        (scan_kind = 'affected'
            AND mutation_revision IS NULL
            AND dirty_node_id IS NULL
            AND targeted_limit IS NOT NULL)
    )
);

CREATE INDEX console_graph_source_dynamic_branch_scans_mutation_idx
    ON console_graph_source_dynamic_branch_scans(
        scan_kind,
        mutation_revision,
        scan_id
    );

CREATE INDEX console_graph_source_dynamic_branch_scans_resume_idx
    ON console_graph_source_dynamic_branch_scans(
        status,
        lease_expires_at_ms,
        scan_id
    );

CREATE INDEX console_graph_source_dynamic_branch_scans_retention_idx
    ON console_graph_source_dynamic_branch_scans(
        status,
        scan_kind,
        source_revision
    );

CREATE INDEX console_graph_source_dynamic_branch_scans_active_retention_idx
    ON console_graph_source_dynamic_branch_scans(
        status,
        scan_kind,
        lease_expires_at_ms,
        source_revision
    );

CREATE TABLE console_graph_source_dynamic_branch_scan_origins (
    scan_id BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    PRIMARY KEY (scan_id, branch_name),
    FOREIGN KEY (scan_id)
        REFERENCES console_graph_source_dynamic_branch_scans(scan_id)
        ON DELETE CASCADE
);

CREATE TABLE console_graph_source_dynamic_branch_scan_results (
    scan_id BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    PRIMARY KEY (scan_id, branch_name),
    FOREIGN KEY (scan_id)
        REFERENCES console_graph_source_dynamic_branch_scans(scan_id)
        ON DELETE CASCADE
);
