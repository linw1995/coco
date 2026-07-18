ALTER TABLE console_graph_source_refresh_dirty_seeds
    RENAME TO console_graph_source_refresh_dirty_seeds_v22;

CREATE TABLE console_graph_source_refresh_dirty_seeds (
    refresh_id BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    child_high_watermark_relation_revision BIGINT,
    child_high_watermark_node_id TEXT,
    CHECK (
        (child_high_watermark_relation_revision IS NULL) =
        (child_high_watermark_node_id IS NULL)
    ),
    PRIMARY KEY (refresh_id, node_id)
);

INSERT INTO console_graph_source_refresh_dirty_seeds (
    refresh_id,
    node_id,
    child_high_watermark_relation_revision,
    child_high_watermark_node_id
)
SELECT
    refresh_id,
    node_id,
    child_high_watermark_relation_revision,
    child_high_watermark_node_id
FROM console_graph_source_refresh_dirty_seeds_v22;

DROP TABLE console_graph_source_refresh_dirty_seeds_v22;

CREATE TABLE console_graph_source_refresh_cleanup_state (
    id INTEGER PRIMARY KEY NOT NULL CHECK (id = 1),
    upper_bound_refresh_id BIGINT,
    raw_refresh_id_cursor BIGINT NOT NULL DEFAULT 0
        CHECK (raw_refresh_id_cursor >= 0),
    active_refresh_id BIGINT,
    CHECK (upper_bound_refresh_id IS NULL OR upper_bound_refresh_id >= 0),
    CHECK (active_refresh_id IS NULL OR active_refresh_id >= 0),
    CHECK (
        (upper_bound_refresh_id IS NULL
            AND raw_refresh_id_cursor = 0
            AND active_refresh_id IS NULL)
        OR upper_bound_refresh_id IS NOT NULL
    ),
    CHECK (
        active_refresh_id IS NULL
        OR (
            active_refresh_id <= upper_bound_refresh_id
            AND raw_refresh_id_cursor = active_refresh_id
        )
    )
);

INSERT INTO console_graph_source_refresh_cleanup_state (
    id,
    upper_bound_refresh_id,
    raw_refresh_id_cursor,
    active_refresh_id
) VALUES (1, NULL, 0, NULL);

CREATE INDEX console_graph_source_branch_change_journal_refresh_idx
    ON console_graph_source_branch_change_journal(refresh_id, source_revision)
    WHERE refresh_id IS NOT NULL;

CREATE INDEX console_graph_source_dynamic_branch_scans_protection_idx
    ON console_graph_source_dynamic_branch_scans(
        status,
        scan_kind,
        lease_expires_at_ms,
        raw_refresh_id_upper_bound,
        source_revision
    );

CREATE INDEX console_graph_materialization_branches_contribution_idx
    ON console_graph_materialization_branches(
        contribution_generation,
        generation,
        name
    );
