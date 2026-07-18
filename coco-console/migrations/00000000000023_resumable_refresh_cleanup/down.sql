DROP INDEX IF EXISTS console_graph_materialization_branches_contribution_idx;
DROP INDEX IF EXISTS console_graph_source_dynamic_branch_scans_protection_idx;
DROP INDEX IF EXISTS console_graph_source_branch_change_journal_refresh_idx;
DROP TABLE IF EXISTS console_graph_source_refresh_cleanup_state;

ALTER TABLE console_graph_source_refresh_dirty_seeds
    RENAME TO console_graph_source_refresh_dirty_seeds_v23;

CREATE TABLE console_graph_source_refresh_dirty_seeds (
    refresh_id BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    child_high_watermark_relation_revision BIGINT,
    child_high_watermark_node_id TEXT,
    CHECK (
        (child_high_watermark_relation_revision IS NULL) =
        (child_high_watermark_node_id IS NULL)
    ),
    PRIMARY KEY (refresh_id, node_id),
    FOREIGN KEY (refresh_id)
        REFERENCES console_graph_source_refresh_runs(refresh_id)
        ON DELETE CASCADE
);

INSERT INTO console_graph_source_refresh_dirty_seeds (
    refresh_id,
    node_id,
    child_high_watermark_relation_revision,
    child_high_watermark_node_id
)
SELECT
    dirty.refresh_id,
    dirty.node_id,
    dirty.child_high_watermark_relation_revision,
    dirty.child_high_watermark_node_id
FROM console_graph_source_refresh_dirty_seeds_v23 AS dirty
INNER JOIN console_graph_source_refresh_runs AS refresh
    ON refresh.refresh_id = dirty.refresh_id;

DROP TABLE console_graph_source_refresh_dirty_seeds_v23;
