DROP TABLE IF EXISTS console_graph_source_orphan_gc_queue;
DROP TABLE IF EXISTS console_graph_source_orphan_gc_state;
DROP INDEX IF EXISTS console_graph_source_refresh_queue_node_idx;
DROP INDEX IF EXISTS console_graph_source_refresh_queue_pending_idx;
DROP INDEX IF EXISTS console_graph_source_child_rechecks_generation_idx;
DROP INDEX IF EXISTS console_graph_source_branch_nodes_generation_idx;
DROP INDEX IF EXISTS console_graph_source_refresh_runs_active_base_idx;
DROP INDEX IF EXISTS console_graph_source_refresh_runs_gc_idx;
DROP INDEX IF EXISTS console_graph_source_refresh_runs_active_branch_idx;

DELETE FROM console_graph_source_child_rechecks
WHERE NOT EXISTS (
    SELECT 1 FROM console_graph_source_branches AS branch
    WHERE branch.name = console_graph_source_child_rechecks.branch_name
      AND branch.contribution_generation =
          console_graph_source_child_rechecks.contribution_generation
);

DELETE FROM console_graph_source_branch_nodes
WHERE NOT EXISTS (
    SELECT 1 FROM console_graph_source_branches AS branch
    WHERE branch.name = console_graph_source_branch_nodes.branch_name
      AND branch.contribution_generation =
          console_graph_source_branch_nodes.contribution_generation
);

DROP TABLE IF EXISTS console_graph_source_refresh_runs;
DROP TABLE IF EXISTS console_graph_source_refresh_queue;

CREATE TABLE console_graph_source_refresh_queue (
    contribution_generation BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    node_id TEXT NOT NULL,
    traversal_kind TEXT NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (contribution_generation, node_id, traversal_kind)
);

CREATE INDEX console_graph_source_refresh_queue_pending_idx
    ON console_graph_source_refresh_queue(contribution_generation, processed, node_id);
