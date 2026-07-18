ALTER TABLE console_graph_build_nodes
    ADD COLUMN parent_requirement_cursor TEXT NOT NULL DEFAULT '';
ALTER TABLE console_graph_build_nodes
    ADD COLUMN parent_requirements_complete INTEGER NOT NULL DEFAULT 0;

CREATE INDEX console_graph_build_nodes_parent_requirements_idx
    ON console_graph_build_nodes(
        run_id,
        parent_requirements_complete,
        processed,
        remaining_parents,
        frontier_enqueued,
        created_at_ns,
        node_id
    );

-- Work created by the previous builder has an aggregate parent count but no
-- durable cursor proving which relations contributed to it.
UPDATE console_graph_build_runs
SET status = 'abandoned', lease_expires_at_ms = 0
WHERE status IN ('building', 'paused');
