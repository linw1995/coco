CREATE TABLE console_graph_build_runs (
    run_id BIGINT PRIMARY KEY NOT NULL,
    source_version BIGINT NOT NULL,
    status TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    lease_expires_at_ms BIGINT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    completed_at TEXT
);

CREATE INDEX console_graph_build_runs_lease_idx
    ON console_graph_build_runs(status, lease_expires_at_ms, run_id);

CREATE TABLE console_graph_build_nodes (
    run_id BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    created_at_ns BIGINT NOT NULL,
    remaining_parents INTEGER NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0,
    anchor_ancestor_id TEXT,
    PRIMARY KEY (run_id, node_id),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_build_nodes_pending_idx
    ON console_graph_build_nodes(run_id, processed, remaining_parents, node_id);

CREATE TABLE console_graph_build_frontier (
    run_id BIGINT NOT NULL,
    created_at_ns BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    pending INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (run_id, node_id),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_build_frontier_order_idx
    ON console_graph_build_frontier(run_id, pending, created_at_ns, node_id);

CREATE TABLE console_graph_build_rank_slots (
    run_id BIGINT NOT NULL,
    mode TEXT NOT NULL,
    rank INTEGER NOT NULL,
    row INTEGER NOT NULL,
    node_id TEXT NOT NULL,
    x INTEGER NOT NULL,
    y INTEGER NOT NULL,
    active INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, mode, rank, row),
    UNIQUE (run_id, mode, node_id),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_build_rank_slots_node_idx
    ON console_graph_build_rank_slots(run_id, mode, node_id);

CREATE TABLE console_graph_build_edge_ports (
    run_id BIGINT NOT NULL,
    mode TEXT NOT NULL,
    edge_key TEXT NOT NULL,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    source_slot INTEGER NOT NULL,
    target_slot INTEGER NOT NULL,
    active INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, mode, edge_key),
    UNIQUE (run_id, mode, source_id, source_slot),
    UNIQUE (run_id, mode, target_id, target_slot),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_build_edge_ports_source_idx
    ON console_graph_build_edge_ports(run_id, mode, source_id, source_slot);
CREATE INDEX console_graph_build_edge_ports_target_idx
    ON console_graph_build_edge_ports(run_id, mode, target_id, target_slot);

CREATE TABLE console_graph_edge_ports (
    generation BIGINT NOT NULL,
    mode TEXT NOT NULL,
    edge_key TEXT NOT NULL,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    source_slot INTEGER NOT NULL,
    target_slot INTEGER NOT NULL,
    PRIMARY KEY (generation, mode, edge_key)
);

CREATE INDEX console_graph_edge_ports_source_idx
    ON console_graph_edge_ports(generation, mode, source_id, source_slot);
CREATE INDEX console_graph_edge_ports_target_idx
    ON console_graph_edge_ports(generation, mode, target_id, target_slot);

CREATE TABLE console_graph_materialization_branches (
    generation BIGINT NOT NULL,
    name TEXT NOT NULL,
    head_id TEXT NOT NULL,
    state_json TEXT NOT NULL,
    PRIMARY KEY (generation, name)
);

INSERT INTO console_graph_materialization_branches (
    generation,
    name,
    head_id,
    state_json
)
SELECT state.active_generation, branches.name, branches.head_id, branches.state_json
FROM console_graph_source_branches AS branches
CROSS JOIN console_graph_generation_state AS state
WHERE state.id = 1;

CREATE TABLE console_graph_branch_build_state (
    branch_name TEXT PRIMARY KEY NOT NULL,
    desired_head_id TEXT NOT NULL,
    inflight_head_id TEXT,
    published_head_id TEXT,
    build_generation BIGINT
);

CREATE TABLE console_graph_source_child_rechecks (
    branch_name TEXT NOT NULL,
    contribution_generation BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    traversal_kind TEXT NOT NULL,
    PRIMARY KEY (branch_name, contribution_generation, node_id, traversal_kind),
    FOREIGN KEY (node_id)
        REFERENCES console_graph_source_nodes(node_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_source_child_rechecks_node_idx
    ON console_graph_source_child_rechecks(node_id, branch_name, contribution_generation);

-- Existing contributions do not record the traversal context needed for safe child rechecks.
-- They are derived cache data, so rebuild them once after this migration.
DELETE FROM console_graph_source_refresh_queue;
DELETE FROM console_graph_source_branch_nodes;
DELETE FROM console_graph_source_branches;
