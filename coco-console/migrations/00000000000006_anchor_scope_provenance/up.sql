CREATE TABLE console_graph_anchor_scopes (
    generation BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    node_id TEXT NOT NULL,
    PRIMARY KEY (generation, branch_name, node_id)
);

CREATE INDEX console_graph_anchor_scopes_node_idx
    ON console_graph_anchor_scopes(generation, node_id, branch_name);

CREATE TABLE console_graph_anchor_scope_manifests (
    generation BIGINT PRIMARY KEY NOT NULL,
    scope_count BIGINT NOT NULL CHECK (scope_count >= 0)
);

CREATE TABLE console_graph_build_anchor_edges (
    run_id BIGINT NOT NULL,
    target_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    edge_kind TEXT NOT NULL,
    edge_order INTEGER NOT NULL,
    PRIMARY KEY (run_id, target_id, source_id),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_build_anchor_edges_target_idx
    ON console_graph_build_anchor_edges(run_id, target_id, edge_order, source_id);
