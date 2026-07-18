CREATE TABLE console_graph_build_branch_label_resolutions (
    run_id BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    current_node_id TEXT NOT NULL,
    anchor_id TEXT,
    phase TEXT NOT NULL CHECK (phase IN ('ancestry', 'ready')),
    PRIMARY KEY (run_id, branch_name),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE TABLE console_graph_build_branch_label_visited (
    run_id BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    node_id TEXT NOT NULL,
    PRIMARY KEY (run_id, branch_name, node_id),
    FOREIGN KEY (run_id, branch_name)
        REFERENCES console_graph_build_branch_label_resolutions(run_id, branch_name)
        ON DELETE CASCADE
);
