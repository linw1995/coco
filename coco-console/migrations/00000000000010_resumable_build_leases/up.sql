ALTER TABLE console_graph_build_runs
ADD COLUMN lease_epoch BIGINT NOT NULL DEFAULT 0;

CREATE INDEX console_graph_build_runs_resume_idx
    ON console_graph_build_runs(source_version, status, lease_expires_at_ms, run_id);

CREATE TABLE console_graph_build_shell_projections (
    run_id BIGINT NOT NULL,
    mode TEXT NOT NULL,
    phase TEXT NOT NULL DEFAULT 'nodes',
    row_cursor BIGINT NOT NULL DEFAULT 0,
    node_count BIGINT NOT NULL DEFAULT 0,
    edge_count BIGINT NOT NULL DEFAULT 0,
    node_max_x INTEGER,
    node_max_y INTEGER,
    edge_max_x INTEGER,
    edge_max_y INTEGER,
    tick_cursor_created_at_ns BIGINT,
    tick_cursor_node_id TEXT,
    tick_ordinal BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, mode),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);
