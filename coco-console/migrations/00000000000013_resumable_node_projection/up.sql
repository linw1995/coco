ALTER TABLE console_graph_build_nodes
    ADD COLUMN projection_phase TEXT NOT NULL DEFAULT 'all_prepare';
ALTER TABLE console_graph_build_nodes
    ADD COLUMN projection_raw_cursor BIGINT NOT NULL DEFAULT -1;
ALTER TABLE console_graph_build_nodes
    ADD COLUMN projection_edge_order_cursor INTEGER NOT NULL DEFAULT -1;
ALTER TABLE console_graph_build_nodes
    ADD COLUMN projection_edge_source_cursor TEXT NOT NULL DEFAULT '';
ALTER TABLE console_graph_build_nodes
    ADD COLUMN projection_required_rank INTEGER NOT NULL DEFAULT 0;

ALTER TABLE console_graph_build_anchor_edges
    ADD COLUMN ancestor_depth INTEGER NOT NULL DEFAULT 0;

CREATE TABLE console_graph_build_projection_edges (
    run_id BIGINT NOT NULL,
    mode TEXT NOT NULL,
    target_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    edge_kind TEXT NOT NULL,
    edge_order INTEGER NOT NULL,
    PRIMARY KEY (run_id, mode, target_id, source_id),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_build_projection_edges_page_idx
    ON console_graph_build_projection_edges(
        run_id,
        mode,
        target_id,
        edge_order,
        source_id
    );

CREATE TABLE console_graph_build_anchor_projection_state (
    run_id BIGINT NOT NULL,
    target_id TEXT NOT NULL,
    raw_cursor BIGINT NOT NULL DEFAULT -1,
    raw_complete INTEGER NOT NULL DEFAULT 0,
    resolution_complete INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, target_id),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE TABLE console_graph_build_anchor_raw_edges (
    run_id BIGINT NOT NULL,
    target_id TEXT NOT NULL,
    edge_order INTEGER NOT NULL,
    raw_parent_id TEXT NOT NULL,
    edge_kind TEXT NOT NULL,
    current_ancestor_id TEXT NOT NULL,
    ancestor_depth INTEGER NOT NULL DEFAULT 0,
    complete INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, target_id, edge_order),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_build_anchor_raw_edges_pending_idx
    ON console_graph_build_anchor_raw_edges(
        run_id,
        target_id,
        complete,
        edge_order
    );

CREATE TABLE console_graph_build_anchor_ancestor_visits (
    run_id BIGINT NOT NULL,
    target_id TEXT NOT NULL,
    edge_order INTEGER NOT NULL,
    ancestor_id TEXT NOT NULL,
    PRIMARY KEY (run_id, target_id, edge_order, ancestor_id),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

-- Runs started by the previous projection implementation do not have durable
-- per-node cursors and therefore cannot be resumed without replay ambiguity.
UPDATE console_graph_build_runs
SET status = 'abandoned', lease_expires_at_ms = 0
WHERE status IN ('building', 'paused');
