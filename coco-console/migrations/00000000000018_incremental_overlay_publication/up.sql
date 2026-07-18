CREATE TABLE console_graph_overlay_runs (
    run_id BIGINT PRIMARY KEY NOT NULL,
    base_generation BIGINT NOT NULL CHECK (base_generation >= 0),
    baseline_source_revision BIGINT NOT NULL CHECK (baseline_source_revision >= 0),
    baseline_publication_epoch BIGINT NOT NULL CHECK (baseline_publication_epoch >= 0),
    target_source_version BIGINT NOT NULL CHECK (target_source_version >= 0),
    target_source_revision BIGINT NOT NULL CHECK (target_source_revision >= 0),
    target_publication_epoch BIGINT NOT NULL CHECK (target_publication_epoch >= 0),
    status TEXT NOT NULL
        CHECK (status IN ('preparing', 'compacting', 'completed')),
    phase TEXT NOT NULL CHECK (length(phase) > 0),
    cursor_node_id TEXT NOT NULL DEFAULT '',
    cursor_endpoint TEXT NOT NULL DEFAULT ''
        CHECK (cursor_endpoint IN ('', 'source', 'target')),
    cursor_mode TEXT NOT NULL DEFAULT ''
        CHECK (cursor_mode IN ('', 'anchors', 'all')),
    cursor_work_kind TEXT NOT NULL DEFAULT ''
        CHECK (cursor_work_kind IN ('', 'global', 'anchor_only')),
    cursor_key TEXT NOT NULL DEFAULT '',
    owner_id TEXT NOT NULL DEFAULT '',
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_expires_at_ms BIGINT NOT NULL DEFAULT 0
        CHECK (lease_expires_at_ms >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    completed_at TEXT,
    CHECK (target_publication_epoch >= baseline_publication_epoch),
    CHECK ((status = 'completed') = (completed_at IS NOT NULL)),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_overlay_runs_resume_idx
    ON console_graph_overlay_runs(status, lease_expires_at_ms, run_id);

CREATE INDEX console_graph_overlay_runs_base_idx
    ON console_graph_overlay_runs(base_generation, status, run_id);

ALTER TABLE console_graph_generation_state
    ADD COLUMN active_overlay_run_id BIGINT
        REFERENCES console_graph_overlay_runs(run_id)
        ON DELETE RESTRICT;

CREATE TABLE console_graph_build_edge_route_tombstones (
    run_id BIGINT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('anchors', 'all')),
    edge_key TEXT NOT NULL,
    PRIMARY KEY (run_id, mode, edge_key),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE TABLE console_graph_build_edge_port_tombstones (
    run_id BIGINT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('anchors', 'all')),
    edge_key TEXT NOT NULL,
    PRIMARY KEY (run_id, mode, edge_key),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_edge_routes_target_keyset_idx
    ON console_graph_edge_routes(generation, mode, target_id, edge_key);

CREATE INDEX console_graph_edge_ports_source_keyset_idx
    ON console_graph_edge_ports(generation, mode, source_id, edge_key);

CREATE INDEX console_graph_edge_ports_target_keyset_idx
    ON console_graph_edge_ports(generation, mode, target_id, edge_key);
