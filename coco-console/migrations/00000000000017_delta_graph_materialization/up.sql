CREATE TABLE console_graph_generation_delta_capabilities (
    generation BIGINT PRIMARY KEY NOT NULL,
    delta_compatible INTEGER NOT NULL CHECK (delta_compatible IN (0, 1)),
    publication_epoch BIGINT NOT NULL CHECK (publication_epoch >= 0)
);

CREATE TRIGGER console_graph_edge_ports_invalidate_delta_capability_on_delete
AFTER DELETE ON console_graph_edge_ports
BEGIN
    UPDATE console_graph_generation_delta_capabilities
    SET delta_compatible = 0
    WHERE generation = OLD.generation;
END;

CREATE TRIGGER console_graph_edge_ports_invalidate_delta_capability_on_update
AFTER UPDATE ON console_graph_edge_ports
BEGIN
    UPDATE console_graph_generation_delta_capabilities
    SET delta_compatible = 0
    WHERE generation = OLD.generation;
END;

CREATE TABLE console_graph_build_changed_branches (
    run_id BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    baseline_contribution_generation BIGINT,
    target_contribution_generation BIGINT,
    change_kind TEXT NOT NULL
        CHECK (change_kind IN ('added', 'append', 'metadata', 'replace', 'delete')),
    removal_cursor TEXT NOT NULL DEFAULT '',
    removal_complete INTEGER NOT NULL DEFAULT 1 CHECK (removal_complete IN (0, 1)),
    scope_removal_cursor TEXT NOT NULL DEFAULT '',
    scope_removal_complete INTEGER NOT NULL DEFAULT 1
        CHECK (scope_removal_complete IN (0, 1)),
    scope_reuses_baseline INTEGER NOT NULL DEFAULT 0
        CHECK (scope_reuses_baseline IN (0, 1)),
    PRIMARY KEY (run_id, branch_name),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE TABLE console_graph_build_branch_deltas (
    run_id BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    published_source_revision BIGINT NOT NULL,
    refresh_id BIGINT NOT NULL,
    refresh_kind TEXT NOT NULL CHECK (refresh_kind IN ('full', 'append')),
    seed_cursor TEXT NOT NULL DEFAULT '',
    seed_complete INTEGER NOT NULL DEFAULT 0 CHECK (seed_complete IN (0, 1)),
    PRIMARY KEY (run_id, branch_name, published_source_revision, refresh_id),
    FOREIGN KEY (run_id, branch_name)
        REFERENCES console_graph_build_changed_branches(run_id, branch_name)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_build_branch_deltas_seed_idx
    ON console_graph_build_branch_deltas(
        run_id,
        seed_complete,
        branch_name,
        published_source_revision,
        refresh_id
    );

CREATE TABLE console_graph_build_delta_nodes (
    run_id BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    PRIMARY KEY (run_id, node_id),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_build_delta_nodes_order_idx
    ON console_graph_build_delta_nodes(run_id, node_id);

CREATE TABLE console_graph_build_node_tombstones (
    run_id BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    PRIMARY KEY (run_id, node_id),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE TABLE console_graph_build_scope_tombstones (
    run_id BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    node_id TEXT NOT NULL,
    PRIMARY KEY (run_id, branch_name, node_id),
    FOREIGN KEY (run_id, branch_name)
        REFERENCES console_graph_build_changed_branches(run_id, branch_name)
        ON DELETE CASCADE
);

CREATE TABLE console_graph_build_anchor_node_tombstones (
    run_id BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    PRIMARY KEY (run_id, node_id),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_node_locations_extent_x_idx
    ON console_graph_node_locations(generation, mode, max_x DESC, node_id);
CREATE INDEX console_graph_node_locations_extent_y_idx
    ON console_graph_node_locations(generation, mode, max_y DESC, node_id);
CREATE INDEX console_graph_edge_routes_extent_x_idx
    ON console_graph_edge_routes(generation, mode, max_x DESC, edge_key);
CREATE INDEX console_graph_edge_routes_extent_y_idx
    ON console_graph_edge_routes(generation, mode, max_y DESC, edge_key);
CREATE INDEX console_graph_edge_routes_source_idx
    ON console_graph_edge_routes(generation, mode, source_id, edge_key);

CREATE TABLE console_graph_branch_label_assignments (
    generation BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    mode TEXT NOT NULL,
    node_id TEXT NOT NULL,
    label TEXT NOT NULL,
    PRIMARY KEY (generation, branch_name, mode)
);

CREATE INDEX console_graph_branch_label_assignments_node_idx
    ON console_graph_branch_label_assignments(generation, mode, node_id, branch_name);

CREATE INDEX console_graph_branch_label_assignments_branch_idx
    ON console_graph_branch_label_assignments(generation, branch_name, mode, node_id);

CREATE TABLE console_graph_build_label_affected_nodes (
    run_id BIGINT NOT NULL,
    mode TEXT NOT NULL,
    node_id TEXT NOT NULL,
    PRIMARY KEY (run_id, mode, node_id),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE TABLE console_graph_build_publications (
    build_run_id BIGINT PRIMARY KEY NOT NULL,
    published_graph_generation BIGINT NOT NULL,
    publication_epoch BIGINT NOT NULL,
    build_kind TEXT NOT NULL CHECK (build_kind IN ('full', 'append')),
    source_version BIGINT NOT NULL,
    source_revision BIGINT NOT NULL,
    committed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX console_graph_build_publications_generation_idx
    ON console_graph_build_publications(published_graph_generation, build_run_id);

ALTER TABLE console_graph_materialization_time_ticks
    ADD COLUMN node_id TEXT NOT NULL DEFAULT '';

UPDATE console_graph_materialization_time_ticks AS tick
SET node_id = COALESCE((
    SELECT node.node_id
    FROM console_graph_node_locations AS node
    WHERE node.generation = tick.generation
      AND node.mode = tick.mode
      AND node.node_target = tick.node_target
    LIMIT 1
), '');

ALTER TABLE console_graph_build_shell_projections
    ADD COLUMN build_kind TEXT NOT NULL DEFAULT 'full';
ALTER TABLE console_graph_build_shell_projections
    ADD COLUMN baseline_generation BIGINT;
ALTER TABLE console_graph_build_shell_projections
    ADD COLUMN baseline_initialized INTEGER NOT NULL DEFAULT 0;
ALTER TABLE console_graph_build_shell_projections
    ADD COLUMN baseline_phase TEXT NOT NULL DEFAULT 'seed';
ALTER TABLE console_graph_build_shell_projections
    ADD COLUMN baseline_work_kind TEXT NOT NULL DEFAULT '';
ALTER TABLE console_graph_build_shell_projections
    ADD COLUMN baseline_cursor_node_id TEXT NOT NULL DEFAULT '';
ALTER TABLE console_graph_build_shell_projections
    ADD COLUMN baseline_endpoint TEXT NOT NULL DEFAULT '';
ALTER TABLE console_graph_build_shell_projections
    ADD COLUMN baseline_cursor_key TEXT NOT NULL DEFAULT '';

CREATE TABLE console_graph_build_shell_tick_candidates (
    run_id BIGINT NOT NULL,
    mode TEXT NOT NULL,
    created_at_ns BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    node_target TEXT NOT NULL,
    x INTEGER NOT NULL,
    y INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, mode, created_at_ns, node_id),
    FOREIGN KEY (run_id, mode)
        REFERENCES console_graph_build_shell_projections(run_id, mode)
        ON DELETE CASCADE
);

ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_published_graph_generation BIGINT;
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_baseline_source_revision BIGINT;
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_baseline_publication_epoch BIGINT;
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_build_classification_reason TEXT NOT NULL DEFAULT 'unclassified';

-- Checkpoint and staging semantics changed. Earlier runs cannot be resumed safely.
UPDATE console_graph_build_runs
SET status = 'abandoned', lease_expires_at_ms = 0
WHERE status IN ('building', 'paused');
