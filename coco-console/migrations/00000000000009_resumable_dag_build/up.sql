ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_initialized INTEGER NOT NULL DEFAULT 0;
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_baseline_generation BIGINT;
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_root_id TEXT;
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_build_kind TEXT;
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_seed_cursor TEXT NOT NULL DEFAULT '';
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_seed_complete INTEGER NOT NULL DEFAULT 0;
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_init_phase TEXT NOT NULL DEFAULT 'new';
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_init_row_cursor BIGINT NOT NULL DEFAULT 0;
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_init_text_cursor TEXT NOT NULL DEFAULT '';
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_init_text_cursor_secondary TEXT NOT NULL DEFAULT '';
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_init_counter BIGINT NOT NULL DEFAULT 0;
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_source_revision BIGINT NOT NULL DEFAULT 0;
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_scope_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_finalize_phase TEXT NOT NULL DEFAULT 'traversal';
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_finalize_mode TEXT NOT NULL DEFAULT '';
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_finalize_cursor TEXT NOT NULL DEFAULT '';
ALTER TABLE console_graph_build_runs
    ADD COLUMN dag_frontier_pending_count BIGINT NOT NULL DEFAULT 0;

ALTER TABLE console_graph_build_nodes
    ADD COLUMN projection_complete INTEGER NOT NULL DEFAULT 0;
ALTER TABLE console_graph_build_nodes
    ADD COLUMN frontier_enqueued INTEGER NOT NULL DEFAULT 0;

ALTER TABLE console_graph_materialization_branches
    ADD COLUMN contribution_generation BIGINT NOT NULL DEFAULT 0;

CREATE TABLE console_graph_build_source_manifest (
    run_id BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    contribution_generation BIGINT NOT NULL,
    head_id TEXT NOT NULL,
    state_json TEXT NOT NULL,
    PRIMARY KEY (run_id, branch_name),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_build_source_manifest_contribution_idx
    ON console_graph_build_source_manifest(
        run_id,
        contribution_generation,
        branch_name
    );

CREATE INDEX console_graph_build_source_manifest_gc_idx
    ON console_graph_build_source_manifest(
        contribution_generation,
        run_id,
        branch_name
    );

CREATE TABLE console_graph_source_identity (
    id INTEGER PRIMARY KEY NOT NULL CHECK (id = 1),
    revision BIGINT NOT NULL
);

INSERT INTO console_graph_source_identity (id, revision) VALUES (1, 0);

CREATE TRIGGER console_graph_source_branches_identity_insert
AFTER INSERT ON console_graph_source_branches
BEGIN
    UPDATE console_graph_source_identity
    SET revision = revision + 1
    WHERE id = 1;
END;

CREATE TRIGGER console_graph_source_branches_identity_update
AFTER UPDATE ON console_graph_source_branches
BEGIN
    UPDATE console_graph_source_identity
    SET revision = revision + 1
    WHERE id = 1;
END;

CREATE TRIGGER console_graph_source_branches_identity_delete
AFTER DELETE ON console_graph_source_branches
BEGIN
    UPDATE console_graph_source_identity
    SET revision = revision + 1
    WHERE id = 1;
END;

CREATE TABLE console_graph_build_scope_queue (
    run_id BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    node_id TEXT NOT NULL,
    traversal_kind TEXT NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0,
    node_expanded INTEGER NOT NULL DEFAULT 0,
    parent_cursor TEXT NOT NULL DEFAULT '',
    child_cursor TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (run_id, branch_name, node_id, traversal_kind),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_build_scope_queue_pending_idx
    ON console_graph_build_scope_queue(
        run_id,
        processed,
        branch_name,
        node_id,
        traversal_kind
    );

CREATE TABLE console_graph_build_parent_expansions (
    run_id BIGINT NOT NULL,
    parent_id TEXT NOT NULL,
    next_child_id TEXT NOT NULL DEFAULT '',
    complete INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, parent_id),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE TABLE console_graph_build_parent_satisfactions (
    run_id BIGINT NOT NULL,
    parent_id TEXT NOT NULL,
    child_id TEXT NOT NULL,
    PRIMARY KEY (run_id, parent_id, child_id),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_build_parent_satisfactions_child_idx
    ON console_graph_build_parent_satisfactions(run_id, child_id, parent_id);

CREATE INDEX console_graph_build_nodes_ready_idx
    ON console_graph_build_nodes(
        run_id,
        processed,
        remaining_parents,
        frontier_enqueued,
        created_at_ns,
        node_id
    );

-- Work created by the pre-checkpoint builder cannot be resumed safely because it
-- has no durable projection or expansion cursors. Leave it to bounded abandoned
-- work cleanup and start a fresh run with the new state machine.
UPDATE console_graph_build_runs
SET status = 'abandoned', lease_expires_at_ms = 0
WHERE status = 'building';
