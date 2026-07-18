ALTER TABLE console_graph_source_refresh_runs
    RENAME COLUMN contribution_generation TO refresh_id;

DROP TRIGGER IF EXISTS console_graph_source_branches_identity_delete;
DROP TRIGGER IF EXISTS console_graph_source_branches_identity_update;
DROP TRIGGER IF EXISTS console_graph_source_branches_identity_insert;

ALTER TABLE console_graph_source_refresh_runs
    ADD COLUMN target_contribution_generation BIGINT NOT NULL DEFAULT 0;
ALTER TABLE console_graph_source_refresh_runs
    ADD COLUMN refresh_kind TEXT NOT NULL DEFAULT 'full'
        CHECK (refresh_kind IN ('full', 'append'));
ALTER TABLE console_graph_source_refresh_runs
    ADD COLUMN published_source_revision BIGINT;
ALTER TABLE console_graph_source_refresh_runs
    ADD COLUMN owner_id TEXT NOT NULL DEFAULT '';
ALTER TABLE console_graph_source_refresh_runs
    ADD COLUMN lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0);
ALTER TABLE console_graph_source_refresh_runs
    ADD COLUMN lease_expires_at_ms BIGINT NOT NULL DEFAULT 0
        CHECK (lease_expires_at_ms >= 0);
ALTER TABLE console_graph_source_refresh_runs
    ADD COLUMN target_invalidation_version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE console_graph_source_refresh_runs
    ADD COLUMN target_invalidation_incarnation TEXT NOT NULL DEFAULT '';
ALTER TABLE console_graph_source_refresh_runs
    ADD COLUMN target_invalidation_kind TEXT NOT NULL DEFAULT 'targeted'
        CHECK (target_invalidation_kind IN ('targeted', 'full'));
ALTER TABLE console_graph_source_refresh_runs
    ADD COLUMN relation_revision BIGINT NOT NULL DEFAULT 0 CHECK (relation_revision >= 0);
ALTER TABLE console_graph_source_refresh_runs
    ADD COLUMN expected_branch_head_id TEXT;
ALTER TABLE console_graph_source_refresh_runs
    ADD COLUMN expected_branch_state_json TEXT;
ALTER TABLE console_graph_source_refresh_runs
    ADD COLUMN expected_branch_source_revision BIGINT;
ALTER TABLE console_graph_source_refresh_runs
    ADD COLUMN expected_branch_contribution_generation BIGINT;
ALTER TABLE console_graph_source_refresh_runs
    ADD COLUMN expected_branch_absent INTEGER NOT NULL DEFAULT 0
        CHECK (expected_branch_absent IN (0, 1));

UPDATE console_graph_source_refresh_runs
SET target_contribution_generation = refresh_id,
    owner_id = 'migration',
    lease_epoch = 1,
    published_source_revision = CASE
        WHEN status = 'published' THEN (
            SELECT revision FROM console_graph_source_identity WHERE id = 1
        )
        ELSE NULL
    END;

UPDATE console_graph_source_refresh_runs
SET status = 'superseded', lease_expires_at_ms = 0
WHERE status = 'building';

CREATE INDEX console_graph_source_refresh_runs_target_delta_idx
    ON console_graph_source_refresh_runs(
        target_contribution_generation,
        status,
        published_source_revision,
        refresh_id
    );

CREATE INDEX console_graph_source_refresh_runs_branch_delta_idx
    ON console_graph_source_refresh_runs(
        branch_name,
        status,
        published_source_revision,
        refresh_id
    );

CREATE UNIQUE INDEX console_graph_source_refresh_runs_invalidation_branch_idx
    ON console_graph_source_refresh_runs(
        target_invalidation_incarnation,
        target_invalidation_version,
        branch_name
    )
    WHERE target_invalidation_version > 0 AND status = 'building';

ALTER TABLE console_graph_source_refresh_queue
    RENAME COLUMN contribution_generation TO refresh_id;

ALTER TABLE console_graph_source_refresh_queue
    ADD COLUMN parent_cursor_offset BIGINT NOT NULL DEFAULT 0;
ALTER TABLE console_graph_source_refresh_queue
    ADD COLUMN parent_traversal_complete INTEGER NOT NULL DEFAULT 0;
ALTER TABLE console_graph_source_refresh_queue
    ADD COLUMN child_scan_required INTEGER NOT NULL DEFAULT 0;
ALTER TABLE console_graph_source_refresh_queue
    ADD COLUMN child_high_watermark_frozen INTEGER NOT NULL DEFAULT 0;
ALTER TABLE console_graph_source_refresh_queue
    ADD COLUMN child_high_watermark_relation_revision BIGINT;
ALTER TABLE console_graph_source_refresh_queue
    ADD COLUMN child_high_watermark_node_id TEXT;

DROP INDEX console_graph_source_refresh_queue_pending_idx;

CREATE INDEX console_graph_source_refresh_queue_pending_idx
    ON console_graph_source_refresh_queue(
        refresh_id,
        processed,
        node_committed DESC,
        parent_traversal_complete,
        node_id,
        traversal_kind,
        force_child_scan,
        child_cursor_relation_revision,
        child_cursor_node_id,
        parent_cursor_offset,
        child_scan_required,
        child_high_watermark_frozen,
        child_high_watermark_relation_revision,
        child_high_watermark_node_id
    );

CREATE INDEX console_graph_source_child_rechecks_branch_order_idx
    ON console_graph_source_child_rechecks(
        branch_name,
        node_id,
        traversal_kind,
        contribution_generation
    );

ALTER TABLE console_graph_source_nodes
    ADD COLUMN relation_cursor_offset BIGINT NOT NULL DEFAULT 0;
ALTER TABLE console_graph_source_nodes
    ADD COLUMN relation_ingest_complete INTEGER NOT NULL DEFAULT 1;

CREATE TABLE console_graph_source_branch_publications (
    branch_name TEXT PRIMARY KEY NOT NULL,
    target_contribution_generation BIGINT NOT NULL,
    source_revision BIGINT NOT NULL
);

CREATE TABLE console_graph_source_branch_change_journal (
    source_revision BIGINT NOT NULL,
    target_invalidation_incarnation TEXT NOT NULL,
    target_invalidation_version BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    change_kind TEXT NOT NULL
        CHECK (change_kind IN ('append', 'metadata', 'replace', 'delete')),
    refresh_id BIGINT,
    base_contribution_generation BIGINT,
    target_contribution_generation BIGINT,
    head_id TEXT,
    state_json TEXT,
    PRIMARY KEY (source_revision, branch_name)
);

CREATE INDEX console_graph_source_branch_change_journal_branch_idx
    ON console_graph_source_branch_change_journal(branch_name, source_revision);

CREATE TABLE console_graph_source_invalidation_receipts (
    receipt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_invalidation_incarnation TEXT NOT NULL,
    target_invalidation_version BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    source_revision BIGINT NOT NULL,
    relation_revision BIGINT NOT NULL CHECK (relation_revision >= 0),
    refresh_id BIGINT,
    invalidation_kind TEXT NOT NULL CHECK (invalidation_kind IN ('targeted', 'full')),
    target_head_id TEXT NOT NULL,
    target_state_json TEXT NOT NULL
);

CREATE INDEX console_graph_source_invalidation_receipts_token_idx
    ON console_graph_source_invalidation_receipts(
        target_invalidation_incarnation,
        target_invalidation_version,
        branch_name,
        receipt_id
    );

CREATE TABLE console_graph_source_invalidation_receipt_seeds (
    receipt_id BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    child_high_watermark_relation_revision BIGINT,
    child_high_watermark_node_id TEXT,
    CHECK (
        (child_high_watermark_relation_revision IS NULL) =
        (child_high_watermark_node_id IS NULL)
    ),
    PRIMARY KEY (receipt_id, node_id),
    FOREIGN KEY (receipt_id)
        REFERENCES console_graph_source_invalidation_receipts(receipt_id)
        ON DELETE CASCADE
);

CREATE TABLE console_graph_source_sweep_runs (
    target_invalidation_incarnation TEXT NOT NULL,
    target_invalidation_version BIGINT NOT NULL,
    relation_revision BIGINT NOT NULL CHECK (relation_revision >= 0),
    status TEXT NOT NULL CHECK (status IN ('building', 'completed')),
    phase TEXT NOT NULL CHECK (phase IN ('enumerate', 'reconcile')),
    source_upper_bound TEXT,
    page_cursor TEXT,
    branch_recheck_node_cursor TEXT,
    branch_recheck_traversal_cursor TEXT,
    branch_recheck_raw_node_cursor TEXT,
    branch_recheck_raw_traversal_cursor TEXT,
    branch_recheck_raw_refresh_id_cursor BIGINT,
    branch_recheck_active_node_id TEXT,
    branch_recheck_active_traversal_kind TEXT,
    branch_recheck_child_cursor_relation_revision BIGINT,
    branch_recheck_child_cursor_node_id TEXT,
    owner_id TEXT NOT NULL,
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch >= 0),
    lease_expires_at_ms BIGINT NOT NULL CHECK (lease_expires_at_ms >= 0),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (
        (branch_recheck_node_cursor IS NULL) =
        (branch_recheck_traversal_cursor IS NULL)
    ),
    CHECK (
        (branch_recheck_active_node_id IS NULL) =
        (branch_recheck_active_traversal_kind IS NULL)
    ),
    CHECK (
        (branch_recheck_raw_node_cursor IS NULL) =
        (branch_recheck_raw_traversal_cursor IS NULL) AND
        (branch_recheck_raw_node_cursor IS NULL) =
        (branch_recheck_raw_refresh_id_cursor IS NULL)
    ),
    CHECK (
        (branch_recheck_child_cursor_relation_revision IS NULL) =
        (branch_recheck_child_cursor_node_id IS NULL)
    ),
    CHECK (
        branch_recheck_child_cursor_relation_revision IS NULL OR
        branch_recheck_active_node_id IS NOT NULL
    ),
    CHECK (
        branch_recheck_active_node_id IS NULL OR
        branch_recheck_raw_node_cursor IS NOT NULL
    ),
    PRIMARY KEY (target_invalidation_incarnation, target_invalidation_version)
);

CREATE INDEX console_graph_source_sweep_runs_resume_idx
    ON console_graph_source_sweep_runs(
        status,
        relation_revision,
        target_invalidation_incarnation,
        target_invalidation_version
    );

CREATE INDEX console_graph_source_sweep_runs_gc_idx
    ON console_graph_source_sweep_runs(
        status,
        updated_at,
        target_invalidation_incarnation,
        target_invalidation_version
    );

CREATE TABLE console_graph_source_invalidation_boundaries (
    target_invalidation_incarnation TEXT NOT NULL,
    target_invalidation_version BIGINT NOT NULL,
    relation_revision BIGINT NOT NULL CHECK (relation_revision >= 0),
    requested_scope TEXT NOT NULL CHECK (requested_scope IN ('targeted', 'full')),
    status TEXT NOT NULL CHECK (status IN ('building', 'completed')),
    source_revision BIGINT,
    changed_branch_count BIGINT NOT NULL CHECK (changed_branch_count >= 0),
    dirty_parent_count BIGINT NOT NULL CHECK (dirty_parent_count >= 0),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (target_invalidation_incarnation, target_invalidation_version)
);

CREATE INDEX console_graph_source_invalidation_boundaries_resume_idx
    ON console_graph_source_invalidation_boundaries(
        status,
        relation_revision,
        target_invalidation_incarnation,
        target_invalidation_version
    );

CREATE INDEX console_graph_source_invalidation_boundaries_gc_idx
    ON console_graph_source_invalidation_boundaries(
        status,
        updated_at,
        target_invalidation_incarnation,
        target_invalidation_version
    );

CREATE TABLE console_graph_source_refresh_dirty_seeds (
    refresh_id BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    child_high_watermark_relation_revision BIGINT,
    child_high_watermark_node_id TEXT,
    CHECK (
        (child_high_watermark_relation_revision IS NULL) =
        (child_high_watermark_node_id IS NULL)
    ),
    PRIMARY KEY (refresh_id, node_id),
    FOREIGN KEY (refresh_id)
        REFERENCES console_graph_source_refresh_runs(refresh_id)
        ON DELETE CASCADE
);

INSERT INTO console_graph_source_branch_publications (
    branch_name,
    target_contribution_generation,
    source_revision
)
SELECT
    branch.name,
    branch.contribution_generation,
    identity.revision
FROM console_graph_source_branches AS branch
CROSS JOIN console_graph_source_identity AS identity
WHERE identity.id = 1;

CREATE TABLE console_graph_build_source_refresh_manifest (
    run_id BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    refresh_id BIGINT NOT NULL,
    PRIMARY KEY (run_id, branch_name, refresh_id),
    FOREIGN KEY (run_id)
        REFERENCES console_graph_build_runs(run_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_build_source_refresh_manifest_refresh_idx
    ON console_graph_build_source_refresh_manifest(refresh_id, run_id, branch_name);

CREATE VIEW console_graph_source_current_branch_nodes AS
SELECT DISTINCT
    branch.name AS branch_name,
    branch.contribution_generation,
    membership.node_id
FROM console_graph_source_branches AS branch
INNER JOIN console_graph_source_branch_publications AS publication
    ON publication.branch_name = branch.name
   AND publication.target_contribution_generation = branch.contribution_generation
INNER JOIN console_graph_source_refresh_runs AS refresh
    ON refresh.branch_name = branch.name
   AND refresh.target_contribution_generation = branch.contribution_generation
   AND refresh.status = 'published'
   AND refresh.published_source_revision <= publication.source_revision
INNER JOIN console_graph_source_branch_nodes AS membership
    ON membership.branch_name = branch.name
   AND membership.contribution_generation = refresh.refresh_id;

CREATE VIEW console_graph_source_current_completed_work AS
SELECT DISTINCT
    branch.name AS branch_name,
    branch.contribution_generation,
    work.node_id,
    work.traversal_kind
FROM console_graph_source_branches AS branch
INNER JOIN console_graph_source_branch_publications AS publication
    ON publication.branch_name = branch.name
   AND publication.target_contribution_generation = branch.contribution_generation
INNER JOIN console_graph_source_refresh_runs AS refresh
    ON refresh.branch_name = branch.name
   AND refresh.target_contribution_generation = branch.contribution_generation
   AND refresh.status = 'published'
   AND refresh.published_source_revision <= publication.source_revision
INNER JOIN console_graph_source_refresh_queue AS work
    ON work.refresh_id = refresh.refresh_id
   AND work.branch_name = branch.name
   AND work.processed = 1;

CREATE VIEW console_graph_source_current_child_rechecks AS
SELECT DISTINCT
    branch.name AS branch_name,
    branch.contribution_generation,
    recheck.node_id,
    recheck.traversal_kind
FROM console_graph_source_branches AS branch
INNER JOIN console_graph_source_branch_publications AS publication
    ON publication.branch_name = branch.name
   AND publication.target_contribution_generation = branch.contribution_generation
INNER JOIN console_graph_source_refresh_runs AS refresh
    ON refresh.branch_name = branch.name
   AND refresh.target_contribution_generation = branch.contribution_generation
   AND refresh.status = 'published'
   AND refresh.published_source_revision <= publication.source_revision
INNER JOIN console_graph_source_child_rechecks AS recheck
    ON recheck.branch_name = branch.name
   AND recheck.contribution_generation = refresh.refresh_id;

CREATE VIEW console_graph_build_effective_source_branch_nodes AS
SELECT DISTINCT
    manifest.run_id,
    manifest.branch_name,
    membership.node_id
FROM console_graph_build_source_refresh_manifest AS manifest
INNER JOIN console_graph_source_branch_nodes AS membership
    ON membership.branch_name = manifest.branch_name
   AND membership.contribution_generation = manifest.refresh_id;

CREATE VIEW console_graph_source_published_delta_nodes AS
SELECT
    refresh.published_source_revision,
    refresh.branch_name,
    refresh.refresh_id,
    refresh.target_contribution_generation,
    refresh.refresh_kind,
    membership.node_id
FROM console_graph_source_refresh_runs AS refresh
INNER JOIN console_graph_source_branch_nodes AS membership
    ON membership.branch_name = refresh.branch_name
   AND membership.contribution_generation = refresh.refresh_id
WHERE refresh.status = 'published'
  AND refresh.published_source_revision IS NOT NULL;
