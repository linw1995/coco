ALTER TABLE graph_relation_state
ADD COLUMN baseline_revision BIGINT NOT NULL DEFAULT 0
CHECK (baseline_revision >= 0 AND baseline_revision <= current_revision);

UPDATE graph_relation_state
SET baseline_revision = current_revision
WHERE singleton = 1;

CREATE TABLE graph_mutation_events (
    revision BIGINT PRIMARY KEY NOT NULL CHECK (revision > 0)
);

CREATE TABLE graph_mutation_event_branch_changes (
    revision BIGINT NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('upserted', 'removed')),
    head_id TEXT,
    state_json TEXT,
    PRIMARY KEY (revision, name),
    FOREIGN KEY (revision) REFERENCES graph_mutation_events(revision) ON DELETE CASCADE,
    CHECK (
        (kind = 'upserted' AND head_id IS NOT NULL AND state_json IS NOT NULL)
        OR (kind = 'removed' AND head_id IS NULL AND state_json IS NULL)
    )
);

CREATE TABLE graph_mutation_event_dirty_parents (
    revision BIGINT NOT NULL,
    parent_id TEXT NOT NULL,
    PRIMARY KEY (revision, parent_id),
    FOREIGN KEY (revision) REFERENCES graph_mutation_events(revision) ON DELETE CASCADE
);

CREATE TABLE graph_mutation_event_branch_change_prune_staging (
    revision BIGINT NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('upserted', 'removed')),
    head_id TEXT,
    state_json TEXT,
    PRIMARY KEY (revision, name),
    CHECK (
        (kind = 'upserted' AND head_id IS NOT NULL AND state_json IS NOT NULL)
        OR (kind = 'removed' AND head_id IS NULL AND state_json IS NULL)
    )
);

CREATE TABLE graph_mutation_event_dirty_parent_prune_staging (
    revision BIGINT NOT NULL,
    parent_id TEXT NOT NULL,
    PRIMARY KEY (revision, parent_id)
);

CREATE TABLE graph_branch_history (
    name TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    head_id TEXT,
    state_json TEXT,
    removed INTEGER NOT NULL CHECK (removed IN (0, 1)),
    PRIMARY KEY (name, revision),
    CHECK (
        (removed = 0 AND head_id IS NOT NULL AND state_json IS NOT NULL)
        OR (removed = 1 AND head_id IS NULL AND state_json IS NULL)
    )
);

CREATE TABLE graph_branch_names (
    name TEXT PRIMARY KEY NOT NULL,
    first_revision BIGINT NOT NULL CHECK (first_revision >= 0)
);

CREATE INDEX graph_branch_names_revision_name_idx
ON graph_branch_names(first_revision, name);

CREATE TRIGGER graph_branch_history_insert_name
AFTER INSERT ON graph_branch_history
BEGIN
    INSERT INTO graph_branch_names (name, first_revision)
    VALUES (NEW.name, NEW.revision)
    ON CONFLICT(name) DO UPDATE SET
        first_revision = MIN(graph_branch_names.first_revision, excluded.first_revision);
END;

CREATE INDEX graph_branch_history_revision_name_idx
ON graph_branch_history(revision, name);

INSERT INTO graph_branch_history (
    name,
    revision,
    head_id,
    state_json,
    removed
)
SELECT
    branches.name,
    graph_relation_state.baseline_revision,
    branches.head_id,
    CASE
        WHEN sessions.state = 'active' THEN json_quote('Active')
        WHEN sessions.state = 'attached' THEN json_object(
            'Attached',
            json_object(
                'target_branch', sessions.target_branch,
                'base_head_id', sessions.base_head_id
            )
        )
        WHEN sessions.pause_reason = 'closed' THEN json_object(
            'Paused',
            json_object(
                'target_branch', sessions.target_branch,
                'reason', json('"Closed"')
            )
        )
        ELSE json_object(
            'Paused',
            json_object(
                'target_branch', sessions.target_branch,
                'reason', json_object(
                    'Merged',
                    json_object('merged_anchor_id', sessions.merged_anchor_id)
                )
            )
        )
    END,
    0
FROM branches
INNER JOIN sessions ON sessions.branch_name = branches.name
CROSS JOIN graph_relation_state
WHERE graph_relation_state.singleton = 1;
