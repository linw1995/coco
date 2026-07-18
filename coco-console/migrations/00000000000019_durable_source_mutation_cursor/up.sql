CREATE TABLE console_graph_source_mutation_journal_state (
    id INTEGER PRIMARY KEY NOT NULL CHECK (id = 1),
    consumed_revision BIGINT NOT NULL DEFAULT 0 CHECK (consumed_revision >= 0),
    initialized INTEGER NOT NULL DEFAULT 0 CHECK (initialized IN (0, 1))
);

INSERT INTO console_graph_source_mutation_journal_state (id, consumed_revision)
VALUES (1, 0);

CREATE TABLE console_graph_source_mutation_event_runs (
    revision BIGINT PRIMARY KEY NOT NULL CHECK (revision > 0),
    phase TEXT NOT NULL CHECK (phase IN ('branch_changes', 'dirty_parents')),
    branch_cursor TEXT,
    dirty_parent_cursor TEXT,
    active_dirty_parent_id TEXT,
    peer_branch_cursor TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (
        (active_dirty_parent_id IS NULL AND peer_branch_cursor IS NULL)
        OR active_dirty_parent_id IS NOT NULL
    )
);
