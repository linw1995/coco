CREATE TEMP TABLE session_state_relational_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO session_state_relational_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM sessions
        WHERE state NOT IN ('active', 'attached', 'paused')
           OR (
               state = 'active'
               AND (
                   target_branch IS NOT NULL
                   OR base_head_id IS NOT NULL
                   OR pause_reason IS NOT NULL
                   OR merged_anchor_id IS NOT NULL
               )
           )
           OR (
               state = 'attached'
               AND (
                   target_branch IS NULL
                   OR base_head_id IS NULL
                   OR pause_reason IS NOT NULL
                   OR merged_anchor_id IS NOT NULL
               )
           )
           OR (
               state = 'paused'
               AND (
                   target_branch IS NULL
                   OR base_head_id IS NOT NULL
                   OR pause_reason IS NULL
                   OR pause_reason NOT IN ('closed', 'merged')
                   OR (pause_reason = 'closed' AND merged_anchor_id IS NOT NULL)
                   OR (pause_reason = 'merged' AND merged_anchor_id IS NULL)
               )
           )
    ) THEN 1
    ELSE 0
END;

DROP TABLE session_state_relational_migration_guard;

CREATE TABLE sessions_with_state_json (
    branch_name TEXT PRIMARY KEY NOT NULL,
    state TEXT NOT NULL,
    target_branch TEXT,
    base_head_id TEXT,
    pause_reason TEXT,
    merged_anchor_id TEXT,
    state_json TEXT NOT NULL,
    FOREIGN KEY (branch_name) REFERENCES branches(name) ON DELETE CASCADE
);

INSERT INTO sessions_with_state_json (
    branch_name,
    state,
    target_branch,
    base_head_id,
    pause_reason,
    merged_anchor_id,
    state_json
)
SELECT
    branch_name,
    state,
    target_branch,
    base_head_id,
    pause_reason,
    merged_anchor_id,
    CASE
        WHEN state = 'active' THEN json_quote('Active')
        WHEN state = 'attached' THEN json_object(
            'Attached',
            json_object(
                'target_branch', target_branch,
                'base_head_id', base_head_id
            )
        )
        WHEN pause_reason = 'closed' THEN json_object(
            'Paused',
            json_object(
                'target_branch', target_branch,
                'reason', json('"Closed"')
            )
        )
        ELSE json_object(
            'Paused',
            json_object(
                'target_branch', target_branch,
                'reason', json_object(
                    'Merged',
                    json_object('merged_anchor_id', merged_anchor_id)
                )
            )
        )
    END
FROM sessions;

DROP TABLE sessions;
ALTER TABLE sessions_with_state_json RENAME TO sessions;
