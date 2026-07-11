CREATE TEMP TABLE session_state_json_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO session_state_json_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM sessions
        WHERE NOT json_valid(state_json)
    ) THEN 1
    ELSE 0
END;

DELETE FROM session_state_json_migration_guard;

INSERT INTO session_state_json_migration_guard (complete)
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
                   OR json_type(state_json) IS NOT 'text'
                   OR json_extract(state_json, '$') IS NOT 'Active'
               )
           )
           OR (
               state = 'attached'
               AND (
                   target_branch IS NULL
                   OR base_head_id IS NULL
                   OR pause_reason IS NOT NULL
                   OR merged_anchor_id IS NOT NULL
                   OR json_type(state_json) IS NOT 'object'
                   OR json_type(state_json, '$.Attached') IS NOT 'object'
                   OR json_type(
                       state_json,
                       '$.Attached.target_branch'
                   ) IS NOT 'text'
                   OR json_extract(
                       state_json,
                       '$.Attached.target_branch'
                   ) IS NOT target_branch
                   OR json_type(
                       state_json,
                       '$.Attached.base_head_id'
                   ) IS NOT 'text'
                   OR json_extract(
                       state_json,
                       '$.Attached.base_head_id'
                   ) IS NOT base_head_id
               )
           )
           OR (
               state = 'paused'
               AND (
                   target_branch IS NULL
                   OR base_head_id IS NOT NULL
                   OR pause_reason IS NULL
                   OR pause_reason NOT IN ('closed', 'merged')
                   OR json_type(state_json) IS NOT 'object'
                   OR json_type(state_json, '$.Paused') IS NOT 'object'
                   OR json_type(
                       state_json,
                       '$.Paused.target_branch'
                   ) IS NOT 'text'
                   OR json_extract(
                       state_json,
                       '$.Paused.target_branch'
                   ) IS NOT target_branch
                   OR (
                       pause_reason = 'closed'
                       AND (
                           merged_anchor_id IS NOT NULL
                           OR json_type(
                               state_json,
                               '$.Paused.reason'
                           ) IS NOT 'text'
                           OR json_extract(
                               state_json,
                               '$.Paused.reason'
                           ) IS NOT 'Closed'
                       )
                   )
                   OR (
                       pause_reason = 'merged'
                       AND (
                           merged_anchor_id IS NULL
                           OR json_type(
                               state_json,
                               '$.Paused.reason.Merged'
                           ) IS NOT 'object'
                           OR json_type(
                               state_json,
                               '$.Paused.reason.Merged.merged_anchor_id'
                           ) IS NOT 'text'
                           OR json_extract(
                               state_json,
                               '$.Paused.reason.Merged.merged_anchor_id'
                           ) IS NOT merged_anchor_id
                       )
                   )
               )
           )
    ) THEN 1
    ELSE 0
END;

DROP TABLE session_state_json_migration_guard;

ALTER TABLE sessions DROP COLUMN state_json;
