CREATE TEMP TABLE node_kind_discriminator_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO node_kind_discriminator_migration_guard (complete)
WITH anchor_payloads AS (
    SELECT node_id, 'anchor_session' AS kind FROM node_anchor_sessions
    UNION ALL
    SELECT node_id, 'anchor_session_patch' AS kind FROM node_anchor_session_patches
    UNION ALL
    SELECT node_id, 'anchor_prompt' AS kind FROM node_anchor_prompts
    UNION ALL
    SELECT node_id, 'anchor_skill_invocation' AS kind FROM node_anchor_skill_invocations
    UNION ALL
    SELECT node_id, 'anchor_skill_result' AS kind FROM node_anchor_skill_results
)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM nodes
        WHERE kind = 'anchor'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM nodes AS node
        WHERE node.kind IN (
            'anchor_session',
            'anchor_session_patch',
            'anchor_prompt',
            'anchor_skill_invocation',
            'anchor_skill_result'
        )
          AND NOT EXISTS (
              SELECT 1
              FROM anchor_payloads AS payload
              WHERE payload.node_id = node.id
                AND payload.kind = node.kind
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM anchor_payloads AS payload
        LEFT JOIN nodes AS node ON node.id = payload.node_id
        WHERE node.id IS NULL OR node.kind <> payload.kind
    )
    THEN 1
    ELSE 0
END;

DROP TABLE node_kind_discriminator_migration_guard;

UPDATE nodes
SET kind = 'anchor'
WHERE kind IN (
    'anchor_session',
    'anchor_session_patch',
    'anchor_prompt',
    'anchor_skill_invocation',
    'anchor_skill_result'
);
