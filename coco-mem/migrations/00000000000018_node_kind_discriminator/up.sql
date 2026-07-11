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
        FROM nodes AS node
        WHERE node.kind = 'anchor'
          AND (
              SELECT COUNT(*)
              FROM anchor_payloads AS payload
              WHERE payload.node_id = node.id
          ) <> 1
    )
    AND NOT EXISTS (
        SELECT 1
        FROM anchor_payloads AS payload
        LEFT JOIN nodes AS node ON node.id = payload.node_id
        WHERE node.id IS NULL OR node.kind <> 'anchor'
    )
    THEN 1
    ELSE 0
END;

DROP TABLE node_kind_discriminator_migration_guard;

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
UPDATE nodes AS node
SET kind = payload.kind
FROM anchor_payloads AS payload
WHERE node.id = payload.node_id;
