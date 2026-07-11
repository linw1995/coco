CREATE TEMP TABLE node_anchor_kind_json_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO node_anchor_kind_json_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM node_anchors
        WHERE kind NOT IN ('session', 'session_patch', 'prompt', 'skill_invocation', 'skill_result')
           OR (
                kind = 'session'
                AND prompt IS NOT NULL
           )
           OR (
                kind = 'session_patch'
                AND prompt IS NOT NULL
           )
           OR (
                kind = 'prompt'
                AND prompt IS NULL
           )
           OR (
                kind = 'skill_invocation'
                AND prompt IS NOT NULL
           )
           OR (
                kind = 'skill_result'
                AND prompt IS NOT NULL
           )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchors AS anchor
        WHERE anchor.kind = 'session'
          AND NOT EXISTS (
              SELECT 1
              FROM node_anchor_sessions AS session
              WHERE session.node_id = anchor.node_id
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchor_sessions AS session
        JOIN node_anchors AS anchor ON anchor.node_id = session.node_id
        WHERE anchor.kind <> 'session'
           OR session.role NOT IN ('orchestrator', 'runner')
           OR (session.max_tokens IS NOT NULL AND (
               json_valid(session.max_tokens) = 0
               OR json_type(session.max_tokens) <> 'integer'
           ))
           OR (session.additional_params_json IS NOT NULL AND json_valid(session.additional_params_json) = 0)
           OR (session.active_skill_name IS NULL AND session.active_skill_handoff IS NOT NULL)
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchor_session_tools AS tool
        JOIN node_anchors AS anchor ON anchor.node_id = tool.node_id
        WHERE anchor.kind <> 'session'
           OR tool.ordinal < 0
           OR json_valid(tool.input_schema_json) = 0
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchor_session_tools
        GROUP BY node_id
        HAVING MIN(ordinal) <> 0 OR MAX(ordinal) <> COUNT(*) - 1
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchors AS anchor
        WHERE anchor.kind = 'session_patch'
          AND NOT EXISTS (
              SELECT 1 FROM node_anchor_session_patches AS patch WHERE patch.node_id = anchor.node_id
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchor_session_patches AS patch
        JOIN node_anchors AS anchor ON anchor.node_id = patch.node_id
        WHERE anchor.kind <> 'session_patch'
           OR patch.role NOT IN ('orchestrator', 'runner')
           OR (patch.provider_profile_present = 0 AND patch.provider_profile IS NOT NULL)
           OR (patch.provider_present = 0 AND patch.provider IS NOT NULL)
           OR (patch.temperature_present = 0 AND patch.temperature IS NOT NULL)
           OR (patch.max_tokens_present = 0 AND patch.max_tokens IS NOT NULL)
           OR (patch.max_tokens IS NOT NULL AND (
               json_valid(patch.max_tokens) = 0
               OR json_type(patch.max_tokens) <> 'integer'
           ))
           OR (patch.additional_params_present = 0 AND patch.additional_params_json IS NOT NULL)
           OR (patch.additional_params_json IS NOT NULL AND json_valid(patch.additional_params_json) = 0)
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchor_session_patch_tools AS tool
        JOIN node_anchor_session_patches AS patch ON patch.node_id = tool.node_id
        WHERE patch.tools_present = 0
           OR tool.ordinal < 0
           OR json_valid(tool.input_schema_json) = 0
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchor_session_patch_tools
        GROUP BY node_id
        HAVING MIN(ordinal) <> 0 OR MAX(ordinal) <> COUNT(*) - 1
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchor_prompt_attachments AS attachment
        JOIN node_anchors AS anchor ON anchor.node_id = attachment.node_id
        WHERE anchor.kind <> 'prompt'
           OR attachment.kind <> 'image'
           OR attachment.ordinal < 0
           OR attachment.width NOT BETWEEN 0 AND 4294967295
           OR attachment.height NOT BETWEEN 0 AND 4294967295
           OR (attachment.file_size IS NOT NULL AND (
               json_valid(attachment.file_size) = 0
               OR json_type(attachment.file_size) <> 'integer'
           ))
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchor_prompt_attachments
        GROUP BY node_id
        HAVING MIN(ordinal) <> 0 OR MAX(ordinal) <> COUNT(*) - 1
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchors AS anchor
        WHERE anchor.kind = 'skill_invocation'
          AND NOT EXISTS (
              SELECT 1
              FROM node_anchor_skill_invocations AS invocation
              WHERE invocation.node_id = anchor.node_id
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchor_skill_invocations AS invocation
        JOIN node_anchors AS anchor ON anchor.node_id = invocation.node_id
        WHERE anchor.kind <> 'skill_invocation'
           OR invocation.mode NOT IN ('inherit_context', 'handoff')
           OR (invocation.mode = 'inherit_context' AND invocation.prompt IS NOT NULL)
           OR (invocation.mode = 'handoff' AND invocation.prompt IS NULL)
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchors AS anchor
        WHERE anchor.kind = 'skill_result'
          AND NOT EXISTS (
              SELECT 1
              FROM node_anchor_skill_results AS result
              WHERE result.node_id = anchor.node_id
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchor_skill_results AS result
        JOIN node_anchors AS anchor ON anchor.node_id = result.node_id
        WHERE anchor.kind <> 'skill_result'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchors AS anchor
        JOIN nodes AS node ON node.id = anchor.node_id
        WHERE (
            SELECT COUNT(*)
            FROM node_relations AS relation
            WHERE relation.child_node_id = anchor.node_id
              AND relation.kind = 'primary'
        ) <> 1
        OR NOT EXISTS (
            SELECT 1
            FROM node_relations AS relation
            WHERE relation.child_node_id = anchor.node_id
              AND relation.parent_node_id = node.parent_id
              AND relation.kind = 'primary'
              AND relation.ordinal = 0
        )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_relations AS relation
        JOIN node_anchors AS anchor ON anchor.node_id = relation.child_node_id
        WHERE relation.kind NOT IN ('primary', 'merge', 'shadow')
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_relations
        WHERE kind IN ('merge', 'shadow')
        GROUP BY child_node_id
        HAVING MIN(ordinal) <> 0
            OR MAX(ordinal) <> COUNT(*) - 1
            OR COUNT(DISTINCT ordinal) <> COUNT(*)
            OR SUM(kind = 'shadow') > 1
    )
    THEN 1
    ELSE 0
END;

DROP TABLE node_anchor_kind_json_migration_guard;

ALTER TABLE node_anchors DROP COLUMN kind_json;
