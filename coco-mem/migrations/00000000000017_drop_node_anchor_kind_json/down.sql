CREATE TABLE node_anchors (
    node_id TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL,
    kind_json TEXT NOT NULL DEFAULT '{"Anchor":null}',
    FOREIGN KEY (node_id) REFERENCES nodes(id)
);

CREATE INDEX node_anchors_kind_idx ON node_anchors(kind);

INSERT INTO node_anchors (node_id, kind)
SELECT node_id, 'session' FROM node_anchor_sessions
UNION ALL
SELECT node_id, 'session_patch' FROM node_anchor_session_patches
UNION ALL
SELECT node_id, 'prompt' FROM node_anchor_prompts
UNION ALL
SELECT node_id, 'skill_invocation' FROM node_anchor_skill_invocations
UNION ALL
SELECT node_id, 'skill_result' FROM node_anchor_skill_results;

UPDATE node_anchors AS anchor
SET kind_json = json_object(
    'Anchor',
    json_object(
        'merge_parents',
        json(COALESCE((
            SELECT json_group_array(json(parent_json))
            FROM (
                SELECT json_object(
                    'kind', relation.kind,
                    'node_id', relation.parent_node_id
                ) AS parent_json
                FROM node_relations AS relation
                WHERE relation.child_node_id = anchor.node_id
                  AND relation.kind IN ('merge', 'shadow')
                ORDER BY relation.ordinal
            )
        ), '[]')),
        'payload',
        json_object(
            'Session',
            json(json_patch(
                json_object(
                    'role', session.role,
                    'provider_profile', session.provider_profile,
                    'provider', session.provider,
                    'model', session.model,
                    'tools', json(COALESCE((
                        SELECT json_group_array(json(tool_json))
                        FROM (
                            SELECT json_object(
                                'name', tool.name,
                                'description', tool.description,
                                'input_schema', json(tool.input_schema_json)
                            ) AS tool_json
                            FROM node_anchor_session_tools AS tool
                            WHERE tool.node_id = anchor.node_id
                            ORDER BY tool.ordinal
                        )
                    ), '[]')),
                    'system_prompt', session.system_prompt,
                    'prompt', session.prompt,
                    'temperature', session.temperature,
                    'max_tokens', json(COALESCE(session.max_tokens, 'null')),
                    'additional_params', json(COALESCE(session.additional_params_json, 'null')),
                    'enable_coco_shim', json(CASE WHEN session.enable_coco_shim THEN 'true' ELSE 'false' END)
                ),
                CASE
                    WHEN session.active_skill_name IS NULL THEN '{}'
                    ELSE json_object(
                        'active_skill',
                        json_object(
                            'name', session.active_skill_name,
                            'handoff', session.active_skill_handoff
                        )
                    )
                END
            ))
        )
    )
)
FROM node_anchor_sessions AS session
WHERE anchor.kind = 'session'
  AND session.node_id = anchor.node_id;

UPDATE node_anchors AS anchor
SET kind_json = json_object(
    'Anchor',
    json_object(
        'merge_parents',
        json(COALESCE((
            SELECT json_group_array(json(parent_json))
            FROM (
                SELECT json_object(
                    'kind', relation.kind,
                    'node_id', relation.parent_node_id
                ) AS parent_json
                FROM node_relations AS relation
                WHERE relation.child_node_id = anchor.node_id
                  AND relation.kind IN ('merge', 'shadow')
                ORDER BY relation.ordinal
            )
        ), '[]')),
        'payload',
        json_object(
            'SessionPatch',
            json_object(
                'role', patch.role,
                'provider_profile', patch.provider_profile,
                'provider', patch.provider,
                'model', patch.model,
                'tools', json(CASE
                    WHEN patch.tools_present THEN COALESCE((
                        SELECT json_group_array(json(tool_json))
                        FROM (
                            SELECT json_object(
                                'name', tool.name,
                                'description', tool.description,
                                'input_schema', json(tool.input_schema_json)
                            ) AS tool_json
                            FROM node_anchor_session_patch_tools AS tool
                            WHERE tool.node_id = anchor.node_id
                            ORDER BY tool.ordinal
                        )
                    ), '[]')
                    ELSE 'null'
                END),
                'system_prompt', patch.system_prompt,
                'temperature', patch.temperature,
                'max_tokens', json(CASE
                    WHEN patch.max_tokens_present THEN COALESCE(patch.max_tokens, 'null')
                    ELSE 'null'
                END),
                'additional_params', json(CASE
                    WHEN patch.additional_params_present THEN COALESCE(patch.additional_params_json, 'null')
                    ELSE 'null'
                END),
                'enable_coco_shim', json(CASE
                    WHEN patch.enable_coco_shim IS NULL THEN 'null'
                    WHEN patch.enable_coco_shim THEN 'true'
                    ELSE 'false'
                END)
            )
        )
    )
)
FROM node_anchor_session_patches AS patch
WHERE anchor.kind = 'session_patch'
  AND patch.node_id = anchor.node_id;

UPDATE node_anchors AS anchor
SET kind_json = json_object(
    'Anchor',
    json_object(
        'merge_parents',
        json(COALESCE((
            SELECT json_group_array(json(parent_json))
            FROM (
                SELECT json_object(
                    'kind', relation.kind,
                    'node_id', relation.parent_node_id
                ) AS parent_json
                FROM node_relations AS relation
                WHERE relation.child_node_id = anchor.node_id
                  AND relation.kind IN ('merge', 'shadow')
                ORDER BY relation.ordinal
            )
        ), '[]')),
        'payload',
        json_object(
            'Prompt',
            json_object(
                'prompt', prompt.prompt,
                'attachments', json(COALESCE((
                    SELECT json_group_array(json(attachment_json))
                    FROM (
                        SELECT json_object(
                            'kind', attachment.kind,
                            'id', attachment.attachment_id,
                            'width', attachment.width,
                            'height', attachment.height,
                            'file_size', json(COALESCE(attachment.file_size, 'null')),
                            'media_type', attachment.media_type
                        ) AS attachment_json
                        FROM node_anchor_prompt_attachments AS attachment
                        WHERE attachment.node_id = anchor.node_id
                        ORDER BY attachment.ordinal
                    )
                ), '[]'))
            )
        )
    )
)
FROM node_anchor_prompts AS prompt
WHERE anchor.kind = 'prompt'
  AND prompt.node_id = anchor.node_id;

UPDATE node_anchors AS anchor
SET kind_json = json_object(
    'Anchor',
    json_object(
        'merge_parents',
        json(COALESCE((
            SELECT json_group_array(json(parent_json))
            FROM (
                SELECT json_object(
                    'kind', relation.kind,
                    'node_id', relation.parent_node_id
                ) AS parent_json
                FROM node_relations AS relation
                WHERE relation.child_node_id = anchor.node_id
                  AND relation.kind IN ('merge', 'shadow')
                ORDER BY relation.ordinal
            )
        ), '[]')),
        'payload',
        json_object(
            'SkillInvocation',
            json_object(
                'skill_name', invocation.skill_name,
                'mode', json(CASE invocation.mode
                    WHEN 'inherit_context' THEN json_object('kind', 'inherit_context')
                    WHEN 'handoff' THEN json_object(
                        'kind', 'handoff',
                        'prompt', invocation.prompt
                    )
                END)
            )
        )
    )
)
FROM node_anchor_skill_invocations AS invocation
WHERE anchor.kind = 'skill_invocation'
  AND invocation.node_id = anchor.node_id;

UPDATE node_anchors AS anchor
SET kind_json = json_object(
    'Anchor',
    json_object(
        'merge_parents',
        json(COALESCE((
            SELECT json_group_array(json(parent_json))
            FROM (
                SELECT json_object(
                    'kind', relation.kind,
                    'node_id', relation.parent_node_id
                ) AS parent_json
                FROM node_relations AS relation
                WHERE relation.child_node_id = anchor.node_id
                  AND relation.kind IN ('merge', 'shadow')
                ORDER BY relation.ordinal
            )
        ), '[]')),
        'payload',
        json_object(
            'SkillResult',
            json_object(
                'skill_name', result.skill_name,
                'output', result.output
            )
        )
    )
)
FROM node_anchor_skill_results AS result
WHERE anchor.kind = 'skill_result'
  AND result.node_id = anchor.node_id;
