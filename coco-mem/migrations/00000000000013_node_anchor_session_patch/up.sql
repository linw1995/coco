CREATE TEMP TABLE node_anchor_session_patch_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO node_anchor_session_patch_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM node_anchors
        WHERE kind = 'session_patch'
          AND (
              json_valid(kind_json) = 0
              OR json_type(kind_json, '$.Anchor') <> 'object'
              OR json_type(kind_json, '$.Anchor.merge_parents') <> 'array'
              OR json_type(kind_json, '$.Anchor.payload') <> 'object'
              OR json_type(kind_json, '$.Anchor.payload.SessionPatch') <> 'object'
              OR json_type(kind_json, '$.Anchor.payload.SessionPatch.role') NOT IN ('null', 'text')
              OR COALESCE(json_extract(kind_json, '$.Anchor.payload.SessionPatch.role') NOT IN ('orchestrator', 'runner'), 0)
              OR json_type(kind_json, '$.Anchor.payload.SessionPatch.provider_profile') NOT IN ('null', 'text')
              OR json_type(kind_json, '$.Anchor.payload.SessionPatch.provider') NOT IN ('null', 'text')
              OR json_type(kind_json, '$.Anchor.payload.SessionPatch.model') NOT IN ('null', 'text')
              OR json_type(kind_json, '$.Anchor.payload.SessionPatch.tools') NOT IN ('null', 'array')
              OR json_type(kind_json, '$.Anchor.payload.SessionPatch.system_prompt') NOT IN ('null', 'text')
              OR json_type(kind_json, '$.Anchor.payload.SessionPatch.temperature') NOT IN ('null', 'integer', 'real')
              OR json_type(kind_json, '$.Anchor.payload.SessionPatch.max_tokens') NOT IN ('null', 'integer')
              OR json_type(kind_json, '$.Anchor.payload.SessionPatch.additional_params') IS NULL
              OR json_type(kind_json, '$.Anchor.payload.SessionPatch.enable_coco_shim') NOT IN ('null', 'true', 'false')
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchors AS anchor
        JOIN json_each(anchor.kind_json, '$.Anchor.payload.SessionPatch.tools') AS tool
        WHERE anchor.kind = 'session_patch'
          AND json_type(anchor.kind_json, '$.Anchor.payload.SessionPatch.tools') = 'array'
          AND (
              json_type(tool.value) <> 'object'
              OR json_type(tool.value, '$.name') <> 'text'
              OR json_type(tool.value, '$.description') <> 'text'
              OR json_type(tool.value, '$.input_schema') IS NULL
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchors AS anchor
        JOIN json_each(anchor.kind_json, '$.Anchor.merge_parents') AS merge_parent
        WHERE anchor.kind = 'session_patch'
          AND (
              json_extract(merge_parent.value, '$.kind') NOT IN ('merge', 'shadow')
              OR json_type(merge_parent.value, '$.node_id') <> 'text'
              OR NOT EXISTS (
                  SELECT 1
                  FROM node_relations AS relation
                  WHERE relation.child_node_id = anchor.node_id
                    AND relation.parent_node_id = json_extract(merge_parent.value, '$.node_id')
                    AND relation.kind = json_extract(merge_parent.value, '$.kind')
                    AND relation.ordinal = CAST(merge_parent.key AS INTEGER)
              )
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_relations AS relation
        JOIN node_anchors AS anchor ON anchor.node_id = relation.child_node_id
        WHERE anchor.kind = 'session_patch'
          AND relation.kind IN ('merge', 'shadow')
          AND NOT EXISTS (
              SELECT 1
              FROM json_each(anchor.kind_json, '$.Anchor.merge_parents') AS merge_parent
              WHERE CAST(merge_parent.key AS INTEGER) = relation.ordinal
                AND json_extract(merge_parent.value, '$.kind') = relation.kind
                AND json_extract(merge_parent.value, '$.node_id') = relation.parent_node_id
          )
    )
    THEN 1
    ELSE 0
END;

DROP TABLE node_anchor_session_patch_migration_guard;

CREATE TABLE node_anchor_session_patches (
    node_id TEXT PRIMARY KEY NOT NULL,
    role TEXT,
    provider_profile_present BOOLEAN NOT NULL,
    provider_profile TEXT,
    provider_present BOOLEAN NOT NULL,
    provider TEXT,
    model TEXT,
    tools_present BOOLEAN NOT NULL,
    system_prompt TEXT,
    temperature_present BOOLEAN NOT NULL,
    temperature DOUBLE,
    max_tokens_present BOOLEAN NOT NULL,
    max_tokens TEXT,
    additional_params_present BOOLEAN NOT NULL,
    additional_params_json TEXT,
    enable_coco_shim BOOLEAN,
    FOREIGN KEY (node_id) REFERENCES nodes(id)
);

INSERT INTO node_anchor_session_patches (
    node_id,
    role,
    provider_profile_present,
    provider_profile,
    provider_present,
    provider,
    model,
    tools_present,
    system_prompt,
    temperature_present,
    temperature,
    max_tokens_present,
    max_tokens,
    additional_params_present,
    additional_params_json,
    enable_coco_shim
)
SELECT
    node_id,
    json_extract(kind_json, '$.Anchor.payload.SessionPatch.role'),
    json_type(kind_json, '$.Anchor.payload.SessionPatch.provider_profile') <> 'null',
    json_extract(kind_json, '$.Anchor.payload.SessionPatch.provider_profile'),
    json_type(kind_json, '$.Anchor.payload.SessionPatch.provider') <> 'null',
    json_extract(kind_json, '$.Anchor.payload.SessionPatch.provider'),
    json_extract(kind_json, '$.Anchor.payload.SessionPatch.model'),
    json_type(kind_json, '$.Anchor.payload.SessionPatch.tools') = 'array',
    json_extract(kind_json, '$.Anchor.payload.SessionPatch.system_prompt'),
    json_type(kind_json, '$.Anchor.payload.SessionPatch.temperature') <> 'null',
    json_extract(kind_json, '$.Anchor.payload.SessionPatch.temperature'),
    json_type(kind_json, '$.Anchor.payload.SessionPatch.max_tokens') <> 'null',
    CASE
        WHEN json_type(kind_json, '$.Anchor.payload.SessionPatch.max_tokens') = 'null' THEN NULL
        ELSE kind_json -> '$.Anchor.payload.SessionPatch.max_tokens'
    END,
    json_type(kind_json, '$.Anchor.payload.SessionPatch.additional_params') <> 'null',
    CASE
        WHEN json_type(kind_json, '$.Anchor.payload.SessionPatch.additional_params') = 'null' THEN NULL
        ELSE kind_json -> '$.Anchor.payload.SessionPatch.additional_params'
    END,
    json_extract(kind_json, '$.Anchor.payload.SessionPatch.enable_coco_shim')
FROM node_anchors
WHERE kind = 'session_patch';

CREATE TABLE node_anchor_session_patch_tools (
    node_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    input_schema_json TEXT NOT NULL,
    PRIMARY KEY (node_id, ordinal),
    FOREIGN KEY (node_id) REFERENCES node_anchor_session_patches(node_id) ON DELETE CASCADE
);

INSERT INTO node_anchor_session_patch_tools (
    node_id,
    ordinal,
    name,
    description,
    input_schema_json
)
SELECT
    anchor.node_id,
    CAST(tool.key AS INTEGER),
    json_extract(tool.value, '$.name'),
    json_extract(tool.value, '$.description'),
    tool.value -> '$.input_schema'
FROM node_anchors AS anchor
JOIN json_each(anchor.kind_json, '$.Anchor.payload.SessionPatch.tools') AS tool
WHERE anchor.kind = 'session_patch'
  AND json_type(anchor.kind_json, '$.Anchor.payload.SessionPatch.tools') = 'array';
