CREATE TEMP TABLE node_anchor_session_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO node_anchor_session_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM node_anchors
        WHERE kind = 'session'
          AND (
              session_role IS NULL
              OR model IS NULL
              OR prompt IS NULL
              OR json_valid(kind_json) = 0
              OR json_type(kind_json, '$.Anchor') <> 'object'
              OR json_type(kind_json, '$.Anchor.merge_parents') <> 'array'
              OR json_type(kind_json, '$.Anchor.payload') <> 'object'
              OR json_type(kind_json, '$.Anchor.payload.Session') <> 'object'
              OR json_type(kind_json, '$.Anchor.payload.Session.tools') <> 'array'
              OR json_type(kind_json, '$.Anchor.payload.Session.system_prompt') <> 'text'
              OR json_type(kind_json, '$.Anchor.payload.Session.temperature') NOT IN ('null', 'integer', 'real')
              OR json_type(kind_json, '$.Anchor.payload.Session.max_tokens') NOT IN ('null', 'integer')
              OR json_type(kind_json, '$.Anchor.payload.Session.enable_coco_shim') NOT IN ('true', 'false')
              OR json_type(kind_json, '$.Anchor.payload.Session.active_skill') NOT IN ('null', 'object')
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchors AS anchor
        JOIN json_each(anchor.kind_json, '$.Anchor.payload.Session.tools') AS tool
        WHERE anchor.kind = 'session'
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
        WHERE anchor.kind = 'session'
          AND json_type(anchor.kind_json, '$.Anchor.payload.Session.active_skill') = 'object'
          AND (
              json_type(anchor.kind_json, '$.Anchor.payload.Session.active_skill.name') <> 'text'
              OR json_type(anchor.kind_json, '$.Anchor.payload.Session.active_skill.handoff') NOT IN ('null', 'text')
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchors AS anchor
        JOIN json_each(anchor.kind_json, '$.Anchor.merge_parents') AS merge_parent
        WHERE anchor.kind = 'session'
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
        WHERE anchor.kind = 'session'
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

DROP TABLE node_anchor_session_migration_guard;

CREATE TABLE node_anchor_sessions (
    node_id TEXT PRIMARY KEY NOT NULL,
    role TEXT NOT NULL,
    provider_profile TEXT,
    provider TEXT,
    model TEXT NOT NULL,
    system_prompt TEXT NOT NULL,
    prompt TEXT NOT NULL,
    temperature DOUBLE,
    max_tokens TEXT,
    additional_params_json TEXT,
    enable_coco_shim BOOLEAN NOT NULL,
    active_skill_name TEXT,
    active_skill_handoff TEXT,
    FOREIGN KEY (node_id) REFERENCES nodes(id)
);

INSERT INTO node_anchor_sessions (
    node_id,
    role,
    provider_profile,
    provider,
    model,
    system_prompt,
    prompt,
    temperature,
    max_tokens,
    additional_params_json,
    enable_coco_shim,
    active_skill_name,
    active_skill_handoff
)
SELECT
    node_id,
    session_role,
    provider_profile,
    provider,
    model,
    json_extract(kind_json, '$.Anchor.payload.Session.system_prompt'),
    prompt,
    json_extract(kind_json, '$.Anchor.payload.Session.temperature'),
    CASE
        WHEN json_type(kind_json, '$.Anchor.payload.Session.max_tokens') = 'null' THEN NULL
        ELSE kind_json -> '$.Anchor.payload.Session.max_tokens'
    END,
    CASE
        WHEN json_type(kind_json, '$.Anchor.payload.Session.additional_params') = 'null' THEN NULL
        ELSE kind_json -> '$.Anchor.payload.Session.additional_params'
    END,
    COALESCE(
        json_extract(kind_json, '$.Anchor.payload.Session.enable_coco_shim'),
        0
    ),
    json_extract(kind_json, '$.Anchor.payload.Session.active_skill.name'),
    json_extract(kind_json, '$.Anchor.payload.Session.active_skill.handoff')
FROM node_anchors
WHERE kind = 'session';

CREATE TABLE node_anchor_session_tools (
    node_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    input_schema_json TEXT NOT NULL,
    PRIMARY KEY (node_id, ordinal),
    FOREIGN KEY (node_id) REFERENCES node_anchor_sessions(node_id) ON DELETE CASCADE
);

INSERT INTO node_anchor_session_tools (
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
JOIN json_each(anchor.kind_json, '$.Anchor.payload.Session.tools') AS tool
WHERE anchor.kind = 'session';

UPDATE node_anchors
SET prompt = NULL
WHERE kind = 'session';

ALTER TABLE node_anchors DROP COLUMN model;
ALTER TABLE node_anchors DROP COLUMN provider;
ALTER TABLE node_anchors DROP COLUMN provider_profile;
ALTER TABLE node_anchors DROP COLUMN session_role;
