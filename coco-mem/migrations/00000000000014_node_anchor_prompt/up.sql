CREATE TEMP TABLE node_anchor_prompt_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO node_anchor_prompt_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM node_anchors
        WHERE kind = 'prompt'
          AND (
              prompt IS NULL
              OR json_valid(kind_json) = 0
              OR json_type(kind_json, '$.Anchor') <> 'object'
              OR json_type(kind_json, '$.Anchor.merge_parents') <> 'array'
              OR json_type(kind_json, '$.Anchor.payload') <> 'object'
              OR json_type(kind_json, '$.Anchor.payload.Prompt') <> 'object'
              OR json_type(kind_json, '$.Anchor.payload.Prompt.attachments') <> 'array'
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchors AS anchor
        JOIN json_each(anchor.kind_json, '$.Anchor.payload.Prompt.attachments') AS attachment
        WHERE anchor.kind = 'prompt'
          AND (
              json_type(attachment.value) <> 'object'
              OR json_extract(attachment.value, '$.kind') <> 'image'
              OR json_type(attachment.value, '$.id') <> 'text'
              OR json_type(attachment.value, '$.width') NOT IN ('null', 'integer')
              OR COALESCE(json_extract(attachment.value, '$.width') NOT BETWEEN 0 AND 4294967295, 0)
              OR json_type(attachment.value, '$.height') NOT IN ('null', 'integer')
              OR COALESCE(json_extract(attachment.value, '$.height') NOT BETWEEN 0 AND 4294967295, 0)
              OR json_type(attachment.value, '$.file_size') NOT IN ('null', 'integer')
              OR json_type(attachment.value, '$.media_type') NOT IN ('null', 'text')
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchors AS anchor
        JOIN json_each(anchor.kind_json, '$.Anchor.merge_parents') AS merge_parent
        WHERE anchor.kind = 'prompt'
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
        WHERE anchor.kind = 'prompt'
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

DROP TABLE node_anchor_prompt_migration_guard;

CREATE TABLE node_anchor_prompts (
    node_id TEXT PRIMARY KEY NOT NULL,
    prompt TEXT NOT NULL,
    FOREIGN KEY (node_id) REFERENCES node_anchors(node_id) ON DELETE CASCADE
);

INSERT INTO node_anchor_prompts (node_id, prompt)
SELECT node_id, prompt
FROM node_anchors
WHERE kind = 'prompt';

CREATE TABLE node_anchor_prompt_attachments (
    node_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    kind TEXT NOT NULL,
    attachment_id TEXT NOT NULL,
    width BIGINT,
    height BIGINT,
    file_size TEXT,
    media_type TEXT,
    PRIMARY KEY (node_id, ordinal),
    FOREIGN KEY (node_id) REFERENCES node_anchor_prompts(node_id) ON DELETE CASCADE
);

INSERT INTO node_anchor_prompt_attachments (
    node_id,
    ordinal,
    kind,
    attachment_id,
    width,
    height,
    file_size,
    media_type
)
SELECT
    anchor.node_id,
    CAST(attachment.key AS INTEGER),
    json_extract(attachment.value, '$.kind'),
    json_extract(attachment.value, '$.id'),
    json_extract(attachment.value, '$.width'),
    json_extract(attachment.value, '$.height'),
    CASE
        WHEN json_type(attachment.value, '$.file_size') = 'null' THEN NULL
        ELSE attachment.value -> '$.file_size'
    END,
    json_extract(attachment.value, '$.media_type')
FROM node_anchors AS anchor
JOIN json_each(anchor.kind_json, '$.Anchor.payload.Prompt.attachments') AS attachment
WHERE anchor.kind = 'prompt';

UPDATE node_anchors
SET prompt = NULL
WHERE kind = 'prompt';
