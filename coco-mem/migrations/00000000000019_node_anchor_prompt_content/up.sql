CREATE TEMP TABLE node_anchor_prompt_content_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO node_anchor_prompt_content_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM nodes AS node
        WHERE node.kind = 'anchor_prompt'
          AND (
              node.content IS NOT NULL
              OR NOT EXISTS (
                  SELECT 1
                  FROM node_anchor_prompts AS prompt
                  WHERE prompt.node_id = node.id
              )
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchor_prompts AS prompt
        LEFT JOIN nodes AS node ON node.id = prompt.node_id
        WHERE node.id IS NULL
           OR node.kind <> 'anchor_prompt'
           OR node.content IS NOT NULL
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchor_prompt_attachments AS attachment
        LEFT JOIN node_anchor_prompts AS prompt ON prompt.node_id = attachment.node_id
        WHERE prompt.node_id IS NULL
    )
    THEN 1
    ELSE 0
END;

DROP TABLE node_anchor_prompt_content_migration_guard;

UPDATE nodes
SET content = (
    SELECT prompt
    FROM node_anchor_prompts
    WHERE node_id = nodes.id
)
WHERE kind = 'anchor_prompt';

CREATE TABLE node_anchor_prompt_attachments_new (
    node_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    kind TEXT NOT NULL,
    attachment_id TEXT NOT NULL,
    width BIGINT,
    height BIGINT,
    file_size TEXT,
    media_type TEXT,
    PRIMARY KEY (node_id, ordinal),
    FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
);

INSERT INTO node_anchor_prompt_attachments_new (
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
    node_id,
    ordinal,
    kind,
    attachment_id,
    width,
    height,
    file_size,
    media_type
FROM node_anchor_prompt_attachments;

DROP TABLE node_anchor_prompt_attachments;
ALTER TABLE node_anchor_prompt_attachments_new RENAME TO node_anchor_prompt_attachments;
DROP TABLE node_anchor_prompts;
