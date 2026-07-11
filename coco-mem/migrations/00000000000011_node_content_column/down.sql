CREATE TEMP TABLE node_content_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO node_content_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM nodes
        WHERE kind NOT IN ('anchor', 'tool_use', 'tool_result', 'text', 'failure')
           OR (kind IN ('text', 'failure') AND content IS NULL)
           OR (kind NOT IN ('text', 'failure') AND content IS NOT NULL)
    ) THEN 1
    ELSE 0
END;

DROP TABLE node_content_migration_guard;

ALTER TABLE nodes
ADD COLUMN kind_json TEXT NOT NULL DEFAULT '{"Anchor":null}';

UPDATE nodes
SET kind_json = CASE kind
    WHEN 'anchor' THEN '{"Anchor":null}'
    WHEN 'tool_use' THEN '{"ToolUse":[]}'
    WHEN 'tool_result' THEN '{"ToolResult":[]}'
    WHEN 'text' THEN json_object('Text', content)
    WHEN 'failure' THEN json_object('Failure', content)
END;

ALTER TABLE nodes DROP COLUMN content;
