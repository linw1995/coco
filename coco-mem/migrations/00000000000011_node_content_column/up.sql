CREATE TEMP TABLE node_content_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO node_content_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM nodes
        WHERE json_valid(kind_json) = 0
    ) THEN 1
    ELSE 0
END;

INSERT INTO node_content_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM nodes
        WHERE COALESCE(
            CASE kind
                WHEN 'anchor' THEN
                    json_type(kind_json) = 'object'
                    AND (SELECT COUNT(*) FROM json_each(nodes.kind_json)) = 1
                    AND json_type(kind_json, '$.Anchor') = 'null'
                WHEN 'tool_use' THEN
                    json_type(kind_json) = 'object'
                    AND (SELECT COUNT(*) FROM json_each(nodes.kind_json)) = 1
                    AND json_type(kind_json, '$.ToolUse') = 'array'
                    AND json_array_length(kind_json, '$.ToolUse') = 0
                WHEN 'tool_result' THEN
                    json_type(kind_json) = 'object'
                    AND (SELECT COUNT(*) FROM json_each(nodes.kind_json)) = 1
                    AND json_type(kind_json, '$.ToolResult') = 'array'
                    AND json_array_length(kind_json, '$.ToolResult') = 0
                WHEN 'text' THEN
                    json_type(kind_json) = 'object'
                    AND (SELECT COUNT(*) FROM json_each(nodes.kind_json)) = 1
                    AND json_type(kind_json, '$.Text') = 'text'
                WHEN 'failure' THEN
                    json_type(kind_json) = 'object'
                    AND (SELECT COUNT(*) FROM json_each(nodes.kind_json)) = 1
                    AND json_type(kind_json, '$.Failure') = 'text'
                ELSE 0
            END,
            0
        ) = 0
    ) THEN 1
    ELSE 0
END;

DROP TABLE node_content_migration_guard;

ALTER TABLE nodes ADD COLUMN content TEXT;

UPDATE nodes
SET content = CASE kind
    WHEN 'text' THEN json_extract(kind_json, '$.Text')
    WHEN 'failure' THEN json_extract(kind_json, '$.Failure')
    ELSE NULL
END;

ALTER TABLE nodes DROP COLUMN kind_json;
