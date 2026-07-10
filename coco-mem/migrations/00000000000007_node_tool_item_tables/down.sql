UPDATE nodes
SET kind_json = (
    SELECT '{"ToolUse":[' || COALESCE(group_concat(tool_use, ','), '') || ']}'
    FROM (
        SELECT json_object(
            'id', tool_use_id,
            'name', name,
            'input', json(input_json)
        ) AS tool_use
        FROM node_tool_uses
        WHERE node_tool_uses.node_id = nodes.id
        ORDER BY ordinal
    )
)
WHERE kind = 'tool_use';

UPDATE nodes
SET kind_json = (
    SELECT '{"ToolResult":[' || COALESCE(group_concat(tool_result, ','), '') || ']}'
    FROM (
        SELECT json_object(
            'id', tool_result_id,
            'output', output
        ) AS tool_result
        FROM node_tool_results
        WHERE node_tool_results.node_id = nodes.id
        ORDER BY ordinal
    )
)
WHERE kind = 'tool_result';

DELETE FROM store_meta WHERE key IN (
    'node_item_rows_backfilled',
    'node_items_backfilled'
);

DROP INDEX node_tool_results_tool_result_id_idx;
DROP TABLE node_tool_results;

DROP INDEX node_tool_uses_tool_use_id_idx;
DROP TABLE node_tool_uses;
