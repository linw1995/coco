DELETE FROM store_meta WHERE key IN (
    'node_item_rows_backfilled',
    'node_items_backfilled'
);

DROP INDEX node_tool_results_tool_result_id_idx;
DROP TABLE node_tool_results;

DROP INDEX node_tool_uses_tool_use_id_idx;
DROP TABLE node_tool_uses;
