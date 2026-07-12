CREATE TABLE store_meta (
    key TEXT PRIMARY KEY NOT NULL,
    value_json TEXT NOT NULL
);

INSERT INTO store_meta (key, value_json)
SELECT 'root_id', json_quote(id)
FROM nodes
WHERE parent_id = '';

INSERT INTO store_meta (key, value_json) VALUES
    ('fs_migration_complete', 'true'),
    ('node_items_backfilled', 'true');
