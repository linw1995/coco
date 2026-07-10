CREATE TEMP TABLE node_item_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO node_item_migration_guard (complete)
SELECT CASE
    WHEN EXISTS (
        SELECT 1
        FROM store_meta
        WHERE key = 'node_items_backfilled'
          AND value_json = 'true'
    ) THEN 1
    ELSE 0
END;

DROP TABLE node_item_migration_guard;

ALTER TABLE nodes
ADD COLUMN metadata_present INTEGER NOT NULL DEFAULT 0
CHECK (metadata_present IN (0, 1));

UPDATE nodes
SET metadata_present = metadata_json IS NOT NULL;

ALTER TABLE nodes DROP COLUMN metadata_json;
