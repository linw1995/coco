CREATE TEMP TABLE store_meta_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO store_meta_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM store_meta
        WHERE key = 'root_id'
          AND NOT json_valid(value_json)
    ) THEN 1
    ELSE 0
END;

INSERT INTO store_meta_migration_guard (complete)
SELECT CASE
    WHEN (
        NOT EXISTS (SELECT 1 FROM nodes)
        AND NOT EXISTS (
            SELECT 1
            FROM store_meta
            WHERE key = 'root_id'
        )
    ) OR (
        (SELECT COUNT(*) FROM nodes WHERE parent_id = '') = 1
        AND EXISTS (
            SELECT 1
            FROM store_meta AS meta
            JOIN nodes AS root
              ON root.id = json_extract(meta.value_json, '$')
            WHERE meta.key = 'root_id'
              AND json_type(meta.value_json) IS 'text'
              AND root.parent_id = ''
        )
    ) THEN 1
    ELSE 0
END;

DROP TABLE store_meta_migration_guard;
DROP TABLE store_meta;
