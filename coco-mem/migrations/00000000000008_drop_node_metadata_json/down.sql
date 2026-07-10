ALTER TABLE nodes ADD COLUMN metadata_json TEXT;

UPDATE nodes
SET metadata_json = (
    SELECT '[' || COALESCE(group_concat(metadata, ','), '') || ']'
    FROM (
        SELECT json_object(
            'execution_id', execution_id,
            'call_id', call_id
        ) AS metadata
        FROM node_metadata
        WHERE node_metadata.node_id = nodes.id
        ORDER BY ordinal
    )
)
WHERE metadata_present = 1;

ALTER TABLE nodes DROP COLUMN metadata_present;
