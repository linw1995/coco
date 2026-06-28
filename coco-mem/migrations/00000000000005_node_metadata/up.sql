CREATE TABLE node_metadata (
    node_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    execution_id TEXT,
    call_id TEXT,
    PRIMARY KEY (node_id, ordinal),
    FOREIGN KEY (node_id) REFERENCES nodes(id)
);

INSERT INTO node_metadata (node_id, ordinal, execution_id, call_id)
SELECT
    id,
    0,
    json_extract(metadata_json, '$.execution_id'),
    json_extract(metadata_json, '$.call_id')
FROM nodes
WHERE metadata_json IS NOT NULL
  AND json_type(metadata_json) = 'object';

INSERT INTO node_metadata (node_id, ordinal, execution_id, call_id)
SELECT
    nodes.id,
    CAST(json_each.key AS INTEGER),
    json_extract(json_each.value, '$.execution_id'),
    json_extract(json_each.value, '$.call_id')
FROM nodes, json_each(nodes.metadata_json)
WHERE nodes.metadata_json IS NOT NULL
  AND json_type(nodes.metadata_json) = 'array';
