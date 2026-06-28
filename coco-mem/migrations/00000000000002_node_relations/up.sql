CREATE TABLE node_relations (
    child_node_id TEXT NOT NULL,
    parent_node_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (child_node_id, kind, ordinal),
    FOREIGN KEY (child_node_id) REFERENCES nodes(id),
    FOREIGN KEY (parent_node_id) REFERENCES nodes(id)
);

INSERT INTO node_relations (child_node_id, parent_node_id, kind, ordinal)
SELECT
    child_id,
    parent_id,
    kind,
    ROW_NUMBER() OVER (PARTITION BY child_id, kind ORDER BY rowid) - 1
FROM node_edges;

CREATE INDEX node_relations_child_kind_idx ON node_relations(child_node_id, kind);
CREATE INDEX node_relations_parent_kind_idx ON node_relations(parent_node_id, kind);

DROP TABLE node_edges;
