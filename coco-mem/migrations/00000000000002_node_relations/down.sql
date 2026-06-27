CREATE TABLE node_edges (
    parent_id TEXT NOT NULL,
    child_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    PRIMARY KEY (parent_id, child_id, kind),
    FOREIGN KEY (child_id) REFERENCES nodes(id)
);

INSERT INTO node_edges (parent_id, child_id, kind)
SELECT parent_node_id, child_node_id, kind
FROM node_relations;

CREATE INDEX node_edges_parent_idx ON node_edges(parent_id);

DROP TABLE node_relations;
