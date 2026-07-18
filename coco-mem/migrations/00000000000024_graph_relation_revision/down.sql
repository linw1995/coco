DROP TRIGGER node_relations_insert_graph_child_adjacency;
DROP INDEX graph_child_adjacency_page_idx;
DROP TABLE graph_child_adjacency;

DROP INDEX node_relations_parent_created_revision_idx;

CREATE TABLE node_relations_without_revision (
    child_node_id TEXT NOT NULL,
    parent_node_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (child_node_id, kind, ordinal),
    FOREIGN KEY (child_node_id) REFERENCES nodes(id),
    FOREIGN KEY (parent_node_id) REFERENCES nodes(id)
);

INSERT INTO node_relations_without_revision (
    child_node_id,
    parent_node_id,
    kind,
    ordinal
)
SELECT
    child_node_id,
    parent_node_id,
    kind,
    ordinal
FROM node_relations;

DROP TABLE node_relations;
ALTER TABLE node_relations_without_revision RENAME TO node_relations;

CREATE INDEX node_relations_child_kind_idx
ON node_relations(child_node_id, kind);

CREATE INDEX node_relations_parent_kind_idx
ON node_relations(parent_node_id, kind);

DROP TABLE graph_relation_state;
