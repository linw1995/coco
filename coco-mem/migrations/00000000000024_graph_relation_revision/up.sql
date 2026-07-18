ALTER TABLE node_relations
ADD COLUMN created_revision BIGINT NOT NULL DEFAULT 0
CHECK (created_revision >= 0);

CREATE INDEX node_relations_parent_created_revision_idx
ON node_relations(parent_node_id, created_revision);

CREATE TABLE graph_child_adjacency (
    parent_node_id TEXT NOT NULL,
    child_node_id TEXT NOT NULL,
    first_created_revision BIGINT NOT NULL CHECK (first_created_revision >= 0),
    PRIMARY KEY (parent_node_id, child_node_id),
    FOREIGN KEY (parent_node_id) REFERENCES nodes(id),
    FOREIGN KEY (child_node_id) REFERENCES nodes(id)
);

CREATE INDEX graph_child_adjacency_page_idx
ON graph_child_adjacency(parent_node_id, first_created_revision, child_node_id);

INSERT INTO graph_child_adjacency (
    parent_node_id,
    child_node_id,
    first_created_revision
)
SELECT
    parent_node_id,
    child_node_id,
    MIN(created_revision)
FROM node_relations
GROUP BY parent_node_id, child_node_id;

CREATE TRIGGER node_relations_insert_graph_child_adjacency
AFTER INSERT ON node_relations
BEGIN
    INSERT INTO graph_child_adjacency (
        parent_node_id,
        child_node_id,
        first_created_revision
    ) VALUES (
        NEW.parent_node_id,
        NEW.child_node_id,
        NEW.created_revision
    )
    ON CONFLICT(parent_node_id, child_node_id) DO UPDATE SET
        first_created_revision = MIN(
            graph_child_adjacency.first_created_revision,
            excluded.first_created_revision
        );
END;

CREATE TABLE graph_relation_state (
    singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
    current_revision BIGINT NOT NULL CHECK (current_revision >= 0)
);

INSERT INTO graph_relation_state (singleton, current_revision)
VALUES (1, 0);
