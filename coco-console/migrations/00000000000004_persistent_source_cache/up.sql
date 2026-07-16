CREATE TABLE console_graph_source_state (
    id INTEGER PRIMARY KEY NOT NULL CHECK (id = 1),
    next_generation BIGINT NOT NULL
);

INSERT INTO console_graph_source_state (id, next_generation)
VALUES (1, 1);

CREATE TABLE console_graph_source_nodes (
    node_id TEXT PRIMARY KEY NOT NULL,
    parent_id TEXT NOT NULL,
    node_json TEXT NOT NULL
);

CREATE INDEX console_graph_source_nodes_parent_idx
    ON console_graph_source_nodes(parent_id, node_id);

CREATE TABLE console_graph_source_node_relations (
    parent_id TEXT NOT NULL,
    child_id TEXT NOT NULL,
    PRIMARY KEY (parent_id, child_id),
    FOREIGN KEY (child_id)
        REFERENCES console_graph_source_nodes(node_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_source_node_relations_child_idx
    ON console_graph_source_node_relations(child_id, parent_id);

CREATE TABLE console_graph_source_branches (
    name TEXT PRIMARY KEY NOT NULL,
    head_id TEXT NOT NULL,
    state_json TEXT NOT NULL,
    contribution_generation BIGINT NOT NULL
);

CREATE TABLE console_graph_source_branch_nodes (
    branch_name TEXT NOT NULL,
    contribution_generation BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    PRIMARY KEY (branch_name, contribution_generation, node_id),
    FOREIGN KEY (node_id)
        REFERENCES console_graph_source_nodes(node_id)
        ON DELETE CASCADE
);

CREATE INDEX console_graph_source_branch_nodes_node_idx
    ON console_graph_source_branch_nodes(node_id, branch_name, contribution_generation);

CREATE TABLE console_graph_source_refresh_queue (
    contribution_generation BIGINT NOT NULL,
    branch_name TEXT NOT NULL,
    node_id TEXT NOT NULL,
    traversal_kind TEXT NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (contribution_generation, node_id, traversal_kind)
);

CREATE INDEX console_graph_source_refresh_queue_pending_idx
    ON console_graph_source_refresh_queue(contribution_generation, processed, node_id);
