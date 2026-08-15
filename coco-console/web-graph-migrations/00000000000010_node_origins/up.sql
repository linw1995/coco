CREATE TABLE web_graph_node_origins (
    node_id TEXT PRIMARY KEY NOT NULL,
    branch_instance_id TEXT NOT NULL CHECK (length(branch_instance_id) > 0),
    branch_name TEXT NOT NULL,
    FOREIGN KEY (node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE
);

CREATE INDEX web_graph_node_origins_instance_idx
ON web_graph_node_origins (branch_instance_id, node_id);
