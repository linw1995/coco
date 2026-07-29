CREATE TABLE web_graph_tool_uses (
    tool_use_id TEXT PRIMARY KEY NOT NULL,
    source_row_id BIGINT NOT NULL CHECK (source_row_id > 0),
    node_id TEXT NOT NULL,
    name TEXT NOT NULL,
    FOREIGN KEY (node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE
);

CREATE TABLE web_graph_exec_sessions (
    session_id TEXT PRIMARY KEY NOT NULL,
    source_row_id BIGINT NOT NULL CHECK (source_row_id > 0),
    exec_node_id TEXT NOT NULL,
    FOREIGN KEY (exec_node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE
);
