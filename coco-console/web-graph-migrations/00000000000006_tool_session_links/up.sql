CREATE TABLE web_graph_tool_session_states (
    node_id TEXT PRIMARY KEY NOT NULL,
    source_row_id BIGINT NOT NULL UNIQUE CHECK (source_row_id > 0),
    parent_id TEXT,
    FOREIGN KEY (node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE,
    FOREIGN KEY (parent_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE
);

CREATE TABLE web_graph_tool_uses (
    node_id TEXT NOT NULL,
    ordinal BIGINT NOT NULL CHECK (ordinal >= 0),
    tool_use_id TEXT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (node_id, ordinal),
    FOREIGN KEY (node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE
);

CREATE TABLE web_graph_exec_sessions (
    node_id TEXT NOT NULL,
    ordinal BIGINT NOT NULL CHECK (ordinal >= 0),
    session_id TEXT NOT NULL,
    exec_node_id TEXT NOT NULL,
    PRIMARY KEY (node_id, ordinal),
    FOREIGN KEY (node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE,
    FOREIGN KEY (exec_node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE
);

CREATE TABLE web_graph_tool_use_input_links (
    node_id TEXT NOT NULL,
    ordinal BIGINT NOT NULL CHECK (ordinal >= 0),
    session_id TEXT NOT NULL,
    exec_node_id TEXT NOT NULL,
    PRIMARY KEY (node_id, ordinal),
    FOREIGN KEY (node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE,
    FOREIGN KEY (exec_node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE
);
