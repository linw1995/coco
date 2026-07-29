CREATE TABLE web_graph_tool_uses (
    node_id TEXT NOT NULL,
    ordinal BIGINT NOT NULL CHECK (ordinal >= 0),
    tool_use_id TEXT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (node_id, ordinal),
    FOREIGN KEY (node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE
);

CREATE INDEX web_graph_tool_uses_tool_use_id_idx
    ON web_graph_tool_uses(tool_use_id);

CREATE TABLE web_graph_exec_sessions (
    node_id TEXT NOT NULL,
    ordinal BIGINT NOT NULL CHECK (ordinal >= 0),
    session_id TEXT NOT NULL,
    exec_node_id TEXT NOT NULL,
    PRIMARY KEY (node_id, ordinal),
    FOREIGN KEY (node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE,
    FOREIGN KEY (exec_node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE
);

CREATE INDEX web_graph_exec_sessions_session_id_idx
    ON web_graph_exec_sessions(session_id);

CREATE TABLE web_graph_tool_use_input_links (
    node_id TEXT NOT NULL,
    ordinal BIGINT NOT NULL CHECK (ordinal >= 0),
    session_id TEXT NOT NULL,
    exec_node_id TEXT NOT NULL,
    PRIMARY KEY (node_id, ordinal),
    FOREIGN KEY (node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE,
    FOREIGN KEY (exec_node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE
);
