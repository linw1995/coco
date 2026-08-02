DROP TABLE web_graph_provider_context_nodes;

CREATE TABLE web_graph_provider_contexts (
    node_id TEXT PRIMARY KEY,
    source_row_id BIGINT NOT NULL UNIQUE CHECK (source_row_id > 0),
    context_id TEXT NOT NULL UNIQUE CHECK (length(context_id) > 0),
    previous_node_id TEXT,
    is_tool_use INTEGER NOT NULL CHECK (is_tool_use IN (0, 1)),
    FOREIGN KEY (node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE,
    FOREIGN KEY (previous_node_id) REFERENCES web_graph_provider_contexts(node_id)
        ON DELETE CASCADE
);

CREATE INDEX web_graph_provider_contexts_previous_idx
    ON web_graph_provider_contexts(previous_node_id, source_row_id DESC, node_id);
