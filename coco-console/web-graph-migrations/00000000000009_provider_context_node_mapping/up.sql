DROP TABLE web_graph_provider_contexts;

CREATE TABLE web_graph_provider_context_nodes (
    node_id TEXT PRIMARY KEY,
    source_row_id BIGINT NOT NULL UNIQUE CHECK (source_row_id > 0),
    context_id TEXT NOT NULL CHECK (length(context_id) > 0),
    previous_context_id TEXT,
    previous_node_id TEXT,
    source_parent_node_id TEXT NOT NULL CHECK (length(source_parent_node_id) > 0),
    is_tool_use INTEGER NOT NULL CHECK (is_tool_use IN (0, 1)),
    FOREIGN KEY (node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE,
    CHECK (previous_context_id IS NULL OR length(previous_context_id) > 0),
    FOREIGN KEY (previous_node_id) REFERENCES web_graph_provider_context_nodes(node_id)
        ON DELETE CASCADE
);

CREATE INDEX web_graph_provider_context_nodes_context_idx
    ON web_graph_provider_context_nodes(context_id, source_row_id, node_id);

CREATE INDEX web_graph_provider_context_nodes_previous_idx
    ON web_graph_provider_context_nodes(previous_node_id, source_row_id DESC, node_id);

CREATE INDEX web_graph_provider_context_nodes_source_parent_idx
    ON web_graph_provider_context_nodes(source_parent_node_id, source_row_id, node_id);

CREATE INDEX web_graph_provider_context_nodes_previous_context_idx
    ON web_graph_provider_context_nodes(previous_context_id, context_id);
