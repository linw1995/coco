CREATE TABLE web_graph_provider_branch_history (
    change_id BIGINT NOT NULL CHECK (change_id > 0),
    graph_revision BIGINT NOT NULL CHECK (graph_revision >= 0),
    branch_name TEXT NOT NULL,
    head_node_id TEXT CHECK (head_node_id IS NULL OR length(head_node_id) > 0),
    PRIMARY KEY (change_id, branch_name)
);

CREATE INDEX web_graph_provider_branch_history_branch_change_idx
    ON web_graph_provider_branch_history(branch_name, change_id DESC);

CREATE TABLE web_graph_provider_contexts (
    branch_name TEXT NOT NULL,
    branch_head_node_id TEXT NOT NULL CHECK (length(branch_head_node_id) > 0),
    context_id TEXT NOT NULL,
    head_created_at_seconds BIGINT,
    head_created_at_nanoseconds INTEGER,
    ordinal BIGINT NOT NULL CHECK (ordinal >= -1),
    node_id TEXT,
    PRIMARY KEY (branch_name, context_id, ordinal),
    FOREIGN KEY (branch_head_node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE,
    FOREIGN KEY (node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE,
    CHECK (
        (
            context_id = ''
            AND head_created_at_seconds IS NULL
            AND head_created_at_nanoseconds IS NULL
            AND ordinal = -1
            AND node_id IS NULL
        )
        OR
        (
            length(context_id) > 0
            AND head_created_at_seconds IS NOT NULL
            AND head_created_at_nanoseconds >= 0
            AND head_created_at_nanoseconds < 1000000000
            AND ordinal >= 0
            AND node_id IS NOT NULL
        )
    )
);

CREATE INDEX web_graph_provider_contexts_node_idx
    ON web_graph_provider_contexts(node_id, context_id);
