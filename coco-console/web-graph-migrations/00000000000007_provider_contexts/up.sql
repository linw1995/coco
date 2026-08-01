CREATE TABLE web_graph_provider_branch_history_changes (
    change_id BIGINT PRIMARY KEY NOT NULL CHECK (change_id > 0),
    graph_revision BIGINT NOT NULL CHECK (graph_revision >= 0)
);

CREATE TABLE web_graph_provider_branch_history (
    change_id BIGINT NOT NULL,
    branch_name TEXT NOT NULL CHECK (length(branch_name) > 0),
    head_node_id TEXT CHECK (head_node_id IS NULL OR length(head_node_id) > 0),
    PRIMARY KEY (change_id, branch_name),
    FOREIGN KEY (change_id)
        REFERENCES web_graph_provider_branch_history_changes(change_id) ON DELETE CASCADE
);

CREATE INDEX web_graph_provider_branch_history_branch_change_idx
    ON web_graph_provider_branch_history(branch_name, change_id DESC);

CREATE TABLE web_graph_provider_branches (
    branch_name TEXT PRIMARY KEY NOT NULL CHECK (length(branch_name) > 0),
    head_node_id TEXT NOT NULL,
    FOREIGN KEY (head_node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE
);

CREATE TABLE web_graph_provider_contexts (
    context_id TEXT PRIMARY KEY NOT NULL CHECK (length(context_id) > 0),
    branch_name TEXT NOT NULL,
    head_created_at_seconds BIGINT NOT NULL,
    head_created_at_nanoseconds INTEGER NOT NULL CHECK (
        head_created_at_nanoseconds >= 0
        AND head_created_at_nanoseconds < 1000000000
    ),
    FOREIGN KEY (branch_name)
        REFERENCES web_graph_provider_branches(branch_name) ON DELETE CASCADE
);

CREATE INDEX web_graph_provider_contexts_branch_idx
    ON web_graph_provider_contexts(branch_name);

CREATE TABLE web_graph_provider_nodes (
    node_id TEXT PRIMARY KEY NOT NULL,
    short_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at TEXT NOT NULL,
    summary TEXT NOT NULL,
    FOREIGN KEY (node_id) REFERENCES web_graph_nodes(node_id) ON DELETE CASCADE
);

CREATE TABLE web_graph_provider_context_nodes (
    context_id TEXT NOT NULL,
    ordinal BIGINT NOT NULL CHECK (ordinal >= 0),
    node_id TEXT NOT NULL,
    PRIMARY KEY (context_id, ordinal),
    FOREIGN KEY (context_id)
        REFERENCES web_graph_provider_contexts(context_id) ON DELETE CASCADE,
    FOREIGN KEY (node_id) REFERENCES web_graph_provider_nodes(node_id) ON DELETE CASCADE
);

CREATE INDEX web_graph_provider_context_nodes_node_idx
    ON web_graph_provider_context_nodes(node_id, context_id);
