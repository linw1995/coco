CREATE TABLE store_meta (
    key TEXT PRIMARY KEY NOT NULL,
    value_json TEXT NOT NULL
);

CREATE TABLE nodes (
    id TEXT PRIMARY KEY NOT NULL,
    parent_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    role TEXT NOT NULL,
    metadata_json TEXT,
    kind_json TEXT NOT NULL
);

CREATE INDEX nodes_parent_idx ON nodes(parent_id);
CREATE INDEX nodes_created_at_id_idx ON nodes(created_at, id);

CREATE TABLE node_edges (
    parent_id TEXT NOT NULL,
    child_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    PRIMARY KEY (parent_id, child_id, kind),
    FOREIGN KEY (child_id) REFERENCES nodes(id)
);

CREATE INDEX node_edges_parent_idx ON node_edges(parent_id);

CREATE TABLE branches (
    name TEXT PRIMARY KEY NOT NULL,
    head_id TEXT NOT NULL,
    FOREIGN KEY (head_id) REFERENCES nodes(id)
);

CREATE TABLE sessions (
    branch_name TEXT PRIMARY KEY NOT NULL,
    state_json TEXT NOT NULL,
    FOREIGN KEY (branch_name) REFERENCES branches(name) ON DELETE CASCADE
);

CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE message_queue_items (
    queue TEXT NOT NULL,
    message_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (queue, message_id)
);

CREATE INDEX message_queue_items_dequeue_idx ON message_queue_items(queue, created_at, message_id);

CREATE TABLE presets (
    name TEXT PRIMARY KEY NOT NULL,
    record_json TEXT NOT NULL
);

CREATE TABLE skills (
    role TEXT NOT NULL,
    name TEXT NOT NULL,
    record_json TEXT NOT NULL,
    PRIMARY KEY (role, name)
);
