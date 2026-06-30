CREATE TABLE store_meta (
    key TEXT PRIMARY KEY NOT NULL,
    value_json TEXT NOT NULL
);

CREATE TABLE nodes (
    id TEXT PRIMARY KEY NOT NULL,
    parent_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    role TEXT NOT NULL,
    kind TEXT NOT NULL,
    anchor_kind TEXT,
    anchor_session_role TEXT,
    anchor_provider_profile TEXT,
    anchor_provider TEXT,
    anchor_model TEXT,
    anchor_prompt TEXT,
    anchor_skill_name TEXT,
    anchor_skill_invocation_mode TEXT,
    metadata_json TEXT,
    kind_json TEXT NOT NULL
);

CREATE INDEX nodes_parent_idx ON nodes(parent_id);
CREATE INDEX nodes_created_at_id_idx ON nodes(created_at, id);
CREATE INDEX nodes_kind_created_at_id_idx ON nodes(kind, created_at, id);
CREATE INDEX nodes_anchor_kind_created_at_id_idx ON nodes(anchor_kind, created_at, id);

CREATE TABLE node_relations (
    child_node_id TEXT NOT NULL,
    parent_node_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (child_node_id, kind, ordinal),
    FOREIGN KEY (child_node_id) REFERENCES nodes(id),
    FOREIGN KEY (parent_node_id) REFERENCES nodes(id)
);

CREATE INDEX node_relations_child_kind_idx ON node_relations(child_node_id, kind);
CREATE INDEX node_relations_parent_kind_idx ON node_relations(parent_node_id, kind);

CREATE TABLE branches (
    name TEXT PRIMARY KEY NOT NULL,
    head_id TEXT NOT NULL,
    FOREIGN KEY (head_id) REFERENCES nodes(id)
);

CREATE TABLE sessions (
    branch_name TEXT PRIMARY KEY NOT NULL,
    state TEXT NOT NULL,
    target_branch TEXT,
    base_head_id TEXT,
    pause_reason TEXT,
    merged_anchor_id TEXT,
    state_json TEXT NOT NULL,
    FOREIGN KEY (branch_name) REFERENCES branches(name) ON DELETE CASCADE
);

CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    branch TEXT NOT NULL,
    work_branch TEXT NOT NULL,
    base TEXT NOT NULL,
    status TEXT NOT NULL,
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

CREATE TABLE node_metadata (
    node_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    execution_id TEXT,
    call_id TEXT,
    PRIMARY KEY (node_id, ordinal),
    FOREIGN KEY (node_id) REFERENCES nodes(id)
);

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
