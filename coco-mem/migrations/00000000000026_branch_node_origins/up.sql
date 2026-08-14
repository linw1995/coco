CREATE TABLE branch_instances (
    instance_id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    created_at TEXT,
    deleted_at TEXT
);

CREATE INDEX branch_instances_name_idx
ON branch_instances (name);

INSERT INTO branch_instances (instance_id, name, created_at, deleted_at)
SELECT 'branch-legacy-' || lower(hex(randomblob(16))), name, NULL, NULL
FROM branches;

CREATE TABLE branches_with_instances (
    name TEXT PRIMARY KEY NOT NULL,
    head_id TEXT NOT NULL REFERENCES nodes(id),
    instance_id TEXT NOT NULL UNIQUE REFERENCES branch_instances(instance_id)
);

INSERT INTO branches_with_instances (name, head_id, instance_id)
SELECT branches.name, branches.head_id, branch_instances.instance_id
FROM branches
JOIN branch_instances ON branch_instances.name = branches.name;

CREATE TABLE sessions_with_instances (
    branch_name TEXT PRIMARY KEY NOT NULL,
    state TEXT NOT NULL,
    target_branch TEXT,
    base_head_id TEXT,
    pause_reason TEXT,
    merged_anchor_id TEXT,
    FOREIGN KEY (branch_name) REFERENCES branches_with_instances(name) ON DELETE CASCADE
);

INSERT INTO sessions_with_instances (
    branch_name,
    state,
    target_branch,
    base_head_id,
    pause_reason,
    merged_anchor_id
)
SELECT
    branch_name,
    state,
    target_branch,
    base_head_id,
    pause_reason,
    merged_anchor_id
FROM sessions;

DROP TABLE sessions;
DROP TABLE branches;
ALTER TABLE branches_with_instances RENAME TO branches;
ALTER TABLE sessions_with_instances RENAME TO sessions;

CREATE TABLE node_origins (
    node_id TEXT PRIMARY KEY NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    branch_instance_id TEXT NOT NULL REFERENCES branch_instances(instance_id)
);

CREATE INDEX node_origins_branch_instance_id_node_id_idx
ON node_origins (branch_instance_id, node_id);
