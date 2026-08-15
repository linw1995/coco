DROP TABLE node_origins;

CREATE TABLE branches_without_instances (
    name TEXT PRIMARY KEY NOT NULL,
    head_id TEXT NOT NULL REFERENCES nodes(id)
);

INSERT INTO branches_without_instances (name, head_id)
SELECT name, head_id
FROM branches;

CREATE TABLE sessions_without_instances (
    branch_name TEXT PRIMARY KEY NOT NULL,
    state TEXT NOT NULL,
    target_branch TEXT,
    base_head_id TEXT,
    pause_reason TEXT,
    merged_anchor_id TEXT,
    FOREIGN KEY (branch_name) REFERENCES branches_without_instances(name) ON DELETE CASCADE
);

INSERT INTO sessions_without_instances (
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
ALTER TABLE branches_without_instances RENAME TO branches;
ALTER TABLE sessions_without_instances RENAME TO sessions;
DROP TABLE branch_instances;
