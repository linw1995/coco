ALTER TABLE jobs DROP COLUMN status;
ALTER TABLE jobs DROP COLUMN base;
ALTER TABLE jobs DROP COLUMN work_branch;
ALTER TABLE jobs DROP COLUMN branch;
ALTER TABLE jobs DROP COLUMN finished_at;
ALTER TABLE jobs DROP COLUMN created_at;

ALTER TABLE sessions DROP COLUMN merged_anchor_id;
ALTER TABLE sessions DROP COLUMN pause_reason;
ALTER TABLE sessions DROP COLUMN base_head_id;
ALTER TABLE sessions DROP COLUMN target_branch;
ALTER TABLE sessions DROP COLUMN state;
