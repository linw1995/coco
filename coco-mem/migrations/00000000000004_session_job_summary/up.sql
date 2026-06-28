ALTER TABLE sessions ADD COLUMN state TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN target_branch TEXT;
ALTER TABLE sessions ADD COLUMN base_head_id TEXT;
ALTER TABLE sessions ADD COLUMN pause_reason TEXT;
ALTER TABLE sessions ADD COLUMN merged_anchor_id TEXT;

UPDATE sessions
SET
    state = CASE
        WHEN state_json = '"Active"' THEN 'active'
        WHEN json_type(state_json, '$.Attached') IS NOT NULL THEN 'attached'
        WHEN json_type(state_json, '$.Paused') IS NOT NULL THEN 'paused'
        ELSE ''
    END,
    target_branch = CASE
        WHEN json_type(state_json, '$.Attached') IS NOT NULL THEN json_extract(state_json, '$.Attached.target_branch')
        WHEN json_type(state_json, '$.Paused') IS NOT NULL THEN json_extract(state_json, '$.Paused.target_branch')
        ELSE NULL
    END,
    base_head_id = CASE
        WHEN json_type(state_json, '$.Attached') IS NOT NULL THEN json_extract(state_json, '$.Attached.base_head_id')
        ELSE NULL
    END,
    pause_reason = CASE
        WHEN json_extract(state_json, '$.Paused.reason') = 'Closed' THEN 'closed'
        WHEN json_type(state_json, '$.Paused.reason.Merged') IS NOT NULL THEN 'merged'
        ELSE NULL
    END,
    merged_anchor_id = CASE
        WHEN json_type(state_json, '$.Paused.reason.Merged') IS NOT NULL THEN json_extract(state_json, '$.Paused.reason.Merged.merged_anchor_id')
        ELSE NULL
    END;

ALTER TABLE jobs ADD COLUMN created_at TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN finished_at TEXT;
ALTER TABLE jobs ADD COLUMN branch TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN work_branch TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN base TEXT NOT NULL DEFAULT '';
ALTER TABLE jobs ADD COLUMN status TEXT NOT NULL DEFAULT '';

UPDATE jobs
SET
    created_at = COALESCE(json_extract(payload_json, '$.created_at'), ''),
    finished_at = json_extract(payload_json, '$.finished_at'),
    branch = COALESCE(json_extract(payload_json, '$.branch'), ''),
    work_branch = COALESCE(NULLIF(json_extract(payload_json, '$.work_branch'), ''), json_extract(payload_json, '$.branch'), ''),
    base = COALESCE(json_extract(payload_json, '$.base'), ''),
    status = COALESCE(json_extract(payload_json, '$.status'), '');
