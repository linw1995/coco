ALTER TABLE nodes ADD COLUMN kind TEXT NOT NULL DEFAULT '';
ALTER TABLE nodes ADD COLUMN anchor_kind TEXT;

UPDATE nodes
SET kind = CASE
    WHEN kind_json LIKE '{"Anchor":%' THEN 'anchor'
    WHEN kind_json LIKE '{"ToolUse":%' THEN 'tool_use'
    WHEN kind_json LIKE '{"ToolResult":%' THEN 'tool_result'
    WHEN kind_json LIKE '{"Text":%' THEN 'text'
    WHEN kind_json LIKE '{"Failure":%' THEN 'failure'
    ELSE ''
END;

UPDATE nodes
SET anchor_kind = CASE
    WHEN kind_json LIKE '%"payload":{"Session":%' THEN 'session'
    WHEN kind_json LIKE '%"payload":{"SessionPatch":%' THEN 'session_patch'
    WHEN kind_json LIKE '%"payload":{"Prompt":%' THEN 'prompt'
    WHEN kind_json LIKE '%"payload":{"SkillInvocation":%' THEN 'skill_invocation'
    WHEN kind_json LIKE '%"payload":{"SkillResult":%' THEN 'skill_result'
    ELSE NULL
END
WHERE kind = 'anchor';

CREATE INDEX nodes_kind_created_at_id_idx ON nodes(kind, created_at, id);
CREATE INDEX nodes_anchor_kind_created_at_id_idx ON nodes(anchor_kind, created_at, id);
