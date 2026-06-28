ALTER TABLE nodes ADD COLUMN anchor_session_role TEXT;
ALTER TABLE nodes ADD COLUMN anchor_provider_profile TEXT;
ALTER TABLE nodes ADD COLUMN anchor_provider TEXT;
ALTER TABLE nodes ADD COLUMN anchor_model TEXT;
ALTER TABLE nodes ADD COLUMN anchor_prompt TEXT;
ALTER TABLE nodes ADD COLUMN anchor_skill_name TEXT;
ALTER TABLE nodes ADD COLUMN anchor_skill_invocation_mode TEXT;

UPDATE nodes
SET
    anchor_session_role = CASE json_extract(kind_json, '$.Anchor.payload.Session.role')
        WHEN 'Orchestrator' THEN 'orchestrator'
        WHEN 'Runner' THEN 'runner'
        ELSE json_extract(kind_json, '$.Anchor.payload.Session.role')
    END,
    anchor_provider_profile = json_extract(kind_json, '$.Anchor.payload.Session.provider_profile'),
    anchor_provider = json_extract(kind_json, '$.Anchor.payload.Session.provider'),
    anchor_model = json_extract(kind_json, '$.Anchor.payload.Session.model'),
    anchor_prompt = json_extract(kind_json, '$.Anchor.payload.Session.prompt')
WHERE json_type(kind_json, '$.Anchor.payload.Session') IS NOT NULL;

UPDATE nodes
SET
    anchor_session_role = CASE json_extract(kind_json, '$.Anchor.payload.SessionPatch.role')
        WHEN 'Orchestrator' THEN 'orchestrator'
        WHEN 'Runner' THEN 'runner'
        ELSE json_extract(kind_json, '$.Anchor.payload.SessionPatch.role')
    END,
    anchor_provider_profile = json_extract(kind_json, '$.Anchor.payload.SessionPatch.provider_profile'),
    anchor_provider = json_extract(kind_json, '$.Anchor.payload.SessionPatch.provider'),
    anchor_model = json_extract(kind_json, '$.Anchor.payload.SessionPatch.model')
WHERE json_type(kind_json, '$.Anchor.payload.SessionPatch') IS NOT NULL;

UPDATE nodes
SET anchor_prompt = json_extract(kind_json, '$.Anchor.payload.Prompt.prompt')
WHERE json_type(kind_json, '$.Anchor.payload.Prompt') IS NOT NULL;

UPDATE nodes
SET
    anchor_skill_name = json_extract(kind_json, '$.Anchor.payload.SkillInvocation.skill_name'),
    anchor_skill_invocation_mode = json_extract(kind_json, '$.Anchor.payload.SkillInvocation.mode.kind'),
    anchor_prompt = json_extract(kind_json, '$.Anchor.payload.SkillInvocation.mode.prompt')
WHERE json_type(kind_json, '$.Anchor.payload.SkillInvocation') IS NOT NULL;

UPDATE nodes
SET anchor_skill_name = json_extract(kind_json, '$.Anchor.payload.SkillResult.skill_name')
WHERE json_type(kind_json, '$.Anchor.payload.SkillResult') IS NOT NULL;
