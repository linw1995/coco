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
WHERE kind_json LIKE '%"payload":{"Session":%';

UPDATE nodes
SET kind_json = json_remove(
    kind_json,
    '$.Anchor.payload.Session.role',
    '$.Anchor.payload.Session.provider_profile',
    '$.Anchor.payload.Session.provider',
    '$.Anchor.payload.Session.model',
    '$.Anchor.payload.Session.prompt'
)
WHERE kind_json LIKE '%"payload":{"Session":%';

UPDATE nodes
SET anchor_prompt = json_extract(kind_json, '$.Anchor.payload.Prompt.prompt')
WHERE kind_json LIKE '%"payload":{"Prompt":%';

UPDATE nodes
SET kind_json = json_remove(kind_json, '$.Anchor.payload.Prompt.prompt')
WHERE kind_json LIKE '%"payload":{"Prompt":%';

UPDATE nodes
SET
    anchor_skill_name = json_extract(kind_json, '$.Anchor.payload.SkillInvocation.skill_name'),
    anchor_skill_invocation_mode = json_extract(kind_json, '$.Anchor.payload.SkillInvocation.mode.kind'),
    anchor_prompt = json_extract(kind_json, '$.Anchor.payload.SkillInvocation.mode.prompt')
WHERE kind_json LIKE '%"payload":{"SkillInvocation":%';

UPDATE nodes
SET kind_json = json_remove(
    kind_json,
    '$.Anchor.payload.SkillInvocation.skill_name',
    '$.Anchor.payload.SkillInvocation.mode.kind',
    '$.Anchor.payload.SkillInvocation.mode.prompt'
)
WHERE kind_json LIKE '%"payload":{"SkillInvocation":%';

UPDATE nodes
SET anchor_skill_name = json_extract(kind_json, '$.Anchor.payload.SkillResult.skill_name')
WHERE kind_json LIKE '%"payload":{"SkillResult":%';

UPDATE nodes
SET kind_json = json_remove(kind_json, '$.Anchor.payload.SkillResult.skill_name')
WHERE kind_json LIKE '%"payload":{"SkillResult":%';
