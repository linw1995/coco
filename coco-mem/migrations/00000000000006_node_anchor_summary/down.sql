UPDATE nodes
SET kind_json = json_set(
    kind_json,
    '$.Anchor.payload.Session.role',
    anchor_session_role,
    '$.Anchor.payload.Session.provider_profile',
    anchor_provider_profile,
    '$.Anchor.payload.Session.provider',
    anchor_provider,
    '$.Anchor.payload.Session.model',
    anchor_model,
    '$.Anchor.payload.Session.prompt',
    anchor_prompt
)
WHERE anchor_kind = 'session';

UPDATE nodes
SET kind_json = json_set(
    kind_json,
    '$.Anchor.payload.Prompt.prompt',
    anchor_prompt
)
WHERE anchor_kind = 'prompt';

UPDATE nodes
SET kind_json = json_set(
    kind_json,
    '$.Anchor.payload.SkillInvocation.skill_name',
    anchor_skill_name,
    '$.Anchor.payload.SkillInvocation.mode.kind',
    anchor_skill_invocation_mode
)
WHERE anchor_kind = 'skill_invocation';

UPDATE nodes
SET kind_json = json_set(
    kind_json,
    '$.Anchor.payload.SkillInvocation.mode.prompt',
    anchor_prompt
)
WHERE anchor_kind = 'skill_invocation'
  AND anchor_skill_invocation_mode = 'handoff';

UPDATE nodes
SET kind_json = json_set(
    kind_json,
    '$.Anchor.payload.SkillResult.skill_name',
    anchor_skill_name
)
WHERE anchor_kind = 'skill_result';

ALTER TABLE nodes DROP COLUMN anchor_skill_invocation_mode;
ALTER TABLE nodes DROP COLUMN anchor_skill_name;
ALTER TABLE nodes DROP COLUMN anchor_prompt;
ALTER TABLE nodes DROP COLUMN anchor_model;
ALTER TABLE nodes DROP COLUMN anchor_provider;
ALTER TABLE nodes DROP COLUMN anchor_provider_profile;
ALTER TABLE nodes DROP COLUMN anchor_session_role;
