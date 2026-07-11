UPDATE node_anchors
SET kind_json = json_remove(
    kind_json,
    '$.Anchor.payload.SkillInvocation.skill_name',
    '$.Anchor.payload.SkillInvocation.mode.kind',
    '$.Anchor.payload.SkillInvocation.mode.prompt'
)
WHERE kind = 'skill_invocation';
