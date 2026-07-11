UPDATE node_anchors AS anchor
SET skill_name = invocation.skill_name,
    skill_invocation_mode = invocation.mode,
    prompt = invocation.prompt
FROM node_anchor_skill_invocations AS invocation
WHERE anchor.node_id = invocation.node_id;

UPDATE node_anchors
SET kind_json = json_remove(
    kind_json,
    '$.Anchor.payload.SkillInvocation.skill_name',
    '$.Anchor.payload.SkillInvocation.mode.kind',
    '$.Anchor.payload.SkillInvocation.mode.prompt'
)
WHERE kind = 'skill_invocation';

DROP TABLE node_anchor_skill_invocations;
