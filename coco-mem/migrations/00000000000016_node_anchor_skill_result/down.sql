ALTER TABLE node_anchors ADD COLUMN skill_name TEXT;
ALTER TABLE node_anchors ADD COLUMN skill_invocation_mode TEXT;

UPDATE node_anchors AS anchor
SET skill_name = invocation.skill_name,
    skill_invocation_mode = invocation.mode
FROM node_anchor_skill_invocations AS invocation
WHERE anchor.node_id = invocation.node_id;

UPDATE node_anchors AS anchor
SET skill_name = result.skill_name
FROM node_anchor_skill_results AS result
WHERE anchor.node_id = result.node_id;

UPDATE node_anchors
SET kind_json = json_remove(kind_json, '$.Anchor.payload.SkillResult.skill_name')
WHERE kind = 'skill_result';

DROP TABLE node_anchor_skill_results;
