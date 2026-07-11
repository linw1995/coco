UPDATE node_anchors
SET kind_json = json_remove(kind_json, '$.Anchor.payload.SkillResult.skill_name')
WHERE kind = 'skill_result';

ALTER TABLE node_anchors DROP COLUMN skill_result_output;
