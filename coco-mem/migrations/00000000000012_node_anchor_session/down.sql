DROP TABLE node_anchor_session_tools;

ALTER TABLE node_anchors DROP COLUMN session_active_skill_handoff;
ALTER TABLE node_anchors DROP COLUMN session_active_skill_name;
ALTER TABLE node_anchors DROP COLUMN session_enable_coco_shim;
ALTER TABLE node_anchors DROP COLUMN session_additional_params_json;
ALTER TABLE node_anchors DROP COLUMN session_max_tokens;
ALTER TABLE node_anchors DROP COLUMN session_temperature;
ALTER TABLE node_anchors DROP COLUMN session_system_prompt;
