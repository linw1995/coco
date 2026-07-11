UPDATE node_anchors AS anchor
SET prompt = payload.prompt
FROM node_anchor_prompts AS payload
WHERE anchor.node_id = payload.node_id;

UPDATE node_anchors
SET kind_json = json_remove(kind_json, '$.Anchor.payload.Prompt.prompt')
WHERE kind = 'prompt';

DROP TABLE node_anchor_prompt_attachments;
DROP TABLE node_anchor_prompts;
