UPDATE node_anchors
SET kind_json = json_remove(kind_json, '$.Anchor.payload.Prompt.prompt')
WHERE kind = 'prompt';

DROP TABLE node_anchor_prompt_attachments;
