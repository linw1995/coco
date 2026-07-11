ALTER TABLE node_anchors ADD COLUMN session_role TEXT;
ALTER TABLE node_anchors ADD COLUMN provider_profile TEXT;
ALTER TABLE node_anchors ADD COLUMN provider TEXT;
ALTER TABLE node_anchors ADD COLUMN model TEXT;

UPDATE node_anchors AS anchor
SET session_role = session.role,
    provider_profile = session.provider_profile,
    provider = session.provider,
    model = session.model,
    prompt = session.prompt
FROM node_anchor_sessions AS session
WHERE anchor.node_id = session.node_id;

UPDATE node_anchors
SET kind_json = json_remove(
    kind_json,
    '$.Anchor.payload.Session.role',
    '$.Anchor.payload.Session.provider_profile',
    '$.Anchor.payload.Session.provider',
    '$.Anchor.payload.Session.model',
    '$.Anchor.payload.Session.prompt'
)
WHERE kind = 'session';

DROP TABLE node_anchor_session_tools;
DROP TABLE node_anchor_sessions;
