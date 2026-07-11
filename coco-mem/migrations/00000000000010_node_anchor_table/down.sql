CREATE TEMP TABLE node_anchor_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO node_anchor_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM nodes
        WHERE kind = 'anchor'
          AND NOT EXISTS (
              SELECT 1
              FROM node_anchors
              WHERE node_anchors.node_id = nodes.id
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchors
        LEFT JOIN nodes ON nodes.id = node_anchors.node_id
        WHERE nodes.id IS NULL OR nodes.kind <> 'anchor'
    ) THEN 1
    ELSE 0
END;

DROP TABLE node_anchor_migration_guard;

ALTER TABLE nodes ADD COLUMN anchor_kind TEXT;
ALTER TABLE nodes ADD COLUMN anchor_session_role TEXT;
ALTER TABLE nodes ADD COLUMN anchor_provider_profile TEXT;
ALTER TABLE nodes ADD COLUMN anchor_provider TEXT;
ALTER TABLE nodes ADD COLUMN anchor_model TEXT;
ALTER TABLE nodes ADD COLUMN anchor_prompt TEXT;
ALTER TABLE nodes ADD COLUMN anchor_skill_name TEXT;
ALTER TABLE nodes ADD COLUMN anchor_skill_invocation_mode TEXT;

UPDATE nodes
SET anchor_kind = (
        SELECT kind FROM node_anchors WHERE node_id = nodes.id
    ),
    anchor_session_role = (
        SELECT session_role FROM node_anchors WHERE node_id = nodes.id
    ),
    anchor_provider_profile = (
        SELECT provider_profile FROM node_anchors WHERE node_id = nodes.id
    ),
    anchor_provider = (
        SELECT provider FROM node_anchors WHERE node_id = nodes.id
    ),
    anchor_model = (
        SELECT model FROM node_anchors WHERE node_id = nodes.id
    ),
    anchor_prompt = (
        SELECT prompt FROM node_anchors WHERE node_id = nodes.id
    ),
    anchor_skill_name = (
        SELECT skill_name FROM node_anchors WHERE node_id = nodes.id
    ),
    anchor_skill_invocation_mode = (
        SELECT skill_invocation_mode FROM node_anchors WHERE node_id = nodes.id
    ),
    kind_json = (
        SELECT kind_json FROM node_anchors WHERE node_id = nodes.id
    )
WHERE kind = 'anchor';

CREATE INDEX nodes_anchor_kind_created_at_id_idx ON nodes(anchor_kind, created_at, id);

DROP INDEX node_anchors_kind_idx;
DROP TABLE node_anchors;
