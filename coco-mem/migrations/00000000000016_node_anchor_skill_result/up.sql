CREATE TEMP TABLE node_anchor_skill_result_migration_guard (
    complete INTEGER NOT NULL CHECK (complete = 1)
);

INSERT INTO node_anchor_skill_result_migration_guard (complete)
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1
        FROM node_anchors
        WHERE kind = 'skill_result'
          AND (
              skill_name IS NULL
              OR json_valid(kind_json) = 0
              OR json_type(kind_json, '$.Anchor') <> 'object'
              OR json_type(kind_json, '$.Anchor.merge_parents') <> 'array'
              OR json_type(kind_json, '$.Anchor.payload') <> 'object'
              OR json_type(kind_json, '$.Anchor.payload.SkillResult') <> 'object'
              OR json_type(kind_json, '$.Anchor.payload.SkillResult.output') <> 'text'
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_anchors AS anchor
        JOIN json_each(anchor.kind_json, '$.Anchor.merge_parents') AS merge_parent
        WHERE anchor.kind = 'skill_result'
          AND (
              json_extract(merge_parent.value, '$.kind') NOT IN ('merge', 'shadow')
              OR json_type(merge_parent.value, '$.node_id') <> 'text'
              OR NOT EXISTS (
                  SELECT 1
                  FROM node_relations AS relation
                  WHERE relation.child_node_id = anchor.node_id
                    AND relation.parent_node_id = json_extract(merge_parent.value, '$.node_id')
                    AND relation.kind = json_extract(merge_parent.value, '$.kind')
                    AND relation.ordinal = CAST(merge_parent.key AS INTEGER)
              )
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM node_relations AS relation
        JOIN node_anchors AS anchor ON anchor.node_id = relation.child_node_id
        WHERE anchor.kind = 'skill_result'
          AND relation.kind IN ('merge', 'shadow')
          AND NOT EXISTS (
              SELECT 1
              FROM json_each(anchor.kind_json, '$.Anchor.merge_parents') AS merge_parent
              WHERE CAST(merge_parent.key AS INTEGER) = relation.ordinal
                AND json_extract(merge_parent.value, '$.kind') = relation.kind
                AND json_extract(merge_parent.value, '$.node_id') = relation.parent_node_id
          )
    )
    THEN 1
    ELSE 0
END;

DROP TABLE node_anchor_skill_result_migration_guard;

ALTER TABLE node_anchors ADD COLUMN skill_result_output TEXT;

UPDATE node_anchors
SET skill_result_output = json_extract(kind_json, '$.Anchor.payload.SkillResult.output')
WHERE kind = 'skill_result';
