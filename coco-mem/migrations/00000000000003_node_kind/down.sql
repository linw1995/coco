DROP INDEX nodes_anchor_kind_created_at_id_idx;
DROP INDEX nodes_kind_created_at_id_idx;

ALTER TABLE nodes DROP COLUMN anchor_kind;
ALTER TABLE nodes DROP COLUMN kind;
