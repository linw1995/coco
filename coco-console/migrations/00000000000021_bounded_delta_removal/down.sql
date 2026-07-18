ALTER TABLE console_graph_build_changed_branches
    DROP COLUMN removal_bound_frozen;

ALTER TABLE console_graph_build_changed_branches
    DROP COLUMN removal_refresh_id_upper_bound;

ALTER TABLE console_graph_build_changed_branches
    DROP COLUMN removal_refresh_id_cursor;
