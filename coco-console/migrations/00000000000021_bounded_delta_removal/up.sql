ALTER TABLE console_graph_build_changed_branches
    ADD COLUMN removal_refresh_id_cursor BIGINT NOT NULL DEFAULT -1;

ALTER TABLE console_graph_build_changed_branches
    ADD COLUMN removal_refresh_id_upper_bound BIGINT NOT NULL DEFAULT -1;

ALTER TABLE console_graph_build_changed_branches
    ADD COLUMN removal_bound_frozen INTEGER NOT NULL DEFAULT 0
        CHECK (removal_bound_frozen IN (0, 1));

UPDATE console_graph_build_changed_branches
SET removal_cursor = '',
    removal_refresh_id_cursor = -1,
    removal_refresh_id_upper_bound = -1,
    removal_bound_frozen = 0
WHERE removal_complete = 0;
