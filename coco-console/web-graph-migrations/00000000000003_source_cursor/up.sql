ALTER TABLE web_graph_state
    ADD COLUMN source_cursor_row_id BIGINT
    CHECK (
        source_cursor_row_id IS NULL
        OR source_cursor_row_id > 0
    );

ALTER TABLE web_graph_state
    ADD COLUMN source_cursor_node_id TEXT
    CHECK (
        source_cursor_node_id IS NULL
        OR length(source_cursor_node_id) > 0
    );
