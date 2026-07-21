ALTER TABLE web_graph_state
ADD COLUMN layout_version INTEGER NOT NULL DEFAULT 0
CHECK (layout_version >= 0);
