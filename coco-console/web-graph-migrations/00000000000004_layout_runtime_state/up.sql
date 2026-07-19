ALTER TABLE web_graph_node_placements
ADD COLUMN outgoing_edge_count BIGINT NOT NULL DEFAULT 0
CHECK (outgoing_edge_count >= 0);

UPDATE web_graph_node_placements AS placement
SET outgoing_edge_count = (
    SELECT COUNT(*)
    FROM web_graph_edge_routes AS route
    WHERE route.layout_kind = placement.layout_kind
      AND route.source_id = placement.node_id
);

CREATE INDEX web_graph_node_placements_column_idx
    ON web_graph_node_placements(layout_kind, x, y);
