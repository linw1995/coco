CREATE TABLE web_graph_node_spatial_items (
    spatial_id INTEGER PRIMARY KEY NOT NULL,
    layout_kind TEXT NOT NULL,
    node_id TEXT NOT NULL,
    UNIQUE (layout_kind, node_id),
    FOREIGN KEY (layout_kind, node_id)
        REFERENCES web_graph_node_placements(layout_kind, node_id) ON DELETE RESTRICT
);

CREATE TABLE web_graph_route_spatial_items (
    spatial_id INTEGER PRIMARY KEY NOT NULL,
    layout_kind TEXT NOT NULL,
    edge_kind TEXT NOT NULL,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    UNIQUE (layout_kind, edge_kind, source_id, target_id),
    FOREIGN KEY (layout_kind, edge_kind, source_id, target_id)
        REFERENCES web_graph_edge_routes(layout_kind, edge_kind, source_id, target_id)
        ON DELETE RESTRICT
);

CREATE VIRTUAL TABLE web_graph_node_spatial_index USING rtree_i32(
    spatial_id,
    min_x,
    max_x,
    min_y,
    max_y,
    +layout_kind
);

CREATE VIRTUAL TABLE web_graph_route_spatial_index USING rtree_i32(
    spatial_id,
    min_x,
    max_x,
    min_y,
    max_y,
    +layout_kind
);

INSERT INTO web_graph_node_spatial_items (layout_kind, node_id)
SELECT layout_kind, node_id
FROM web_graph_node_placements
ORDER BY layout_kind, node_id;

INSERT INTO web_graph_node_spatial_index (
    spatial_id,
    min_x,
    max_x,
    min_y,
    max_y,
    layout_kind
)
SELECT
    spatial.spatial_id,
    max(-2147483648, placement.x - 64),
    min(2147483647, placement.x + 64),
    max(-2147483648, placement.y - 24),
    min(2147483647, placement.y + 52),
    CAST(spatial.layout_kind AS BLOB)
FROM web_graph_node_spatial_items AS spatial
JOIN web_graph_node_placements AS placement
    ON placement.layout_kind = spatial.layout_kind
    AND placement.node_id = spatial.node_id;

INSERT INTO web_graph_route_spatial_items (
    layout_kind,
    edge_kind,
    source_id,
    target_id
)
SELECT layout_kind, edge_kind, source_id, target_id
FROM web_graph_edge_routes
ORDER BY layout_kind, edge_kind, source_id, target_id;

INSERT INTO web_graph_route_spatial_index (
    spatial_id,
    min_x,
    max_x,
    min_y,
    max_y,
    layout_kind
)
SELECT
    spatial.spatial_id,
    min(route.source_x, route.control_1_x, route.control_2_x, route.target_x),
    max(route.source_x, route.control_1_x, route.control_2_x, route.target_x),
    min(route.source_y, route.control_1_y, route.control_2_y, route.target_y),
    max(route.source_y, route.control_1_y, route.control_2_y, route.target_y),
    CAST(spatial.layout_kind AS BLOB)
FROM web_graph_route_spatial_items AS spatial
JOIN web_graph_edge_routes AS route
    ON route.layout_kind = spatial.layout_kind
    AND route.edge_kind = spatial.edge_kind
    AND route.source_id = spatial.source_id
    AND route.target_id = spatial.target_id;
