DROP TABLE IF EXISTS console_graph_edge_routes;
DROP TABLE IF EXISTS console_graph_node_locations;
DROP TABLE IF EXISTS console_graph_materializations;

CREATE TABLE console_graph_materializations (
    mode TEXT PRIMARY KEY NOT NULL,
    source_version INTEGER NOT NULL,
    coordinate_space TEXT NOT NULL,
    world_min_x INTEGER NOT NULL,
    world_min_y INTEGER NOT NULL,
    world_max_x INTEGER NOT NULL,
    world_max_y INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE console_graph_node_locations (
    mode TEXT NOT NULL,
    node_id TEXT NOT NULL,
    node_key TEXT NOT NULL,
    node_target TEXT NOT NULL,
    short_id TEXT NOT NULL,
    node_kind TEXT NOT NULL,
    summary TEXT NOT NULL,
    labels_json TEXT NOT NULL,
    rank INTEGER NOT NULL,
    sort_order INTEGER NOT NULL,
    x INTEGER NOT NULL,
    y INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    created_at_ns BIGINT NOT NULL,
    min_x INTEGER NOT NULL,
    min_y INTEGER NOT NULL,
    max_x INTEGER NOT NULL,
    max_y INTEGER NOT NULL,
    PRIMARY KEY (mode, node_id),
    UNIQUE (mode, node_key)
);

CREATE INDEX console_graph_node_locations_viewport_idx
    ON console_graph_node_locations(mode, min_x, min_y, max_x, max_y);
CREATE INDEX console_graph_node_locations_rank_idx
    ON console_graph_node_locations(mode, rank, sort_order);
CREATE INDEX console_graph_node_locations_time_idx
    ON console_graph_node_locations(mode, created_at_ns, node_id);

CREATE TABLE console_graph_edge_routes (
    mode TEXT NOT NULL,
    edge_key TEXT NOT NULL,
    edge_kind TEXT NOT NULL,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    source_x INTEGER NOT NULL,
    source_y INTEGER NOT NULL,
    control_1_x INTEGER NOT NULL,
    control_1_y INTEGER NOT NULL,
    control_2_x INTEGER NOT NULL,
    control_2_y INTEGER NOT NULL,
    target_x INTEGER NOT NULL,
    target_y INTEGER NOT NULL,
    min_x INTEGER NOT NULL,
    min_y INTEGER NOT NULL,
    max_x INTEGER NOT NULL,
    max_y INTEGER NOT NULL,
    PRIMARY KEY (mode, edge_key)
);

CREATE INDEX console_graph_edge_routes_viewport_idx
    ON console_graph_edge_routes(mode, min_x, min_y, max_x, max_y);
CREATE INDEX console_graph_edge_routes_target_idx
    ON console_graph_edge_routes(mode, target_id, edge_kind);
