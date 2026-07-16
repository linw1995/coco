CREATE TABLE console_graph_materializations_v2 (
    mode TEXT PRIMARY KEY NOT NULL,
    source_version INTEGER NOT NULL,
    coordinate_space TEXT NOT NULL,
    world_min_x INTEGER NOT NULL,
    world_min_y INTEGER NOT NULL,
    world_max_x INTEGER NOT NULL,
    world_max_y INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

INSERT INTO console_graph_materializations_v2
SELECT
    mode,
    source_version,
    coordinate_space,
    world_min_x,
    world_min_y,
    world_max_x,
    world_max_y,
    updated_at
FROM console_graph_materializations
WHERE generation = (
    SELECT active_generation
    FROM console_graph_generation_state
    WHERE id = 1
);

CREATE TABLE console_graph_node_locations_v2 (
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

INSERT INTO console_graph_node_locations_v2
SELECT
    mode,
    node_id,
    node_key,
    node_target,
    short_id,
    node_kind,
    summary,
    labels_json,
    rank,
    sort_order,
    x,
    y,
    created_at,
    created_at_ns,
    min_x,
    min_y,
    max_x,
    max_y
FROM console_graph_node_locations
WHERE generation = (
    SELECT active_generation
    FROM console_graph_generation_state
    WHERE id = 1
);

CREATE TABLE console_graph_edge_routes_v2 (
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

INSERT INTO console_graph_edge_routes_v2
SELECT
    mode,
    edge_key,
    edge_kind,
    source_id,
    target_id,
    source_x,
    source_y,
    control_1_x,
    control_1_y,
    control_2_x,
    control_2_y,
    target_x,
    target_y,
    min_x,
    min_y,
    max_x,
    max_y
FROM console_graph_edge_routes
WHERE generation = (
    SELECT active_generation
    FROM console_graph_generation_state
    WHERE id = 1
);

DROP TABLE console_graph_edge_routes;
DROP TABLE console_graph_node_locations;
DROP TABLE console_graph_materializations;
DROP TABLE console_graph_generation_state;

ALTER TABLE console_graph_materializations_v2 RENAME TO console_graph_materializations;
ALTER TABLE console_graph_node_locations_v2 RENAME TO console_graph_node_locations;
ALTER TABLE console_graph_edge_routes_v2 RENAME TO console_graph_edge_routes;

CREATE INDEX console_graph_node_locations_viewport_idx
    ON console_graph_node_locations(mode, min_x, min_y, max_x, max_y);
CREATE INDEX console_graph_node_locations_rank_idx
    ON console_graph_node_locations(mode, rank, sort_order);
CREATE INDEX console_graph_node_locations_time_idx
    ON console_graph_node_locations(mode, created_at_ns, node_id);
CREATE INDEX console_graph_edge_routes_viewport_idx
    ON console_graph_edge_routes(mode, min_x, min_y, max_x, max_y);
CREATE INDEX console_graph_edge_routes_target_idx
    ON console_graph_edge_routes(mode, target_id, edge_kind);
