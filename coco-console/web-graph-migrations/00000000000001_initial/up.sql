CREATE TABLE web_graph_state (
    id INTEGER PRIMARY KEY NOT NULL CHECK (id = 1),
    format_version INTEGER NOT NULL CHECK (format_version > 0),
    revision BIGINT NOT NULL CHECK (revision >= 0),
    source_version BIGINT NOT NULL CHECK (source_version >= 0)
);

CREATE TABLE web_graph_nodes (
    node_id TEXT PRIMARY KEY NOT NULL CHECK (length(node_id) > 0)
);

CREATE TABLE web_graph_edges (
    edge_kind TEXT NOT NULL CHECK (
        edge_kind IN ('primary_parent', 'merge_parent', 'shadow_parent')
    ),
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    PRIMARY KEY (edge_kind, source_id, target_id),
    FOREIGN KEY (source_id) REFERENCES web_graph_nodes(node_id) ON DELETE RESTRICT,
    FOREIGN KEY (target_id) REFERENCES web_graph_nodes(node_id) ON DELETE RESTRICT,
    CHECK (source_id <> target_id)
);

CREATE INDEX web_graph_edges_source_idx
    ON web_graph_edges(source_id);

CREATE INDEX web_graph_edges_target_idx
    ON web_graph_edges(target_id);

CREATE TABLE web_graph_layouts (
    layout_kind TEXT PRIMARY KEY NOT NULL CHECK (layout_kind IN ('anchors', 'all')),
    canvas_width INTEGER NOT NULL CHECK (canvas_width > 0),
    canvas_height INTEGER NOT NULL CHECK (canvas_height > 0)
);

CREATE TABLE web_graph_node_placements (
    layout_kind TEXT NOT NULL,
    node_id TEXT NOT NULL,
    x INTEGER NOT NULL,
    y INTEGER NOT NULL,
    PRIMARY KEY (layout_kind, node_id),
    FOREIGN KEY (layout_kind) REFERENCES web_graph_layouts(layout_kind) ON DELETE RESTRICT,
    FOREIGN KEY (node_id) REFERENCES web_graph_nodes(node_id) ON DELETE RESTRICT
);

CREATE INDEX web_graph_node_placements_node_idx
    ON web_graph_node_placements(node_id);

CREATE TABLE web_graph_edge_routes (
    layout_kind TEXT NOT NULL,
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
    PRIMARY KEY (layout_kind, edge_kind, source_id, target_id),
    FOREIGN KEY (layout_kind) REFERENCES web_graph_layouts(layout_kind) ON DELETE RESTRICT,
    FOREIGN KEY (edge_kind, source_id, target_id)
        REFERENCES web_graph_edges(edge_kind, source_id, target_id) ON DELETE RESTRICT,
    FOREIGN KEY (layout_kind, source_id)
        REFERENCES web_graph_node_placements(layout_kind, node_id) ON DELETE RESTRICT,
    FOREIGN KEY (layout_kind, target_id)
        REFERENCES web_graph_node_placements(layout_kind, node_id) ON DELETE RESTRICT
);

CREATE INDEX web_graph_edge_routes_source_idx
    ON web_graph_edge_routes(layout_kind, source_id);

CREATE INDEX web_graph_edge_routes_target_idx
    ON web_graph_edge_routes(layout_kind, target_id);
