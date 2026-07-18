CREATE TABLE console_graph_materialization_shells (
    generation BIGINT NOT NULL,
    mode TEXT NOT NULL,
    node_count BIGINT NOT NULL CHECK (node_count >= 0),
    edge_count BIGINT NOT NULL CHECK (edge_count >= 0),
    PRIMARY KEY (generation, mode),
    FOREIGN KEY (generation, mode)
        REFERENCES console_graph_materializations(generation, mode)
        ON DELETE CASCADE
);

CREATE TABLE console_graph_materialization_time_ticks (
    generation BIGINT NOT NULL,
    mode TEXT NOT NULL,
    sample_index INTEGER NOT NULL CHECK (sample_index >= 0),
    node_target TEXT NOT NULL,
    x INTEGER NOT NULL,
    y INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    created_at_ns BIGINT NOT NULL,
    PRIMARY KEY (generation, mode, sample_index),
    FOREIGN KEY (generation, mode)
        REFERENCES console_graph_materialization_shells(generation, mode)
        ON DELETE CASCADE
);

INSERT INTO console_graph_materialization_shells (
    generation,
    mode,
    node_count,
    edge_count
)
SELECT materializations.generation,
       materializations.mode,
       (
           SELECT COUNT(*)
           FROM console_graph_node_locations AS nodes
           WHERE nodes.generation = materializations.generation
             AND nodes.mode = materializations.mode
       ),
       (
           SELECT COUNT(*)
           FROM console_graph_edge_routes AS edges
           WHERE edges.generation = materializations.generation
             AND edges.mode = materializations.mode
       )
FROM console_graph_materializations AS materializations;

WITH ordered_nodes AS (
    SELECT nodes.generation,
           nodes.mode,
           nodes.node_target,
           nodes.x,
           nodes.y,
           nodes.created_at,
           nodes.created_at_ns,
           ROW_NUMBER() OVER (
               PARTITION BY nodes.generation, nodes.mode
               ORDER BY nodes.created_at_ns, nodes.node_id
           ) AS row_number,
           COUNT(*) OVER (
               PARTITION BY nodes.generation, nodes.mode
           ) AS node_count
    FROM console_graph_node_locations AS nodes
    INNER JOIN console_graph_materializations AS materializations
        ON materializations.generation = nodes.generation
       AND materializations.mode = nodes.mode
)
INSERT INTO console_graph_materialization_time_ticks (
    generation,
    mode,
    sample_index,
    node_target,
    x,
    y,
    created_at,
    created_at_ns
)
SELECT generation,
       mode,
       CASE
           WHEN node_count <= 256 THEN row_number - 1
           ELSE (row_number - 1) * 255 / (node_count - 1)
       END,
       node_target,
       x,
       y,
       created_at,
       created_at_ns
FROM ordered_nodes
WHERE node_count <= 256
   OR row_number = 1
   OR ((row_number - 1) * 255 / (node_count - 1))
      > ((row_number - 2) * 255 / (node_count - 1));
