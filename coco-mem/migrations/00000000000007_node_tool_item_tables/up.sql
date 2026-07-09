CREATE TABLE node_tool_uses (
    node_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    tool_use_id TEXT NOT NULL,
    name TEXT NOT NULL,
    input_json TEXT NOT NULL,
    PRIMARY KEY (node_id, ordinal),
    FOREIGN KEY (node_id) REFERENCES nodes(id)
);

CREATE INDEX node_tool_uses_tool_use_id_idx ON node_tool_uses(tool_use_id);

CREATE TABLE node_tool_results (
    node_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    tool_result_id TEXT NOT NULL,
    output TEXT NOT NULL,
    PRIMARY KEY (node_id, ordinal),
    FOREIGN KEY (node_id) REFERENCES nodes(id)
);

CREATE INDEX node_tool_results_tool_result_id_idx ON node_tool_results(tool_result_id);
