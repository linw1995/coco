CREATE TABLE console_graph_source_branch_history (
    branch_name TEXT NOT NULL,
    source_revision BIGINT NOT NULL CHECK (source_revision >= 0),
    contribution_generation BIGINT,
    head_id TEXT,
    state_json TEXT,
    removed INTEGER NOT NULL CHECK (removed IN (0, 1)),
    PRIMARY KEY (branch_name, source_revision),
    CHECK (
        (removed = 0
            AND contribution_generation IS NOT NULL
            AND head_id IS NOT NULL
            AND state_json IS NOT NULL)
        OR (removed = 1
            AND contribution_generation IS NULL
            AND head_id IS NULL
            AND state_json IS NULL)
    )
);

CREATE INDEX console_graph_source_branch_history_revision_idx
ON console_graph_source_branch_history(source_revision, branch_name);

CREATE TABLE console_graph_source_branch_names (
    branch_name TEXT PRIMARY KEY NOT NULL,
    first_source_revision BIGINT NOT NULL CHECK (first_source_revision >= 0)
);

CREATE INDEX console_graph_source_branch_names_revision_idx
ON console_graph_source_branch_names(first_source_revision, branch_name);

INSERT INTO console_graph_source_branch_history (
    branch_name,
    source_revision,
    contribution_generation,
    head_id,
    state_json,
    removed
)
SELECT
    branch.name,
    publication.source_revision,
    branch.contribution_generation,
    branch.head_id,
    branch.state_json,
    0
FROM console_graph_source_branches AS branch
INNER JOIN console_graph_source_branch_publications AS publication
    ON publication.branch_name = branch.name
   AND publication.target_contribution_generation = branch.contribution_generation;

INSERT INTO console_graph_source_branch_names (branch_name, first_source_revision)
SELECT branch_name, MIN(source_revision)
FROM console_graph_source_branch_history
GROUP BY branch_name;

CREATE INDEX console_graph_source_refresh_runs_manifest_idx
ON console_graph_source_refresh_runs(
    status,
    published_source_revision,
    refresh_id,
    branch_name,
    target_contribution_generation
);

UPDATE console_graph_build_runs
SET dag_init_phase = 'full_reset',
    dag_init_row_cursor = 0,
    dag_init_text_cursor = '',
    dag_init_text_cursor_secondary = '',
    dag_init_counter = 0
WHERE dag_initialized = 0
  AND dag_init_phase IN ('manifest_copy', 'manifest_reset');
