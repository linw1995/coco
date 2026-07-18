UPDATE console_graph_build_runs
SET dag_init_phase = 'full_reset',
    dag_init_row_cursor = 0,
    dag_init_text_cursor = '',
    dag_init_text_cursor_secondary = '',
    dag_init_counter = 0
WHERE dag_initialized = 0
  AND dag_init_phase IN (
      'manifest_copy',
      'manifest_refresh_reset',
      'manifest_branch_reset',
      'manifest_refresh_copy'
  );

DROP INDEX console_graph_source_refresh_runs_manifest_idx;
DROP INDEX console_graph_source_branch_names_revision_idx;
DROP TABLE console_graph_source_branch_names;
DROP INDEX console_graph_source_branch_history_revision_idx;
DROP TABLE console_graph_source_branch_history;
