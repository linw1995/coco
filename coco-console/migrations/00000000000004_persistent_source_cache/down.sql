DROP INDEX IF EXISTS console_graph_source_refresh_queue_pending_idx;
DROP TABLE IF EXISTS console_graph_source_refresh_queue;

DROP INDEX IF EXISTS console_graph_source_branch_nodes_node_idx;
DROP TABLE IF EXISTS console_graph_source_branch_nodes;

DROP TABLE IF EXISTS console_graph_source_branches;

DROP INDEX IF EXISTS console_graph_source_node_relations_child_idx;
DROP TABLE IF EXISTS console_graph_source_node_relations;

DROP INDEX IF EXISTS console_graph_source_nodes_parent_idx;
DROP TABLE IF EXISTS console_graph_source_nodes;

DROP TABLE IF EXISTS console_graph_source_state;
