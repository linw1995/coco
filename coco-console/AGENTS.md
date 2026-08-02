# coco-console Guidelines

## Web Graph Rebuilds

- Treat `host::web_graph_store::LAYOUT_VERSION` as the algorithm version for
  all source-derived web graph projections, not only visual layout data.
- Increment `LAYOUT_VERSION` whenever a change can alter projections already
  materialized from source nodes, including topology, layout, provider context,
  tool-use, or exec-session projection logic. A schema migration alone does not
  trigger source replay.
- Add a rebuild regression test for each version increment. The test should
  persist the previous version, reopen the runtime, verify that the source
  cursor is reset, run catch-up, and assert that historical source nodes are
  projected with the new algorithm.
- Keep durable event history, such as provider branch history, outside the
  projection reset unless the history schema or semantics explicitly require a
  migration.

## Provider Context Projection

- Keep one provider-context mapping row per source node. Do not materialize a
  full context snapshot for every leaf because linear growth would create
  quadratic storage.
- Reuse a context id along a linear segment. When a node gains multiple primary
  children, assign each outgoing segment a distinct context id and reassign any
  previously projected suffix in place.
- Preserve both `previous_node_id` and `previous_context_id`: the former restores
  the exact provider node path, while the latter records context-level lineage
  across forks.
- Preserve `source_parent_node_id` separately for structural suffix updates.
  Provider paths may intentionally skip launcher nodes, so `previous_node_id`
  must not be used as the source-tree parent relation.
