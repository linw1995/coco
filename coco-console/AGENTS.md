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
