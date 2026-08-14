## 1. Authoritative Provenance Schema

- [x] 1.1 Add the next coco-mem SQLite migration for `branch_instances`, `node_origins`, and live `branches.instance_id`, including a provenance-discarding down migration.
- [x] 1.2 Update Diesel schema and add focused storage types for branch instances and optional node origins without changing `Node` or its hash payload.
- [x] 1.3 Implement branch-instance creation, deletion retention, same-name recreation, and current branch reads that preserve multiple refs at one head.
- [x] 1.4 Implement transactional single-node and batch origin persistence with indexed, bounded graph-read joins.
- [x] 1.5 Add migration tests covering legacy active, attached, paused, shared-head, and same-name-after-delete data plus up/down schema behavior.

## 2. Branch-Aware Node Creation

- [x] 2.1 Add explicit store operations for detached append, branch-aware append, and atomic branch bootstrap while preserving existing detached behavior.
- [ ] 2.2 Change session creation to bootstrap its branch instance, initial session anchor, live ref, and session state atomically.
- [x] 2.3 Record origins for prompt job bases, optional session patches, and every node created by branch batch/head-update transactions.
- [ ] 2.4 Propagate authoritative branch context through backend trace appenders, terminal response and failure persistence, retries, and recovery execution.
- [ ] 2.5 Record origins for rebase, handoff, skill invocation, and skill-session creation paths.
- [ ] 2.6 Add integration tests proving detached nodes remain unknown, fork and head movement do not rewrite origins, failed transactions leave no partial provenance, and all production branch-aware paths assign origins.

## 3. Console Projection and API

- [ ] 3.1 Add a disposable web-graph migration and projection fields or tables for optional origin instance identity and historical branch name.
- [ ] 3.2 Extend bounded coco-mem graph reads and Console catch-up/rebuild to ingest origins without per-node source queries.
- [ ] 3.3 Project current branch refs onto actual heads in All view and nearest visible anchors or root in Anchors view, grouping shared-head labels deterministically.
- [ ] 3.4 Extend graph API payloads additively with optional origin details while retaining current Node and edge topology.
- [ ] 3.5 Render stable origin styles for nodes and primary-parent segments in SSR and WASM, retain merge/shadow semantics, and render unknown origins neutrally.
- [ ] 3.6 Add Console tests for day-origin history, deleted and recreated branch instances, shared heads, current refs differing from origin, legacy unknown nodes, incremental catch-up, and full projection rebuild parity.

## 4. Compatibility and Performance Validation

- [ ] 4.1 Verify writable upgrade from the previous coco-mem schema preserves Node IDs, branch heads, session states, jobs, and CLI-visible behavior while leaving legacy origins unknown.
- [ ] 4.2 Verify current-schema read-only opens succeed, pure read-only opens of the previous schema retain existing failure behavior, and downgrade expectations are documented by tests.
- [ ] 4.3 Verify storage growth is limited to one row per branch generation and at most one row per branch-originated node, with no writes on head-only movement.
- [ ] 4.4 Run `cargo test -p coco-mem`.
- [ ] 4.5 Run `cargo test -p coco-llm -p coco-core -p coco-cli`.
- [ ] 4.6 Run `cargo test -p coco-console`.
- [ ] 4.7 Run `prek -a` and resolve every reported issue.
