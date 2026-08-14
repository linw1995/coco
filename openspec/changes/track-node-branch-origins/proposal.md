## Why

The current graph can identify live branch heads, but it cannot reliably explain which branch execution created an older node after heads move, branches are deleted, or names are reused. This makes the built-in `day` branch indistinguishable from ordinary history in coco-console and makes the Console's observed head snapshots an unreliable provenance source.

## What Changes

- Persist stable branch generations as `branch_instances`, independently from mutable branch heads and reusable branch names.
- Persist optional node creation provenance as `node_origins` without changing immutable Node payloads or Node IDs.
- Add branch-aware, transactionally atomic node creation paths while preserving detached node creation with unknown origin.
- Project origin information into coco-console so historical paths can be styled by origin while current branch names remain badges on head nodes.
- Group every live branch reference that points to the same head on one graph node; do not duplicate nodes or structural lanes.
- Migrate existing stores by creating one legacy instance for every live branch and leaving all pre-migration node origins unknown. Existing Node IDs, branch names, heads, session states, jobs, and CLI behavior remain unchanged.
- Keep full branch-head time travel and reflog persistence out of scope.

## Capabilities

### New Capabilities

- `branch-origin-provenance`: Stable branch instances, optional node origin persistence, branch-aware mutation semantics, and legacy-store compatibility.
- `console-branch-visualization`: Graph projection and rendering of node origins together with multiple current branch refs at one head.

### Modified Capabilities

None.

## Impact

- `coco-mem`: SQLite schema and migration runner, branch/node/job persistence paths, read-only graph records, and store APIs.
- `coco-llm`, `coco-core`, and `coco-cli`: branch-aware node creation at session, prompt, skill, trace, and recovery call sites.
- `coco-console`: disposable graph projection schema, catch-up/rebuild logic, graph API payloads, and WASM/SSR rendering.
- Compatibility: writable opens migrate legacy stores forward; old binaries cannot open the newer schema after migration, consistent with the existing schema-version policy.
