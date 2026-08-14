# Track Node Branch Origins Design

<!-- markdownlint-disable MD013 -->

## Context

See `proposal.md` for motivation. coco-mem currently stores mutable branches as `name -> head_id`; Nodes contain immutable DAG content and do not carry branch context. Many high-level operations know their branch, but the low-level `NodeStore::append` API does not. Session creation also appends its first session anchor before creating the branch. coco-console reads current branch names and heads from coco-mem and maintains an observed branch-head history for provider-context consistency, but that derived history is neither complete nor authoritative provenance.

The design must preserve two independent facts:

1. A node may have zero or one creation origin.
2. A node may currently be reachable from, or be the head of, zero to many branch refs.

## Goals / Non-Goals

**Goals:**

- Give every branch generation an identity that survives deletion and name reuse.
- Capture new node origins transactionally wherever the creating operation has branch context.
- Keep legacy and genuinely detached nodes valid with unknown origin.
- Make Console projection rebuildable from coco-mem and represent shared heads without duplicating topology.
- Preserve Node IDs, Node serialization, existing names, heads, session state, and job behavior during migration.

**Non-Goals:**

- Reconstruct origins for existing nodes.
- Record every branch-head movement or answer historical reachability at an arbitrary time.
- Assign exclusive branch ownership to shared ancestry.
- Change merge, shadow-parent, provider-context, or job-head semantics.
- Make a migrated database readable by older binaries.

## Decisions

### 1. Persist provenance outside immutable Nodes

Add two authoritative coco-mem tables:

```sql
CREATE TABLE branch_instances (
    instance_id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    created_at TEXT,
    deleted_at TEXT
);

CREATE INDEX branch_instances_name_idx
    ON branch_instances(name, created_at, instance_id);

CREATE TABLE node_origins (
    node_id TEXT PRIMARY KEY NOT NULL,
    branch_instance_id TEXT NOT NULL,
    FOREIGN KEY (node_id) REFERENCES nodes(id),
    FOREIGN KEY (branch_instance_id) REFERENCES branch_instances(instance_id)
);

CREATE INDEX node_origins_branch_instance_idx
    ON node_origins(branch_instance_id, node_id);
```

Rebuild `branches` with a non-null, unique `instance_id` foreign key while retaining `name` as the external live-ref key and allowing `head_id` to remain non-unique:

```sql
CREATE TABLE branches (
    name TEXT PRIMARY KEY NOT NULL,
    instance_id TEXT NOT NULL UNIQUE,
    head_id TEXT NOT NULL,
    FOREIGN KEY (instance_id) REFERENCES branch_instances(instance_id),
    FOREIGN KEY (head_id) REFERENCES nodes(id)
);
```

`created_at IS NULL` identifies a legacy instance whose true creation time is unknown. A new instance receives an opaque generated identifier and a creation timestamp. Deleting a branch sets `deleted_at` and removes the live `branches` row in one transaction; the instance is retained. Reusing the name inserts another instance.

`node_origins` is deliberately separate from Node metadata and normalized content tables. Adding provenance to `Node`, `NewNode`, or the Node hash would make identical DAG content depend on mutable execution context and would require rewriting every legacy Node ID.

**Alternatives considered:**

- A branch-head reflog was rejected for this capability because it adds a write for every head movement and still answers historical reachability rather than creation origin.
- A `branch_name` column on `nodes` was rejected because names can be reused and branch provenance is not intrinsic Node content.
- Inferring origin from current ancestry was rejected because shared ancestry, resets, deletion, and name reuse make the result ambiguous.

### 2. Keep detached append and add explicit branch-aware persistence

The existing detached append behavior remains valid and produces no `node_origins` row. New branch-aware store primitives resolve the active branch instance and write the Node plus origin in one SQLite transaction.

The implementation must cover these paths:

| Creation path | Origin behavior |
| --- | --- |
| Branch bootstrap and initial session anchor | New branch instance |
| Prompt job base and optional session patch | Job branch instance |
| Backend trace, terminal response, and failure nodes | Completion work-branch instance |
| Batch append plus head movement | Target branch instance for every new node |
| Rebase and handoff nodes | Rebased or handed-off branch instance |
| Skill invocation and skill session nodes | Invoking branch instance |
| Low-level detached `NodeStore::append` | Unknown |
| Move or fork a ref to an existing node | Preserve existing origin |

Session creation needs an atomic branch-bootstrap primitive because the current append-then-fork sequence cannot attribute the initial anchor and can expose or leave an invalid partial branch on failure. The primitive creates the instance, initial nodes and origins, live branch row, and initial session state in one transaction.

Public Node payload types and existing detached methods stay unchanged. The Rust storage traits gain explicit branch-aware operations; existing call sites are migrated only where branch context is authoritative. This is an additive behavioral API for callers, although third-party trait implementers must implement or deliberately decline the new provenance capability.

### 3. Treat origin as immutable and refs as many-to-one

An origin row is inserted only with a newly inserted node. Head movement, fork, merge, retry, and recovery never overwrite it. The unique `node_id` key enforces at most one origin.

The current branch relation remains independent:

```text
branch instance A --live ref--> head X
branch instance B --live ref--> head X
node X             --origin--> branch instance A
```

This means X was created on A while both A and B currently point to X. It is valid and requires no duplicate graph node or lane.

### 4. Extend bounded graph reads with optional origin data

Extend coco-mem's read-only graph projection records with optional origin data containing at least `instance_id`, historical `name`, and whether the instance is currently live. Load it by joining `nodes`, `node_origins`, and `branch_instances` in the existing batched graph-node queries. Do not add per-node lookups.

Current branch records include their instance IDs. Console groups them by visible head:

```text
head_id -> sorted [branch ref]
```

In All view, labels attach to the actual head. In Anchors view, non-anchor heads project to the nearest visible anchor or root while retaining per-label navigation to the actual head in All view. Multiple refs at one actual or projected head become multiple labels on one rendered node, including when distinct hidden heads share the same visible anchor.

### 5. Store origin in the disposable Console projection

The Console web-graph schema stores nullable origin instance ID and name alongside its projected node record, or in a one-to-one origin projection table. Either representation is acceptable if it remains rebuildable and indexed by node ID and origin instance ID.

The existing provider branch history may remain as an internal generation-consistency mechanism, but it must not be used or described as node provenance. A projection rebuild reads origins from coco-mem. Incremental catch-up receives origin in the same bounded source-node batches.

Graph API payloads expose optional origin metadata additively. Current branch refs populate the existing node-label concept. Rendering uses a stable style key derived from `branch_instance_id`; the historical name is presentation text. Unknown origins use the existing neutral node and edge style.

For an edge between differently originated nodes, the incoming primary segment uses the target node's origin style. Merge and shadow edges retain their semantic styles so provenance does not obscure relationship type.

### 6. Migrate legacy data without fabrication

The next coco-mem schema migration performs these operations atomically:

1. Create `branch_instances` and one legacy instance per current `branches` row.
2. Leave each legacy `created_at` null.
3. Rebuild `branches` with `instance_id`, preserving every name and head exactly.
4. Preserve and reconnect `sessions` without changing its state columns.
5. Create an empty `node_origins` table; do not backfill from ancestry or Console data.
6. Update Diesel schema and the current schema version.

After migration, new branch-aware writes produce origins even when their parents are legacy unknown nodes. A Console sidecar created before the upgrade can migrate its own projection schema in place; existing projected nodes remain unknown and newly ingested nodes carry origins. A full sidecar rebuild produces the same result from coco-mem.

Writable open remains the upgrade authority. Existing `open_read_only_or_upgrade_schema` users first perform the writable migration. Pure read-only open of an old schema continues to fail rather than serving a mixed contract.

The down migration reconstructs the original live `branches(name, head_id)` table and drops `node_origins` and `branch_instances`. It necessarily discards provenance. Production rollback after any provenance writes therefore requires a pre-migration database backup if that data must be retained; an older binary will otherwise reject the newer schema under the existing version guard.

### 7. Do not make Console layout proportional to branch-head changes

Storage overhead is one branch-instance row per branch generation and at most one origin row per branch-created node. Head-only movements write neither table. Batched node insertion writes origins in the same transaction and can use multi-row inserts.

Console work remains bounded by changed nodes and the small current branch snapshot. Branch labels require grouping current refs by head, while origin styling is a direct projected lookup. No recursive traversal is added to ordinary All-view node ingestion.

## Risks / Trade-offs

- **[Incomplete provenance at missed call sites]** A branch-known path may accidentally use detached append. → Centralize branch-aware node persistence, enumerate all production append call sites, and add integration tests for session, prompt, trace, failure, recovery, rebase, handoff, and skill flows.
- **[Legacy graph appears visually mixed]** Existing nodes remain neutral while new descendants gain branch styles. → Expose unknown explicitly and never present inferred reachability as origin.
- **[Branch-table rebuild can disturb session foreign keys]** SQLite table replacement interacts with `sessions.branch_name`. → Perform the migration in one transaction with foreign-key-safe table ordering and add up/down migration tests containing active, attached, paused, and shared-head branches.
- **[Name reuse is visually confusing]** Two historical instances can both display the same name. → Use instance ID as the style key and show a shortened instance ID plus lifecycle state in details.
- **[Origin colors can conflict with edge semantics]** Merge and shadow relationships already use semantic colors. → Apply origin styling only to node bodies and primary-parent segments; retain existing merge and shadow styling.
- **[Trait evolution affects alternate Store implementations]** New branch-aware operations require capability support. → Keep existing detached operations unchanged, use focused traits/default unsupported behavior where practical, and document the source-compatibility boundary separately from persisted-data compatibility.

## Migration Plan

1. Add and test the coco-mem migration, branch-instance lifecycle, origin persistence helpers, and bounded graph reads.
2. Convert atomic branch, job, session, completion, and skill creation paths to branch-aware writes.
3. Add the Console projection migration and ingest optional origins without changing rendering.
4. Populate current head labels, including multiple refs and Anchors projection.
5. Enable origin styles and details, then force/rehearse a disposable Console rebuild to verify parity.
6. Validate writable upgrade from the previous schema, current-schema read-only open, rejected old-schema pure read-only open, and destructive downgrade behavior.
7. Run relevant crate tests and `prek -a` before accepting implementation.

Operational rollback before step 1 commits is a binary rollback. After the coco-mem migration commits, restore the pre-migration database backup or run the explicit down migration with acceptance of provenance loss before using an older binary.
