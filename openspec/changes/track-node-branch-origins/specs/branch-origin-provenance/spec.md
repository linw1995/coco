# Branch Origin Provenance Specification

<!-- markdownlint-disable MD013 -->

## Purpose

Preserve stable branch-generation identity and optional node creation provenance without treating mutable branch references as intrinsic Node data.

## ADDED Requirements

### Requirement: Stable branch instances

The store SHALL assign every newly created branch a stable, opaque branch instance identifier that remains addressable after the live branch is deleted. A branch name MAY be reused only by creating a different branch instance.

#### Scenario: Create a new branch

- **WHEN** a caller creates a branch with a currently unused name
- **THEN** the store creates one live branch reference and one branch instance with a non-null creation timestamp
- **AND** the live branch reference identifies that branch instance

#### Scenario: Delete and recreate a branch name

- **WHEN** a caller deletes a branch and later creates another branch with the same name
- **THEN** the deleted instance remains available to historical node-origin reads
- **AND** the recreated branch receives a different branch instance identifier

### Requirement: Optional immutable node origin

The store SHALL represent node creation provenance as an optional relation from a Node ID to one branch instance. The relation SHALL NOT participate in Node hashing or serialized Node content and SHALL NOT be changed by later branch-head movement.

#### Scenario: Append nodes on a branch

- **WHEN** one or more new nodes are appended through a branch-aware operation
- **THEN** every newly persisted node records the active branch instance as its origin in the same transaction

#### Scenario: Branch-aware append fails

- **WHEN** any node, origin, branch-head, job, or session-state write in a branch-aware operation fails
- **THEN** the transaction commits none of those writes

#### Scenario: Move a branch to an existing node

- **WHEN** a branch head is moved to a node that already exists
- **THEN** the node's existing origin remains unchanged
- **AND** no origin is created for an origin-less node

#### Scenario: Append a detached node

- **WHEN** a caller uses the existing detached append operation
- **THEN** the node is persisted without an origin

#### Scenario: Fork from existing history

- **WHEN** a new branch is forked from an existing node
- **THEN** no existing node origin is changed or backfilled

### Requirement: Atomic branch bootstrap

The store SHALL provide a branch-aware creation operation that can create a branch instance, its initial session node or nodes, the live branch reference, and session state atomically.

#### Scenario: Create a session branch

- **WHEN** a service creates a new session and its branch
- **THEN** the initial session anchor records the new branch instance as its origin
- **AND** no externally visible intermediate branch without a valid session is committed

#### Scenario: Branch bootstrap fails

- **WHEN** validation or persistence fails during branch bootstrap
- **THEN** neither the branch instance, live branch, session state, nodes, nor node origins are committed

### Requirement: Multiple live refs may share a head

The store SHALL continue to allow multiple live branch names and branch instances to reference the same head Node ID.

#### Scenario: Two branches share one head

- **WHEN** two live branches point to the same Node ID
- **THEN** reads return both branch references without changing or duplicating the node
- **AND** the node retains at most one creation origin

### Requirement: Legacy store migration

A writable open of a pre-provenance SQLite store SHALL migrate the store without changing existing Node IDs, branch names, branch heads, session states, jobs, or node contents.

#### Scenario: Migrate live legacy branches

- **WHEN** a writable process opens a supported legacy schema
- **THEN** the migration creates one active legacy branch instance for every live branch
- **AND** each legacy instance has an unknown creation timestamp
- **AND** each live branch keeps its existing name and head

#### Scenario: Preserve legacy node uncertainty

- **WHEN** a legacy store is migrated
- **THEN** every pre-migration node remains without an origin
- **AND** the system does not infer an origin from current reachability, session ancestry, or observed Console history

#### Scenario: Continue after migration

- **WHEN** new branch-aware nodes are created after migration
- **THEN** those new nodes receive origins even when their ancestors have unknown origins

#### Scenario: Read-only consumer encounters an old schema

- **WHEN** a read-only consumer opens a store that has not completed the required writable migration
- **THEN** it fails with the existing unsupported-schema behavior rather than exposing partial provenance

### Requirement: Compatibility boundary

Existing branch names, detached append behavior, Node serialization, and Node ID calculation SHALL remain compatible. The newer writable binary SHALL read and migrate supported older schemas, but an older binary is not required to open a store after its schema has been migrated.

#### Scenario: Read an existing node through the public model

- **WHEN** a caller reads a legacy or new node through the existing Node API
- **THEN** the returned Node payload has the same shape and identity semantics as before provenance tracking

#### Scenario: Attempt binary downgrade after migration

- **WHEN** an older binary encounters the newer schema version
- **THEN** the existing schema-version guard rejects the store
- **AND** operational rollback requires a pre-migration backup or an explicit schema downgrade that discards provenance
