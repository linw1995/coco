# Console Branch Visualization Specification

<!-- markdownlint-disable MD013 -->

## Purpose

Render current branch references and historical node creation origins as separate graph concepts, including shared heads and legacy unknown provenance.

## ADDED Requirements

### Requirement: Current branch refs label graph heads

The graph SHALL render branch names as labels on their current head nodes rather than as an exclusive property of every reachable node.

#### Scenario: One branch points to a head

- **WHEN** a visible node is the current head of one branch
- **THEN** the node displays that branch name as a head label

#### Scenario: Multiple branches point to one head

- **WHEN** a visible node is the current head of multiple branches
- **THEN** the node displays all branch labels in deterministic name order
- **AND** the graph renders one node and one structural path rather than one copy per branch

#### Scenario: Head is hidden in Anchors view

- **WHEN** a branch head is not an anchor and is hidden by Anchors view
- **THEN** its label is projected to the nearest visible anchor or root
- **AND** navigation from the label can reach the actual head in All view

#### Scenario: Distinct hidden heads share one visible anchor

- **GIVEN** multiple branches have distinct non-anchor heads beneath the same visible anchor
- **WHEN** the graph is rendered in Anchors view
- **THEN** all branch labels are displayed on that single anchor node
- **AND** each label navigates to its own actual head in All view

### Requirement: Node origins style historical paths

The graph SHALL use persisted node origin information to style historical nodes and their incoming primary path segments without changing DAG topology.

#### Scenario: Render nodes created by day

- **WHEN** visible nodes have a persisted origin whose branch-instance name is `day`
- **THEN** those nodes and their incoming primary path segments share a stable visual identity distinct from unrelated origins
- **AND** the graph does not require `day` to remain the only live ref reaching those nodes

#### Scenario: Render a deleted branch instance

- **WHEN** visible nodes reference an origin branch instance whose live branch was deleted
- **THEN** the historical origin styling and origin name remain available
- **AND** no current head label is shown for the deleted instance

#### Scenario: Reuse a branch name

- **WHEN** visible nodes originate from two branch instances that reused the same name
- **THEN** the graph keeps their instance identities distinct
- **AND** detail views expose enough instance identity to disambiguate them

### Requirement: Unknown origin remains explicit

The graph SHALL render nodes without persisted provenance using a neutral unknown-origin style and SHALL NOT infer a creation branch from current branch reachability.

#### Scenario: Render a legacy node

- **WHEN** a pre-migration node has no origin
- **THEN** the graph renders it successfully with neutral styling
- **AND** details identify its origin as unknown

#### Scenario: Current branches reach an unknown node

- **WHEN** one or more current branch heads can reach an origin-less node
- **THEN** the graph may show those branches as current reachability information
- **AND** it does not present any of them as the node's creation origin

### Requirement: Disposable projection is rebuildable

coco-console SHALL treat coco-mem branch instances and node origins as authoritative and its own branch and origin tables as disposable projections.

#### Scenario: Rebuild the Console graph database

- **WHEN** the derived Console graph database is absent or rebuilt
- **THEN** current branch labels and every persisted node origin are reconstructed from coco-mem
- **AND** legacy unknown origins remain unknown

#### Scenario: Incrementally ingest new nodes

- **WHEN** coco-mem appends branch-originated and detached nodes after the Console cursor
- **THEN** Console ingests their optional origins in the existing bounded node batches
- **AND** origin loading does not issue one source query per node

### Requirement: Origin and head labels remain independent

The graph SHALL preserve the distinction between where a node was created and which branches currently reference it.

#### Scenario: A different branch moves to a day-origin node

- **WHEN** `main` moves to a node whose persisted origin is `day`
- **THEN** the node retains day-origin styling
- **AND** the node displays `main` as a current head label

#### Scenario: Origin branch advances beyond a historical node

- **WHEN** the branch that created a node advances to a newer head
- **THEN** the historical node retains its origin styling
- **AND** the branch head label moves to the newer head
