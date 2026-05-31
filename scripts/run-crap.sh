#!/usr/bin/env bash
set -euxo pipefail

workspace_root="$(pwd -P)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${workspace_root}/target/coverage}"

echo "Generating CRAP metric report..."

crap_threshold="${CRAP_THRESHOLD:-30}"
crap_args=(
  --workspace
  --lcov "${CARGO_TARGET_DIR}/result/lcov.info"
  --exclude "build.rs"
  --threshold "${crap_threshold}"
)
crap_allow_args=(
  --allow "VirtualGraph::upsert_node"
  --allow "render_next_viewport_patch"
  --allow "VirtualGraph::apply_diff"
  --allow "ViewportState::load"
  --allow "VirtualGraph::new"
  --allow "VirtualGraph::apply_full"
  --allow "refresh_on_graph_version"
  --allow "VirtualGraph::upsert_edge"
  --allow "VirtualGraph::primary_edge_element"
  --allow "VirtualGraph::routed_edge_element"
  --allow "render_full_viewport"
  --allow "drain_viewport_patches"
)

if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
  cargo-crap "${crap_args[@]}" "${crap_allow_args[@]}" --format github
fi

cargo-crap "${crap_args[@]}" "${crap_allow_args[@]}" --format markdown --output "${CARGO_TARGET_DIR}/result/crap.md"
cargo-crap "${crap_args[@]}" "${crap_allow_args[@]}" --fail-above --summary
