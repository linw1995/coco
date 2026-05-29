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
  --exclude "src/client.rs"
  --threshold "${crap_threshold}"
)
crap_allow_args=(
  --allow "render_node_content"
  --allow "abort_channel_task"
)

if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
  cargo-crap "${crap_args[@]}" --format github
fi

cargo-crap "${crap_args[@]}" --format markdown --output "${CARGO_TARGET_DIR}/result/crap.md"
cargo-crap "${crap_args[@]}" "${crap_allow_args[@]}" --fail-above --summary
