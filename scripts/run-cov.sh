#!/usr/bin/env bash
set -euxo pipefail

export RUST_LOG="${RUST_LOG:-debug}"
export CARGO_INCREMENTAL=0
export RUSTFLAGS="-Cinstrument-coverage -Ccodegen-units=1 -Copt-level=0 -Clink-dead-code"
workspace_root="$(pwd -P)"
export CARGO_TARGET_DIR="${workspace_root}/target/coverage"
export LLVM_PROFILE_FILE="${CARGO_TARGET_DIR}/data/coco-%p-%m.profraw"

cargo clean --target-dir "${CARGO_TARGET_DIR}"
rm -rf "${CARGO_TARGET_DIR}/data/" "${CARGO_TARGET_DIR}/result/"
mkdir -p "${CARGO_TARGET_DIR}/data/" "${CARGO_TARGET_DIR}/result/"

cargo nextest run --workspace "$@"

echo "Generating code coverage report..."

grcov "${CARGO_TARGET_DIR}/data" \
  --llvm \
  --branch \
  --source-dir "${workspace_root}" \
  --ignore-not-existing \
  --ignore '../*' \
  --ignore '/*' \
  --binary-path "${CARGO_TARGET_DIR}/debug/deps" \
  --output-types html,cobertura,lcov,markdown \
  --output-path "${CARGO_TARGET_DIR}/result/"

cp "${CARGO_TARGET_DIR}/result/lcov" "${CARGO_TARGET_DIR}/result/lcov.info"
tail -n 1 "${CARGO_TARGET_DIR}/result/markdown.md"

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
  --allow "handle_connection"
  --allow "RigBackend::step"
  --allow "main"
  --allow "Persistence::load"
  --allow "RunnerCli::into_cli"
  --allow "run_daemon_command"
  --allow "PromptJobMessageQueueWorker::handle_prompt_request_queue_head"
  --allow "apply_forwarded_defaults"
  --allow "JsonValueKind::fmt"
  --allow "forward_to_socket"
  --allow "collect_visible_skill_invocation_subtrees"
  --allow "run_session_command"
  --allow "render_node_content"
  --allow "command_name"
  --allow "abort_channel_task"
  --allow "ForwardSocketError::fmt"
  --allow "read_request"
  --allow "merge_json_value"
  --allow "configure_completion_request_builder"
  --allow "LlmService::run_locked"
  --allow "start_coco_cli_runtime_server"
  --allow "render_node_show_text"
  --allow "render_graph_connector_row"
)

if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
  cargo-crap "${crap_args[@]}" --format github
fi

cargo-crap "${crap_args[@]}" --format markdown --output "${CARGO_TARGET_DIR}/result/crap.md"
cargo-crap "${crap_args[@]}" "${crap_allow_args[@]}" --fail-above --summary
