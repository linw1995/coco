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
  --allow "handle_connection"
  --allow "RigBackend::step"
  --allow "main"
  --allow "Persistence::load"
  --allow "RunnerCli::into_cli"
  --allow "run_daemon_command"
  --allow "PromptJobMessageQueueWorker::handle_prompt_request_queue_head"
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
)

if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
  cargo-crap "${crap_args[@]}" --format github
fi

cargo-crap "${crap_args[@]}" --format markdown --output "${CARGO_TARGET_DIR}/result/crap.md"
cargo-crap "${crap_args[@]}" "${crap_allow_args[@]}" --fail-above --summary
