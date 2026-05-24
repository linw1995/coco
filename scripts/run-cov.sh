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

if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
  cargo-crap --workspace --lcov "${CARGO_TARGET_DIR}/result/lcov.info" --format github
fi

cargo-crap --workspace --lcov "${CARGO_TARGET_DIR}/result/lcov.info" --format markdown --output "${CARGO_TARGET_DIR}/result/crap.md"
