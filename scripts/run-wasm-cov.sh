#!/usr/bin/env bash
set -euxo pipefail

workspace_root="$(pwd -P)"
coverage_root="${workspace_root}/target/wasm-coverage"
profraw_dir="${coverage_root}/data"
objects_dir="${coverage_root}/objects"
result_dir="${coverage_root}/result"
wasm_deps_dir="${coverage_root}/wasm32-unknown-unknown/debug/deps"
host_triple="$(rustc -vV | sed -n 's/^host: //p')"
rustlib_bin="$(rustc --print sysroot)/lib/rustlib/${host_triple}/bin"
llvm_profdata="${rustlib_bin}/llvm-profdata"
llvm_cov="${rustlib_bin}/llvm-cov"
coverage_clang="${WASM_COVERAGE_CLANG:-clang}"
coverage_object_target="${WASM_COVERAGE_OBJECT_TARGET:-x86_64-unknown-linux-gnu}"

export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR="${coverage_root}"
export CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner
export WASM_BINDGEN_USE_BROWSER=1
export LLVM_PROFILE_FILE="${profraw_dir}/coco-console-%m-%p.profraw"
export RUSTFLAGS="-Cinstrument-coverage -Zno-profiler-runtime --emit=llvm-ir --cfg=wasm_bindgen_unstable_test_coverage"
export CC="${CC_wasm32_unknown_unknown:-${coverage_clang}}"
export NIX_HARDENING_ENABLE="${WASM_COVERAGE_NIX_HARDENING_ENABLE:-}"

if command -v chromedriver >/dev/null 2>&1 || [[ -n "${CHROMEDRIVER:-}" ]]; then
  export CHROMEDRIVER="${CHROMEDRIVER:-$(command -v chromedriver)}"
  unset GECKODRIVER
  unset SAFARIDRIVER
elif command -v geckodriver >/dev/null 2>&1 || [[ -n "${GECKODRIVER:-}" ]]; then
  export GECKODRIVER="${GECKODRIVER:-$(command -v geckodriver)}"
  unset SAFARIDRIVER
elif command -v safaridriver >/dev/null 2>&1 || [[ -n "${SAFARIDRIVER:-}" ]]; then
  export SAFARIDRIVER="${SAFARIDRIVER:-$(command -v safaridriver)}"
else
  echo "No browser WebDriver is available for wasm coverage tests." >&2
  exit 1
fi

cargo clean --target-dir "${coverage_root}"
rm -rf "${profraw_dir}" "${objects_dir}" "${result_dir}"
mkdir -p "${profraw_dir}" "${objects_dir}" "${result_dir}"

env "CC_wasm32-unknown-unknown=${CC}" \
  cargo test -p coco-console --target wasm32-unknown-unknown graph_items_

mapfile -d "" llvm_ir_files < <(find "${wasm_deps_dir}" -name "*.ll" -print0)
if [[ "${#llvm_ir_files[@]}" -eq 0 ]]; then
  echo "No LLVM IR files were generated for wasm coverage." >&2
  exit 1
fi

llvm_cov_main_object=""
llvm_cov_extra_objects=()
for llvm_ir in "${llvm_ir_files[@]}"; do
  object="${objects_dir}/$(basename "${llvm_ir}" .ll).o"
  "${coverage_clang}" --target="${coverage_object_target}" -Wno-override-module -c "${llvm_ir}" -o "${object}"
  if [[ "$(basename "${llvm_ir}")" == coco_console-* ]]; then
    llvm_cov_main_object="${object}"
  else
    llvm_cov_extra_objects+=("-object" "${object}")
  fi
done

if [[ -z "${llvm_cov_main_object}" ]]; then
  echo "No coco-console LLVM IR object was generated for wasm coverage." >&2
  exit 1
fi

profraw_files=("${profraw_dir}"/*.profraw)
if [[ "${#profraw_files[@]}" -eq 0 ]]; then
  echo "No profraw files were generated for wasm coverage." >&2
  exit 1
fi

"${llvm_profdata}" merge -sparse "${profraw_files[@]}" -o "${result_dir}/wasm.profdata"
"${llvm_cov}" export \
  --format=lcov \
  --instr-profile="${result_dir}/wasm.profdata" \
  "${llvm_cov_main_object}" \
  "${llvm_cov_extra_objects[@]}" \
  >"${result_dir}/wasm.lcov.info"
