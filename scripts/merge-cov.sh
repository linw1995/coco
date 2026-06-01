#!/usr/bin/env bash
set -euxo pipefail

workspace_root="$(pwd -P)"
coverage_result_dir="${workspace_root}/target/coverage/result"
native_lcov="${coverage_result_dir}/lcov.info"
wasm_lcov="${workspace_root}/target/wasm-coverage/result/wasm.lcov.info"
merged_lcov="${coverage_result_dir}/lcov.info"
native_copy="${coverage_result_dir}/native.lcov.info"
wasm_copy="${coverage_result_dir}/wasm.lcov.info"
summary="${coverage_result_dir}/coverage-summary.md"
merge_marker="${coverage_result_dir}/.merged-coverage"

if [[ ! -f "${native_lcov}" ]]; then
  echo "Native coverage report is missing: ${native_lcov}" >&2
  exit 1
fi

native_input="${native_lcov}"
if [[ -f "${merge_marker}" && -f "${native_copy}" && ! "${native_lcov}" -nt "${merge_marker}" ]]; then
  native_input="${native_copy}"
fi

if [[ "${native_input}" != "${native_copy}" ]]; then
  cp "${native_input}" "${native_copy}"
fi

if [[ -f "${wasm_lcov}" ]]; then
  awk '
    /^SF:/ {
      source = substr($0, 4)
      if (index(source, workspace_root "/") == 1) {
        source = substr(source, length(workspace_root) + 2)
      }
      include_source = source !~ /^\//
      if (include_source) {
        print "SF:" source
      }
      next
    }
    /^end_of_record/ {
      if (include_source) {
        print
      }
      include_source = 0
      next
    }
    include_source {
      print
    }
  ' workspace_root="${workspace_root}" "${wasm_lcov}" >"${wasm_copy}"
else
  echo "Wasm coverage report is missing; merging native coverage only: ${wasm_lcov}" >&2
  : >"${wasm_copy}"
fi

{
  cat "${native_copy}"
  printf "\n"
  cat "${wasm_copy}"
} >"${merged_lcov}"
touch "${merge_marker}"

awk '
  BEGIN {
    source = ""
  }
  /^SF:/ {
    source = substr($0, 4)
    if (index(source, workspace_root "/") == 1) {
      source = substr(source, length(workspace_root) + 2)
    }
    include_source = source !~ /^\//
    next
  }
  /^DA:/ {
    if (!include_source) {
      next
    }
    data = substr($0, 4)
    split(data, fields, ",")
    key = source ":" fields[1]
    lines[key] = 1
    if (fields[2] + 0 > 0) {
      covered[key] = 1
    }
  }
  END {
    for (key in lines) {
      total += 1
      if (key in covered) {
        covered_lines += 1
      }
    }
    percent = total == 0 ? 0 : covered_lines * 100 / total
    printf "| report | line coverage | covered |\n"
    printf "|--------|---------------|---------|\n"
    printf "| merged | %.2f%% | %d / %d |\n", percent, covered_lines, total
    printf "\nMerged line coverage: %.2f%% (%d / %d)\n", percent, covered_lines, total
  }
' workspace_root="${workspace_root}" "${merged_lcov}" | tee "${summary}"
