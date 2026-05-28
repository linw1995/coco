#!/usr/bin/env bash
set -euo pipefail

export PYTHONDONTWRITEBYTECODE=1
python_bin="${PYTHON_BIN:-python3}"

mapfile -t tests < <(
  find coco-mem/src/default_skills -path '*/tests/*_test.py' -print | sort
)

if [[ "${#tests[@]}" -eq 0 ]]; then
  echo "No builtin skill script tests found." >&2
  exit 1
fi

"${python_bin}" -m unittest -v "${tests[@]}"
