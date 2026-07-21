#!/usr/bin/env bash

set -euo pipefail

workspace_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
generated_notices="$(mktemp "${TMPDIR:-/tmp}/coco-third-party-notices-check.XXXXXX")"

trap 'rm -f "${generated_notices}"' EXIT

"${workspace_root}/scripts/generate-third-party-notices.sh" "${generated_notices}"

if ! cmp -s "${workspace_root}/THIRD_PARTY_NOTICES.html" "${generated_notices}"; then
  echo "THIRD_PARTY_NOTICES.html is stale. Regenerate it with:" >&2
  echo "  scripts/generate-third-party-notices.sh" >&2
  exit 1
fi
