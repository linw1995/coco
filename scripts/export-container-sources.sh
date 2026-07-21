#!/usr/bin/env bash

set -euo pipefail

if (($# != 3)); then
  echo "Usage: $0 <docker-image.tar.gz> <source-layer.tar.gz> <platform>" >&2
  exit 1
fi

image_path="$1"
output_path="$2"
platform="$3"
workspace_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/coco-container-sources.XXXXXX")"
archive_dir="${work_dir}/image"
payload_dir="${work_dir}/payload"
runtime_paths_file="${payload_dir}/sources/RUNTIME_STORE_PATHS-${platform}.txt"
runtime_derivations_file="${payload_dir}/sources/RUNTIME_DERIVATIONS-${platform}.tsv"
source_paths_file="${payload_dir}/sources/SOURCE_STORE_PATHS-${platform}.txt"
source_derivations_file="${payload_dir}/sources/SOURCE_DERIVATIONS-${platform}.tsv"
derivations_dir="${payload_dir}/sources/derivations"
source_store_dir="${payload_dir}/sources/nix-store"

cleanup() {
  find "${work_dir}" -type d -exec chmod u+w {} + 2>/dev/null || true
  rm -rf "${work_dir}"
}
trap cleanup EXIT

if [[ ! "${platform}" =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
  echo "Invalid platform name: ${platform}" >&2
  exit 1
fi

for command_name in gzip jq nix nix-store; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 1
  fi
done

tar_command="${TAR:-tar}"
if ! "${tar_command}" --version 2>/dev/null | grep -q 'GNU tar'; then
  if command -v gtar >/dev/null 2>&1; then
    tar_command="gtar"
  else
    echo "GNU tar is required to export container sources." >&2
    exit 1
  fi
fi

if [[ ! -f "${image_path}" ]]; then
  echo "Docker image archive not found: ${image_path}" >&2
  exit 1
fi

mkdir -p \
  "${archive_dir}" \
  "${derivations_dir}" \
  "${source_store_dir}" \
  "${payload_dir}/sources/flake"

gzip -dc "${image_path}" | "${tar_command}" -xf - -C "${archive_dir}"

: > "${runtime_paths_file}"
layer_count=0
while IFS= read -r layer_path; do
  ((layer_count += 1))
  "${tar_command}" -tf "${archive_dir}/${layer_path}" \
    | sed -nE \
      -e 's#^/nix/store/([^/]+).*#/nix/store/\1#p' \
      -e 's#^\./nix/store/([^/]+).*#/nix/store/\1#p' \
      -e 's#^nix/store/([^/]+).*#/nix/store/\1#p' \
    >> "${runtime_paths_file}"
done < <(jq -r '.[0].Layers[]' "${archive_dir}/manifest.json")
if ((layer_count == 0)); then
  echo "Docker image archive contains no layers." >&2
  exit 1
fi
sort -u -o "${runtime_paths_file}" "${runtime_paths_file}"

if [[ ! -s "${runtime_paths_file}" ]]; then
  echo "No Nix store paths were found in the Docker image." >&2
  exit 1
fi

: > "${runtime_derivations_file}"
while IFS= read -r runtime_path; do
  if [[ ! -e "${runtime_path}" && ! -L "${runtime_path}" ]]; then
    echo "Runtime store path is unavailable: ${runtime_path}" >&2
    exit 1
  fi

  if ! derivation_path="$(nix-store --query --deriver "${runtime_path}" 2>/dev/null)"; then
    continue
  fi
  if [[ "${derivation_path}" == "unknown-deriver" ]]; then
    continue
  fi

  printf '%s\t%s\n' "${runtime_path}" "${derivation_path}" \
    >> "${runtime_derivations_file}"
done < "${runtime_paths_file}"
sort -u -o "${runtime_derivations_file}" "${runtime_derivations_file}"

if [[ ! -s "${runtime_derivations_file}" ]]; then
  echo "No runtime derivations were found for the Docker image." >&2
  exit 1
fi

: > "${source_derivations_file}"
while IFS= read -r derivation_path; do
  derivation_name="$(basename "${derivation_path}")"
  derivation_json="${derivations_dir}/${derivation_name%.drv}.json"

  nix derivation show "${derivation_path}" > "${derivation_json}"
  while IFS= read -r source_path; do
    printf '%s\t%s\n' "${source_path}" "${derivation_path}" \
      >> "${source_derivations_file}"
  done < <(jq -r '
    def records:
      if has("derivations") then .derivations[] else .[] end;
    records |
      (.inputSrcs[]?),
      (.inputs.srcs[]? | "/nix/store/" + .),
      (
        .env
        | to_entries[]
        | select(
            .key
            | test(
                "^(srcs?|patches|vendor|cargo(deps|vendor.*)|go(deps|modules))$";
                "i"
              )
          )
        | .value
        | scan("/nix/store/[0-9a-z]{32}-[A-Za-z0-9+._?=-]+")
      )
  ' "${derivation_json}")
done < <(cut -f2 "${runtime_derivations_file}" | sort -u)
sort -u -o "${source_derivations_file}" "${source_derivations_file}"
awk -F '\t' '
  NR == FNR { runtime[$1] = 1; next }
  !($1 in runtime)
' "${runtime_paths_file}" "${source_derivations_file}" \
  > "${source_derivations_file}.filtered"
mv "${source_derivations_file}.filtered" "${source_derivations_file}"
cut -f1 "${source_derivations_file}" | sort -u > "${source_paths_file}"

while IFS= read -r source_path; do
  if [[ ! -e "${source_path}" && ! -L "${source_path}" ]]; then
    nix-store --realise "${source_path}" >/dev/null 2>&1 || true
  fi

  if [[ ! -e "${source_path}" && ! -L "${source_path}" ]]; then
    parent_derivation="$(
      awk -F '\t' -v source_path="${source_path}" \
        '$1 == source_path { print $2; exit }' \
        "${source_derivations_file}"
    )"
    parent_json="${derivations_dir}/$(basename "${parent_derivation}" .drv).json"
    source_name="$(basename "${source_path}")"
    source_name="${source_name:33}"

    while IFS= read -r input_derivation; do
      if [[ "${input_derivation}" != /nix/store/* ]]; then
        input_derivation="/nix/store/${input_derivation}"
      fi

      input_name="$(basename "${input_derivation}")"
      input_name="${input_name:33}"
      input_name="${input_name%.drv}"
      if [[ "${input_name}" == "${source_name}" ]]; then
        nix-store --realise "${input_derivation}" >/dev/null
        break
      fi
    done < <(jq -r '
      def records:
        if has("derivations") then .derivations[] else .[] end;
      records |
        (.inputDrvs // {} | keys[]?),
        (.inputs.drvs // {} | keys[]?)
    ' "${parent_json}")

    if [[ ! -e "${source_path}" && ! -L "${source_path}" ]]; then
      echo "Source store path is unavailable: ${source_path}" >&2
      exit 1
    fi
  fi

  cp -a "${source_path}" "${source_store_dir}/$(basename "${source_path}")"
done < "${source_paths_file}"

flake_archive_json="$(nix flake archive --json --no-write-lock-file "${workspace_root}")"
flake_source="$(jq -r '.path' <<< "${flake_archive_json}")"
nixpkgs_source="$(jq -r '.inputs.nixpkgs.path' <<< "${flake_archive_json}")"

cp -a "${flake_source}" "${payload_dir}/sources/flake/coco"
cp -a "${nixpkgs_source}" "${payload_dir}/sources/flake/nixpkgs"
cp "${workspace_root}/docker/CONTAINER_SOURCE.md" "${payload_dir}/sources/README.md"

mkdir -p "$(dirname "${output_path}")"
"${tar_command}" \
  --sort=name \
  --mtime='@1' \
  --owner=0 \
  --group=0 \
  --numeric-owner \
  -czf "${output_path}" \
  -C "${payload_dir}" \
  sources
