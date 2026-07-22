#!/usr/bin/env bash

set -euo pipefail

script_name="$(basename "$0")"
workspace_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
dependency_format_version="1"
temporary_dirs=()
current_work_dir=""

usage() {
  cat >&2 <<EOF
Usage:
  ${script_name} inspect <docker-image.tar.gz> <state-dir> <platform>
  ${script_name} dependencies <state-dir> <source-layer.tar.gz>
  ${script_name} release <state-dir> <source-layer.tar.gz>
  ${script_name} <docker-image.tar.gz> <source-layer.tar.gz> <platform>
EOF
}

cleanup() {
  local temporary_dir

  for temporary_dir in "${temporary_dirs[@]}"; do
    find "${temporary_dir}" -type d -exec chmod u+w {} + 2>/dev/null || true
    rm -rf -- "${temporary_dir}"
  done
}
trap cleanup EXIT

new_work_dir() {
  current_work_dir="$(
    mktemp -d "${TMPDIR:-/tmp}/coco-container-sources.XXXXXX"
  )"
  temporary_dirs+=("${current_work_dir}")
}

require_commands() {
  local command_name

  for command_name in "$@"; do
    if ! command -v "${command_name}" >/dev/null 2>&1; then
      echo "Required command not found: ${command_name}" >&2
      exit 1
    fi
  done
}

select_tar_command() {
  tar_command="${TAR:-tar}"
  if "${tar_command}" --version 2>/dev/null | grep -q 'GNU tar'; then
    return
  fi

  if command -v gtar >/dev/null 2>&1; then
    tar_command="gtar"
    return
  fi

  echo "GNU tar is required to export container sources." >&2
  exit 1
}

validate_platform() {
  local platform="$1"

  if [[ ! "${platform}" =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
    echo "Invalid platform name: ${platform}" >&2
    exit 1
  fi
}

require_state_file() {
  local state_dir="$1"
  local state_file="$2"

  if [[ ! -f "${state_dir}/${state_file}" ]]; then
    echo "Container source state is missing ${state_file}." >&2
    exit 1
  fi
}

hash_file() {
  local input_path="$1"

  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${input_path}" | cut -d ' ' -f1
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${input_path}" | cut -d ' ' -f1
    return
  fi

  nix hash file --base16 --type sha256 "${input_path}"
}

create_layer_archive() {
  local payload_dir="$1"
  local output_path="$2"

  "${tar_command}" \
    --sort=name \
    --mtime='@1' \
    --owner=0 \
    --group=0 \
    --numeric-owner \
    -czf "${output_path}" \
    -C "${payload_dir}" \
    sources
}

inspect_sources() {
  local image_path="$1"
  local state_dir="$2"
  local platform="$3"
  local work_dir
  local archive_dir
  local state_build_dir
  local runtime_paths_file
  local runtime_derivations_file
  local source_paths_file
  local source_derivations_file
  local derivations_dir
  local dependency_paths_file
  local release_paths_file
  local layer_count
  local layer_path
  local runtime_path
  local derivation_path
  local derivation_name
  local derivation_json
  local flake_archive_json
  local flake_source
  local nixpkgs_source
  local key_material_file
  local dependency_key
  local exporter_hash

  validate_platform "${platform}"
  require_commands gzip jq nix nix-store

  if [[ ! -f "${image_path}" ]]; then
    echo "Docker image archive not found: ${image_path}" >&2
    exit 1
  fi
  if [[ -e "${state_dir}" ]]; then
    echo "Container source state already exists: ${state_dir}" >&2
    exit 1
  fi

  new_work_dir
  work_dir="${current_work_dir}"
  archive_dir="${work_dir}/image"
  state_build_dir="${work_dir}/state"
  runtime_paths_file="${state_build_dir}/RUNTIME_STORE_PATHS-${platform}.txt"
  runtime_derivations_file="${state_build_dir}/RUNTIME_DERIVATIONS-${platform}.tsv"
  source_paths_file="${state_build_dir}/SOURCE_STORE_PATHS-${platform}.txt"
  source_derivations_file="${state_build_dir}/SOURCE_DERIVATIONS-${platform}.tsv"
  derivations_dir="${state_build_dir}/derivations"
  dependency_paths_file="${state_build_dir}/DEPENDENCY_SOURCE_PATHS.txt"
  release_paths_file="${state_build_dir}/RELEASE_SOURCE_PATHS.txt"

  mkdir -p "${archive_dir}" "${derivations_dir}"
  gzip -dc "${image_path}" | "${tar_command}" -xf - -C "${archive_dir}"

  : > "${runtime_paths_file}"
  layer_count=0
  while IFS= read -r layer_path; do
    ((layer_count += 1))
    "${tar_command}" --absolute-names -tf "${archive_dir}/${layer_path}" \
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

    while IFS= read -r derivation_path; do
      printf '%s\t%s\n' "${runtime_path}" "${derivation_path}" \
        >> "${runtime_derivations_file}"
    done < <(
      nix-store --query --valid-derivers "${runtime_path}" 2>/dev/null || true
    )
  done < "${runtime_paths_file}"
  sort -u -o "${runtime_derivations_file}" "${runtime_derivations_file}"

  if [[ ! -s "${runtime_derivations_file}" ]]; then
    echo "No runtime derivations were found for the Docker image." >&2
    exit 1
  fi

  : > "${source_derivations_file}"
  : > "${release_paths_file}"
  while IFS= read -r derivation_path; do
    derivation_name="$(basename "${derivation_path}")"
    derivation_json="${derivations_dir}/${derivation_name%.drv}.json"

    nix derivation show "${derivation_path}" > "${derivation_json}"
    jq -r '
      def records:
        if has("derivations") then .derivations[] else .[] end;
      records |
        select(.env.pname? == "coco-cli") |
        (.env.src? // empty)
    ' "${derivation_json}" >> "${release_paths_file}"
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

  flake_archive_json="$(
    nix flake archive --json --no-write-lock-file "${workspace_root}"
  )"
  flake_source="$(jq -r '.path' <<< "${flake_archive_json}")"
  nixpkgs_source="$(jq -r '.inputs.nixpkgs.path' <<< "${flake_archive_json}")"

  if [[ ! -s "${release_paths_file}" ]]; then
    echo "No CoCo release source path was found in the runtime derivations." >&2
    exit 1
  fi
  printf '%s\n' "${flake_source}" >> "${release_paths_file}"
  sort -u -o "${release_paths_file}" "${release_paths_file}"
  awk '
    NR == FNR { release[$1] = 1; next }
    !($0 in release)
  ' "${release_paths_file}" "${source_paths_file}" \
    > "${dependency_paths_file}"

  printf '%s\n' "${platform}" > "${state_build_dir}/PLATFORM"
  printf '%s\n' "${flake_source}" > "${state_build_dir}/FLAKE_SOURCE_PATH"
  printf '%s\n' "${nixpkgs_source}" > "${state_build_dir}/NIXPKGS_SOURCE_PATH"

  key_material_file="${state_build_dir}/DEPENDENCY_KEY_INPUT"
  exporter_hash="$(hash_file "${BASH_SOURCE[0]}")"
  {
    printf 'format=%s\n' "${dependency_format_version}"
    printf 'exporter=%s\n' "${exporter_hash}"
    printf 'nixpkgs=%s\n' "${nixpkgs_source}"
    sed 's/^/source=/' "${dependency_paths_file}"
  } > "${key_material_file}"
  dependency_key="$(hash_file "${key_material_file}")"
  printf '%s\n' "${dependency_key}" > "${state_build_dir}/DEPENDENCY_KEY"

  mkdir -p "$(dirname "${state_dir}")"
  mv "${state_build_dir}" "${state_dir}"
}

restore_source_path() {
  local source_path="$1"
  local state_dir="$2"
  local source_derivations_file="$3"
  local parent_derivation
  local parent_json
  local source_name
  local input_derivation
  local input_name

  if [[ -e "${source_path}" || -L "${source_path}" ]]; then
    return
  fi

  nix-store --realise "${source_path}" >/dev/null 2>&1 || true
  if [[ -e "${source_path}" || -L "${source_path}" ]]; then
    return
  fi

  parent_derivation="$(
    awk -F '\t' -v source_path="${source_path}" \
      '$1 == source_path { print $2; exit }' \
      "${source_derivations_file}"
  )"
  parent_json="${state_dir}/derivations/$(
    basename "${parent_derivation}" .drv
  ).json"
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
}

package_dependencies() {
  local state_dir="$1"
  local output_path="$2"
  local platform
  local source_derivations_file
  local dependency_paths_file
  local nixpkgs_source
  local work_dir
  local payload_dir
  local source_store_dir
  local source_path
  local archive_path

  require_commands jq nix-store
  for state_file in \
    PLATFORM \
    NIXPKGS_SOURCE_PATH \
    DEPENDENCY_SOURCE_PATHS.txt; do
    require_state_file "${state_dir}" "${state_file}"
  done

  platform="$(< "${state_dir}/PLATFORM")"
  validate_platform "${platform}"
  source_derivations_file="${state_dir}/SOURCE_DERIVATIONS-${platform}.tsv"
  dependency_paths_file="${state_dir}/DEPENDENCY_SOURCE_PATHS.txt"
  nixpkgs_source="$(< "${state_dir}/NIXPKGS_SOURCE_PATH")"
  require_state_file "${state_dir}" "SOURCE_DERIVATIONS-${platform}.tsv"

  new_work_dir
  work_dir="${current_work_dir}"
  payload_dir="${work_dir}/payload"
  source_store_dir="${payload_dir}/sources/nix-store"
  archive_path="${work_dir}/dependencies.tar.gz"
  mkdir -p "${source_store_dir}" "${payload_dir}/sources/flake"

  while IFS= read -r source_path; do
    restore_source_path \
      "${source_path}" \
      "${state_dir}" \
      "${source_derivations_file}"
    cp -a "${source_path}" "${source_store_dir}/$(basename "${source_path}")"
  done < "${dependency_paths_file}"

  if [[ ! -e "${nixpkgs_source}" && ! -L "${nixpkgs_source}" ]]; then
    echo "Nixpkgs source is unavailable: ${nixpkgs_source}" >&2
    exit 1
  fi
  cp -a "${nixpkgs_source}" "${payload_dir}/sources/flake/nixpkgs"

  create_layer_archive "${payload_dir}" "${archive_path}"
  mkdir -p "$(dirname "${output_path}")"
  mv "${archive_path}" "${output_path}"
}

package_release() {
  local state_dir="$1"
  local output_path="$2"
  local platform
  local flake_source
  local release_paths_file
  local source_derivations_file
  local work_dir
  local payload_dir
  local archive_path
  local source_name
  local source_path
  local metadata_file

  require_commands jq nix-store
  for state_file in PLATFORM FLAKE_SOURCE_PATH DEPENDENCY_KEY; do
    require_state_file "${state_dir}" "${state_file}"
  done

  platform="$(< "${state_dir}/PLATFORM")"
  validate_platform "${platform}"
  flake_source="$(< "${state_dir}/FLAKE_SOURCE_PATH")"
  release_paths_file="${state_dir}/RELEASE_SOURCE_PATHS.txt"
  source_derivations_file="${state_dir}/SOURCE_DERIVATIONS-${platform}.tsv"
  require_state_file "${state_dir}" "RELEASE_SOURCE_PATHS.txt"
  for metadata_file in \
    "RUNTIME_STORE_PATHS-${platform}.txt" \
    "RUNTIME_DERIVATIONS-${platform}.tsv" \
    "SOURCE_STORE_PATHS-${platform}.txt" \
    "SOURCE_DERIVATIONS-${platform}.tsv"; do
    require_state_file "${state_dir}" "${metadata_file}"
  done
  if [[ ! -d "${state_dir}/derivations" ]]; then
    echo "Container source state is missing derivations." >&2
    exit 1
  fi
  if [[ ! -e "${flake_source}" && ! -L "${flake_source}" ]]; then
    echo "Flake source is unavailable: ${flake_source}" >&2
    exit 1
  fi

  new_work_dir
  work_dir="${current_work_dir}"
  payload_dir="${work_dir}/payload"
  archive_path="${work_dir}/release.tar.gz"
  mkdir -p \
    "${payload_dir}/sources/derivations" \
    "${payload_dir}/sources/flake" \
    "${payload_dir}/sources/nix-store"

  for metadata_file in \
    "RUNTIME_STORE_PATHS-${platform}.txt" \
    "RUNTIME_DERIVATIONS-${platform}.tsv" \
    "SOURCE_STORE_PATHS-${platform}.txt" \
    "SOURCE_DERIVATIONS-${platform}.tsv"; do
    cp "${state_dir}/${metadata_file}" "${payload_dir}/sources/${metadata_file}"
  done
  cp -a "${state_dir}/derivations/." "${payload_dir}/sources/derivations/"
  cp -a "${flake_source}" "${payload_dir}/sources/flake/coco"
  cp "${workspace_root}/docker/CONTAINER_SOURCE.md" \
    "${payload_dir}/sources/README.md"
  cp "${state_dir}/DEPENDENCY_KEY" \
    "${payload_dir}/sources/DEPENDENCY_SOURCE_KEY-${platform}.txt"

  while IFS= read -r source_path; do
    source_name="$(basename "${source_path}")"
    if [[ "${source_path}" == "${flake_source}" ]]; then
      ln -s ../flake/coco "${payload_dir}/sources/nix-store/${source_name}"
      continue
    fi

    restore_source_path \
      "${source_path}" \
      "${state_dir}" \
      "${source_derivations_file}"
    cp -a \
      "${source_path}" \
      "${payload_dir}/sources/nix-store/${source_name}"
  done < "${release_paths_file}"

  create_layer_archive "${payload_dir}" "${archive_path}"
  mkdir -p "$(dirname "${output_path}")"
  mv "${archive_path}" "${output_path}"
}

package_legacy_bundle() {
  local image_path="$1"
  local output_path="$2"
  local platform="$3"
  local work_dir
  local state_dir
  local dependencies_path
  local release_path
  local payload_dir
  local archive_path

  new_work_dir
  work_dir="${current_work_dir}"
  state_dir="${work_dir}/state"
  dependencies_path="${work_dir}/dependencies.tar.gz"
  release_path="${work_dir}/release.tar.gz"
  payload_dir="${work_dir}/combined"
  archive_path="${work_dir}/combined.tar.gz"

  inspect_sources "${image_path}" "${state_dir}" "${platform}"
  package_dependencies "${state_dir}" "${dependencies_path}"
  package_release "${state_dir}" "${release_path}"

  mkdir -p "${payload_dir}"
  "${tar_command}" -xzf "${dependencies_path}" -C "${payload_dir}"
  "${tar_command}" -xzf "${release_path}" -C "${payload_dir}"
  create_layer_archive "${payload_dir}" "${archive_path}"
  mkdir -p "$(dirname "${output_path}")"
  mv "${archive_path}" "${output_path}"
}

require_commands cut grep sed tar
select_tar_command

case "${1:-}" in
  inspect)
    if (($# != 4)); then
      usage
      exit 1
    fi
    inspect_sources "$2" "$3" "$4"
    ;;
  dependencies)
    if (($# != 3)); then
      usage
      exit 1
    fi
    package_dependencies "$2" "$3"
    ;;
  release)
    if (($# != 3)); then
      usage
      exit 1
    fi
    package_release "$2" "$3"
    ;;
  *)
    if (($# != 3)); then
      usage
      exit 1
    fi
    package_legacy_bundle "$1" "$2" "$3"
    ;;
esac
