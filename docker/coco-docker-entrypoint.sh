current_uid="$(id -u)"
current_gid="$(id -g)"
runtime_uid="${COCO_UID:-${current_uid}}"
runtime_gid="${COCO_GID:-${current_gid}}"

validate_numeric_id() {
  name="$1"
  value="$2"
  case "${value}" in
    "" | *[!0-9]*)
      printf '%s must be a numeric id\n' "${name}" >&2
      exit 64
      ;;
  esac
}

validate_runtime_id_pair() {
  if { [ -n "${COCO_UID:-}" ] && [ -z "${COCO_GID:-}" ]; } \
    || { [ -z "${COCO_UID:-}" ] && [ -n "${COCO_GID:-}" ]; }; then
    printf 'COCO_UID and COCO_GID must be set together\n' >&2
    exit 64
  fi
}

setup_timezone() {
  if [ -n "${TZ:-}" ] && [ -n "${TZDIR:-}" ] && [ -f "${TZDIR}/${TZ}" ]; then
    if ! { ln -snf "${TZDIR}/${TZ}" /etc/localtime; } 2>/dev/null; then
      printf 'warning: failed to link /etc/localtime to %s/%s\n' \
        "${TZDIR}" \
        "${TZ}" \
        >&2
    fi
    if ! { printf '%s\n' "${TZ}" >/etc/timezone; } 2>/dev/null; then
      printf 'warning: failed to write /etc/timezone for %s\n' "${TZ}" >&2
    fi
  fi
}

ensure_runtime_identity() {
  validate_numeric_id COCO_UID "${runtime_uid}"
  validate_numeric_id COCO_GID "${runtime_gid}"

  if [ "${runtime_uid}:${runtime_gid}" = "0:0" ]; then
    return 0
  fi

  if ! grep -Eq "^[^:]+:[^:]*:${runtime_gid}:" /etc/group; then
    printf 'coco:x:%s:\n' "${runtime_gid}" >>/etc/group
  fi

  if ! grep -Eq "^[^:]+:[^:]*:${runtime_uid}:" /etc/passwd; then
    printf '%s:x:%s:%s:CoCo runtime:/data:/bin/bash\n' \
      "coco" \
      "${runtime_uid}" \
      "${runtime_gid}" \
      >>/etc/passwd
  fi
}

chown_runtime_paths() {
  if [ "${runtime_uid}:${runtime_gid}" = "0:0" ]; then
    return 0
  fi

  mkdir -p /data
  chown -R "${runtime_uid}:${runtime_gid}" /data \
    || printf 'warning: failed to chown /data to %s:%s\n' \
      "${runtime_uid}" \
      "${runtime_gid}" \
      >&2
}

start_cron() {
  if [ "${COCO_START_CRON:-1}" != "1" ]; then
    return 0
  fi

  cronjob_install_dir="${COCO_SKILL_PERSIST_ROOT:-/data/skills}/orchestrator/cronjob/data/install"
  cronjob_crontab_dir="${COCO_CRONTAB_DIR:-${cronjob_install_dir}/crontabs}"
  cronjob_managed_crontab_dir="${cronjob_install_dir}/managed-crontabs"
  mkdir -p "${cronjob_crontab_dir}" "${cronjob_managed_crontab_dir}"
  if [ ! -f "${cronjob_crontab_dir}/local.crontab" ]; then
    printf '# CoCo cronjobs\n' >"${cronjob_crontab_dir}/local.crontab"
  fi
  export COCO_CRONTAB_DIR="${cronjob_crontab_dir}"
  if [ -f "${cronjob_install_dir}/cronjob_restore.py" ] && [ -d "${cronjob_managed_crontab_dir}" ]; then
    uv run --script "${cronjob_install_dir}/cronjob_restore.py" \
      --snapshot-dir "${cronjob_managed_crontab_dir}" \
      --crontab-dir "${cronjob_crontab_dir}" \
      || printf 'warning: failed to restore managed CoCo cronjob files\n' >&2
  fi
  supervise_crontabs "${cronjob_crontab_dir}" &
}

supervise_crontabs() {
  crontab_dir="$1"
  pid_dir="${crontab_dir}/.pids"
  mkdir -p "${pid_dir}"
  while :; do
    for crontab_file in "${crontab_dir}"/*.crontab; do
      if [ ! -f "${crontab_file}" ]; then
        continue
      fi
      start_supercronic_file "${crontab_file}" "${pid_dir}"
    done
    sleep "${COCO_CRON_SCAN_INTERVAL:-5}"
  done
}

start_supercronic_file() {
  crontab_file="$1"
  pid_dir="$2"
  pid_file="${pid_dir}/$(basename "${crontab_file}").pid"
  if [ -f "${pid_file}" ]; then
    pid="$(cat "${pid_file}" 2>/dev/null || true)"
    if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
      return 0
    fi
  fi
  supervise_supercronic_file "${crontab_file}" &
  printf '%s\n' "$!" >"${pid_file}"
}

supervise_supercronic_file() {
  crontab_file="$1"
  while [ -f "${crontab_file}" ]; do
    supercronic -inotify "${crontab_file}"
    printf 'warning: supercronic exited for %s; restarting\n' "${crontab_file}" >&2
    sleep "${COCO_CRON_RESTART_DELAY:-2}"
  done
}

validate_runtime_id_pair

if [ "${current_uid}" = "0" ]; then
  setup_timezone
  ensure_runtime_identity
  chown_runtime_paths
  if [ "${runtime_uid}:${runtime_gid}" != "0:0" ]; then
    exec setpriv --reuid "${runtime_uid}" --regid "${runtime_gid}" --clear-groups "$0" "$@"
  fi
fi

start_cron
exec "$@"
