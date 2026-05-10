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
  cronjob_crontab_file="${COCO_CRONTAB_FILE:-${cronjob_install_dir}/crontab}"
  mkdir -p "$(dirname "${cronjob_crontab_file}")"
  if [ ! -f "${cronjob_crontab_file}" ]; then
    printf '# CoCo cronjobs\n' >"${cronjob_crontab_file}"
  fi
  export COCO_CRONTAB_FILE="${cronjob_crontab_file}"
  if [ -f "${cronjob_install_dir}/cronjob_restore.py" ] && [ -f "${cronjob_install_dir}/managed-crontab" ]; then
    uv run --script "${cronjob_install_dir}/cronjob_restore.py" \
      --snapshot "${cronjob_install_dir}/managed-crontab" \
      --crontab-file "${cronjob_crontab_file}" \
      || printf 'warning: failed to restore managed CoCo cronjobs\n' >&2
  fi
  supercronic -inotify "${cronjob_crontab_file}" &
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
