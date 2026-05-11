# /// script
# dependencies = []
# ///
"""Restore managed CoCo cronjob entries from a persistent snapshot."""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path


TIMEZONE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_+./:-]{0,127}$")


def main() -> int:
    args = parse_args()
    snapshot_dir = resolve_snapshot_dir(args.snapshot_dir)
    crontab_dir = resolve_crontab_dir(args.crontab_dir)
    restore_snapshot_dir(snapshot_dir, crontab_dir)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-dir", type=Path)
    parser.add_argument("--crontab-dir", type=Path, help="Restore direct crontab files into this directory.")
    return parser.parse_args()


def resolve_snapshot_dir(value: Path | None) -> Path:
    if value is not None:
        return value.expanduser()
    raise SystemExit("--snapshot-dir is required")


def resolve_crontab_dir(value: Path | None) -> Path:
    if value is not None:
        return value.expanduser()
    env_value = os.environ.get("COCO_CRONTAB_DIR")
    if not env_value:
        raise SystemExit("--crontab-dir is required unless COCO_CRONTAB_DIR is set")
    return Path(env_value).expanduser()


def restore_snapshot_dir(snapshot_dir: Path, crontab_dir: Path) -> None:
    if not snapshot_dir.is_dir():
        return
    crontab_dir.mkdir(parents=True, exist_ok=True)
    for snapshot_path in sorted(snapshot_dir.glob("*.crontab")):
        snapshot = snapshot_path.read_text(encoding="utf-8")
        crontab_file = crontab_dir / snapshot_path.name
        active_crontab = read_crontab(crontab_file)
        final_crontab = normalize_direct_crontab(snapshot)
        if final_crontab != active_crontab:
            write_crontab(crontab_file, final_crontab)


def normalize_timezone(value: str | None) -> str | None:
    if value is None:
        return None
    timezone = value.strip()
    if not timezone:
        return None
    if not TIMEZONE_PATTERN.fullmatch(timezone):
        raise SystemExit("timezone must be a single CRON_TZ token")
    return timezone


def read_crontab(crontab_file: Path) -> str:
    if not crontab_file.exists():
        return ""
    return crontab_file.read_text(encoding="utf-8")


def write_crontab(crontab_file: Path, content: str) -> None:
    crontab_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = crontab_file.with_suffix(crontab_file.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(crontab_file)


def normalize_direct_crontab(content: str) -> str:
    job_timezones, has_unqualified_jobs = collect_direct_job_timezones(content)
    if len(job_timezones) > 1:
        raise SystemExit(
            "direct crontab files run under supercronic and support only one "
            f"schedule timezone; found {', '.join(sorted(job_timezones))}"
        )
    if job_timezones and has_unqualified_jobs:
        raise SystemExit(
            "direct crontab files run under supercronic and cannot mix "
            "timezone-qualified jobs with timezone-less jobs"
        )

    timezone = next(iter(job_timezones), None)
    body = "\n".join(
        line
        for line in content.splitlines()
        if parse_env_assignment(line.strip())[0] != "CRON_TZ"
    ).strip("\n")
    lines: list[str] = []
    if timezone is not None:
        lines.append(f"CRON_TZ={timezone}")
    if body:
        lines.append(body)
    return "\n".join(lines).rstrip("\n") + ("\n" if lines else "")


def collect_direct_job_timezones(content: str) -> tuple[set[str], bool]:
    current_timezone: str | None = None
    job_timezones: set[str] = set()
    has_unqualified_jobs = False
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = parse_env_assignment(line)
        if key is not None:
            if key == "CRON_TZ":
                current_timezone = normalize_timezone(value)
            continue
        if current_timezone is None:
            has_unqualified_jobs = True
        else:
            job_timezones.add(current_timezone)
    return job_timezones, has_unqualified_jobs


def parse_env_assignment(line: str) -> tuple[str | None, str]:
    if "=" not in line or line.split("=", 1)[0].strip() != line.split("=", 1)[0]:
        return None, ""
    key, value = line.split("=", 1)
    if not key or any(char.isspace() for char in key):
        return None, ""
    return key, value.strip()


if __name__ == "__main__":
    raise SystemExit(main())
