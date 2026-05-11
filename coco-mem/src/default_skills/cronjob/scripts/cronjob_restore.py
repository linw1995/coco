# /// script
# dependencies = []
# ///
"""Restore managed CoCo cronjob entries from a persistent snapshot."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
from pathlib import Path


MANAGED_PREFIX = "coco-cronjob"
TIMEZONE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_+./:-]{0,127}$")


def main() -> int:
    args = parse_args()
    snapshot_dir = resolve_snapshot_dir(args.snapshot_dir)
    crontab_dir = resolve_crontab_dir(args.crontab_dir)
    if snapshot_dir is not None or crontab_dir is not None:
        if snapshot_dir is None or crontab_dir is None:
            raise SystemExit("use --snapshot-dir and --crontab-dir together")
        restore_snapshot_dir(snapshot_dir, crontab_dir)
        return 0

    if args.snapshot is None:
        raise SystemExit("--snapshot is required unless --snapshot-dir is used")
    snapshot_path = args.snapshot
    if not snapshot_path.is_file():
        return 0

    snapshot = snapshot_path.read_text(encoding="utf-8")

    crontab_file = resolve_crontab_file(args.crontab_file)
    active_crontab = read_crontab(args.crontab_bin, crontab_file)
    if crontab_file is not None:
        # Direct crontab files are active schedule files for this skill, not shared user
        # crontabs. The managed snapshot is the canonical fixed-format source for this
        # path, so restore it as-is instead of merging with arbitrary existing content.
        final_crontab = normalize_direct_crontab(snapshot)
    else:
        final_crontab = restore_managed_blocks(active_crontab, snapshot)
    if final_crontab != active_crontab:
        write_crontab(args.crontab_bin, crontab_file, final_crontab)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--snapshot-dir", type=Path)
    parser.add_argument("--crontab-bin", default="crontab")
    parser.add_argument("--crontab-file", type=Path, help="Manage this crontab file directly.")
    parser.add_argument("--crontab-dir", type=Path, help="Restore direct crontab files into this directory.")
    return parser.parse_args()


def resolve_crontab_file(value: Path | None) -> Path | None:
    if value is not None:
        return value.expanduser()
    return None


def resolve_snapshot_dir(value: Path | None) -> Path | None:
    if value is not None:
        return value.expanduser()
    return None


def resolve_crontab_dir(value: Path | None) -> Path | None:
    if value is not None:
        return value.expanduser()
    env_value = os.environ.get("COCO_CRONTAB_DIR")
    if not env_value:
        return None
    return Path(env_value).expanduser()


def restore_snapshot_dir(snapshot_dir: Path, crontab_dir: Path) -> None:
    if not snapshot_dir.is_dir():
        return
    crontab_dir.mkdir(parents=True, exist_ok=True)
    for snapshot_path in sorted(snapshot_dir.glob("*.crontab")):
        snapshot = snapshot_path.read_text(encoding="utf-8")
        crontab_file = crontab_dir / snapshot_path.name
        active_crontab = read_crontab("crontab", crontab_file)
        final_crontab = normalize_direct_crontab(snapshot)
        if final_crontab != active_crontab:
            write_crontab("crontab", crontab_file, final_crontab)


def normalize_timezone(value: str | None) -> str | None:
    if value is None:
        return None
    timezone = value.strip()
    if not timezone:
        return None
    if not TIMEZONE_PATTERN.fullmatch(timezone):
        raise SystemExit("timezone must be a single CRON_TZ token")
    return timezone


def read_crontab(crontab_bin: str, crontab_file: Path | None) -> str:
    if crontab_file is not None:
        if not crontab_file.exists():
            return ""
        return crontab_file.read_text(encoding="utf-8")

    result = subprocess.run(
        [crontab_bin, "-l"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode == 0:
        return result.stdout
    combined = (result.stdout + result.stderr).lower()
    if "no crontab" in combined or "no crontab for" in combined:
        return ""
    raise SystemExit(f"failed to read crontab: {result.stderr.strip()}")


def write_crontab(crontab_bin: str, crontab_file: Path | None, content: str) -> None:
    if crontab_file is not None:
        crontab_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = crontab_file.with_suffix(crontab_file.suffix + ".tmp")
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(crontab_file)
        return

    result = subprocess.run(
        [crontab_bin, "-"],
        input=content,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(f"failed to install crontab: {result.stderr.strip()}")


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


def restore_managed_blocks(current: str, snapshot: str) -> str:
    updated = remove_managed_blocks(current)
    blocks = snapshot.rstrip("\n").splitlines()
    if not blocks:
        return updated
    if updated and not updated.endswith("\n"):
        updated += "\n"
    if updated and not updated.endswith("\n\n"):
        updated += "\n"
    updated += "\n".join(blocks).rstrip("\n") + "\n"
    return updated


def remove_managed_blocks(content: str) -> str:
    lines = content.splitlines()
    output: list[str] = []
    index = 0
    while index < len(lines):
        if lines[index].startswith(f"# BEGIN {MANAGED_PREFIX} id="):
            index += 1
            while index < len(lines) and not lines[index].startswith(
                f"# END {MANAGED_PREFIX} id="
            ):
                index += 1
            if index == len(lines):
                raise SystemExit("managed crontab block is missing its end marker")
            index += 1
            continue
        output.append(lines[index])
        index += 1
    return "\n".join(output).rstrip("\n") + ("\n" if output else "")


if __name__ == "__main__":
    raise SystemExit(main())
