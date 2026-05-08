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
    snapshot_path = args.snapshot
    if not snapshot_path.is_file():
        return 0

    snapshot = snapshot_path.read_text(encoding="utf-8")
    if not snapshot.strip():
        return 0

    crontab_file = resolve_crontab_file(args.crontab_file)
    timezone_reset = resolve_timezone_reset(crontab_file)
    if crontab_file is not None:
        snapshot = normalize_direct_crontab(snapshot, timezone_reset)
    current = read_crontab(args.crontab_bin, crontab_file)
    if crontab_file is not None:
        current = normalize_direct_crontab(current, timezone_reset)
    updated = restore_managed_blocks(current, snapshot)
    if updated != current:
        write_crontab(args.crontab_bin, crontab_file, updated)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--crontab-bin", default="crontab")
    parser.add_argument("--crontab-file", type=Path, help="Manage this crontab file directly.")
    return parser.parse_args()


def resolve_crontab_file(value: Path | None) -> Path | None:
    if value is not None:
        return value.expanduser()
    env_value = os.environ.get("COCO_CRONTAB_FILE")
    if not env_value:
        return None
    return Path(env_value).expanduser()


def resolve_timezone_reset(crontab_file: Path | None) -> str:
    if crontab_file is None:
        return ""
    timezone = normalize_timezone(os.environ.get("TZ"))
    return timezone or "UTC"


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


def normalize_direct_crontab(content: str, timezone_reset: str) -> str:
    return "\n".join(
        f"CRON_TZ={timezone_reset}" if line == "CRON_TZ=" else line
        for line in content.split("\n")
    )


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
