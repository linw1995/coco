# /// script
# dependencies = []
# ///
"""Restore managed CoCo cronjob entries from a persistent snapshot."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


MANAGED_PREFIX = "coco-cronjob"


def main() -> int:
    args = parse_args()
    snapshot_path = args.snapshot
    if not snapshot_path.is_file():
        return 0

    snapshot = snapshot_path.read_text(encoding="utf-8")
    if not snapshot.strip():
        return 0

    current = read_crontab(args.crontab_bin)
    updated = restore_managed_blocks(current, snapshot)
    if updated != current:
        write_crontab(args.crontab_bin, updated)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--crontab-bin", default="crontab")
    return parser.parse_args()


def read_crontab(crontab_bin: str) -> str:
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


def write_crontab(crontab_bin: str, content: str) -> None:
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
