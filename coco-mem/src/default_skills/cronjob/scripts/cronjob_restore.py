# /// script
# dependencies = []
# ///
"""Restore managed CoCo cronjob entries from a persistent snapshot."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from cronjob_crontab import project_managed_crontabs


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
    project_managed_crontabs(snapshot_dir, crontab_dir)


if __name__ == "__main__":
    raise SystemExit(main())
