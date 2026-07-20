#!/usr/bin/env python3

import tomllib
from pathlib import Path


def main() -> int:
    workspace_root = Path(__file__).resolve().parent.parent
    with (workspace_root / "about.toml").open("rb") as about_file:
        about = tomllib.load(about_file)
    with (workspace_root / "deny.toml").open("rb") as deny_file:
        deny = tomllib.load(deny_file)

    graph = deny["graph"]
    licenses = deny["licenses"]
    mismatches = []

    if about["accepted"] != licenses["allow"]:
        mismatches.append("accepted license lists")
    if about["targets"] != graph["targets"]:
        mismatches.append("target lists")
    if about["ignore-dev-dependencies"] == licenses["include-dev"]:
        mismatches.append("development dependency settings")
    if about["ignore-build-dependencies"] == licenses["include-build"]:
        mismatches.append("build dependency settings")

    if mismatches:
        print("about.toml and deny.toml disagree on:")
        for mismatch in mismatches:
            print(f"  - {mismatch}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
