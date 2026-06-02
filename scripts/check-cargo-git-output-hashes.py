#!/usr/bin/env python3
import json
import sys
import tomllib
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[1]
CARGO_LOCK = ROOT / "Cargo.lock"
OUTPUT_HASHES = ROOT / "nix" / "cargo-git-output-hashes.json"


def cargo_git_sources() -> set[str]:
    lock = tomllib.loads(CARGO_LOCK.read_text())
    sources = set()

    for package in lock.get("package", []):
        source = package.get("source")
        if source is not None and source.startswith("git+"):
            # Crane percent-decodes Cargo.lock git sources before outputHashes lookup.
            sources.add(unquote(source))

    return sources


def output_hash_sources() -> set[str]:
    relative_path = OUTPUT_HASHES.relative_to(ROOT)
    output_hashes = json.loads(OUTPUT_HASHES.read_text())

    if not isinstance(output_hashes, dict):
        print(
            f"error: {relative_path} must contain a JSON object",
            file=sys.stderr,
        )
        sys.exit(1)

    for source, output_hash in output_hashes.items():
        if not source.startswith("git+"):
            print(
                f"error: {relative_path} contains a non-git source: {source}",
                file=sys.stderr,
            )
            sys.exit(1)

        if not isinstance(output_hash, str) or not output_hash.startswith("sha256-"):
            print(
                f"error: {relative_path} has an invalid hash for {source}",
                file=sys.stderr,
            )
            sys.exit(1)

    return set(output_hashes)


def main() -> int:
    cargo_sources = cargo_git_sources()
    output_hashes = output_hash_sources()

    missing = sorted(cargo_sources - output_hashes)
    stale = sorted(output_hashes - cargo_sources)

    if not missing and not stale:
        return 0

    if missing:
        print(
            "Missing crane outputHashes entries for Cargo git dependencies:",
            file=sys.stderr,
        )
        for source in missing:
            print(f"  {source}", file=sys.stderr)

    if stale:
        print(
            "Stale crane outputHashes entries not present in Cargo.lock:",
            file=sys.stderr,
        )
        for source in stale:
            print(f"  {source}", file=sys.stderr)

    return 1


if __name__ == "__main__":
    sys.exit(main())
