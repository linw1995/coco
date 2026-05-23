#!/usr/bin/env python3
import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

TOKEN_ENV_NAMES = ("COCO_TELEGRAM_BOT_TOKEN",)


def resolve_token(explicit_token: str | None) -> str:
    if explicit_token:
        return explicit_token
    for name in TOKEN_ENV_NAMES:
        value = os.environ.get(name)
        if value:
            return value
    raise SystemExit(
        "Telegram token is missing. Set COCO_TELEGRAM_BOT_TOKEN or pass --token."
    )


def post_api(token: str, method: str, payload: dict) -> dict:
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        details = error.read().decode("utf-8", errors="replace")
        raise SystemExit(f"Telegram API HTTP error {error.code}: {details}") from error
    except urllib.error.URLError as error:
        raise SystemExit(f"Telegram API request failed: {error}") from error

    if not body.get("ok"):
        raise SystemExit(
            f"Telegram API returned an error: {body.get('description', body)}"
        )
    return body["result"]


def default_output_path(file_path: str, output_dir: str) -> Path:
    name = Path(file_path).name
    if not name:
        raise SystemExit("Telegram file path does not contain a file name.")
    return Path(output_dir) / name


def download_file(token: str, file_path: str, output_path: Path) -> int:
    encoded_path = urllib.parse.quote(file_path, safe="/")
    request = urllib.request.Request(
        f"https://api.telegram.org/file/bot{token}/{encoded_path}",
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
    except urllib.error.HTTPError as error:
        details = error.read().decode("utf-8", errors="replace")
        raise SystemExit(f"Telegram file HTTP error {error.code}: {details}") from error
    except urllib.error.URLError as error:
        raise SystemExit(f"Telegram file request failed: {error}") from error

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(body)
    return len(body)


def main() -> int:
    parser = argparse.ArgumentParser(description="Download a Telegram file by file_id.")
    parser.add_argument("--file-id", required=True)
    parser.add_argument("--output", "-o")
    parser.add_argument("--output-dir", default=".")
    parser.add_argument("--token", "-t")
    args = parser.parse_args()

    token = resolve_token(args.token)
    file_info = post_api(token, "getFile", {"file_id": args.file_id})
    file_path = file_info.get("file_path")
    if not file_path:
        raise SystemExit("Telegram API did not return file_path for this file_id.")

    output_path = Path(args.output) if args.output else default_output_path(
        file_path, args.output_dir
    )
    bytes_written = download_file(token, file_path, output_path)

    print(
        json.dumps(
            {
                "file_id": args.file_id,
                "file_unique_id": file_info.get("file_unique_id"),
                "telegram_file_path": file_path,
                "output_path": str(output_path),
                "bytes": bytes_written,
                "file_size": file_info.get("file_size"),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
