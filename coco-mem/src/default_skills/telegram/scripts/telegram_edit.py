#!/usr/bin/env python3
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

TOKEN_ENV_NAMES = ("COCO_TELEGRAM_BOT_TOKEN",)
BASE_URL_ENV_NAME = "TELEGRAM_BASE_URL"
DEFAULT_BASE_URL = "https://api.telegram.org"


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


def api_base_url() -> str:
    return os.environ.get(BASE_URL_ENV_NAME, DEFAULT_BASE_URL).rstrip("/")


def post_api(token: str, method: str, payload: dict) -> dict:
    request = urllib.request.Request(
        f"{api_base_url()}/bot{token}/{method}",
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Edit a Telegram message.")
    parser.add_argument("--chat-id", "-c", required=True)
    parser.add_argument("--message-id", required=True, type=int)
    parser.add_argument("--text", required=True)
    parser.add_argument("--token", "-t")
    args = parser.parse_args()

    result = post_api(
        resolve_token(args.token),
        "editMessageText",
        {
            "chat_id": args.chat_id,
            "message_id": args.message_id,
            "text": args.text,
        },
    )
    print(
        json.dumps(
            {
                "chat_id": result["chat"]["id"],
                "message_id": result["message_id"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
