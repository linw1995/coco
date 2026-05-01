#!/usr/bin/env python3
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

MESSAGE_LIMIT = 4096
TOKEN_ENV_NAMES = ("COCO_TELEGRAM_TOKEN", "TELEGRAM_BOT_TOKEN", "BUB_TELEGRAM_TOKEN")


def resolve_token(explicit_token: str | None) -> str:
    if explicit_token:
        return explicit_token
    for name in TOKEN_ENV_NAMES:
        value = os.environ.get(name)
        if value:
            return value
    raise SystemExit(
        "Telegram token is missing. Set COCO_TELEGRAM_TOKEN, TELEGRAM_BOT_TOKEN, "
        "or pass --token."
    )


def split_message(text: str) -> list[str]:
    if not text:
        return [""]
    return [text[index : index + MESSAGE_LIMIT] for index in range(0, len(text), MESSAGE_LIMIT)]


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
        raise SystemExit(f"Telegram API returned an error: {body.get('description', body)}")
    return body["result"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Send Telegram messages.")
    parser.add_argument("--chat-id", "-c", required=True)
    parser.add_argument("--message", "-m", required=True)
    parser.add_argument("--reply-to", "-r")
    parser.add_argument("--token", "-t")
    args = parser.parse_args()

    token = resolve_token(args.token)
    sent = []
    for chat_id in [value.strip() for value in args.chat_id.split(",") if value.strip()]:
        reply_to = args.reply_to
        for chunk in split_message(args.message):
            payload = {"chat_id": chat_id, "text": chunk}
            if reply_to:
                payload["reply_parameters"] = {"message_id": int(reply_to)}
            result = post_api(token, "sendMessage", payload)
            sent.append(
                {
                    "chat_id": result["chat"]["id"],
                    "message_id": result["message_id"],
                }
            )
            reply_to = None

    print(json.dumps({"sent": sent}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
