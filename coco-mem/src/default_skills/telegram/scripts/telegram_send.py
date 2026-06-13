#!/usr/bin/env python3
import argparse
import json
import mimetypes
import os
from pathlib import Path
import sys
import urllib.error
import urllib.request
import uuid

CAPTION_LIMIT = 1024
MESSAGE_LIMIT = 4096
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


def split_message(text: str) -> list[str]:
    if not text:
        return [""]
    return [
        text[index : index + MESSAGE_LIMIT]
        for index in range(0, len(text), MESSAGE_LIMIT)
    ]


def split_caption(text: str) -> tuple[str | None, list[str]]:
    if not text:
        return None, []
    caption = text[:CAPTION_LIMIT]
    remaining = text[CAPTION_LIMIT:]
    if not remaining:
        return caption, []
    return caption, split_message(remaining)


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


def post_multipart(
    token: str,
    method: str,
    fields: dict[str, str],
    file_field: str,
    file_path: Path,
) -> dict:
    boundary = f"----coco-telegram-{uuid.uuid4().hex}"
    content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    body_parts = []
    for name, value in fields.items():
        body_parts.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode(
                    "utf-8"
                ),
                value.encode("utf-8"),
                b"\r\n",
            ]
        )

    body_parts.extend(
        [
            f"--{boundary}\r\n".encode("utf-8"),
            (
                f'Content-Disposition: form-data; name="{file_field}"; '
                f'filename="{file_path.name}"\r\n'
            ).encode("utf-8"),
            f"Content-Type: {content_type}\r\n\r\n".encode("utf-8"),
            file_path.read_bytes(),
            b"\r\n",
            f"--{boundary}--\r\n".encode("utf-8"),
        ]
    )
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=b"".join(body_parts),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
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


def result_summary(result: dict) -> dict:
    return {
        "chat_id": result["chat"]["id"],
        "message_id": result["message_id"],
    }


def reply_parameters(reply_to: str | None) -> dict[str, int] | None:
    if not reply_to:
        return None
    return {"message_id": int(reply_to)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Send Telegram messages.")
    parser.add_argument("--chat-id", "-c", required=True)
    parser.add_argument("--message", "-m")
    parser.add_argument("--reply-to", "-r")
    attachment = parser.add_mutually_exclusive_group()
    attachment.add_argument("--photo", "--image", dest="photo")
    attachment.add_argument("--document", "--file", dest="document")
    attachment.add_argument("--voice")
    parser.add_argument("--token", "-t")
    args = parser.parse_args()

    if (
        args.message is None
        and args.photo is None
        and args.document is None
        and args.voice is None
    ):
        parser.error(
            "--message is required when no --photo, --document, or --voice is provided"
        )

    token = resolve_token(args.token)
    sent = []
    for chat_id in [
        value.strip() for value in args.chat_id.split(",") if value.strip()
    ]:
        reply_to = args.reply_to
        if args.photo or args.document or args.voice:
            if args.photo:
                file_field = "photo"
                method = "sendPhoto"
                file_path = Path(args.photo)
            elif args.document:
                file_field = "document"
                method = "sendDocument"
                file_path = Path(args.document)
            else:
                file_field = "voice"
                method = "sendVoice"
                file_path = Path(args.voice)
            if not file_path.is_file():
                raise SystemExit(f"Telegram attachment does not exist: {file_path}")
            caption, text_chunks = split_caption(args.message or "")
            fields = {"chat_id": chat_id}
            if caption is not None:
                fields["caption"] = caption
            if reply_to:
                fields["reply_parameters"] = json.dumps(reply_parameters(reply_to))
            result = post_multipart(token, method, fields, file_field, file_path)
            sent.append(result_summary(result))
            reply_to = None
            for chunk in text_chunks:
                payload = {"chat_id": chat_id, "text": chunk}
                result = post_api(token, "sendMessage", payload)
                sent.append(result_summary(result))
        else:
            for chunk in split_message(args.message or ""):
                payload = {"chat_id": chat_id, "text": chunk}
                parameters = reply_parameters(reply_to)
                if parameters:
                    payload["reply_parameters"] = parameters
                result = post_api(token, "sendMessage", payload)
                sent.append(result_summary(result))
                reply_to = None

    print(json.dumps({"sent": sent}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
