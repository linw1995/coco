from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SEND_SCRIPT = ROOT / "scripts" / "telegram_send.py"
EDIT_SCRIPT = ROOT / "scripts" / "telegram_edit.py"


def load_script(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"failed to load script: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TelegramSendScriptTests(unittest.TestCase):
    def test_split_message_chunks_by_telegram_limit(self) -> None:
        module = load_script(SEND_SCRIPT)
        message = ("a" * module.MESSAGE_LIMIT) + "b"

        self.assertEqual(module.split_message(""), [""])
        self.assertEqual(module.split_message(message), ["a" * module.MESSAGE_LIMIT, "b"])

    def test_resolve_token_prefers_explicit_token_then_environment(self) -> None:
        module = load_script(SEND_SCRIPT)

        with mock.patch.dict(os.environ, {"COCO_TELEGRAM_BOT_TOKEN": "env-token"}):
            self.assertEqual(module.resolve_token("explicit-token"), "explicit-token")
            self.assertEqual(module.resolve_token(None), "env-token")

    def test_resolve_token_fails_closed_when_missing(self) -> None:
        module = load_script(SEND_SCRIPT)

        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(SystemExit, "Telegram token is missing"):
                module.resolve_token(None)

    def test_main_sends_each_chunk_to_each_chat_without_network(self) -> None:
        module = load_script(SEND_SCRIPT)
        calls = []

        def fake_post_api(token: str, method: str, payload: dict) -> dict:
            calls.append({"token": token, "method": method, "payload": payload})
            return {
                "chat": {"id": int(payload["chat_id"])},
                "message_id": len(calls),
            }

        message = ("a" * module.MESSAGE_LIMIT) + "b"
        argv = [
            "telegram_send.py",
            "--chat-id",
            "100, 200",
            "--reply-to",
            "123",
            "--message",
            message,
            "--token",
            "test-token",
        ]
        stdout = io.StringIO()

        with mock.patch.object(sys, "argv", argv):
            with mock.patch.object(module, "post_api", fake_post_api):
                with contextlib.redirect_stdout(stdout):
                    result = module.main()

        output = json.loads(stdout.getvalue())
        self.assertEqual(result, 0)
        self.assertEqual(
            [call["payload"]["chat_id"] for call in calls],
            ["100", "100", "200", "200"],
        )
        self.assertEqual([call["method"] for call in calls], ["sendMessage"] * 4)
        self.assertEqual([call["token"] for call in calls], ["test-token"] * 4)
        self.assertEqual(calls[0]["payload"]["reply_parameters"], {"message_id": 123})
        self.assertNotIn("reply_parameters", calls[1]["payload"])
        self.assertEqual(calls[0]["payload"]["text"], "a" * module.MESSAGE_LIMIT)
        self.assertEqual(calls[1]["payload"]["text"], "b")
        self.assertEqual(
            output,
            {
                "sent": [
                    {"chat_id": 100, "message_id": 1},
                    {"chat_id": 100, "message_id": 2},
                    {"chat_id": 200, "message_id": 3},
                    {"chat_id": 200, "message_id": 4},
                ]
            },
        )


class TelegramEditScriptTests(unittest.TestCase):
    def test_main_edits_message_without_network(self) -> None:
        module = load_script(EDIT_SCRIPT)
        calls = []

        def fake_post_api(token: str, method: str, payload: dict) -> dict:
            calls.append({"token": token, "method": method, "payload": payload})
            return {
                "chat": {"id": int(payload["chat_id"])},
                "message_id": payload["message_id"],
            }

        argv = [
            "telegram_edit.py",
            "--chat-id",
            "100",
            "--message-id",
            "42",
            "--text",
            "Updated text",
            "--token",
            "test-token",
        ]
        stdout = io.StringIO()

        with mock.patch.object(sys, "argv", argv):
            with mock.patch.object(module, "post_api", fake_post_api):
                with contextlib.redirect_stdout(stdout):
                    result = module.main()

        self.assertEqual(result, 0)
        self.assertEqual(
            calls,
            [
                {
                    "token": "test-token",
                    "method": "editMessageText",
                    "payload": {
                        "chat_id": "100",
                        "message_id": 42,
                        "text": "Updated text",
                    },
                }
            ],
        )
        self.assertEqual(json.loads(stdout.getvalue()), {"chat_id": 100, "message_id": 42})


if __name__ == "__main__":
    unittest.main()
