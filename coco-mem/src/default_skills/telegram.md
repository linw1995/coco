# Telegram Skill

Agent-facing execution guide for Telegram communication.

Use this skill whenever you need to send, reply to, edit Telegram messages, or
download inbound Telegram attachments. Assume a Telegram bot token is available
from `COCO_TELEGRAM_BOT_TOKEN` unless an explicit `--token` is provided.

## Required Inputs

Collect these before execution:

- `chat_id` is required for every operation.
- `message_id` is required for edit operations.
- `reply_to_message_id` is required when replying to an inbound message.
- `file_id` is required when downloading an inbound attachment.
- Message content is required for send, reply, and edit operations.

## Execution Policy

1. If handling a Telegram message and `reply_to_message_id` is known, send a
   reply message with `--reply-to`.
2. If there is no message to reply to, send a normal message to `chat_id`.
3. For long-running tasks, optionally send one progress message, then edit that
   same message for final status.
4. For multi-line text, pass the content via heredoc command substitution
   instead of embedding raw line breaks in quoted strings.
5. Avoid emitting HTML tags in message content; use Markdown-style plain text.
6. If an inbound Telegram prompt includes image attachment metadata, use
   `file_id` with `telegram_download.py` before answering image-dependent
   requests. Download attachments into the current workspace so downstream
   tools can load the saved files.

## Command Templates

Script paths use `COCO_SKILL_DIR`. Run commands from the current workspace so
relative output paths remain loadable by follow-up tools. `telegram_download.py`
defaults to `COCO_EXEC_WORKSPACE/telegram-downloads` when `--output` and
`--output-dir` are omitted.

```bash
# Send a simple text message.
uv run --script "$COCO_SKILL_DIR/scripts/telegram_send.py" \
  --chat-id "<chat_id>" \
  --message "Message text"

# Send a reply to a specific Telegram message.
uv run --script "$COCO_SKILL_DIR/scripts/telegram_send.py" \
  --chat-id "<chat_id>" \
  --reply-to "<reply_to_message_id>" \
  --message "Reply text"

# Send a multi-line reply.
uv run --script "$COCO_SKILL_DIR/scripts/telegram_send.py" \
  --chat-id "<chat_id>" \
  --reply-to "<reply_to_message_id>" \
  --message "$(cat <<'EOF'
Build finished successfully.

Summary:
- 12 tests passed
- 0 failures
EOF
)"

# Edit an existing Telegram message.
uv run --script "$COCO_SKILL_DIR/scripts/telegram_edit.py" \
  --chat-id "<chat_id>" \
  --message-id "<message_id>" \
  --text "Updated text"

# Download an inbound image attachment by file_id.
uv run --script "$COCO_SKILL_DIR/scripts/telegram_download.py" \
  --file-id "<file_id>"
```

## Script Interface Reference

### `telegram_send.py`

- `--chat-id`, `-c`: required, supports comma-separated ids
- `--message`, `-m`: required
- `--reply-to`, `-r`: optional
- `--token`, `-t`: optional

### `telegram_edit.py`

- `--chat-id`, `-c`: required
- `--message-id`: required
- `--text`: required
- `--token`, `-t`: optional

### `telegram_download.py`

- `--file-id`: required
- `--output`, `-o`: optional explicit output file path
- `--output-dir`: optional directory used when `--output` is omitted
- `--token`, `-t`: optional
