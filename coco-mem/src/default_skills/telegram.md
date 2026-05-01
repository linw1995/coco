# Telegram Skill

Agent-facing execution guide for Telegram outbound communication.

Use this skill whenever you need to send, reply to, or edit Telegram messages.
Assume a Telegram bot token is available from `COCO_TELEGRAM_TOKEN`,
`TELEGRAM_BOT_TOKEN`, or `BUB_TELEGRAM_TOKEN` unless an explicit `--token` is
provided.

## Required Inputs

Collect these before execution:

- `chat_id` is required for every operation.
- `message_id` is required for edit operations.
- `reply_to_message_id` is required when replying to an inbound message.
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

## Command Templates

Paths are relative to this skill directory.

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
