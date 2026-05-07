# CoCo

[![CI](https://img.shields.io/github/actions/workflow/status/linw1995/coco/CI.yaml?branch=main&label=CI)](https://github.com/linw1995/coco/actions/workflows/CI.yaml)
[![CD](https://img.shields.io/github/actions/workflow/status/linw1995/coco/CD.yaml?branch=main&label=CD)](https://github.com/linw1995/coco/actions/workflows/CD.yaml)
[![codecov](https://codecov.io/gh/linw1995/coco/graph/badge.svg?token=A99GUI386I)](https://codecov.io/gh/linw1995/coco)
[![ghcr](https://img.shields.io/badge/ghcr.io-linw1995%2Fcoco-blue?logo=github)](https://github.com/linw1995/coco/pkgs/container/coco)

An AI Copilot.

## Acknowledgements

Thanks to `codex` and `bub` for the inspiration and implementation references.

## Docker

Build and load the image with Nix:

```bash
nix build .#coco-image
docker load < result
```

The image starts `coco daemon serve` by default. It listens on port
`17667`, stores CoCo config and runtime data under `/data`, and exposes
`/workspace` as the isolated exec tool workspace.

Skill data is persisted under `/data/skills`.

```bash
mkdir -p .coco-data
docker run --rm -it \
  --name coco \
  -p 17667:17667 \
  -v "$PWD/.coco-data:/data" \
  -v "$PWD:/workspace" \
  -e TZ="${TZ:-UTC}" \
  -e COCO_UID="$(id -u)" \
  -e COCO_GID="$(id -g)" \
  coco:latest
```

Set `COCO_UID` and `COCO_GID` when bind-mounting host directories. The
entrypoint starts as root, prepares the runtime user/group, fixes ownership for
`/data`, and then runs both `supercronic` and CoCo as that uid/gid.

The image starts `supercronic` before the default `coco daemon serve` command.
Cronjob skill state, logs, runner scripts, managed snapshots, and the active
crontab file are persisted under `/data/skills`; the container does not rely on
system crontab spool state. Set `TZ` to the host timezone so cron schedules,
logs, and CoCo agree on wall-clock time. You can also mount the host localtime
file when your host exposes it:

```bash
docker run --rm -it \
  --name coco \
  -p 17667:17667 \
  -v "$PWD/.coco-data:/data" \
  -v "$PWD:/workspace" \
  -v /etc/localtime:/etc/localtime:ro \
  -e TZ="${TZ:-UTC}" \
  -e COCO_UID="$(id -u)" \
  -e COCO_GID="$(id -g)" \
  coco:latest
```

Set `COCO_START_CRON=0` when you only need one-shot CLI commands and do not
want the container entrypoint to start `supercronic`. Prefer `COCO_UID` and
`COCO_GID` over forcing Docker's `--user`; if the container starts non-root, the
entrypoint cannot create passwd/group entries or repair mounted directory
ownership before CoCo starts.

### ChatGPT Subscription

CoCo uses the `chatgpt` provider for ChatGPT subscription access. This is not
the OpenAI API-key flow. Do not set `COCO_API_KEY` for `chatgpt`; that variable
is for API-key providers and is rejected by the `chatgpt` provider.

Create a provider profile in the mounted data directory:

```bash
cat > .coco-data/config.toml <<'EOF'
[providers.gpt-subscription]
provider = "chatgpt"
default_model = "gpt-5.4"
EOF
```

#### Getting ChatGPT Secrets

The recommended Docker flow is to create the session config first, then run one
prompt to start the ChatGPT OAuth device flow. `session create` only records the
session anchor; the verification URL and device code appear on the first
provider request.

```bash
docker run --rm -it \
  -v "$PWD/.coco-data:/data" \
  -v "$PWD:/workspace" \
  -e COCO_START_CRON=0 \
  -e COCO_UID="$(id -u)" \
  -e COCO_GID="$(id -g)" \
  coco:latest \
  coco session create \
    --provider-profile gpt-subscription \
    --system-prompt "You are a pragmatic coding assistant."
```

Then trigger a single ChatGPT request:

```bash
docker run --rm -it \
  -v "$PWD/.coco-data:/data" \
  -v "$PWD:/workspace" \
  -e COCO_START_CRON=0 \
  -e COCO_UID="$(id -u)" \
  -e COCO_GID="$(id -g)" \
  coco:latest \
  coco prompt --branch main "Reply with OK after authentication succeeds."
```

When the prompt command displays the verification URL, open it, enter the device
code, and finish signing in with the ChatGPT account that has the subscription.
The OAuth cache is written to:

```text
.coco-data/.config/chatgpt/auth.json
```

You can then export the token values from that file:

```bash
export CHATGPT_ACCESS_TOKEN="$(
  jq -r '.access_token' .coco-data/.config/chatgpt/auth.json
)"
export CHATGPT_ACCOUNT_ID="$(
  jq -r '.account_id // empty' .coco-data/.config/chatgpt/auth.json
)"
```

`CHATGPT_ACCESS_TOKEN` is required when using the explicit-token configuration.
`CHATGPT_ACCOUNT_ID` is optional for many accounts, but keep it when the OAuth
cache has one, especially if the ChatGPT login can access multiple accounts or
workspaces.

To use explicit secrets instead of OAuth cache lookup, add the secret references
to `config.toml`:

```bash
cat >> .coco-data/config.toml <<'EOF'

[providers.gpt-subscription.secrets]
access_token = "${CHATGPT_ACCESS_TOKEN}"
account_id = "${CHATGPT_ACCOUNT_ID}"
EOF
```

Start the daemon with the subscription secrets:

```bash
docker run --rm -it \
  --name coco \
  -p 17667:17667 \
  -v "$PWD/.coco-data:/data" \
  -v "$PWD:/workspace" \
  -e CHATGPT_ACCESS_TOKEN="$CHATGPT_ACCESS_TOKEN" \
  -e CHATGPT_ACCOUNT_ID="$CHATGPT_ACCOUNT_ID" \
  coco:latest
```

Create the first session:

```bash
docker run --rm -it \
  -v "$PWD/.coco-data:/data" \
  -v "$PWD:/workspace" \
  -e CHATGPT_ACCESS_TOKEN="$CHATGPT_ACCESS_TOKEN" \
  -e CHATGPT_ACCOUNT_ID="$CHATGPT_ACCOUNT_ID" \
  coco:latest \
  coco session create \
    --provider-profile gpt-subscription \
    --system-prompt "You are a pragmatic coding assistant."
```

If there is only one provider profile in `config.toml`, `session create` can
omit `--provider-profile`.

### Telegram

CoCo can poll Telegram and route incoming messages to a CoCo branch. The
Telegram channel is receive-only: outbound replies must be sent by the built-in
runner-role `telegram` skill. Telegram inbound prompts automatically include
the target `chat_id` and `reply_to_message_id`. Configure the target session
with the tools needed to call that skill for the reply.

Create a Telegram bot and get its token:

1. Open Telegram and talk to `@BotFather`.
2. Run `/newbot`.
3. Follow the prompts and copy the bot token.
4. Export it as `COCO_TELEGRAM_BOT_TOKEN`.

```bash
export COCO_TELEGRAM_BOT_TOKEN="123456:replace-with-your-bot-token"
```

Get the chat id you want to allow:

1. Send a message to your bot from the Telegram chat.
2. Query Telegram updates.
3. Read `message.chat.id` from the JSON response.

```bash
curl "https://api.telegram.org/bot${COCO_TELEGRAM_BOT_TOKEN}/getUpdates"
```

Add Telegram configuration to `.coco-data/config.toml`:

```bash
cat >> .coco-data/config.toml <<'EOF'

[channels.telegram]
enabled = true
token = "${COCO_TELEGRAM_BOT_TOKEN}"
branch = "main"
poll_timeout_secs = 30
allowed_chat_ids = ["123456789"]
EOF
```

`allowed_chat_ids` is recommended. If it is empty or omitted, the bot accepts
messages from any chat that can reach it.

Start the daemon with both ChatGPT and Telegram secrets. The `telegram` skill
uses `COCO_TELEGRAM_BOT_TOKEN` for Bot API calls:

```bash
docker run --rm -it \
  --name coco \
  -p 17667:17667 \
  -v "$PWD/.coco-data:/data" \
  -v "$PWD:/workspace" \
  -e CHATGPT_ACCESS_TOKEN="$CHATGPT_ACCESS_TOKEN" \
  -e CHATGPT_ACCOUNT_ID="$CHATGPT_ACCOUNT_ID" \
  -e COCO_TELEGRAM_BOT_TOKEN="$COCO_TELEGRAM_BOT_TOKEN" \
  coco:latest
```
