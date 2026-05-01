# CoCo

[![CI](https://img.shields.io/github/actions/workflow/status/linw1995/coco/CI.yaml?branch=main&label=CI)](https://github.com/linw1995/coco/actions/workflows/CI.yaml)
[![CD](https://img.shields.io/github/actions/workflow/status/linw1995/coco/CD.yaml?branch=main&label=CD)](https://github.com/linw1995/coco/actions/workflows/CD.yaml)
[![codecov](https://codecov.io/gh/linw1995/coco/graph/badge.svg?token=A99GUI386I)](https://codecov.io/gh/linw1995/coco)
[![ghcr](https://img.shields.io/badge/ghcr.io-linw1995%2Fcoco-blue?logo=github)](https://github.com/linw1995/coco/pkgs/container/coco)

An AI Copilot.

## Docker

Build and load the image with Nix:

```bash
nix build .#coco-image
docker load < result
```

The image starts `coco-cli daemon serve` by default. It listens on port
`17667` and stores runtime data under `/data`.

```bash
mkdir -p .coco-data
docker run --rm -it \
  --name coco \
  -p 17667:17667 \
  -v "$PWD/.coco-data:/data" \
  coco:latest
```

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

The recommended Docker flow is to run the ChatGPT OAuth device flow once and
persist its cache under `/data/.config`.

```bash
docker run --rm -it \
  -e XDG_CONFIG_HOME=/data/.config \
  -v "$PWD/.coco-data:/data" \
  coco:latest \
  coco-cli session create \
    --provider-profile gpt-subscription \
    --system-prompt "You are a pragmatic coding assistant."
```

When prompted, open the displayed verification URL, enter the device code, and
finish signing in with the ChatGPT account that has the subscription. The OAuth
cache is written to:

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
  -e CHATGPT_ACCESS_TOKEN="$CHATGPT_ACCESS_TOKEN" \
  -e CHATGPT_ACCOUNT_ID="$CHATGPT_ACCOUNT_ID" \
  coco:latest
```

Create the first session:

```bash
docker run --rm -it \
  -v "$PWD/.coco-data:/data" \
  -e CHATGPT_ACCESS_TOKEN="$CHATGPT_ACCESS_TOKEN" \
  -e CHATGPT_ACCOUNT_ID="$CHATGPT_ACCOUNT_ID" \
  coco:latest \
  coco-cli session create \
    --provider-profile gpt-subscription \
    --system-prompt "You are a pragmatic coding assistant."
```

If there is only one provider profile in `config.toml`, `session create` can
omit `--provider-profile`.

### Telegram

CoCo can also poll Telegram and route incoming messages to a CoCo branch.

Create a Telegram bot and get its token:

1. Open Telegram and talk to `@BotFather`.
2. Run `/newbot`.
3. Follow the prompts and copy the bot token.
4. Export it as `TELEGRAM_BOT_TOKEN`.

```bash
export TELEGRAM_BOT_TOKEN="123456:replace-with-your-bot-token"
```

Get the chat id you want to allow:

1. Send a message to your bot from the Telegram chat.
2. Query Telegram updates.
3. Read `message.chat.id` from the JSON response.

```bash
curl "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getUpdates"
```

Add Telegram configuration to `.coco-data/config.toml`:

```bash
cat >> .coco-data/config.toml <<'EOF'

[channels.telegram]
enabled = true
token = "${TELEGRAM_BOT_TOKEN}"
branch = "main"
poll_timeout_secs = 30
allowed_chat_ids = ["123456789"]
EOF
```

`allowed_chat_ids` is recommended. If it is empty or omitted, the bot accepts
messages from any chat that can reach it.

Start the daemon with both ChatGPT and Telegram secrets:

```bash
docker run --rm -it \
  --name coco \
  -p 17667:17667 \
  -v "$PWD/.coco-data:/data" \
  -e CHATGPT_ACCESS_TOKEN="$CHATGPT_ACCESS_TOKEN" \
  -e CHATGPT_ACCOUNT_ID="$CHATGPT_ACCOUNT_ID" \
  -e TELEGRAM_BOT_TOKEN="$TELEGRAM_BOT_TOKEN" \
  coco:latest
```
