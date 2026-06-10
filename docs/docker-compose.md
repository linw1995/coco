# Docker Compose Deployment

This guide describes the Docker Compose deployment path for CoCo. Compose runs
the long-lived `coco daemon serve` process, `.env` stores host-specific
deployment settings and optional secrets, and `./.coco-data` persists config,
OAuth cache, memory store, skill data, cronjob state, and logs.

## Layout

Deploy from the repository root:

```text
.
├── .env
├── .coco-data/
│   ├── config.toml
│   ├── .config/
│   ├── store/
│   ├── logs/
│   └── skills/
├── .coco-workspace/
├── docker-compose.yaml
└── docs/
```

Do not commit `.env`. The repository only tracks `.env.example`.

## Prepare `.env`

Copy the template and write the current host uid/gid into it:

```bash
cp .env.example .env
${EDITOR:-vi} .env
```

Set `COCO_UID` and `COCO_GID` in `.env` before starting Compose. You can get
the correct values with:

```bash
id -u
id -g
```

Use `.env.example` as the source of truth for available deployment variables.
At minimum, review the image, host port, data directory, workspace directory,
store path, timezone, runtime uid/gid, and optional ChatGPT or Telegram
secrets.

`COCO_UID` and `COCO_GID` must match the current host user. CoCo uses them to
write bind-mounted paths as the host user. Prefer these variables over Docker's
`--user`: the entrypoint needs to start as root, prepare passwd/group entries
and `/data` ownership, then drop privileges before running CoCo and
`supercronic`.

## Prepare Persistent Data

```bash
mkdir -p .coco-data .coco-workspace
```

The image uses these container paths:

- `/data`: config, store, OAuth cache, skill data, and logs.
- `/workspace`: workspace for the unified exec tool.
- `/data/skills`: skill persistence root.
- `/data/skills/orchestrator/cronjob/data/install/crontabs`: active crontab
  files for the cronjob skill.

Compose reads `.env`, mounts `${COCO_DATA_DIR}` to `/data`, and mounts
`${COCO_WORKSPACE_DIR}` to `/workspace`.

## Configure ChatGPT Provider

Create the default provider profile before starting Compose or generating
credentials:

```bash
cat > .coco-data/config.toml <<'EOF'
[providers.gpt-subscription]
provider = "chatgpt"
default_model = "gpt-5.5"
reasoning_level = "high"
service_tier = "fast"
EOF
```

The `chatgpt` provider uses ChatGPT subscription access, not the OpenAI API-key
flow. Do not set `COCO_API_KEY` for `chatgpt`.
`reasoning_level` accepts GPT reasoning levels such as `low`, `medium`, `high`,
and `xhigh`. `service_tier = "fast"` requests the priority GPT tier.

## Get ChatGPT OAuth Cache

Generate credentials in a throwaway Compose data directory, then copy only the
OAuth cache into the persistent data directory. This keeps temporary auth
sessions, prompt jobs, and workspace files out of the long-lived Compose
environment.

Run the following commands in the same shell:

```bash
auth_data_dir="$(mktemp -d)"
auth_workspace_dir="$(mktemp -d)"
cp .coco-data/config.toml "${auth_data_dir}/config.toml"
```

Create the session anchor in the temporary data directory:

```bash
COCO_DATA_DIR="${auth_data_dir}" \
COCO_WORKSPACE_DIR="${auth_workspace_dir}" \
docker compose run --rm --no-deps \
  -e COCO_START_CRON=0 \
  coco \
  coco session create \
    --provider-profile gpt-subscription \
    --system-prompt "You are a pragmatic coding assistant."
```

Then trigger the first provider request:

```bash
COCO_DATA_DIR="${auth_data_dir}" \
COCO_WORKSPACE_DIR="${auth_workspace_dir}" \
docker compose run --rm --no-deps \
  -e COCO_START_CRON=0 \
  coco \
  coco job --branch main "Reply with OK after authentication succeeds."
```

When the command prints the verification URL and device code, finish the login
flow in the browser. Then install the OAuth cache into the persistent data
directory and remove the temporary directories:

```bash
mkdir -p .coco-data/.config/chatgpt
cp "${auth_data_dir}/.config/chatgpt/auth.json" \
  .coco-data/.config/chatgpt/auth.json
rm -rf "${auth_data_dir}" "${auth_workspace_dir}"
```

The persistent OAuth cache is stored at:

```text
.coco-data/.config/chatgpt/auth.json
```

If you prefer explicit environment secrets instead of OAuth cache lookup, read
the values and write them into `.env`:

```bash
jq -r '.access_token' .coco-data/.config/chatgpt/auth.json
jq -r '.account_id // empty' .coco-data/.config/chatgpt/auth.json
```

Then add secret references to `.coco-data/config.toml`:

```toml
[providers.gpt-subscription.secrets]
access_token = "${CHATGPT_ACCESS_TOKEN}"
account_id = "${CHATGPT_ACCOUNT_ID}"
```

When `.env` contains an empty token, CoCo can still fall back to device-code
OAuth. If the config references an environment variable that is completely
missing at runtime, CoCo treats that as a configuration error.

## Configure Telegram Channel

After creating a Telegram bot, write its token to `.env`:

```dotenv
COCO_TELEGRAM_BOT_TOKEN=123456:replace-with-your-bot-token
```

Fetch the chat id:

```bash
curl "https://api.telegram.org/bot${COCO_TELEGRAM_BOT_TOKEN}/getUpdates"
```

Append Telegram channel config to `.coco-data/config.toml`:

```toml
[channels.telegram]
enabled = true
token = "${COCO_TELEGRAM_BOT_TOKEN}"
branch = "main"
poll_timeout_secs = 30
allowed_chat_ids = ["123456789"]
```

Set `allowed_chat_ids` explicitly when possible. If it is empty or omitted, the
bot accepts messages from any chat that can reach it.

The Telegram channel only receives messages. Outbound replies must be sent by
the built-in runner-role `telegram` skill, so configure the target session with
tools that can call that skill.

## Start and Stop

Start:

```bash
docker compose up -d
```

Follow logs:

```bash
docker compose logs -f coco
```

Stop:

```bash
docker compose down
```

Update the image:

```bash
docker compose pull
docker compose up -d
```

## Local Image Builds

The published GHCR image is a multi-arch image built by CD for `linux/amd64`
and `linux/arm64`. For normal deployments, use:

```dotenv
COCO_IMAGE=ghcr.io/linw1995/coco:latest
```

Manual local builds are mainly for development or debugging:

```bash
nix build .#coco-image
docker load < result
docker compose up -d
```

`coco-image` always builds a Linux container image. Linux hosts use their own
system by default. Darwin hosts map to the matching Linux image target, so
`aarch64-darwin` selects `aarch64-linux` and `x86_64-darwin` selects
`x86_64-linux`.

Use explicit image outputs when you need a specific architecture:

```bash
nix build .#coco-image-linux-amd64
nix build .#coco-image-linux-arm64
```

Run manual image builds on Linux, or on a host whose Nix setup can build the
selected Linux target. For example, a macOS host needs a Linux builder before it
can build `.#coco-image-linux-arm64` locally.

## Cronjob Skill

The entrypoint supervises one `supercronic -inotify` process per active
crontab file. It scans the crontab directory periodically, so adding the first
task for a new timezone does not require a container restart.
When an installed cronjob runner already exists, startup refreshes
`data/install/cronjob_run.py` from the current built-in `cronjob` skill before
starting `supercronic`.

Persistent cronjob paths live under `/data/skills/orchestrator/cronjob`:

- `data/install/crontabs/`: CoCo-managed crontab files, grouped by schedule
  timezone and consumed directly by `supercronic`.
- `data/install/cronjob_run.py`: installed runner script refreshed from the
  current built-in skill at startup when present.
- `data/state/`: runner state.
- `data/logs/`: cronjob logs.

As long as `${COCO_DATA_DIR}` is persistent, cronjob config and state survive
container rebuilds.

The Docker path uses direct `supercronic` crontab files. Current `supercronic`
scheduling treats `CRON_TZ` as a file-wide timezone, so the cronjob skill
groups managed tasks by schedule timezone and writes one file-level `CRON_TZ`
per crontab file. Tasks without `--timezone` go to `local.crontab` and use the
container `TZ`.

For one-shot CLI commands, set `COCO_START_CRON=0` so temporary containers do
not start another scheduler:

```bash
docker compose run --rm \
  -e COCO_START_CRON=0 \
  coco \
  coco --help
```

## Common Checks

Check daemon status:

```bash
docker compose ps
docker compose logs --tail=100 coco
```

Check the active crontab files:

```bash
docker compose exec coco sh -lc '
  crontab_dir="${COCO_SKILL_PERSIST_ROOT:-/data/skills}/orchestrator/cronjob/data/install/crontabs"
  for file in "$crontab_dir"/*.crontab; do
    test -f "$file" || continue
    printf "== %s ==\n" "$file"
    sed -n "1,120p" "$file"
  done
'
```

Check the `supercronic` process:

```bash
docker compose exec coco sh -lc 'ps -ef | grep supercronic | grep -v grep'
```

Check the runtime identity:

```bash
docker compose exec coco id
```
