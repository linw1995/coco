# CoCo Cronjob

Use this orchestrator skill to manage host cron entries that submit CoCo
prompts. It relies on the system `crontab` command and a persistent runner
script installed by the skill script.

Useful scripts:

```bash
uv run --script "$COCO_SKILL_DIR/scripts/cronjob_add.py" \
  --id <task-id> \
  --branch <target-branch> \
  --cronexpr "*/15 * * * *" \
  --repeat skip \
  --prompt "<prompt>"

uv run --script "$COCO_SKILL_DIR/scripts/cronjob_add.py" \
  --id <task-id> \
  --target <target-branch> \
  --cronexpr "0,15,30,45 * * * *" \
  --repeat serial \
  --timezone "${TZ:-UTC}" \
  --prompt "<prompt>"
```

Rules:

- Prefer stable, descriptive task ids such as `daily-review` or
  `quarter-hour-health-check`.
- Always confirm the target branch exists before registering a cronjob.
- Cron expressions must have exactly five fields. For safety, the minute field
  must not schedule more often than every 15 minutes.
- Supported repeat policies:
  - `parallel`: always submit a new prompt.
  - `serial`: wait for the previous job for this task to finish before
    submitting the next prompt.
  - `skip`: do not submit a new prompt while the previous job is still queued
    or running.
- The add script is idempotent by task id. Re-running it updates the managed
  crontab block and task config instead of adding a duplicate entry.
- By default, installed runner scripts, task config, task state, logs, and the
  managed crontab snapshot are stored under `$COCO_SKILL_PERSIST_DIR`. The
  Docker image restores managed cron entries from this snapshot before starting
  `crond`, so mounting `/data` is enough to preserve schedules across container
  rebuilds.
- The runner submits work with `coco prompt --async --json --branch <branch>
  <prompt>` and records the latest prompt job id in the task state file.
- Use `--timezone <zone>` only when the host cron implementation supports
  `CRON_TZ`. Docker users should prefer setting the container `TZ` environment
  variable so the cron daemon and CoCo process share the same timezone.
- Use `--dry-run` before changing a host crontab when reviewing the exact
  managed block matters.

Example:

```bash
uv run --script "$COCO_SKILL_DIR/scripts/cronjob_add.py" \
  --id daily-review \
  --branch main \
  --cronexpr "0 9 * * *" \
  --repeat skip \
  --prompt "Review open work, summarize risks, and propose the next concrete step."
```
