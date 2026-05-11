# CoCo Cronjob

Use this orchestrator skill to manage persisted `supercronic` crontab files
that submit CoCo prompts. The active crontabs are managed as a directory, with
one `.crontab` file per schedule timezone.

Useful scripts:

```bash
uv run --script "$COCO_SKILL_DIR/scripts/cronjob_add.py" \
  --id <task-id> \
  --branch <target-branch> \
  --cronexpr "*/15 * * * *" \
  --repeat skip \
  --crontab-dir "$COCO_CRONTAB_DIR" \
  --prompt "<prompt>"

uv run --script "$COCO_SKILL_DIR/scripts/cronjob_add.py" \
  --id <task-id> \
  --target <target-branch> \
  --cronexpr "0,15,30,45 * * * *" \
  --repeat serial \
  --timezone "${TZ:-UTC}" \
  --crontab-dir "$COCO_CRONTAB_DIR" \
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
- By default, installed runner scripts, task config, task state, logs, and
  managed crontab snapshots are stored under `$COCO_SKILL_PERSIST_DIR`. The
  Docker entrypoint derives `COCO_CRONTAB_DIR` from that persistent root,
  restores managed cron files from snapshots, and starts supervised
  `supercronic` processes, so mounting `/data` is enough to preserve schedules
  across container rebuilds.
- The runner submits work with `coco prompt --async --json --branch <branch>
  <prompt>` and records the latest prompt job id in the task state file.
- Use `--timezone <zone>` only when the cron implementation supports
  `CRON_TZ`. The `supercronic` path groups managed jobs into one crontab file
  per schedule timezone because `supercronic` treats `CRON_TZ` as file-wide.
  Jobs without `--timezone` use the container `TZ` via `local.crontab`.
- Use `--dry-run` before changing managed crontab files when reviewing the
  exact managed block matters.

Example:

```bash
uv run --script "$COCO_SKILL_DIR/scripts/cronjob_add.py" \
  --id daily-review \
  --branch main \
  --cronexpr "0 9 * * *" \
  --repeat skip \
  --crontab-dir "$COCO_CRONTAB_DIR" \
  --prompt "Review open work, summarize risks, and propose the next concrete step."
```
