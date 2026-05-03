# CoCo Scheduler

Use the injected `coco` command through `exec_command` to manage scheduled
inbound prompts. Scheduler tasks are persisted in the CoCo store and are
claimed by the daemon scheduler channel when `next_run_at` is due.

Useful commands:

```bash
coco scheduler list
coco scheduler list --json
coco scheduler show <task-id>
coco scheduler add --id <task-id> --branch <branch> --interval-secs <seconds> --next-run-at <timestamp> "<prompt>"
coco scheduler add --id <task-id> --branch <branch> --interval-secs <seconds> --initial-delay-secs <seconds> "<prompt>"
coco scheduler update <task-id> --prompt "<prompt>"
coco scheduler update <task-id> --interval-secs <seconds> --next-run-at <timestamp>
coco scheduler update <task-id> --enable
coco scheduler update <task-id> --disable
coco scheduler delete <task-id>
```

Rules:

- Prefer stable, descriptive task ids such as `daily-review` or
  `weekly-pr-triage`.
- Always confirm the target branch exists before registering a task.
- Use RFC 3339 UTC timestamps for `--next-run-at`, for example
  `2026-05-03T10:00:00Z`.
- Keep scheduled prompts self-contained. Include the expected output and any
  branch-specific context the future run will need.
- Use `--json` when inspecting tasks for automation or when comparing stored
  fields before an update.
- Disable a task before large edits when avoiding a near-term run matters.

Example:

```bash
coco scheduler add \
  --id daily-review \
  --branch main \
  --interval-secs 86400 \
  --next-run-at 2026-05-04T01:00:00Z \
  "Review open work, summarize risks, and propose the next concrete step."
```
