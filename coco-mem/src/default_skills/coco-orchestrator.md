# CoCo Orchestrator Workflow

Use the injected `coco` command through `bash` for branch-aware workflow control.
Default output is human-readable; use `--json` whenever piping to `jq` or scripts.

Useful commands:

```bash
coco session list
coco session get --branch <branch>
coco session get --json --branch <branch>
coco session show <ref>
coco session fork --branch <branch> --from-ref <ref>
coco session pr --branch <branch> --target-branch <branch>
coco session feedback --branch <branch> --prompt "<text>"
coco session merge --branch <branch> --target-branch <branch> --prompt "<text>"
coco preset list
coco preset show --name <preset>
coco preset set --name <preset> [session options]
coco session rebase --preset <preset> --branch <branch>
coco prompt --branch <branch> "<text>"
coco prompt status --job <job>
coco prompt branch-status --job <job> --branch <branch>
```

Rules:

- You are already in `coco-orchestrator`; do not call `use_skill` for it again.
- Prefer `coco` commands over direct store edits.
- Hand off bounded work with `coco session fork` and `coco prompt`; do not use
  `use_skill` as the handoff mechanism here.
- On a `*/skill/*` branch, fork from the node before the `use_skill` ToolUse
  that invoked this skill, not from the skill session anchor.
- After forking, apply the runner role and restricted tools on the runner prompt.
  Prompt-level role/tool changes are session patches; they preserve the forked
  branch history while changing the runner configuration.

Example:

```bash
ANCHOR=$(coco session get --json --branch "$COCO_BRANCH" | jq -r '.anchor_id')
USE_SKILL=$(coco session show --json "$ANCHOR" | jq -r '.node.parent')
BASE=$(coco session show --json "$USE_SKILL" | jq -r '.node.parent')
coco session fork --branch "$RUNNER_BRANCH" --from-ref "$BASE"
coco prompt --branch "$RUNNER_BRANCH" --role runner --tool bash --tool search_skill "<task>"
```
