# CoCo Orchestrator Workflow

Use the injected `coco` command through `exec_command` for branch-aware
workflow control. Default output is human-readable; use `--json` whenever
piping to `jq` or scripts.

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
coco skill run <skill>
coco skill run <skill> --handoff "<task>"
```

Rules:

- Prefer `coco` commands over direct store edits.
- Use `coco skill run <skill> --handoff "<task>"` for bounded skill handoff.
  Omit `--handoff` only when the skill should inherit the current context.
- On a `*/skill/*` branch, fork from the node before the `SkillInvocation`
  anchor that invoked this skill, not from the skill session anchor.
- After forking, apply the runner role and restricted tools on the runner
  prompt. Prompt-level role/tool changes are session patches; they preserve
  the forked branch history while changing the runner configuration.
- When the runner work finishes, include the runner branch and final observed
  status in the result sent back to the caller.

Example:

```bash
ANCHOR=$(coco session get --json --branch "$COCO_BRANCH" | jq -r '.anchor_id')
INVOCATION=$(coco session show --json "$ANCHOR" | jq -r '.node.parent')
BASE=$(coco session show --json "$INVOCATION" | jq -r '.node.parent')
coco session fork --branch "$RUNNER_BRANCH" --from-ref "$BASE"
coco prompt --branch "$RUNNER_BRANCH" --role runner \
  --tool exec_command --tool write_stdin --tool search_skill --tool load_image \
  --enable-coco-shim "<task>"
```
