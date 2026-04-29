# CoCo Orchestrator Workflow

Use the injected `coco` command through `bash` whenever you need branch-aware session workflow control.

Useful commands:

```bash
coco session list
coco session get --branch <branch>
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

Execution rules:

- You are already executing `coco-orchestrator`. Do not call `use_skill` for
  `coco-orchestrator` again.
- Treat requests such as "use the coco-orchestrator skill" as already satisfied
  by this execution context. Continue by using `bash` with the injected `coco`
  command.
- Prefer `coco` over editing store files directly.
- Use this orchestrator session for coordination, branching, and merge
  decisions.
- Hand off bounded implementation work to runner sessions with `coco session
  fork`, `coco session rebase`, and `coco prompt`. Do not use `use_skill` as the
  handoff mechanism from inside this skill.
- When this skill is running on a `*/skill/*` branch and needs to create a runner branch, fork from the node before the `use_skill` ToolUse that invoked this skill. Do not fork the runner from the skill execution Session Anchor itself.
- To find that base node, inspect this skill session anchor, read its parent `use_skill` node, then inspect that `use_skill` node and use its parent as `--from-ref`.
- After forking a runner branch, immediately rebase it to runner settings and replace its tool set so it cannot call `use_skill` again.
- Example:

```bash
ANCHOR=$(coco session get --branch "$COCO_BRANCH" | jq -r '.anchor_id')
USE_SKILL=$(coco session show --json "$ANCHOR" | jq -r '.node.parent')
BASE=$(coco session show --json "$USE_SKILL" | jq -r '.node.parent')
coco session fork --branch "$RUNNER_BRANCH" --from-ref "$BASE"
coco session rebase --branch "$RUNNER_BRANCH" --role runner --tool bash --tool search_skill
```
