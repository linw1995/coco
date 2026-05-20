# CoCo Compact

Compact a branch by summarizing the active session anchors and rebasing the
branch configuration so future turns carry a shorter, sharper instruction set.

Useful commands:

```bash
coco session get --json --branch <branch>
coco session graph --json
coco session show --json <ref>
coco session rebase --branch <branch> --system-prompt "<compacted system prompt>"
```

Rules:

- Inspect the target branch before changing it. Capture the current
  `anchor_id`, model, tools, system prompt, and branch head.
- Summarize durable operating constraints, unresolved work, important branch
  relationships, and active recovery state. Drop stale narration and repeated
  tool transcripts.
- Preserve concrete identifiers that future recovery depends on: branch names,
  job ids, node ids, external ticket ids, and explicit user constraints.
- Keep the compacted system prompt procedural and branch-specific. It should
  tell the next turn what must remain true, not retell every past step.
- Use `coco session rebase --branch <branch> --system-prompt ...` for the
  update. Re-read the branch afterwards and report the new head id.
- Do not compact a branch while its active prompt job is still running unless
  the caller explicitly asks for an emergency context reduction.
