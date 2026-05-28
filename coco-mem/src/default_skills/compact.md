# CoCo Compact

Compact a branch by using the session graph to inspect the final provider
context, selectively pulling any missing details, then appending a concise
handoff session anchor.

Useful commands:

```bash
coco session get --json --branch <branch>
coco session graph --json
coco session show --json <ref>
coco job list --json
coco job status --json --job <job-id>
coco session handoff --branch <branch> --system-prompt "<compacted system prompt>"
```

Rules:

- Inspect the target branch first with `coco session get --json --branch
  <branch>`. Preserve its role, provider profile, model, tools, and explicit
  runtime constraints unless the caller asks to change them.
- Use `coco session graph --json` as the primary source. Its default scope is
  the last provider context, which is the intended compaction window.
- Use `coco session show --json <ref>` only for graph nodes whose summary is not
  enough to decide whether the information is still needed.
- Use prompt job commands only when the graph or handoff mentions active job
  ids, recovery state, or branch handoff state that must be verified before
  compaction.
- Keep durable facts: current objective, unresolved decisions, branch
  relationships, active jobs, recovery state, concrete ids, user constraints,
  and external references.
- Drop stale narration, superseded plans, raw tool transcripts, repeated
  reasoning, and details that can be recovered from commands when needed.
- Write the compacted content as a procedural system prompt for the next turn on
  this branch. It should say what must remain true and what the branch should do
  next, not retell the full history.
- Apply the result with `coco session handoff --branch <branch> --system-prompt
  "<compacted system prompt>"`. Do not use `session rebase` for compaction;
  compaction should append a new provider-context boundary, not rewrite the
  branch configuration in place.
- Re-read the branch after handoff and report the new head id.
- Do not compact a branch while its active prompt job is still running unless
  the caller explicitly asks for emergency context reduction.
