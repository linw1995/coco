# CoCo Compact

Compact a branch by using the session graph to inspect the final provider
context, selectively pulling any missing details, then appending a concise
handoff session anchor.

Useful commands:

```bash
coco session get --json --branch <branch>
coco session graph --json
coco session show --json <ref>
coco session fork --branch <worker-branch> --from-ref <ref>
coco job list --json
coco job --async --json --branch <worker-branch> "<worker prompt>"
coco job status --json --job <job-id>
coco session handoff --branch <branch> --prompt "<compacted handoff>"
```

Rules:

- Inspect the target branch first with `coco session get --json --branch
  <branch>`. Preserve its role, provider profile, model, tools, and explicit
  runtime constraints unless the caller asks to change them.
- Resolve the caller branch from `COCO_BRANCH`. Resolve the target branch from
  the handoff; if the handoff asks to compact the current branch, the target is
  the caller branch.
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
- Write the compacted content as the handoff prompt for the next turn on this
  branch. It should say what must remain true and what the branch should do
  next, not retell the full history.
- If the caller branch is different from the target branch, apply the result
  with `coco session handoff --branch <branch> --prompt "<compacted
  handoff>"`. Do not use `session rebase` for compaction; compaction should
  append a new provider-context boundary, not rewrite the branch configuration
  in place.
- Re-read the branch after a direct handoff and report the new head id.
- Do not compact a branch while its active prompt job is still running unless
  the caller explicitly asks for emergency context reduction.
- If the caller branch is the target branch, do not run `coco session handoff`
  directly. The caller job still needs to receive this skill result and finish,
  so a direct handoff would race the caller job's final branch-head update.
  Delegate self-compaction to a durable worker branch instead, then return
  immediately.

Self-compaction delegation:

1. Confirm `caller_branch == target_branch`.
2. Create a durable worker branch with a unique name such as
   `<target>/compact/<short-id>`. Do not rely on the temporary `*/skill/*`
   branch to continue after this skill returns.
3. Fork the worker from a stable orchestrator or controller reference. If using
   the caller branch history, fork from the node before the `SkillInvocation`
   anchor that invoked this skill, not from the skill session anchor.
4. Submit an async job on the worker branch. The worker prompt must include the
   target branch, caller branch, observed target head, and this instruction:
   wait until the target branch has no active prompt jobs, then re-read
   `coco session get --json --branch <target>` and `coco session graph --json`
   before building and applying the handoff.
5. The worker job applies `coco session handoff --branch <target> --prompt
   "<compacted handoff>"` only after the target is idle and the graph has been
   re-read from the post-caller head.
6. If the target branch becomes active again or the handoff fails because the
   branch moved, the worker reports `deferred` instead of blindly retrying from
   stale context.
7. Return a concise skill result to the caller that includes the worker branch,
   worker job id, target branch, and the instruction that no handoff was
   applied by this skill and the caller should finish quickly so the target can
   become idle.
