# CoCo Recovery

Use this orchestrator skill only from the branch that CoCo core selected to
handle a failed branch. That branch is the active work branch for the original
job while recovery is running.

The handoff normally includes:

- `job_id`
- `root_branch`
- `work_branch`
- `failed_branch`
- `retry_from_node_id`
- `error_node_id`
- `message`

Interpretation:

- `failed_branch` is the branch that produced the backend failure event.
- `work_branch` is the branch currently responsible for this job. When this
  skill runs, it should match the branch executing the skill.
- `root_branch` is the branch that should receive the recovered result after the
  recovery branch succeeds.
- `retry_from_node_id` is the last known node before the failed backend call.
- `error_node_id` is the persisted failure node and should not be used as the
  continuation base.

Useful commands:

```bash
coco prompt status --json --job <job-id>
coco prompt branch-status --job <job-id> --branch <branch>
coco session get --json --branch <branch>
coco session show --json <ref>
coco session handoff --branch <branch> --system-prompt "<recovered context>"
coco session rebase --branch <branch> --model <model>
coco session rebase --branch <branch> --temperature <temperature>
coco session rebase --branch <branch> --max-tokens <tokens>
coco session fork --branch <scratch-branch> --from-ref <retry-from-node-id>
coco prompt --branch <scratch-branch> "<restart prompt>"
```

Rules:

- Treat the event payload as authoritative. Do not guess missing job ids,
  branch names, or node ids.
- Do not create another recovery branch unless a later system event explicitly
  assigns one. If this branch cannot recover the job, fail clearly and let CoCo
  core route the next branch.
- Inspect the job and relevant branches before acting. If `work_branch` is not
  the current branch, report the mismatch instead of repairing the wrong job.
- Continue from `retry_from_node_id`, not from `error_node_id`. The error node is
  evidence, not a valid continuation base.
- Use the failure `message` to choose the smallest recovery strategy that can
  produce a normal result for the original user task.
- If the failure is likely caused by model choice, provider behavior, sampling,
  output limit, or branch configuration, rebase the current `work_branch` to a
  better model or parameter set before retrying.
- If the branch context is too noisy or too large, compact it with `coco session
  handoff` before retrying. Preserve only the durable state needed to finish the
  original task.
- If the failed branch is not salvageable in place, fork a deterministic scratch
  branch from `retry_from_node_id` and restart the task there. Treat that branch
  as reconstruction workspace, not as an implicit job `work_branch` change.
- Use scratch branch output as evidence to produce the final recovered result
  from the current recovery branch. If the scratch branch itself must become the
  job work branch, stop and fail clearly so CoCo core can route it explicitly.
- Keep the output shaped like a normal successful answer for the original job.
  Do not ask a supervisor to run follow-up commands.
- If recovery succeeds, return the recovered result. CoCo core is responsible
  for restoring `root_branch` as the current work branch.
