# CoCo Recovery

Use this orchestrator skill from the built-in `day` branch after CoCo routes an
LLM backend failure system event to it. The `day` branch is the recovery
executor. The failed `work_branch` in the handoff is the target branch to inspect
or repair, not the branch executing this skill.

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
- `work_branch` is the branch that was responsible for the original job when the
  failure happened. It usually matches `failed_branch`.
- `root_branch` is the branch that should receive the recovered result after
  recovery succeeds.
- `retry_from_node_id` is the last known node before the failed backend call.
- `error_node_id` is the persisted failure node and should not be used as the
  continuation base.

Useful commands:

```bash
coco job status --json --job <job-id>
coco job worker --job <job-id>
coco session get --json --branch <branch>
coco session show --json <ref>
coco session rebase --branch <branch> --provider-profile <profile>
coco session handoff --branch <branch> --prompt "<recovered context>"
coco session rebase --branch <branch> --model <model>
coco session rebase --branch <branch> --temperature <temperature>
coco session rebase --branch <branch> --max-tokens <tokens>
```

Rules:

- Treat the event payload as authoritative. Do not guess missing job ids,
  branch names, or node ids.
- Run from `day`. Do not create another recovery branch and do not treat
  `work_branch` as the current execution branch.
- Inspect the job and relevant branches before acting. If the handoff does not
  identify a valid job or target branch, fail clearly instead of repairing the
  wrong job.
- Continue from `retry_from_node_id`, not from `error_node_id`. The error node is
  evidence, not a valid continuation base.
- Use the failure `message` to choose the smallest recovery strategy that can
  produce a normal result for the original user task.
- If the failure is likely caused by model choice, provider behavior, sampling,
  output limit, or branch configuration, rebase the affected branch to a better
  model or parameter set before retrying.
- If the branch context is too noisy or too large, compact it with `coco session
  handoff` before retrying. Preserve only the durable state needed to finish the
  original task.
- After repairing the target branch, run `coco job worker --job <job-id>` to
  retry the original job. Then run `coco job status --json --job <job-id>` and
  verify that the original job is `finished` before declaring recovery success.
- If the failed branch is not salvageable in place, rebuild the answer from
  `day` using the graph state and available `coco` commands. Do not fork a
  scratch branch.
- Keep the output shaped like a normal successful answer for the original job.
  Do not ask a supervisor to run follow-up commands.
- If recovery succeeds, return the recovered result from the original job.
