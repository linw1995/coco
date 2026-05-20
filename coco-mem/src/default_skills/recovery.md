# CoCo Recovery

Recover a prompt job after an LLM backend failure by keeping the original job as
the unit of work and moving its current work branch to an isolated recovery
branch.

Inputs normally come from an `llm.backend_failure.recovery_requested` event:

- `job_id`
- `root_branch`
- `failed_branch`
- `retry_from_node_id`
- `error_node_id`
- `message`

Workflow:

```bash
coco prompt status --json --job <job-id>
coco session fork --branch <recovery-branch> --from-ref <retry-from-node-id>
coco prompt recover \
  --job <job-id> \
  --expected-work-branch <failed-branch> \
  --work-branch <recovery-branch>
coco prompt worker --job <job-id>
coco prompt status --json --job <job-id>
```

Rules:

- Use deterministic recovery branch names such as
  `recovery/<job-id>/<failed-branch>` when the caller did not provide one.
- Fork the recovery branch from `retry_from_node_id`, not from the failed
  branch head. The failed branch head is expected to be a failure node.
- Do not submit a separate prompt job for the failed user task. The original
  job must remain the source of truth.
- If the recovery branch fails, leave the job running on that work branch and
  report the new failure event details so another branch can take over.
- If recovery succeeds, `coco prompt worker` should finish the original job and
  restore the root branch automatically.
- Prefer `coco` commands over direct store edits.
