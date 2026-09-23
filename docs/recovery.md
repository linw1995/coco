# Backend Failure Recovery

CoCo routes an LLM backend failure through three built-in recovery branches:
`day`, `day-2`, and `day-3`. Each branch has its own prompt history and job
queue. The daemon creates missing recovery branches on startup from the current
provider profile. Existing branch anchors retain their selected model until
they are explicitly handed off or rebased.

The first recovery attempt runs on `day`. If its job encounters another backend
failure, CoCo finishes that attempt and sends the original failure event to
`day-2`, then to `day-3` if needed. The prompt anchor records the original event
and attempt number so this routing survives daemon restarts. A duplicate event
cannot create a second job for the same attempt.

After the third failed attempt, CoCo stops dispatching recovery jobs, marks
the original job finished, and logs `recovery attempts exhausted`.
Inspect the affected job with `coco job status --json --job <job-id>` and its
branch with `coco session get --json --branch <branch>`. A later retry requires
an explicit operator action.

Recovery code should inspect persisted state before repeating work because an
earlier attempt may have completed an external action before failing. The
`recovery` skill runs on the branch selected by the dispatcher; it does not
select or create recovery branches itself.
