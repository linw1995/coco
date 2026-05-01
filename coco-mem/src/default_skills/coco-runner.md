# CoCo Runner Workflow

Use the injected `coco` command through `exec_command` for runner-safe
inspection. Default output is human-readable; use `--json` for scripts.

Useful commands:

```bash
coco session list
coco session get --branch <branch>
coco session get --json --branch <branch>
coco session graph
coco session show <ref>
coco prompt status --job <job>
coco prompt branch-status --job <job> --branch <branch>
```

Rules:

- Runner-scoped `coco` is read-oriented and hides write entrypoints.
- Use runner sessions for isolated execution, inspection, and handoff prep.
- Hand workflow mutations back to an orchestrator session.
