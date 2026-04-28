# CoCo Runner Workflow

Use the injected `coco` command through `bash` for runner-safe visibility and status inspection.

Useful commands:

```bash
coco session list
coco session get --branch <branch>
coco session graph
coco session show <ref>
coco prompt status --job <job>
coco prompt branch-status --job <job> --branch <branch>
```

Guidelines:

- Runner-scoped `coco` is read-oriented and intentionally hides write entrypoints.
- Use runner sessions for isolated execution, inspection, and handoff preparation.
- If you need workflow mutations such as create, merge, feedback, or prompt submission, hand back to an orchestrator session.
