# Recovery Skill

Use this skill when a CoCo session must recover from a failed LLM completion,
tool failure, broken branch state, or provider/runtime interruption.

## Workflow

1. Inspect the current branch and nearby graph nodes:

```bash
coco session get --json --branch "$COCO_BRANCH"
coco session graph
```

1. Identify the smallest recoverable point:

- If the failure is provider-side or transient, keep the existing prompt anchor
  and retry from the current branch head.
- If a tool call failed after partial progress, inspect the failure node and
  decide whether to continue from the persisted context or fork before the bad
  node.
- If the conversation history is too large or incoherent, use the `compact`
  skill before retrying.

1. Apply only branch-aware CoCo operations:

```bash
coco prompt --branch "$COCO_BRANCH" "<recovery prompt>"
coco session fork --branch <recovery-branch> --from-ref <safe-ref>
coco session merge --branch <recovery-branch> --target-branch "$COCO_BRANCH" --prompt "<merge prompt>"
```

## Constraints

- Do not edit store files directly.
- Preserve failed nodes for auditability.
- Prefer a concise recovery prompt over replaying a large conversation.
- Report the chosen recovery point, attempted action, and final branch status.
