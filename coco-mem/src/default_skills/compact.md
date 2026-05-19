# Compact Skill

Use this skill when a CoCo branch needs a smaller, cleaner working context
before continuing. Typical triggers include context-window pressure, repeated
provider failures, noisy tool traces, or a long branch history that should be
summarized into an actionable checkpoint.

## Workflow

1. Inspect the current session and branch history:

```bash
coco session get --json --branch "$COCO_BRANCH"
coco session graph
```

1. Create an isolated compact branch with a fresh session anchor before
   writing the checkpoint. Do not compact directly on the source branch.

Use a stable branch name that makes the source branch clear:

```bash
COMPACT_BRANCH="${COCO_BRANCH}/compact/$(date +%Y%m%d%H%M%S)"
coco session fork --branch "$COMPACT_BRANCH" --from-ref "$COCO_BRANCH"
coco session rebase --branch "$COMPACT_BRANCH" \
  --system-prompt "You are compacting a CoCo branch into a clean continuation checkpoint."
```

1. Build a compact continuation note on the isolated branch. Keep only durable
   context:

- User goal and latest explicit requirements.
- Current branch, important refs, and relevant file paths.
- Decisions already made.
- Work completed and remaining concrete tasks.
- Known failures, blocked commands, and validation status.

1. Submit the compact note only on the isolated compact branch:

```bash
coco prompt --branch "$COMPACT_BRANCH" "<compact continuation prompt>"
```

1. Hand the compact branch and checkpoint node back to the caller. The caller
   decides whether to continue from the compact branch, merge it, or keep it as
   a recovery reference.

## Constraints

- Do not discard unresolved user requirements.
- Do not overwrite the source branch head while compacting.
- Do not include secrets, local private paths, tokens, or account identifiers.
- Keep the compact prompt operational rather than narrative.
- Prefer explicit file paths, command names, and branch refs over vague summary.
