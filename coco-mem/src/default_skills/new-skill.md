# New Skill Workflow

Create dynamic CoCo skills through the `coco skill add` command. Do not edit
`skills.json` or skill history files directly.

Use this workflow when the user wants to add a new skill, persist a reusable
workflow, or turn an ad hoc procedure into a searchable skill.

## Workflow

1. Choose a kebab-case skill name and confirm it is not already registered:

```bash
coco skill show --role orchestrator --name <skill-name>
coco skill show --role runner --name <skill-name>
```

1. Draft a concise skill body in a temporary markdown file. The persisted skill
body should contain the operating instructions only; `coco skill add` stores
the name and description separately.

1. Add the skill with the appropriate role:

```bash
coco skill add \
  --role orchestrator \
  --name <skill-name> \
  --description "<when to use this skill>" \
  --file /path/to/skill-body.md \
  --enable-coco-shim
```

Use `--role runner` for read-oriented execution skills. Omit
`--enable-coco-shim` only when the skill must not use the injected `coco`
command.

1. Verify the persisted skill:

```bash
coco skill show --role orchestrator --name <skill-name>
coco skill list --role orchestrator
```

## Skill Body Shape

Keep the body small and operational:

```markdown
# Skill Title

Use this skill when ...

## Workflow

1. Inspect ...
2. Change ...
3. Validate ...

## Constraints

- Keep changes scoped.
- Prefer existing project patterns.
- Report validation results.
```

## Update Existing Skills

If the skill already exists, use `coco skill update` instead of `add`:

```bash
coco skill update \
  --role orchestrator \
  --name <skill-name> \
  --description "<updated trigger>" \
  --file /path/to/skill-body.md
```
