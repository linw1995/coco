# /// script
# dependencies = []
# ///
"""Install or update a managed CoCo cronjob."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path


MANAGED_PREFIX = "coco-cronjob"
MANAGED_CRONTAB_FILE = "managed-crontab"
ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
TIMEZONE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_+./:-]{0,127}$")
MONTH_NAMES = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
WEEKDAY_NAMES = {
    "sun": 0,
    "mon": 1,
    "tue": 2,
    "wed": 3,
    "thu": 4,
    "fri": 5,
    "sat": 6,
}


def main() -> int:
    args = parse_args()
    task_id = normalize_task_id(args.id)
    branch = args.target or args.branch
    prompt = resolve_prompt(args)
    validate_cronexpr(args.cronexpr)
    timezone = normalize_timezone(args.timezone)
    crontab_file = resolve_crontab_file(args.crontab_file)
    timezone_reset = resolve_timezone_reset(crontab_file)

    install_dir = resolve_install_dir(args.install_dir)
    task_dir = install_dir / "tasks"
    state_dir = resolve_state_dir(args.state_dir)
    log_dir = resolve_log_dir(args.log_dir)
    task_path = task_dir / f"{task_id}.json"
    runner_path = install_dir / "cronjob_run.py"

    block = render_crontab_block(
        task_id=task_id,
        cronexpr=args.cronexpr,
        timezone=timezone,
        timezone_reset=timezone_reset,
        uv_bin=args.uv_bin,
        runner_path=runner_path,
        task_path=task_path,
        log_path=log_dir / f"{task_id}.log",
    )

    if args.dry_run:
        print(block, end="")
        return 0

    install_dir.mkdir(parents=True, exist_ok=True)
    task_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    runner_path = install_script(args.runner_source, install_dir, "cronjob_run.py")
    restore_path = install_script(None, install_dir, "cronjob_restore.py")
    write_task_config(
        task_path,
        {
            "id": task_id,
            "branch": branch,
            "prompt": prompt,
            "repeat": args.repeat,
            "coco_bin": args.coco_bin,
            "state_dir": str(state_dir),
            "log_dir": str(log_dir),
        },
    )
    block = render_crontab_block(
        task_id=task_id,
        cronexpr=args.cronexpr,
        timezone=timezone,
        timezone_reset=timezone_reset,
        uv_bin=args.uv_bin,
        runner_path=runner_path,
        task_path=task_path,
        log_path=log_dir / f"{task_id}.log",
    )

    original = read_crontab(args.crontab_bin, crontab_file)
    current = original
    if crontab_file is not None:
        current = normalize_direct_crontab(current, timezone_reset)
    updated, action = upsert_managed_block(current, task_id, block)
    if updated != original:
        write_crontab(args.crontab_bin, crontab_file, updated)
    write_managed_crontab_snapshot(install_dir / MANAGED_CRONTAB_FILE, updated)
    print(
        json.dumps(
            {
                "id": task_id,
                "action": action,
                "branch": branch,
                "cronexpr": args.cronexpr,
                "repeat": args.repeat,
                "task_file": str(task_path),
                "runner": str(runner_path),
                "restore": str(restore_path),
                "managed_crontab": str(install_dir / MANAGED_CRONTAB_FILE),
                "crontab_file": str(crontab_file) if crontab_file is not None else None,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--id", required=True, help="Stable cronjob id.")
    parser.add_argument("--branch", default="main", help="Target CoCo branch.")
    parser.add_argument("--target", help="Alias for --branch.")
    parser.add_argument("--cronexpr", required=True, help="Five-field cron expression.")
    parser.add_argument(
        "--repeat",
        choices=("parallel", "serial", "skip"),
        default="skip",
        help="Duplicate execution policy.",
    )
    parser.add_argument("--prompt", help="Prompt text to submit.")
    parser.add_argument("--prompt-file", type=Path, help="Read prompt text from a file.")
    parser.add_argument(
        "--timezone",
        help="Optional CRON_TZ value for cron implementations that support it.",
    )
    parser.add_argument("--coco-bin", default="coco", help="coco command path.")
    parser.add_argument("--uv-bin", default="uv", help="uv command path.")
    parser.add_argument("--crontab-bin", default="crontab", help="crontab command path.")
    parser.add_argument("--crontab-file", type=Path, help="Manage this crontab file directly.")
    parser.add_argument("--install-dir", type=Path, help="Persistent runner install directory.")
    parser.add_argument("--state-dir", type=Path, help="Persistent task state directory.")
    parser.add_argument("--log-dir", type=Path, help="Cronjob log directory.")
    parser.add_argument(
        "--runner-source",
        type=Path,
        help="Source cronjob_run.py path. Defaults to $COCO_SKILL_DIR/scripts/cronjob_run.py.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the managed block only.")
    return parser.parse_args()


def normalize_task_id(value: str) -> str:
    task_id = value.strip()
    if not ID_PATTERN.fullmatch(task_id):
        raise SystemExit(
            "task id must start with an alphanumeric character and contain only "
            "letters, digits, dot, underscore, or dash"
        )
    return task_id


def normalize_timezone(value: str | None, *, allow_blank: bool = False) -> str | None:
    if value is None:
        return None
    timezone = value.strip()
    if allow_blank and not timezone:
        return None
    if not TIMEZONE_PATTERN.fullmatch(timezone):
        raise SystemExit(
            "timezone must start with an alphanumeric character and contain only "
            "letters, digits, underscore, plus, dot, slash, colon, or dash"
        )
    return timezone


def resolve_prompt(args: argparse.Namespace) -> str:
    sources = [args.prompt is not None, args.prompt_file is not None]
    if sum(sources) > 1:
        raise SystemExit("use only one of --prompt or --prompt-file")
    if args.prompt_file is not None:
        prompt = args.prompt_file.read_text(encoding="utf-8")
    elif args.prompt is not None:
        prompt = args.prompt
    else:
        prompt = sys.stdin.read()
    prompt = prompt.strip()
    if not prompt:
        raise SystemExit("prompt must not be empty")
    return prompt


def validate_cronexpr(value: str) -> None:
    fields = value.split()
    if len(fields) != 5:
        raise SystemExit("cronexpr must contain exactly five fields")
    minutes = expand_minute_field(fields[0])
    hours = expand_cron_field(fields[1], "hour", minimum=0, maximum=23)
    days_of_month = expand_cron_field(fields[2], "day-of-month", minimum=1, maximum=31)
    months = expand_cron_field(fields[3], "month", minimum=1, maximum=12, names=MONTH_NAMES)
    days_of_week = expand_cron_field(
        fields[4],
        "day-of-week",
        minimum=0,
        maximum=7,
        names=WEEKDAY_NAMES,
    )
    if not minutes:
        raise SystemExit("minute field must select at least one minute")
    validate_minimum_cadence(
        minutes=minutes,
        hours=hours,
        days_of_month=days_of_month,
        months=months,
        days_of_week=days_of_week,
        day_of_month_field=fields[2],
        day_of_week_field=fields[4],
    )


def validate_minimum_cadence(
    *,
    minutes: set[int],
    hours: set[int],
    days_of_month: set[int],
    months: set[int],
    days_of_week: set[int],
    day_of_month_field: str,
    day_of_week_field: str,
) -> None:
    ordered = sorted(minutes)
    if any(right - left < 15 for left, right in zip(ordered, ordered[1:])):
        raise SystemExit("cronexpr minute granularity must be at least 15 minutes")

    previous = None
    for occurrence in iter_cron_occurrences(
        minutes=minutes,
        hours=hours,
        days_of_month=days_of_month,
        months=months,
        days_of_week=days_of_week,
        day_of_month_field=day_of_month_field,
        day_of_week_field=day_of_week_field,
    ):
        if previous is not None and occurrence - previous < timedelta(minutes=15):
            raise SystemExit("cronexpr minute granularity must be at least 15 minutes")
        previous = occurrence


def iter_cron_occurrences(
    *,
    minutes: set[int],
    hours: set[int],
    days_of_month: set[int],
    months: set[int],
    days_of_week: set[int],
    day_of_month_field: str,
    day_of_week_field: str,
):
    start = datetime(2024, 1, 1)
    for offset in range(366 * 5):
        day = start + timedelta(days=offset)
        if day.month not in months:
            continue
        if not cron_day_matches(
            day,
            days_of_month=days_of_month,
            days_of_week=days_of_week,
            day_of_month_field=day_of_month_field,
            day_of_week_field=day_of_week_field,
        ):
            continue
        for hour in sorted(hours):
            for minute in sorted(minutes):
                yield day.replace(hour=hour, minute=minute)


def cron_day_matches(
    day: datetime,
    *,
    days_of_month: set[int],
    days_of_week: set[int],
    day_of_month_field: str,
    day_of_week_field: str,
) -> bool:
    matches_day_of_month = day.day in days_of_month
    cron_weekday = (day.weekday() + 1) % 7
    matches_day_of_week = cron_weekday in days_of_week or (
        cron_weekday == 0 and 7 in days_of_week
    )
    day_of_month_wildcard = day_of_month_field == "*"
    day_of_week_wildcard = day_of_week_field == "*"
    if day_of_month_wildcard and day_of_week_wildcard:
        return True
    if day_of_month_wildcard:
        return matches_day_of_week
    if day_of_week_wildcard:
        return matches_day_of_month
    return matches_day_of_month or matches_day_of_week


def expand_cron_field(
    field: str,
    name: str,
    *,
    minimum: int,
    maximum: int,
    names: dict[str, int] | None = None,
) -> set[int]:
    values: set[int] = set()
    for part in field.split(","):
        if not part:
            raise SystemExit(f"empty {name} field segment")
        values.update(
            expand_cron_part(part, name, minimum=minimum, maximum=maximum, names=names)
        )
    return values


def expand_cron_part(
    part: str,
    name: str,
    *,
    minimum: int,
    maximum: int,
    names: dict[str, int] | None,
) -> set[int]:
    base, step = split_step(part, name)
    if base == "*":
        start, end = minimum, maximum
    if "-" in base:
        left, right = base.split("-", 1)
        start = parse_cron_value(left, name, minimum=minimum, maximum=maximum, names=names)
        end = parse_cron_value(right, name, minimum=minimum, maximum=maximum, names=names)
        if start > end:
            raise SystemExit(f"{name} ranges must be ascending")
    elif base != "*":
        value = parse_cron_value(base, name, minimum=minimum, maximum=maximum, names=names)
        if step is not None:
            raise SystemExit(f"single {name} values cannot use a step")
        return {value}
    return set(range(start, end + 1, step or 1))


def expand_minute_field(field: str) -> set[int]:
    minutes: set[int] = set()
    for part in field.split(","):
        if not part:
            raise SystemExit("empty minute field segment")
        minutes.update(expand_minute_part(part))
    return minutes


def expand_minute_part(part: str) -> set[int]:
    base, step = split_step(part, "minute")
    if base == "*":
        start, end = 0, 59
    elif "-" in base:
        left, right = base.split("-", 1)
        start = parse_cron_value(left, "minute", minimum=0, maximum=59)
        end = parse_cron_value(right, "minute", minimum=0, maximum=59)
        if start > end:
            raise SystemExit("minute ranges must be ascending")
    else:
        minute = parse_cron_value(base, "minute", minimum=0, maximum=59)
        if step is not None:
            raise SystemExit("single minute values cannot use a step")
        return {minute}
    step = step or 1
    if step < 15:
        raise SystemExit("minute step must be at least 15")
    return set(range(start, end + 1, step))


def split_step(part: str, name: str) -> tuple[str, int | None]:
    if "/" not in part:
        return part, None
    base, step_text = part.split("/", 1)
    if not base or not step_text:
        raise SystemExit(f"{name} step syntax must be <range>/<step>")
    try:
        step = int(step_text)
    except ValueError as error:
        raise SystemExit(f"{name} step must be an integer") from error
    if step <= 0:
        raise SystemExit(f"{name} step must be positive")
    return base, step


def parse_cron_value(
    value: str,
    name: str,
    *,
    minimum: int,
    maximum: int,
    names: dict[str, int] | None = None,
) -> int:
    if names is not None:
        resolved = names.get(value.lower())
        if resolved is not None:
            return resolved
    try:
        parsed = int(value)
    except ValueError as error:
        raise SystemExit(f"{name} values must be integers") from error
    if parsed < minimum or parsed > maximum:
        raise SystemExit(f"{name} values must be between {minimum} and {maximum}")
    return parsed


def resolve_install_dir(value: Path | None) -> Path:
    if value is not None:
        return value.expanduser()
    persist_dir = resolve_skill_persist_dir()
    if persist_dir is not None:
        return persist_dir / "install"
    data_home = Path(os.environ.get("XDG_DATA_HOME", "~/.local/share")).expanduser()
    return data_home / "coco" / "cronjob"


def resolve_state_dir(value: Path | None) -> Path:
    if value is not None:
        return value.expanduser()
    persist_dir = resolve_skill_persist_dir()
    if persist_dir is not None:
        return persist_dir / "state"
    state_home = Path(os.environ.get("XDG_STATE_HOME", "~/.local/state")).expanduser()
    return state_home / "coco" / "cronjob"


def resolve_log_dir(value: Path | None) -> Path:
    if value is not None:
        return value.expanduser()
    persist_dir = resolve_skill_persist_dir()
    if persist_dir is not None:
        return persist_dir / "logs"
    state_home = Path(os.environ.get("XDG_STATE_HOME", "~/.local/state")).expanduser()
    return state_home / "coco" / "logs" / "cronjob"


def resolve_skill_persist_dir() -> Path | None:
    value = os.environ.get("COCO_SKILL_PERSIST_DIR")
    if not value:
        return None
    return Path(value).expanduser()


def resolve_crontab_file(value: Path | None) -> Path | None:
    if value is not None:
        return value.expanduser()
    env_value = os.environ.get("COCO_CRONTAB_FILE")
    if not env_value:
        return None
    return Path(env_value).expanduser()


def resolve_timezone_reset(crontab_file: Path | None) -> str:
    if crontab_file is None:
        return ""
    timezone = normalize_timezone(os.environ.get("TZ"), allow_blank=True)
    return timezone or "UTC"


def install_script(source: Path | None, install_dir: Path, script_name: str) -> Path:
    source_path = source
    if source_path is None:
        skill_dir = os.environ.get("COCO_SKILL_DIR")
        if not skill_dir:
            raise SystemExit("COCO_SKILL_DIR is required unless --runner-source is provided")
        source_path = Path(skill_dir) / "scripts" / script_name
    if not source_path.is_file():
        raise SystemExit(f"script source does not exist: {source_path}")
    target = install_dir / script_name
    shutil.copyfile(source_path, target)
    target.chmod(0o755)
    return target


def write_task_config(path: Path, config: dict[str, str]) -> None:
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def render_crontab_block(
    *,
    task_id: str,
    cronexpr: str,
    timezone: str | None,
    timezone_reset: str,
    uv_bin: str,
    runner_path: Path,
    task_path: Path,
    log_path: Path,
) -> str:
    command = " ".join(
        shell_quote(part)
        for part in [
            uv_bin,
            "run",
            "--script",
            str(runner_path),
            "--task-file",
            str(task_path),
        ]
    )
    redirect = f">> {shell_quote(str(log_path))} 2>&1"
    lines = [begin_marker(task_id)]
    if timezone:
        lines.append(f"CRON_TZ={timezone}")
    lines.append(f"{cronexpr} {command} {redirect}")
    if timezone:
        lines.append(f"CRON_TZ={timezone_reset}")
    lines.append(end_marker(task_id))
    return "\n".join(lines) + "\n"


def shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def read_crontab(crontab_bin: str, crontab_file: Path | None) -> str:
    if crontab_file is not None:
        if not crontab_file.exists():
            return ""
        return crontab_file.read_text(encoding="utf-8")

    result = subprocess.run(
        [crontab_bin, "-l"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode == 0:
        return result.stdout
    combined = (result.stdout + result.stderr).lower()
    if "no crontab" in combined or "no crontab for" in combined:
        return ""
    raise SystemExit(f"failed to read crontab: {result.stderr.strip()}")


def write_crontab(crontab_bin: str, crontab_file: Path | None, content: str) -> None:
    if crontab_file is not None:
        crontab_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = crontab_file.with_suffix(crontab_file.suffix + ".tmp")
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(crontab_file)
        return

    result = subprocess.run(
        [crontab_bin, "-"],
        input=content,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(f"failed to install crontab: {result.stderr.strip()}")


def normalize_direct_crontab(content: str, timezone_reset: str) -> str:
    lines = content.splitlines()
    output: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if not line.startswith(f"# BEGIN {MANAGED_PREFIX} id="):
            output.append(line)
            index += 1
            continue

        output.append(line)
        index += 1
        while index < len(lines):
            managed_line = lines[index]
            output.append(
                f"CRON_TZ={timezone_reset}" if managed_line == "CRON_TZ=" else managed_line
            )
            if managed_line.startswith(f"# END {MANAGED_PREFIX} id="):
                break
            index += 1
        else:
            raise SystemExit(f"managed crontab block {line!r} is missing its end marker")
        index += 1

    return "\n".join(output).rstrip("\n") + ("\n" if output else "")


def write_managed_crontab_snapshot(path: Path, content: str) -> None:
    snapshot = extract_managed_blocks(content)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(snapshot, encoding="utf-8")
    tmp.replace(path)


def extract_managed_blocks(content: str) -> str:
    lines = content.splitlines()
    blocks: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if not line.startswith(f"# BEGIN {MANAGED_PREFIX} id="):
            index += 1
            continue

        block = [line]
        index += 1
        while index < len(lines):
            block.append(lines[index])
            if lines[index].startswith(f"# END {MANAGED_PREFIX} id="):
                break
            index += 1
        else:
            raise SystemExit(f"managed crontab block {line!r} is missing its end marker")

        blocks.append("\n".join(block))
        index += 1

    return "\n\n".join(blocks).rstrip("\n") + ("\n" if blocks else "")


def upsert_managed_block(current: str, task_id: str, block: str) -> tuple[str, str]:
    lines = current.splitlines()
    begin = begin_marker(task_id)
    end = end_marker(task_id)
    output: list[str] = []
    replaced = False
    index = 0
    while index < len(lines):
        if lines[index] == begin:
            end_index = index + 1
            while end_index < len(lines) and lines[end_index] != end:
                end_index += 1
            if end_index == len(lines):
                raise SystemExit(f"managed crontab block for {task_id!r} is missing its end marker")
            if not replaced:
                output.extend(block.rstrip("\n").splitlines())
                replaced = True
            index = end_index + 1
            continue
        output.append(lines[index])
        index += 1

    if not replaced:
        if output and output[-1] != "":
            output.append("")
        output.extend(block.rstrip("\n").splitlines())
    return "\n".join(output).rstrip("\n") + "\n", "updated" if replaced else "added"


def begin_marker(task_id: str) -> str:
    return f"# BEGIN {MANAGED_PREFIX} id={task_id}"


def end_marker(task_id: str) -> str:
    return f"# END {MANAGED_PREFIX} id={task_id}"


if __name__ == "__main__":
    raise SystemExit(main())
