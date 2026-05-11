from __future__ import annotations

import re
from pathlib import Path


TIMEZONE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_+./:-]{0,127}$")


def read_crontab(crontab_file: Path) -> str:
    if not crontab_file.exists():
        return ""
    return crontab_file.read_text(encoding="utf-8")


def write_crontab(crontab_file: Path, content: str) -> None:
    crontab_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = crontab_file.with_suffix(crontab_file.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(crontab_file)


def normalize_direct_crontab(content: str, *, requested_timezone: str | None) -> str:
    job_timezones, has_unqualified_jobs = collect_direct_job_timezones(content)
    timezones = set(job_timezones)
    if requested_timezone is not None:
        timezones.add(requested_timezone)
    if len(timezones) > 1:
        raise SystemExit(
            "direct crontab files run under supercronic and support only one "
            f"schedule timezone; found {', '.join(sorted(timezones))}"
        )
    if requested_timezone is None and job_timezones and has_unqualified_jobs:
        raise SystemExit(
            "direct crontab files run under supercronic and cannot mix "
            "timezone-qualified jobs with timezone-less jobs"
        )

    timezone = requested_timezone or next(iter(timezones), None)
    body = "\n".join(
        line
        for line in content.splitlines()
        if parse_env_assignment(line.strip())[0] != "CRON_TZ"
    ).strip("\n")
    lines: list[str] = []
    if timezone is not None:
        lines.append(f"CRON_TZ={timezone}")
    if body:
        lines.append(body)
    return "\n".join(lines).rstrip("\n") + ("\n" if lines else "")


def collect_direct_job_timezones(content: str) -> tuple[set[str], bool]:
    current_timezone: str | None = None
    job_timezones: set[str] = set()
    has_unqualified_jobs = False
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = parse_env_assignment(line)
        if key is not None:
            if key == "CRON_TZ":
                current_timezone = normalize_timezone(value, allow_blank=True)
            continue
        if current_timezone is None:
            has_unqualified_jobs = True
        else:
            job_timezones.add(current_timezone)
    return job_timezones, has_unqualified_jobs


def normalize_timezone(value: str | None, *, allow_blank: bool = False) -> str | None:
    if value is None:
        return None
    timezone = value.strip()
    if allow_blank and not timezone:
        return None
    if not TIMEZONE_PATTERN.fullmatch(timezone):
        raise SystemExit("timezone must be a single CRON_TZ token")
    return timezone


def parse_env_assignment(line: str) -> tuple[str | None, str]:
    if "=" not in line or line.split("=", 1)[0].strip() != line.split("=", 1)[0]:
        return None, ""
    key, value = line.split("=", 1)
    if not key or any(char.isspace() for char in key):
        return None, ""
    return key, value.strip()
