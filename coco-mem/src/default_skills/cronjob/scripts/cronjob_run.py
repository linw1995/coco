# /// script
# dependencies = []
# ///
"""Submit a managed CoCo cronjob prompt."""

from __future__ import annotations

import argparse
import fcntl
import json
import subprocess
import sys
import time
from pathlib import Path


ACTIVE_STATUSES = {"queued", "running"}


def main() -> int:
    args = parse_args()
    task = load_task(args.task_file)
    task_id = task["id"]
    repeat = task["repeat"]
    state_dir = Path(task["state_dir"])
    state_dir.mkdir(parents=True, exist_ok=True)

    lock_path = state_dir / f"{task_id}.lock"
    state_path = state_dir / f"{task_id}.state.json"
    with lock_for_policy(lock_path, repeat) as acquired:
        if not acquired:
            print(f"Skipping {task_id}: another cron invocation is updating task state.")
            return 0
        wait_for_previous_job(task, state_path)
        job = submit_prompt(task)
        write_state(state_path, {"last_job_id": job["job_id"], "branch": task["branch"]})
        print(json.dumps(job, indent=2, sort_keys=True))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-file", type=Path, required=True)
    return parser.parse_args()


def load_task(path: Path) -> dict[str, str]:
    task = json.loads(path.read_text(encoding="utf-8"))
    required = {"id", "branch", "prompt", "repeat", "coco_bin", "state_dir"}
    missing = sorted(required - task.keys())
    if missing:
        raise SystemExit(f"task file is missing required fields: {', '.join(missing)}")
    if task["repeat"] not in {"parallel", "serial", "skip"}:
        raise SystemExit("repeat must be one of parallel, serial, or skip")
    return task


class lock_for_policy:
    def __init__(self, path: Path, repeat: str) -> None:
        self.path = path
        self.repeat = repeat
        self.handle = None

    def __enter__(self) -> bool:
        self.handle = self.path.open("a+", encoding="utf-8")
        if self.repeat == "parallel":
            return True
        flags = fcntl.LOCK_EX
        if self.repeat == "skip":
            flags |= fcntl.LOCK_NB
        try:
            fcntl.flock(self.handle.fileno(), flags)
        except BlockingIOError:
            return False
        return True

    def __exit__(self, *_exc: object) -> None:
        if self.handle is not None:
            try:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            finally:
                self.handle.close()


def wait_for_previous_job(task: dict[str, str], state_path: Path) -> None:
    if task["repeat"] == "parallel" or not state_path.exists():
        return
    state = json.loads(state_path.read_text(encoding="utf-8"))
    job_id = state.get("last_job_id")
    if not job_id:
        return
    while True:
        status = prompt_status(task["coco_bin"], job_id)
        if status not in ACTIVE_STATUSES:
            return
        if task["repeat"] == "skip":
            print(f"Skipping {task['id']}: previous job {job_id} is {status}.")
            raise SystemExit(0)
        time.sleep(30)


def prompt_status(coco_bin: str, job_id: str) -> str | None:
    result = subprocess.run(
        [coco_bin, "prompt", "status", "--json", "--job", job_id],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        return None
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    job = payload.get("job", payload)
    status = job.get("status")
    return status if isinstance(status, str) else None


def submit_prompt(task: dict[str, str]) -> dict[str, str]:
    result = subprocess.run(
        [
            task["coco_bin"],
            "prompt",
            "--async",
            "--json",
            "--branch",
            task["branch"],
            task["prompt"],
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(f"failed to submit prompt: {result.stderr.strip()}")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise SystemExit(f"coco prompt did not return JSON: {result.stdout}") from error
    if "job_id" not in payload:
        raise SystemExit(f"coco prompt response did not include job_id: {result.stdout}")
    return payload


def write_state(path: Path, state: dict[str, str]) -> None:
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


if __name__ == "__main__":
    raise SystemExit(main())
