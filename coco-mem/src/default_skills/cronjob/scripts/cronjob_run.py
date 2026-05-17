# /// script
# dependencies = []
# ///
"""Submit a managed CoCo cronjob prompt."""

from __future__ import annotations

import argparse
import fcntl
import json
import subprocess
import time
from pathlib import Path


ACTIVE_STATUSES = {"queued", "running"}


def main() -> int:
    args = parse_args()
    task = load_task(args.task_file)
    task_id = task["id"]
    repeat = task["repeat"]
    data_dir = resolve_task_data_dir(task, args.task_file)
    state_dir = data_dir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)

    lock_path = state_dir / f"{task_id}.lock"
    state_path = state_dir / f"{task_id}.state.json"
    pending_path = state_dir / f"{task_id}.pending"
    migrate_legacy_state(task, args.task_file, state_path)
    persist_task_data_dir(task, args.task_file, data_dir)
    task["data_dir"] = str(data_dir)
    with lock_for_policy(lock_path, repeat) as acquired:
        if not acquired:
            if repeat == "serial":
                count = increment_pending(pending_path)
                print(
                    f"Queued {task_id}: another cron invocation is updating task state "
                    f"({count} pending)."
                )
            else:
                print(
                    f"Skipping {task_id}: another cron invocation is updating task state."
                )
            return 0
        while True:
            wait_for_previous_job(task, state_path)
            job = submit_prompt(task)
            write_state(
                state_path, {"last_job_id": job["job_id"], "branch": task["branch"]}
            )
            print(json.dumps(job, indent=2, sort_keys=True))
            if repeat != "serial" or not consume_pending(pending_path):
                break
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-file", type=Path, required=True)
    return parser.parse_args()


def load_task(path: Path) -> dict[str, str]:
    task = json.loads(path.read_text(encoding="utf-8"))
    required = {"id", "branch", "prompt", "repeat", "coco_bin"}
    missing = sorted(required - task.keys())
    if missing:
        raise SystemExit(f"task file is missing required fields: {', '.join(missing)}")
    if task["repeat"] not in {"parallel", "serial", "skip"}:
        raise SystemExit("repeat must be one of parallel, serial, or skip")
    return task


def resolve_task_data_dir(task: dict[str, str], task_file: Path) -> Path:
    data_dir = Path(task.get("data_dir") or infer_task_data_dir(task_file)).expanduser()
    if not data_dir.is_absolute():
        data_dir = task_file.parent / data_dir
    return data_dir.resolve()


def infer_task_data_dir(task_file: Path) -> Path:
    task_dir = task_file.parent
    if task_dir.name != "tasks":
        raise SystemExit("task file is missing required fields: data_dir")
    install_dir = task_dir.parent
    if install_dir.name == "install":
        return install_dir.parent
    return install_dir


def persist_task_data_dir(
    task: dict[str, str], task_file: Path, data_dir: Path
) -> None:
    if task.get("data_dir") == str(data_dir) and "state_dir" not in task:
        return
    migrated = dict(task)
    migrated["data_dir"] = str(data_dir)
    migrated.pop("state_dir", None)
    tmp = task_file.with_suffix(task_file.suffix + ".tmp")
    tmp.write_text(
        json.dumps(migrated, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    tmp.replace(task_file)


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
        if self.repeat in {"serial", "skip"}:
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


def increment_pending(path: Path) -> int:
    with pending_counter_lock(path):
        count = read_pending_count(path) + 1
        write_pending_count(path, count)
        return count


def consume_pending(path: Path) -> bool:
    with pending_counter_lock(path):
        count = read_pending_count(path)
        if count <= 0:
            return False
        write_pending_count(path, count - 1)
        return True


class pending_counter_lock:
    def __init__(self, path: Path) -> None:
        self.lock_path = path.with_suffix(".pending.lock")
        self.handle = None

    def __enter__(self) -> None:
        self.handle = self.lock_path.open("a+", encoding="utf-8")
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)

    def __exit__(self, *_exc: object) -> None:
        if self.handle is not None:
            try:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            finally:
                self.handle.close()


def read_pending_count(path: Path) -> int:
    if not path.exists():
        return 0
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return 0
    try:
        count = int(text)
    except ValueError as error:
        raise SystemExit(f"pending counter is invalid: {path}") from error
    if count < 0:
        raise SystemExit(f"pending counter is invalid: {path}")
    return count


def write_pending_count(path: Path, count: int) -> None:
    if count <= 0:
        path.unlink(missing_ok=True)
        return
    tmp = path.with_suffix(".pending.tmp")
    tmp.write_text(f"{count}\n", encoding="utf-8")
    tmp.replace(path)


class state_file_lock:
    def __init__(self, path: Path) -> None:
        self.lock_path = path.with_suffix(path.suffix + ".lock")
        self.handle = None

    def __enter__(self) -> None:
        self.handle = self.lock_path.open("a+", encoding="utf-8")
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)

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
        if status is None:
            raise SystemExit(
                f"failed to resolve previous job {job_id} status for task {task['id']}"
            )
        if status not in ACTIVE_STATUSES:
            return
        if task["repeat"] == "skip":
            print(f"Skipping {task['id']}: previous job {job_id} is {status}.")
            raise SystemExit(0)
        time.sleep(30)


def migrate_legacy_state(
    task: dict[str, str], task_file: Path, state_path: Path
) -> None:
    legacy_state_dir_value = task.get("state_dir")
    if not legacy_state_dir_value or state_path.exists():
        return
    legacy_state_dir = Path(legacy_state_dir_value).expanduser()
    if not legacy_state_dir.is_absolute():
        legacy_state_dir = task_file.parent / legacy_state_dir
    legacy_state_path = legacy_state_dir.resolve() / f"{task['id']}.state.json"
    if legacy_state_path == state_path or not legacy_state_path.exists():
        return
    state = json.loads(legacy_state_path.read_text(encoding="utf-8"))
    write_state(state_path, state)


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
        raise SystemExit(
            f"coco prompt response did not include job_id: {result.stdout}"
        )
    return payload


def write_state(path: Path, state: dict[str, str]) -> None:
    with state_file_lock(path):
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        tmp.replace(path)


if __name__ == "__main__":
    raise SystemExit(main())
