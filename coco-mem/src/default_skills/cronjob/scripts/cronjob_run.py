# /// script
# dependencies = []
# ///
"""Submit a managed CoCo cronjob prompt."""

from __future__ import annotations

import argparse
import fcntl
import json
import subprocess
from pathlib import Path


CRONJOB_TASK_QUEUE = "cronjob.task"


def main() -> int:
    args = parse_args()
    task = load_task(args.task_file)
    task_id = task["id"]
    repeat = task["repeat"]
    data_dir = resolve_task_data_dir(task, args.task_file)
    state_dir = data_dir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    task["data_dir"] = str(data_dir)

    lock_path = state_dir / f"{task_id}.lock"
    pending_path = state_dir / f"{task_id}.pending"
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
            item = enqueue_task_event(task)
            print(json.dumps(item, indent=2, sort_keys=True))
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
    install_dir = task_dir.parent
    if task_dir.name == "tasks" and install_dir.name == "install":
        return install_dir.parent
    raise SystemExit("task file is missing required fields: data_dir")


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


def enqueue_task_event(task: dict[str, str]) -> dict[str, str]:
    payload = json.dumps(
        {
            "task_id": task["id"],
            "branch": task["branch"],
            "prompt": task["prompt"],
            "repeat": task["repeat"],
            "data_dir": task["data_dir"],
        },
        separators=(",", ":"),
    )
    result = subprocess.run(
        [
            task["coco_bin"],
            "mq",
            "enqueue",
            "--json",
            "--queue",
            CRONJOB_TASK_QUEUE,
            "--payload-json",
            payload,
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"failed to enqueue cronjob task event: {result.stderr.strip()}"
        )
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise SystemExit(
            f"coco mq enqueue did not return JSON: {result.stdout}"
        ) from error
    if "message_id" not in payload:
        raise SystemExit(
            f"coco mq enqueue response did not include message_id: {result.stdout}"
        )
    return payload


if __name__ == "__main__":
    raise SystemExit(main())
