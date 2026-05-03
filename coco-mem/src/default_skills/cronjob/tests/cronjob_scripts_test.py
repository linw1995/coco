from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ADD_SCRIPT = ROOT / "scripts" / "cronjob_add.py"
RUN_SCRIPT = ROOT / "scripts" / "cronjob_run.py"


class CronjobScriptTests(unittest.TestCase):
    def test_add_rejects_cronexpr_under_fifteen_minutes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            result = run_add(
                workspace,
                "--id",
                "too-fast",
                "--branch",
                "main",
                "--cronexpr",
                "*/5 * * * *",
                "--repeat",
                "skip",
                "--prompt",
                "Run too often",
                "--dry-run",
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("minute step must be at least 15", result.stderr)

    def test_add_is_idempotent_by_managed_task_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            fake_crontab = write_fake_crontab(workspace)
            crontab_file = workspace / "crontab.txt"

            first = run_add(
                workspace,
                "--id",
                "daily-review",
                "--branch",
                "main",
                "--cronexpr",
                "0,15,30,45 * * * *",
                "--repeat",
                "skip",
                "--prompt",
                "First prompt",
                "--crontab-bin",
                str(fake_crontab),
                env={"FAKE_CRONTAB_FILE": str(crontab_file)},
            )
            second = run_add(
                workspace,
                "--id",
                "daily-review",
                "--branch",
                "release",
                "--cronexpr",
                "15 * * * *",
                "--repeat",
                "serial",
                "--prompt",
                "Updated prompt",
                "--crontab-bin",
                str(fake_crontab),
                env={"FAKE_CRONTAB_FILE": str(crontab_file)},
            )

            crontab = crontab_file.read_text(encoding="utf-8")
            task_file = workspace / "install" / "tasks" / "daily-review.json"
            task = json.loads(task_file.read_text(encoding="utf-8"))

        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(crontab.count("# BEGIN coco-cronjob id=daily-review"), 1)
        self.assertIn("15 * * * *", crontab)
        self.assertEqual(task["branch"], "release")
        self.assertEqual(task["prompt"], "Updated prompt")
        self.assertEqual(task["repeat"], "serial")

    def test_runner_skip_policy_does_not_submit_while_previous_job_is_active(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            fake_coco = write_fake_coco(workspace, status="running")
            task_file = write_task_file(workspace, fake_coco, repeat="skip")
            state_file = workspace / "state" / "daily-review.state.json"
            state_file.parent.mkdir(parents=True)
            state_file.write_text('{"last_job_id": "job-old", "branch": "main"}\n', encoding="utf-8")

            result = subprocess.run(
                [sys.executable, str(RUN_SCRIPT), "--task-file", str(task_file)],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            calls = read_fake_coco_calls(workspace)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("previous job job-old is running", result.stdout)
        self.assertEqual([call["kind"] for call in calls], ["status"])

    def test_runner_serial_policy_submits_after_previous_job_finishes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            fake_coco = write_fake_coco(workspace, status="finished")
            task_file = write_task_file(workspace, fake_coco, repeat="serial")
            state_file = workspace / "state" / "daily-review.state.json"
            state_file.parent.mkdir(parents=True)
            state_file.write_text('{"last_job_id": "job-old", "branch": "main"}\n', encoding="utf-8")

            result = subprocess.run(
                [sys.executable, str(RUN_SCRIPT), "--task-file", str(task_file)],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            calls = read_fake_coco_calls(workspace)
            state = json.loads(state_file.read_text(encoding="utf-8"))

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([call["kind"] for call in calls], ["status", "submit"])
        self.assertEqual(state["last_job_id"], "job-new")


def run_add(
    workspace: Path,
    *args: str,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    full_env = os.environ.copy()
    full_env["COCO_SKILL_DIR"] = str(ROOT)
    if env:
        full_env.update(env)
    return subprocess.run(
        [
            sys.executable,
            str(ADD_SCRIPT),
            "--install-dir",
            str(workspace / "install"),
            "--state-dir",
            str(workspace / "state"),
            "--log-dir",
            str(workspace / "logs"),
            *args,
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=full_env,
    )


def write_fake_crontab(workspace: Path) -> Path:
    path = workspace / "fake-crontab.py"
    path.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            import os
            import sys
            from pathlib import Path

            crontab = Path(os.environ["FAKE_CRONTAB_FILE"])
            if sys.argv[1:] == ["-l"]:
                if crontab.exists():
                    print(crontab.read_text(encoding="utf-8"), end="")
                    raise SystemExit(0)
                print("no crontab for test-user", file=sys.stderr)
                raise SystemExit(1)
            if sys.argv[1:] == ["-"]:
                crontab.write_text(sys.stdin.read(), encoding="utf-8")
                raise SystemExit(0)
            raise SystemExit(f"unexpected crontab args: {sys.argv[1:]}")
            """
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def write_fake_coco(workspace: Path, *, status: str) -> Path:
    path = workspace / "fake-coco.py"
    calls_file = workspace / "coco-calls.jsonl"
    path.write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env python3
            import json
            import sys
            from pathlib import Path

            calls = Path({str(calls_file)!r})
            args = sys.argv[1:]
            if args[:3] == ["prompt", "status", "--json"]:
                with calls.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps({{"kind": "status", "args": args}}) + "\\n")
                print(json.dumps({{"job": {{"status": {status!r}}}}}))
                raise SystemExit(0)
            if args[:4] == ["prompt", "--async", "--json", "--branch"]:
                with calls.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps({{"kind": "submit", "args": args}}) + "\\n")
                print(json.dumps({{"job_id": "job-new", "status": "queued", "branch": args[4]}}))
                raise SystemExit(0)
            raise SystemExit(f"unexpected coco args: {{args}}")
            """
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def write_task_file(workspace: Path, fake_coco: Path, *, repeat: str) -> Path:
    task = {
        "id": "daily-review",
        "branch": "main",
        "prompt": "Review the work queue.",
        "repeat": repeat,
        "coco_bin": str(fake_coco),
        "state_dir": str(workspace / "state"),
        "log_dir": str(workspace / "logs"),
    }
    path = workspace / "daily-review.json"
    path.write_text(json.dumps(task), encoding="utf-8")
    return path


def read_fake_coco_calls(workspace: Path) -> list[dict[str, object]]:
    calls_file = workspace / "coco-calls.jsonl"
    if not calls_file.exists():
        return []
    return [
        json.loads(line)
        for line in calls_file.read_text(encoding="utf-8").splitlines()
        if line
    ]


if __name__ == "__main__":
    unittest.main()
