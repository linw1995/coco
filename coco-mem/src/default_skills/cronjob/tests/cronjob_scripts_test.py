from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ADD_SCRIPT = ROOT / "scripts" / "cronjob_add.py"
RUN_SCRIPT = ROOT / "scripts" / "cronjob_run.py"
RESTORE_SCRIPT = ROOT / "scripts" / "cronjob_restore.py"


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

    def test_add_rejects_cross_hour_cronexpr_under_fifteen_minutes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            result = run_add(
                workspace,
                "--id",
                "too-fast-across-hours",
                "--branch",
                "main",
                "--cronexpr",
                "0,59 * * * *",
                "--repeat",
                "skip",
                "--prompt",
                "Run too often across hours",
                "--dry-run",
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("minute granularity must be at least 15 minutes", result.stderr)

    def test_add_rejects_invalid_cronexpr_fields(self) -> None:
        cases = [
            ("15 foo * * *", "hour values must be integers"),
            ("15 24 * * *", "hour values must be between 0 and 23"),
            ("15 9 0 * *", "day-of-month values must be between 1 and 31"),
            ("15 9 32 * *", "day-of-month values must be between 1 and 31"),
            ("15 9 * nope *", "month values must be integers"),
            ("15 9 * 13 *", "month values must be between 1 and 12"),
            ("15 9 * dec-jan *", "month ranges must be ascending"),
            ("15 9 * */x *", "month step must be an integer"),
            ("15 9 * * funday", "day-of-week values must be integers"),
            ("15 9 * * 8", "day-of-week values must be between 0 and 7"),
            ("15 9 * * mon/2", "single day-of-week values cannot use a step"),
        ]
        for cronexpr, expected_error in cases:
            with self.subTest(cronexpr=cronexpr):
                with tempfile.TemporaryDirectory() as directory:
                    workspace = Path(directory)
                    result = run_add(
                        workspace,
                        "--id",
                        "invalid-schedule",
                        "--branch",
                        "main",
                        "--cronexpr",
                        cronexpr,
                        "--repeat",
                        "skip",
                        "--prompt",
                        "Run with invalid cron field",
                        "--dry-run",
                    )

                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(expected_error, result.stderr)
                    self.assertFalse((workspace / "install").exists())

    def test_add_accepts_valid_cronexpr_field_syntax(self) -> None:
        cases = [
            "15 9 * jan,mar mon-fri",
            "0,30 */2 1-15/2 1-12/3 0,6",
            "0,59 0 * * *",
            "45 23 31 dec sun",
        ]
        for cronexpr in cases:
            with self.subTest(cronexpr=cronexpr):
                with tempfile.TemporaryDirectory() as directory:
                    workspace = Path(directory)
                    result = run_add(
                        workspace,
                        "--id",
                        "weekday-review",
                        "--branch",
                        "main",
                        "--cronexpr",
                        cronexpr,
                        "--repeat",
                        "skip",
                        "--prompt",
                        "Run with valid cron fields",
                        "--dry-run",
                    )

                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(cronexpr, result.stdout)

    def test_add_is_idempotent_by_managed_task_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            crontab_file = workspace / "supercronic" / "local.crontab"

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
            )

            crontab = crontab_file.read_text(encoding="utf-8")
            task_file = workspace / "install" / "tasks" / "daily-review.json"
            task = json.loads(task_file.read_text(encoding="utf-8"))
            result_data = json.loads(second.stdout)
            managed_crontab = Path(result_data["managed_crontab"]).read_text(
                encoding="utf-8"
            )

        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(crontab.count("# BEGIN coco-cronjob id=daily-review"), 1)
        self.assertIn("15 * * * *", crontab)
        self.assertEqual(managed_crontab, crontab)
        self.assertEqual(task["branch"], "release")
        self.assertEqual(task["prompt"], "Updated prompt")
        self.assertEqual(task["repeat"], "serial")

    def test_add_defaults_to_skill_persistent_directories(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            persist_dir = workspace / "persist"

            result = run_add(
                workspace,
                "--id",
                "daily-review",
                "--branch",
                "main",
                "--cronexpr",
                "15 * * * *",
                "--repeat",
                "skip",
                "--prompt",
                "Persisted prompt",
                env={"COCO_SKILL_PERSIST_DIR": str(persist_dir)},
                explicit_dirs=False,
            )
            task_file = persist_dir / "install" / "tasks" / "daily-review.json"
            task = json.loads(task_file.read_text(encoding="utf-8"))

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(task["state_dir"], str(persist_dir / "state"))
        self.assertEqual(task["log_dir"], str(persist_dir / "logs"))

    def test_add_crontab_dir_groups_direct_files_by_timezone(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            crontab_dir = workspace / "supercronic"

            shanghai = run_add(
                workspace,
                "--id",
                "daily-shanghai",
                "--branch",
                "main",
                "--cronexpr",
                "30 8 * * *",
                "--repeat",
                "skip",
                "--prompt",
                "Shanghai prompt",
                "--timezone",
                "Asia/Shanghai",
                "--crontab-dir",
                str(crontab_dir),
            )
            tokyo = run_add(
                workspace,
                "--id",
                "daily-tokyo",
                "--branch",
                "main",
                "--cronexpr",
                "30 8 * * *",
                "--repeat",
                "skip",
                "--prompt",
                "Tokyo prompt",
                "--timezone",
                "Asia/Tokyo",
                "--crontab-dir",
                str(crontab_dir),
            )
            shanghai_crontab = (crontab_dir / "tz-Asia_Shanghai.crontab").read_text(
                encoding="utf-8"
            )
            tokyo_crontab = (crontab_dir / "tz-Asia_Tokyo.crontab").read_text(
                encoding="utf-8"
            )
            shanghai_snapshot = (
                workspace / "install" / "managed-crontabs" / "tz-Asia_Shanghai.crontab"
            ).read_text(encoding="utf-8")

        self.assertEqual(shanghai.returncode, 0, shanghai.stderr)
        self.assertEqual(tokyo.returncode, 0, tokyo.stderr)
        self.assertTrue(shanghai_crontab.startswith("CRON_TZ=Asia/Shanghai\n"))
        self.assertTrue(tokyo_crontab.startswith("CRON_TZ=Asia/Tokyo\n"))
        self.assertIn("# BEGIN coco-cronjob id=daily-shanghai", shanghai_crontab)
        self.assertIn("# BEGIN coco-cronjob id=daily-tokyo", tokyo_crontab)
        self.assertEqual(shanghai_snapshot, shanghai_crontab)

    def test_add_crontab_dir_moves_task_between_timezone_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            crontab_dir = workspace / "supercronic"

            first = run_add(
                workspace,
                "--id",
                "daily-review",
                "--branch",
                "main",
                "--cronexpr",
                "30 8 * * *",
                "--repeat",
                "skip",
                "--prompt",
                "First prompt",
                "--timezone",
                "Asia/Shanghai",
                "--crontab-dir",
                str(crontab_dir),
            )
            second = run_add(
                workspace,
                "--id",
                "daily-review",
                "--branch",
                "main",
                "--cronexpr",
                "30 9 * * *",
                "--repeat",
                "skip",
                "--prompt",
                "Second prompt",
                "--timezone",
                "Asia/Tokyo",
                "--crontab-dir",
                str(crontab_dir),
            )
            shanghai_crontab = crontab_dir / "tz-Asia_Shanghai.crontab"
            shanghai_snapshot = (
                workspace / "install" / "managed-crontabs" / "tz-Asia_Shanghai.crontab"
            )
            tokyo_crontab = (crontab_dir / "tz-Asia_Tokyo.crontab").read_text(
                encoding="utf-8"
            )

        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertFalse(shanghai_crontab.exists())
        self.assertFalse(shanghai_snapshot.exists())
        self.assertIn("# BEGIN coco-cronjob id=daily-review", tokyo_crontab)
        self.assertIn("30 9 * * *", tokyo_crontab)

    def test_add_keeps_existing_timezone_file_when_target_update_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            crontab_dir = workspace / "supercronic"

            first = run_add(
                workspace,
                "--id",
                "daily-review",
                "--branch",
                "main",
                "--cronexpr",
                "30 8 * * *",
                "--repeat",
                "skip",
                "--prompt",
                "First prompt",
                "--timezone",
                "Asia/Shanghai",
                "--crontab-dir",
                str(crontab_dir),
            )
            tokyo_crontab = crontab_dir / "tz-Asia_Tokyo.crontab"
            tokyo_crontab.write_text(
                "\n".join(
                    [
                        "CRON_TZ=Asia/Tokyo",
                        "# BEGIN coco-cronjob id=broken",
                        "30 9 * * * echo broken",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            second = run_add(
                workspace,
                "--id",
                "daily-review",
                "--branch",
                "main",
                "--cronexpr",
                "30 9 * * *",
                "--repeat",
                "skip",
                "--prompt",
                "Second prompt",
                "--timezone",
                "Asia/Tokyo",
                "--crontab-dir",
                str(crontab_dir),
            )
            shanghai_crontab = (crontab_dir / "tz-Asia_Shanghai.crontab").read_text(
                encoding="utf-8"
            )

        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertNotEqual(second.returncode, 0)
        self.assertIn("missing its end marker", second.stderr)
        self.assertIn("# BEGIN coco-cronjob id=daily-review", shanghai_crontab)

    def test_add_dry_run_does_not_mutate_local_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)

            result = run_add(
                workspace,
                "--id",
                "daily-review",
                "--branch",
                "main",
                "--cronexpr",
                "15 * * * *",
                "--repeat",
                "skip",
                "--prompt",
                "Preview prompt",
                "--dry-run",
                env={"COCO_CRONTAB_DIR": ""},
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("# BEGIN coco-cronjob id=daily-review", result.stdout)
        self.assertFalse((workspace / "install").exists())
        self.assertFalse((workspace / "state").exists())
        self.assertFalse((workspace / "logs").exists())

    def test_add_resets_cron_tz_inside_managed_block(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)

            result = run_add(
                workspace,
                "--id",
                "daily-review",
                "--branch",
                "main",
                "--cronexpr",
                "15 * * * *",
                "--repeat",
                "skip",
                "--prompt",
                "Preview prompt",
                "--timezone",
                "UTC",
                "--dry-run",
                env={"TZ": "UTC"},
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            "\n".join(
                [
                    "# BEGIN coco-cronjob id=daily-review",
                    "CRON_TZ=UTC",
                    "15 * * * *",
                ]
            ),
            result.stdout,
        )
        self.assertIn("\nCRON_TZ=UTC\n# END coco-cronjob id=daily-review\n", result.stdout)

    def test_add_rejects_cron_tz_injection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)

            result = run_add(
                workspace,
                "--id",
                "daily-review",
                "--branch",
                "main",
                "--cronexpr",
                "15 * * * *",
                "--repeat",
                "skip",
                "--prompt",
                "Preview prompt",
                "--timezone",
                "UTC\n* * * * echo injected",
                "--dry-run",
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("timezone", result.stderr)
        self.assertNotIn("echo injected", result.stdout)
        self.assertFalse((workspace / "install").exists())

    def test_restore_can_manage_crontab_dir_directly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            snapshot_dir = workspace / "managed-crontabs"
            crontab_dir = workspace / "supercronic"
            snapshot_dir.mkdir()
            (snapshot_dir / "tz-Asia_Shanghai.crontab").write_text(
                render_restore_block(),
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(RESTORE_SCRIPT),
                    "--snapshot-dir",
                    str(snapshot_dir),
                    "--crontab-dir",
                    str(crontab_dir),
                ],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            crontab = (crontab_dir / "tz-Asia_Shanghai.crontab").read_text(
                encoding="utf-8"
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(crontab.startswith("CRON_TZ=Asia/Shanghai\n"))
        self.assertEqual(crontab.count("CRON_TZ="), 1)

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

    def test_runner_serial_policy_queues_when_state_is_locked(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            fake_coco = write_fake_coco(workspace, status="finished")
            task_file = write_task_file(workspace, fake_coco, repeat="serial")
            state_dir = workspace / "state"
            state_dir.mkdir(parents=True)
            lock_file = state_dir / "daily-review.lock"

            with lock_file.open("a+", encoding="utf-8") as lock:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                result = subprocess.run(
                    [sys.executable, str(RUN_SCRIPT), "--task-file", str(task_file)],
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=5,
                )
                calls = read_fake_coco_calls(workspace)
                pending_count = (state_dir / "daily-review.pending").read_text(encoding="utf-8")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Queued daily-review", result.stdout)
        self.assertEqual(calls, [])
        self.assertEqual(pending_count, "1\n")

    def test_runner_serial_policy_drains_queued_invocations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            fake_coco = write_fake_coco(workspace, status="finished")
            task_file = write_task_file(workspace, fake_coco, repeat="serial")
            pending_file = workspace / "state" / "daily-review.pending"
            pending_file.parent.mkdir(parents=True)
            pending_file.write_text("1\n", encoding="utf-8")

            result = subprocess.run(
                [sys.executable, str(RUN_SCRIPT), "--task-file", str(task_file)],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=5,
            )
            calls = read_fake_coco_calls(workspace)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([call["kind"] for call in calls], ["submit", "status", "submit"])
        self.assertFalse(pending_file.exists())

    def test_runner_parallel_policy_serializes_state_writes_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            fake_coco = write_fake_coco(workspace, status="finished")
            task_file = write_task_file(workspace, fake_coco, repeat="parallel")
            state_dir = workspace / "state"
            state_dir.mkdir(parents=True)
            state_lock = state_dir / "daily-review.state.json.lock"

            with state_lock.open("a+", encoding="utf-8") as lock:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                process = subprocess.Popen(
                    [sys.executable, str(RUN_SCRIPT), "--task-file", str(task_file)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                try:
                    wait_for_fake_coco_call(workspace, expected_count=1)
                    self.assertIsNone(process.poll())
                finally:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                stdout, stderr = process.communicate(timeout=5)
            state = json.loads(
                (state_dir / "daily-review.state.json").read_text(encoding="utf-8")
            )

        self.assertEqual(process.returncode, 0, stderr)
        self.assertIn('"job_id": "job-new"', stdout)
        self.assertEqual(state["last_job_id"], "job-new")

    def test_runner_fails_closed_when_previous_job_status_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            fake_coco = write_fake_coco(workspace, status=None)
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

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("failed to resolve previous job job-old status", result.stderr)
        self.assertEqual([call["kind"] for call in calls], ["status"])


def run_add(
    workspace: Path,
    *args: str,
    env: dict[str, str] | None = None,
    explicit_dirs: bool = True,
) -> subprocess.CompletedProcess[str]:
    full_env = os.environ.copy()
    full_env["COCO_SKILL_DIR"] = str(ROOT)
    full_env["COCO_CRONTAB_DIR"] = str(workspace / "supercronic")
    if env:
        full_env.update(env)
    directory_args = [
        "--install-dir",
        str(workspace / "install"),
        "--state-dir",
        str(workspace / "state"),
        "--log-dir",
        str(workspace / "logs"),
    ] if explicit_dirs else []
    return subprocess.run(
        [
            sys.executable,
            str(ADD_SCRIPT),
            *directory_args,
            *args,
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=full_env,
    )


def render_restore_block() -> str:
    return "\n".join(
        [
            "# BEGIN coco-cronjob id=daily-review",
            "CRON_TZ=Asia/Shanghai",
            (
                "15 * * * * 'uv' 'run' '--script' '/data/cronjob_run.py' "
                "'--task-file' '/data/daily-review.json' >> '/data/daily-review.log' 2>&1"
            ),
            "CRON_TZ=",
            "# END coco-cronjob id=daily-review",
            "",
        ]
    )


def write_fake_coco(workspace: Path, *, status: str | None) -> Path:
    path = workspace / "fake-coco.py"
    calls_file = workspace / "coco-calls.jsonl"
    status_response = (
        f"print(json.dumps({{'job': {{'status': {status!r}}}}}))\n"
        "                raise SystemExit(0)"
        if status is not None
        else "print('status unavailable', file=sys.stderr)\n                raise SystemExit(1)"
    )
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
                {status_response}
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


def wait_for_fake_coco_call(workspace: Path, *, expected_count: int) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if len(read_fake_coco_calls(workspace)) >= expected_count:
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {expected_count} fake coco calls")


if __name__ == "__main__":
    unittest.main()
