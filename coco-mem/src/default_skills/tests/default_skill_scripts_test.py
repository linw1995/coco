from __future__ import annotations

import py_compile
import tempfile
import unittest
from pathlib import Path


DEFAULT_SKILLS_DIR = Path(__file__).resolve().parents[1]


class DefaultSkillScriptTests(unittest.TestCase):
    def test_all_builtin_python_scripts_compile(self) -> None:
        scripts = sorted(DEFAULT_SKILLS_DIR.glob("*/scripts/*.py"))

        self.assertGreater(len(scripts), 0)
        with tempfile.TemporaryDirectory() as directory:
            cache_dir = Path(directory)
            for script in scripts:
                with self.subTest(script=script.relative_to(DEFAULT_SKILLS_DIR)):
                    cfile = (
                        cache_dir
                        / script.relative_to(DEFAULT_SKILLS_DIR).with_suffix(".pyc")
                    )
                    cfile.parent.mkdir(parents=True, exist_ok=True)
                    py_compile.compile(str(script), cfile=str(cfile), doraise=True)

    def test_each_builtin_script_directory_has_tests(self) -> None:
        script_dirs = sorted(
            {path.parent.parent for path in DEFAULT_SKILLS_DIR.glob("*/scripts/*.py")}
        )

        self.assertGreater(len(script_dirs), 0)
        for skill_dir in script_dirs:
            with self.subTest(skill=skill_dir.name):
                tests = sorted((skill_dir / "tests").glob("*_test.py"))
                self.assertGreater(
                    len(tests),
                    0,
                    f"{skill_dir.name} builtin scripts must have offline tests",
                )


if __name__ == "__main__":
    unittest.main()
