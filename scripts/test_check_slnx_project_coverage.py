import io
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock


SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import check_slnx_project_coverage as sut


class CheckSlnxProjectCoverageTests(unittest.TestCase):
    def test_load_slnx_projects_normalizes_backslashes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            slnx_path = Path(temp_dir) / "DbSqlLikeMem.slnx"
            slnx_path.write_text(
                '<Solution><Project Path="DbSqlLikeMem.SqlAzure\\DbSqlLikeMem.SqlAzure.csproj" /></Solution>',
                encoding="utf-8",
            )

            included = sut.load_slnx_projects(slnx_path)

            self.assertEqual(included, {"DbSqlLikeMem.SqlAzure/DbSqlLikeMem.SqlAzure.csproj"})

    def test_main_accepts_equivalent_paths_with_mixed_separators(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            src_dir = root / "src"
            project_path = src_dir / "DbSqlLikeMem.SqlAzure" / "DbSqlLikeMem.SqlAzure.csproj"
            project_path.parent.mkdir(parents=True, exist_ok=True)
            project_path.write_text("<Project />", encoding="utf-8")

            slnx_path = root / "DbSqlLikeMem.slnx"
            slnx_path.write_text(
                '<Solution><Project Path="DbSqlLikeMem.SqlAzure\\DbSqlLikeMem.SqlAzure.csproj" /></Solution>',
                encoding="utf-8",
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch.object(
                sys,
                "argv",
                [
                    "check_slnx_project_coverage.py",
                    "--src-dir",
                    str(src_dir),
                    "--slnx",
                    str(slnx_path),
                ],
            ):
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = sut.main()

            self.assertEqual(exit_code, 0)
            self.assertIn("missing=0 extra=0 root_level_unbalanced=0", stdout.getvalue())
            self.assertEqual(stderr.getvalue(), "")

    def test_main_accepts_allowed_root_level_project(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            src_dir = root / "src"
            project_path = src_dir / "DbSqlLikeMem.csproj"
            project_path.parent.mkdir(parents=True, exist_ok=True)
            project_path.write_text("<Project />", encoding="utf-8")

            slnx_path = root / "DbSqlLikeMem.slnx"
            slnx_path.write_text(
                '<Solution><Project Path="DbSqlLikeMem.csproj" /></Solution>',
                encoding="utf-8",
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch.object(
                sys,
                "argv",
                [
                    "check_slnx_project_coverage.py",
                    "--src-dir",
                    str(src_dir),
                    "--slnx",
                    str(slnx_path),
                ],
            ):
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = sut.main()

            self.assertEqual(exit_code, 0)
            self.assertIn("root_level_unbalanced=0", stdout.getvalue())
            self.assertEqual(stderr.getvalue(), "")

    def test_main_reports_unbalanced_root_level_projects(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            src_dir = root / "src"
            project_path = src_dir / "DbSqlLikeMem.NewFeature.csproj"
            project_path.parent.mkdir(parents=True, exist_ok=True)
            project_path.write_text("<Project />", encoding="utf-8")

            slnx_path = root / "DbSqlLikeMem.slnx"
            slnx_path.write_text(
                '<Solution><Project Path="DbSqlLikeMem.NewFeature.csproj" /></Solution>',
                encoding="utf-8",
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch.object(
                sys,
                "argv",
                [
                    "check_slnx_project_coverage.py",
                    "--src-dir",
                    str(src_dir),
                    "--slnx",
                    str(slnx_path),
                ],
            ):
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = sut.main()

            self.assertEqual(exit_code, 1)
            self.assertIn("root_level_unbalanced=1", stdout.getvalue())
            self.assertIn("Root-level projects outside the shared organization buckets:", stdout.getvalue())
            self.assertEqual(stderr.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
