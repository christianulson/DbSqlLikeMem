import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import check_cross_dialect_snapshot_format as sut


class CheckCrossDialectSnapshotFormatTests(unittest.TestCase):
    def test_validate_snapshot_accepts_strategy_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "cross-dialect-strategy-snapshot.md"
            path.write_text(
                "\n".join(
                    [
                        "# Cross-dialect strategy snapshot",
                        "",
                        "Generated at: manual-placeholder",
                        "Profile: strategy",
                        "",
                        "| Provider project | Test filter | Status |",
                        "| --- | --- | --- |",
                        "| _Generate with `bash scripts/refresh_cross_dialect_snapshots.sh`_ | _Category=Strategy_ | _PENDING_ |",
                    ]
                ),
                encoding="utf-8",
            )

            issues = sut.validate_snapshot(path, "strategy")

            self.assertEqual(issues, [])

    def test_validate_snapshot_reports_profile_mismatch_for_strategy(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "cross-dialect-strategy-snapshot.md"
            path.write_text(
                "\n".join(
                    [
                        "# Cross-dialect strategy snapshot",
                        "",
                        "Generated at: manual-placeholder",
                        "Profile: parser",
                        "",
                        "| Provider project | Test filter | Status |",
                        "| --- | --- | --- |",
                        "| _Generate with `bash scripts/refresh_cross_dialect_snapshots.sh`_ | _Category=Strategy_ | _PENDING_ |",
                    ]
                ),
                encoding="utf-8",
            )

            issues = sut.validate_snapshot(path, "strategy")

            self.assertEqual(
                issues,
                ["profile mismatch: expected 'strategy', found 'parser'"],
            )


if __name__ == "__main__":
    unittest.main()
