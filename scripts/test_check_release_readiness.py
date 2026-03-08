import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import check_release_readiness as sut


class CheckReleaseReadinessWikiPathTests(unittest.TestCase):
    def test_get_wiki_doc_paths_prefers_submodule_layout(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            current_home = root / "docs" / "Wiki" / "Home.md"
            legacy_home = root / "docs" / "wiki" / "pages" / "Home.md"
            current_home.parent.mkdir(parents=True, exist_ok=True)
            legacy_home.parent.mkdir(parents=True, exist_ok=True)
            current_home.write_text("# Home", encoding="utf-8")
            legacy_home.write_text("# Legacy Home", encoding="utf-8")

            paths = sut.get_wiki_doc_paths(root)

            self.assertEqual(paths["home"], current_home)

    def test_get_wiki_doc_paths_falls_back_to_legacy_layout(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            legacy_getting_started = root / "docs" / "wiki" / "pages" / "Getting-Started.md"
            legacy_publishing = root / "docs" / "wiki" / "pages" / "Publishing.md"
            legacy_providers = root / "docs" / "wiki" / "pages" / "Providers-and-Compatibility.md"
            legacy_home = root / "docs" / "wiki" / "README.md"

            legacy_getting_started.parent.mkdir(parents=True, exist_ok=True)
            legacy_getting_started.write_text("# Getting Started", encoding="utf-8")
            legacy_publishing.write_text("# Publishing", encoding="utf-8")
            legacy_providers.write_text("# Providers", encoding="utf-8")
            legacy_home.write_text("# Home", encoding="utf-8")

            paths = sut.get_wiki_doc_paths(root)

            self.assertEqual(paths["home"], legacy_home)
            self.assertEqual(paths["getting_started"], legacy_getting_started)
            self.assertEqual(paths["publishing"], legacy_publishing)
            self.assertEqual(paths["providers"], legacy_providers)


class CheckReleaseReadinessCommunicationTests(unittest.TestCase):
    def test_check_release_communication_accepts_current_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._write(
                root / "CHANGELOG.md",
                "## [Unreleased]\n### Tooling and docs\nKnown limitations still open\n",
            )
            self._write(
                root / "docs" / "publishing.md",
                "CHANGELOG.md\nlimitação\nv<versao>\nvsix-v<versao-da-vsix>\nvscode-v<versao-da-extensao>\n",
            )
            self._write(
                root / "src" / "DbSqlLikeMem.VsCodeExtension" / "README.md",
                "CHANGELOG.md\ndocs/publishing.md\nvscode-v*\npackage.json\n",
            )
            self._write(
                root / "src" / "DbSqlLikeMem.VisualStudioExtension" / "README.md",
                "CHANGELOG.md\ndocs/publishing.md\nvsix-v*\nsource.extension.vsixmanifest\n",
            )
            self._write(
                root / "docs" / "Wiki" / "Publishing.md",
                "CHANGELOG.md\nKnown limitations still open\nv*\nvsix-v*\nvscode-v*\n",
            )

            failures = sut.check_release_communication(root)

            self.assertEqual(failures, [])

    def test_check_release_communication_reports_missing_changelog_reference(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._write(
                root / "CHANGELOG.md",
                "## [Unreleased]\n### Tooling and docs\nKnown limitations still open\n",
            )
            self._write(
                root / "docs" / "publishing.md",
                "CHANGELOG.md\nlimitação\nv<versao>\nvsix-v<versao-da-vsix>\nvscode-v<versao-da-extensao>\n",
            )
            self._write(
                root / "src" / "DbSqlLikeMem.VsCodeExtension" / "README.md",
                "docs/publishing.md\nvscode-v*\npackage.json\n",
            )
            self._write(
                root / "src" / "DbSqlLikeMem.VisualStudioExtension" / "README.md",
                "CHANGELOG.md\ndocs/publishing.md\nvsix-v*\nsource.extension.vsixmanifest\n",
            )
            self._write(
                root / "docs" / "Wiki" / "Publishing.md",
                "CHANGELOG.md\nKnown limitations still open\nv*\nvsix-v*\nvscode-v*\n",
            )

            failures = sut.check_release_communication(root)

            self.assertTrue(any("DbSqlLikeMem.VsCodeExtension/README.md" in failure for failure in failures))

    @staticmethod
    def _write(path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
