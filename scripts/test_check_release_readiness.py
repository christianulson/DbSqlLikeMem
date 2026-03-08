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


class CheckReleaseReadinessWorkflowTests(unittest.TestCase):
    def test_check_workflows_accepts_tag_and_version_source_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._write(
                root / ".github" / "workflows" / "nuget-publish.yml",
                'tags:\n- "v*"\nNUGET_API_KEY\nvars.NUGET_PUBLISH_ENVIRONMENT\nscripts/check_release_readiness.py\nsrc/Directory.Build.props\nscripts/check_nuget_package_metadata.py --artifacts-dir ./artifacts\n',
            )
            self._write(
                root / ".github" / "workflows" / "vsix-publish.yml",
                'tags:\n- "vsix-v*"\nVS_MARKETPLACE_TOKEN\nsource.extension.vsixmanifest\n--strict-marketplace-placeholders\n',
            )
            self._write(
                root / ".github" / "workflows" / "vscode-extension-publish.yml",
                'tags:\n- "vscode-v*"\nVSCE_PAT\nsrc/DbSqlLikeMem.VsCodeExtension/package.json\nnpm run publish\n',
            )

            failures = sut.check_workflows(root)

            self.assertEqual(failures, [])

    def test_check_workflows_reports_missing_version_source_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._write(
                root / ".github" / "workflows" / "nuget-publish.yml",
                'tags:\n- "v*"\nNUGET_API_KEY\nvars.NUGET_PUBLISH_ENVIRONMENT\nscripts/check_release_readiness.py\nscripts/check_nuget_package_metadata.py --artifacts-dir ./artifacts\n',
            )
            self._write(
                root / ".github" / "workflows" / "vsix-publish.yml",
                'tags:\n- "vsix-v*"\nVS_MARKETPLACE_TOKEN\nsource.extension.vsixmanifest\n--strict-marketplace-placeholders\n',
            )
            self._write(
                root / ".github" / "workflows" / "vscode-extension-publish.yml",
                'tags:\n- "vscode-v*"\nVSCE_PAT\nsrc/DbSqlLikeMem.VsCodeExtension/package.json\nnpm run publish\n',
            )

            failures = sut.check_workflows(root)

            self.assertTrue(any("nuget-publish.yml" in failure and "src/Directory.Build.props" in failure for failure in failures))

    def test_check_workflows_reports_missing_nuget_readiness_gate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._write(
                root / ".github" / "workflows" / "nuget-publish.yml",
                'tags:\n- "v*"\nNUGET_API_KEY\nvars.NUGET_PUBLISH_ENVIRONMENT\nsrc/Directory.Build.props\nscripts/check_nuget_package_metadata.py --artifacts-dir ./artifacts\n',
            )
            self._write(
                root / ".github" / "workflows" / "vsix-publish.yml",
                'tags:\n- "vsix-v*"\nVS_MARKETPLACE_TOKEN\nsource.extension.vsixmanifest\n--strict-marketplace-placeholders\n',
            )
            self._write(
                root / ".github" / "workflows" / "vscode-extension-publish.yml",
                'tags:\n- "vscode-v*"\nVSCE_PAT\nsrc/DbSqlLikeMem.VsCodeExtension/package.json\nnpm run publish\n',
            )

            failures = sut.check_workflows(root)

            self.assertTrue(
                any(
                    "nuget-publish.yml" in failure and "scripts/check_release_readiness.py" in failure
                    for failure in failures
                )
            )

    @staticmethod
    def _write(path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


class CheckReleaseReadinessDocsTests(unittest.TestCase):
    def test_check_docs_accepts_old_provider_guide_with_framework_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._write_doc_set(root)
            self._write(
                root / "docs" / "old" / "providers-and-features.md",
                "src/Directory.Build.props\nnet462\nnetstandard2.0\nnet8.0\ndocs/getting-started.md\ndocs/publishing.md\n",
            )

            failures = sut.check_docs(root)

            self.assertEqual(failures, [])

    def test_check_docs_reports_missing_framework_contract_in_old_provider_guide(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._write_doc_set(root)
            self._write(
                root / "docs" / "old" / "providers-and-features.md",
                "docs/getting-started.md\ndocs/publishing.md\n",
            )

            failures = sut.check_docs(root)

            self.assertTrue(
                any("docs/old/providers-and-features.md" in failure and "net462" in failure for failure in failures)
            )

    @classmethod
    def _write_doc_set(cls, root: Path) -> None:
        cls._write(
            root / "templates" / "dbsqllikemem" / "README.md",
            "vCurrent/\nvNext/\nreview-checklist.md\nreview-metadata.json\napi\nworker\n{{ClassName}}\n{{Namespace}}\n",
        )
        cls._write(
            root / "templates" / "dbsqllikemem" / "review-checklist.md",
            "TemplateContentRenderer\nCHANGELOG.md\ndocs/features-backlog/index.md\ndocs/features-backlog/status-operational.md\ntemplates/dbsqllikemem/vCurrent\nreview-metadata.json\n{{Namespace}}\n",
        )
        cls._write(
            root / "templates" / "dbsqllikemem" / "vNext" / "README.md",
            "vCurrent\ndocs/features-backlog/index.md\nstatus-operational.md\nCHANGELOG.md\nreview-metadata.json\n",
        )
        cls._write(
            root / "docs" / "publishing.md",
            "CHANGELOG.md\ndocs/features-backlog/index.md\ndocs/features-backlog/status-operational.md\nscripts/refresh_cross_dialect_snapshots.sh\nscripts/check_nuget_package_metadata.py\n.github/workflows/nuget-publish.yml\n.github/workflows/vsix-publish.yml\n.github/workflows/vscode-extension-publish.yml\nsrc/Directory.Build.props\nsrc/DbSqlLikeMem.VisualStudioExtension/source.extension.vsixmanifest\nsrc/DbSqlLikeMem.VsCodeExtension/package.json\nv<versao>\nvsix-v<versao-da-vsix>\nvscode-v<versao-da-extensao>\nstrategy\n",
        )
        cls._write(
            root / "docs" / "README.md",
            "cross-dialect-parser-snapshot.md\ncross-dialect-strategy-snapshot.md\nfeatures-backlog/status-operational.md\nfeatures-backlog/progress-update-checklist.md\nCHANGELOG.md\npublishing.md\ntemplates/dbsqllikemem/vCurrent\ntemplates/dbsqllikemem/review-checklist.md\ntemplates/dbsqllikemem/review-metadata.json\nscripts/check_nuget_package_metadata.py\nDirectory.Build.props\nsource.extension.vsixmanifest\npackage.json\nWiki/Home.md\n",
        )
        cls._write(
            root / "docs" / "getting-started.md",
            "net462\nnetstandard2.0\nnet8.0\nnet6.0\nsrc/Directory.Build.props\ndocs/publishing.md\nWiki/Home.md\n",
        )
        cls._write(
            root / "docs" / "info" / "multi-target-compat-audit.md",
            "artefato histórico\nsrc/Directory.Build.props\nREADME.md\ndocs/getting-started.md\n",
        )
        cls._write(
            root / "docs" / "wiki_setup" / "README.md",
            "docs/Wiki\n.wiki.git\ndocs/Wiki/Home.md\n",
        )
        cls._write(
            root / "docs" / "features-backlog" / "progress-update-checklist.md",
            "Seção/item do backlog:\nArquivos alterados:\nProviders afetados:\nTestes novos ou atualizados:\nComando planejado ou executado:\nResultado observado:\n",
        )
        cls._write(
            root / ".github" / "pull_request_template.md",
            "Backlog item or section:\nPercentual updated in `docs/features-backlog/index.md`:\nEvidence checklist reference: `docs/features-backlog/progress-update-checklist.md`\nValidation evidence is recorded above\n",
        )
        cls._write(root / "README.md", "net462\nnetstandard2.0\nnet8.0\nnet6.0\nsrc/Directory.Build.props\n")
        cls._write(root / "src" / "README.md", "net462\nnetstandard2.0\nnet8.0\nnet6.0\nsrc/Directory.Build.props\n")
        cls._write(
            root / "src" / "DbSqlLikeMem.VsCodeExtension" / "README.md",
            ".github/workflows/vscode-extension-publish.yml\nVSCE_PAT\npublisher\npackage.json\nContrato do workflow\ntemplates/dbsqllikemem/vCurrent\nvscode-v*\nnpm run package\nnpm run publish\n",
        )
        cls._write(
            root / "src" / "DbSqlLikeMem.VisualStudioExtension" / "README.md",
            "Visual Studio **2022\n.github/workflows/vsix-publish.yml\nVS_MARKETPLACE_TOKEN\nvsix-v*\nsource.extension.vsixmanifest\nContrato do workflow\neng/visualstudio/PublishManifest.json\ntemplates/dbsqllikemem/vCurrent\nscripts/check_release_readiness.py --strict-marketplace-placeholders\n",
        )
        cls._write(root / "CHANGELOG.md", "## [Unreleased]\nKnown limitations still open\n")
        cls._write(
            root / "docs" / "Wiki" / "Home.md",
            "https://github.com/christianulson/DbSqlLikeMem/blob/main/README.md\nhttps://github.com/christianulson/DbSqlLikeMem/blob/main/docs/getting-started.md\nhttps://github.com/christianulson/DbSqlLikeMem/blob/main/docs/old/providers-and-features.md\nhttps://github.com/christianulson/DbSqlLikeMem/blob/main/docs/publishing.md\n",
        )
        cls._write(
            root / "docs" / "Wiki" / "Getting-Started.md",
            "net462\nnetstandard2.0\nnet8.0\nnet6.0\nsrc/Directory.Build.props\ndocs/publishing.md\n",
        )
        cls._write(
            root / "docs" / "Wiki" / "Publishing.md",
            ".github/workflows/nuget-publish.yml\n.github/workflows/vsix-publish.yml\n.github/workflows/vscode-extension-publish.yml\nscripts/check_release_readiness.py\nscripts/check_nuget_package_metadata.py\nsrc/Directory.Build.props\nsrc/DbSqlLikeMem.VisualStudioExtension/source.extension.vsixmanifest\nsrc/DbSqlLikeMem.VsCodeExtension/package.json\nv*\nvsix-v*\nvscode-v*\nstrategy\n",
        )
        cls._write(
            root / "docs" / "Wiki" / "Providers-and-Compatibility.md",
            "MySQL\nSQL Server\nSQL Azure\nOracle\nPostgreSQL\nSQLite\nDB2\ndocs/old/providers-and-features.md\n",
        )

    @staticmethod
    def _write(path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


class CheckReleaseReadinessSnapshotTests(unittest.TestCase):
    def test_check_snapshots_accepts_strategy_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._write_snapshot(root / "docs" / "cross-dialect-smoke-snapshot.md", "smoke")
            self._write_snapshot(
                root / "docs" / "cross-dialect-aggregation-snapshot.md",
                "aggregation",
            )
            self._write_snapshot(root / "docs" / "cross-dialect-parser-snapshot.md", "parser")
            self._write_snapshot(
                root / "docs" / "cross-dialect-strategy-snapshot.md",
                "strategy",
            )

            failures = sut.check_snapshots(root)

            self.assertEqual(failures, [])

    @staticmethod
    def _write_snapshot(path: Path, profile: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "\n".join(
                [
                    f"# Cross-dialect {profile} snapshot",
                    "",
                    "Generated at: manual-placeholder",
                    f"Profile: {profile}",
                    "",
                    "| Provider project | Test filter | Status |",
                    "| --- | --- | --- |",
                    "| _placeholder_ | _manual-placeholder_ | _PENDING_ |",
                ]
            ),
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main()
