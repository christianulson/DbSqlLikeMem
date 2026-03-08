import sys
import tempfile
import unittest
import zipfile
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import check_nuget_package_metadata as sut


class CheckNugetPackageMetadataTests(unittest.TestCase):
    def test_validate_package_accepts_matching_version_and_filename(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            package_path = root / "DbSqlLikeMem.Core.1.12.0.nupkg"
            self._create_package(package_path, "1.12.0")

            issues = sut.validate_package(package_path, self._expected_metadata())

            self.assertEqual(issues, [])

    def test_validate_package_reports_version_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            package_path = root / "DbSqlLikeMem.Core.1.12.0.nupkg"
            self._create_package(package_path, "1.11.9")

            issues = sut.validate_package(package_path, self._expected_metadata())

            self.assertTrue(any("version mismatch" in issue for issue in issues))

    def test_validate_package_reports_filename_suffix_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            package_path = root / "DbSqlLikeMem.Core.latest.nupkg"
            self._create_package(package_path, "1.12.0")

            issues = sut.validate_package(package_path, self._expected_metadata())

            self.assertTrue(any("filename does not end" in issue for issue in issues))

    def test_validate_package_reports_license_acceptance_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            package_path = root / "DbSqlLikeMem.Core.1.12.0.nupkg"
            self._create_package(package_path, "1.12.0", require_license_acceptance="true")

            issues = sut.validate_package(package_path, self._expected_metadata())

            self.assertTrue(any("requireLicenseAcceptance mismatch" in issue for issue in issues))

    @staticmethod
    def _expected_metadata() -> dict[str, str]:
        return {
            "Version": "1.12.0",
            "RepositoryUrl": "https://github.com/christianulson/DbSqlLikeMem",
            "RepositoryType": "git",
            "PackageLicenseExpression": "MIT",
            "PackageProjectUrl": "https://github.com/christianulson/DbSqlLikeMem",
            "Authors": "DbSqlLikeMem Contributors",
            "PackageReadmeFile": "README.md",
            "PackageTags": "dbsqllikemem sql",
            "PackageReleaseNotes": "See release notes and changelog in the GitHub repository.",
            "PackageRequireLicenseAcceptance": "false",
        }

    @staticmethod
    def _create_package(
        package_path: Path,
        version: str,
        require_license_acceptance: str = "false",
    ) -> None:
        nuspec = f"""<?xml version="1.0"?>
<package xmlns="http://schemas.microsoft.com/packaging/2013/05/nuspec.xsd">
  <metadata>
    <id>DbSqlLikeMem.Core</id>
    <version>{version}</version>
    <authors>DbSqlLikeMem Contributors</authors>
    <requireLicenseAcceptance>{require_license_acceptance}</requireLicenseAcceptance>
    <license type="expression">MIT</license>
    <projectUrl>https://github.com/christianulson/DbSqlLikeMem</projectUrl>
    <readme>README.md</readme>
    <tags>dbsqllikemem sql</tags>
    <releaseNotes>See release notes and changelog in the GitHub repository.</releaseNotes>
    <repository type="git" url="https://github.com/christianulson/DbSqlLikeMem.git" />
  </metadata>
</package>
"""
        with zipfile.ZipFile(package_path, "w") as archive:
            archive.writestr("DbSqlLikeMem.Core.nuspec", nuspec)
            archive.writestr("README.md", "# Package")


if __name__ == "__main__":
    unittest.main()
