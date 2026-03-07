#!/usr/bin/env python3
"""Validates NuGet package metadata inside .nupkg artifacts."""

from __future__ import annotations

import argparse
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path


def get_child(parent: ET.Element, local_name: str) -> ET.Element | None:
    for child in parent:
        tag = child.tag.split("}", 1)[-1] if "}" in child.tag else child.tag
        if tag == local_name:
            return child
    return None


def collect_packages(artifacts_dir: Path, exclude_prefix: str) -> list[Path]:
    return sorted(
        [
            path
            for path in artifacts_dir.glob("*.nupkg")
            if exclude_prefix not in path.name
        ]
    )


def normalize_tag_tokens(value: str) -> set[str]:
    return {
        token.strip().lower()
        for token in value.replace(";", " ").replace(",", " ").split()
        if token.strip()
    }


def parse_expected_metadata(props_path: Path) -> dict[str, str]:
    root = ET.fromstring(props_path.read_text(encoding="utf-8"))
    expected: dict[str, str] = {}

    for property_group in root.findall("PropertyGroup"):
        for child in property_group:
            tag = child.tag.split("}", 1)[-1] if "}" in child.tag else child.tag
            text = (child.text or "").strip()
            if text and tag not in expected:
                expected[tag] = text

    return expected


def validate_package(package: Path, expected_metadata: dict[str, str]) -> list[str]:
    issues: list[str] = []

    with zipfile.ZipFile(package) as archive:
        archive_entries = set(archive.namelist())
        nuspec_name = next((name for name in archive.namelist() if name.endswith(".nuspec")), None)
        if not nuspec_name:
            return [f"{package.name}: does not contain a .nuspec file"]

        with archive.open(nuspec_name) as nuspec:
            root = ET.parse(nuspec).getroot()

    metadata = get_child(root, "metadata")
    if metadata is None:
        return [f"{package.name}: has no metadata section in nuspec"]

    repository = get_child(metadata, "repository")
    repo_url = repository.attrib.get("url") if repository is not None else None
    expected_repository_url = expected_metadata.get("RepositoryUrl", "")
    if not repo_url:
        issues.append(f"{package.name}: missing repository URL metadata")
    elif expected_repository_url and expected_repository_url not in repo_url:
        issues.append(
            f"{package.name}: repository URL mismatch (expected contains '{expected_repository_url}', found '{repo_url}')"
        )
    repo_type = repository.attrib.get("type") if repository is not None else None
    expected_repository_type = expected_metadata.get("RepositoryType", "")
    if expected_repository_type and repo_type != expected_repository_type:
        issues.append(
            f"{package.name}: repository type mismatch (expected '{expected_repository_type}', found '{repo_type}')"
        )

    license_expression = get_child(metadata, "license")
    if license_expression is None:
        issues.append(f"{package.name}: missing license metadata")
    else:
        expected_license = expected_metadata.get("PackageLicenseExpression", "")
        actual_license = (license_expression.text or "").strip()
        actual_license_type = str(license_expression.attrib.get("type", "")).strip()
        if expected_license and actual_license != expected_license:
            issues.append(
                f"{package.name}: license mismatch (expected '{expected_license}', found '{actual_license}')"
            )
        if expected_license and actual_license_type != "expression":
            issues.append(
                f"{package.name}: expected license type 'expression', found '{actual_license_type}'"
            )

    project_url = get_child(metadata, "projectUrl")
    if project_url is None or not (project_url.text or "").strip():
        issues.append(f"{package.name}: missing project URL metadata")
    else:
        expected_project_url = expected_metadata.get("PackageProjectUrl", "")
        actual_project_url = (project_url.text or "").strip()
        if expected_project_url and actual_project_url != expected_project_url:
            issues.append(
                f"{package.name}: project URL mismatch (expected '{expected_project_url}', found '{actual_project_url}')"
            )

    authors = get_child(metadata, "authors")
    expected_authors = expected_metadata.get("Authors", "")
    if authors is None or not (authors.text or "").strip():
        issues.append(f"{package.name}: missing authors metadata")
    elif expected_authors and (authors.text or "").strip() != expected_authors:
        issues.append(
            f"{package.name}: authors mismatch (expected '{expected_authors}', found '{(authors.text or '').strip()}')"
        )

    readme = get_child(metadata, "readme")
    expected_readme = expected_metadata.get("PackageReadmeFile", "")
    if readme is None or not (readme.text or "").strip():
        issues.append(f"{package.name}: missing readme metadata")
    else:
        actual_readme = (readme.text or "").strip()
        if expected_readme and actual_readme != expected_readme:
            issues.append(
                f"{package.name}: readme mismatch (expected '{expected_readme}', found '{actual_readme}')"
            )
        if actual_readme:
            readme_entry = next(
                (
                    name
                    for name in archive_entries
                    if name == actual_readme or name.endswith(f"/{actual_readme}")
                ),
                None,
            )
            if readme_entry is None:
                issues.append(f"{package.name}: readme file '{actual_readme}' not found inside package")

    tags = get_child(metadata, "tags")
    expected_tags = normalize_tag_tokens(expected_metadata.get("PackageTags", ""))
    if tags is None or not (tags.text or "").strip():
        issues.append(f"{package.name}: missing tags metadata")
    elif expected_tags:
        actual_tags = normalize_tag_tokens((tags.text or "").strip())
        missing_tags = sorted(expected_tags - actual_tags)
        if missing_tags:
            issues.append(
                f"{package.name}: missing expected tags {', '.join(missing_tags)}"
            )

    release_notes = get_child(metadata, "releaseNotes")
    expected_release_notes = expected_metadata.get("PackageReleaseNotes", "")
    if release_notes is None or not (release_notes.text or "").strip():
        issues.append(f"{package.name}: missing release notes metadata")
    elif expected_release_notes and (release_notes.text or "").strip() != expected_release_notes:
        issues.append(
            f"{package.name}: release notes mismatch (expected '{expected_release_notes}', found '{(release_notes.text or '').strip()}')"
        )

    return issues


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts-dir", default="artifacts", help="Directory containing .nupkg files.")
    parser.add_argument(
        "--props",
        default="src/Directory.Build.props",
        help="MSBuild props file used as source of truth for package metadata.",
    )
    parser.add_argument(
        "--exclude-prefix",
        default="DbSqlLikeMem.VisualStudioExtension.",
        help="Package filename prefix to exclude from validation.",
    )
    parser.add_argument(
        "--allow-missing-artifacts",
        action="store_true",
        help="Return success when no publishable packages are found.",
    )
    args = parser.parse_args()

    artifacts_dir = Path(args.artifacts_dir)
    if not artifacts_dir.exists():
        print(f"[FAIL] artifacts directory not found: {artifacts_dir}")
        return 0 if args.allow_missing_artifacts else 1

    packages = collect_packages(artifacts_dir, args.exclude_prefix)
    if not packages:
        print(f"[FAIL] No publishable packages found in {artifacts_dir}.")
        return 0 if args.allow_missing_artifacts else 1

    props_path = Path(args.props)
    if not props_path.exists():
        print(f"[FAIL] props file not found: {props_path}")
        return 1

    expected_metadata = parse_expected_metadata(props_path)

    failures: list[str] = []
    for package in packages:
        package_issues = validate_package(package, expected_metadata)
        if package_issues:
            failures.extend(package_issues)
        else:
            print(f"[PASS] {package.name}")

    if failures:
        print("[FAIL] NuGet package metadata validation found issues:")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print(f"[PASS] Validated metadata for {len(packages)} publishable package(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
