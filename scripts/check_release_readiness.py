#!/usr/bin/env python3
"""Validates release-readiness documentation, metadata, and publishing workflows."""

from __future__ import annotations

import argparse
import json
import re
import xml.etree.ElementTree as ET
from datetime import date
from dataclasses import dataclass
from pathlib import Path

from check_cross_dialect_snapshot_format import validate_snapshot

REPOSITORY_URL_RE = re.compile(r"<RepositoryUrl>(?P<value>[^<]+)</RepositoryUrl>")
VERSION_RE = re.compile(r"<Version>(?P<value>[^<]+)</Version>")
MINIMUM_VISUAL_STUDIO_VERSION_RE = re.compile(
    r"<MinimumVisualStudioVersion>(?P<value>[^<]+)</MinimumVisualStudioVersion>"
)
SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)
PACKAGE_NLS_TOKEN_RE = re.compile(r"^%(?P<key>[^%]+)%$")
TEMPLATE_TOKEN_RE = re.compile(r"\{\{[^{}\r\n]+\}\}")
SUPPORTED_TEMPLATE_TOKENS = {
    "{{ClassName}}",
    "{{ObjectName}}",
    "{{Schema}}",
    "{{ObjectType}}",
    "{{DatabaseType}}",
    "{{DatabaseName}}",
    "{{Namespace}}",
}
SEMVER_BREAKING_NOTE_MARKERS = (
    "breaking change",
    "breaking changes",
    "breaking:",
    "quebra de contrato",
    "incompatible",
    "incompatível",
    "incompativel",
    "remove public",
    "removed public",
    "removes public",
    "removido",
    "removida",
)
SEMVER_FEATURE_NOTE_MARKERS = (
    "feature",
    "features",
    "suporte",
    "support",
    "supported",
    "agora também",
    "agora tambem",
    "passou a",
    "adicionado",
    "adicionada",
    "adicionados",
    "adicionadas",
    "implementado",
    "implementada",
    "implementados",
    "implementadas",
    "ganhou",
    "materializado",
    "materializada",
    "expandido",
    "expandida",
)
SEMVER_FIX_NOTE_MARKERS = (
    "fix",
    "fixed",
    "bug",
    "corrigido",
    "corrigida",
    "corrigidos",
    "corrigidas",
    "hardening",
    "valida",
    "validation",
    "rejeita",
    "bloqueia",
    "reduz",
    "normaliza",
    "alinha",
    "alinhar",
    "auditoria",
    "warning",
    "warning",
    "regress",
    "refator",
    "documentation",
    "documentação",
    "documentacao",
)


@dataclass(frozen=True)
class SemVerImpactSummary:
    """Summarizes the inferred release impact from unreleased changelog notes."""

    suggested_bump: str
    breaking_count: int
    feature_count: int
    fix_count: int


def load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def resolve_first_existing_path(*paths: Path) -> Path | None:
    for path in paths:
        if path.exists():
            return path

    return None


def get_wiki_doc_paths(root: Path) -> dict[str, Path | None]:
    return {
        "home": resolve_first_existing_path(
            root / "docs" / "Wiki" / "Home.md",
            root / "docs" / "wiki" / "pages" / "Home.md",
            root / "docs" / "wiki" / "README.md",
        ),
        "getting_started": resolve_first_existing_path(
            root / "docs" / "Wiki" / "Getting-Started.md",
            root / "docs" / "wiki" / "pages" / "Getting-Started.md",
        ),
        "publishing": resolve_first_existing_path(
            root / "docs" / "Wiki" / "Publishing.md",
            root / "docs" / "wiki" / "pages" / "Publishing.md",
        ),
        "providers": resolve_first_existing_path(
            root / "docs" / "Wiki" / "Providers-and-Compatibility.md",
            root / "docs" / "wiki" / "pages" / "Providers-and-Compatibility.md",
        ),
    }


def parse_directory_build_props(path: Path) -> tuple[str | None, str | None]:
    content = load_text(path)
    version_match = VERSION_RE.search(content)
    repository_match = REPOSITORY_URL_RE.search(content)
    version = version_match.group("value").strip() if version_match else None
    repository_url = repository_match.group("value").strip() if repository_match else None
    return version, repository_url


def parse_minimum_visual_studio_version(path: Path) -> str | None:
    content = load_text(path)
    match = MINIMUM_VISUAL_STUDIO_VERSION_RE.search(content)
    return match.group("value").strip() if match else None


def validate_semver(label: str, value: str | None) -> list[str]:
    if not value:
        return [f"{label}: missing version"]

    if not SEMVER_RE.fullmatch(value):
        return [f"{label}: invalid SemVer value '{value}'"]

    return []


def extract_unreleased_changelog_bullets(changelog_content: str) -> list[str]:
    bullets: list[str] = []
    in_unreleased = False

    for raw_line in changelog_content.splitlines():
        line = raw_line.strip()
        if line.startswith("## "):
            if line == "## [Unreleased]":
                in_unreleased = True
                continue
            if in_unreleased:
                break

        if in_unreleased and line.startswith("- "):
            bullets.append(line[2:].strip())

    return bullets


def classify_release_note_impact(note: str) -> str:
    lower_note = note.lower()
    if any(marker in lower_note for marker in SEMVER_BREAKING_NOTE_MARKERS):
        return "breaking"
    if any(marker in lower_note for marker in SEMVER_FEATURE_NOTE_MARKERS):
        return "feature"
    if any(marker in lower_note for marker in SEMVER_FIX_NOTE_MARKERS):
        return "fix"
    return "fix"


def analyze_release_semver_impact(changelog_content: str) -> SemVerImpactSummary | None:
    bullets = extract_unreleased_changelog_bullets(changelog_content)
    if not bullets:
        return None

    breaking_count = 0
    feature_count = 0
    fix_count = 0

    for bullet in bullets:
        impact = classify_release_note_impact(bullet)
        if impact == "breaking":
            breaking_count += 1
        elif impact == "feature":
            feature_count += 1
        else:
            fix_count += 1

    if breaking_count > 0:
        suggested_bump = "MAJOR"
    elif feature_count > 0:
        suggested_bump = "MINOR"
    else:
        suggested_bump = "PATCH"

    return SemVerImpactSummary(
        suggested_bump=suggested_bump,
        breaking_count=breaking_count,
        feature_count=feature_count,
        fix_count=fix_count,
    )


def collect_package_nls_tokens(value: object) -> set[str]:
    tokens: set[str] = set()

    if isinstance(value, dict):
        for item in value.values():
            tokens.update(collect_package_nls_tokens(item))
    elif isinstance(value, list):
        for item in value:
            tokens.update(collect_package_nls_tokens(item))
    elif isinstance(value, str):
        match = PACKAGE_NLS_TOKEN_RE.fullmatch(value.strip())
        if match:
            tokens.add(match.group("key"))

    return tokens


def check_required_files(root: Path) -> list[str]:
    wiki_paths = get_wiki_doc_paths(root)
    required_paths = [
        root / "CHANGELOG.md",
        root / "README.md",
        root / "src" / "README.md",
        root / "docs" / "publishing.md",
        root / "docs" / "README.md",
        root / "docs" / "getting-started.md",
        root / "docs" / "wiki_setup" / "README.md",
        root / "docs" / "features-backlog" / "index.md",
        root / "docs" / "features-backlog" / "progress-update-checklist.md",
        root / "docs" / "features-backlog" / "status-operational.md",
        root / "docs" / "cross-dialect-smoke-snapshot.md",
        root / "docs" / "cross-dialect-aggregation-snapshot.md",
        root / "docs" / "cross-dialect-parser-snapshot.md",
        root / "docs" / "cross-dialect-strategy-snapshot.md",
        root / "templates" / "dbsqllikemem" / "README.md",
        root / "templates" / "dbsqllikemem" / "review-checklist.md",
        root / "templates" / "dbsqllikemem" / "review-metadata.json",
        root / "templates" / "dbsqllikemem" / "vCurrent" / "api" / "model.template.txt",
        root / "templates" / "dbsqllikemem" / "vCurrent" / "api" / "repository.template.txt",
        root / "templates" / "dbsqllikemem" / "vCurrent" / "worker" / "model.template.txt",
        root / "templates" / "dbsqllikemem" / "vCurrent" / "worker" / "repository.template.txt",
        root / "templates" / "dbsqllikemem" / "vNext" / "README.md",
        root / "scripts" / "check_nuget_package_metadata.py",
        root / ".github" / "pull_request_template.md",
        root / ".github" / "workflows" / "nuget-publish.yml",
        root / ".github" / "workflows" / "vsix-publish.yml",
        root / ".github" / "workflows" / "vscode-extension-publish.yml",
        root / "src" / "DbSqlLikeMem.VsCodeExtension" / "package.json",
        root / "src" / "DbSqlLikeMem.VsCodeExtension" / "README.md",
        root / "src" / "DbSqlLikeMem.VisualStudioExtension" / "README.md",
        root / "src" / "DbSqlLikeMem.VisualStudioExtension" / "source.extension.vsixmanifest",
        root / "eng" / "visualstudio" / "PublishManifest.json",
    ]

    failures: list[str] = []
    for label, path in wiki_paths.items():
        if path is None:
            failures.append(f"required wiki file not found for '{label}'")

    for path in required_paths:
        if not path.exists():
            failures.append(f"required file not found: {path.relative_to(root)}")

    return failures


def check_snapshots(root: Path) -> list[str]:
    checks = [
        (root / "docs" / "cross-dialect-smoke-snapshot.md", "smoke"),
        (root / "docs" / "cross-dialect-aggregation-snapshot.md", "aggregation"),
        (root / "docs" / "cross-dialect-parser-snapshot.md", "parser"),
        (root / "docs" / "cross-dialect-strategy-snapshot.md", "strategy"),
    ]

    failures: list[str] = []
    for path, profile in checks:
        for issue in validate_snapshot(path, profile):
            failures.append(f"{path.relative_to(root)}: {issue}")

    return failures


def check_docs(root: Path) -> list[str]:
    wiki_paths = get_wiki_doc_paths(root)
    historical_multi_target_audit = root / "docs" / "info" / "multi-target-compat-audit.md"
    checks = {
        root / "templates" / "dbsqllikemem" / "README.md": [
            "vCurrent/",
            "vNext/",
            "review-checklist.md",
            "review-metadata.json",
            "api",
            "worker",
            "{{ClassName}}",
            "{{Namespace}}",
        ],
        root / "templates" / "dbsqllikemem" / "review-checklist.md": [
            "TemplateContentRenderer",
            "CHANGELOG.md",
            "docs/features-backlog/index.md",
            "docs/features-backlog/status-operational.md",
            "templates/dbsqllikemem/vCurrent",
            "review-metadata.json",
            "{{Namespace}}",
        ],
        root / "templates" / "dbsqllikemem" / "vNext" / "README.md": [
            "vCurrent",
            "docs/features-backlog/index.md",
            "status-operational.md",
            "CHANGELOG.md",
            "review-metadata.json",
        ],
        root / "docs" / "publishing.md": [
            "CHANGELOG.md",
            "docs/features-backlog/index.md",
            "docs/features-backlog/status-operational.md",
            "scripts/refresh_cross_dialect_snapshots.sh",
            "scripts/check_nuget_package_metadata.py",
            ".github/workflows/nuget-publish.yml",
            ".github/workflows/vsix-publish.yml",
            ".github/workflows/vscode-extension-publish.yml",
            "src/Directory.Build.props",
            "src/DbSqlLikeMem.VisualStudioExtension/source.extension.vsixmanifest",
            "src/DbSqlLikeMem.VsCodeExtension/package.json",
            "v<versao>",
            "vsix-v<versao-da-vsix>",
            "vscode-v<versao-da-extensao>",
            "strategy",
        ],
        root / "docs" / "README.md": [
            "cross-dialect-parser-snapshot.md",
            "cross-dialect-strategy-snapshot.md",
            "features-backlog/status-operational.md",
            "features-backlog/progress-update-checklist.md",
            "CHANGELOG.md",
            "publishing.md",
            "templates/dbsqllikemem/vCurrent",
            "templates/dbsqllikemem/review-checklist.md",
            "templates/dbsqllikemem/review-metadata.json",
            "scripts/check_nuget_package_metadata.py",
            "Directory.Build.props",
            "source.extension.vsixmanifest",
            "package.json",
            "Wiki/Home.md",
        ],
        root / "docs" / "getting-started.md": [
            "net462",
            "netstandard2.0",
            "net8.0",
            "net6.0",
            "src/Directory.Build.props",
            "docs/publishing.md",
            "Wiki/Home.md",
        ],
        root / "docs" / "old" / "providers-and-features.md": [
            "src/Directory.Build.props",
            "net462",
            "netstandard2.0",
            "net8.0",
            "docs/getting-started.md",
            "docs/publishing.md",
        ],
        root / "docs" / "wiki_setup" / "README.md": [
            "docs/Wiki",
            ".wiki.git",
            "docs/Wiki/Home.md",
        ],
        root / "docs" / "features-backlog" / "progress-update-checklist.md": [
            "Seção/item do backlog:",
            "Arquivos alterados:",
            "Providers afetados:",
            "Testes novos ou atualizados:",
            "Comando planejado ou executado:",
            "Resultado observado:",
        ],
        root / ".github" / "pull_request_template.md": [
            "Backlog item or section:",
            "Percentual updated in `docs/features-backlog/index.md`:",
            "Evidence checklist reference: `docs/features-backlog/progress-update-checklist.md`",
            "Validation evidence is recorded above",
        ],
        root / "README.md": [
            "net462",
            "netstandard2.0",
            "net8.0",
            "net6.0",
            "src/Directory.Build.props",
        ],
        root / "src" / "README.md": [
            "net462",
            "netstandard2.0",
            "net8.0",
            "net6.0",
            "src/Directory.Build.props",
        ],
        root / "src" / "DbSqlLikeMem.VsCodeExtension" / "README.md": [
            ".github/workflows/vscode-extension-publish.yml",
            "VSCE_PAT",
            "publisher",
            "package.json",
            "Contrato do workflow",
            "templates/dbsqllikemem/vCurrent",
            "vscode-v*",
            "npm run package",
            "npm run publish",
        ],
        root / "src" / "DbSqlLikeMem.VisualStudioExtension" / "README.md": [
            "Visual Studio **2022",
            ".github/workflows/vsix-publish.yml",
            "VS_MARKETPLACE_TOKEN",
            "vsix-v*",
            "source.extension.vsixmanifest",
            "Contrato do workflow",
            "eng/visualstudio/PublishManifest.json",
            "templates/dbsqllikemem/vCurrent",
            "scripts/check_release_readiness.py --strict-marketplace-placeholders",
        ],
        root / "CHANGELOG.md": [
            "## [Unreleased]",
            "Known limitations still open",
        ],
    }

    if historical_multi_target_audit.exists():
        checks[historical_multi_target_audit] = [
            "artefato histórico",
            "src/Directory.Build.props",
            "README.md",
            "docs/getting-started.md",
        ]

    wiki_home = wiki_paths["home"]
    if wiki_home is not None:
        checks[wiki_home] = [
            "https://github.com/christianulson/DbSqlLikeMem/blob/main/README.md",
            "https://github.com/christianulson/DbSqlLikeMem/blob/main/docs/getting-started.md",
            "https://github.com/christianulson/DbSqlLikeMem/blob/main/docs/old/providers-and-features.md",
            "https://github.com/christianulson/DbSqlLikeMem/blob/main/docs/publishing.md",
        ]

    wiki_getting_started = wiki_paths["getting_started"]
    if wiki_getting_started is not None:
        checks[wiki_getting_started] = [
            "net462",
            "netstandard2.0",
            "net8.0",
            "net6.0",
            "src/Directory.Build.props",
            "docs/publishing.md",
        ]

    wiki_publishing = wiki_paths["publishing"]
    if wiki_publishing is not None:
        checks[wiki_publishing] = [
            ".github/workflows/nuget-publish.yml",
            ".github/workflows/vsix-publish.yml",
            ".github/workflows/vscode-extension-publish.yml",
            "scripts/check_release_readiness.py",
            "scripts/check_nuget_package_metadata.py",
            "src/Directory.Build.props",
            "src/DbSqlLikeMem.VisualStudioExtension/source.extension.vsixmanifest",
            "src/DbSqlLikeMem.VsCodeExtension/package.json",
            "v*",
            "vsix-v*",
            "vscode-v*",
            "strategy",
        ]

    wiki_providers = wiki_paths["providers"]
    if wiki_providers is not None:
        checks[wiki_providers] = [
            "MySQL",
            "SQL Server",
            "SQL Azure",
            "Oracle",
            "PostgreSQL",
            "SQLite",
            "DB2",
            "docs/old/providers-and-features.md",
        ]

    failures: list[str] = []
    for label, path in wiki_paths.items():
        if path is None:
            failures.append(f"wiki documentation path not found for '{label}'")

    for path, required_tokens in checks.items():
        if not path.exists():
            continue
        content = load_text(path)
        for token in required_tokens:
            if token not in content:
                failures.append(f"{path.relative_to(root)}: missing reference '{token}'")

    return failures


def check_workflows(root: Path) -> list[str]:
    expected_tokens = {
        root / ".github" / "workflows" / "nuget-publish.yml": [
            'tags:',
            '- "v*"',
            "NUGET_API_KEY",
            "vars.NUGET_PUBLISH_ENVIRONMENT",
            "scripts/check_release_readiness.py",
            "src/Directory.Build.props",
            "scripts/check_nuget_package_metadata.py --artifacts-dir ./artifacts",
        ],
        root / ".github" / "workflows" / "vsix-publish.yml": [
            'tags:',
            '- "vsix-v*"',
            "VS_MARKETPLACE_TOKEN",
            "source.extension.vsixmanifest",
            "--strict-marketplace-placeholders",
        ],
        root / ".github" / "workflows" / "vscode-extension-publish.yml": [
            'tags:',
            '- "vscode-v*"',
            "VSCE_PAT",
            "src/DbSqlLikeMem.VsCodeExtension/package.json",
            "npm run publish",
        ],
    }

    failures: list[str] = []
    for path, tokens in expected_tokens.items():
        content = load_text(path)
        for token in tokens:
            if token not in content:
                failures.append(f"{path.relative_to(root)}: missing token '{token}'")

    return failures


def check_release_communication(root: Path) -> list[str]:
    failures: list[str] = []

    changelog_path = root / "CHANGELOG.md"
    changelog_content = load_text(changelog_path)
    if "## [Unreleased]" not in changelog_content:
        failures.append("CHANGELOG.md: missing '## [Unreleased]' heading")
    if "Known limitations still open" not in changelog_content:
        failures.append("CHANGELOG.md: missing 'Known limitations still open' section")
    if "### " not in changelog_content:
        failures.append("CHANGELOG.md: missing at least one scoped subsection ('### ...')")

    wiki_paths = get_wiki_doc_paths(root)
    checks = {
        root / "docs" / "publishing.md": [
            "CHANGELOG.md",
            "limitação",
            "v<versao>",
            "vsix-v<versao-da-vsix>",
            "vscode-v<versao-da-extensao>",
        ],
        root / "src" / "DbSqlLikeMem.VsCodeExtension" / "README.md": [
            "CHANGELOG.md",
            "docs/publishing.md",
            "vscode-v*",
            "package.json",
        ],
        root / "src" / "DbSqlLikeMem.VisualStudioExtension" / "README.md": [
            "CHANGELOG.md",
            "docs/publishing.md",
            "vsix-v*",
            "source.extension.vsixmanifest",
        ],
    }

    wiki_publishing = wiki_paths["publishing"]
    if wiki_publishing is not None:
        checks[wiki_publishing] = [
            "CHANGELOG.md",
            "Known limitations still open",
            "v*",
            "vsix-v*",
            "vscode-v*",
        ]

    for path, tokens in checks.items():
        content = load_text(path)
        for token in tokens:
            if token not in content:
                failures.append(f"{path.relative_to(root)}: missing release communication token '{token}'")

    return failures


def check_template_baselines(root: Path) -> list[str]:
    expected_tokens = {
        root / "templates" / "dbsqllikemem" / "vCurrent" / "api" / "model.template.txt": [
            "{{ClassName}}",
            "{{Schema}}",
            "{{ObjectName}}",
            "{{Namespace}}",
        ],
        root / "templates" / "dbsqllikemem" / "vCurrent" / "api" / "repository.template.txt": [
            "{{ClassName}}",
            "{{Schema}}",
            "{{ObjectName}}",
            "ListAsync",
        ],
        root / "templates" / "dbsqllikemem" / "vCurrent" / "worker" / "model.template.txt": [
            "{{ClassName}}",
            "{{Schema}}",
            "{{ObjectName}}",
            "SourceObject",
        ],
        root / "templates" / "dbsqllikemem" / "vCurrent" / "worker" / "repository.template.txt": [
            "{{ClassName}}",
            "{{Schema}}",
            "{{ObjectName}}",
            "ExecuteAsync",
        ],
    }

    failures: list[str] = []
    for path, tokens in expected_tokens.items():
        content = load_text(path)
        for token in tokens:
            if token not in content:
                failures.append(f"{path.relative_to(root)}: missing token '{token}'")

        referenced_tokens = sorted(set(TEMPLATE_TOKEN_RE.findall(content)))
        unsupported_tokens = [
            token for token in referenced_tokens if token not in SUPPORTED_TEMPLATE_TOKENS
        ]
        if unsupported_tokens:
            failures.append(
                f"{path.relative_to(root)}: unsupported template tokens {', '.join(unsupported_tokens)}"
            )

    return failures


def check_template_review_metadata(root: Path) -> list[str]:
    metadata_path = root / "templates" / "dbsqllikemem" / "review-metadata.json"
    data = json.loads(load_text(metadata_path))

    failures: list[str] = []

    current_baseline = str(data.get("currentBaseline", "")).strip()
    if current_baseline != "vCurrent":
        failures.append(
            "templates/dbsqllikemem/review-metadata.json: "
            f"currentBaseline must be 'vCurrent' (found '{current_baseline}')"
        )

    promotion_staging_path = str(data.get("promotionStagingPath", "")).strip()
    if promotion_staging_path != "templates/dbsqllikemem/vNext":
        failures.append(
            "templates/dbsqllikemem/review-metadata.json: "
            "promotionStagingPath must be 'templates/dbsqllikemem/vNext'"
        )

    review_cadence = str(data.get("reviewCadence", "")).strip()
    if review_cadence != "quarterly":
        failures.append(
            "templates/dbsqllikemem/review-metadata.json: "
            f"reviewCadence must be 'quarterly' (found '{review_cadence}')"
        )

    def parse_iso_date(field_name: str) -> date | None:
        raw_value = str(data.get(field_name, "")).strip()
        if not raw_value:
            failures.append(
                f"templates/dbsqllikemem/review-metadata.json: missing '{field_name}'"
            )
            return None

        try:
            return date.fromisoformat(raw_value)
        except ValueError:
            failures.append(
                "templates/dbsqllikemem/review-metadata.json: "
                f"'{field_name}' must use YYYY-MM-DD (found '{raw_value}')"
            )
            return None

    last_reviewed_on = parse_iso_date("lastReviewedOn")
    next_planned_review_on = parse_iso_date("nextPlannedReviewOn")
    if last_reviewed_on and next_planned_review_on and next_planned_review_on < last_reviewed_on:
        failures.append(
            "templates/dbsqllikemem/review-metadata.json: "
            "nextPlannedReviewOn must be greater than or equal to lastReviewedOn"
        )
    if next_planned_review_on and next_planned_review_on < date.today():
        failures.append(
            "templates/dbsqllikemem/review-metadata.json: "
            f"template baseline review is overdue since '{next_planned_review_on.isoformat()}'"
        )

    profiles = data.get("profiles", {})
    if not isinstance(profiles, dict):
        failures.append(
            "templates/dbsqllikemem/review-metadata.json: 'profiles' must be an object"
        )
    else:
        for profile_id in ("api", "worker"):
            profile_data = profiles.get(profile_id)
            if not isinstance(profile_data, dict):
                failures.append(
                    "templates/dbsqllikemem/review-metadata.json: "
                    f"missing object for profile '{profile_id}'"
                )
                continue

            focus = str(profile_data.get("focus", "")).strip()
            if not focus:
                failures.append(
                    "templates/dbsqllikemem/review-metadata.json: "
                    f"profile '{profile_id}' must define a non-empty focus"
                )

    evidence_files = data.get("evidenceFiles", [])
    if not isinstance(evidence_files, list) or not evidence_files:
        failures.append(
            "templates/dbsqllikemem/review-metadata.json: "
            "evidenceFiles must define at least one repository-relative file"
        )
    else:
        for evidence_file in evidence_files:
            path = root / str(evidence_file)
            if not path.exists():
                failures.append(
                    "templates/dbsqllikemem/review-metadata.json: "
                    f"evidence file not found '{evidence_file}'"
                )

    return failures


def check_vscode_extension(root: Path, repository_url: str) -> tuple[list[str], list[str]]:
    package_path = root / "src" / "DbSqlLikeMem.VsCodeExtension" / "package.json"
    data = json.loads(load_text(package_path))
    extension_root = package_path.parent

    failures: list[str] = []
    warnings: list[str] = []

    version = str(data.get("version", "")).strip()
    failures.extend(
        validate_semver("src/DbSqlLikeMem.VsCodeExtension/package.json", version)
    )

    scripts = data.get("scripts", {})
    for script_name in ("compile", "package", "publish", "vscode:prepublish", "generate:icon"):
        if not str(scripts.get(script_name, "")).strip():
            failures.append(
                f"src/DbSqlLikeMem.VsCodeExtension/package.json: missing script '{script_name}'"
            )

    expected_urls = {
        "repository.url": f"{repository_url}.git",
        "bugs.url": f"{repository_url}/issues",
        "homepage": f"{repository_url}#readme",
    }

    actual_repo_url = str(data.get("repository", {}).get("url", "")).strip()
    if actual_repo_url != expected_urls["repository.url"]:
        failures.append(
            "src/DbSqlLikeMem.VsCodeExtension/package.json: "
            f"repository.url mismatch (expected '{expected_urls['repository.url']}', found '{actual_repo_url}')"
        )

    actual_bugs_url = str(data.get("bugs", {}).get("url", "")).strip()
    if actual_bugs_url != expected_urls["bugs.url"]:
        failures.append(
            "src/DbSqlLikeMem.VsCodeExtension/package.json: "
            f"bugs.url mismatch (expected '{expected_urls['bugs.url']}', found '{actual_bugs_url}')"
        )

    actual_homepage = str(data.get("homepage", "")).strip()
    if actual_homepage != expected_urls["homepage"]:
        failures.append(
            "src/DbSqlLikeMem.VsCodeExtension/package.json: "
            f"homepage mismatch (expected '{expected_urls['homepage']}', found '{actual_homepage}')"
        )

    publisher = str(data.get("publisher", "")).strip()
    if not publisher:
        failures.append("src/DbSqlLikeMem.VsCodeExtension/package.json: missing 'publisher'")

    icon_path = extension_root / str(data.get("icon", "")).strip()
    if not icon_path.exists():
        failures.append(
            f"src/DbSqlLikeMem.VsCodeExtension/package.json: referenced icon not found at '{icon_path.relative_to(root)}'"
        )

    required_files = [
        extension_root / "package.nls.json",
        extension_root / "package.nls.pt-br.json",
        extension_root / "l10n",
        extension_root / "resources" / "database.svg",
        extension_root / "resources" / "icon.png.base64",
        extension_root / "scripts" / "generate-icon.js",
    ]
    for path in required_files:
        if not path.exists():
            failures.append(f"required VS Code extension file not found: {path.relative_to(root)}")

    package_nls = json.loads(load_text(extension_root / "package.nls.json"))
    package_nls_pt_br = json.loads(load_text(extension_root / "package.nls.pt-br.json"))
    referenced_tokens = collect_package_nls_tokens(data)
    for token in sorted(referenced_tokens):
        if token not in package_nls:
            failures.append(
                "src/DbSqlLikeMem.VsCodeExtension/package.json: "
                f"missing '{token}' in package.nls.json"
            )
        if token not in package_nls_pt_br:
            warnings.append(
                "src/DbSqlLikeMem.VsCodeExtension/package.json: "
                f"missing '{token}' in package.nls.pt-br.json"
            )

    contributes = data.get("contributes", {})
    commands = contributes.get("commands", [])
    command_ids = {
        str(command.get("command", "")).strip()
        for command in commands
        if str(command.get("command", "")).strip()
    }

    activation_events = [str(item).strip() for item in data.get("activationEvents", []) if str(item).strip()]
    for activation_event in activation_events:
        if activation_event.startswith("onCommand:"):
            command_id = activation_event.split(":", 1)[1]
            if command_id not in command_ids:
                failures.append(
                    "src/DbSqlLikeMem.VsCodeExtension/package.json: "
                    f"activation event references unknown command '{command_id}'"
                )

    views = contributes.get("views", {})
    view_ids = {
        str(view.get("id", "")).strip()
        for view_group in views.values()
        for view in view_group
        if str(view.get("id", "")).strip()
    }
    for activation_event in activation_events:
        if activation_event.startswith("onView:"):
            view_id = activation_event.split(":", 1)[1]
            if view_id not in view_ids:
                failures.append(
                    "src/DbSqlLikeMem.VsCodeExtension/package.json: "
                    f"activation event references unknown view '{view_id}'"
                )

    return failures, warnings


def check_visual_studio_extension(
    root: Path,
    repository_url: str,
    strict_marketplace_placeholders: bool,
) -> tuple[list[str], list[str]]:
    vsix_manifest_path = root / "src" / "DbSqlLikeMem.VisualStudioExtension" / "source.extension.vsixmanifest"
    vsix_project_path = root / "src" / "DbSqlLikeMem.VisualStudioExtension" / "DbSqlLikeMem.VisualStudioExtension.csproj"
    publish_manifest_path = root / "eng" / "visualstudio" / "PublishManifest.json"

    failures: list[str] = []
    warnings: list[str] = []

    vsix_root = ET.fromstring(load_text(vsix_manifest_path))
    namespace = {"vsix": "http://schemas.microsoft.com/developer/vsx-schema/2011"}
    identifier = vsix_root.find("vsix:Identifier", namespace)
    if identifier is None:
        failures.append("src/DbSqlLikeMem.VisualStudioExtension/source.extension.vsixmanifest: missing Identifier node")
    else:
        version_node = identifier.find("vsix:Version", namespace)
        version_text = (version_node.text or "").strip() if version_node is not None else None
        failures.extend(
            validate_semver(
                "src/DbSqlLikeMem.VisualStudioExtension/source.extension.vsixmanifest",
                version_text,
            )
            )

    minimum_visual_studio_version = parse_minimum_visual_studio_version(vsix_project_path)
    if not minimum_visual_studio_version:
        failures.append(
            "src/DbSqlLikeMem.VisualStudioExtension/DbSqlLikeMem.VisualStudioExtension.csproj: missing MinimumVisualStudioVersion"
        )

    supported_products = vsix_root.findall(".//vsix:SupportedProducts/vsix:VisualStudio", namespace)
    if not supported_products:
        failures.append(
            "src/DbSqlLikeMem.VisualStudioExtension/source.extension.vsixmanifest: missing SupportedProducts/VisualStudio entries"
        )
    elif minimum_visual_studio_version:
        expected_prefix = f"[{minimum_visual_studio_version},"
        for product in supported_products:
            version_range = str(product.attrib.get("Version", "")).strip()
            if not version_range.startswith(expected_prefix):
                failures.append(
                    "src/DbSqlLikeMem.VisualStudioExtension/source.extension.vsixmanifest: "
                    f"supported Visual Studio range '{version_range}' does not align with MinimumVisualStudioVersion '{minimum_visual_studio_version}'"
                )

    publish_manifest = json.loads(load_text(publish_manifest_path))
    repo_value = str(publish_manifest.get("repo", "")).strip()
    if repo_value != repository_url:
        failures.append(
            "eng/visualstudio/PublishManifest.json: "
            f"repo mismatch (expected '{repository_url}', found '{repo_value}')"
        )

    internal_name = str(publish_manifest.get("identity", {}).get("internalName", "")).strip()
    if not internal_name:
        failures.append("eng/visualstudio/PublishManifest.json: missing identity.internalName")

    overview = str(publish_manifest.get("overview", "")).strip()
    if not overview:
        failures.append("eng/visualstudio/PublishManifest.json: missing overview")
    else:
        overview_candidates = [
            publish_manifest_path.parent / overview,
            root / "src" / "DbSqlLikeMem.VisualStudioExtension" / overview,
        ]
        if not any(candidate.exists() for candidate in overview_candidates):
            failures.append(
                "eng/visualstudio/PublishManifest.json: "
                f"overview file '{overview}' was not found next to the manifest or in the VSIX project directory"
            )

    tags = publish_manifest.get("tags", [])
    if not isinstance(tags, list) or not tags:
        failures.append("eng/visualstudio/PublishManifest.json: missing tags")

    categories = publish_manifest.get("categories", [])
    if not isinstance(categories, list) or not categories:
        failures.append("eng/visualstudio/PublishManifest.json: missing categories")

    publisher = str(publish_manifest.get("publisher", "")).strip()
    if not publisher:
        failures.append("eng/visualstudio/PublishManifest.json: missing publisher")
    elif publisher == "SEU_PUBLISHER":
        message = "eng/visualstudio/PublishManifest.json: publisher still uses placeholder 'SEU_PUBLISHER'"
        if strict_marketplace_placeholders:
            failures.append(message)
        else:
            warnings.append(message)

    return failures, warnings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strict-marketplace-placeholders",
        action="store_true",
        help="Treat unresolved marketplace placeholders as errors instead of warnings.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]

    failures: list[str] = []
    warnings: list[str] = []

    failures.extend(check_required_files(root))

    props_path = root / "src" / "Directory.Build.props"
    version, repository_url = parse_directory_build_props(props_path)
    failures.extend(validate_semver("src/Directory.Build.props", version))
    if not repository_url:
        failures.append("src/Directory.Build.props: missing RepositoryUrl")

    failures.extend(check_snapshots(root))
    failures.extend(check_docs(root))
    failures.extend(check_template_baselines(root))
    failures.extend(check_template_review_metadata(root))
    failures.extend(check_workflows(root))
    failures.extend(check_release_communication(root))

    changelog_content = load_text(root / "CHANGELOG.md")
    semver_impact = analyze_release_semver_impact(changelog_content)

    if repository_url:
        vscode_failures, vscode_warnings = check_vscode_extension(root, repository_url)
        failures.extend(vscode_failures)
        warnings.extend(vscode_warnings)

        vsix_failures, vsix_warnings = check_visual_studio_extension(
            root,
            repository_url,
            args.strict_marketplace_placeholders,
        )
        failures.extend(vsix_failures)
        warnings.extend(vsix_warnings)

    if failures:
        print("[FAIL] Release readiness validation found issues:")
        for failure in failures:
            print(f"  - {failure}")
    else:
        print(f"[PASS] Release readiness baseline is coherent for version {version}.")

    if semver_impact is not None:
        print(
            "[INFO] SemVer impact suggestion from CHANGELOG.md: "
            f"{semver_impact.suggested_bump} "
            f"(breaking={semver_impact.breaking_count}, "
            f"feature={semver_impact.feature_count}, fix={semver_impact.fix_count})"
        )

    if warnings:
        print("[WARN] Remaining release-readiness warnings:")
        for warning in warnings:
            print(f"  - {warning}")

    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
