#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROJECT_PATTERN = re.compile(r'Project Path="([^"]+\.csproj)"')
ALLOWED_ROOT_LEVEL_PROJECT_STEMS = {
    'DbSqlLikeMem',
    'DbSqlLikeMem.CsvReader',
    'DbSqlLikeMem.Dapper.TestTools',
    'DbSqlLikeMem.EfCore',
    'DbSqlLikeMem.EfCore.TestTools',
    'DbSqlLikeMem.LinqToDb',
    'DbSqlLikeMem.LinqToDb.TestTools',
    'DbSqlLikeMem.MiniProfiler.TestTools',
    'DbSqlLikeMem.NHibernate.TestTools',
    'DbSqlLikeMem.Test',
    'DbSqlLikeMem.TestTools',
    'DbSqlLikeMem.XUnit',
}


def normalize_path_text(path_text: str) -> str:
    return path_text.replace('\\', '/')


def load_slnx_projects(slnx_path: Path) -> set[str]:
    content = slnx_path.read_text(encoding='utf-8')
    return {
        normalize_path_text(project_path)
        for project_path in PROJECT_PATTERN.findall(content)
    }


def collect_csproj(search_dir: Path) -> set[str]:
    return {
        normalize_path_text(str(path.resolve()))
        for path in search_dir.rglob('*.csproj')
    }


def collect_root_level_projects(search_dir: Path) -> set[str]:
    search_root = search_dir.resolve()
    return {
        normalize_path_text(path.name)
        for path in search_dir.rglob('*.csproj')
        if path.parent.resolve() == search_root
    }


def resolve_included_project_path(project_path: str, slnx_base_dir: Path, search_dir: Path) -> str:
    candidate_paths = [slnx_base_dir / project_path, search_dir / project_path]
    for candidate_path in candidate_paths:
        if candidate_path.exists():
            return normalize_path_text(str(candidate_path.resolve()))
    return normalize_path_text(str(candidate_paths[0].resolve()))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--src-dir', default='src', help='Directory containing project tree (default: src).')
    parser.add_argument('--slnx', default='src/DbSqlLikeMem.slnx', help='Path to solution .slnx file.')
    args = parser.parse_args()

    src_dir = Path(args.src_dir)
    slnx_path = Path(args.slnx)

    if not src_dir.exists() or not src_dir.is_dir():
        print(f"ERROR: src-dir not found or not a directory: {src_dir}", file=sys.stderr)
        return 2

    if not slnx_path.exists():
        print(f"ERROR: slnx file not found: {slnx_path}", file=sys.stderr)
        return 2

    included_relative = load_slnx_projects(slnx_path)
    discovered_absolute = collect_csproj(src_dir)
    included_absolute = {
        resolve_included_project_path(project_path, slnx_path.parent, src_dir)
        for project_path in included_relative
    }
    root_level_projects = collect_root_level_projects(src_dir)

    missing_absolute = sorted(discovered_absolute - included_absolute)
    extra_relative = sorted(
        project_path
        for project_path in included_relative
        if resolve_included_project_path(project_path, slnx_path.parent, src_dir) not in discovered_absolute
    )
    organization_drift = sorted(
        project_name
        for project_name in root_level_projects
        if Path(project_name).stem not in ALLOWED_ROOT_LEVEL_PROJECT_STEMS
    )

    print(
        f"csproj_total={len(discovered_absolute)} included_total={len(included_relative)} "
        f"missing={len(missing_absolute)} extra={len(extra_relative)} "
        f"root_level_unbalanced={len(organization_drift)}"
    )

    if missing_absolute:
        print('\nMissing from .slnx:')
        for item in missing_absolute:
            print(f"  - {item}")

    if extra_relative:
        print('\nReferenced in .slnx but not found in scanned tree:')
        for item in extra_relative:
            print(f"  - {item}")

    if organization_drift:
        print('\nRoot-level projects outside the shared organization buckets:')
        for item in organization_drift:
            print(f"  - {item}")

    return 0 if not missing_absolute and not extra_relative and not organization_drift else 1


if __name__ == '__main__':
    raise SystemExit(main())
