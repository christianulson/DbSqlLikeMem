#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROJECT_PATTERN = re.compile(r'Project Path="([^"]+\.csproj)"')


def normalize(path: Path) -> str:
    return str(path.resolve()).replace('\\', '/')


def load_slnx_projects(slnx_path: Path) -> set[str]:
    content = slnx_path.read_text(encoding='utf-8')
    base_dir = slnx_path.parent
    return {
        normalize(base_dir / project_path)
        for project_path in PROJECT_PATTERN.findall(content)
    }


def collect_csproj(search_dir: Path) -> set[str]:
    return {normalize(p) for p in search_dir.rglob('*.csproj')}


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

    discovered = collect_csproj(src_dir)
    included = load_slnx_projects(slnx_path)

    missing = sorted(discovered - included)
    extra = sorted(included - discovered)

    print(f"csproj_total={len(discovered)} included_total={len(included)} missing={len(missing)} extra={len(extra)}")

    if missing:
        print('\nMissing from .slnx:')
        for item in missing:
            print(f"  - {item}")

    if extra:
        print('\nReferenced in .slnx but not found in scanned tree:')
        for item in extra:
            print(f"  - {item}")

    return 0 if not missing and not extra else 1


if __name__ == '__main__':
    raise SystemExit(main())