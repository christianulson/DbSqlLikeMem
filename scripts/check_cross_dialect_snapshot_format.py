#!/usr/bin/env python3
"""Validates cross-dialect snapshot markdown structure for smoke/aggregation/parser/strategy profiles."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROFILE_RE = re.compile(r"^Profile:\s*(?P<profile>[a-zA-Z0-9_-]+)\s*$", re.MULTILINE)
TABLE_HEADER = "| Provider project | Test filter | Status |"
SUMMARY_HEADER = "| Summary | Checks | Failed |"
PLACEHOLDER_TOKEN = "manual-placeholder"


def validate_snapshot(path: Path, expected_profile: str) -> list[str]:
    issues: list[str] = []
    if not path.exists():
        return [f"file not found: {path}"]

    content = path.read_text(encoding="utf-8")

    match = PROFILE_RE.search(content)
    if not match:
        issues.append("missing 'Profile:' line")
    else:
        profile = match.group("profile")
        if profile != expected_profile:
            issues.append(f"profile mismatch: expected '{expected_profile}', found '{profile}'")

    if TABLE_HEADER not in content:
        issues.append("missing primary result table header")

    # Accept either: placeholder baseline OR summary section from executed snapshot.
    has_placeholder = PLACEHOLDER_TOKEN in content
    has_summary = SUMMARY_HEADER in content

    if not has_placeholder and not has_summary:
        issues.append("missing both placeholder token and summary section")

    return issues


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--smoke", default="docs/cross-dialect-smoke-snapshot.md")
    parser.add_argument("--aggregation", default="docs/cross-dialect-aggregation-snapshot.md")
    parser.add_argument("--parser", default="docs/cross-dialect-parser-snapshot.md")
    parser.add_argument("--strategy", default="docs/cross-dialect-strategy-snapshot.md")
    args = parser.parse_args()

    checks = [
        (Path(args.smoke), "smoke"),
        (Path(args.aggregation), "aggregation"),
        (Path(args.parser), "parser"),
        (Path(args.strategy), "strategy"),
    ]

    failures = 0
    for path, profile in checks:
        issues = validate_snapshot(path, profile)
        if issues:
            failures += 1
            print(f"[FAIL] {path}")
            for issue in issues:
                print(f"  - {issue}")
        else:
            print(f"[PASS] {path}")

    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
