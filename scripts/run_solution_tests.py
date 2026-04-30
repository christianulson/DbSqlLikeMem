#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

PROJECT_PATTERN = re.compile(r'Project Path="([^"]+\.csproj)"')
TARGET_FRAMEWORK_PATTERN = re.compile(r"<TargetFrameworks?>(.*?)</TargetFrameworks?>", re.IGNORECASE | re.DOTALL)
IS_TEST_PROJECT_PATTERN = re.compile(r"<IsTestProject>\s*true\s*</IsTestProject>", re.IGNORECASE | re.DOTALL)


def normalize_path(path: Path) -> str:
    return path.resolve().as_posix().lower()


def load_solution_projects(solution_path: Path) -> list[Path]:
    content = solution_path.read_text(encoding="utf-8")
    projects: list[Path] = []
    for project_path in PROJECT_PATTERN.findall(content):
        projects.append((solution_path.parent / project_path).resolve())
    return projects


def get_project_targets(project_path: Path) -> set[str]:
    content = project_path.read_text(encoding="utf-8")
    targets: set[str] = set()
    for match in TARGET_FRAMEWORK_PATTERN.findall(content):
        for target in match.split(";"):
            target = target.strip().lower()
            if target:
                targets.add(target)
    return targets


def is_test_project(project_path: Path) -> bool:
    content = project_path.read_text(encoding="utf-8")
    return IS_TEST_PROJECT_PATTERN.search(content) is not None


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the test projects included in a .slnx solution for a single target framework.")
    parser.add_argument("--solution", default="src/DbSqlLikeMem.slnx", help="Path to the .slnx file.")
    parser.add_argument("--framework", required=True, help="Target framework to test, such as net8.0 or net472.")
    parser.add_argument("--configuration", default="Release", help="Build configuration passed to dotnet test.")
    parser.add_argument("--no-build", action="store_true", help="Pass --no-build to dotnet test.")
    parser.add_argument("--exclude", action="append", default=[], help="Project path to exclude. Can be repeated.")
    args = parser.parse_args()

    solution_path = Path(args.solution)
    if not solution_path.exists():
        print(f"ERROR: solution not found: {solution_path}", file=sys.stderr)
        return 2

    framework = args.framework.lower()
    excluded = {normalize_path((solution_path.parent / Path(item)).resolve()) for item in args.exclude}

    selected_projects: list[Path] = []
    for project_path in load_solution_projects(solution_path):
        if normalize_path(project_path) in excluded:
            continue

        if not project_path.exists():
            continue

        if not is_test_project(project_path):
            continue

        targets = get_project_targets(project_path)
        if framework not in targets:
            continue

        selected_projects.append(project_path)

    if not selected_projects:
        print(f"ERROR: no test projects found for framework {args.framework}", file=sys.stderr)
        return 1

    for project_path in selected_projects:
        print(f"==> Running {project_path}")
        command = [
            "dotnet",
            "test",
            str(project_path),
            "--framework",
            args.framework,
            "--configuration",
            args.configuration,
            "--verbosity",
            "normal",
        ]
        if args.no_build:
            command.append("--no-build")
            command.append("--no-restore")

        result = subprocess.run(command, check=False)
        if result.returncode != 0:
            return result.returncode

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
