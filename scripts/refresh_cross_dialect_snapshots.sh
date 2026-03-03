#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: bash scripts/refresh_cross_dialect_snapshots.sh [--smoke-output <path>] [--aggregation-output <path>] [--continue-on-error] [--dry-run]

Defaults:
  --smoke-output       docs/cross-dialect-smoke-snapshot.md
  --aggregation-output docs/cross-dialect-aggregation-snapshot.md

Options:
  --continue-on-error  Propagates full-matrix execution mode to each profile runner.
  --dry-run            Prints planned commands without running profile checks.
USAGE
}

smoke_output="docs/cross-dialect-smoke-snapshot.md"
aggregation_output="docs/cross-dialect-aggregation-snapshot.md"
continue_on_error="false"
dry_run="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --smoke-output)
      shift
      smoke_output="${1:-}"
      ;;
    --aggregation-output)
      shift
      aggregation_output="${1:-}"
      ;;
    --continue-on-error)
      continue_on_error="true"
      ;;
    --dry-run)
      dry_run="true"
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift || true
done

mkdir -p "$(dirname "$smoke_output")" "$(dirname "$aggregation_output")"

runner_extra_args=()
if [[ "$dry_run" == "true" ]]; then
  runner_extra_args+=("--dry-run")
fi
if [[ "$continue_on_error" == "true" ]]; then
  runner_extra_args+=("--continue-on-error")
fi

echo "Refreshing smoke snapshot -> ${smoke_output}"
bash scripts/run_cross_dialect_equivalence.sh --profile smoke --snapshot-file "$smoke_output" "${runner_extra_args[@]}"

echo "Refreshing aggregation snapshot -> ${aggregation_output}"
bash scripts/run_cross_dialect_equivalence.sh --profile aggregation --snapshot-file "$aggregation_output" "${runner_extra_args[@]}"

echo "Snapshots refreshed successfully."
