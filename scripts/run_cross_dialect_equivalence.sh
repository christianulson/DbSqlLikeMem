#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: bash scripts/run_cross_dialect_equivalence.sh [--profile smoke|aggregation] [--snapshot-file <path>] [--continue-on-error] [--dry-run]

Profiles:
  smoke        Runs provider core test projects with foundational cross-dialect filters.
  aggregation  Runs provider Dapper test projects with aggregation-focused filters.

Options:
  --continue-on-error  Runs full matrix and reports summary before exiting with failure when any check fails.
  --dry-run            Prints planned project/filter matrix and exits without running dotnet.
USAGE
}

smoke_projects=(
  "src/DbSqlLikeMem.MySql.Test/DbSqlLikeMem.MySql.Test.csproj"
  "src/DbSqlLikeMem.SqlServer.Test/DbSqlLikeMem.SqlServer.Test.csproj"
  "src/DbSqlLikeMem.SqlAzure.Test/DbSqlLikeMem.SqlAzure.Test.csproj"
  "src/DbSqlLikeMem.Oracle.Test/DbSqlLikeMem.Oracle.Test.csproj"
  "src/DbSqlLikeMem.Npgsql.Test/DbSqlLikeMem.Npgsql.Test.csproj"
  "src/DbSqlLikeMem.Sqlite.Test/DbSqlLikeMem.Sqlite.Test.csproj"
  "src/DbSqlLikeMem.Db2.Test/DbSqlLikeMem.Db2.Test.csproj"
)

aggregation_projects=(
  "src/DbSqlLikeMem.MySql.Dapper.Test/DbSqlLikeMem.MySql.Dapper.Test.csproj"
  "src/DbSqlLikeMem.SqlServer.Dapper.Test/DbSqlLikeMem.SqlServer.Dapper.Test.csproj"
  "src/DbSqlLikeMem.Oracle.Dapper.Test/DbSqlLikeMem.Oracle.Dapper.Test.csproj"
  "src/DbSqlLikeMem.Npgsql.Dapper.Test/DbSqlLikeMem.Npgsql.Dapper.Test.csproj"
  "src/DbSqlLikeMem.Sqlite.Dapper.Test/DbSqlLikeMem.Sqlite.Dapper.Test.csproj"
  "src/DbSqlLikeMem.Db2.Dapper.Test/DbSqlLikeMem.Db2.Dapper.Test.csproj"
)

smoke_filters=(
  "ExistsTests"
  "SubqueryFromAndJoinsTests"
  "SelectIntoInsertSelectUpdateDeleteFromSelectTests"
)

aggregation_filters=(
  "AggregationTests"
  "StringAggregation_"
  "WithinGroup_ShouldThrowNotSupported"
)

profile="smoke"
snapshot_file=""
continue_on_error="false"
dry_run="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot-file)
      shift
      snapshot_file="${1:-}"
      ;;
    --profile)
      shift
      profile="${1:-smoke}"
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

case "$profile" in
  smoke)
    projects=("${smoke_projects[@]}")
    filters=("${smoke_filters[@]}")
    ;;
  aggregation)
    projects=("${aggregation_projects[@]}")
    filters=("${aggregation_filters[@]}")
    ;;
  *)
    echo "Invalid --profile value: ${profile}. Use: smoke | aggregation" >&2
    usage >&2
    exit 2
    ;;
esac

if [[ -n "$snapshot_file" ]]; then
  mkdir -p "$(dirname "$snapshot_file")"
  {
    echo "# Cross-dialect equivalence snapshot"
    echo
    echo "Generated at: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "Profile: ${profile}"
    echo
    echo "| Provider project | Test filter | Status |"
    echo "| --- | --- | --- |"
  } > "$snapshot_file"
fi

echo "Running cross-dialect checks (profile=${profile}, continue_on_error=${continue_on_error}, dry_run=${dry_run}) over common SQL filters..."

if [[ "$dry_run" == "true" ]]; then
  echo "Planned matrix:"
  planned_total=0
  for project in "${projects[@]}"; do
    for filter_name in "${filters[@]}"; do
      echo "  - ${project} :: ${filter_name}"
      planned_total=$((planned_total + 1))
      if [[ -n "$snapshot_file" ]]; then
        echo "| ${project} | ${filter_name} | SKIPPED (dry-run) |" >> "$snapshot_file"
      fi
    done
  done

  if [[ -n "$snapshot_file" ]]; then
    echo >> "$snapshot_file"
    echo "| Summary | Checks | Failed |" >> "$snapshot_file"
    echo "| --- | --- | --- |" >> "$snapshot_file"
    echo "| ${profile} | ${planned_total} | 0 |" >> "$snapshot_file"
    echo "Snapshot written to: $snapshot_file"
  fi

  exit 0
fi

checks_total=0
checks_failed=0

for project in "${projects[@]}"; do
  echo "==> Restoring ${project}"
  restore_status="PASS"
  if ! dotnet restore "${project}" -p:TargetFramework=net8.0 >/dev/null; then
    restore_status="FAIL"
  fi

  for filter_name in "${filters[@]}"; do
    echo "==> ${project} :: ${filter_name}"

    status="PASS"
    if [[ "$restore_status" == "PASS" ]]; then
      if ! dotnet test "${project}" \
        --framework net8.0 \
        --configuration Release \
        --no-restore \
        --verbosity minimal \
        --filter "FullyQualifiedName~.${filter_name}"; then
        status="FAIL"
      fi
    else
      status="FAIL"
    fi

    checks_total=$((checks_total + 1))

    if [[ -n "$snapshot_file" ]]; then
      echo "| ${project} | ${filter_name} | ${status} |" >> "$snapshot_file"
    fi

    if [[ "$status" != "PASS" ]]; then
      checks_failed=$((checks_failed + 1))
      echo "Cross-dialect check failed at ${project} :: ${filter_name} (profile=${profile})" >&2
      if [[ "$continue_on_error" != "true" ]]; then
        exit 1
      fi
    fi
  done
done

if [[ "$checks_failed" -gt 0 ]]; then
  echo "Cross-dialect checks finished with failures: ${checks_failed}/${checks_total} (profile=${profile})." >&2
  if [[ -n "$snapshot_file" ]]; then
    echo
    echo "| Summary | Checks | Failed |" >> "$snapshot_file"
    echo "| --- | --- | --- |" >> "$snapshot_file"
    echo "| ${profile} | ${checks_total} | ${checks_failed} |" >> "$snapshot_file"
    echo "Snapshot written to: $snapshot_file"
  fi
  exit 1
fi

echo "Cross-dialect checks finished successfully: ${checks_total}/${checks_total} passed (profile=${profile})."
if [[ -n "$snapshot_file" ]]; then
  echo
  echo "| Summary | Checks | Failed |" >> "$snapshot_file"
  echo "| --- | --- | --- |" >> "$snapshot_file"
  echo "| ${profile} | ${checks_total} | 0 |" >> "$snapshot_file"
  echo "Snapshot written to: $snapshot_file"
fi
