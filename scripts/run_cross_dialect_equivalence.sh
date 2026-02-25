#!/usr/bin/env bash
set -euo pipefail

projects=(
  "src/DbSqlLikeMem.MySql.Test/DbSqlLikeMem.MySql.Test.csproj"
  "src/DbSqlLikeMem.SqlServer.Test/DbSqlLikeMem.SqlServer.Test.csproj"
  "src/DbSqlLikeMem.Oracle.Test/DbSqlLikeMem.Oracle.Test.csproj"
  "src/DbSqlLikeMem.Npgsql.Test/DbSqlLikeMem.Npgsql.Test.csproj"
  "src/DbSqlLikeMem.Sqlite.Test/DbSqlLikeMem.Sqlite.Test.csproj"
  "src/DbSqlLikeMem.Db2.Test/DbSqlLikeMem.Db2.Test.csproj"
)

cross_dialect_classes=(
  "ExistsTests"
  "SubqueryFromAndJoinsTests"
  "SelectIntoInsertSelectUpdateDeleteFromSelectTests"
)

snapshot_file=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot-file)
      shift
      snapshot_file="${1:-}"
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift || true
done

if [[ -n "$snapshot_file" ]]; then
  mkdir -p "$(dirname "$snapshot_file")"
  {
    echo "# Cross-dialect smoke snapshot"
    echo
    echo "Generated at: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo
    echo "| Provider project | Test class | Status |"
    echo "| --- | --- | --- |"
  } > "$snapshot_file"
fi

echo "Running cross-dialect smoke checks over common SQL test classes..."

for project in "${projects[@]}"; do
  echo "==> Restoring ${project}"
  restore_status="PASS"
  if ! dotnet restore "${project}" -p:TargetFramework=net8.0 >/dev/null; then
    restore_status="FAIL"
  fi

  for class_name in "${cross_dialect_classes[@]}"; do
    echo "==> ${project} :: ${class_name}"

    status="PASS"
    if [[ "$restore_status" == "PASS" ]]; then
      if ! dotnet test "${project}" \
        --framework net8.0 \
        --configuration Release \
        --no-restore \
        --verbosity minimal \
        --filter "FullyQualifiedName~.${class_name}"; then
        status="FAIL"
      fi
    else
      status="FAIL"
    fi

    if [[ -n "$snapshot_file" ]]; then
      echo "| ${project} | ${class_name} | ${status} |" >> "$snapshot_file"
    fi

    if [[ "$status" != "PASS" ]]; then
      echo "Cross-dialect smoke checks failed at ${project} :: ${class_name}" >&2
      exit 1
    fi
  done
done

echo "Cross-dialect smoke checks finished successfully."
if [[ -n "$snapshot_file" ]]; then
  echo "Snapshot written to: $snapshot_file"
fi
