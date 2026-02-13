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

echo "Running cross-dialect smoke checks over common SQL test classes..."

for project in "${projects[@]}"; do
  echo "==> Restoring ${project}"
  dotnet restore "${project}" >/dev/null

  for class_name in "${cross_dialect_classes[@]}"; do
    echo "==> ${project} :: ${class_name}"
    dotnet test "${project}" \
      --configuration Release \
      --no-restore \
      --verbosity minimal \
      --filter "FullyQualifiedName~.${class_name}"
  done
done

echo "Cross-dialect smoke checks finished successfully."
