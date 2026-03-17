$targets = @(
    '*MySql_DbSqlLikeMem_Benchmarks*',
    '*MySql_Testcontainers_Benchmarks*',
    '*Npgsql_DbSqlLikeMem_Benchmarks*',
    '*Npgsql_Testcontainers_Benchmarks*',
    '*SqlServer_DbSqlLikeMem_Benchmarks*',
    '*SqlServer_Testcontainers_Benchmarks*',
    '*Sqlite_DbSqlLikeMem_Benchmarks*',
    '*Sqlite_Native_Benchmarks*'
)

foreach ($target in $targets) {
    dotnet run -c Release --filter $target
}
