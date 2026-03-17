param(
    [string]$ProjectPath = ".\DbSqlLikeMem.Benchmarks.csproj",
    [string]$Filter = "*",
    [switch]$InProcess
)

$ErrorActionPreference = "Stop"

$env:DBSQLLIKEMEM_BENCH_MYSQL_CONNECTION_STRING="Server=127.0.0.1;Port=13306;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"
$env:DBSQLLIKEMEM_BENCH_NPGSQL_CONNECTION_STRING="Host=127.0.0.1;Port=15432;Database=benchmark;Username=postgres;Password=postgres;Pooling=false;"
$env:DBSQLLIKEMEM_BENCH_POSTGRES_CONNECTION_STRING=$env:DBSQLLIKEMEM_BENCH_NPGSQL_CONNECTION_STRING
$env:DBSQLLIKEMEM_BENCH_SQLSERVER_CONNECTION_STRING="Server=127.0.0.1,11433;Database=master;User Id=sa;Password=Your_password123;TrustServerCertificate=True;Encrypt=False;Pooling=false;"
$env:DBSQLLIKEMEM_BENCH_ORACLE_CONNECTION_STRING="User Id=benchmark;Password=benchmark;Data Source=127.0.0.1:15211/FREEPDB1;Pooling=false;"
$env:DBSQLLIKEMEM_BENCH_DB2_CONNECTION_STRING="Server=127.0.0.1:15000;Database=BENCH;UID=db2inst1;PWD=db2inst1;Pooling=false;"

# Backward-compatible aliases
$env:MYSQL_CONNECTION_STRING=$env:DBSQLLIKEMEM_BENCH_MYSQL_CONNECTION_STRING
$env:NPGSQL_CONNECTION_STRING=$env:DBSQLLIKEMEM_BENCH_NPGSQL_CONNECTION_STRING
$env:SQLSERVER_CONNECTION_STRING=$env:DBSQLLIKEMEM_BENCH_SQLSERVER_CONNECTION_STRING
$env:ORACLE_CONNECTION_STRING=$env:DBSQLLIKEMEM_BENCH_ORACLE_CONNECTION_STRING
$env:DB2_CONNECTION_STRING=$env:DBSQLLIKEMEM_BENCH_DB2_CONNECTION_STRING
$env:SQLAZURE_CONNECTION_STRING=$env:DBSQLLIKEMEM_BENCH_SQLSERVER_CONNECTION_STRING

$benchmarkArgs = @("preprovisioned")
if ($InProcess) {
    $benchmarkArgs += "inprocess"
}

$benchmarkArgs += "--filter"
$benchmarkArgs += $Filter

dotnet run -c Release --project $ProjectPath -- @benchmarkArgs
