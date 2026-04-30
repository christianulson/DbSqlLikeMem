param(
    [string]$ProjectPath = ".\DbSqlLikeMem.Benchmarks.csproj",
    [string]$Filter = "*",
    [switch]$InProcess
)

$ErrorActionPreference = "Stop"

$env:MYSQL_CONNECTION_STRING="Server=127.0.0.1;Port=13306;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"
$env:MARIADB_CONNECTION_STRING="Server=127.0.0.1;Port=13307;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"
$env:POSTGRES_CONNECTION_STRING="Host=127.0.0.1;Port=15432;Database=benchmark;Username=postgres;Password=postgres;Pooling=false;"
$env:SQLSERVER_CONNECTION_STRING="Server=127.0.0.1,11433;Database=master;User Id=sa;Password=Your_password123;TrustServerCertificate=True;Encrypt=False;Pooling=false;"
$env:ORACLE_CONNECTION_STRING="User Id=benchmark;Password=benchmark;Data Source=127.0.0.1:15211/FREEPDB1;Pooling=false;"
$env:DB2_CONNECTION_STRING="Server=127.0.0.1:15000;Database=BENCH;UID=db2inst1;PWD=db2inst1;Pooling=false;"
$env:FIREBIRD_CONNECTION_STRING="User=benchmark;Password=benchmark;Database=127.0.0.1/13050:/var/lib/firebird/data/benchmark.fdb;Dialect=3;Charset=UTF8;Pooling=false;"


function Invoke-BenchmarkRun {
    param(
        [string]$ProjectPath,
        [string]$Filter,
        [switch]$InProcess
    )

    $benchmarkArgs = @("preprovisioned")
    if ($InProcess) {
        $benchmarkArgs += "inprocess"
    }

    $benchmarkArgs += "--filter"
    $benchmarkArgs += $Filter

    dotnet run -c Release --project $ProjectPath -- @benchmarkArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Benchmark run failed for filter '$Filter'."
    }
}

function Should-SplitBroadRun {
    param([string]$Filter, [switch]$InProcess)

    if (-not $InProcess) {
        return $false
    }

    if ([string]::IsNullOrWhiteSpace($Filter)) {
        return $true
    }

    return $Filter -eq "*"
}

if (Should-SplitBroadRun -Filter $Filter -InProcess:$InProcess) {
    Invoke-BenchmarkRun -ProjectPath $ProjectPath -Filter "*DbSqlLikeMem_Benchmarks*" -InProcess
    Invoke-BenchmarkRun -ProjectPath $ProjectPath -Filter "*Sqlite_Native_Benchmarks*" -InProcess
    Invoke-BenchmarkRun -ProjectPath $ProjectPath -Filter "*Testcontainers_Benchmarks*"
    return
}

$useInProcess = $InProcess -and ($Filter -like "*DbSqlLikeMem*" -or $Filter -like "*Sqlite*") -and $Filter -notlike "*Testcontainers*"
if ($InProcess -and -not $useInProcess) {
    Write-Warning "InProcess was requested but skipped for this filter. Use it only for short DbSqlLikeMem or Sqlite benchmark filters."
}

Invoke-BenchmarkRun -ProjectPath $ProjectPath -Filter $Filter -InProcess:([bool]$useInProcess)
