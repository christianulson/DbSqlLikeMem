param(
    [string]$ComposeFile = ".\docker-compose.benchmarks.yml",
    [string]$MariaDbContainerName = "dbsqllikemem-bench-mariadb",
    [string]$Db2ContainerName = "dbsqllikemem-bench-db2",
    [int]$MariaDbReadyTimeoutSeconds = 300,
    [int]$Db2ReadyTimeoutSeconds = 300
)

$ErrorActionPreference = "Stop"

Write-Host "Starting benchmark databases..."

docker compose -f $ComposeFile up -d

function Wait-ForMariaDbReady {
    param(
        [string]$ContainerName,
        [int]$TimeoutSeconds
    )

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)

    Write-Host "Waiting MariaDB to start..."

    while ([DateTime]::UtcNow -lt $deadline) {
        docker exec $ContainerName sh -c "mariadb-admin ping -h 127.0.0.1 -uroot -proot --silent" | Out-Null

        if ($LASTEXITCODE -eq 0) {
            Write-Host "MariaDB is ready."
            return
        }

        Start-Sleep -Seconds 3
    }

    throw "MariaDB did not become ready in time."
}

function Wait-ForDb2Ready {
    param(
        [string]$ContainerName,
        [int]$TimeoutSeconds
    )

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)

    Write-Host "Waiting DB2 to start..."
    Start-Sleep -Seconds 20

    while ([DateTime]::UtcNow -lt $deadline) {
        docker exec $ContainerName bash -lc "su - db2inst1 -c 'db2 list db directory'" | Out-Null

        if ($LASTEXITCODE -eq 0) {
            $dbExists = docker exec $ContainerName bash -lc "su - db2inst1 -c 'db2 list db directory'" | Select-String "BENCH"
            if (-not $dbExists) {
                Write-Host "Creating BENCH database..."
                docker exec $ContainerName bash -lc "su - db2inst1 -c 'db2 create database BENCH'"
            }
            else {
                Write-Host "BENCH database already exists."
            }

            return
        }

        Start-Sleep -Seconds 5
    }

    throw "DB2 did not become ready in time."
}

function Ensure-Db2UserTemporaryTablespace {
    param(
        [string]$DatabaseName = "BENCH",
        [string]$TablespaceName = "USRTMPSPC32K"
    )

    Write-Host "Checking DB2 user temporary tablespace '$TablespaceName'..."

    $tablespaceOutput = docker exec $Db2ContainerName bash -lc "su - db2inst1 -c 'db2 connect to $DatabaseName >/dev/null 2>&1 && db2 list tablespaces show detail'" 2>$null | Out-String
    if ($tablespaceOutput -match [regex]::Escape("Name = $TablespaceName")) {
        Write-Host "DB2 user temporary tablespace '$TablespaceName' already exists."
        return
    }

    Write-Host "Creating DB2 user temporary tablespace '$TablespaceName'..."
    docker exec $Db2ContainerName bash -lc "su - db2inst1 -c 'db2 connect to $DatabaseName >/dev/null 2>&1 && db2 create user temporary tablespace $TablespaceName pagesize 32 k managed by automatic storage'"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create DB2 user temporary tablespace '$TablespaceName'."
    }

    docker exec $Db2ContainerName bash -lc "su - db2inst1 -c 'db2 connect to $DatabaseName >/dev/null 2>&1 && db2 grant use of tablespace $TablespaceName to user db2inst1'"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to grant use of DB2 user temporary tablespace '$TablespaceName'."
    }

    Write-Host "DB2 user temporary tablespace '$TablespaceName' created."
}

Wait-ForMariaDbReady -ContainerName $MariaDbContainerName -TimeoutSeconds $MariaDbReadyTimeoutSeconds
Wait-ForDb2Ready -ContainerName $Db2ContainerName -TimeoutSeconds $Db2ReadyTimeoutSeconds
Ensure-Db2UserTemporaryTablespace -DatabaseName "BENCH"

Write-Host "Databases ready."
Write-Host "Suggested environment variables:"
Write-Host '$env:DBSQLLIKEMEM_BENCH_MYSQL_CONNECTION_STRING="Server=127.0.0.1;Port=13306;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"'
Write-Host '$env:DBSQLLIKEMEM_BENCH_MARIADB_CONNECTION_STRING="Server=127.0.0.1;Port=13307;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"'
Write-Host '$env:MARIADB_CONNECTION_STRING="Server=127.0.0.1;Port=13307;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"'
Write-Host '$env:MYSQL_CONNECTION_STRING="Server=127.0.0.1;Port=13306;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"'
Write-Host '$env:NPGSQL_CONNECTION_STRING="Host=127.0.0.1;Port=15432;Database=benchmark;Username=postgres;Password=postgres;Pooling=false;"'
Write-Host '$env:SQLSERVER_CONNECTION_STRING="Server=127.0.0.1,11433;Database=master;User Id=sa;Password=Your_password123;TrustServerCertificate=True;Encrypt=False;Pooling=false;"'
Write-Host '$env:SQLAZURE_CONNECTION_STRING="Server=127.0.0.1,11433;Database=master;User Id=sa;Password=Your_password123;TrustServerCertificate=True;Encrypt=False;Pooling=false;"'
Write-Host '$env:ORACLE_CONNECTION_STRING="User Id=benchmark;Password=benchmark;Data Source=127.0.0.1:15211/FREEPDB1;Pooling=false;"'
Write-Host '$env:DB2_CONNECTION_STRING="Server=127.0.0.1:15000;Database=BENCH;UID=db2inst1;PWD=db2inst1;Pooling=false;"'
Write-Host '$env:DBSQLLIKEMEM_BENCH_DB2_CONNECTION_STRING="Server=127.0.0.1:15000;Database=BENCH;UID=db2inst1;PWD=db2inst1;Pooling=false;"'
