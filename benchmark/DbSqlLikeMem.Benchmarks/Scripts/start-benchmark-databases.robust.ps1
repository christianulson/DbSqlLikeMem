param(
    [string]$ComposeFile = ".\docker-compose.benchmarks.yml",
    [string]$MariaDbContainerName = "dbsqllikemem-bench-mariadb",
    [string]$Db2ContainerName = "dbsqllikemem-bench-db2",
    [int]$GenericRetries = 60,
    [int]$MariaDbRetries = 90,
    [int]$Db2Retries = 90
)

$ErrorActionPreference = "Stop"

function Wait-ForMySqlReady {
    param(
        [string]$ContainerName = "dbsqllikemem-bench-mysql",
        [int]$Retries = 90,
        [int]$DelaySeconds = 3
    )

    Write-Host "Waiting MySQL to accept connections..."

    for ($i = 0; $i -lt $Retries; $i++) {
        docker exec $ContainerName sh -c "mysqladmin ping -h 127.0.0.1 -uroot -proot --silent" | Out-Null

        if ($LASTEXITCODE -eq 0) {
            Write-Host "MySQL is ready."
            return
        }

        Start-Sleep -Seconds $DelaySeconds
    }

    throw "MySQL did not become ready in time."
}

function Wait-ForMariaDbReady {
    param(
        [string]$ContainerName = "dbsqllikemem-bench-mariadb",
        [int]$Retries = 90,
        [int]$DelaySeconds = 3
    )

    Write-Host "Waiting MariaDB to accept connections..."

    for ($i = 0; $i -lt $Retries; $i++) {
        docker exec $ContainerName sh -c "mariadb-admin ping -h 127.0.0.1 -uroot -proot --silent" | Out-Null

        if ($LASTEXITCODE -eq 0) {
            Write-Host "MariaDB is ready."
            return
        }

        Start-Sleep -Seconds $DelaySeconds
    }

    throw "MariaDB did not become ready in time."
}

function Wait-ForContainerHealthy {
    param(
        [Parameter(Mandatory)][string]$ContainerName,
        [int]$Retries = 60,
        [int]$DelaySeconds = 2
    )

    Write-Host "Waiting for container '$ContainerName' to become healthy..."

    for ($i = 0; $i -lt $Retries; $i++) {
        $status = docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $ContainerName 2>$null

        if ($LASTEXITCODE -eq 0) {
            $status = ($status | Out-String).Trim()

            if ($status -eq "healthy" -or $status -eq "running") {
                Write-Host "Container '$ContainerName' is $status."
                return
            }
        }

        Start-Sleep -Seconds $DelaySeconds
    }

    throw "Container '$ContainerName' did not become healthy in time."
}

function Wait-ForDb2Instance {
    param(
        [int]$Retries = 90,
        [int]$DelaySeconds = 4
    )

    Write-Host "Waiting DB2 instance manager to be ready..."

    for ($i = 0; $i -lt $Retries; $i++) {
        docker exec $Db2ContainerName bash -lc "su - db2inst1 -c 'db2 get instance >/dev/null 2>&1 && db2 list active databases >/dev/null 2>&1'"
        if ($LASTEXITCODE -eq 0) {
            Write-Host "DB2 instance is ready."
            return
        }

        docker exec $Db2ContainerName bash -lc "su - db2inst1 -c 'db2start >/dev/null 2>&1'" | Out-Null
        Start-Sleep -Seconds $DelaySeconds
    }

    throw "DB2 instance did not become ready in time."
}

function Ensure-Db2Database {
    param(
        [string]$DatabaseName = "BENCH"
    )

    Write-Host "Checking DB2 database '$DatabaseName'..."

    $listOutput = docker exec $Db2ContainerName bash -lc "su - db2inst1 -c 'db2 list db directory'" 2>$null | Out-String

    if ($listOutput -match [regex]::Escape($DatabaseName)) {
        Write-Host "DB2 database '$DatabaseName' already exists."
        return
    }

    Write-Host "Creating DB2 database '$DatabaseName'..."
    docker exec $Db2ContainerName bash -lc "su - db2inst1 -c 'db2 create database $DatabaseName'"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create DB2 database '$DatabaseName'."
    }

    Write-Host "DB2 database '$DatabaseName' created."
}

function Ensure-Db2UserTemporaryTablespace {
    param(
        [string]$DatabaseName = "BENCH",
        [string]$TablespaceName = "USRTMPSPC1"
    )

    Write-Host "Checking DB2 user temporary tablespace '$TablespaceName'..."

    $tablespaceOutput = docker exec $Db2ContainerName bash -lc "su - db2inst1 -c 'db2 connect to $DatabaseName >/dev/null 2>&1 && db2 list tablespaces show detail'" 2>$null | Out-String
    if ($tablespaceOutput -match [regex]::Escape("Name = $TablespaceName")) {
        Write-Host "DB2 user temporary tablespace '$TablespaceName' already exists."
        return
    }

    Write-Host "Creating DB2 user temporary tablespace '$TablespaceName'..."
    $createOutput = docker exec $Db2ContainerName bash -lc "su - db2inst1 -c 'db2 connect to $DatabaseName >/dev/null 2>&1 && db2 create user temporary tablespace $TablespaceName managed by automatic storage'" 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0 -and $createOutput -notmatch [regex]::Escape("SQL0601N") -and $createOutput -notmatch [regex]::Escape("identical to the existing name")) {
        throw "Failed to create DB2 user temporary tablespace '$TablespaceName'."
    }

    $grantOutput = docker exec $Db2ContainerName bash -lc "su - db2inst1 -c 'db2 connect to $DatabaseName >/dev/null 2>&1 && db2 grant use of tablespace $TablespaceName to user db2inst1'" 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0 -and $grantOutput -notmatch [regex]::Escape("SQL0554N") -and $grantOutput -notmatch [regex]::Escape("cannot grant a privilege or authority to itself")) {
        throw "Failed to grant use of DB2 user temporary tablespace '$TablespaceName'."
    }

    Write-Host "DB2 user temporary tablespace '$TablespaceName' created."
}

Write-Host "Starting benchmark databases..."
docker compose -f $ComposeFile up -d
if ($LASTEXITCODE -ne 0) {
    throw "docker compose up failed."
}

$genericContainers = @(
    "dbsqllikemem-bench-postgres",
    "dbsqllikemem-bench-sqlserver",
    "dbsqllikemem-bench-oracle"
)

foreach ($container in $genericContainers) {
    Wait-ForContainerHealthy -ContainerName $container -Retries $GenericRetries
}

Wait-ForMariaDbReady -ContainerName $MariaDbContainerName -Retries $MariaDbRetries
Wait-ForMySqlReady -ContainerName "dbsqllikemem-bench-mysql" -Retries 90
Wait-ForContainerHealthy -ContainerName $Db2ContainerName -Retries $Db2Retries
Wait-ForDb2Instance -Retries $Db2Retries
Ensure-Db2Database -DatabaseName "BENCH"
Ensure-Db2UserTemporaryTablespace -DatabaseName "BENCH"

Write-Host ""
Write-Host "Databases ready."
Write-Host ""
Write-Host "Suggested environment variables:"
Write-Host '$env:DBSQLLIKEMEM_BENCH_MYSQL_CONNECTION_STRING="Server=127.0.0.1;Port=13306;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"'
Write-Host '$env:DBSQLLIKEMEM_BENCH_MARIADB_CONNECTION_STRING="Server=127.0.0.1;Port=13307;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"'
Write-Host '$env:MYSQL_CONNECTION_STRING="Server=127.0.0.1;Port=13306;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"'
Write-Host '$env:MARIADB_CONNECTION_STRING="Server=127.0.0.1;Port=13307;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"'
Write-Host '$env:NPGSQL_CONNECTION_STRING="Host=127.0.0.1;Port=15432;Database=benchmark;Username=postgres;Password=postgres;Pooling=false;"'
Write-Host '$env:SQLSERVER_CONNECTION_STRING="Server=127.0.0.1,11433;Database=master;User Id=sa;Password=Your_password123;TrustServerCertificate=True;Encrypt=False;Pooling=false;"'
Write-Host '$env:SQLAZURE_CONNECTION_STRING="Server=127.0.0.1,11433;Database=master;User Id=sa;Password=Your_password123;TrustServerCertificate=True;Encrypt=False;Pooling=false;"'
Write-Host '$env:ORACLE_CONNECTION_STRING="User Id=benchmark;Password=benchmark;Data Source=127.0.0.1:15211/FREEPDB1;Pooling=false;"'
Write-Host '$env:DB2_CONNECTION_STRING="Server=127.0.0.1:15000;Database=BENCH;UID=db2inst1;PWD=db2inst1;Pooling=false;"'
Write-Host '$env:DBSQLLIKEMEM_BENCH_DB2_CONNECTION_STRING="Server=127.0.0.1:15000;Database=BENCH;UID=db2inst1;PWD=db2inst1;Pooling=false;"'
