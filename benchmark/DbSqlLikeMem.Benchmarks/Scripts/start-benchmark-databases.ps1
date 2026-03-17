param(
    [string]$ComposeFile = ".\docker-compose.benchmarks.yml",
    [string]$Db2ContainerName = "dbsqllikemem-bench-db2",
    [int]$Db2ReadyTimeoutSeconds = 300
)

$ErrorActionPreference = "Stop"

Write-Host "Starting benchmark databases..."

docker compose -f docker-compose.benchmarks.yml up -d

Write-Host "Waiting DB2 to start..."
Start-Sleep -Seconds 20

$container = "dbsqllikemem-bench-db2"

Write-Host "Checking DB2 database..."

$dbExists = docker exec $container bash -c "su - db2inst1 -c 'db2 list db directory'" | Select-String "BENCH"

if (-not $dbExists) {
    Write-Host "Creating BENCH database..."
    docker exec $container bash -c "su - db2inst1 -c 'db2 create database BENCH'"
}
else {
    Write-Host "BENCH database already exists."
}

Write-Host "Databases ready."