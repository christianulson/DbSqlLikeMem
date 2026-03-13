param(
    [string]$ComposeFile = ".\docker-compose.benchmarks.yml"
)

$ErrorActionPreference = "Stop"

docker compose -f $ComposeFile down
