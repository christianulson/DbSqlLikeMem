$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
$requiredFiles = @(
    'benchmark-feature-map.json',
    'benchmark-feature-map.app-specific.json',
    'benchmark-result.schema.json'
)

foreach ($file in $requiredFiles) {
    $path = Join-Path $projectRoot $file
    if (-not (Test-Path $path)) {
        throw "Required benchmark file not found: $path"
    }

    $null = Get-Content -Raw $path | ConvertFrom-Json
}

dotnet run -c Release -- --validate-catalog
