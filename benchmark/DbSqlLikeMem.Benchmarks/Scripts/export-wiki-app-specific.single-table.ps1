param(
    [string] $ArtifactsDir = "../../docs/Wiki/BenchmarkResults/results",
    [string] $OutFile = "../../docs/Wiki/performance-matrix-app-specific.md",
    [string] $CatalogFile = ".\benchmark-feature-map.app-specific.json"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not (Test-Path $CatalogFile)) {
    throw "Catalog file not found: $CatalogFile"
}

if (-not (Test-Path $ArtifactsDir)) {
    throw "Artifacts directory not found: $ArtifactsDir"
}

function Read-TextFileSmart {
    param([Parameter(Mandatory)] [string] $PathValue)

    $bytes = [System.IO.File]::ReadAllBytes($PathValue)
    if ($bytes.Length -eq 0) {
        return ''
    }

    $utf8Strict = New-Object System.Text.UTF8Encoding($false, $true)
    try {
        return $utf8Strict.GetString($bytes)
    }
    catch {
        $cp1252 = [System.Text.Encoding]::GetEncoding(1252)
        return $cp1252.GetString($bytes)
    }
}

function Parse-BenchmarkValue {
    param([Parameter(Mandatory)] [string] $ValueText)

    $text = ($ValueText -replace '\*', '').Trim()
    if ([string]::IsNullOrWhiteSpace($text)) {
        return $null
    }

    if ($text -eq 'NA' -or $text -eq 'N/A') {
        return $null
    }

    $normalized = $text.Replace([string][char]0x03BC, 'u').Replace([string][char]0x00B5, 'u')
    $match = [regex]::Match($normalized, '^(?<value>[\d\.,]+)\s*(?<unit>ns|us|ms|s)$')
    if (-not $match.Success) {
        return $null
    }

    $numberText = $match.Groups['value'].Value.Replace(',', '')
    $value = [double]::Parse($numberText, [System.Globalization.CultureInfo]::InvariantCulture)
    $unit = $match.Groups['unit'].Value

    switch ($unit) {
        'ns' { return $value / 1000.0 }
        'us' { return $value }
        'ms' { return $value * 1000.0 }
        's'  { return $value * 1000000.0 }
        default { return $null }
    }
}

function Get-DisplayTitle {
    param([Parameter(Mandatory)] [pscustomobject] $Provider)

    if (-not [string]::IsNullOrWhiteSpace([string]$Provider.displayName)) {
        return [string]$Provider.displayName
    }

    return [string]$Provider.id
}

function Parse-BenchmarkReport {
    param([Parameter(Mandatory)] [string] $PathValue)

    $content = Read-TextFileSmart -PathValue $PathValue
    $lines = $content -split "`r?`n"
    $results = @{}
    $headerFound = $false

    foreach ($line in $lines) {
        if (-not $headerFound) {
            if ($line -match '^\|\s*Method\s*\|') {
                $headerFound = $true
            }
            continue
        }

        if ($line -match '^\|\s*-+') {
            continue
        }

        if ($line -notmatch '^\|') {
            continue
        }

        $parts = $line.Trim('|').Split('|') | ForEach-Object { $_.Trim() }
        if ($parts.Count -lt 2) {
            continue
        }

        $method = $parts[0]
        $mean = $parts[1]

        if ([string]::IsNullOrWhiteSpace($method)) {
            continue
        }

        $results[$method] = [pscustomobject]@{
            Raw = $mean
            Microseconds = (Parse-BenchmarkValue -ValueText $mean)
        }
    }

    return $results
}

function Get-SupportedFeatures {
    param([Parameter(Mandatory)] [pscustomobject] $Provider)

    if ($Provider.PSObject.Properties.Name -contains 'supportsAppFeatures') {
        return @($Provider.supportsAppFeatures)
    }

    if ($Provider.PSObject.Properties.Name -contains 'supportsMockFeatures') {
        return @($Provider.supportsMockFeatures)
    }

    return @()
}

$catalog = Get-Content $CatalogFile -Raw | ConvertFrom-Json
$reportFiles = Get-ChildItem -Path $ArtifactsDir -Filter '*-report-github.md' | Sort-Object Name

$reports = @{}
foreach ($file in $reportFiles) {
    $reports[$file.Name] = Parse-BenchmarkReport -PathValue $file.FullName
}

$lines = New-Object System.Collections.Generic.List[string]
$null = $lines.Add('# Performance features - App Specific')
$null = $lines.Add('')
$null = $lines.Add('> Gerado automaticamente a partir dos relatórios `*-report-github.md` do BenchmarkDotNet e do catálogo `benchmark-feature-map.app-specific.json`.')
$null = $lines.Add('> Tabela única com uma coluna por banco/provider.')
$null = $lines.Add('')

$providerTitles = New-Object System.Collections.Generic.List[string]
foreach ($provider in $catalog.providers) {
    $providerTitles.Add((Get-DisplayTitle -Provider $provider)) | Out-Null
}

$header = '| Feature | ' + (($providerTitles | ForEach-Object { $_ }) -join ' | ') + ' |'
$sep = '|---|' + (($providerTitles | ForEach-Object { '---:' }) -join '|') + '|'
$null = $lines.Add($header)
$null = $lines.Add($sep)

foreach ($feature in $catalog.features) {
    $row = New-Object System.Collections.Generic.List[string]
    $row.Add([string]$feature.id) | Out-Null

    foreach ($provider in $catalog.providers) {
        $mockReportName = "DbSqlLikeMem.Benchmarks.Suites.$($provider.id)_DbSqlLikeMem_Benchmarks-report-github.md"
        $mockResults = if ($reports.ContainsKey($mockReportName)) { $reports[$mockReportName] } else { @{} }
        $supportedFeatures = Get-SupportedFeatures -Provider $provider
        $supported = $supportedFeatures -contains $feature.id

        $mockCell = 'N/A'

        if ($supported) {
            if ($mockResults.ContainsKey($feature.id)) {
                $mockCell = $mockResults[$feature.id].Raw
            }
            else {
                $mockCell = 'pending'
            }
        }

        $row.Add($mockCell) | Out-Null
    }

    $null = $lines.Add('| ' + ($row -join ' | ') + ' |')
}

$null = $lines.Add('')
$null = $lines.Add('## Source files')
$null = $lines.Add('')

foreach ($provider in $catalog.providers) {
    $title = Get-DisplayTitle -Provider $provider
    $mockReportName = "DbSqlLikeMem.Benchmarks.Suites.$($provider.id)_DbSqlLikeMem_Benchmarks-report-github.md"
    $null = $lines.Add("- $($title): ./$ArtifactsDir/results/$($mockReportName.Replace('-github.md', '.html'))")
}

$dir = Split-Path -Parent $OutFile
if ($dir -and -not (Test-Path $dir)) {
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
}

Set-Content -Path $OutFile -Value $lines -Encoding UTF8
Write-Host "Wiki matrix written to $OutFile"
