param(
    [string] $ArtifactsDir = "../../docs/Wiki/BenchmarkResults/results",
    [string] $OutFile = "../../docs/Wiki/performance-matrix.md",
    [string] $CatalogFile = ".\benchmark-feature-map.json"
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

function Get-ExternalEngineLabel {
    param([Parameter(Mandatory)] [pscustomobject] $Provider)

    if ($Provider.externalEngine -eq 'NativeAdoNet') {
        return 'Native'
    }

    if ([string]::IsNullOrWhiteSpace([string]$Provider.externalEngine) -or $Provider.externalEngine -eq 'NotAvailable') {
        return 'External'
    }

    return [string]$Provider.externalEngine
}

function Get-ExternalEngineReportName {
    param([Parameter(Mandatory)] [pscustomobject] $Provider)

    if ($Provider.id -eq 'Sqlite' -and $Provider.externalEngine -eq 'NativeAdoNet') {
        return "DbSqlLikeMem.Benchmarks.Suites.$($Provider.id)_Native_Benchmarks-report-github.md"
    }

    if ([string]::IsNullOrWhiteSpace([string]$Provider.externalEngine) -or $Provider.externalEngine -eq 'NotAvailable') {
        return $null
    }

    return "DbSqlLikeMem.Benchmarks.Suites.$($Provider.id)_$((Get-ExternalEngineLabel -Provider $Provider))_Benchmarks-report-github.md"
}

function Get-DisplayTitle {
    param([Parameter(Mandatory)] [pscustomobject] $Provider)

    if (-not [string]::IsNullOrWhiteSpace([string]$Provider.displayName)) {
        return [string]$Provider.displayName
    }

    return [string]$Provider.id
}

function Get-PercentText {
    param(
        [double] $AppMicroseconds,
        [double] $OtherMicroseconds
    )

    if ($OtherMicroseconds -eq 0) {
        return '-'
    }

    $percent = (($OtherMicroseconds - $AppMicroseconds) / $OtherMicroseconds) * 100.0
    $culture = [System.Globalization.CultureInfo]::GetCultureInfo('pt-BR')
    return ([math]::Round($percent, 2, [System.MidpointRounding]::AwayFromZero).ToString('0.##', $culture)) + '%'
}

function Get-ResultText {
    param(
        [double] $AppMicroseconds,
        [double] $OtherMicroseconds
    )

    if ($AppMicroseconds -lt $OtherMicroseconds) {
        return ':white_check_mark:'
    }

    if ($AppMicroseconds -gt $OtherMicroseconds) {
        return ':x:'
    }

    return '='
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

$catalog = Get-Content $CatalogFile -Raw | ConvertFrom-Json
$reportFiles = Get-ChildItem -Path $ArtifactsDir -Filter '*-report-github.md' | Sort-Object Name

$reports = @{}
foreach ($file in $reportFiles) {
    $reports[$file.Name] = Parse-BenchmarkReport -PathValue $file.FullName
}

$lines = New-Object System.Collections.Generic.List[string]
$null = $lines.Add('# Performance matrix')
$null = $lines.Add('')
$null = $lines.Add('> Gerado automaticamente a partir dos relatórios `*-report-github.md` do BenchmarkDotNet e do catálogo `benchmark-feature-map.json`.')
$null = $lines.Add('> Percentual: valor positivo = app mais rápida; valor negativo = banco mais rápido.')
$null = $lines.Add('')

foreach ($provider in $catalog.providers) {
    $title = Get-DisplayTitle -Provider $provider
    $externalLabel = Get-ExternalEngineLabel -Provider $provider

    $mockReportName = "DbSqlLikeMem.Benchmarks.Suites.$($provider.id)_DbSqlLikeMem_Benchmarks-report-github.md"
    $externalReportName = Get-ExternalEngineReportName -Provider $provider

    $mockResults = if ($reports.ContainsKey($mockReportName)) { $reports[$mockReportName] } else { @{} }
    $externalResults = if ($externalReportName -and $reports.ContainsKey($externalReportName)) { $reports[$externalReportName] } else { @{} }

    $null = $lines.Add("## $title")
    $null = $lines.Add('')
    $null = $lines.Add("| Feature | DbSqlLikeMem | $externalLabel | Diff | Percent | Result | Description |")
    $null = $lines.Add("|---|---:|---:|---:|---:|:---:|:---:|")

    $category = $null

    foreach ($feature in ($catalog.features | Sort-Object category, id)) {
        if ($category -ne $feature.category) {
            $category = $feature.category
            $null = $lines.Add("| **$category** |  |  |  |  |  |  |")
        }

        $mockSupported = @($provider.supportsMockFeatures) -contains $feature.id
        $externalSupported = @($provider.supportsExternalFeatures) -contains $feature.id

        $mockCell = 'N/A'
        $externalCell = 'N/A'
        $diffCell = '-'
        $percentCell = '-'
        $resultCell = '-'

        $mockValue = $null
        $externalValue = $null

        if ($mockSupported) {
            if ($mockResults.ContainsKey($feature.id)) {
                $mockValue = $mockResults[$feature.id]
                $mockCell = $mockValue.Raw
            }
            else {
                $mockCell = 'pending'
            }
        }

        if ($externalSupported) {
            if ($externalResults.ContainsKey($feature.id)) {
                $externalValue = $externalResults[$feature.id]
                $externalCell = $externalValue.Raw
            }
            else {
                $externalCell = 'pending'
            }
        }

        if ($mockSupported -and $externalSupported -and $null -ne $mockValue -and $null -ne $externalValue) {
            if ($null -ne $mockValue.Microseconds -and $null -ne $externalValue.Microseconds) {
                $diff = [double]$externalValue.Microseconds - [double]$mockValue.Microseconds
                $culture = [System.Globalization.CultureInfo]::GetCultureInfo('pt-BR')
                $diffCell = [math]::Round($diff, 3, [System.MidpointRounding]::AwayFromZero).ToString('N3', $culture) + ' us'
                $percentCell = Get-PercentText -AppMicroseconds ([double]$mockValue.Microseconds) -OtherMicroseconds ([double]$externalValue.Microseconds)
                $resultCell = Get-ResultText -AppMicroseconds ([double]$mockValue.Microseconds) -OtherMicroseconds ([double]$externalValue.Microseconds)
            }
        }

        $notes = if ($feature.PSObject.Properties['notes']) { $feature.notes } else { '' }

        $null = $lines.Add("| $($feature.displayName) | $mockCell | $externalCell | $diffCell | $percentCell | $resultCell | $notes |")
    }

    $null = $lines.Add('')
    $null = $lines.Add('Source files:')
    $null = $lines.Add("- ./$ArtifactsDir/results/$($mockReportName.Replace('-github.md', '.html'))")
    if ($externalReportName) {
        $null = $lines.Add("- ./$ArtifactsDir/results/$($externalReportName.Replace('-github.md', '.html'))")
    }
    $null = $lines.Add('')
}

$dir = Split-Path -Parent $OutFile
if ($dir -and -not (Test-Path $dir)) {
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
}

Set-Content -Path $OutFile -Value $lines -Encoding UTF8
Write-Host "Wiki matrix written to $OutFile"
