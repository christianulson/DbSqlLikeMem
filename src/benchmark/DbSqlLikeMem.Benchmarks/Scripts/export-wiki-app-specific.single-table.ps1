param(
    [string] $ArtifactsDir = "../../../docs/Wiki/BenchmarkResults/results",
    [string] $OutFile = "../../../docs/Wiki/performance-matrix-app-specific.md",
    [string] $CatalogFile = ".\benchmark-feature-map.app-specific.json",
    [string] $EnvironmentManifestFile = "../../../docs/Wiki/BenchmarkResults/benchmark-run.environment.json"
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

function Get-FeatureKey {
    param([Parameter(Mandatory)] [pscustomobject] $Feature)

    if ($Feature.PSObject.Properties.Name -contains 'stableId' -and -not [string]::IsNullOrWhiteSpace([string]$Feature.stableId)) {
        return [string]$Feature.stableId
    }

    return [string]$Feature.id
}

function Get-FeatureStatus {
    param([Parameter(Mandatory)] [pscustomobject] $Feature)

    if ($Feature.PSObject.Properties.Name -contains 'status' -and -not [string]::IsNullOrWhiteSpace([string]$Feature.status)) {
        return [string]$Feature.status
    }

    return 'Active'
}

function Get-CategoryRow {
    param(
        [Parameter(Mandatory)] [string] $Category,
        [Parameter(Mandatory)] [int] $ColumnCount
    )

    $cells = New-Object System.Collections.Generic.List[string]
    $cells.Add("**$Category**") | Out-Null

    for ($i = 1; $i -lt $ColumnCount; $i++) {
        $cells.Add('') | Out-Null
    }

    return '| ' + ($cells -join ' | ') + ' |'
}

function Get-EnvironmentHeaderLine {
    param([Parameter(Mandatory)] [pscustomobject] $Environment)

    $runEnvironment = if ($Environment.PSObject.Properties.Name -contains 'environment') { $Environment.environment } else { $Environment }
    $profile = if ($runEnvironment.PSObject.Properties.Name -contains 'profile') { [string]$runEnvironment.profile } else { '' }
    $os = if ($runEnvironment.PSObject.Properties.Name -contains 'os') { [string]$runEnvironment.os } elseif ($runEnvironment.PSObject.Properties.Name -contains 'operatingSystem') { [string]$runEnvironment.operatingSystem } else { '' }
    $framework = if ($runEnvironment.PSObject.Properties.Name -contains 'framework') { [string]$runEnvironment.framework } else { '' }
    $runtime = if ($runEnvironment.PSObject.Properties.Name -contains 'runtime') { [string]$runEnvironment.runtime } else { '' }
    $timestampUtc = if ($runEnvironment.PSObject.Properties.Name -contains 'timestampUtc') { [string]$runEnvironment.timestampUtc } else { '' }
    $runId = if ($Environment.PSObject.Properties.Name -contains 'runId') { [string]$Environment.runId } else { '' }
    $jobId = if ($Environment.PSObject.Properties.Name -contains 'jobId') { [string]$Environment.jobId } else { '' }

    return "> Ambiente: profile=$profile; runId=$runId; jobId=$jobId; os=$os; framework=$framework; runtime=$runtime; timestampUtc=$timestampUtc"
}

$catalog = Get-Content $CatalogFile -Raw | ConvertFrom-Json
$schemaPath = if ($catalog.PSObject.Properties.Name -contains 'resultSchema') { [string]$catalog.resultSchema } else { $null }
if ([string]::IsNullOrWhiteSpace($schemaPath)) {
    throw "Catalog file does not declare resultSchema: $CatalogFile"
}

$catalogDir = Split-Path -Parent $CatalogFile
$schemaFullPath = Join-Path $catalogDir $schemaPath
if (-not (Test-Path $schemaFullPath)) {
    throw "Result schema file not found: $schemaFullPath"
}

$resultSchema = Get-Content $schemaFullPath -Raw | ConvertFrom-Json
if ([string]::IsNullOrWhiteSpace([string]$resultSchema.title)) {
    throw "Result schema title is missing: $schemaFullPath"
}

$environmentManifest = $null
if (Test-Path $EnvironmentManifestFile) {
    $environmentManifest = Get-Content $EnvironmentManifestFile -Raw | ConvertFrom-Json
}

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
if ($null -ne $environmentManifest) {
    $null = $lines.Add((Get-EnvironmentHeaderLine -Environment $environmentManifest))
}
$null = $lines.Add('')

$providerTitles = New-Object System.Collections.Generic.List[string]
foreach ($provider in $catalog.providers) {
    $providerTitles.Add((Get-DisplayTitle -Provider $provider)) | Out-Null
}

$header = '| Feature | Status | ' + (($providerTitles | ForEach-Object { $_ }) -join ' | ') + ' | Description |'
$sep = '|---|:---:|' + (($providerTitles | ForEach-Object { '---:' }) -join '|') + '|:---:|'
$null = $lines.Add($header)
$null = $lines.Add($sep)

$category = $null

foreach ($feature in ($catalog.features | Sort-Object category, id)) {
    $featureKey = Get-FeatureKey -Feature $feature

    if ($category -ne $feature.category) {
        $category = $feature.category
        $null = $lines.Add((Get-CategoryRow -Category $category -ColumnCount ($providerTitles.Count + 3)))
    }

    $row = New-Object System.Collections.Generic.List[string]
    $row.Add([string]$feature.displayName) | Out-Null
    $row.Add((Get-FeatureStatus -Feature $feature)) | Out-Null

    foreach ($provider in $catalog.providers) {
        $mockReportName = "DbSqlLikeMem.Benchmarks.Suites.$($provider.id)_DbSqlLikeMem_Benchmarks-report-github.md"
        $mockResults = if ($reports.ContainsKey($mockReportName)) { $reports[$mockReportName] } else { @{} }
        $supportedFeatures = Get-SupportedFeatures -Provider $provider
        $supported = $supportedFeatures -contains $featureKey

        $mockCell = 'N/A'

        if ($supported) {
            if ($mockResults.ContainsKey($featureKey)) {
                $culture = [System.Globalization.CultureInfo]::GetCultureInfo('pt-BR')
                $mockCell = [math]::Round($mockResults[$featureKey].Microseconds, 2, [System.MidpointRounding]::AwayFromZero).ToString('N2', $culture) + ' ' + ([string][char]0x03BC) +'s'
            }
            else {
                $mockCell = 'pending'
            }
        }

        $row.Add($mockCell) | Out-Null
    }

    $notes = if ($feature.PSObject.Properties['notes']) { $feature.notes } else { '' }

    $null = $lines.Add('| ' + ($row -join ' | ') + ' | ' + $($notes) + ' |')
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
