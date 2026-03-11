[CmdletBinding()]
param(
    [Alias('ReportsPath')]
    [string] $ArtifactsDir = './BenchmarkDotNet.Artifacts/results',

    [Alias('OutputPath')]
    [string] $OutFile = '../../docs/Wiki/performance-matrix.md',

    [Alias('FeatureMapPath')]
    [string] $CatalogFile = './benchmark-feature-map.json'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$InvariantCulture = [System.Globalization.CultureInfo]::InvariantCulture

function Resolve-InputPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,

        [Parameter(Mandatory = $true)]
        [string] $Description
    )

    $candidates = New-Object System.Collections.Generic.List[string]

    if ([System.IO.Path]::IsPathRooted($Path)) {
        $null = $candidates.Add($Path)
    }
    else {
        $currentPath = (Get-Location).Path
        $null = $candidates.Add((Join-Path -Path $currentPath -ChildPath $Path))

        if ($PSScriptRoot) {
            $null = $candidates.Add((Join-Path -Path $PSScriptRoot -ChildPath $Path))

            $scriptParent = Split-Path -Parent $PSScriptRoot
            if (-not [string]::IsNullOrWhiteSpace($scriptParent)) {
                $null = $candidates.Add((Join-Path -Path $scriptParent -ChildPath $Path))
            }
        }
    }

    foreach ($candidate in ($candidates | Select-Object -Unique)) {
        if (Test-Path -LiteralPath $candidate) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }

    throw "$Description not found: $Path"
}

function Resolve-OutputFilePath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    if ([System.IO.Path]::IsPathRooted($Path)) {
        $targetPath = $Path
    }
    else {
        $targetPath = Join-Path -Path (Get-Location).Path -ChildPath $Path
    }

    $directoryPath = Split-Path -Parent $targetPath
    if ([string]::IsNullOrWhiteSpace($directoryPath)) {
        $directoryPath = (Get-Location).Path
    }

    if (-not (Test-Path -LiteralPath $directoryPath)) {
        New-Item -ItemType Directory -Path $directoryPath -Force | Out-Null
    }

    $resolvedDirectory = (Resolve-Path -LiteralPath $directoryPath).Path
    $fileName = Split-Path -Leaf $targetPath

    return (Join-Path -Path $resolvedDirectory -ChildPath $fileName)
}

function Get-SuiteInfoFromFileName {
    param(
        [Parameter(Mandatory = $true)]
        [string] $FileName
    )

    $pattern = '^(?:DbSqlLikeMem\.Benchmarks\.Suites\.)?(?<Provider>[A-Za-z0-9]+)_(?<Engine>[A-Za-z0-9]+)_Benchmarks-report-github\.md$'
    $match = [System.Text.RegularExpressions.Regex]::Match($FileName, $pattern)

    if (-not $match.Success) {
        return $null
    }

    return [pscustomobject]@{
        Provider = $match.Groups['Provider'].Value
        Engine   = $match.Groups['Engine'].Value
    }
}

function Get-ExternalReportEngineName {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ExternalEngine
    )

    switch ($ExternalEngine) {
        'Testcontainers' { return 'Testcontainers' }
        'NativeAdoNet'   { return 'Native' }
        default          { return $null }
    }
}

function Normalize-MeanToken {
    param(
        [AllowNull()]
        [string] $Value
    )

    if ($null -eq $Value) {
        return ''
    }

    $normalized = $Value.Trim()
    $normalized = $normalized -replace '[μµ]s', 'us'
    $normalized = $normalized -replace '\s+', ' '

    return $normalized
}

function Format-MeanFromNanoseconds {
    param(
        [Parameter(Mandatory = $true)]
        [double] $Nanoseconds
    )

    $absoluteValue = [math]::Abs($Nanoseconds)

    if ($absoluteValue -ge 1000000000.0) {
        $value = $Nanoseconds / 1000000000.0
        $unit = 's'
    }
    elseif ($absoluteValue -ge 1000000.0) {
        $value = $Nanoseconds / 1000000.0
        $unit = 'ms'
    }
    elseif ($absoluteValue -ge 1000.0) {
        $value = $Nanoseconds / 1000.0
        $unit = 'us'
    }
    else {
        $value = $Nanoseconds
        $unit = 'ns'
    }

    $text = $value.ToString('#,0.###', $InvariantCulture)

    return "$text $unit"
}

function Parse-MeanValue {
    param(
        [AllowNull()]
        [string] $Value
    )

    $display = Normalize-MeanToken -Value $Value

    if ([string]::IsNullOrWhiteSpace($display)) {
        return [pscustomobject]@{
            State       = 'Pending'
            Display     = 'pending'
            Nanoseconds = $null
        }
    }

    if ($display -match '^(NA|N/A)$') {
        return [pscustomobject]@{
            State       = 'NA'
            Display     = 'NA'
            Nanoseconds = $null
        }
    }

    $pattern = '^(?<Number>[+-]?(?:\d{1,3}(?:,\d{3})*|\d+)(?:\.\d+)?)\s*(?<Unit>ns|us|ms|s)$'
    $match = [System.Text.RegularExpressions.Regex]::Match($display, $pattern)

    if (-not $match.Success) {
        return [pscustomobject]@{
            State       = 'Text'
            Display     = $display
            Nanoseconds = $null
        }
    }

    $numberText = $match.Groups['Number'].Value -replace ',', ''
    $numberValue = [double]::Parse($numberText, $InvariantCulture)
    $unit = $match.Groups['Unit'].Value

    switch ($unit) {
        'ns' { $nanoseconds = $numberValue }
        'us' { $nanoseconds = $numberValue * 1000.0 }
        'ms' { $nanoseconds = $numberValue * 1000000.0 }
        's'  { $nanoseconds = $numberValue * 1000000000.0 }
        default {
            return [pscustomobject]@{
                State       = 'Text'
                Display     = $display
                Nanoseconds = $null
            }
        }
    }

    return [pscustomobject]@{
        State       = 'Numeric'
        Display     = (Format-MeanFromNanoseconds -Nanoseconds $nanoseconds)
        Nanoseconds = $nanoseconds
    }
}

function Get-ReportRows {
    param(
        [Parameter(Mandatory = $true)]
        [string] $FilePath
    )

    $content = Get-Content -LiteralPath $FilePath -Raw -Encoding UTF8
    $lines = [System.Text.RegularExpressions.Regex]::Split($content, '\r?\n')
    $rows = @{}
    $tableStarted = $false

    foreach ($line in $lines) {
        if ($line -match '^\|\s*Method\s*\|') {
            $tableStarted = $true
            continue
        }

        if (-not $tableStarted) {
            continue
        }

        if ($line -match '^\|\s*-+') {
            continue
        }

        if ($line -notmatch '^\|') {
            continue
        }

        $trimmed = $line.Trim()
        $trimmed = $trimmed.Trim('|')
        $parts = $trimmed.Split('|') | ForEach-Object { $_.Trim() }

        if ($parts.Count -lt 2) {
            continue
        }

        $method = $parts[0]
        $mean = $parts[1]

        if ([string]::IsNullOrWhiteSpace($method)) {
            continue
        }

        $rows[$method] = Parse-MeanValue -Value $mean
    }

    return $rows
}

function New-CellResult {
    param(
        [Parameter(Mandatory = $true)]
        [string] $State,

        [Parameter(Mandatory = $true)]
        [string] $Display,

        [AllowNull()]
        [object] $Nanoseconds
    )

    return [pscustomobject]@{
        State       = $State
        Display     = $Display
        Nanoseconds = $Nanoseconds
    }
}

function Get-CellResult {
    param(
        [Parameter(Mandatory = $true)]
        [psobject] $ProviderInfo,

        [Parameter(Mandatory = $true)]
        [string] $Engine,

        [Parameter(Mandatory = $true)]
        [string] $FeatureId
    )

    $supported = $false

    if ($Engine -eq 'DbSqlLikeMem') {
        $supported = @($ProviderInfo.supportsMockFeatures) -contains $FeatureId
    }
    elseif ($Engine -eq 'Testcontainers' -or $Engine -eq 'Native') {
        $supported = @($ProviderInfo.supportsExternalFeatures) -contains $FeatureId
    }

    if (-not $supported) {
        return (New-CellResult -State 'Unsupported' -Display 'N/A' -Nanoseconds $null)
    }

    $reportKey = '{0}|{1}' -f $ProviderInfo.id, $Engine
    if (-not $script:ReportIndex.ContainsKey($reportKey)) {
        return (New-CellResult -State 'Pending' -Display 'pending' -Nanoseconds $null)
    }

    $rows = $script:ReportIndex[$reportKey]
    if (-not $rows.ContainsKey($FeatureId)) {
        return (New-CellResult -State 'Pending' -Display 'pending' -Nanoseconds $null)
    }

    return $rows[$FeatureId]
}

function Format-ImprovementPercent {
    param(
        [Parameter(Mandatory = $true)]
        [double] $WinnerNanoseconds,

        [Parameter(Mandatory = $true)]
        [double] $LoserNanoseconds
    )

    if ($LoserNanoseconds -le 0.0) {
        return '0.0%'
    }

    $percent = (($LoserNanoseconds - $WinnerNanoseconds) / $LoserNanoseconds) * 100.0
    if ($percent -lt 0.0) {
        $percent = 0.0
    }

    return ($percent.ToString('0.0', $InvariantCulture) + '%')
}

function Decorate-PairedCells {
    param(
        [Parameter(Mandatory = $true)]
        [psobject] $AppCell,

        [Parameter(Mandatory = $true)]
        [psobject] $ExternalCell
    )

    $appDisplay = $AppCell.Display
    $externalDisplay = $ExternalCell.Display

    if ($AppCell.State -ne 'Numeric' -or $ExternalCell.State -ne 'Numeric') {
        return [pscustomobject]@{
            AppDisplay      = $appDisplay
            ExternalDisplay = $externalDisplay
        }
    }

    $appNs = [double] $AppCell.Nanoseconds
    $externalNs = [double] $ExternalCell.Nanoseconds

    $difference = [math]::Abs($appNs - $externalNs)
    $maxValue = [math]::Max([math]::Abs($appNs), [math]::Abs($externalNs))
    $isTie = $difference -le ([math]::Max(0.000000001, $maxValue * 0.000000000001))

    if ($isTie) {
        return [pscustomobject]@{
            AppDisplay      = ($appDisplay + ' = tie')
            ExternalDisplay = ($externalDisplay + ' = tie')
        }
    }

    if ($appNs -lt $externalNs) {
        $delta = Format-ImprovementPercent -WinnerNanoseconds $appNs -LoserNanoseconds $externalNs

        return [pscustomobject]@{
            AppDisplay      = ($appDisplay + ' :white_check_mark: app +' + $delta)
            ExternalDisplay = ($externalDisplay + ' :x: app +' + $delta)
        }
    }

    $delta = Format-ImprovementPercent -WinnerNanoseconds $externalNs -LoserNanoseconds $appNs

    return [pscustomobject]@{
        AppDisplay      = ($appDisplay + ' :x: db +' + $delta)
        ExternalDisplay = ($externalDisplay + ' :white_check_mark: db +' + $delta)
    }
}

$resolvedCatalogPath = Resolve-InputPath -Path $CatalogFile -Description 'Catalog file'
$resolvedArtifactsPath = Resolve-InputPath -Path $ArtifactsDir -Description 'Artifacts directory'
$resolvedOutputPath = Resolve-OutputFilePath -Path $OutFile

$catalog = Get-Content -LiteralPath $resolvedCatalogPath -Raw -Encoding UTF8 | ConvertFrom-Json
$reportFiles = Get-ChildItem -LiteralPath $resolvedArtifactsPath -File -Filter '*-report-github.md' | Sort-Object -Property Name

$script:ProviderMap = @{}
foreach ($providerInfo in $catalog.providers) {
    $script:ProviderMap[$providerInfo.id] = $providerInfo
}

$script:ReportIndex = @{}
foreach ($file in $reportFiles) {
    $suiteInfo = Get-SuiteInfoFromFileName -FileName $file.Name
    if ($null -eq $suiteInfo) {
        continue
    }

    $reportKey = '{0}|{1}' -f $suiteInfo.Provider, $suiteInfo.Engine
    $script:ReportIndex[$reportKey] = Get-ReportRows -FilePath $file.FullName
}

$columns = New-Object System.Collections.Generic.List[string]
foreach ($providerInfo in $catalog.providers) {
    $null = $columns.Add(($providerInfo.id + '-DbSqlLikeMem'))

    $externalReportEngine = Get-ExternalReportEngineName -ExternalEngine $providerInfo.externalEngine
    if ($null -ne $externalReportEngine) {
        $null = $columns.Add(($providerInfo.id + '-' + $externalReportEngine))
    }
}

$lines = New-Object System.Collections.Generic.List[string]
$null = $lines.Add('# Performance matrix')
$null = $lines.Add('')
$null = $lines.Add('> Generated automatically from BenchmarkDotNet `*-report-github.md` reports and `benchmark-feature-map.json`.')
$null = $lines.Add('')

$header = '| Feature | ' + (($columns | ForEach-Object { $_ }) -join ' | ') + ' |'
$separator = '|' + ((1..($columns.Count + 1) | ForEach-Object { '---|' }) -join '')
$null = $lines.Add($header)
$null = $lines.Add($separator)

foreach ($feature in $catalog.features) {
    $row = New-Object System.Collections.Generic.List[string]
    $null = $row.Add($feature.id)

    foreach ($providerInfo in $catalog.providers) {
        $appCell = Get-CellResult -ProviderInfo $providerInfo -Engine 'DbSqlLikeMem' -FeatureId $feature.id
        $externalReportEngine = Get-ExternalReportEngineName -ExternalEngine $providerInfo.externalEngine

        if ($null -eq $externalReportEngine) {
            $null = $row.Add($appCell.Display)
            continue
        }

        $externalCell = Get-CellResult -ProviderInfo $providerInfo -Engine $externalReportEngine -FeatureId $feature.id
        $decorated = Decorate-PairedCells -AppCell $appCell -ExternalCell $externalCell

        $null = $row.Add($decorated.AppDisplay)
        $null = $row.Add($decorated.ExternalDisplay)
    }

    $null = $lines.Add('| ' + ($row -join ' | ') + ' |')
}

$null = $lines.Add('')
$null = $lines.Add('## Legend')
$null = $lines.Add('')
$null = $lines.Add('- `:white_check_mark: app +X%`: DbSqlLikeMem was faster than the paired real/native engine by X%.')
$null = $lines.Add('- `:white_check_mark: db +X%`: The paired real/native engine was faster than DbSqlLikeMem by X%.')
$null = $lines.Add('- `:x: db +X%`: This DbSqlLikeMem cell lost to the paired real/native engine by X%.')
$null = $lines.Add('- `:x: app +X%`: This real/native engine cell lost to DbSqlLikeMem by X%.')
$null = $lines.Add('- `NA`: the benchmark ran but BenchmarkDotNet reported no numeric result for that method.')
$null = $lines.Add('- `pending`: the feature is supported in the catalog, but the report file or method row was not found.')
$null = $lines.Add('- `N/A`: the feature is not applicable for that provider/engine pair.')
$null = $lines.Add('')
$null = $lines.Add('> Comparison is always pairwise within the same provider family: DbSqlLikeMem vs external engine (or SQLite Native). Lower mean is better.')

$utf8WithBom = New-Object System.Text.UTF8Encoding -ArgumentList $true
[System.IO.File]::WriteAllLines($resolvedOutputPath, $lines, $utf8WithBom)

Write-Host ("Wiki matrix written to {0}" -f $resolvedOutputPath)
