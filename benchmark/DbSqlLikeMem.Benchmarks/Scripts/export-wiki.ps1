param(
    [string]$ReportsPath = './BenchmarkDotNet.Artifacts/results',
    [string]$FeatureMapPath = '.\benchmark-feature-map.json',
    [string]$OutputPath = '../../docs/Wiki/performance-matrix.md'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Resolve-FullPath {
    param([Parameter(Mandatory)][string]$PathValue)

    $parent = Split-Path -Parent $PathValue
    $leaf = Split-Path -Leaf $PathValue

    if ([string]::IsNullOrWhiteSpace($parent)) { $parent = '.' }
    if (-not (Test-Path -LiteralPath $parent)) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }

    $resolvedParent = (Resolve-Path -LiteralPath $parent).Path
    if ([string]::IsNullOrWhiteSpace($leaf)) { return $resolvedParent }
    return (Join-Path $resolvedParent $leaf)
}

function Read-TextFileSmart {
    param([Parameter(Mandatory)][string]$PathValue)

    $bytes = [System.IO.File]::ReadAllBytes($PathValue)
    if ($bytes.Length -eq 0) { return '' }

    $utf8Strict = New-Object System.Text.UTF8Encoding($false, $true)
    try {
        return $utf8Strict.GetString($bytes)
    }
    catch {
        $cp1252 = [System.Text.Encoding]::GetEncoding(1252)
        return $cp1252.GetString($bytes)
    }
}

function Read-FeatureMap {
    param([string]$PathValue)

    if (-not (Test-Path -LiteralPath $PathValue)) { return $null }

    $json = Read-TextFileSmart -PathValue $PathValue
    if ([string]::IsNullOrWhiteSpace($json)) { return $null }

    return ($json | ConvertFrom-Json)
}

function Get-ProviderConfig {
    param($FeatureMap, [string]$ProviderId)

    if ($null -eq $FeatureMap -or $null -eq $FeatureMap.providers) { return $null }
    foreach ($provider in $FeatureMap.providers) {
        if ([string]$provider.id -eq $ProviderId) { return $provider }
    }
    return $null
}

function Get-FeatureOrder {
    param($FeatureMap, [string[]]$DiscoveredFeatures)

    $ordered = New-Object System.Collections.Generic.List[string]
    $seen = @{}

    if ($null -ne $FeatureMap -and $null -ne $FeatureMap.features) {
        foreach ($feature in $FeatureMap.features) {
            $id = [string]$feature.id
            if (-not [string]::IsNullOrWhiteSpace($id) -and -not $seen.ContainsKey($id)) {
                [void]$ordered.Add($id)
                $seen[$id] = $true
            }
        }
    }

    foreach ($featureId in $DiscoveredFeatures) {
        if (-not [string]::IsNullOrWhiteSpace($featureId) -and -not $seen.ContainsKey($featureId)) {
            [void]$ordered.Add([string]$featureId)
            $seen[[string]$featureId] = $true
        }
    }

    return $ordered
}

function Get-SupportedFeatureState {
    param([string]$FeatureId, [object[]]$SupportedFeatures)

    if ($null -eq $SupportedFeatures -or $SupportedFeatures.Count -eq 0) { return $true }
    foreach ($item in $SupportedFeatures) {
        if ([string]$item -eq $FeatureId) { return $true }
    }
    return $false
}

function Normalize-Unit {
    param([string]$Unit)

    if ([string]::IsNullOrWhiteSpace($Unit)) { return '' }

    $raw = $Unit.Trim().ToLowerInvariant().Replace(' ', '')
    $ascii = [regex]::Replace($raw, '[^a-z]', '')

    if ($ascii -eq 'ns' -or $raw -match 'ns$') { return 'ns' }
    if ($ascii -eq 'ms' -or $raw -match 'ms$') { return 'ms' }
    if ($raw -eq 's') { return 's' }
    if ($ascii -eq 'us') { return 'us' }

    if ($ascii -eq 's' -and $raw.Length -gt 1) { return 'us' }
    if ($raw -match 's$') {
        if ($raw.Length -gt 1) { return 'us' }
        return 's'
    }

    return ''
}

function Convert-ToMicroseconds {
    param(
        [Parameter(Mandatory)][double]$Value,
        [Parameter(Mandatory)][string]$Unit
    )

    switch (Normalize-Unit $Unit) {
        'ns' { return $Value / 1000.0 }
        'us' { return $Value }
        'ms' { return $Value * 1000.0 }
        's'  { return $Value * 1000000.0 }
        default { throw "Unsupported unit '$Unit'." }
    }
}

function Format-Number {
    param(
        [Parameter(Mandatory)][double]$Value,
        [int]$Decimals = 3
    )

    $culture = [System.Globalization.CultureInfo]::GetCultureInfo('pt-BR')
    $rounded = [math]::Round($Value, $Decimals, [System.MidpointRounding]::AwayFromZero)
    return $rounded.ToString("N$Decimals", $culture)
}

function Format-Us {
    param(
        [Parameter(Mandatory)][double]$Value,
        [int]$Decimals = 3
    )

    return ((Format-Number -Value $Value -Decimals $Decimals) + ' us')
}

function Parse-MeanText {
    param([Parameter(Mandatory)][string]$MeanText)

    $clean = $MeanText.Trim()
    if ($clean -match '^(NA|N/A)$') {
        return [pscustomobject]@{ Status = 'NA'; MeanUs = $null }
    }

    $pattern = '^(?<value>[-+]?\d{1,3}(?:,\d{3})*(?:\.\d+)?|[-+]?\d+(?:\.\d+)?)\s*(?<unit>\S+)$'
    $m = [regex]::Match($clean, $pattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    if (-not $m.Success) {
        return $null
    }

    $rawValue = $m.Groups['value'].Value.Replace(',', '')
    $unit = $m.Groups['unit'].Value
    $value = [double]::Parse($rawValue, [System.Globalization.CultureInfo]::InvariantCulture)
    return [pscustomobject]@{ Status = 'OK'; MeanUs = (Convert-ToMicroseconds -Value $value -Unit $unit) }
}

function Parse-BenchmarkReport {
    param([Parameter(Mandatory)][string]$PathValue)

    $fileName = [System.IO.Path]::GetFileName($PathValue)
    $nameMatch = [regex]::Match(
        $fileName,
        '^DbSqlLikeMem\.Benchmarks\.Suites\.(?<provider>[^_]+)_(?<engine>[^_]+)_Benchmarks-report-github\.md$',
        [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
    )
    if (-not $nameMatch.Success) { return $null }

    $provider = $nameMatch.Groups['provider'].Value
    $engine = $nameMatch.Groups['engine'].Value
    $content = Read-TextFileSmart -PathValue $PathValue
    $content = $content -replace "`r`n", "`n"

    $rows = @{}
    $tableStarted = $false

    foreach ($line in ($content -split "`n")) {
        if (-not $tableStarted) {
            if ($line -match '^\|\s*Method\s*\|') { $tableStarted = $true }
            continue
        }

        if ($line -match '^\|\s*-+') { continue }
        if ($line -notmatch '^\|') { if ($tableStarted) { break } else { continue } }

        $trimmed = $line.Trim().Trim('|')
        $columns = @($trimmed -split '\|')
        if ($columns.Count -lt 2) { continue }

        $method = $columns[0].Trim()
        $meanText = $columns[1].Trim()
        if ([string]::IsNullOrWhiteSpace($method) -or [string]::IsNullOrWhiteSpace($meanText)) { continue }

        $parsedMean = Parse-MeanText -MeanText $meanText
        if ($null -eq $parsedMean) { continue }

        if ($parsedMean.Status -eq 'NA') {
            $rows[$method] = [pscustomobject]@{ Status = 'NA'; MeanUs = $null; SourceFile = $fileName }
        }
        else {
            $rows[$method] = [pscustomobject]@{ Status = 'OK'; MeanUs = [double]$parsedMean.MeanUs; SourceFile = $fileName }
        }
    }

    return [pscustomobject]@{
        Provider = $provider
        Engine   = $engine
        Rows     = $rows
        FileName = $fileName
    }
}

function Build-Cell {
    param([string]$FeatureId, [bool]$Supported, $Rows)

    if (-not $Supported) {
        return [pscustomobject]@{ Display = 'N/A'; Status = 'N/A'; MeanUs = $null }
    }

    if ($null -ne $Rows -and $Rows.ContainsKey($FeatureId)) {
        $row = $Rows[$FeatureId]
        if ($row.Status -eq 'NA') {
            return [pscustomobject]@{ Display = 'NA'; Status = 'NA'; MeanUs = $null }
        }
        return [pscustomobject]@{ Display = (Format-Us -Value ([double]$row.MeanUs)); Status = 'OK'; MeanUs = [double]$row.MeanUs }
    }

    return [pscustomobject]@{ Display = 'pending'; Status = 'pending'; MeanUs = $null }
}

function Build-DiffAndResult {
    param($AppCell, $OtherCell)

    if ($AppCell.Status -in @('N/A', 'NA') -or $OtherCell.Status -in @('N/A', 'NA')) {
        return [pscustomobject]@{ Diff = '-'; Result = '-' }
    }
    if ($AppCell.Status -ne 'OK' -or $OtherCell.Status -ne 'OK') {
        return [pscustomobject]@{ Diff = '-'; Result = '-' }
    }

    $diff = [double]$OtherCell.MeanUs - [double]$AppCell.MeanUs
    $result = if ($AppCell.MeanUs -le $OtherCell.MeanUs) { ':white_check_mark:' } else { ':x:' }
    return [pscustomobject]@{ Diff = (Format-Us -Value $diff); Result = $result }
}

$reportsRoot = Resolve-FullPath -PathValue $ReportsPath
$featureMap = Read-FeatureMap -PathValue $FeatureMapPath

$reportFiles = Get-ChildItem -LiteralPath $reportsRoot -File -Filter '*-report-github.md' -Recurse | Sort-Object Name
$parsedReports = New-Object System.Collections.Generic.List[object]
foreach ($file in $reportFiles) {
    $parsed = Parse-BenchmarkReport -PathValue $file.FullName
    if ($null -ne $parsed) { [void]$parsedReports.Add($parsed) }
}

$groupedByProvider = @{}
foreach ($report in $parsedReports) {
    if (-not $groupedByProvider.ContainsKey($report.Provider)) { $groupedByProvider[$report.Provider] = @{} }
    $groupedByProvider[$report.Provider][$report.Engine] = $report
}

$providersInOrder = New-Object System.Collections.Generic.List[string]
$seenProviders = @{}
if ($null -ne $featureMap -and $null -ne $featureMap.providers) {
    foreach ($provider in $featureMap.providers) {
        $id = [string]$provider.id
        if (-not $seenProviders.ContainsKey($id)) {
            [void]$providersInOrder.Add($id)
            $seenProviders[$id] = $true
        }
    }
}
foreach ($providerId in ($groupedByProvider.Keys | Sort-Object)) {
    if (-not $seenProviders.ContainsKey($providerId)) {
        [void]$providersInOrder.Add([string]$providerId)
        $seenProviders[[string]$providerId] = $true
    }
}

$lines = New-Object System.Collections.Generic.List[string]
[void]$lines.Add('# Performance matrix')
[void]$lines.Add('')
[void]$lines.Add('> All numeric values are normalized to **us**.')
[void]$lines.Add('> Diff = OtherEngine - DbSqlLikeMem.')
[void]$lines.Add('> :white_check_mark: means DbSqlLikeMem is faster. :x: means the external/native engine is faster.')
[void]$lines.Add('')

foreach ($providerId in $providersInOrder) {
    $providerReports = if ($groupedByProvider.ContainsKey($providerId)) { $groupedByProvider[$providerId] } else { @{} }
    $providerConfig = Get-ProviderConfig -FeatureMap $featureMap -ProviderId $providerId
    $providerTitle = if ($null -ne $providerConfig -and -not [string]::IsNullOrWhiteSpace([string]$providerConfig.displayName)) { [string]$providerConfig.displayName } else { $providerId }
    $appReport = if ($providerReports.ContainsKey('DbSqlLikeMem')) { $providerReports['DbSqlLikeMem'] } else { $null }

    $otherEngineName = $null
    foreach ($preferredEngine in @('Native', 'Testcontainers')) {
        if ($providerReports.ContainsKey($preferredEngine)) { $otherEngineName = $preferredEngine; break }
    }
    if (-not $otherEngineName) {
        foreach ($engineName in ($providerReports.Keys | Sort-Object)) {
            if ($engineName -ne 'DbSqlLikeMem') { $otherEngineName = $engineName; break }
        }
    }
    if (-not $otherEngineName -and $null -ne $providerConfig) {
        if ([string]$providerConfig.externalEngine -eq 'NativeAdoNet') { $otherEngineName = 'Native' }
        elseif ([string]$providerConfig.externalEngine -eq 'Testcontainers') { $otherEngineName = 'Testcontainers' }
        else { $otherEngineName = 'External' }
    }
    if (-not $otherEngineName) { $otherEngineName = 'External' }
    $otherReport = if ($providerReports.ContainsKey($otherEngineName)) { $providerReports[$otherEngineName] } else { $null }

    $discoveredFeatures = New-Object System.Collections.Generic.List[string]
    if ($null -ne $appReport) { foreach ($featureName in $appReport.Rows.Keys) { [void]$discoveredFeatures.Add([string]$featureName) } }
    if ($null -ne $otherReport) { foreach ($featureName in $otherReport.Rows.Keys) { [void]$discoveredFeatures.Add([string]$featureName) } }

    $featureOrder = Get-FeatureOrder -FeatureMap $featureMap -DiscoveredFeatures ($discoveredFeatures | Select-Object -Unique)
    $appSupported = if ($null -ne $providerConfig) { @($providerConfig.supportsMockFeatures) } else { $null }
    $otherSupported = if ($null -ne $providerConfig) { @($providerConfig.supportsExternalFeatures) } else { $null }

    [void]$lines.Add("## $providerTitle")
    [void]$lines.Add('')
    [void]$lines.Add("| Feature | DbSqlLikeMem | $otherEngineName | Diff | Result |")
    [void]$lines.Add('|---|---:|---:|---:|:---:|')

    foreach ($featureId in $featureOrder) {
        $supportsApp = Get-SupportedFeatureState -FeatureId $featureId -SupportedFeatures $appSupported
        $supportsOther = Get-SupportedFeatureState -FeatureId $featureId -SupportedFeatures $otherSupported
        $appRows = if ($null -ne $appReport) { $appReport.Rows } else { $null }
        $otherRows = if ($null -ne $otherReport) { $otherReport.Rows } else { $null }
        $appCell = Build-Cell -FeatureId $featureId -Supported $supportsApp -Rows $appRows
        $otherCell = Build-Cell -FeatureId $featureId -Supported $supportsOther -Rows $otherRows
        $comparison = Build-DiffAndResult -AppCell $appCell -OtherCell $otherCell
        [void]$lines.Add("| $featureId | $($appCell.Display) | $($otherCell.Display) | $($comparison.Diff) | $($comparison.Result) |")
    }

    [void]$lines.Add('')
}

$finalOutputPath = Resolve-FullPath -PathValue $OutputPath
$utf8Bom = New-Object System.Text.UTF8Encoding($true)
[System.IO.File]::WriteAllLines($finalOutputPath, $lines, $utf8Bom)
Write-Host "Wiki exported to: $finalOutputPath"
