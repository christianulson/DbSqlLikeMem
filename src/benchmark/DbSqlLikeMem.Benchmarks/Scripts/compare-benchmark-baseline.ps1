param(
    [Parameter(Mandatory = $true)]
    [string] $CurrentReportFile,

    [Parameter(Mandatory = $true)]
    [string] $BaselineFile,

    [string] $OutFile = "../../../docs/Wiki/BenchmarkResults/benchmark-regression-summary.md",

    [double] $ThresholdPercent = 5.0
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

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

$baseline = Get-Content $BaselineFile -Raw | ConvertFrom-Json
if (-not ($baseline.PSObject.Properties.Name -contains 'providerId')) {
    throw "Baseline file is missing providerId: $BaselineFile"
}

if (-not ($baseline.PSObject.Properties.Name -contains 'engine')) {
    throw "Baseline file is missing engine: $BaselineFile"
}

if (-not ($baseline.PSObject.Properties.Name -contains 'profile')) {
    throw "Baseline file is missing profile: $BaselineFile"
}

if (-not ($baseline.PSObject.Properties.Name -contains 'entries')) {
    throw "Baseline file is missing entries: $BaselineFile"
}

$current = Parse-BenchmarkReport -PathValue $CurrentReportFile
$entries = @($baseline.entries)

$rows = New-Object System.Collections.Generic.List[object]
$improved = 0
$regressed = 0
$stable = 0
$missing = 0

foreach ($entry in $entries) {
    $methodName = [string]$entry.methodName
    if ([string]::IsNullOrWhiteSpace($methodName)) {
        continue
    }

    $baselineValue = if ($entry.PSObject.Properties.Name -contains 'meanMicroseconds') { [double]$entry.meanMicroseconds } else { $null }
    $currentValue = $null
    $currentEntry = $null
    if ($current.ContainsKey($methodName)) {
        $currentEntry = $current[$methodName]
        $currentValue = $currentEntry.Microseconds
    }

    $delta = $null
    $deltaPercent = $null
    $verdict = 'Missing'
    if ($null -ne $baselineValue -and $null -ne $currentValue) {
        $delta = $currentValue - $baselineValue
        if ($baselineValue -ne 0) {
            $deltaPercent = ($delta / $baselineValue) * 100.0
        }

        if ($deltaPercent -gt $ThresholdPercent) {
            $verdict = 'Regressed'
            $regressed++
        }
        elseif ($deltaPercent -lt (-1.0 * $ThresholdPercent)) {
            $verdict = 'Improved'
            $improved++
        }
        else {
            $verdict = 'Stable'
            $stable++
        }
    }
    else {
        $missing++
    }

    $rows.Add([pscustomobject]@{
        MethodName = $methodName
        Baseline = $baselineValue
        Current = $currentValue
        Delta = $delta
        DeltaPercent = $deltaPercent
        Verdict = $verdict
    }) | Out-Null
}

$summary = New-Object System.Collections.Generic.List[string]
$summary.Add("# Benchmark Regression Summary") | Out-Null
$summary.Add('') | Out-Null
$summary.Add("> Baseline provider: $([string]$baseline.providerId)") | Out-Null
$summary.Add("> Baseline engine: $([string]$baseline.engine)") | Out-Null
$summary.Add("> Baseline profile: $([string]$baseline.profile)") | Out-Null
$summary.Add("> Threshold: $ThresholdPercent%") | Out-Null
$summary.Add('') | Out-Null
$summary.Add("## Totals") | Out-Null
$summary.Add('') | Out-Null
$summary.Add("| Improved | Stable | Regressed | Missing |") | Out-Null
$summary.Add("| --- | --- | --- | --- |") | Out-Null
$summary.Add("| $improved | $stable | $regressed | $missing |") | Out-Null
$summary.Add('') | Out-Null
$summary.Add("## Details") | Out-Null
$summary.Add('') | Out-Null
$summary.Add("| Method | Baseline (us) | Current (us) | Delta (us) | Delta (%) | Verdict |") | Out-Null
$summary.Add("| --- | --- | --- | --- | --- | --- |") | Out-Null

foreach ($row in $rows) {
    $baselineText = if ($null -ne $row.Baseline) { ([double]$row.Baseline).ToString('0.###', [System.Globalization.CultureInfo]::InvariantCulture) } else { '-' }
    $currentText = if ($null -ne $row.Current) { ([double]$row.Current).ToString('0.###', [System.Globalization.CultureInfo]::InvariantCulture) } else { '-' }
    $deltaText = if ($null -ne $row.Delta) { ([double]$row.Delta).ToString('0.###', [System.Globalization.CultureInfo]::InvariantCulture) } else { '-' }
    $percentText = if ($null -ne $row.DeltaPercent) { ([double]$row.DeltaPercent).ToString('0.##', [System.Globalization.CultureInfo]::InvariantCulture) + '%' } else { '-' }

    $summary.Add("| $($row.MethodName) | $baselineText | $currentText | $deltaText | $percentText | $($row.Verdict) |") | Out-Null
}

$outputFullPath = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot $OutFile))
$outputDir = Split-Path -Parent $outputFullPath
[System.IO.Directory]::CreateDirectory($outputDir) | Out-Null

$text = $summary -join [Environment]::NewLine
[System.IO.File]::WriteAllText($outputFullPath, $text)
