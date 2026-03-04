param(
    [string]$SrcDir = "src",
    [string]$Slnx = "src/DbSqlLikeMem.slnx"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $SrcDir -PathType Container)) {
    Write-Error "ERROR: src-dir not found or not a directory: $SrcDir"
    exit 2
}

if (-not (Test-Path -LiteralPath $Slnx -PathType Leaf)) {
    Write-Error "ERROR: slnx file not found: $Slnx"
    exit 2
}

$srcRoot = (Resolve-Path -LiteralPath $SrcDir).Path
$allProjects = Get-ChildItem -Path $SrcDir -Recurse -Filter *.csproj -File |
    ForEach-Object {
        $_.FullName.Substring($srcRoot.Length + 1).Replace('\', '/')
    } |
    Sort-Object -Unique

$slnxContent = Get-Content -LiteralPath $Slnx -Raw
$includedProjects = [regex]::Matches($slnxContent, 'Project Path="([^"]+\.csproj)"') |
    ForEach-Object {
        $_.Groups[1].Value.Replace('\', '/')
    } |
    Sort-Object -Unique

$missing = @($allProjects | Where-Object { $_ -notin $includedProjects })
$extra = @($includedProjects | Where-Object { $_ -notin $allProjects })

Write-Output "csproj_total=$($allProjects.Count) included_total=$($includedProjects.Count) missing=$($missing.Count) extra=$($extra.Count)"

if ($missing.Count -gt 0) {
    Write-Output ""
    Write-Output "Missing from .slnx:"
    foreach ($item in $missing) {
        Write-Output "  - $item"
    }
}

if ($extra.Count -gt 0) {
    Write-Output ""
    Write-Output "Referenced in .slnx but missing on disk:"
    foreach ($item in $extra) {
        Write-Output "  - $item"
    }
}

if ($missing.Count -eq 0 -and $extra.Count -eq 0) {
    exit 0
}

exit 1
