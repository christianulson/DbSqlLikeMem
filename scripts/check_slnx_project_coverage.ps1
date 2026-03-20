param(
    [string]$SrcDir = "src",
    [string]$Slnx = "src/DbSqlLikeMem.slnx"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$allowedRootLevelProjectStems = @(
    "DbSqlLikeMem",
    "DbSqlLikeMem.CsvReader",
    "DbSqlLikeMem.Dapper.TestTools",
    "DbSqlLikeMem.EfCore",
    "DbSqlLikeMem.EfCore.TestTools",
    "DbSqlLikeMem.LinqToDb",
    "DbSqlLikeMem.LinqToDb.TestTools",
    "DbSqlLikeMem.MiniProfiler.TestTools",
    "DbSqlLikeMem.NHibernate.TestTools",
    "DbSqlLikeMem.Test",
    "DbSqlLikeMem.TestTools",
    "DbSqlLikeMem.XUnit"
)

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

$rootLevelProjects = Get-ChildItem -Path $SrcDir -Recurse -Filter *.csproj -File |
    Where-Object { $_.Directory.FullName -eq $srcRoot } |
    ForEach-Object { $_.Name } |
    Sort-Object -Unique

$slnxContent = Get-Content -LiteralPath $Slnx -Raw
$includedProjects = [regex]::Matches($slnxContent, 'Project Path="([^"]+\.csproj)"') |
    ForEach-Object {
        $_.Groups[1].Value.Replace('\', '/')
    } |
    Sort-Object -Unique

$missing = @($allProjects | Where-Object { $_ -notin $includedProjects })
$extra = @($includedProjects | Where-Object { $_ -notin $allProjects })
$organizationDrift = @(
    $rootLevelProjects | Where-Object {
        $stem = [System.IO.Path]::GetFileNameWithoutExtension($_)
        $stem -notin $allowedRootLevelProjectStems
    }
)

Write-Output "csproj_total=$($allProjects.Count) included_total=$($includedProjects.Count) missing=$($missing.Count) extra=$($extra.Count) root_level_unbalanced=$($organizationDrift.Count)"

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

if ($organizationDrift.Count -gt 0) {
    Write-Output ""
    Write-Output "Root-level projects outside the shared organization buckets:"
    foreach ($item in $organizationDrift) {
        Write-Output "  - $item"
    }
}

if ($missing.Count -eq 0 -and $extra.Count -eq 0 -and $organizationDrift.Count -eq 0) {
    exit 0
}

exit 1
