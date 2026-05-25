param(
    [string] $ArtifactsDir = "../../../docs/Wiki/BenchmarkResults/results",
    [string] $ComparativeOutFile = "../../../docs/Wiki/performance-matrix.md",
    [string] $AppSpecificOutFile = "../../../docs/Wiki/performance-matrix-app-specific.md",
    [string] $SingleTableOutFile = "../../../docs/Wiki/performance-matrix-app-specific.single-table.md",
    [switch] $IncludeLegacySingleTable
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$scriptDir = $PSScriptRoot

Push-Location $scriptDir
try {
    & (Join-Path $scriptDir 'export-wiki.ps1') `
        -ArtifactsDir $ArtifactsDir `
        -OutFile $ComparativeOutFile

    & (Join-Path $scriptDir 'export-wiki-app-specific.ps1') `
        -ArtifactsDir $ArtifactsDir `
        -OutFile $AppSpecificOutFile

    if ($IncludeLegacySingleTable) {
        & (Join-Path $scriptDir 'export-wiki-app-specific.single-table.ps1') `
            -ArtifactsDir $ArtifactsDir `
            -OutFile $SingleTableOutFile
    }
}
finally {
    Pop-Location
}
