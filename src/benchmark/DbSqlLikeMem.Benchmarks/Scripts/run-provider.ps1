param(
    [Parameter(Mandatory = $true)]
    [string] $Provider,

    [Parameter(Mandatory = $true)]
    [ValidateSet('DbSqlLikeMem', 'Testcontainers', 'Native')]
    [string] $Engine
)

$engineFilter = switch ($Engine) {
    'DbSqlLikeMem' { 'DbSqlLikeMem' }
    'Testcontainers' { 'Testcontainers' }
    'Native' { 'Native' }
}

$filter = "*${Provider}_${engineFilter}_Benchmarks*"
dotnet run -c Release --filter $filter
