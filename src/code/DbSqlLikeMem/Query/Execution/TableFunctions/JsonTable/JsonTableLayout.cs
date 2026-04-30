namespace DbSqlLikeMem;

internal sealed record JsonTableClauseLayout(
    IReadOnlyList<JsonTableColumnLayout> Columns,
    IReadOnlyList<JsonTableNestedPathLayout> NestedPaths,
    IReadOnlyList<JsonTableColumnLayout> AllColumns,
    IReadOnlyList<int> AllOrdinals);

internal sealed record JsonTableColumnLayout(
    int Ordinal,
    SqlJsonTableColumn Column);

internal sealed record JsonTableNestedPathLayout(
    SqlJsonTableNestedPath NestedPath,
    JsonTableClauseLayout Layout);
