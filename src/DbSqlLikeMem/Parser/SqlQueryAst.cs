namespace DbSqlLikeMem;
internal abstract record SqlQueryBase
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public string RawSql { get; init; } = "";

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public SqlTableSource? Table { get; init; }
}

internal sealed record SqlSelectQuery(
    IReadOnlyList<SqlCte> Ctes,
    bool Distinct,
    IReadOnlyList<SqlSelectItem> SelectItems,
    IReadOnlyList<SqlJoin> Joins,
    SqlExpr? Where,
    IReadOnlyList<SqlOrderByItem> OrderBy,
    SqlRowLimit? RowLimit,
    IReadOnlyList<string> GroupBy,
    SqlExpr? Having
) : SqlQueryBase;

internal sealed record SqlUnionQuery(
    IReadOnlyList<SqlSelectQuery> Parts,
    IReadOnlyList<bool> AllFlags,
    IReadOnlyList<SqlOrderByItem> OrderBy,
    SqlRowLimit? RowLimit
) : SqlQueryBase;

internal sealed record SqlInsertQuery : SqlQueryBase
{
    internal IReadOnlyList<string> Columns { get; init; } = [];              // pode ser vazio (INSERT INTO t VALUES...)
    internal IReadOnlyList<List<string>> ValuesRaw { get; init; } = [];      // tokens raw por valor (ou expressão raw)
    internal IReadOnlyList<List<SqlExpr?>> ValuesExpr { get; init; } = [];   // best-effort parsed values (aligned with ValuesRaw)
    internal bool HasOnDuplicateKeyUpdate { get; init; }
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public IReadOnlyList<(string Col, string ExprRaw)> OnDupAssigns { get; init; } = [];
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public IReadOnlyList<SqlAssignment> OnDupAssignsParsed { get; init; } = [];
    internal SqlSelectQuery? InsertSelect { get; init; }               // INSERT INTO t (...) SELECT ...
}

internal sealed record SqlUpdateQuery : SqlQueryBase
{
    internal List<(string Col, string ExprRaw)> Set { get; init; } = [];
    internal IReadOnlyList<SqlAssignment> SetParsed { get; init; } = [];
    internal SqlExpr? Where { get; init; }
    internal string? WhereRaw { get; init; }                           // ou SqlExpr se você já tem
    internal SqlSelectQuery? UpdateFromSelect { get; init; }           // se você quiser UPDATE ... JOIN (SELECT..)
}

internal sealed record SqlDeleteQuery : SqlQueryBase
{
    internal string? WhereRaw { get; init; }
    internal SqlExpr? Where { get; init; }
    internal SqlSelectQuery? DeleteFromSelect { get; init; }           // DELETE ... USING (SELECT..)
}


internal sealed record SqlCreateTemporaryTableQuery : SqlQueryBase
{
    internal bool Temporary { get; init; } = true;
    internal TemporaryTableScope Scope { get; init; } = TemporaryTableScope.Connection;
    internal bool IfNotExists { get; init; }
    internal IReadOnlyList<string> ColumnNames { get; init; } = [];
    internal SqlSelectQuery AsSelect { get; init; } = null!;
}

internal enum TemporaryTableScope
{
    None,
    Connection,
    Global
}

internal sealed record SqlCreateViewQuery : SqlQueryBase
{
    internal bool OrReplace { get; init; }
    // MySQL doesn't support IF NOT EXISTS for VIEW, but accepting it keeps the mock flexible.
    internal bool IfNotExists { get; init; }
    internal IReadOnlyList<string> ColumnNames { get; init; } = [];
    internal SqlSelectQuery Select { get; init; } = null!;
}

internal sealed record SqlDropViewQuery : SqlQueryBase
{
    internal bool IfExists { get; init; }
}

internal sealed record SqlMergeQuery : SqlQueryBase
{
    internal SqlTableSource? Source { get; init; } // opcional (pode deixar null por enquanto)
}

internal sealed record SqlSelectItem(string Raw, string? Alias);

internal sealed record SqlTableSource(
    string? DbName,
    string? Name,
    string? Alias,
    SqlSelectQuery? Derived,
    SqlQueryParser.UnionChain? DerivedUnion,
    string? DerivedSql,
    SqlPivotSpec? Pivot
);

internal sealed record SqlPivotSpec(
    string AggregateFunction,
    string AggregateArgRaw,
    string ForColumnRaw,
    IReadOnlyList<SqlPivotInItem> InItems
);

internal sealed record SqlPivotInItem(
    string ValueRaw,
    string Alias
);

/// <summary>
/// EN: Join types represented in the SQL AST.
/// PT: Tipos de join representados na AST SQL.
/// </summary>
internal enum SqlJoinType
{
    Inner,
    Left,
    Right,
    Cross,
    //Full
}

internal sealed record SqlJoin(SqlJoinType Type, SqlTableSource Table, SqlExpr On);

internal sealed record SqlOrderByItem(string Raw, bool Desc, bool? NullsFirst = null);

internal abstract record SqlRowLimit;
internal sealed record SqlLimitOffset(int Count, int? Offset) : SqlRowLimit;
internal sealed record SqlTop(int Count) : SqlRowLimit;
internal sealed record SqlFetch(int Count, int? Offset) : SqlRowLimit;

internal sealed record SqlCte(string Name, SqlSelectQuery Query);

internal sealed record SqlOnDuplicateKeyUpdate(
    IReadOnlyList<SqlAssignment> Assignments
);

internal sealed record SqlAssignment(string Column, string ValueRaw, SqlExpr? ValueExpr = null);
