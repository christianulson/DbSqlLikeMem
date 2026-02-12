namespace DbSqlLikeMem.SqlServer;

internal sealed class SqlServerDialect : SqlDialectBase
{
    internal const string DialectName = "sqlserver";
    internal SqlServerDialect(
        int version
        ) : base(
        name: DialectName,
        version: version,
        keywords: [],
        binOps:
        [
            new KeyValuePair<string, SqlBinaryOp>("AND", SqlBinaryOp.And),
            new KeyValuePair<string, SqlBinaryOp>("OR", SqlBinaryOp.Or),
            new KeyValuePair<string, SqlBinaryOp>("=", SqlBinaryOp.Eq),
            new KeyValuePair<string, SqlBinaryOp>("<>", SqlBinaryOp.Neq),
            new KeyValuePair<string, SqlBinaryOp>("!=", SqlBinaryOp.Neq),
            new KeyValuePair<string, SqlBinaryOp>(">", SqlBinaryOp.Greater),
            new KeyValuePair<string, SqlBinaryOp>(">=", SqlBinaryOp.GreaterOrEqual),
            new KeyValuePair<string, SqlBinaryOp>("<", SqlBinaryOp.Less),
            new KeyValuePair<string, SqlBinaryOp>("<=", SqlBinaryOp.LessOrEqual),
        ],
        operators:
        [
            ">=", "<=", "<>", "!="
        ])
    { }


    internal const int WithCteMinVersion = 2005;
    internal const int MergeMinVersion = 2008;
    internal const int OffsetFetchMinVersion = 2012;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsBracketIdentifiers => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsDoubleQuoteIdentifiers => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.bracket;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool IsStringQuote(char ch) => ch == '\'';
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsTop => true;

    // OFFSET ... FETCH entrou no SQL Server 2012.
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsOffsetFetch => Version >= OffsetFetchMinVersion;
    public override bool RequiresOrderByForOffsetFetch => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsFetchFirst => false;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsDeleteWithoutFrom => true; // DELETE [FROM] t
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsDeleteTargetAlias => true; // DELETE alias FROM t alias JOIN ...
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsWithCte => Version >= WithCteMinVersion;
    public override bool SupportsWithRecursive => Version >= WithCteMinVersion;
    public override bool SupportsWithMaterializedHint => false;
    public override bool SupportsOnConflictClause => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsMerge => Version >= MergeMinVersion;
    public override bool SupportsSqlServerTableHints => true;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsHashIdentifiers => true;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override TemporaryTableScope GetTemporaryTableScope(string tableName, string? schemaName)
    {
        _ = schemaName;
        if (string.IsNullOrWhiteSpace(tableName)) return TemporaryTableScope.None;
        if (tableName.StartsWith("##", StringComparison.Ordinal))
            return TemporaryTableScope.Global;
        if (tableName.StartsWith("#", StringComparison.Ordinal))
            return TemporaryTableScope.Connection;
        return TemporaryTableScope.None;
    }
}
