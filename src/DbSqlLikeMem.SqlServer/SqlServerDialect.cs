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
    internal const int JsonFunctionsMinVersion = 2016;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool AllowsBracketIdentifiers => true;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.bracket;

    /// <summary>
    /// EN: Summary for IsStringQuote.
    /// PT: Resumo para IsStringQuote.
    /// </summary>
    public override bool IsStringQuote(char ch) => ch == '\'';
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override StringComparison TextComparison => StringComparison.OrdinalIgnoreCase;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsTop => true;

    // OFFSET ... FETCH entrou no SQL Server 2012.
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsOffsetFetch => Version >= OffsetFetchMinVersion;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool RequiresOrderByForOffsetFetch => true;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsDeleteWithoutFrom => true; // DELETE [FROM] t
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsDeleteTargetAlias => true; // DELETE alias FROM t alias JOIN ...
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsWithCte => Version >= WithCteMinVersion;
    // SQL Server supports CTE but not the "WITH RECURSIVE" keyword form.
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsWithRecursive => false;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsJsonValueFunction => Version >= JsonFunctionsMinVersion;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsOpenJsonFunction => Version >= JsonFunctionsMinVersion;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsMerge => Version >= MergeMinVersion;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsPivotClause => true;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsSqlServerTableHints => true;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsSqlServerQueryHints => true;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override IReadOnlyCollection<string> NullSubstituteFunctionNames => ["ISNULL"];
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool ConcatReturnsNullOnNullInput => false;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool AllowsHashIdentifiers => true;

    /// <summary>
    /// EN: Summary for GetTemporaryTableScope.
    /// PT: Resumo para GetTemporaryTableScope.
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

    /// <summary>
    /// EN: Summary for SupportsDateAddFunction.
    /// PT: Resumo para SupportsDateAddFunction.
    /// </summary>
    public override bool SupportsDateAddFunction(string functionName)
        => functionName.Equals("DATEADD", StringComparison.OrdinalIgnoreCase);
}
