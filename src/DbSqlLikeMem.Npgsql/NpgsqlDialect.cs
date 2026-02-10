namespace DbSqlLikeMem.Npgsql;

internal sealed class NpgsqlDialect : SqlDialectBase
{
    internal const string DialectName = "postgresql";

    internal NpgsqlDialect(
        int version
        ) : base(
        name: DialectName,
        version: version,
        keywords: ["ILIKE"],
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
            "->>", "->",
            "#>>", "#>",
            "::",
            ">=", "<=", "<>", "!=", "==",
            "&&", "||"
        ])
    { }


    internal const int WithCteMinVersion = 8;
    internal const int MergeMinVersion = 15;
    public override bool AllowsDoubleQuoteIdentifiers => true;
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.double_quote;

    public override bool IsStringQuote(char ch) => ch == '\'';
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;
    public override bool SupportsDollarQuotedStrings => true;

    public override bool SupportsLimitOffset => true;
    public override bool SupportsFetchFirst => true;
    public override bool SupportsOffsetFetch => true;
    public override bool SupportsReturning => true;

    public override bool SupportsDeleteWithoutFrom => false;
    public override bool SupportsDeleteTargetAlias => true;

    public override bool SupportsJsonArrowOperators => true;
    public override bool SupportsWithCte => Version >= WithCteMinVersion;
    public override bool SupportsMerge => Version >= MergeMinVersion;

    public override TemporaryTableScope GetTemporaryTableScope(string tableName, string? schemaName)
    {
        _ = tableName;
        if (string.IsNullOrWhiteSpace(schemaName)) return TemporaryTableScope.None;
        return schemaName.StartsWith("pg_temp", StringComparison.OrdinalIgnoreCase)
            ? TemporaryTableScope.Connection
            : TemporaryTableScope.None;
    }
}
