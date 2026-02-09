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
            ">=", "<=", "<>", "!=", "==",
            "&&", "||"
        ])
    { }


    internal const int WithCteMinVersion = 2005;
    internal const int MergeMinVersion = 2008;
    internal const int OffsetFetchMinVersion = 2012;

    public override bool AllowsBracketIdentifiers => true;
    public override bool AllowsDoubleQuoteIdentifiers => true;
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.bracket;

    public override bool IsStringQuote(char ch) => ch == '\'';
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;

    public override bool SupportsTop => true;

    // OFFSET ... FETCH entrou no SQL Server 2012.
    public override bool SupportsOffsetFetch => Version >= OffsetFetchMinVersion;
    public override bool SupportsFetchFirst => false;

    public override bool SupportsDeleteWithoutFrom => true; // DELETE [FROM] t
    public override bool SupportsDeleteTargetAlias => true; // DELETE alias FROM t alias JOIN ...
    public override bool SupportsWithCte => Version >= WithCteMinVersion;
    public override bool SupportsMerge => Version >= MergeMinVersion;
}
