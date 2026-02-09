namespace DbSqlLikeMem.Oracle;

internal sealed class OracleDialect : SqlDialectBase
{
    internal const string DialectName = "oracle";
    internal OracleDialect(
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


    internal const int WithCteMinVersion = 9;
    internal const int MergeMinVersion = 9;
    internal const int OffsetFetchMinVersion = 12;
    internal const int FetchFirstMinVersion = 12;
    public override bool AllowsBracketIdentifiers => true;
    public override bool AllowsDoubleQuoteIdentifiers => true;
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.bracket;

    public override bool IsStringQuote(char ch) => ch == '\'';
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;

    public override bool SupportsTop => true;

    // OFFSET ... FETCH / FETCH FIRST entrou no Oracle 12c.
    public override bool SupportsOffsetFetch => Version >= OffsetFetchMinVersion;
    public override bool SupportsFetchFirst => Version >= FetchFirstMinVersion;

    public override bool SupportsDeleteWithoutFrom => false;
    public override bool SupportsDeleteTargetAlias => false;
    public override bool SupportsWithCte => Version >= WithCteMinVersion;
    public override bool SupportsMerge => Version >= MergeMinVersion;
}
