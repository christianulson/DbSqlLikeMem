namespace DbSqlLikeMem.MySql;

internal sealed class MySqlDialect : SqlDialectBase
{
    internal const string DialectName = "mysql";

    internal MySqlDialect(
        int version
        ) : base(
        name: DialectName,
        version: version,
        keywords: ["REGEXP"],
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
            new KeyValuePair<string, SqlBinaryOp>("<=>", SqlBinaryOp.NullSafeEq),
        ],
        operators:
        [
            "<=>", "->>", "->",
            ">=", "<=", "<>", "!=", "==",
            "&&", "||"
        ])
    { }

 
    internal const int WithCteMinVersion = 8;
    internal const int MergeMinVersion = int.MaxValue;
    public override bool AllowsBacktickIdentifiers => true;
    public override bool AllowsDoubleQuoteIdentifiers => false; // keep tokenizer behavior: " as string
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.backtick;

    public override bool IsStringQuote(char ch) => ch is '\'' or '"';
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.backslash;

    public override bool SupportsHashLineComment => true;


    public override bool SupportsLimitOffset => true;
    public override bool SupportsOnDuplicateKeyUpdate => true;

    public override bool SupportsDeleteWithoutFrom => true;
    public override bool SupportsDeleteTargetAlias => true;

    public override bool SupportsWithCte => Version >= WithCteMinVersion;
    public override bool SupportsNullSafeEq => true;
    public override bool SupportsJsonArrowOperators => true;
}
