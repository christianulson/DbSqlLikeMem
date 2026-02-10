namespace DbSqlLikeMem.Sqlite;

internal sealed class SqliteDialect : SqlDialectBase
{
    internal const string DialectName = "sqlite";

    internal SqliteDialect(
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
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsBacktickIdentifiers => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsDoubleQuoteIdentifiers => false; // keep tokenizer behavior: " as string
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.backtick;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool IsStringQuote(char ch) => ch is '\'' or '"';
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.backslash;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsHashLineComment => true;


    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsLimitOffset => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsOnDuplicateKeyUpdate => true;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsDeleteWithoutFrom => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsDeleteTargetAlias => true;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsWithCte => Version >= WithCteMinVersion;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsNullSafeEq => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsJsonArrowOperators => true;
}
