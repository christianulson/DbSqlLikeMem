namespace DbSqlLikeMem.Db2;

internal sealed class Db2Dialect : SqlDialectBase
{
    internal const string DialectName = "db2";

    internal Db2Dialect(
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

 
    internal const int WithCteMinVersion = 8;
    internal const int MergeMinVersion = 9;
    
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsBacktickIdentifiers => false;
    
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsDoubleQuoteIdentifiers => true;
    
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.double_quote;

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
    public override bool SupportsHashLineComment => false;


    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsLimitOffset => false;
    public override bool SupportsFetchFirst => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsOnDuplicateKeyUpdate => false;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsDeleteWithoutFrom => false;
    
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsDeleteTargetAlias => false;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsWithCte => Version >= WithCteMinVersion;
    public override bool SupportsWithRecursive => false;
    public override bool SupportsWithMaterializedHint => false;
    public override bool SupportsOnConflictClause => false;
    public override bool SupportsMerge => Version >= MergeMinVersion;
    
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsNullSafeEq => false;
    
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsJsonArrowOperators => false;
}
