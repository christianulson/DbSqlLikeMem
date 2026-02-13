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
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.double_quote;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool IsStringQuote(char ch) => ch == '\'';
    
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;

    public override bool SupportsFetchFirst => true;
    
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsDeleteTargetAlias => false;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsWithCte => Version >= WithCteMinVersion;
    public override bool SupportsWithRecursive => Version >= WithCteMinVersion;
    public override bool SupportsMerge => Version >= MergeMinVersion;
    
    public override IReadOnlyCollection<string> NullSubstituteFunctionNames => ["IFNULL"];

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsParserLimitOffsetCompatibility => true;

    /// <summary>
    /// EN: Mock rule: DB2 text comparisons are case-insensitive by default unless explicit collation is introduced.
    /// PT: Regra do mock: comparações textuais DB2 são case-insensitive por padrão até existir collation explícita.
    /// </summary>
    public override StringComparison TextComparison => StringComparison.OrdinalIgnoreCase;

    public override bool SupportsDateAddFunction(string functionName)
        => functionName.Equals("DATE_ADD", StringComparison.OrdinalIgnoreCase)
        || functionName.Equals("TIMESTAMPADD", StringComparison.OrdinalIgnoreCase);
}
