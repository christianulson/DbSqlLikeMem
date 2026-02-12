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
            new KeyValuePair<string, SqlBinaryOp>("<=>", SqlBinaryOp.NullSafeEq),
        ],
        operators:
        [
            "<=>", ">=", "<=", "<>", "!="
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
    public override bool SupportsWithRecursive => Version >= WithCteMinVersion;
    public override bool SupportsWithMaterializedHint => false;
    public override bool SupportsOnConflictClause => false;
    public override bool SupportsMerge => Version >= MergeMinVersion;
    
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsNullSafeEq => true;
    
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsJsonArrowOperators => false;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsParserCrossDialectQuotedIdentifiers => false;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsParserCrossDialectJsonOperators => false;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsParserInsertSelectUpsertSuffix => false;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsParserDeleteWithoutFromCompatibility => false;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsParserLimitOffsetCompatibility => true;

    /// <summary>
    /// EN: Mock rule: DB2 text comparisons are case-insensitive by default unless explicit collation is introduced.
    /// PT: Regra do mock: comparações textuais DB2 são case-insensitive por padrão até existir collation explícita.
    /// </summary>
    public override StringComparison TextComparison => StringComparison.OrdinalIgnoreCase;

    /// <summary>
    /// EN: Mock rule: allow numeric-vs-numeric-string implicit comparisons (e.g. id = '2').
    /// PT: Regra do mock: permite comparação implícita número-vs-string-numérica (ex.: id = '2').
    /// </summary>
    public override bool SupportsImplicitNumericStringComparison => true;

    /// <summary>
    /// EN: Mock LIKE behavior follows dialect default and is case-insensitive.
    /// PT: Comportamento de LIKE no mock segue o padrão do dialeto e é case-insensitive.
    /// </summary>
    public override bool LikeIsCaseInsensitive => true;
}
