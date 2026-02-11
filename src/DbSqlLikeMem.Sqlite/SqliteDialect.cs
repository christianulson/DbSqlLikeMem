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
        ],
        operators:
        [
            "->>", "->",
            ">=", "<=", "<>", "!=", "==",
            "&&", "||"
        ])
    { }

 
    internal const int WithCteMinVersion = 3;
    internal const int MergeMinVersion = int.MaxValue;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsBacktickIdentifiers => true;
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
    public override bool IsStringQuote(char ch) => ch is '\'' or '"';
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;

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
    public override bool SupportsOnDuplicateKeyUpdate => false;
    public override bool SupportsOnConflictClause => true;

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
    public override bool SupportsWithMaterializedHint => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsNullSafeEq => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsJsonArrowOperators => true;

    /// <summary>
    /// EN: Mock rule: SQLite text comparisons are case-insensitive by default in this project.
    /// PT: Regra do mock: comparações textuais de SQLite são case-insensitive por padrão neste projeto.
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
