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
            "<=>",
            "->>", "->",
            ">=", "<=", "<>", "!=", "==",
            "&&", "||"
        ])
    { }

 
    internal const int WithCteMinVersion = 3;
    internal const int MergeMinVersion = int.MaxValue;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool AllowsBacktickIdentifiers => true;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.double_quote;

    /// <summary>
    /// EN: Summary for IsStringQuote.
    /// PT: Resumo para IsStringQuote.
    /// </summary>
    public override bool IsStringQuote(char ch) => ch is '\'' or '"';
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsHashLineComment => true;


    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsLimitOffset => true;
    /// <summary>
    /// EN: Enables OFFSET/FETCH compatibility syntax for shared smoke tests.
    /// PT: Habilita sintaxe de compatibilidade OFFSET/FETCH para testes smoke compartilhados.
    /// </summary>
    public override bool SupportsOffsetFetch => true;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsOnDuplicateKeyUpdate => true;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsOnConflictClause => true;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsOrderByNullsModifier => true;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsDeleteTargetAlias => false;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsWithCte => Version >= WithCteMinVersion;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsWithRecursive => Version >= WithCteMinVersion;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsWithMaterializedHint => true;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsNullSafeEq => true;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override IReadOnlyCollection<string> NullSubstituteFunctionNames => ["IFNULL"];
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool ConcatReturnsNullOnNullInput => false;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsJsonArrowOperators => true;
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsJsonExtractFunction => true;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override StringComparison TextComparison => StringComparison.OrdinalIgnoreCase;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool SupportsImplicitNumericStringComparison => true;

    /// <summary>
    /// EN: Summary for SupportsDateAddFunction.
    /// PT: Resumo para SupportsDateAddFunction.
    /// </summary>
    public override bool SupportsDateAddFunction(string functionName)
        => functionName.Equals("DATE_ADD", StringComparison.OrdinalIgnoreCase);
}
