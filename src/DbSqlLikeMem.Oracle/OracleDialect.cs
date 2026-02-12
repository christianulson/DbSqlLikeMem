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
            ">=", "<=", "<>", "!=", "||"
        ])
    { }


    internal const int WithCteMinVersion = 9;
    internal const int MergeMinVersion = 9;
    internal const int OffsetFetchMinVersion = 12;
    internal const int FetchFirstMinVersion = 12;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsBracketIdentifiers => false;
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
    /// EN: Uses case-insensitive textual comparisons in the in-memory executor for deterministic tests.
    /// PT: Usa comparações textuais case-insensitive no executor em memória para testes determinísticos.
    /// </summary>
    public override StringComparison TextComparison => StringComparison.OrdinalIgnoreCase;

    /// <summary>
    /// EN: Enables implicit numeric/string comparison only when both values are numeric-convertible.
    /// PT: Habilita comparação implícita numérica/string apenas quando ambos os valores são conversíveis para número.
    /// </summary>
    public override bool SupportsImplicitNumericStringComparison => true;

    /// <summary>
    /// EN: Keeps LIKE case-insensitive by default in the mock provider.
    /// PT: Mantém LIKE case-insensitive por padrão no provider mock.
    /// </summary>
    public override bool LikeIsCaseInsensitive => true;


    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsTop => false;

    // OFFSET ... FETCH / FETCH FIRST entrou no Oracle 12c.
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsOffsetFetch => Version >= OffsetFetchMinVersion;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsFetchFirst => Version >= FetchFirstMinVersion;

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
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsMerge => Version >= MergeMinVersion;
    public override IReadOnlyCollection<string> NullSubstituteFunctionNames => ["NVL"];
    public override bool ConcatReturnsNullOnNullInput => false;

    public override bool IsIntegerCastTypeName(string typeName)
        => base.IsIntegerCastTypeName(typeName)
            || typeName.StartsWith("NUMBER", StringComparison.OrdinalIgnoreCase);

    public override bool SupportsDateAddFunction(string functionName)
        => false;
}
