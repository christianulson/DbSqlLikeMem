namespace DbSqlLikeMem;

/// <summary>
/// EN: Provides the first compatibility slice of the automatic SQL dialect mode.
/// PT: Fornece a primeira fatia de compatibilidade do modo automatico de dialeto SQL.
/// </summary>
internal sealed class AutoSqlDialect : SqlDialectBase
{
    internal const string DialectName = "auto";
    private static readonly IReadOnlyDictionary<string, SqlTemporalFunctionKind> _temporalFunctionNames =
        new Dictionary<string, SqlTemporalFunctionKind>(StringComparer.OrdinalIgnoreCase)
        {
            ["CURRENT_DATE"] = SqlTemporalFunctionKind.Date,
            ["CURRENT_TIME"] = SqlTemporalFunctionKind.Time,
            ["CURRENT_TIMESTAMP"] = SqlTemporalFunctionKind.DateTime,
            ["NOW"] = SqlTemporalFunctionKind.DateTime,
            ["SYSDATE"] = SqlTemporalFunctionKind.DateTime,
            ["SYSTEMDATE"] = SqlTemporalFunctionKind.DateTime,
            ["GETDATE"] = SqlTemporalFunctionKind.DateTime,
            ["SYSDATETIME"] = SqlTemporalFunctionKind.DateTime,
            ["SYSTIMESTAMP"] = SqlTemporalFunctionKind.DateTime,
        };

    private static readonly KeyValuePair<string, SqlBinaryOp>[] _binaryOperators =
    [
        new("AND", SqlBinaryOp.And),
        new("OR", SqlBinaryOp.Or),
        new("=", SqlBinaryOp.Eq),
        new("<>", SqlBinaryOp.Neq),
        new("!=", SqlBinaryOp.Neq),
        new(">", SqlBinaryOp.Greater),
        new(">=", SqlBinaryOp.GreaterOrEqual),
        new("<", SqlBinaryOp.Less),
        new("<=", SqlBinaryOp.LessOrEqual),
    ];

    /// <summary>
    /// EN: Initializes the automatic dialect with a compatibility-oriented default version.
    /// PT: Inicializa o dialeto automatico com uma versao padrao orientada a compatibilidade.
    /// </summary>
    /// <param name="version">EN: Compatibility version marker used by the parser cache key. PT: Marcador de versao de compatibilidade usado pela chave de cache do parser.</param>
    internal AutoSqlDialect(int version = 1)
        : base(
            name: DialectName,
            version: version,
            keywords: [],
            binOps: _binaryOperators,
            operators: [">=", "<=", "<>", "!="])
    {
    }

    /// <summary>
    /// EN: Accepts backtick-quoted identifiers to cover MySQL-family syntax in Auto mode.
    /// PT: Aceita identificadores entre crases para cobrir sintaxe da familia MySQL no modo Auto.
    /// </summary>
    public override bool AllowsBacktickIdentifiers => true;

    /// <summary>
    /// EN: Accepts bracket-quoted identifiers to cover SQL Server-family syntax in Auto mode.
    /// PT: Aceita identificadores entre colchetes para cobrir sintaxe da familia SQL Server no modo Auto.
    /// </summary>
    public override bool AllowsBracketIdentifiers => true;

    /// <summary>
    /// EN: Accepts cross-dialect quoted identifiers while the automatic detector keeps evolving.
    /// PT: Aceita identificadores com quoting cross-dialect enquanto o detector automatico evolui.
    /// </summary>
    public override bool AllowsParserCrossDialectQuotedIdentifiers => true;

    /// <summary>
    /// EN: Enables LIMIT/OFFSET parsing for MySQL/PostgreSQL-style pagination in Auto mode.
    /// PT: Habilita parsing de LIMIT/OFFSET para paginacao no estilo MySQL/PostgreSQL no modo Auto.
    /// </summary>
    public override bool SupportsLimitOffset => true;

    /// <summary>
    /// EN: Enables FETCH FIRST/NEXT parsing for ANSI-style pagination in Auto mode.
    /// PT: Habilita parsing de FETCH FIRST/NEXT para paginacao no estilo ANSI no modo Auto.
    /// </summary>
    public override bool SupportsFetchFirst => true;

    /// <summary>
    /// EN: Enables TOP parsing for SQL Server-style pagination in Auto mode.
    /// PT: Habilita parsing de TOP para paginacao no estilo SQL Server no modo Auto.
    /// </summary>
    public override bool SupportsTop => true;

    /// <summary>
    /// EN: Enables OFFSET/FETCH parsing for dialects that expose the two-clause pagination tail.
    /// PT: Habilita parsing de OFFSET/FETCH para dialetos que expõem a cauda de paginacao em duas clausulas.
    /// </summary>
    public override bool SupportsOffsetFetch => true;

    /// <inheritdoc />
    public override bool SupportsSequenceDdl => true;

    /// <inheritdoc />
    public override bool SupportsNextValueForSequenceExpression => true;

    /// <inheritdoc />
    public override bool SupportsPreviousValueForSequenceExpression => true;

    /// <inheritdoc />
    public override bool SupportsSequenceDotValueExpression(string suffix)
        => suffix.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase)
            || suffix.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsSequenceFunctionCall(string functionName)
        => functionName.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SETVAL", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LASTVAL", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsJsonArrowOperators => true;

    /// <inheritdoc />
    public override bool AllowsParserCrossDialectJsonOperators => true;

    /// <inheritdoc />
    public override bool SupportsJsonExtractFunction => true;

    /// <inheritdoc />
    public override bool SupportsJsonValueFunction => true;

    /// <inheritdoc />
    public override bool SupportsJsonValueReturningClause => true;

    /// <inheritdoc />
    public override IReadOnlyDictionary<string, SqlTemporalFunctionKind> TemporalFunctionNames => _temporalFunctionNames;

    /// <inheritdoc />
    public override IReadOnlyCollection<string> TemporalFunctionIdentifierNames
        => ["CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "SYSTEMDATE", "SYSDATE"];

    /// <inheritdoc />
    public override IReadOnlyCollection<string> TemporalFunctionCallNames
        => ["NOW", "GETDATE", "SYSDATETIME", "SYSTIMESTAMP"];

    /// <inheritdoc />
    public override bool SupportsDateAddFunction(string functionName)
        => functionName.Equals("DATE_ADD", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DATEADD", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("TIMESTAMPADD", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsWithinGroupForStringAggregates => true;

    /// <inheritdoc />
    public override bool SupportsAggregateOrderByForStringAggregates => true;

    /// <inheritdoc />
    public override bool SupportsAggregateSeparatorKeywordForStringAggregates => true;

    /// <inheritdoc />
    public override bool SupportsStringAggregateFunction(string functionName)
        => functionName.Equals("GROUP_CONCAT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("STRING_AGG", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LISTAGG", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsWithinGroupStringAggregateFunction(string functionName)
        => functionName.Equals("GROUP_CONCAT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("STRING_AGG", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LISTAGG", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsAggregateOrderByStringAggregateFunction(string functionName)
        => functionName.Equals("GROUP_CONCAT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("STRING_AGG", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LISTAGG", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsAggregateSeparatorKeywordStringAggregateFunction(string functionName)
        => functionName.Equals("GROUP_CONCAT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("STRING_AGG", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LISTAGG", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsLastFoundRowsFunction(string functionName)
        => functionName.Equals("FOUND_ROWS", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ROW_COUNT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CHANGES", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ROWCOUNT", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsLastFoundRowsIdentifier(string identifier)
        => identifier.Equals("@@ROWCOUNT", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsDoubleAtIdentifierSyntax => true;

    /// <inheritdoc />
    public override bool SupportsSqlCalcFoundRowsModifier => true;

    /// <inheritdoc />
    public override bool SupportsNullSafeEq => true;

    /// <inheritdoc />
    public override bool SupportsIlikeOperator => true;
}
