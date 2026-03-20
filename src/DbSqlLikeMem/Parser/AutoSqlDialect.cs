namespace DbSqlLikeMem;

/// <summary>
/// EN: Provides the first compatibility slice of the automatic SQL dialect mode.
/// PT: Fornece a primeira fatia de compatibilidade do modo automatico de dialeto SQL.
/// </summary>
internal sealed class AutoSqlDialect : SqlDialectBase
{
    internal const string DialectName = "auto";
    private static readonly IReadOnlyCollection<string> _nullSubstituteFunctionNames =
        ["IFNULL", "ISNULL", "NVL"];
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
            ["GETUTCDATE"] = SqlTemporalFunctionKind.DateTime,
            ["SYSDATETIME"] = SqlTemporalFunctionKind.DateTime,
            ["SYSTIMESTAMP"] = SqlTemporalFunctionKind.DateTime,
        };

    private static readonly KeyValuePair<string, SqlBinaryOp>[] _binaryOperators =
    [
        new(SqlConst.AND, SqlBinaryOp.And),
        new(SqlConst.OR, SqlBinaryOp.Or),
        new("=", SqlBinaryOp.Eq),
        new("<=>", SqlBinaryOp.NullSafeEq),
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
            operators: ["<=>", ">=", "<=", "<>", "!="])
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
    public override bool SupportsFunctionDdl => true;

    /// <inheritdoc />
    public override bool SupportsAlterTableAddColumn => true;

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
    public override bool SupportsJsonQueryFunction => true;

    /// <inheritdoc />
    public override bool SupportsJsonValueFunction => true;

    /// <inheritdoc />
    public override bool SupportsJsonValueReturningClause => true;

    /// <inheritdoc />
    public override bool SupportsOpenJsonFunction => true;

    /// <inheritdoc />
    public override IReadOnlyDictionary<string, SqlTemporalFunctionKind> TemporalFunctionNames => _temporalFunctionNames;

    /// <inheritdoc />
    public override IReadOnlyCollection<string> TemporalFunctionIdentifierNames
        => ["CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "SYSTEMDATE", "SYSDATE"];

    /// <inheritdoc />
    public override IReadOnlyCollection<string> TemporalFunctionCallNames
        => ["NOW", "GETDATE", "GETUTCDATE", "SYSDATETIME", "SYSTIMESTAMP"];

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
    public override bool SupportsTryCastFunction => true;

    /// <inheritdoc />
    public override bool SupportsTryConvertFunction => true;

    /// <inheritdoc />
    public override bool SupportsEomonthFunction => true;

    /// <inheritdoc />
    public override bool SupportsGetUtcDateFunction => true;

    /// <inheritdoc />
    public override bool SupportsSqlServerMetadataFunction(string functionName)
        => functionName.Equals("DB_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DB_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SCHEMA_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SCHEMA_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SERVERPROPERTY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SESSION_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SUSER_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SUSER_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SUSER_SNAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("USER_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("USER_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("XACT_STATE", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsSqlServerMetadataIdentifier(string identifier)
        => identifier.Equals("CURRENT_USER", StringComparison.OrdinalIgnoreCase)
            || identifier.Equals("SESSION_USER", StringComparison.OrdinalIgnoreCase)
            || identifier.Equals("SYSTEM_USER", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsSqlServerScalarFunction(string functionName)
        => functionName.Equals("COT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DEGREES", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DIFFERENCE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("EXP", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("FLOOR", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LEN", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LOG", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LOG10", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("PI", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("POWER", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("RADIANS", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("RAND", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ROUND", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SIN", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SQUARE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("TAN", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LTRIM", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("PARSENAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("QUOTENAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("REPLICATE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("REVERSE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("RTRIM", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SOUNDEX", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SPACE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SQRT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("STUFF", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("UNICODE", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsSqlServerFromPartsFunction(string functionName)
        => functionName.Equals("DATEFROMPARTS", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DATETIMEFROMPARTS", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DATETIME2FROMPARTS", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DATETIMEOFFSETFROMPARTS", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("TIMEFROMPARTS", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SMALLDATETIMEFROMPARTS", StringComparison.OrdinalIgnoreCase);

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

    /// <inheritdoc />
    public override bool SupportsMatchAgainstPredicate => true;

    /// <inheritdoc />
    public override bool SupportsIfFunction => true;

    /// <inheritdoc />
    public override bool SupportsIifFunction => true;

    /// <inheritdoc />
    public override IReadOnlyCollection<string> NullSubstituteFunctionNames => _nullSubstituteFunctionNames;

    /// <inheritdoc />
    public override bool SupportsWindowFunctions => true;

    /// <inheritdoc />
    public override bool SupportsForJsonClause => true;

    /// <inheritdoc />
    public override bool SupportsPivotClause => true;

    /// <inheritdoc />
    public override bool SupportsUnpivotClause => true;

    /// <inheritdoc />
    public override bool SupportsWithCte => true;

    /// <inheritdoc />
    public override bool SupportsReturning => true;

    /// <inheritdoc />
    public override bool SupportsOrderByNullsModifier => true;
}
