namespace DbSqlLikeMem;

/// <summary>
/// EN: Provides the compatibility fallback for the automatic SQL dialect mode when the Auto assembly is not loaded.
/// PT: Fornece o fallback de compatibilidade do modo automatico de dialeto SQL quando o assembly Auto nao esta carregado.
/// </summary>
internal sealed class AutoSqlDialect : SqlDialectBase
{
    internal const string DialectName = "auto";
    private static readonly IReadOnlyCollection<string> _nullSubstituteFunctionNames =
        ["IFNULL", "ISNULL", "NVL"];

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
        SqlSharedScalarFunctionRegistry.Register(this);
        AutoScalarFunctionRegistry.Register(this);
        TryRegisterFirebirdScalarFunctions(this, version);
        SqlSharedWindowFunctionRegistry.Register(this);
        AutoSqlServerScalarFunctionRegistry.Register(this);
        AutoTableFunctionRegistry.Register(this);
    }

    private static void TryRegisterFirebirdScalarFunctions(ISqlDialect dialect, int version)
    {
        var registryType = Type.GetType("DbSqlLikeMem.Firebird.FirebirdScalarFunctionRegistry, DbSqlLikeMem.Firebird", throwOnError: false);
        if (registryType is null)
            return;

        var registerMethod = registryType.GetMethod(
            "Register",
            BindingFlags.Static | BindingFlags.NonPublic,
            binder: null,
            types: [typeof(ISqlDialect), typeof(int)],
            modifiers: null);

        if (registerMethod is null)
            return;

        _ = registerMethod.Invoke(null, [dialect, version]);
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
    public override bool SupportsSequenceDotValueExpression(string suffix)
        => this.TryGetScalarFunctionDefinition(suffix, out var definition)
            && definition is not null
            && definition.AllowsCall;

    /// <inheritdoc />
    public override bool SupportsJsonArrowOperators => true;

    /// <inheritdoc />
    public override bool AllowsParserCrossDialectJsonOperators => true;

    /// <inheritdoc />
    public override bool SupportsJsonValueReturningClause => true;

    /// <inheritdoc />
    public override bool SupportsWithinGroupForStringAggregates => true;

    /// <inheritdoc />
    public override bool SupportsAggregateOrderByForStringAggregates => true;

    /// <inheritdoc />
    public override bool SupportsAggregateSeparatorKeywordForStringAggregates => true;

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
