namespace DbSqlLikeMem.Firebird;

internal partial class FirebirdDialect : SqlDialectBase, ISqlDialect
{
    internal const string DialectName = "firebird";

    internal FirebirdDialect(
        int version
    ) : base(
        name: DialectName,
        version: version,
        keywords: [],
        binOps:
        [
            new KeyValuePair<string, SqlBinaryOp>(SqlConst.AND, SqlBinaryOp.And),
            new KeyValuePair<string, SqlBinaryOp>(SqlConst.OR, SqlBinaryOp.Or),
            new KeyValuePair<string, SqlBinaryOp>("||", SqlBinaryOp.Concat),
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
            "||",
            ">=", "<=", "<>", "!="
        ])
    {
    }

    /// <inheritdoc />
    protected override void InitializeFunctionRegistry()
    {
        FirebirdScalarFunctionRegistry.Register(this, Version);
        SqlSharedWindowFunctionRegistry.Register(this);
    }

    internal const int WithCteMinVersion = FirebirdDbVersions.Version2_1;
    internal const int MergeMinVersion = FirebirdDbVersions.Version2_1;
    internal const int ReturningMinVersion = FirebirdDbVersions.Version2_1;
    internal const int OffsetFetchMinVersion = FirebirdDbVersions.Version3_0;
    internal const int FetchFirstMinVersion = FirebirdDbVersions.Version3_0;
    internal const int WindowFunctionsROW_NUMBERMinVersion = FirebirdDbVersions.Version3_0;
    internal const int WindowFunctionsMinVersion = FirebirdDbVersions.Version3_0;
    internal const int FunctionDdlMinVersion = FirebirdDbVersions.Version3_0;

    /// <summary>
    /// EN: Gets or sets identifier escape style.
    /// PT: Obtém ou define identifier escape style.
    /// </summary>
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.double_quote;

    /// <summary>
    /// EN: Determines whether the character is treated as a string quote delimiter.
    /// PT: Determina se o caractere é tratado como delimitador de string.
    /// </summary>
    public override bool IsStringQuote(char ch) => ch == '\'';

    /// <summary>
    /// EN: Gets or sets string escape style.
    /// PT: Obtém ou define string escape style.
    /// </summary>
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;

    /// <inheritdoc />
    public override StringComparison TextComparison => StringComparison.Ordinal;

    /// <summary>
    /// EN: Gets whether fetch first is supported.
    /// PT: Obtém se há suporte a fetch first.
    /// </summary>
    public override bool SupportsFetchFirst => Version >= FetchFirstMinVersion;

    /// <summary>
    /// EN: Gets whether offset fetch is supported.
    /// PT: Obtém se há suporte a offset fetch.
    /// </summary>
    public override bool SupportsOffsetFetch => Version >= OffsetFetchMinVersion;

    /// <summary>
    /// EN: Gets whether order by nulls modifier is supported.
    /// PT: Obtém se há suporte a order by nulls modifier.
    /// </summary>
    public override bool SupportsOrderByNullsModifier => true;

    /// <summary>
    /// EN: Gets whether like escape clause is supported.
    /// PT: Obtém se há suporte a like escape clause.
    /// </summary>
    public override bool SupportsLikeEscapeClause => true;

    /// <inheritdoc />
    public override bool SupportsPipeConcatOperator => true;

    /// <summary>
    /// EN: Indicates whether SQL window functions are supported by the configured Firebird version.
    /// PT: Indica se funções de janela SQL são suportadas pela versão configurada do Firebird.
    /// </summary>
    public override bool SupportsWindowFunctions => Version >= WindowFunctionsROW_NUMBERMinVersion;

    /// <summary>
    /// EN: Indicates whether SQL window frame clauses are supported by the configured version.
    /// PT: Indica se cláusulas de frame de janela SQL são suportadas pela versão configurada.
    /// </summary>
    public override bool SupportsWindowFrameClause => Version >= WindowFunctionsMinVersion;

    /// <summary>
    /// EN: Gets whether delete target alias is supported.
    /// PT: Obtém se há suporte a delete target alias.
    /// </summary>
    public override bool SupportsDeleteTargetAlias => false;

    /// <summary>
    /// EN: Gets whether with cte is supported.
    /// PT: Obtém se há suporte a with cte.
    /// </summary>
    public override bool SupportsWithCte => Version >= WithCteMinVersion;

    /// <summary>
    /// EN: Gets whether with recursive is supported.
    /// PT: Obtém se há suporte a with recursive.
    /// </summary>
    public override bool SupportsWithRecursive => Version >= WithCteMinVersion;

    /// <summary>
    /// EN: Gets whether merge is supported.
    /// PT: Obtém se há suporte a merge.
    /// </summary>
    public override bool SupportsMerge => Version >= MergeMinVersion;

    /// <summary>
    /// EN: Gets whether returning is supported.
    /// PT: Obtém se há suporte a returning.
    /// </summary>
    public override bool SupportsReturning => Version >= ReturningMinVersion;

    /// <inheritdoc />
    public override bool SupportsInsertReturning => Version >= ReturningMinVersion;

    /// <inheritdoc />
    public override bool SupportsUpdateReturning => Version >= ReturningMinVersion;

    /// <inheritdoc />
    public override bool SupportsDeleteReturning => Version >= ReturningMinVersion;

    /// <inheritdoc />
    public override bool SupportsDeleteReturningWithJoin => false;

    /// <inheritdoc />
    public override bool SupportsAlterTableAddColumn => true;

    /// <inheritdoc />
    public override bool SupportsFunctionDdl => Version >= FunctionDdlMinVersion;

    /// <inheritdoc />
    public override bool SupportsCreateOrReplaceFunctionDdl => false;

    /// <inheritdoc />
    public override bool SupportsInlineReturnCreateFunctionDdl => false;

    /// <inheritdoc />
    public override bool SupportsSequenceDdl => true;

    /// <inheritdoc />
    public override bool SupportsNextValueForSequenceExpression => true;

    /// <inheritdoc />
    public override bool SupportsPreviousValueForSequenceExpression => false;

    /// <inheritdoc />
    public override bool SupportsSequenceFunctionCall(string functionName)
        => functionName.Equals("GEN_ID", StringComparison.OrdinalIgnoreCase)
           || base.SupportsSequenceFunctionCall(functionName);

    bool ISqlDialectCompatibility.SupportsDb2TriggerDdl => true;

    bool ISqlDialectCompatibility.SupportsDb2ProcedureDdl => true;

    /// <summary>
    /// EN: Gets or sets null substitute function names.
    /// PT: Obtém ou define null substitute function names.
    /// </summary>
    public override IReadOnlyCollection<string> NullSubstituteFunctionNames => ["COALESCE"];

    /// <summary>
    /// EN: Gets or sets concat returns null on null input.
    /// PT: Obtém ou define concat returns null on null input.
    /// </summary>
    public override bool ConcatReturnsNullOnNullInput => true;
}
