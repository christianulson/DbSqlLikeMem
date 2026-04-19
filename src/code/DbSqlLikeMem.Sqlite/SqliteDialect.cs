namespace DbSqlLikeMem.Sqlite;

internal sealed class SqliteDialect : SqlDialectBase
{
    internal const string DialectName = "sqlite";

    internal SqliteDialect(
        int version
        ) : base(
        name: DialectName,
        version: version,
        keywords: [],//"REGEXP"],
        binOps:
        [
            new KeyValuePair<string, SqlBinaryOp>(SqlConst.AND, SqlBinaryOp.And),
            new KeyValuePair<string, SqlBinaryOp>(SqlConst.OR, SqlBinaryOp.Or),
            new KeyValuePair<string, SqlBinaryOp>("||", SqlBinaryOp.Concat),
            new KeyValuePair<string, SqlBinaryOp>("=", SqlBinaryOp.Eq),
            new KeyValuePair<string, SqlBinaryOp>("==", SqlBinaryOp.Eq),
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
            "||"
        ])
    {
        SqliteScalarFunctionRegistry.Register(this, version);
        SqliteTableFunctionRegistry.Register(this);
        SqlSharedWindowFunctionRegistry.Register(this);
    }


    internal const int WithCteMinVersion = 300;
    internal const int OnUpsertMinVersion = 324;
    internal const int ReturningMinVersion = 335;
    internal const int WithMaterializedHintMinVersion = 335;
    internal const int JsonArrowOperatorsMinVersion = 338;
    internal const int WindowFunctionsMinVersion = 325;
    internal const int OrderByNullsModifierMinVersion = 330;
    internal const int AggregateOrderByMinVersion = 330;
    internal const int MergeMinVersion = int.MaxValue;
    /// <summary>
    /// EN: Gets or sets allows backtick identifiers.
    /// PT: Obtém ou define allows backtick identifiers.
    /// </summary>
    public override bool AllowsBacktickIdentifiers => true;

    public override bool AllowsBracketIdentifiers => true;
    /// <summary>
    /// EN: Gets or sets identifier escape style.
    /// PT: Obtém ou define identifier escape style.
    /// </summary>
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.double_quote;

    /// <summary>
    /// EN: Determines whether the character is treated as a string quote delimiter.
    /// PT: Determina se o caractere é tratado como delimitador de string.
    /// </summary>
    public override bool IsStringQuote(char ch) => ch is '\'';
    /// <summary>
    /// EN: Gets or sets string escape style.
    /// PT: Obtém ou define string escape style.
    /// </summary>
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;

    /// <summary>
    /// EN: Gets whether limit offset is supported.
    /// PT: Obtém se há suporte a limit offset.
    /// </summary>
    public override bool SupportsLimitOffset => true;

    /// <summary>
    /// EN: Gets whether on conflict clause is supported.
    /// PT: Obtém se há suporte a on conflict clause.
    /// </summary>
    public override bool SupportsOnConflictClause => Version >= OnUpsertMinVersion;
    /// <summary>
    /// EN: Gets whether RETURNING clause is supported for DML statements.
    /// PT: Obtém se a cláusula RETURNING é suportada para comandos DML.
    /// </summary>
    public override bool SupportsReturning => Version >= ReturningMinVersion;
    /// <summary>
    /// EN: Gets whether order by nulls modifier is supported.
    /// PT: Obtém se há suporte a order by nulls modifier.
    /// </summary>
    public override bool SupportsOrderByNullsModifier => Version >= OrderByNullsModifierMinVersion;

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

    /// <inheritdoc />
    public override bool SupportsAlterTableAddColumn => true;
    /// <summary>
    /// EN: Gets whether with recursive is supported.
    /// PT: Obtém se há suporte a with recursive.
    /// </summary>
    public override bool SupportsWithRecursive => Version >= WithCteMinVersion;
    /// <summary>
    /// EN: Gets whether with materialized hint is supported.
    /// PT: Obtém se há suporte a with materialized hint.
    /// </summary>
    public override bool SupportsWithMaterializedHint => Version >= WithMaterializedHintMinVersion;

    /// <inheritdoc />
    public override bool SupportsWindowFunctions => Version >= WindowFunctionsMinVersion;

    /// <inheritdoc />
    public override bool SupportsWindowFrameClause => Version >= WindowFunctionsMinVersion;

    /// <summary>
    /// EN: Gets or sets null substitute function names.
    /// PT: Obtém ou define null substitute function names.
    /// </summary>
    public override IReadOnlyCollection<string> NullSubstituteFunctionNames => ["IFNULL", "COALESCE"];
    /// <summary>
    /// EN: Gets or sets concat returns null on null input.
    /// PT: Obtém ou define concat returns null on null input.
    /// </summary>
    public override bool ConcatReturnsNullOnNullInput => true;
    /// <inheritdoc />
    public override bool SupportsPipeConcatOperator => true;
    /// <summary>
    /// EN: Gets whether json arrow operators is supported.
    /// PT: Obtém se há suporte a json arrow operators.
    /// </summary>
    public override bool SupportsJsonArrowOperators => Version >= JsonArrowOperatorsMinVersion;
    /// <summary>
    /// EN: Gets or sets text comparison.
    /// PT: Obtém ou define text comparison.
    /// </summary>
    public override StringComparison TextComparison => StringComparison.Ordinal;

    public override bool SupportsAggregateOrderByForStringAggregates => Version >= AggregateOrderByMinVersion;

}
