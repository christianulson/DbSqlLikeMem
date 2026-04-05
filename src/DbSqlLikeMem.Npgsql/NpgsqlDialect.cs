namespace DbSqlLikeMem.Npgsql;

internal sealed class NpgsqlDialect : SqlDialectBase, ISqlDialect
{
    internal const string DialectName = "postgresql";

    internal NpgsqlDialect(
        int version
        ) : base(
        name: DialectName,
        version: version,
        keywords: [NpgsqlConst.ILIKE],
        binOps:
        [
            new KeyValuePair<string, SqlBinaryOp>(SqlConst.AND, SqlBinaryOp.And),
            new KeyValuePair<string, SqlBinaryOp>(SqlConst.OR, SqlBinaryOp.Or),
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
            "#>>", "#>",
            "::",
            ">=", "<=", "<>", "!=",
            "&&", "||"
        ])
    {
        NpgsqlScalarFunctionRegistry.Register(this, version);
        SqlSharedWindowFunctionRegistry.Register(this);
    }


    // NOTE: in this project the Npgsql "version" axis starts at 6 and
    // parser feature tests expect WITH/CTE support across all tested versions.
    internal const int WithCteMinVersion = 6;
    internal const int MergeMinVersion = 15;
    internal const int JsonbMinVersion = 9;
    internal const int WindowFunctionsMinVersion = 8;
    internal const int WithMaterializedHintMinVersion = 12;

    /// <summary>
    /// EN: Gets whether dollar quoted strings is supported.
    /// PT: Obtém se há suporte a dollar quoted strings.
    /// </summary>
    public override bool SupportsDollarQuotedStrings => true;

    public override bool SupportsIlikeOperator => true;

    public override StringComparison TextComparison => StringComparison.Ordinal;

    public override bool LikeIsCaseInsensitive => false;

    /// <summary>
    /// EN: Gets whether limit offset is supported.
    /// PT: Obtém se há suporte a limit offset.
    /// </summary>
    public override bool SupportsLimitOffset => true;

    /// <summary>
    /// EN: Indicates whether IIF(...) is supported by this dialect.
    /// PT: Indica se IIF(...) é suportada por este dialeto.
    /// </summary>
    public override bool SupportsIifFunction => false;

    /// <summary>
    /// EN: Indicates whether SQL window functions are supported by the configured PostgreSQL version.
    /// PT: Indica se funções de janela SQL são suportadas pela versão configurada do PostgreSQL.
    /// </summary>
    public override bool SupportsWindowFunctions => Version >= WindowFunctionsMinVersion;

    /// <summary>
    /// EN: Exposes window function arity metadata for PostgreSQL-specific analytic functions and skips aggregate COUNT.
    /// PT: Expoe metadados de aridade de funcoes de janela para funcoes analiticas especificas do PostgreSQL e ignora COUNT agregado.
    /// </summary>
    /// <param name="functionName">EN: Window function name. PT: Nome da funcao de janela.</param>
    /// <param name="minArgs">EN: Minimum accepted argument count. PT: Numero minimo de argumentos aceitos.</param>
    /// <param name="maxArgs">EN: Maximum accepted argument count. PT: Numero maximo de argumentos aceitos.</param>
    /// <returns>EN: True when the function exposes arity metadata. PT: True quando a funcao expoe metadados de aridade.</returns>
    public override bool TryGetWindowFunctionArgumentArity(string functionName, out int minArgs, out int maxArgs)
    {
        if (functionName.Equals(SqlConst.COUNT, StringComparison.OrdinalIgnoreCase))
        {
            minArgs = 0;
            maxArgs = 0;
            return false;
        }

        return base.TryGetWindowFunctionArgumentArity(functionName, out minArgs, out maxArgs);
    }

    /// <summary>
    /// EN: Indicates whether SQL window frame clauses are supported by the configured version.
    /// PT: Indica se cláusulas de frame de janela SQL são suportadas pela versão configurada.
    /// </summary>
    public override bool SupportsWindowFrameClause => Version >= WindowFunctionsMinVersion;

    public override bool SupportsWithinGroupForStringAggregates => true;

    public override bool SupportsAggregateOrderByForStringAggregates => true;

    /// <summary>
    /// EN: Gets whether fetch first is supported.
    /// PT: Obtém se há suporte a fetch first.
    /// </summary>
    public override bool SupportsFetchFirst => true;

    /// <summary>
    /// EN: Gets whether offset fetch is supported.
    /// PT: Obtém se há suporte a offset fetch.
    /// </summary>
    public override bool SupportsOffsetFetch => true;

    /// <summary>
    /// EN: Gets whether order by nulls modifier is supported.
    /// PT: Obtém se há suporte a order by nulls modifier.
    /// </summary>
    public override bool SupportsOrderByNullsModifier => true;

    /// <summary>
    /// EN: Gets whether on conflict clause is supported.
    /// PT: Obtém se há suporte a on conflict clause.
    /// </summary>
    public override bool SupportsOnConflictClause => true;

    /// <summary>
    /// EN: Gets whether returning is supported.
    /// PT: Obtém se há suporte a returning.
    /// </summary>
    public override bool SupportsReturning => true;

    /// <inheritdoc />
    public override bool SupportsAlterTableAddColumn => true;
    public override bool SupportsFunctionDdl => true;
    public override bool SupportsCreateOrReplaceFunctionDdl => true;
    bool ISqlDialect.SupportsPostgreSqlCreateFunctionDdl => true;

    public override bool SupportsSequenceDdl => true;
    /// <summary>
    /// EN: Gets whether sequence dot value expressions are supported.
    /// PT: Obtém se há suporte a expressões de sequence com ponto.
    /// </summary>
    public override bool SupportsSequenceDotValueExpression(string suffix)
    {
        _ = suffix;
        return false;
    }
    /// <summary>
    /// EN: Gets whether delete target alias is supported.
    /// PT: Obtém se há suporte a delete target alias.
    /// </summary>
    public override bool SupportsDeleteTargetAlias => false;
    public override bool SupportsUpdateFromJoinSubquerySyntax => true;
    public override bool SupportsDeleteUsingSubquerySyntax => true;

    /// <summary>
    /// EN: Gets whether json arrow operators is supported.
    /// PT: Obtém se há suporte a json arrow operators.
    /// </summary>
    public override bool SupportsJsonArrowOperators => Version >= JsonbMinVersion;

    /// <summary>
    /// EN: Gets or sets allows parser cross dialect json operators.
    /// PT: Obtém ou define allows parser cross dialect json operators.
    /// </summary>
    public override bool AllowsParserCrossDialectJsonOperators => Version >= JsonbMinVersion;

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
    /// EN: Gets whether with materialized hint is supported.
    /// PT: Obtém se há suporte a with materialized hint.
    /// </summary>
    public override bool SupportsWithMaterializedHint => Version >= WithMaterializedHintMinVersion;
    /// <summary>
    /// EN: Gets whether merge is supported.
    /// PT: Obtém se há suporte a merge.
    /// </summary>
    public override bool SupportsMerge => Version >= MergeMinVersion;
    /// <summary>
    /// EN: Gets or sets null substitute function names.
    /// PT: Obtém ou define null substitute function names.
    /// </summary>
    public override IReadOnlyCollection<string> NullSubstituteFunctionNames => ["COALESCE"];

    /// <summary>
    /// EN: Gets or sets concat returns null on null input.
    /// PT: Obtém ou define concat returns null on null input.
    /// </summary>
    public override bool ConcatReturnsNullOnNullInput => false;

    /// <inheritdoc />
    public override bool PlusStringConcatReturnsNullOnNullInput => true;

    /// <inheritdoc />
    public override bool ConcatFunctionReturnsNullOnNullInput => false;

    /// <summary>
    /// EN: Gets temporary table scope.
    /// PT: Obtém temporary table scope.
    /// </summary>
    public override TemporaryTableScope GetTemporaryTableScope(string tableName, string? schemaName)
    {
        _ = tableName;
        if (string.IsNullOrWhiteSpace(schemaName)) return TemporaryTableScope.None;
        return schemaName!.StartsWith("pg_temp", StringComparison.OrdinalIgnoreCase)
            ? TemporaryTableScope.Connection
            : TemporaryTableScope.None;
    }

}

