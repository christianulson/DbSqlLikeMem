namespace DbSqlLikeMem.Npgsql;

internal sealed class NpgsqlDialect : SqlDialectBase
{
    internal const string DialectName = "postgresql";

    internal NpgsqlDialect(
        int version
        ) : base(
        name: DialectName,
        version: version,
        keywords: ["ILIKE"],
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
            "#>>", "#>",
            "::",
            ">=", "<=", "<>", "!=", "==",
            "&&", "||"
        ])
    { }


    // NOTE: in this project the Npgsql "version" axis starts at 6 and
    // parser feature tests expect WITH/CTE support across all tested versions.
    internal const int WithCteMinVersion = 6;
    internal const int MergeMinVersion = 15;
    internal const int JsonbMinVersion = 9;
    internal const int WindowFunctionsMinVersion = 8;

    /// <summary>
    /// EN: Gets whether dollar quoted strings is supported.
    /// PT: Obtém se há suporte a dollar quoted strings.
    /// </summary>
    public override bool SupportsDollarQuotedStrings => true;

    public override bool SupportsIlikeOperator => true;

    /// <summary>
    /// EN: Gets whether limit offset is supported.
    /// PT: Obtém se há suporte a limit offset.
    /// </summary>
    public override bool SupportsLimitOffset => true;

    /// <summary>
    /// EN: Indicates whether SQL window functions are supported by the configured PostgreSQL version.
    /// PT: Indica se funções de janela SQL são suportadas pela versão configurada do PostgreSQL.
    /// </summary>
    public override bool SupportsWindowFunctions => Version >= WindowFunctionsMinVersion;

    /// <summary>
    /// EN: Indicates whether SQL window frame clauses are supported by the configured version.
    /// PT: Indica se cláusulas de frame de janela SQL são suportadas pela versão configurada.
    /// </summary>
    public override bool SupportsWindowFrameClause => Version >= WindowFunctionsMinVersion;

    public override bool SupportsWithinGroupForStringAggregates => true;

    public override bool SupportsWithinGroupStringAggregateFunction(string functionName)
        => functionName.Equals("STRING_AGG", StringComparison.OrdinalIgnoreCase);

    public override bool SupportsAggregateOrderByForStringAggregates => true;

    public override bool SupportsAggregateOrderByStringAggregateFunction(string functionName)
        => functionName.Equals("STRING_AGG", StringComparison.OrdinalIgnoreCase);

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

    public override bool SupportsSequenceDdl => true;
    public override bool SupportsSequenceFunctionCall(string functionName)
        => functionName.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SETVAL", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LASTVAL", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Gets whether delete target alias is supported.
    /// PT: Obtém se há suporte a delete target alias.
    /// </summary>
    public override bool SupportsDeleteTargetAlias => false;
    public override bool SupportsUpdateFromJoinSubquerySyntax => true;
    public override bool SupportsDeleteUsingSubquerySyntax => true;

    public override bool SupportsStringAggregateFunction(string functionName)
        => functionName.Equals("STRING_AGG", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Gets whether json arrow operators is supported.
    /// PT: Obtém se há suporte a json arrow operators.
    /// </summary>
    public override bool SupportsJsonArrowOperators => Version >= JsonbMinVersion;

    public override bool SupportsJsonQueryFunction => false;

    public override bool SupportsJsonValueFunction => false;

    public override bool SupportsOracleTimeFunction(string functionName)
        => functionName.Equals("LOCALTIME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LOCALTIMESTAMP", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Gets or sets allows parser cross dialect json operators.
    /// PT: Obtém ou define allows parser cross dialect json operators.
    /// </summary>
    public override bool AllowsParserCrossDialectJsonOperators => true;

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
    public override bool SupportsWithMaterializedHint => true;
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

    public override IReadOnlyDictionary<string, SqlTemporalFunctionKind> TemporalFunctionNames
        => new Dictionary<string, SqlTemporalFunctionKind>(StringComparer.OrdinalIgnoreCase)
        {
            ["CURRENT_DATE"] = SqlTemporalFunctionKind.Date,
            ["CURRENT_TIME"] = SqlTemporalFunctionKind.Time,
            ["CURRENT_TIMESTAMP"] = SqlTemporalFunctionKind.DateTime,
            ["NOW"] = SqlTemporalFunctionKind.DateTime,
            ["SYSTEMDATE"] = SqlTemporalFunctionKind.DateTime,
        };


    public override IReadOnlyCollection<string> TemporalFunctionIdentifierNames
        => ["CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "SYSTEMDATE"];

    public override IReadOnlyCollection<string> TemporalFunctionCallNames
        => ["NOW"];

    public override bool SupportsSqlServerMetadataIdentifier(string identifier)
        => identifier.Equals("CURRENT_USER", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Gets or sets concat returns null on null input.
    /// PT: Obtém ou define concat returns null on null input.
    /// </summary>
    public override bool ConcatReturnsNullOnNullInput => false;

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

    /// <summary>
    /// EN: Represents Supports Date Add Function.
    /// PT: Representa suporte Date Add Function.
    /// </summary>
    public override bool SupportsDateAddFunction(string functionName)
        => false;

    public override bool SupportsLastFoundRowsFunction(string functionName)
        => functionName.Equals("ROW_COUNT", StringComparison.OrdinalIgnoreCase);
}
