namespace DbSqlLikeMem.Db2;

internal sealed class Db2Dialect : SqlDialectBase, ISqlDialect
{
    internal const string DialectName = "db2";

    internal Db2Dialect(
        int version
        ) : base(
        name: DialectName,
        version: version,
        keywords: [],
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
            ">=", "<=", "<>", "!=",
            "||"
        ])
    {
    }

    /// <inheritdoc />
    protected override void InitializeFunctionRegistry()
    {
        Db2ScalarFunctionRegistry.Register(this, Version);
        Db2WindowFunctionRegistry.Register(this);
        Db2TableFunctionRegistry.Register(this, Version);
        SqlSharedWindowFunctionRegistry.Register(this);
    }


    internal const int WithCteMinVersion = 8;
    internal const int MergeMinVersion = 9;
    internal const int JsonFunctionsMinVersion = 11;
    internal const int WindowFunctionsMinVersion = 8;
    internal const int OffsetFetchMinVersion = 10;

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

    /// <summary>
    /// EN: Gets whether fetch first is supported.
    /// PT: Obtém se há suporte a fetch first.
    /// </summary>
    public override bool SupportsFetchFirst => true;

    /// <summary>
    /// EN: Gets whether offset fetch is supported.
    /// PT: Obtém se há suporte a offset fetch.
    /// </summary>
    public override bool SupportsOffsetFetch => Version >= OffsetFetchMinVersion;
    /// <inheritdoc />
    public override bool SupportsPipeConcatOperator => true;

    public override bool SupportsLikeEscapeClause => true;

    public override bool IsRowNumberWindowFunction(string functionName)
        => functionName.Equals("ROW_NUMBER", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ROWNUMBER", StringComparison.OrdinalIgnoreCase);



    /// <summary>
    /// EN: Indicates whether SQL window functions are supported by the configured DB2 version.
    /// PT: Indica se funções de janela SQL são suportadas pela versão DB2 configurada.
    /// </summary>
    public override bool SupportsWindowFunctions => Version >= WindowFunctionsMinVersion;

    /// <summary>
    /// EN: Indicates whether SQL window frame clauses are supported by the configured DB2 version.
    /// PT: Indica se cláusulas SQL de frame de janela são suportadas pela versão DB2 configurada.
    /// </summary>
    public override bool SupportsWindowFrameClause => Version >= WindowFunctionsMinVersion;

    public override bool SupportsWithinGroupForStringAggregates => true;

    /// <summary>
    /// EN: Gets whether delete target alias is supported.
    /// PT: Obtém se há suporte a delete target alias.
    /// </summary>
    public override bool SupportsDeleteTargetAlias => false;
    public override bool SupportsIifFunction => false;

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
    /// EN: Checks whether a SQL Server-style aggregate helper is supported by DB2 compatibility behavior.
    /// PT: Verifica se um helper agregado no estilo SQL Server e suportado pelo comportamento de compatibilidade DB2.
    /// </summary>
    public override bool SupportsSqlServerAggregateFunction(string functionName)
        => !string.IsNullOrWhiteSpace(functionName)
            && (
                functionName.Equals("MEDIAN", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("PERCENTILE", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("PERCENTILE_CONT", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("PERCENTILE_DISC", StringComparison.OrdinalIgnoreCase));

    public override bool SupportsAlterTableAddColumn => true;
    public override bool SupportsFunctionDdl => true;
    bool ISqlDialectCompatibility.SupportsInlineReturnCreateFunctionDdl => true;
    bool ISqlDialectCompatibility.SupportsDb2TriggerDdl => true;
    bool ISqlDialectCompatibility.SupportsDb2ProcedureDdl => true;
    /// <summary>
    /// EN: Gets whether CREATE OR REPLACE FUNCTION is supported.
    /// PT: Obtém se CREATE OR REPLACE FUNCTION é suportado.
    /// </summary>
    public override bool SupportsCreateOrReplaceFunctionDdl => true;
    /// <inheritdoc />
    public override bool SupportsOracleSpecificConversionFunction(string functionName)
        => functionName.Equals("CONVERT", StringComparison.OrdinalIgnoreCase);

    public override bool SupportsSequenceDdl => true;
    public override bool SupportsNextValueForSequenceExpression => true;
    public override bool SupportsPreviousValueForSequenceExpression => true;
    /// <summary>
    /// EN: Gets the null substitute function names supported by DB2 compatibility behavior.
    /// PT: Obtém os nomes de funções de substituição de nulos suportados pelo comportamento de compatibilidade do DB2.
    /// </summary>
    public override IReadOnlyCollection<string> NullSubstituteFunctionNames => ["COALESCE", Db2Const.VALUE, "IFNULL", "NVL"];
    /// <summary>
    /// EN: Gets or sets allows parser limit offset compatibility.
    /// PT: Obtém ou define allows parser limit offset compatibility.
    /// </summary>
    public override bool AllowsParserLimitOffsetCompatibility => true;

    /// <summary>
    /// EN: Gets or sets text comparison.
    /// PT: Obtém ou define text comparison.
    /// </summary>
    public override StringComparison TextComparison => StringComparison.Ordinal;

}
