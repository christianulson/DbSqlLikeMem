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
    internal const int WindowFunctionsMinVersion = 8;
    internal const int OffsetFetchMinVersion = 12;
    internal const int FetchFirstMinVersion = 12;

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
    /// EN: Gets or sets text comparison.
    /// PT: Obtém ou define text comparison.
    /// </summary>
    public override StringComparison TextComparison => StringComparison.OrdinalIgnoreCase;

    // OFFSET ... FETCH / FETCH FIRST entrou no Oracle 12c.
    /// <summary>
    /// EN: Gets whether offset fetch is supported.
    /// PT: Obtém se há suporte a offset fetch.
    /// </summary>
    public override bool SupportsOffsetFetch => Version >= OffsetFetchMinVersion;
    /// <summary>
    /// EN: Gets whether fetch first is supported.
    /// PT: Obtém se há suporte a fetch first.
    /// </summary>
    public override bool SupportsFetchFirst => Version >= FetchFirstMinVersion;

    /// <summary>
    /// EN: Indicates whether SQL window functions are supported by the configured Oracle version.
    /// PT: Indica se funções de janela SQL são suportadas pela versão configurada do Oracle.
    /// </summary>
    public override bool SupportsWindowFunctions => Version >= WindowFunctionsMinVersion;

    /// <summary>
    /// EN: Indicates whether SQL window frame clauses are supported by the configured version.
    /// PT: Indica se cláusulas de frame de janela SQL são suportadas pela versão configurada.
    /// </summary>
    public override bool SupportsWindowFrameClause => Version >= WindowFunctionsMinVersion;
    /// <summary>
    /// EN: Gets whether order by nulls modifier is supported.
    /// PT: Obtém se há suporte a order by nulls modifier.
    /// </summary>
    public override bool SupportsOrderByNullsModifier => true;

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
    public override bool SupportsWithRecursive => false;
    /// <summary>
    /// EN: Gets whether json value function is supported.
    /// PT: Obtém se há suporte a função json_value.
    /// </summary>
    public override bool SupportsJsonValueFunction => true;
    /// <summary>
    /// EN: Gets whether merge is supported.
    /// PT: Obtém se há suporte a merge.
    /// </summary>
    public override bool SupportsMerge => Version >= MergeMinVersion;
    /// <summary>
    /// EN: Gets whether pivot clause is supported.
    /// PT: Obtém se há suporte a pivot clause.
    /// </summary>
    public override bool SupportsPivotClause => true;
    /// <summary>
    /// EN: Gets or sets null substitute function names.
    /// PT: Obtém ou define null substitute function names.
    /// </summary>
    public override IReadOnlyCollection<string> NullSubstituteFunctionNames => ["NVL"];
    /// <summary>
    /// EN: Gets or sets concat returns null on null input.
    /// PT: Obtém ou define concat returns null on null input.
    /// </summary>
    public override bool ConcatReturnsNullOnNullInput => false;

    /// <summary>
    /// EN: Represents Is Integer Cast Type Name.
    /// PT: Representa Is Integer Cast Type Name.
    /// </summary>
    public override bool IsIntegerCastTypeName(string typeName)
        => base.IsIntegerCastTypeName(typeName)
            || typeName.StartsWith("NUMBER", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Represents Supports Date Add Function.
    /// PT: Representa suporte Date Add Function.
    /// </summary>
    public override bool SupportsDateAddFunction(string functionName)
        => false;
}
