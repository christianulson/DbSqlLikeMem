namespace DbSqlLikeMem.SqlServer;

internal sealed class SqlServerDialect : SqlDialectBase
{
    internal const string DialectName = "sqlserver";
    internal SqlServerDialect(
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
            ">=", "<=", "<>", "!="
        ])
    { }


    internal const int WithCteMinVersion = 2005;
    internal const int MergeMinVersion = 2008;
    internal const int OffsetFetchMinVersion = 2012;
    internal const int JsonFunctionsMinVersion = 2016;
    internal const int WindowFunctionsMinVersion = 2005;

    /// <summary>
    /// EN: Gets or sets allows bracket identifiers.
    /// PT: Obtém ou define allows bracket identifiers.
    /// </summary>
    public override bool AllowsBracketIdentifiers => true;

    /// <summary>
    /// EN: Gets or sets identifier escape style.
    /// PT: Obtém ou define identifier escape style.
    /// </summary>
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.bracket;

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

    /// <summary>
    /// EN: Gets whether top is supported.
    /// PT: Obtém se há suporte a top.
    /// </summary>
    public override bool SupportsTop => true;

    /// <summary>
    /// EN: Indicates whether SQL window functions are supported by the configured SQL Server version.
    /// PT: Indica se funções de janela SQL são suportadas pela versão configurada do SQL Server.
    /// </summary>
    public override bool SupportsWindowFunctions => Version >= WindowFunctionsMinVersion;

    /// <summary>
    /// EN: Indicates whether SQL window frame clauses are supported by the configured version.
    /// PT: Indica se cláusulas de frame de janela SQL são suportadas pela versão configurada.
    /// </summary>
    public override bool SupportsWindowFrameClause => Version >= WindowFunctionsMinVersion;

    // OFFSET ... FETCH entrou no SQL Server 2012.
    /// <summary>
    /// EN: Gets whether offset fetch is supported.
    /// PT: Obtém se há suporte a offset fetch.
    /// </summary>
    public override bool SupportsOffsetFetch => Version >= OffsetFetchMinVersion;
    /// <summary>
    /// EN: Gets or sets requires order by for offset fetch.
    /// PT: Obtém ou define requires order by for offset fetch.
    /// </summary>
    public override bool RequiresOrderByForOffsetFetch => true;

    /// <summary>
    /// EN: Gets whether delete without from is supported.
    /// PT: Obtém se há suporte a delete without from.
    /// </summary>
    public override bool SupportsDeleteWithoutFrom => true; // DELETE [FROM] t
    /// <summary>
    /// EN: Gets whether delete target alias is supported.
    /// PT: Obtém se há suporte a delete target alias.
    /// </summary>
    public override bool SupportsDeleteTargetAlias => true; // DELETE alias FROM t alias JOIN ...
    /// <summary>
    /// EN: Gets whether with cte is supported.
    /// PT: Obtém se há suporte a with cte.
    /// </summary>
    public override bool SupportsWithCte => Version >= WithCteMinVersion;
    // SQL Server supports CTE but not the "WITH RECURSIVE" keyword form.
    /// <summary>
    /// EN: Gets whether with recursive is supported.
    /// PT: Obtém se há suporte a with recursive.
    /// </summary>
    public override bool SupportsWithRecursive => false;
    /// <summary>
    /// EN: Gets whether json value function is supported.
    /// PT: Obtém se há suporte a função json_value.
    /// </summary>
    public override bool SupportsJsonValueFunction => Version >= JsonFunctionsMinVersion;
    /// <summary>
    /// EN: Gets whether open json function is supported.
    /// PT: Obtém se há suporte a função openjson.
    /// </summary>
    public override bool SupportsOpenJsonFunction => Version >= JsonFunctionsMinVersion;
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
    /// EN: Gets whether sql server table hints is supported.
    /// PT: Obtém se há suporte a sql server table hints.
    /// </summary>
    public override bool SupportsSqlServerTableHints => true;
    /// <summary>
    /// EN: Gets whether sql server query hints is supported.
    /// PT: Obtém se há suporte a sql server consulta hints.
    /// </summary>
    public override bool SupportsSqlServerQueryHints => true;
    /// <summary>
    /// EN: Gets or sets null substitute function names.
    /// PT: Obtém ou define null substitute function names.
    /// </summary>
        public override IReadOnlyCollection<string> NullSubstituteFunctionNames => ["ISNULL"];
    public override IReadOnlyDictionary<string, SqlTemporalFunctionKind> TemporalFunctionNames
        => new Dictionary<string, SqlTemporalFunctionKind>(StringComparer.OrdinalIgnoreCase)
        {
            ["CURRENT_TIMESTAMP"] = SqlTemporalFunctionKind.DateTime,
            ["GETDATE"] = SqlTemporalFunctionKind.DateTime,
            ["SYSDATETIME"] = SqlTemporalFunctionKind.DateTime,
            ["SYSTEMDATE"] = SqlTemporalFunctionKind.DateTime,
        };
    /// <summary>
    /// EN: Gets or sets concat returns null on null input.
    /// PT: Obtém ou define concat returns null on null input.
    /// </summary>
    public override bool ConcatReturnsNullOnNullInput => false;

    /// <summary>
    /// EN: Gets or sets allows hash identifiers.
    /// PT: Obtém ou define allows hash identifiers.
    /// </summary>
    public override bool AllowsHashIdentifiers => true;

    /// <summary>
    /// EN: Gets temporary table scope.
    /// PT: Obtém temporary table scope.
    /// </summary>
    public override TemporaryTableScope GetTemporaryTableScope(string tableName, string? schemaName)
    {
        _ = schemaName;
        if (string.IsNullOrWhiteSpace(tableName)) return TemporaryTableScope.None;
        if (tableName.StartsWith("##", StringComparison.Ordinal))
            return TemporaryTableScope.Global;
        if (tableName.StartsWith("#", StringComparison.Ordinal))
            return TemporaryTableScope.Connection;
        return TemporaryTableScope.None;
    }

    /// <summary>
    /// EN: Represents Supports Date Add Function.
    /// PT: Representa suporte Date Add Function.
    /// </summary>
    public override bool SupportsDateAddFunction(string functionName)
        => functionName.Equals("DATEADD", StringComparison.OrdinalIgnoreCase);
}
