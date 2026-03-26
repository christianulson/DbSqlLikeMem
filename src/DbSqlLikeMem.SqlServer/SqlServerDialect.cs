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
            ">=", "<=", "<>", "!="
        ])
    {
        SqlServerScalarFunctionRegistry.Register(this, version);
        SqlSharedWindowFunctionRegistry.Register(this);
        SqlServerTableFunctionRegistry.Register(this, version);
    }


    internal const int WithCteMinVersion = 2005;
    internal const int MergeMinVersion = 2008;
    internal const int OffsetFetchMinVersion = 2012;
    internal const int ApproxCountDistinctMinVersion = 2019;
    internal const int JsonFunctionsMinVersion = 2016;
    internal const int CompressionFunctionsMinVersion = 2016;
    internal const int SessionContextMinVersion = 2016;
    internal const int StringEscapeMinVersion = 2016;
    internal const int DateDiffBigMinVersion = 2016;
    internal const int SequenceMinVersion = 2012;
    internal const int StringAggMinVersion = 2017;
    internal const int TranslateMinVersion = 2017;
    internal const int StringSplitOrdinalMinVersion = 2022;
    internal const int HighPrecisionTemporalFunctionsMinVersion = 2008;
    internal const int DateTimeOffsetFunctionsMinVersion = 2008;
    internal const int EomonthMinVersion = 2012;
    internal const int FormatMinVersion = 2012;
    internal const int PercentileMinVersion = 2012;
    internal const int FromPartsMinVersion = 2012;
    internal const int ParseMinVersion = 2012;
    internal const int TryCastMinVersion = 2012;
    internal const int TryConvertMinVersion = 2012;
    internal const int WindowFunctionsROW_NUMBERMinVersion = 2005;
    internal const int WindowFunctionsMinVersion = 2012;
    internal const int WindowFrameClauseMinVersion = 2012;

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

    //TODO: Separar ROWS BETWEEN - 2012, RANGE BETWEEN - 2012, GROUPS BETWEEN - 2022
    /// <summary>
    /// EN: Indicates whether SQL window frame clauses are supported by the configured version.
    /// PT: Indica se cláusulas de frame de janela SQL são suportadas pela versão configurada.
    /// </summary>
    public override bool SupportsWindowFrameClause => Version >= WindowFrameClauseMinVersion;

    public override bool SupportsWithinGroupForStringAggregates => Version >= StringAggMinVersion;

    public override bool SupportsFunctionDdl => true;

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
    public override bool SupportsUpdateFromJoinSubquerySyntax => true;
    public override bool SupportsDeleteTargetFromJoinSubquerySyntax => true;
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
    /// <inheritdoc />
    public override bool SupportsForJsonClause => Version >= JsonFunctionsMinVersion;
    /// <summary>
    /// EN: Gets whether merge is supported.
    /// PT: Obtém se há suporte a merge.
    /// </summary>
    public override bool SupportsMerge => Version >= MergeMinVersion;
    public override bool SupportsAlterTableAddColumn => true;
    public override bool SupportsSequenceDdl => Version >= SequenceMinVersion;
    public override bool SupportsNextValueForSequenceExpression => Version >= SequenceMinVersion;
    public override bool SupportsPreviousValueForSequenceExpression => false;
    public override bool SupportsDoubleAtIdentifierSyntax => true;
    /// <summary>
    /// EN: Gets whether pivot clause is supported.
    /// PT: Obtém se há suporte a pivot clause.
    /// </summary>
    public override bool SupportsPivotClause => true;
    /// <inheritdoc />
    public override bool SupportsUnpivotClause => true;
    public override bool SupportsApplyClause => Version >= WithCteMinVersion;
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

    private static readonly string[] SqlServerScalarFunctionNames =
    [
        "ABS",
        "ACOS",
        "ASIN",
        "ATAN",
        "ATN2",
        "CEILING",
        "COS",
        "COT",
        "DEGREES",
        "EXP",
        "FLOOR",
        "FORMAT",
        "LOG",
        "LOG10",
        "PI",
        "POWER",
        "RADIANS",
        "RAND",
        "ROUND",
        "SIGN",
        "SIN",
        "SQUARE",
        "TAN",
        "SQRT",
        "ASCII",
        "CHARINDEX",
        "CHECKSUM",
        "BINARY_CHECKSUM",
        "DATALENGTH",
        "DIFFERENCE",
        "GROUPING",
        "GROUPING_ID",
        "ISDATE",
        "ISJSON",
        "ISNUMERIC",
        "LEN",
        "PATINDEX",
        "UNICODE",
        "ROWCOUNT",
        "ROWCOUNT_BIG",
        "CHAR",
        "CONCAT",
        "CONCAT_WS",
        "FORMATMESSAGE",
        "LEFT",
        "LOWER",
        "NCHAR",
        "NEWID",
        "NEWSEQUENTIALID",
        "PARSENAME",
        "QUOTENAME",
        "REPLICATE",
        "REVERSE",
        "REPLACE",
        "RIGHT",
        "SOUNDEX",
        "SPACE",
        "STR",
        "STUFF",
        "SUBSTRING",
        "TRIM",
        "TRANSLATE",
        "UPPER",
        "LTRIM",
        "RTRIM",
        "IF",
        "IIF",
        "JSON_MODIFY",
        "COMPRESS",
        "DECOMPRESS",
        "STRING_ESCAPE",
        "TODATETIMEOFFSET",
        "SWITCHOFFSET",
        "DATEADD",
        "DATEDIFF",
        "DATENAME",
        "DATEPART",
        "DAY",
        "MONTH",
        "YEAR",
        "DATEDIFF_BIG",
        "PARSE",
        "TRY_PARSE",
        "TRY_CAST",
        "TRY_CONVERT"
    ];

    private static readonly string[] SqlServerMetadataIdentifierNames =
    [
        "CURRENT_USER",
        "SESSION_USER",
        "SYSTEM_USER",
        "@@DATEFIRST",
        "@@IDENTITY",
        "@@MAX_PRECISION",
        "@@ROWCOUNT",
        "@@TEXTSIZE"
    ];

    private static readonly string[] SqlServerDateFunctionNames =
    [
        "CURRENT_TIMESTAMP",
        "GETDATE",
        "GETUTCDATE",
        "SYSTEMDATE",
        "SYSDATETIME",
        "SYSUTCDATETIME",
        "SYSDATETIMEOFFSET",
        "EOMONTH",
        "DATEADD",
        "DATEDIFF",
        "DATENAME",
        "DATEPART",
        "DAY",
        "MONTH",
        "YEAR",
        "DATEDIFF_BIG",
        "TODATETIMEOFFSET",
        "SWITCHOFFSET"
    ];

    private static readonly string[] SqlServerAggregateFunctionNames =
    [
        "CHECKSUM_AGG",
        "STRING_AGG",
        "APPROX_COUNT_DISTINCT",
        "MEDIAN",
        "PERCENTILE",
        "PERCENTILE_CONT",
        "PERCENTILE_DISC"
    ];

    private static readonly string[] SqlServerFromPartsFunctionNames =
    [
        "DATEFROMPARTS",
        "DATETIMEFROMPARTS",
        "DATETIME2FROMPARTS",
        "DATETIMEOFFSETFROMPARTS",
        "TIMEFROMPARTS",
        "SMALLDATETIMEFROMPARTS"
    ];

    private static readonly string[] SqlServerSequenceFunctionNames =
    [
        "NEXT_VALUE_FOR",
        "PREVIOUS_VALUE_FOR"
    ];

    private static readonly string[] SqlServerJsonSpecialFunctionNames =
    [
        "JSON_MODIFY",
        "JSON_QUERY",
        "JSON_VALUE"
    ];

    /// <summary>
    /// EN: Checks whether a SQL Server metadata function is supported by this dialect.
    /// PT: Verifica se uma funcao de metadados do SQL Server e suportada por este dialeto.
    /// </summary>
    public override bool SupportsSqlServerMetadataFunction(string functionName)
        => IsRegisteredSqlServerCall(functionName)
            && !SqlServerScalarFunctionNames.Contains(functionName, StringComparer.OrdinalIgnoreCase)
            && !SqlServerMetadataIdentifierNames.Contains(functionName, StringComparer.OrdinalIgnoreCase)
            && !SqlServerDateFunctionNames.Contains(functionName, StringComparer.OrdinalIgnoreCase)
            && !SqlServerAggregateFunctionNames.Contains(functionName, StringComparer.OrdinalIgnoreCase)
            && !SqlServerFromPartsFunctionNames.Contains(functionName, StringComparer.OrdinalIgnoreCase)
            && !SqlServerSequenceFunctionNames.Contains(functionName, StringComparer.OrdinalIgnoreCase)
            && !SqlServerJsonSpecialFunctionNames.Contains(functionName, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Checks whether a SQL Server metadata identifier is supported by this dialect.
    /// PT: Verifica se um identificador de metadados do SQL Server e suportado por este dialeto.
    /// </summary>
    public override bool SupportsSqlServerMetadataIdentifier(string identifier)
        => !string.IsNullOrWhiteSpace(identifier)
            && SqlServerMetadataIdentifierNames.Contains(identifier, StringComparer.OrdinalIgnoreCase)
            && this.TryGetScalarFunctionDefinition(identifier, out var definition)
            && definition is not null
            && (definition.AllowsCall || definition.AllowsIdentifier);

    /// <summary>
    /// EN: Checks whether a SQL Server scalar function is supported by this dialect.
    /// PT: Verifica se uma funcao escalar do SQL Server e suportada por este dialeto.
    /// </summary>
    public override bool SupportsSqlServerScalarFunction(string functionName)
        => IsRegisteredSqlServerCall(functionName)
            && SqlServerScalarFunctionNames.Contains(functionName, StringComparer.OrdinalIgnoreCase)
            && (!functionName.Equals("ISJSON", StringComparison.OrdinalIgnoreCase)
                || Version >= JsonFunctionsMinVersion);

    /// <summary>
    /// EN: Checks whether a SQL Server date function is supported by this dialect.
    /// PT: Verifica se uma funcao de data do SQL Server e suportada por este dialeto.
    /// </summary>
    public override bool SupportsSqlServerDateFunction(string functionName)
        => !string.IsNullOrWhiteSpace(functionName)
            && SqlServerDateFunctionNames.Contains(functionName, StringComparer.OrdinalIgnoreCase)
            && this.TryGetScalarFunctionDefinition(functionName, out var definition)
            && definition is not null
            && (definition.AllowsCall || definition.AllowsIdentifier);

    /// <summary>
    /// EN: Checks whether a SQL Server aggregate function is supported by this dialect.
    /// PT: Verifica se uma funcao de agregacao do SQL Server e suportada por este dialeto.
    /// </summary>
    public override bool SupportsSqlServerAggregateFunction(string functionName)
        => !string.IsNullOrWhiteSpace(functionName)
            && (
                (functionName.Equals("MEDIAN", StringComparison.OrdinalIgnoreCase)
                    || functionName.Equals("PERCENTILE", StringComparison.OrdinalIgnoreCase)
                    || functionName.Equals("PERCENTILE_CONT", StringComparison.OrdinalIgnoreCase)
                    || functionName.Equals("PERCENTILE_DISC", StringComparison.OrdinalIgnoreCase))
                ? Version >= PercentileMinVersion
                : IsRegisteredSqlServerCall(functionName)
                    && SqlServerAggregateFunctionNames.Contains(functionName, StringComparer.OrdinalIgnoreCase)
                    && (!functionName.Equals("APPROX_COUNT_DISTINCT", StringComparison.OrdinalIgnoreCase)
                        || Version >= ApproxCountDistinctMinVersion));

    /// <summary>
    /// EN: Checks whether a SQL Server FROM PARTS-style function is supported by this dialect.
    /// PT: Verifica se uma funcao estilo FROM PARTS do SQL Server e suportada por este dialeto.
    /// </summary>
    public bool SupportsSqlServerFromPartsFunction(string functionName)
        => IsRegisteredSqlServerCall(functionName)
            && SqlServerFromPartsFunctionNames.Contains(functionName, StringComparer.OrdinalIgnoreCase);
    private bool IsRegisteredSqlServerCall(string functionName)
        => !string.IsNullOrWhiteSpace(functionName)
            && this.TryGetScalarFunctionDefinition(functionName, out var definition)
            && definition is not null
            && definition.AllowsCall;

    /// <summary>
    /// EN: Gets or sets null substitute function names.
    /// PT: Obtém ou define null substitute function names.
    /// </summary>
        public override IReadOnlyCollection<string> NullSubstituteFunctionNames => ["ISNULL"];

    //TODO: implementar + → NULL contamina, CONCAT() → não contamina
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

}


