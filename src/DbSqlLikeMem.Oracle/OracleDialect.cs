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
    internal const int ApproxCountDistinctMinVersion = 12;
    internal const int ApproximateAnalyticsMinVersion = 18;
    internal const int OracleTextConversionMinVersion = 9;
    internal const int OracleBinaryConversionMinVersion = 10;
    internal const int OracleBlobConversionMinVersion = 11;
    internal const int OracleScnFunctionMinVersion = 10;
    internal const int OracleClusterFunctionMinVersion = 10;
    internal const int OracleAdvancedClusterFunctionMinVersion = 12;
    internal const int OracleContainerFunctionMinVersion = 12;
    internal const int OracleRowToNCharFunctionMinVersion = 18;
    internal const int OracleUserEnvMetadataMinVersion = 12;
    internal const int OraclePartitionMetadataMinVersion = 18;
    internal const int OracleValidateConversionMinVersion = 18;
    internal const int OracleJsonTransformMinVersion = 19;
    internal const int OracleJsonSqlFunctionMinVersion = 12;
    internal const int OracleCollationFunctionMinVersion = 18;
    internal const int OracleOraHashMinVersion = 10;
    internal const int OracleStandardHashMinVersion = 12;
    internal const int OracleSysFamilyMinVersion = 9;
    internal const int OracleSysZoneIdMinVersion = 12;
    internal const int OracleTemporalFunctionMinVersion = 9;
    internal const int OracleIntervalFunctionMinVersion = 8;

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

    public override bool SupportsWithinGroupForStringAggregates => true;

    public override bool SupportsWithinGroupStringAggregateFunction(string functionName)
        => functionName.Equals("LISTAGG", StringComparison.OrdinalIgnoreCase);
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
    public override bool SupportsJsonValueFunction => Version >= OracleJsonSqlFunctionMinVersion;
    public override bool SupportsJsonQueryFunction => Version >= OracleJsonSqlFunctionMinVersion;
    public override bool SupportsJsonTableFunction => Version >= OracleJsonSqlFunctionMinVersion;

    public override bool SupportsStringAggregateFunction(string functionName)
        => functionName.Equals("LISTAGG", StringComparison.OrdinalIgnoreCase);

    public override bool SupportsJsonValueReturningClause => Version >= OracleJsonSqlFunctionMinVersion;
    /// <summary>
    /// EN: Gets whether merge is supported.
    /// PT: Obtém se há suporte a merge.
    /// </summary>
    public override bool SupportsMerge => Version >= MergeMinVersion;
    public override bool SupportsAlterTableAddColumn => true;
    public override bool SupportsSequenceDdl => true;
    public override bool SupportsSequenceDotValueExpression(string suffix)
        => suffix.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase)
            || suffix.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase);
    public override bool SupportsSequenceFunctionCall(string functionName)
        => functionName.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Gets whether pivot clause is supported.
    /// PT: Obtém se há suporte a pivot clause.
    /// </summary>
    public override bool SupportsPivotClause => true;
    public override bool PivotAvgReturnsDecimalForIntegralInputs => true;
    /// <summary>
    /// EN: Gets or sets null substitute function names.
    /// PT: Obtém ou define null substitute function names.
    /// </summary>
    public override IReadOnlyCollection<string> NullSubstituteFunctionNames => ["NVL"];
    public override IReadOnlyDictionary<string, SqlTemporalFunctionKind> TemporalFunctionNames
    {
        get
        {
            var map = new Dictionary<string, SqlTemporalFunctionKind>(StringComparer.OrdinalIgnoreCase)
            {
                ["SYSDATE"] = SqlTemporalFunctionKind.DateTime,
                ["SYSTEMDATE"] = SqlTemporalFunctionKind.DateTime,
            };

            if (Version >= OracleTemporalFunctionMinVersion)
            {
                map["CURRENT_DATE"] = SqlTemporalFunctionKind.Date;
                map["CURRENT_TIMESTAMP"] = SqlTemporalFunctionKind.DateTime;
                map["SYSTIMESTAMP"] = SqlTemporalFunctionKind.DateTime;
            }

            return map;
        }
    }


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

    /// <inheritdoc />
    public override bool SupportsApproximateAggregateFunction(string functionName)
        => functionName.ToUpperInvariant() switch
        {
            "APPROX_COUNT_DISTINCT" => Version >= ApproxCountDistinctMinVersion,
            "APPROX_COUNT_DISTINCT_AGG" or "APPROX_COUNT_DISTINCT_DETAIL"
                or "APPROX_MEDIAN"
                or "APPROX_PERCENTILE"
                or "APPROX_PERCENTILE_AGG"
                or "APPROX_PERCENTILE_DETAIL" => Version >= ApproximateAnalyticsMinVersion,
            _ => false
        };
    /// <inheritdoc />
    public override bool SupportsApproximateScalarFunction(string functionName)
        => Version >= ApproximateAnalyticsMinVersion
            && (functionName.Equals("TO_APPROX_COUNT_DISTINCT", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("TO_APPROX_PERCENTILE", StringComparison.OrdinalIgnoreCase));
    /// <inheritdoc />
    public override bool SupportsOracleSpecificConversionFunction(string functionName)
        => functionName.ToUpperInvariant() switch
        {
            "TO_BINARY_DOUBLE" or "TO_BINARY_FLOAT" => Version >= OracleBinaryConversionMinVersion,
            "TO_BLOB" => Version >= OracleBlobConversionMinVersion,
            "TO_CLOB" or "TO_DSINTERVAL" or "TO_NCHAR" or "TO_NCLOB" or "TO_TIMESTAMP_TZ" or "TO_YMINTERVAL"
                => Version >= OracleTextConversionMinVersion,
            "TO_LOB" or "TO_MULTI_BYTE" or "TO_SINGLE_BYTE" => true,
            _ => false
        };
    /// <inheritdoc />
    public override bool SupportsOracleScnFunction(string functionName)
        => Version >= OracleScnFunctionMinVersion
            && (functionName.Equals("SCN_TO_TIMESTAMP", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("TIMESTAMP_TO_SCN", StringComparison.OrdinalIgnoreCase));
    /// <inheritdoc />
    public override bool SupportsOracleAnalyticsFunction(string functionName)
        => functionName.ToUpperInvariant() switch
        {
            "RATIO_TO_REPORT" => Version >= 8,
            "FEATURE_ID" or "FEATURE_SET" or "FEATURE_VALUE"
                or "POWERMULTISET" or "POWERMULTISET_BY_CARDINALITY"
                or "PREDICTION" or "PREDICTION_COST" or "PREDICTION_DETAILS"
                or "PREDICTION_PROBABILITY" or "PREDICTION_SET"
                or "PRESENTNNV" or "PRESENTV" => Version >= 10,
            "PREDICTION_BOUNDS" => Version >= 11,
            "FEATURE_DETAILS" => Version >= 12,
            "FEATURE_COMPARE" or "NCGR" => Version >= 18,
            _ => false
        };
    /// <inheritdoc />
    public override bool SupportsOracleClusterFunction(string functionName)
        => functionName.ToUpperInvariant() switch
        {
            "CLUSTER_ID" or "CLUSTER_PROBABILITY" or "CLUSTER_SET" => Version >= OracleClusterFunctionMinVersion,
            "CLUSTER_DETAILS" or "CLUSTER_DISTANCE" => Version >= OracleAdvancedClusterFunctionMinVersion,
            _ => false
        };
    /// <inheritdoc />
    public override bool SupportsOracleContainerFunction(string functionName)
        => Version >= OracleContainerFunctionMinVersion
            && (functionName.Equals("CON_DBID_TO_ID", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("CON_GUID_TO_ID", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("CON_NAME_TO_ID", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("CON_UID_TO_ID", StringComparison.OrdinalIgnoreCase));
    /// <inheritdoc />
    public override bool SupportsOracleRowIdFunction(string functionName)
        => functionName.ToUpperInvariant() switch
        {
            "ROWIDTOCHAR" => true,
            "ROWTONCHAR" => Version >= OracleRowToNCharFunctionMinVersion,
            _ => false
        };
    /// <inheritdoc />
    public override bool SupportsOracleUserEnvFunction(string functionName)
        => functionName.ToUpperInvariant() switch
        {
            "USERENV" => true,
            "ORA_INVOKING_USER" or "ORA_INVOKING_USERID" or "ORA_DST_AFFECTED" or "ORA_DST_CONVERT" or "ORA_DST_ERROR"
                => Version >= OracleUserEnvMetadataMinVersion,
            "ORA_DM_PARTITION_NAME" => Version >= OraclePartitionMetadataMinVersion,
            _ => false
        };
    /// <inheritdoc />
    public override bool SupportsOracleValidationFunction(string functionName)
        => functionName.Equals("VALIDATE_CONVERSION", StringComparison.OrdinalIgnoreCase)
            && Version >= OracleValidateConversionMinVersion;
    /// <inheritdoc />
    public override bool SupportsOracleJsonTransformFunction(string functionName)
        => functionName.Equals("JSON_TRANSFORM", StringComparison.OrdinalIgnoreCase)
            && Version >= OracleJsonTransformMinVersion;
    /// <inheritdoc />
    public override bool SupportsOracleCollationFunction(string functionName)
        => functionName.Equals("COLLATION", StringComparison.OrdinalIgnoreCase)
            && Version >= OracleCollationFunctionMinVersion;
    /// <inheritdoc />
    public override bool SupportsOracleNlsFunction(string functionName)
        => functionName.ToUpperInvariant() switch
        {
            "NLS_CHARSET_DECL_LEN" or "NLS_CHARSET_ID" or "NLS_CHARSET_NAME"
                or "NLS_INITCAP" or "NLS_LOWER" or "NLS_UPPER" or "NLSSORT" => true,
            "NLS_COLLATION_ID" or "NLS_COLLATION_NAME" => Version >= OracleCollationFunctionMinVersion,
            _ => false
        };
    /// <inheritdoc />
    public override bool SupportsOracleHashFunction(string functionName)
        => functionName.ToUpperInvariant() switch
        {
            "ORA_HASH" => Version >= OracleOraHashMinVersion,
            "STANDARD_HASH" => Version >= OracleStandardHashMinVersion,
            _ => false
        };
    /// <inheritdoc />
    public override bool SupportsOracleSysFunction(string functionName)
        => functionName.ToUpperInvariant() switch
        {
            "SYS_GUID" or "SYS_CONTEXT" => true,
            "SYS_CONNECT_BY_PATH" or "SYS_DBURIGEN" or "SYS_EXTRACT_UTC" or "SYS_TYPEID" or "SYS_XMLAGG" or "SYS_XMLGEN"
                => Version >= OracleSysFamilyMinVersion,
            "SYS_OP_ZONE_ID" => Version >= OracleSysZoneIdMinVersion,
            _ => false
        };
    /// <inheritdoc />
    public override bool SupportsOracleTimeFunction(string functionName)
        => functionName.ToUpperInvariant() switch
        {
            "DBTIMEZONE" or "FROM_TZ" or "LOCALTIMESTAMP" or "SESSIONTIMEZONE" or "TZ_OFFSET" => Version >= OracleTemporalFunctionMinVersion,
            "NUMTODSINTERVAL" or "NUMTOYMINTERVAL" => Version >= OracleIntervalFunctionMinVersion,
            "NEW_TIME" or "NEXT_DAY" => true,
            _ => false
        };

    public override bool SupportsLastFoundRowsFunction(string functionName)
        => functionName.Equals("ROW_COUNT", StringComparison.OrdinalIgnoreCase);
}
