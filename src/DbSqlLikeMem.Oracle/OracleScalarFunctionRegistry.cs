using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Oracle;

internal static class OracleScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        SqlSharedScalarFunctionRegistry.Register(dialect);
        OracleDb2ScalarFunctionRegistry.Register(dialect);

        RegisterTemporalFunctions(dialect, version);
        RegisterConversionFunctions(dialect, version);
        RegisterAnalyticsFunctions(dialect, version);
        RegisterClusterFunctions(dialect, version);
        RegisterContainerFunctions(dialect, version);
        RegisterMetadataFunctions(dialect, version);
        RegisterHashFunctions(dialect, version);
        RegisterStringAggregateFunctions(dialect, version);
        RegisterSysFunctions(dialect, version);
        RegisterValidationFunctions(dialect, version);
        RegisterNlsFunctions(dialect, version);
        RegisterTimeFunctions(dialect, version);
        RegisterSequenceFunctions(dialect);
    }

    private static DbFunctionDef CreateScalarDefinition(
        string name,
        string returnTypeSql,
        AstQueryGeneralScalarFunctionHandler astExecutor)
        => DbFunctionDef.CreateScalar(name, returnTypeSql) with
        {
            AstExecutor = astExecutor
        };

    private static bool TryEvalOracleDb2ConversionFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryOracleDb2ConversionFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result);

    private static void RegisterTemporalFunctions(ISqlDialect dialect, int version)
    {
        dialect.AddScalarFunction("SYSDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);

        if (version >= OracleDialect.OracleTemporalFunctionMinVersion)
        {
            dialect.AddScalarFunctions(
                "DATE",
                SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction,
                DbInvocationStyle.Identifier,
                SqlTemporalFunctionKind.Date,
                "CURRENT_DATE");

            dialect.AddScalarFunctions(
                "DATETIME",
                SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction,
                DbInvocationStyle.Identifier,
                SqlTemporalFunctionKind.DateTime,
                "CURRENT_TIMESTAMP",
                "SYSTIMESTAMP");

            dialect.AddScalarFunctions(
                "DATETIME",
                SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction,
                DbInvocationStyle.Identifier,
                SqlTemporalFunctionKind.DateTime,
                "LOCALTIMESTAMP");
        }
    }

    private static void RegisterConversionFunctions(ISqlDialect dialect, int version)
    {
        if (version >= OracleDialect.OracleBinaryConversionMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("TO_BINARY_DOUBLE", "DOUBLE", TryEvalOracleDb2ConversionFunction),
                "TO_BINARY_DOUBLE",
                "TO_BINARY_FLOAT");

        if (version >= OracleDialect.OracleBlobConversionMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("TO_BLOB", "BLOB", TryEvalOracleDb2ConversionFunction),
                "TO_BLOB");

        if (version >= OracleDialect.OracleTextConversionMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("TO_CLOB", "CLOB", TryEvalOracleDb2ConversionFunction),
                "TO_CLOB",
                "TO_DSINTERVAL",
                "TO_NCHAR",
                "TO_NCLOB",
                "TO_TIMESTAMP_TZ",
                "TO_YMINTERVAL");

        dialect.AddScalarFunction(
            "TO_LOB",
            "CLOB",
            AstQueryOracleDb2ConversionFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunctions(
            "VARCHAR",
            AstQueryOracleDb2ConversionFunctionEvaluator.TryEvaluate,
            "TO_MULTI_BYTE",
            "TO_SINGLE_BYTE");
    }

    private static void RegisterAnalyticsFunctions(ISqlDialect dialect, int version)
    {
        if (version >= 8)
        {
            dialect.AddScalarFunction(
                "RATIO_TO_REPORT",
                "DOUBLE",
                AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);
        }

        if (version >= OracleDialect.ApproximateAnalyticsMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("FEATURE_COMPARE", "DOUBLE", AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate),
                "FEATURE_COMPARE",
                "FEATURE_DETAILS",
                "FEATURE_ID",
                "FEATURE_SET",
                "FEATURE_VALUE",
                "NCGR",
                "POWERMULTISET",
                "POWERMULTISET_BY_CARDINALITY",
                "PREDICTION",
                "PREDICTION_BOUNDS",
                "PREDICTION_COST",
                "PREDICTION_DETAILS",
                "PREDICTION_PROBABILITY",
                "PREDICTION_SET",
                "PRESENTNNV",
                "PRESENTV");

        if (version >= OracleDialect.ApproxCountDistinctMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("APPROX_COUNT_DISTINCT", "BIGINT", AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate),
                "APPROX_COUNT_DISTINCT");

        if (version >= OracleDialect.ApproximateAnalyticsMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("APPROX_COUNT_DISTINCT_AGG", "BIGINT", AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate),
                "APPROX_COUNT_DISTINCT_AGG",
                "APPROX_COUNT_DISTINCT_DETAIL",
                "APPROX_MEDIAN",
                "APPROX_PERCENTILE",
                "APPROX_PERCENTILE_AGG",
                "APPROX_PERCENTILE_DETAIL");

        if (version >= OracleDialect.ApproximateAnalyticsMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("TO_APPROX_COUNT_DISTINCT", "BIGINT", AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate),
                "TO_APPROX_COUNT_DISTINCT");

        if (version >= OracleDialect.ApproximateAnalyticsMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("TO_APPROX_PERCENTILE", "DOUBLE", AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate),
                "TO_APPROX_PERCENTILE");
    }

    private static void RegisterClusterFunctions(ISqlDialect dialect, int version)
    {
        if (version >= OracleDialect.OracleClusterFunctionMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("CLUSTER_ID", "DOUBLE", QueryOracleDb2UtilityFunctionHelper.TryEvalUtilityFunctions),
                "CLUSTER_ID",
                "CLUSTER_PROBABILITY",
                "CLUSTER_SET");

        if (version >= OracleDialect.OracleAdvancedClusterFunctionMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("CLUSTER_DETAILS", "DOUBLE", QueryOracleDb2UtilityFunctionHelper.TryEvalUtilityFunctions),
                "CLUSTER_DETAILS",
                "CLUSTER_DISTANCE");
    }

    private static void RegisterContainerFunctions(ISqlDialect dialect, int version)
    {
        if (version >= OracleDialect.OracleContainerFunctionMinVersion)
            dialect.AddScalarFunctions(
                DbFunctionDef.CreateScalar("CON_DBID_TO_ID", "INT"),
                "CON_DBID_TO_ID",
                "CON_GUID_TO_ID",
                "CON_NAME_TO_ID",
                "CON_UID_TO_ID");
    }

    private static void RegisterMetadataFunctions(ISqlDialect dialect, int version)
    {
        if (version >= OracleDialect.OracleUserEnvMetadataMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("ORA_INVOKING_USER", "VARCHAR", AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate),
                "ORA_INVOKING_USER",
                "ORA_INVOKING_USERID",
                "ORA_DST_AFFECTED",
                "ORA_DST_CONVERT",
                "ORA_DST_ERROR");

        if (version >= OracleDialect.OraclePartitionMetadataMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("ORA_DM_PARTITION_NAME", "VARCHAR", AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate),
                "ORA_DM_PARTITION_NAME");

        dialect.AddScalarFunction(
            "USERENV",
            "VARCHAR",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("ROW_COUNT", "BIGINT"));
    }

    private static void RegisterHashFunctions(ISqlDialect dialect, int version)
    {
        if (version >= OracleDialect.OracleOraHashMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("ORA_HASH", "VARCHAR", AstQueryOracleDb2BinaryTextFunctionEvaluator.TryEvaluate),
                "ORA_HASH");

        if (version >= OracleDialect.OracleStandardHashMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("STANDARD_HASH", "VARCHAR", AstQueryOracleDb2BinaryTextFunctionEvaluator.TryEvaluate),
                "STANDARD_HASH");
    }

    private static void RegisterSysFunctions(ISqlDialect dialect, int version)
    {
        if (version >= OracleDialect.OracleSysFamilyMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("SYS_CONNECT_BY_PATH", "VARCHAR", AstQueryOracleDb2SysFunctionEvaluator.TryEvaluate),
                "SYS_CONNECT_BY_PATH",
                "SYS_DBURIGEN",
                "SYS_EXTRACT_UTC",
                "SYS_TYPEID",
                "SYS_XMLAGG",
                "SYS_XMLGEN");

        dialect.AddScalarFunctions(
            "VARCHAR",
            AstQueryOracleDb2SysFunctionEvaluator.TryEvaluate,
            "SYS_CONTEXT",
            "SYS_GUID");

        if (version >= OracleDialect.OracleSysZoneIdMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("SYS_OP_ZONE_ID", "VARCHAR", AstQueryOracleDb2SysFunctionEvaluator.TryEvaluate),
                "SYS_OP_ZONE_ID");
    }

    private static void RegisterValidationFunctions(ISqlDialect dialect, int version)
    {
        if (version >= OracleDialect.OracleValidateConversionMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("VALIDATE_CONVERSION", "INT", AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate),
                "VALIDATE_CONVERSION");

        if (version >= OracleDialect.OracleJsonTransformMinVersion)
            dialect.AddScalarFunctions(
                DbFunctionDef.CreateScalar("JSON_TRANSFORM", "VARCHAR"),
                "JSON_TRANSFORM");

        if (version >= OracleDialect.OracleJsonSqlFunctionMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("JSON_QUERY", "VARCHAR", AstQueryGeneralScalarFunctionEvaluator.TryEvalJsonExtractionFunction),
                "JSON_QUERY",
                "JSON_VALUE");
    }

    private static void RegisterNlsFunctions(ISqlDialect dialect, int version)
    {
        if (version >= OracleDialect.OracleCollationFunctionMinVersion)
            dialect.AddScalarFunctions(
                DbFunctionDef.CreateScalar("COLLATION", "VARCHAR"),
                "COLLATION",
                "NLS_COLLATION_ID",
                "NLS_COLLATION_NAME");

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("NLS_CHARSET_DECL_LEN", "VARCHAR"),
            "NLS_CHARSET_DECL_LEN",
            "NLS_CHARSET_ID",
            "NLS_CHARSET_NAME",
            "NLS_INITCAP",
            "NLS_LOWER",
            "NLS_UPPER",
            "NLSSORT");
    }

    private static void RegisterTimeFunctions(ISqlDialect dialect, int version)
    {
        if (version >= OracleDialect.OracleTemporalFunctionMinVersion)
            dialect.AddScalarFunctions(
                CreateScalarDefinition("SESSIONTIMEZONE", "DATETIME", AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate),
                "SESSIONTIMEZONE",
                "TZ_OFFSET");

        if (version >= OracleDialect.OracleTemporalFunctionMinVersion)
            dialect.AddScalarFunctions(
                DbFunctionDef.CreateScalar("FROM_TZ", "DATETIME"),
                "FROM_TZ",
                "NEW_TIME",
                "NEXT_DAY");

        if (version >= OracleDialect.OracleIntervalFunctionMinVersion)
            dialect.AddScalarFunctions(
                DbFunctionDef.CreateScalar("NUMTODSINTERVAL", "VARCHAR"),
                "NUMTODSINTERVAL",
                "NUMTOYMINTERVAL");

        if (version >= OracleDialect.OracleScnFunctionMinVersion)
            dialect.AddScalarFunctions(
                DbFunctionDef.CreateScalar("SCN_TO_TIMESTAMP", "DATETIME"),
                "SCN_TO_TIMESTAMP",
                "TIMESTAMP_TO_SCN");
    }

    private static void RegisterSequenceFunctions(ISqlDialect dialect)
    {
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar(OracleConst.NEXTVAL, "BIGINT"),
            OracleConst.NEXTVAL,
            OracleConst.CURRVAL);
    }

    private static void RegisterStringAggregateFunctions(ISqlDialect dialect, int version)
    {
        if (version >= OracleDialect.WindowFunctionsMinVersion)
        {
            dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.LISTAGG, "VARCHAR"));
        }
    }
}
