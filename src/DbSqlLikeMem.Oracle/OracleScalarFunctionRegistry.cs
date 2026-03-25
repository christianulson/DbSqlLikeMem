using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Oracle;

internal static class OracleScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        SqlSharedScalarFunctionRegistry.Register(dialect);
        OracleDb2ScalarFunctionRegistry.Register(dialect);

        var body = SqlFunctionBodyFactory.Identity();

        RegisterTemporalFunctions(dialect, version, body);
        RegisterConversionFunctions(dialect, version, body);
        RegisterAnalyticsFunctions(dialect, version, body);
        RegisterClusterFunctions(dialect, version, body);
        RegisterContainerFunctions(dialect, version, body);
        RegisterMetadataFunctions(dialect, version, body);
        RegisterHashFunctions(dialect, version, body);
        RegisterStringAggregateFunctions(dialect, version, body);
        RegisterSysFunctions(dialect, version, body);
        RegisterValidationFunctions(dialect, version, body);
        RegisterNlsFunctions(dialect, version, body);
        RegisterTimeFunctions(dialect, version, body);
        RegisterSequenceFunctions(dialect, body);
    }

    private static void RegisterTemporalFunctions(ISqlDialect dialect, int version, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunction("SYSDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);

        dialect.AddScalarFunctionsIf(version >= OracleDialect.OracleTemporalFunctionMinVersion, "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction,
            SqlScalarFunctionUsageKind.Identifier,
            SqlTemporalFunctionKind.Date,
            "CURRENT_DATE");

        dialect.AddScalarFunctionsIf(version >= OracleDialect.OracleTemporalFunctionMinVersion, "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction,
            SqlScalarFunctionUsageKind.Identifier,
            SqlTemporalFunctionKind.DateTime,
            "CURRENT_TIMESTAMP",
            "SYSTIMESTAMP");

        dialect.AddScalarFunctionsIf(version >= OracleDialect.OracleTemporalFunctionMinVersion, "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction,
            SqlScalarFunctionUsageKind.Identifier,
            SqlTemporalFunctionKind.DateTime,
            "LOCALTIMESTAMP");
    }

    private static void RegisterConversionFunctions(ISqlDialect dialect, int version, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.OracleBinaryConversionMinVersion,
            "DOUBLE",
            AstQueryOracleDb2ConversionFunctionEvaluator.TryEvaluate,
            "TO_BINARY_DOUBLE",
            "TO_BINARY_FLOAT");

        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.OracleBlobConversionMinVersion,
            "BLOB",
            AstQueryOracleDb2ConversionFunctionEvaluator.TryEvaluate,
            "TO_BLOB");

        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.OracleTextConversionMinVersion,
            "CLOB",
            AstQueryOracleDb2ConversionFunctionEvaluator.TryEvaluate,
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

    private static void RegisterAnalyticsFunctions(ISqlDialect dialect, int version, Func<SqlExpr, object> body)
    {
        if (version >= 8)
        {
            dialect.AddScalarFunction(
                "RATIO_TO_REPORT",
                "DOUBLE",
                AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);
        }

        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            "DOUBLE",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate,
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

        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.ApproxCountDistinctMinVersion,
            "BIGINT",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate,
            "APPROX_COUNT_DISTINCT");

        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            "BIGINT",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate,
            "APPROX_COUNT_DISTINCT_AGG",
            "APPROX_COUNT_DISTINCT_DETAIL",
            "APPROX_MEDIAN",
            "APPROX_PERCENTILE",
            "APPROX_PERCENTILE_AGG",
            "APPROX_PERCENTILE_DETAIL");

        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            "BIGINT",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate,
            "TO_APPROX_COUNT_DISTINCT");

        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            "DOUBLE",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate,
            "TO_APPROX_PERCENTILE");
    }

    private static void RegisterClusterFunctions(ISqlDialect dialect, int version, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.OracleClusterFunctionMinVersion,
            "DOUBLE",
            QueryOracleDb2UtilityFunctionHelper.TryEvalUtilityFunctions,
            "CLUSTER_ID",
            "CLUSTER_PROBABILITY",
            "CLUSTER_SET");

        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.OracleAdvancedClusterFunctionMinVersion,
            "DOUBLE",
            QueryOracleDb2UtilityFunctionHelper.TryEvalUtilityFunctions,
            "CLUSTER_DETAILS",
            "CLUSTER_DISTANCE");
    }

    private static void RegisterContainerFunctions(ISqlDialect dialect, int version, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctionsIf(version >= OracleDialect.OracleContainerFunctionMinVersion, "INT", body,
            "CON_DBID_TO_ID",
            "CON_GUID_TO_ID",
            "CON_NAME_TO_ID",
            "CON_UID_TO_ID");
    }

    private static void RegisterMetadataFunctions(ISqlDialect dialect, int version, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.OracleUserEnvMetadataMinVersion,
            "VARCHAR",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate,
            "ORA_INVOKING_USER",
            "ORA_INVOKING_USERID",
            "ORA_DST_AFFECTED",
            "ORA_DST_CONVERT",
            "ORA_DST_ERROR");

        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.OraclePartitionMetadataMinVersion,
            "VARCHAR",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate,
            "ORA_DM_PARTITION_NAME");

        dialect.AddScalarFunction(
            "USERENV",
            "VARCHAR",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction("ROW_COUNT", "BIGINT", body);
    }

    private static void RegisterHashFunctions(ISqlDialect dialect, int version, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.OracleOraHashMinVersion,
            "VARCHAR",
            AstQueryOracleDb2BinaryTextFunctionEvaluator.TryEvaluate,
            "ORA_HASH");

        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.OracleStandardHashMinVersion,
            "VARCHAR",
            AstQueryOracleDb2BinaryTextFunctionEvaluator.TryEvaluate,
            "STANDARD_HASH");
    }

    private static void RegisterSysFunctions(ISqlDialect dialect, int version, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.OracleSysFamilyMinVersion,
            "VARCHAR",
            AstQueryOracleDb2SysFunctionEvaluator.TryEvaluate,
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

        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.OracleSysZoneIdMinVersion,
            "VARCHAR",
            AstQueryOracleDb2SysFunctionEvaluator.TryEvaluate,
            "SYS_OP_ZONE_ID");
    }

    private static void RegisterValidationFunctions(ISqlDialect dialect, int version, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.OracleValidateConversionMinVersion,
            "INT",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate,
            "VALIDATE_CONVERSION");

        dialect.AddScalarFunctionsIf(version >= OracleDialect.OracleJsonTransformMinVersion, "VARCHAR", body,
            "JSON_TRANSFORM");

        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.OracleJsonSqlFunctionMinVersion,
            "VARCHAR",
            AstQueryExecutorBase.TryEvalJsonExtractionFunction,
            "JSON_QUERY",
            "JSON_VALUE");
    }

    private static void RegisterNlsFunctions(ISqlDialect dialect, int version, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctionsIf(version >= OracleDialect.OracleCollationFunctionMinVersion, "VARCHAR", body,
            "COLLATION",
            "NLS_COLLATION_ID",
            "NLS_COLLATION_NAME");

        dialect.AddScalarFunctions("VARCHAR", body,
            "NLS_CHARSET_DECL_LEN",
            "NLS_CHARSET_ID",
            "NLS_CHARSET_NAME",
            "NLS_INITCAP",
            "NLS_LOWER",
            "NLS_UPPER",
            "NLSSORT");
    }

    private static void RegisterTimeFunctions(ISqlDialect dialect, int version, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctionsIf(
            version >= OracleDialect.OracleTemporalFunctionMinVersion,
            "DATETIME",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate,
            "SESSIONTIMEZONE",
            "TZ_OFFSET");

        dialect.AddScalarFunctionsIf(version >= OracleDialect.OracleTemporalFunctionMinVersion, "DATETIME", body,
            "FROM_TZ",
            "NEW_TIME",
            "NEXT_DAY");

        dialect.AddScalarFunctionsIf(version >= OracleDialect.OracleIntervalFunctionMinVersion, "VARCHAR", body,
            "NUMTODSINTERVAL",
            "NUMTOYMINTERVAL");

        dialect.AddScalarFunctionsIf(version >= OracleDialect.OracleScnFunctionMinVersion, "DATETIME", body,
            "SCN_TO_TIMESTAMP",
            "TIMESTAMP_TO_SCN");
    }

    private static void RegisterSequenceFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctions(
            "BIGINT",
            body,
            SqlScalarFunctionUsageKind.Call,
            null,
            SqlConst.NEXTVAL,
            SqlConst.CURRVAL);
    }

    private static void RegisterStringAggregateFunctions(ISqlDialect dialect, int version, Func<SqlExpr, object> body)
    {
        if (version >= OracleDialect.WindowFunctionsMinVersion)
        {
            dialect.AddScalarFunction("LISTAGG", "VARCHAR", body);
        }
    }
}
