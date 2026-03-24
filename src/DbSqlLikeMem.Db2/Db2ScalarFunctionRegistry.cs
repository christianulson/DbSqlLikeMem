using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Db2;

internal static class Db2ScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        SqlSharedScalarFunctionRegistry.Register(dialect);
        OracleDb2ScalarFunctionRegistry.Register(dialect);

        var body = SqlFunctionBodyFactory.Identity();

        RegisterConversionFunctions(dialect, body);
        RegisterTemporalFunctions(dialect, body);
        RegisterAnalyticsFunctions(dialect, body);
        RegisterStringFunctions(dialect, version, body);
        RegisterRowCountFunctions(dialect, body);
    }

    private static void RegisterConversionFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctions("VARCHAR", body,
            "TO_CLOB",
            "TO_NCHAR",
            "TO_NCLOB");
    }

    private static void RegisterTemporalFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunction("CURDATE", "DATE", body, SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_DATE", "DATE", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT DATE", "DATE", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_TIME", "TIME", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT TIME", "TIME", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("CURRENT TIMESTAMP", "DATETIME", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);

        dialect.AddScalarFunction("NEXT_DAY", "DATE", body);
        dialect.AddScalarFunction("DATE_ADD", "DATE", body);
        dialect.AddScalarFunction("TIMESTAMPADD", "DATE", body);
        dialect.AddScalarFunction("EOMONTH", "DATE", body);
    }

    private static void RegisterAnalyticsFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunction("RATIO_TO_REPORT", "DOUBLE", body);
    }

    private static void RegisterStringFunctions(ISqlDialect dialect, int version, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctions("VARCHAR", body,
            "VALUE",
            "NVL",
            "NVL2",
            "BITAND",
            "BITANDNOT",
            "BITNOT",
            "BITOR",
            "BITXOR",
            "LISTAGG");

        dialect.AddScalarFunctionsIf(version >= Db2Dialect.JsonFunctionsMinVersion, "VARCHAR", body,
            "JSON_QUERY",
            "JSON_VALUE");

        dialect.AddScalarFunctions("INT", body,
            "GROUPING");

        dialect.AddScalarFunctions("VARCHAR", body,
            "TRANSLATE");
    }

    private static void RegisterRowCountFunctions(ISqlDialect dialect, Func<SqlExpr, object> body)
    {
        dialect.AddScalarFunctions("BIGINT", body,
            "ROW_COUNT");
    }
}
