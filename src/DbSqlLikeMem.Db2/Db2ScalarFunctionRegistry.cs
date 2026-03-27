using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Db2;

internal static class Db2ScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        SqlSharedScalarFunctionRegistry.Register(dialect);
        OracleDb2ScalarFunctionRegistry.Register(dialect);

        RegisterConversionFunctions(dialect);
        RegisterTemporalFunctions(dialect);
        RegisterAnalyticsFunctions(dialect);
        RegisterStringFunctions(dialect, version);
        RegisterRowCountFunctions(dialect);
    }

    private static void RegisterConversionFunctions(ISqlDialect dialect)
    {
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("TO_CLOB", "VARCHAR"),
            "TO_CLOB",
            "TO_NCHAR",
            "TO_NCLOB");
    }

    private static void RegisterTemporalFunctions(ISqlDialect dialect)
    {
        dialect.AddScalarFunction("CURDATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_DATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT DATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_TIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT TIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("CURRENT TIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);

        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("NEXT_DAY", "DATE"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("DATE_ADD", "DATE"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("TIMESTAMPADD", "DATE"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("EOMONTH", "DATE"));
    }

    private static void RegisterAnalyticsFunctions(ISqlDialect dialect)
    {
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("RATIO_TO_REPORT", "DOUBLE"));
    }

    private static void RegisterStringFunctions(ISqlDialect dialect, int version)
    {
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar(Db2Const.VALUE, "VARCHAR") with
            {
                AstExecutor = QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions
            },
            Db2Const.VALUE,
            "IFNULL",
            "NVL",
            "NVL2");

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("BITAND", "VARCHAR"),
            "BITAND",
            "BITANDNOT",
            "BITNOT",
            "BITOR",
            "BITXOR",
            SqlConst.LISTAGG);

        if (version >= Db2Dialect.JsonFunctionsMinVersion)
        {
            dialect.AddScalarFunctions(
                DbFunctionDef.CreateScalar("JSON_QUERY", "VARCHAR") with
                {
                    AstExecutor = AstQueryGeneralScalarFunctionEvaluator.TryEvalJsonExtractionFunction
                },
                "JSON_QUERY",
                "JSON_VALUE");
        }

        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("GROUPING", "INT"));

        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("TRANSLATE", "VARCHAR"));
    }

    private static void RegisterRowCountFunctions(ISqlDialect dialect)
    {
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("ROW_COUNT", "BIGINT"));
    }
}
