using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Auto;

internal static class AutoScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        dialect.AddScalarFunction("CURRENT_DATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_TIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("NOW", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("GETDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("GETUTCDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSDATETIME", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunctions(
            new DbScalarFunctionDef("IF", "VARCHAR", [], SqlFunctionBodyFactory.Identity())
            {
                AstExecutor = QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions
            },
            "IF",
            "IIF",
            "IFNULL",
            "ISNULL",
            "NVL");

        dialect.AddScalarFunction(
            new DbScalarFunctionDef("ADDDATE", "DATETIME", [], SqlFunctionBodyFactory.Identity())
            {
                AstExecutor = AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate
            });
        dialect.AddScalarFunction(
            new DbScalarFunctionDef("ADDTIME", "DATETIME", [], SqlFunctionBodyFactory.Identity())
            {
                AstExecutor = AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate
            });
        dialect.AddScalarFunctions("DATETIME", SqlFunctionBodyFactory.Identity(),
            SqlScalarFunctionUsageKind.Call,
            null,
            "DATE_ADD",
            "DATEADD",
            "TIMESTAMPADD");

        dialect.AddScalarFunction("TRY_CAST", "VARCHAR", SqlFunctionBodyFactory.Identity());
        dialect.AddScalarFunction("TRY_CONVERT", "VARCHAR", SqlFunctionBodyFactory.Identity());
        dialect.AddScalarFunction(
            new DbScalarFunctionDef("EOMONTH", "DATE", [], SqlFunctionBodyFactory.Identity())
            {
                AstExecutor = AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate
            });
        dialect.AddScalarFunctions(
            new DbScalarFunctionDef("JSON_EXTRACT", "VARCHAR", [], SqlFunctionBodyFactory.Identity())
            {
                AstExecutor = AstQueryExecutorBase.TryEvalJsonExtractionFunction
            },
            "JSON_EXTRACT",
            "JSON_QUERY",
            "JSON_VALUE");
        dialect.AddScalarFunction("JSON_UNQUOTE", "VARCHAR", SqlFunctionBodyFactory.Identity());

        dialect.AddScalarFunctions("VARCHAR", SqlFunctionBodyFactory.Identity(),
            SqlScalarFunctionUsageKind.Call,
            null,
            "GROUP_CONCAT",
            "STRING_AGG",
            "LISTAGG");

        dialect.AddScalarFunctions("BIGINT", SqlFunctionBodyFactory.Identity(),
            SqlScalarFunctionUsageKind.Call,
            null,
            SqlConst.NEXTVAL,
            SqlConst.CURRVAL,
            SqlConst.SETVAL,
            SqlConst.LASTVAL);
    }
}
