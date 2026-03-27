using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Auto;

internal static class AutoScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        dialect.AddScalarFunction("CURRENT_DATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_TIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("NOW", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("GETDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("GETUTCDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSDATETIME", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("IIF", "VARCHAR") with
            {
                AstExecutor = QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions
            },
            "IF",
            "IIF",
            "IFNULL",
            "ISNULL",
            "NVL");

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ADDDATE", "DATETIME") with
            {
                AstExecutor = AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate
            });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ADDTIME", "DATETIME") with
            {
                AstExecutor = AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate
            });
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("DATE_ADD", "DATETIME"),
            "DATE_ADD",
            "DATEADD",
            "TIMESTAMPADD");

        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("TRY_CAST", "VARCHAR"));
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("TRY_CONVERT", "VARCHAR"));
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("EOMONTH", "DATE") with
            {
                AstExecutor = AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate
            });
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("JSON_EXTRACT", "VARCHAR") with
            {
                AstExecutor = AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction
            },
            "JSON_EXTRACT",
            "JSON_QUERY",
            "JSON_VALUE");
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("JSON_UNQUOTE", "VARCHAR"));

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar(SqlConst.GROUP_CONCAT, "VARCHAR") with
            {
                IsStringAggregate = true
            },
            SqlConst.GROUP_CONCAT,
            SqlConst.STRING_AGG,
            SqlConst.LISTAGG);

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar(SqlConst.NEXTVAL, "BIGINT"),
            SqlConst.NEXTVAL,
            SqlConst.CURRVAL,
            SqlConst.SETVAL,
            SqlConst.LASTVAL);
    }
}
