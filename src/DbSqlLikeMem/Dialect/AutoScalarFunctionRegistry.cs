namespace DbSqlLikeMem;

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
        var conditionalNullFunction = DbFunctionDef.CreateScalar("IIF", "VARCHAR") with
        {
            AstExecutor = QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions
        };
        dialect.AddScalarFunctions(
            conditionalNullFunction,
            "IF",
            "IIF",
            "IFNULL",
            "ISNULL",
            "NVL");

        var addDateFunction = DbFunctionDef.CreateScalar("ADDDATE", "DATETIME") with
        {
            AstExecutor = AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate
        };
        dialect.AddScalarFunction(addDateFunction);
        var addTimeFunction = DbFunctionDef.CreateScalar("ADDTIME", "DATETIME") with
        {
            AstExecutor = AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate
        };
        dialect.AddScalarFunction(addTimeFunction);
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("DATE_ADD", "DATETIME"),
            "DATE_ADD",
            "DATEADD",
            "TIMESTAMPADD");

        var tryCastFunction = DbFunctionDef.CreateScalar("TRY_CAST", "VARCHAR") with
        {
            AstExecutor = AstQueryCastConversionFamilyEvaluator.TryEvalTryCastLikeFunction
        };
        dialect.AddScalarFunction(tryCastFunction);

        var tryConvertFunction = DbFunctionDef.CreateScalar("TRY_CONVERT", "VARCHAR") with
        {
            AstExecutor = AstQueryCastConversionFamilyEvaluator.TryEvalTryConvertLikeFunction
        };
        dialect.AddScalarFunction(tryConvertFunction);
        static bool TryEvalSqlServerEomonthFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;
            return AstQuerySqlServerCompatibilityFunctionEvaluator.TryEvalEomonthFunction(fn, evalArg, out result);
        }

        var eomonthFunction = DbFunctionDef.CreateScalar("EOMONTH", "DATE") with
        {
            AstExecutor = TryEvalSqlServerEomonthFunction
        };
        dialect.AddScalarFunction(eomonthFunction);
        var jsonExtractFunction = DbFunctionDef.CreateScalar("JSON_EXTRACT", "VARCHAR") with
        {
            AstExecutor = AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction
        };
        dialect.AddScalarFunctions(
            jsonExtractFunction,
            "JSON_EXTRACT",
            "JSON_QUERY",
            "JSON_VALUE");
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("JSON_UNQUOTE", "VARCHAR"));
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("JSON_OBJECT", "VARCHAR") with
            {
                AstExecutor = AstQueryJsonObjectFunctionEvaluator.TryEvalJsonObjectFunction
            });

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar(SqlConst.GROUP_CONCAT, "VARCHAR"),
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
