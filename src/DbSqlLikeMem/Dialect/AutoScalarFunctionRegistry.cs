namespace DbSqlLikeMem;

internal static class AutoScalarFunctionRegistry
{
    internal static void Register(AutoSqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        var body = SqlFunctionBodyFactory.Identity();

        dialect.AddScalarFunction("CURRENT_DATE", "DATE", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_TIME", "TIME", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("NOW", "DATETIME", body, SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSDATE", "DATETIME", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("GETDATE", "DATETIME", body, SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("GETUTCDATE", "DATETIME", body, SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSDATETIME", "DATETIME", body, SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTIMESTAMP", "DATETIME", body, SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("IF", "VARCHAR", QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);
        dialect.AddScalarFunction("IIF", "VARCHAR", QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);
        dialect.AddScalarFunction("IFNULL", "VARCHAR", QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);
        dialect.AddScalarFunction("ISNULL", "VARCHAR", QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);
        dialect.AddScalarFunction("NVL", "VARCHAR", QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);

        dialect.AddScalarFunction("ADDDATE", "DATETIME", AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("ADDTIME", "DATETIME", AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunctions("DATETIME", body,
            SqlScalarFunctionUsageKind.Call,
            null,
            "DATE_ADD",
            "DATEADD",
            "TIMESTAMPADD");

        dialect.AddScalarFunction("TRY_CAST", "VARCHAR", body);
        dialect.AddScalarFunction("TRY_CONVERT", "VARCHAR", body);
        dialect.AddScalarFunction("EOMONTH", "DATE", AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("JSON_EXTRACT", "VARCHAR", AstQueryExecutorBase.TryEvalJsonExtractionFunction);
        dialect.AddScalarFunction("JSON_QUERY", "VARCHAR", AstQueryExecutorBase.TryEvalJsonExtractionFunction);
        dialect.AddScalarFunction("JSON_VALUE", "VARCHAR", AstQueryExecutorBase.TryEvalJsonExtractionFunction);
        dialect.AddScalarFunction("JSON_UNQUOTE", "VARCHAR", body);

        dialect.AddScalarFunctions("VARCHAR", body,
            SqlScalarFunctionUsageKind.Call,
            null,
            "GROUP_CONCAT",
            "STRING_AGG",
            "LISTAGG");

        dialect.AddScalarFunctions("BIGINT", body,
            SqlScalarFunctionUsageKind.Call,
            null,
            SqlConst.NEXTVAL,
            SqlConst.CURRVAL,
            SqlConst.SETVAL,
            SqlConst.LASTVAL);
    }
}
