namespace DbSqlLikeMem;

internal static class AutoSqlServerScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        bool TryEvalCurrentUserFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQuerySqlServerUtilityFunctionEvaluator.TryEvalCurrentUserFunction(fn, context, evalArg, out result);

        bool TryEvalSessionUserFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryGeneralSystemAndJsonFunctionEvaluator.TryEvalSessionUserFunction(fn, context, evalArg, out result);

        bool TryEvalSystemUserFunction(
            FunctionCallExpr fn,
            QueryExecutionContext context,
            Func<int, object?> evalArg,
            out object? result)
            => AstQueryGeneralSystemAndJsonFunctionEvaluator.TryEvalSystemUserFunction(fn, context, evalArg, out result);

        dialect.AddScalarFunctions("DOUBLE", AstQueryGeneralScalarFunctionEvaluator.TryEvaluate,
            "COT",
            "DEGREES",
            "EXP",
            "FLOOR",
            "LOG",
            "LOG10",
            "PI",
            "POWER",
            "RADIANS",
            "RAND",
            "ROUND",
            "SIN",
            "TAN",
            "SQRT");

        dialect.AddScalarFunction(
            new DbScalarFunctionDef("SQUARE", "DOUBLE", [], SqlFunctionBodyFactory.Identity())
            {
                AstExecutor = AstQuerySqlServerScalarFunctionEvaluator.TryEvaluate
            });

        dialect.AddScalarFunctions("INT", AstQueryGeneralScalarFunctionEvaluator.TryEvaluate,
            "DIFFERENCE",
            "LEN",
            "UNICODE");

        dialect.AddScalarFunctions("VARCHAR", AstQueryGeneralScalarFunctionEvaluator.TryEvaluate,
            "LTRIM",
            "REVERSE",
            "RTRIM",
            "SOUNDEX",
            "SPACE");

        dialect.AddScalarFunctions(
            new DbScalarFunctionDef("PARSENAME", "VARCHAR", [], SqlFunctionBodyFactory.Identity())
            {
                AstExecutor = AstQuerySqlServerScalarFunctionEvaluator.TryEvaluate
            },
            "PARSENAME",
            "QUOTENAME",
            "REPLICATE",
            "STUFF");

        dialect.AddScalarFunctions("INT", SqlFunctionBodyFactory.Identity(),
            "DB_ID",
            "SCHEMA_ID",
            "SCOPE_IDENTITY",
            "SESSION_ID",
            "SUSER_ID",
            "USER_ID",
            "XACT_STATE");

        dialect.AddScalarFunctions("VARCHAR", SqlFunctionBodyFactory.Identity(),
            "DB_NAME",
            "SCHEMA_NAME",
            "SERVERPROPERTY",
            "SUSER_NAME",
            "SUSER_SNAME",
            "USER_NAME");

        dialect.AddScalarFunction(
            new DbScalarFunctionDef("CURRENT_USER", "VARCHAR", [], SqlFunctionBodyFactory.Identity(), SqlScalarFunctionUsageKind.Identifier)
            {
                AstExecutor = TryEvalCurrentUserFunction
            });
        dialect.AddScalarFunction(
            new DbScalarFunctionDef("SESSION_USER", "VARCHAR", [], SqlFunctionBodyFactory.Identity(), SqlScalarFunctionUsageKind.Identifier)
            {
                AstExecutor = TryEvalSessionUserFunction
            });
        dialect.AddScalarFunction(
            new DbScalarFunctionDef("SYSTEM_USER", "VARCHAR", [], SqlFunctionBodyFactory.Identity(), SqlScalarFunctionUsageKind.Identifier)
            {
                AstExecutor = TryEvalSystemUserFunction
            });

        dialect.AddScalarFunctions(
            new DbScalarFunctionDef("@@DATEFIRST", "INT", [], SqlFunctionBodyFactory.Identity(), SqlScalarFunctionUsageKind.Identifier),
            "@@DATEFIRST",
            "@@MAX_PRECISION",
            "@@TEXTSIZE",
            "@@ROWCOUNT");

        dialect.AddScalarFunction(
            new DbScalarFunctionDef("@@IDENTITY", "BIGINT", [], SqlFunctionBodyFactory.Identity(), SqlScalarFunctionUsageKind.Identifier));

        dialect.AddScalarFunction("DATEFROMPARTS", "DATE", AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("DATETIMEFROMPARTS", "DATETIME", AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("DATETIME2FROMPARTS", "DATETIME", AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("DATETIMEOFFSETFROMPARTS", "DATETIMEOFFSET", AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("TIMEFROMPARTS", "TIME", AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("SMALLDATETIMEFROMPARTS", "DATETIME", AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunctions(
            new DbScalarFunctionDef("FOUND_ROWS", "BIGINT", [], SqlFunctionBodyFactory.Identity()),
            "FOUND_ROWS",
            "ROW_COUNT",
            "CHANGES",
            "ROWCOUNT",
            "ROWCOUNT_BIG");

        dialect.AddScalarFunctions(
            new DbScalarFunctionDef("NEXT_VALUE_FOR", "BIGINT", [], SqlFunctionBodyFactory.Identity()),
            "NEXT_VALUE_FOR",
            "PREVIOUS_VALUE_FOR");

        dialect.AddScalarFunction("CHECKSUM_AGG", "INT", SqlFunctionBodyFactory.Identity());
    }
}
