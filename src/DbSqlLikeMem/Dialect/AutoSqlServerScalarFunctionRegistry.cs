namespace DbSqlLikeMem;

internal static class AutoSqlServerScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        bool TryEvalCurrentUserFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = fn;
            _ = context;
            _ = evalArg;
            result = "dbo";
            return true;
        }

        var squareFunction = DbFunctionDef.CreateScalar("SQUARE", "DOUBLE") with
        {
            AstExecutor = AstQuerySqlServerScalarFunctionEvaluator.TryEvaluate
        };
        dialect.AddScalarFunction(squareFunction);

        dialect.AddScalarFunctions("INT", AstQuerySharedTextFunctionEvaluator.TryEvaluate,
            "LEN");
        dialect.AddScalarFunction("UNICODE", "INT", AstQuerySharedTextFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction("DIFFERENCE", "INT", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalDifferenceFunction);

        dialect.AddScalarFunctions("VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate,
            "LTRIM",
            "REVERSE",
            "RTRIM");
        dialect.AddScalarFunction("SPACE", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction("SOUNDEX", "VARCHAR", AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSoundexFunction);

        var parsenameFunction = DbFunctionDef.CreateScalar("PARSENAME", "VARCHAR") with
        {
            AstExecutor = AstQuerySqlServerScalarFunctionEvaluator.TryEvaluate
        };
        dialect.AddScalarFunctions(
            parsenameFunction,
            "PARSENAME",
            "QUOTENAME",
            "REPLICATE",
            "STUFF");

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("DB_ID", "INT"),
            "DB_ID",
            "SCHEMA_ID",
            "SCOPE_IDENTITY",
            "SESSION_ID",
            "SUSER_ID",
            "USER_ID",
            "XACT_STATE");

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("DB_NAME", "VARCHAR"),
            "DB_NAME",
            "SCHEMA_NAME",
            "SERVERPROPERTY",
            "SUSER_NAME",
            "SUSER_SNAME",
            "USER_NAME");

        var currentUserFunction = DbFunctionDef.CreateIdentifier("CURRENT_USER", "VARCHAR") with
        {
            AstExecutor = TryEvalCurrentUserFunction
        };
        dialect.AddScalarFunction(currentUserFunction);
        var sessionUserFunction = DbFunctionDef.CreateIdentifier("SESSION_USER", "VARCHAR") with
        {
            AstExecutor = AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSessionUserFunction
        };
        dialect.AddScalarFunction(sessionUserFunction);
        var systemUserFunction = DbFunctionDef.CreateIdentifier("SYSTEM_USER", "VARCHAR") with
        {
            AstExecutor = AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSystemUserFunction
        };
        dialect.AddScalarFunction(systemUserFunction);

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateIdentifier("@@DATEFIRST", "INT"),
            "@@DATEFIRST",
            "@@MAX_PRECISION",
            "@@TEXTSIZE",
            "@@ROWCOUNT");

        dialect.AddScalarFunction(
            DbFunctionDef.CreateIdentifier("@@IDENTITY", "BIGINT"));

        dialect.AddScalarFunction("DATEFROMPARTS", "DATE", AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction);
        dialect.AddScalarFunction("DATETIMEFROMPARTS", "DATETIME", AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction);
        dialect.AddScalarFunction("DATETIME2FROMPARTS", "DATETIME", AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction);
        dialect.AddScalarFunction("DATETIMEOFFSETFROMPARTS", "DATETIMEOFFSET", AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction);
        dialect.AddScalarFunction("TIMEFROMPARTS", "TIME", AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction);
        dialect.AddScalarFunction("SMALLDATETIMEFROMPARTS", "DATETIME", AstQuerySqlServerDateConstructionFunctionEvaluator.TryEvaluateSqlServerDateConstructionFunction);

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("FOUND_ROWS", "BIGINT"),
            "FOUND_ROWS",
            "ROW_COUNT",
            "CHANGES",
            "ROWCOUNT",
            "ROWCOUNT_BIG");

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("NEXT_VALUE_FOR", "BIGINT"),
            "NEXT_VALUE_FOR",
            "PREVIOUS_VALUE_FOR");

        dialect.AddScalarFunction(DbFunctionDef.CreateScalar(SqlConst.CHECKSUM_AGG, "INT"));
    }
}
