namespace DbSqlLikeMem;

internal static class AstQueryPostgresSystemFunctionEvaluator
{
    internal static bool TryEvaluatePostgresSystemFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;
        if (context.Dialect.Functions.TryGetValue(fn.Name, out var handler)
            && handler.AstExecutor != null)
            return handler.AstExecutor(context, fn, evalArg, out result);

        return false;
    }

    internal static void CreateHandlers(
        this ISqlDialect dialect)
    {
        dialect.AddScalarFunctions("VARCHAR", TryEvalCurrentDatabaseFunction, "CURRENT_DATABASE", "CURRENT_CATALOG");
        dialect.AddScalarFunctions("VARCHAR", TryEvalCurrentSchemaFunction, "CURRENT_SCHEMA");
        dialect.AddScalarFunctions("VARCHAR", TryEvalCurrentUserFunction, "CURRENT_ROLE");
        dialect.AddScalarFunctions("VARCHAR", TryEvalCurrentUserFunction, "CURRENT_USER");
        dialect.AddScalarFunctions("VARCHAR", TryEvalVersionFunction, "VERSION");
        dialect.AddScalarFunctions("STRING_ARRAY", TryEvalCurrentSchemasFunction, "CURRENT_SCHEMAS");
        dialect.AddScalarFunctions("VARCHAR", TryEvalCurrentSettingFunction, "CURRENT_SETTING");
        dialect.AddScalarFunctions("VARCHAR", TryEvalCurrentQueryFunction, "CURRENT_QUERY");
        dialect.AddScalarFunctions("DATETIME", TryEvalCurrentTimestampFunction, "CLOCK_TIMESTAMP", "STATEMENT_TIMESTAMP", "TRANSACTION_TIMESTAMP");
    }

    private static bool TryEvalCurrentDatabaseFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = "postgres";
        return true;
    }

    private static bool TryEvalCurrentSchemaFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = "public";
        return true;
    }

    private static bool TryEvalCurrentUserFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = "user_postgres";
        return true;
    }

    private static bool TryEvalVersionFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = $"PostgreSQL {context.Dialect.Version}";
        return true;
    }

    private static bool TryEvalCurrentSchemasFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = new[] { "public" };
        return true;
    }

    private static bool TryEvalCurrentSettingFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var settingName = evalArg(0)?.ToString();
        if (string.IsNullOrWhiteSpace(settingName))
        {
            result = null;
            return true;
        }

        result = settingName!.Trim().ToLowerInvariant() switch
        {
            "application_name" => "DbSqlLikeMem",
            "search_path" => "\"$user\", public",
            "server_version" => context.Dialect.Version.ToString(CultureInfo.InvariantCulture),
            "server_version_num" => (context.Dialect.Version * 10000).ToString(CultureInfo.InvariantCulture),
            _ => null
        };
        return true;
    }

    private static bool TryEvalCurrentQueryFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = context.Connection.GetCurrentQueryText();
        return true;
    }

    private static bool TryEvalCurrentTimestampFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = DateTime.Now;
        return true;
    }
}
