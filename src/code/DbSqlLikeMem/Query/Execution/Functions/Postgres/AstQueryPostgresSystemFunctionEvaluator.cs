namespace DbSqlLikeMem;

internal static class AstQueryPostgresSystemFunctionEvaluator
{
    private static readonly IReadOnlyDictionary<string, AstQueryGeneralScalarFunctionHandler> _handlers =
        CreateHandlers();

    internal static bool TryEvaluatePostgresSystemFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(context, fn, evalArg, out result);

        result = null;
        return false;
    }

    private static IReadOnlyDictionary<string, AstQueryGeneralScalarFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryGeneralScalarFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalCurrentDatabaseFunction, "CURRENT_DATABASE", "CURRENT_CATALOG");
        Register(handlers, TryEvalCurrentSchemaFunction, "CURRENT_SCHEMA");
        Register(handlers, TryEvalCurrentUserFunction, "CURRENT_ROLE", "CURRENT_USER");
        Register(handlers, TryEvalVersionFunction, "VERSION");
        Register(handlers, TryEvalCurrentSchemasFunction, "CURRENT_SCHEMAS");
        Register(handlers, TryEvalCurrentSettingFunction, "CURRENT_SETTING");
        Register(handlers, TryEvalCurrentQueryFunction, "CURRENT_QUERY");
        Register(handlers, TryEvalCurrentTimestampFunction, "CLOCK_TIMESTAMP", "STATEMENT_TIMESTAMP", "TRANSACTION_TIMESTAMP");
        return handlers;
    }

    internal static void RegisterHandlers(
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

    private static void Register(
        IDictionary<string, AstQueryGeneralScalarFunctionHandler> handlers,
        AstQueryGeneralScalarFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
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
        result = context.EvaluationUtcNow;
        return true;
    }
}
