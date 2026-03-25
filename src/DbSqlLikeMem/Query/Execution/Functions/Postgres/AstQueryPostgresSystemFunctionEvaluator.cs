using System.Globalization;

namespace DbSqlLikeMem;

internal static class AstQueryPostgresSystemFunctionEvaluator
{
    private delegate bool PostgresSystemFunctionHandler(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string?> getCurrentQueryText,
        out object? result);

    private static readonly IReadOnlyDictionary<string, PostgresSystemFunctionHandler> _handlers = CreateHandlers();

    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string?> getCurrentQueryText,
        out object? result)
    {
        result = null;
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(fn, context, evalArg, getCurrentQueryText, out result);

        return false;
    }

    private static Dictionary<string, PostgresSystemFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, PostgresSystemFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalCurrentDatabaseFunction, "CURRENT_DATABASE", "CURRENT_CATALOG");
        Register(handlers, TryEvalCurrentSchemaFunction, "CURRENT_SCHEMA");
        Register(handlers, TryEvalCurrentUserFunction, "CURRENT_USER", "CURRENT_ROLE");
        Register(handlers, TryEvalVersionFunction, "VERSION");
        Register(handlers, TryEvalCurrentSchemasFunction, "CURRENT_SCHEMAS");
        Register(handlers, TryEvalCurrentSettingFunction, "CURRENT_SETTING");
        Register(handlers, TryEvalCurrentQueryFunction, "CURRENT_QUERY");
        Register(handlers, TryEvalCurrentTimestampFunction, "CLOCK_TIMESTAMP", "STATEMENT_TIMESTAMP", "TRANSACTION_TIMESTAMP");
        return handlers;
    }

    private static void Register(
        IDictionary<string, PostgresSystemFunctionHandler> handlers,
        PostgresSystemFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalCurrentDatabaseFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string?> getCurrentQueryText,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        _ = getCurrentQueryText;
        result = "postgres";
        return true;
    }

    private static bool TryEvalCurrentSchemaFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string?> getCurrentQueryText,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        _ = getCurrentQueryText;
        result = "public";
        return true;
    }

    private static bool TryEvalCurrentUserFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string?> getCurrentQueryText,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        _ = getCurrentQueryText;
        result = "postgres";
        return true;
    }

    private static bool TryEvalVersionFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string?> getCurrentQueryText,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        _ = getCurrentQueryText;
        result = $"PostgreSQL {context.Dialect.Version}";
        return true;
    }

    private static bool TryEvalCurrentSchemasFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string?> getCurrentQueryText,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        _ = getCurrentQueryText;
        result = new[] { "public" };
        return true;
    }

    private static bool TryEvalCurrentSettingFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string?> getCurrentQueryText,
        out object? result)
    {
        _ = getCurrentQueryText;
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
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string?> getCurrentQueryText,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = getCurrentQueryText();
        return true;
    }

    private static bool TryEvalCurrentTimestampFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string?> getCurrentQueryText,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        _ = getCurrentQueryText;
        result = DateTime.Now;
        return true;
    }
}
