namespace DbSqlLikeMem;

internal delegate bool AstQueryTryEvalOracleDb2SysFunction(
    QueryExecutionContext context,
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal static class AstQueryOracleDb2SysFunctionEvaluator
{
    private static readonly IReadOnlyDictionary<string, AstQueryTryEvalOracleDb2SysFunction> _handlers =
        CreateHandlers();

    internal static bool TryEvaluate(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!_handlers.TryGetValue(fn.Name, out var handler))
        {
            result = null;
            return false;
        }

        context.EnsureOracleDb2FunctionSupported(fn.Name);
        return handler(context, fn, evalArg, out result);
    }

    private static Dictionary<string, AstQueryTryEvalOracleDb2SysFunction> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryTryEvalOracleDb2SysFunction>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalSysGuidFunction, "SYS_GUID");
        Register(handlers, TryEvalSysExtractUtcFunction, "SYS_EXTRACT_UTC");
        Register(handlers, TryEvalSysContextFunction, "SYS_CONTEXT");
        Register(handlers, TryEvalUnsupportedSysFunction,
            "SYS_CONNECT_BY_PATH",
            "SYS_DBURIGEN",
            "SYS_OP_ZONE_ID",
            "SYS_TYPEID",
            "SYS_XMLAGG",
            "SYS_XMLGEN");
        return handlers;
    }

    private static void Register(
        IDictionary<string, AstQueryTryEvalOracleDb2SysFunction> handlers,
        AstQueryTryEvalOracleDb2SysFunction handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalSysGuidFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = Guid.NewGuid().ToString("D");
        return true;
    }

    private static bool TryEvalSysExtractUtcFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is DateTimeOffset dto)
        {
            result = dto.UtcDateTime;
            return true;
        }

        if (AstQueryExecutorBase.TryCoerceDateTime(value!, out var dt))
        {
            result = dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime();
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalSysContextFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var namespaceValue = evalArg(0)?.ToString();
        var parameterValue = evalArg(1)?.ToString();
        if (string.Equals(namespaceValue, "USERENV", StringComparison.OrdinalIgnoreCase)
            && string.Equals(parameterValue, "CURRENT_SCHEMA", StringComparison.OrdinalIgnoreCase))
        {
            result = "SYS";
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalUnsupportedSysFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = null;
        return true;
    }
}
