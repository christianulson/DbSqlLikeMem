namespace DbSqlLikeMem;

internal delegate bool AstQueryTryEvalSessionContextFunction(
    QueryExecutionContext context,
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal delegate bool AstQueryTryEvalGeneralJsonFunction(
    FunctionCallExpr fn,
    QueryExecutionContext context,
    Func<int, object?> evalArg,
    out object? result);

internal delegate bool AstQueryTryEvalGeneralSystemAndJsonFunction(
    FunctionCallExpr fn,
    QueryExecutionContext context,
    Func<int, object?> evalArg,
    out object? result);

internal sealed class AstQueryGeneralSystemAndJsonFunctionEvaluator
{
    private readonly AstQueryTryEvalSessionContextFunction _tryEvalSessionContextFunction;
    private readonly AstQueryTryEvalGeneralJsonFunction _tryEvalJsonUtilityFunctions;
    private readonly AstQueryTryEvalGeneralJsonFunction _tryEvalSqliteSystemFunctions;
    private readonly AstQueryTryEvalGeneralJsonFunction _tryEvalSqliteJsonFunctions;
    private readonly IReadOnlyDictionary<string, AstQueryTryEvalGeneralSystemAndJsonFunction> _handlers;

    private static readonly object _uuidShortCounterLock = new();
    private static long _uuidShortCounter;

    internal AstQueryGeneralSystemAndJsonFunctionEvaluator(
        AstQueryTryEvalSessionContextFunction tryEvalSessionContextFunction,
        AstQueryTryEvalGeneralJsonFunction tryEvalJsonUtilityFunctions,
        AstQueryTryEvalGeneralJsonFunction tryEvalSqliteSystemFunctions,
        AstQueryTryEvalGeneralJsonFunction tryEvalSqliteJsonFunctions)
    {
        _tryEvalSessionContextFunction = tryEvalSessionContextFunction ?? throw new ArgumentNullException(nameof(tryEvalSessionContextFunction));
        _tryEvalJsonUtilityFunctions = tryEvalJsonUtilityFunctions ?? throw new ArgumentNullException(nameof(tryEvalJsonUtilityFunctions));
        _tryEvalSqliteSystemFunctions = tryEvalSqliteSystemFunctions ?? throw new ArgumentNullException(nameof(tryEvalSqliteSystemFunctions));
        _tryEvalSqliteJsonFunctions = tryEvalSqliteJsonFunctions ?? throw new ArgumentNullException(nameof(tryEvalSqliteJsonFunctions));
        _handlers = CreateHandlers();
    }

    internal bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler)
            && handler(fn, context, evalArg, out result))
        {
            return true;
        }

        if (_tryEvalJsonUtilityFunctions(fn, context, evalArg, out result)
            || _tryEvalSqliteSystemFunctions(fn, context, evalArg, out result)
            || _tryEvalSqliteJsonFunctions(fn, context, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }

    private Dictionary<string, AstQueryTryEvalGeneralSystemAndJsonFunction> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryTryEvalGeneralSystemAndJsonFunction>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalGetAnsiNullFunction, "GETANSINULL");
        Register(handlers, TryEvalGroupingFunction, "GROUPING");
        Register(handlers, TryEvalGroupingIdFunction, "GROUPING_ID");
        Register(handlers, TryEvalHostIdFunction, "HOST_ID");
        Register(handlers, TryEvalHostNameFunction, "HOST_NAME");
        Register(handlers, TryEvalSessionContextFunction, "SESSION_CONTEXT");
        Register(handlers, TryEvalIsDateFunction, "ISDATE");
        Register(handlers, TryEvalIsJsonFunction, "ISJSON");
        Register(handlers, TryEvalIsNumericFunction, "ISNUMERIC");
        Register(handlers, TryEvalIpFunctions, "IS_IPV4", "IS_IPV4_COMPAT", "IS_IPV4_MAPPED", "IS_IPV6");
        Register(handlers, TryEvalIsUuidFunction, "IS_UUID");
        Register(handlers, TryEvalJsonArrayFunction, "JSON_ARRAY");
        Register(handlers, TryEvalJsonDepthFunction, "JSON_DEPTH");
        Register(handlers, TryEvalSessionUserFunction, "SESSION_USER");
        Register(handlers, TryEvalSystemUserFunction, "SYSTEM_USER");
        Register(handlers, TryEvalUserFunction, "USER");
        Register(handlers, TryEvalUtcDateFunction, "UTC_DATE");
        Register(handlers, TryEvalUtcTimeFunction, "UTC_TIME");
        Register(handlers, TryEvalUtcTimestampFunction, "UTC_TIMESTAMP");
        Register(handlers, TryEvalUuidShortFunction, "UUID_SHORT");
        return handlers;
    }

    private static void Register(
        IDictionary<string, AstQueryTryEvalGeneralSystemAndJsonFunction> handlers,
        AstQueryTryEvalGeneralSystemAndJsonFunction handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    internal static bool TryEvalGetAnsiNullFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;
        _ = evalArg;

        result = 1;
        return true;
    }

    internal static bool TryEvalGroupingFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        result = 0;
        return true;
    }

    internal static bool TryEvalGroupingIdFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        result = 0;
        return true;
    }

    internal static bool TryEvalHostIdFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context.Dialect;
        _ = evalArg;
        result = 1;
        return true;
    }

    internal static bool TryEvalHostNameFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context.Dialect;
        _ = evalArg;
        result = "localhost";
        return true;
    }

    private bool TryEvalSessionContextFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;
        return _tryEvalSessionContextFunction(context, fn, evalArg, out result);
    }

    internal static bool TryEvalIsDateFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = 0;
            return true;
        }

        result = AstQueryExecutorBase.TryCoerceDateTime(value, out _)
            ? 1
            : 0;
        return true;
    }

    internal static bool TryEvalIsJsonFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = 0;
            return true;
        }

        try
        {
            QueryJsonFunctionHelper.TryGetJsonRootElement(value!, out _);
            result = 1;
        }
        catch
        {
            result = 0;
        }

        return true;
    }

    internal static bool TryEvalIsNumericFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = 0;
            return true;
        }

        result = double.TryParse(value?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out _)
            ? 1
            : 0;
        return true;
    }

    private static bool TryEvalIpFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;
        var name = fn.Name;
        if (!(name.Equals("IS_IPV4", StringComparison.OrdinalIgnoreCase)
            || name.Equals("IS_IPV4_COMPAT", StringComparison.OrdinalIgnoreCase)
            || name.Equals("IS_IPV4_MAPPED", StringComparison.OrdinalIgnoreCase)
            || name.Equals("IS_IPV6", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        if (!IPAddress.TryParse(text, out var ip))
        {
            result = 0;
            return true;
        }

        if (name.Equals("IS_IPV4", StringComparison.OrdinalIgnoreCase))
        {
            result = ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ? 1 : 0;
            return true;
        }

        if (name.Equals("IS_IPV6", StringComparison.OrdinalIgnoreCase))
        {
            result = ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6 ? 1 : 0;
            return true;
        }

        if (ip.AddressFamily != System.Net.Sockets.AddressFamily.InterNetworkV6)
        {
            result = 0;
            return true;
        }

        var bytes = ip.GetAddressBytes();
        if (bytes.Length != 16)
        {
            result = 0;
            return true;
        }

        var isV4Mapped = bytes.Take(10).All(static b => b == 0) && bytes[10] == 0xff && bytes[11] == 0xff;
        result = name.Equals("IS_IPV4_MAPPED", StringComparison.OrdinalIgnoreCase)
            ? (isV4Mapped ? 1 : 0)
            : (name.Equals("IS_IPV4_COMPAT", StringComparison.OrdinalIgnoreCase) ? (!isV4Mapped && bytes.Take(12).All(static b => b == 0) ? 1 : 0) : 0);
        return true;
    }

    private static bool TryEvalIsUuidFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;
        _ = evalArg;

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = 0;
            return true;
        }

        result = Guid.TryParse(value?.ToString(), out _)
            ? 1
            : 0;
        return true;
    }

    private static bool TryEvalJsonArrayFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;

        var values = new object?[fn.Args.Count];
        for (var i = 0; i < fn.Args.Count; i++)
            values[i] = evalArg(i);

        result = JsonSerializer.Serialize(values);
        return true;
    }

    private static bool TryEvalJsonDepthFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            if (!QueryJsonFunctionHelper.TryGetJsonRootElement(value!, out var root))
            {
                result = null;
                return true;
            }

            result = GetJsonDepth(root);
        }
        catch
        {
            result = null;
        }

        return true;
    }

    private static int GetJsonDepth(JsonElement element)
    {
        if (element.ValueKind is JsonValueKind.Object)
        {
            var maxDepth = 0;
            foreach (var property in element.EnumerateObject())
                maxDepth = Math.Max(maxDepth, GetJsonDepth(property.Value));

            return 1 + maxDepth;
        }

        if (element.ValueKind is JsonValueKind.Array)
        {
            var maxDepth = 0;
            foreach (var item in element.EnumerateArray())
                maxDepth = Math.Max(maxDepth, GetJsonDepth(item));

            return 1 + maxDepth;
        }

        return 1;
    }

    internal static bool TryEvalSessionUserFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        _ = context;

        result = "dbo";
        return true;
    }

    internal static bool TryEvalSystemUserFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;
        _ = evalArg;

        result = "dbo";
        return true;
    }

    internal static bool TryEvalUserFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;
        _ = evalArg;

        result = "dbo";
        return true;
    }

    private static bool TryEvalUtcDateFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;
        _ = evalArg;

        result = DateTime.UtcNow.Date;
        return true;
    }

    private static bool TryEvalUtcTimeFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;
        _ = evalArg;

        result = DateTime.UtcNow.TimeOfDay;
        return true;
    }

    private static bool TryEvalUtcTimestampFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;
        _ = evalArg;

        result = DateTime.UtcNow;
        return true;
    }

    private static bool TryEvalUuidShortFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;
        _ = evalArg;

        if (fn.Args.Count > 0)
            throw new InvalidOperationException("UUID_SHORT() não aceita argumentos.");

        var baseValue = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000L;
        lock (_uuidShortCounterLock)
        {
            if (_uuidShortCounter < baseValue)
                _uuidShortCounter = baseValue;

            _uuidShortCounter++;
            result = _uuidShortCounter;
        }

        return true;
    }
}
