namespace DbSqlLikeMem;

internal static class QueryOracleDb2UtilityFunctionHelper
{
    private delegate bool OracleDb2UtilityFunctionHandler(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result);

    private static readonly IReadOnlyDictionary<string, OracleDb2UtilityFunctionHandler> _handlers =
        CreateHandlers();

    public static bool TryEvalUtilityFunctions(
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

    private static Dictionary<string, OracleDb2UtilityFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, OracleDb2UtilityFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalCardinalityFunction, "CARDINALITY");
        Register(handlers, TryEvalChrFunction, "CHR");
        Register(handlers, TryEvalComposeFunction, "COMPOSE");
        Register(handlers, TryEvalDbTimeZoneFunction, "DBTIMEZONE");
        Register(handlers, TryEvalDecomposeFunction, "DECOMPOSE");
        Register(handlers, TryEvalEmptyBlobFunction, "EMPTY_BLOB");
        Register(handlers, TryEvalEmptyClobFunction, "EMPTY_CLOB", "EMPTY_DBCLOB", "EMPTY_NCLOB");
        Register(handlers, TryEvalInitCapFunction, "INITCAP");
        Register(handlers, TryEvalChartoRowidFunction, "CHARTOROWID");
        Register(handlers, TryEvalClusterFunctions, "CLUSTER_DETAILS", "CLUSTER_DISTANCE", "CLUSTER_ID", "CLUSTER_PROBABILITY", "CLUSTER_SET");
        Register(handlers, TryEvalLastFoundRowsFunction, "ROW_COUNT", "FOUND_ROWS", "ROWCOUNT", "CHANGES");
        return handlers;
    }

    private static void Register(
        IDictionary<string, OracleDb2UtilityFunctionHandler> handlers,
        OracleDb2UtilityFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalCardinalityFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        context.EnsureOracleDb2FunctionSupported(fn);

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is JsonElement element
            && element.ValueKind == JsonValueKind.Array)
        {
            result = element.GetArrayLength();
            return true;
        }

        if (value is string)
        {
            result = null;
            return true;
        }

        if (value is Array arr)
        {
            result = arr.Length;
            return true;
        }

        if (value is ICollection collection)
        {
            result = collection.Count;
            return true;
        }

        if (value is IEnumerable enumerable)
        {
            var count = 0;
            foreach (var _ in enumerable)
                count++;
            result = count;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalChrFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        context.EnsureOracleDb2FunctionSupported(fn);

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var code = Convert.ToInt32(value.ToDec(), CultureInfo.InvariantCulture);
            if (code < 0 || code > 0x10FFFF)
            {
                result = null;
                return true;
            }

            result = char.ConvertFromUtf32(code);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalComposeFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        context.EnsureOracleDb2FunctionSupported(fn);

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        result = (value?.ToString() ?? string.Empty).Normalize(NormalizationForm.FormC);
        return true;
    }

    private static bool TryEvalDbTimeZoneFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        context.EnsureOracleDb2FunctionSupported(fn);

        result = "+00:00";
        return true;
    }

    private static bool TryEvalDecomposeFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        context.EnsureOracleDb2FunctionSupported(fn);

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        result = (value?.ToString() ?? string.Empty).Normalize(NormalizationForm.FormD);
        return true;
    }

    private static bool TryEvalEmptyBlobFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        context.EnsureOracleDb2FunctionSupported(fn);

        result = Array.Empty<byte>();
        return true;
    }

    private static bool TryEvalEmptyClobFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        context.EnsureOracleDb2FunctionSupported(fn);

        result = string.Empty;
        return true;
    }

    private static bool TryEvalInitCapFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        context.EnsureOracleDb2FunctionSupported(fn);

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        if (text.Length == 0)
        {
            result = string.Empty;
            return true;
        }

        var builder = new StringBuilder(text.Length);
        var makeUpper = true;
        foreach (var ch in text)
        {
            if (char.IsLetterOrDigit(ch))
            {
                builder.Append(makeUpper
                    ? char.ToUpperInvariant(ch)
                    : char.ToLowerInvariant(ch));
                makeUpper = false;
            }
            else
            {
                builder.Append(ch);
                makeUpper = true;
            }
        }

        result = builder.ToString();
        return true;
    }

    private static bool TryEvalChartoRowidFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        context.EnsureOracleDb2FunctionSupported(fn);

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        result = value?.ToString();
        return true;
    }

    private static bool TryEvalClusterFunctions(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        context.EnsureOracleDb2FunctionSupported(fn);
        result = null;
        return true;

    }

    private static bool TryEvalLastFoundRowsFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        context.EnsureOracleDb2FunctionSupported(fn);

        if (fn.Args.Count != 0)
        {
            result = null;
            return false;
        }

        result = context.Connection.GetLastFoundRows();
        return true;
    }

    internal static void EnsureOracleDb2FunctionSupported(
        this QueryExecutionContext context,
        FunctionCallExpr fn)
    {
        if (fn.ResolvedScalarFunction is { AllowsCall: true })
        {
            return;
        }

        context.EnsureOracleDb2FunctionSupported(fn.Name);
    }

    internal static void EnsureOracleDb2FunctionSupported(
        this QueryExecutionContext context,
        string name)
    {
        if (context.Dialect.TryGetScalarFunctionDefinition(name, out var definition)
            && (definition is null || definition.AllowsCall))
        {
            return;
        }

        throw SqlUnsupported.NotSupported(context.Dialect, name);
    }

    private static bool IsNullish(object? value) => value is null or DBNull;
}
