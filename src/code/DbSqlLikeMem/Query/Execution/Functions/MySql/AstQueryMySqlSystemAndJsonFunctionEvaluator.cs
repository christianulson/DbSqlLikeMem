namespace DbSqlLikeMem;

internal static class AstQueryMySqlSystemAndJsonFunctionEvaluator
{
    private static readonly object _uuidShortCounterLock = new();
    private static long _uuidShortCounter;

    internal static bool TryEvaluate(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (string.Equals(fn.Name, "UUID_SHORT", StringComparison.OrdinalIgnoreCase))
            return TryEvalUuidShortFunction(context, fn, evalArg, out result);

        if (string.Equals(fn.Name, "JSON_ARRAY", StringComparison.OrdinalIgnoreCase))
            return AstQueryJsonArrayFunctionEvaluator.TryEvalJsonArrayFunction(context, fn, evalArg, out result);

        if (string.Equals(fn.Name, "JSON_DEPTH", StringComparison.OrdinalIgnoreCase))
            return TryEvalJsonDepthFunction(context, fn, evalArg, out result);

        result = null;
        return false;
    }

    private static bool TryEvalJsonDepthFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
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

    private static bool TryEvalUuidShortFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
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
}
