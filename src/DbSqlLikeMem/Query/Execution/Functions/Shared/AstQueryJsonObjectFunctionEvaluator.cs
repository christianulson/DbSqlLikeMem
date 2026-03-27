namespace DbSqlLikeMem;

internal static class AstQueryJsonObjectFunctionEvaluator
{
    internal static bool TryEvalJsonObjectFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (!string.Equals(fn.Name, "JSON_OBJECT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count % 2 != 0)
            throw new InvalidOperationException("JSON_OBJECT() espera um número par de argumentos.");

        var pairs = new List<(string Key, object? Value)>();
        for (var i = 0; i < fn.Args.Count; i += 2)
        {
            var key = evalArg(i)?.ToString() ?? string.Empty;
            var val = evalArg(i + 1);
            pairs.Add((key, val));
        }

        result = BuildJsonObject(pairs);
        return true;
    }

    private static string BuildJsonObject(IEnumerable<(string Key, object? Value)> pairs)
    {
        var parts = pairs.Select(static pair =>
        {
            var key = JsonSerializer.Serialize(pair.Key ?? string.Empty);
            var value = pair.Value;
            if (value is null or DBNull)
                return $"{key}:null";

            if (value is JsonElement element)
                return $"{key}:{element.GetRawText()}";

            return $"{key}:{JsonSerializer.Serialize(value)}";
        });

        return "{" + string.Join(",", parts) + "}";
    }
}
